package master

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"tendis-migrate/internal/ipc"
	"tendis-migrate/internal/storage"
	"tendis-migrate/pkg/logger"
)

// WorkerPoolManager Worker 进程池管理器
type WorkerPoolManager struct {
	taskID        string
	numWorkers    int
	workerBinary  string // Worker 二进制文件路径
	socketPath    string
	db            *storage.SQLiteDB
	slotManager   *SlotManager
	ipcServer     *ipc.Server
	logger        *logger.TaskLogger

	// Worker 进程管理
	workers   map[int]*WorkerProcess // workerID -> process
	workersMu sync.RWMutex

	// 任务配置（用于重启）
	task *storage.Task

	// 自动重启配置
	autoRestart       bool          // 是否自动重启
	maxRestartCount   int           // 最大重启次数
	restartCooldown   time.Duration // 重启冷却时间
	restartCounts     map[int]int   // workerID -> 重启次数
	lastRestartTimes  map[int]time.Time // workerID -> 最后重启时间

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
}

// WorkerProcess Worker 进程信息
type WorkerProcess struct {
	WorkerID      int
	PID           int
	Cmd           *exec.Cmd
	Status        string // idle, running, completed, crashed
	StartTime     time.Time
	LastHeartbeat time.Time
}

// NewWorkerPoolManager 创建 Worker 池管理器
func NewWorkerPoolManager(
	taskID string,
	numWorkers int,
	workerBinary string,
	socketPath string,
	db *storage.SQLiteDB,
	slotManager *SlotManager,
	ipcServer *ipc.Server,
	logger *logger.TaskLogger,
) *WorkerPoolManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPoolManager{
		taskID:           taskID,
		numWorkers:       numWorkers,
		workerBinary:     workerBinary,
		socketPath:       socketPath,
		db:               db,
		slotManager:      slotManager,
		ipcServer:        ipcServer,
		logger:           logger,
		workers:          make(map[int]*WorkerProcess),
		autoRestart:      true,                    // 默认开启自动重启
		maxRestartCount:  3,                       // 最大重启 3 次
		restartCooldown:  30 * time.Second,        // 冷却时间 30 秒
		restartCounts:    make(map[int]int),
		lastRestartTimes: make(map[int]time.Time),
		ctx:              ctx,
		cancel:           cancel,
	}
}

// StartAllWorkers 启动所有 Worker 进程
func (wpm *WorkerPoolManager) StartAllWorkers(task *storage.Task) error {
	wpm.logger.Info("Starting all workers", map[string]interface{}{
		"num_workers": wpm.numWorkers,
	})

	for workerID := 0; workerID < wpm.numWorkers; workerID++ {
		if err := wpm.StartWorker(workerID, task); err != nil {
			wpm.logger.Error("Failed to start worker", map[string]interface{}{
				"worker_id": workerID,
				"error":     err.Error(),
			})
			return err
		}
	}

	// 启动 Worker 监控 goroutine
	go wpm.monitorWorkers()

	return nil
}

// StartWorker 启动单个 Worker 进程
func (wpm *WorkerPoolManager) StartWorker(workerID int, task *storage.Task) error {
	wpm.workersMu.Lock()
	defer wpm.workersMu.Unlock()

	// 检查是否已存在
	if worker, exists := wpm.workers[workerID]; exists && worker.Status == "running" {
		return fmt.Errorf("worker %d already running", workerID)
	}

	// 获取分配的 Slot
	slots := wpm.slotManager.GetWorkerSlots(workerID)
	if len(slots) == 0 {
		return fmt.Errorf("no slots assigned to worker %d", workerID)
	}

	// 构建命令行参数
	slotStrs := make([]string, len(slots))
	for i, slot := range slots {
		slotStrs[i] = strconv.Itoa(slot)
	}

	args := []string{
		"--task-id", wpm.taskID,
		"--worker-id", strconv.Itoa(workerID),
		"--slots", strings.Join(slotStrs, ","),
		"--master-socket", wpm.socketPath,
		"--source-cluster", task.SourceCluster,
		"--target-cluster", task.TargetCluster,
		"--source-password", task.SourcePassword,
		"--target-password", task.TargetPassword,
		"--options", task.Options,
	}

	cmd := exec.Command(wpm.workerBinary, args...)

	// 设置环境变量
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("WORKER_ID=%d", workerID),
		fmt.Sprintf("TASK_ID=%s", wpm.taskID),
	)

	// 重定向日志
	logDir := "./logs"
	os.MkdirAll(logDir, 0755)
	
	logFile, err := os.Create(fmt.Sprintf("%s/worker-%d.log", logDir, workerID))
	if err != nil {
		return fmt.Errorf("create log file failed: %w", err)
	}

	cmd.Stdout = logFile
	cmd.Stderr = logFile

	// 启动进程
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("start worker process failed: %w", err)
	}

	// 记录 Worker 信息
	worker := &WorkerProcess{
		WorkerID:      workerID,
		PID:           cmd.Process.Pid,
		Cmd:           cmd,
		Status:        "running",
		StartTime:     time.Now(),
		LastHeartbeat: time.Now(),
	}
	wpm.workers[workerID] = worker

	// 持久化到数据库
	if err := wpm.db.CreateWorker(&storage.WorkerStatus{
		TaskID:        wpm.taskID,
		WorkerID:      workerID,
		PID:           worker.PID,
		Status:        "running",
		AssignedSlots: slots,
		CreatedAt:     time.Now().Format(time.RFC3339),
		UpdatedAt:     time.Now().Format(time.RFC3339),
	}); err != nil {
		wpm.logger.Warn("Failed to save worker to database", map[string]interface{}{
			"worker_id": workerID,
			"error":     err.Error(),
		})
	}

	// 监控进程退出
	go wpm.waitForWorker(worker, logFile)

	wpm.logger.Info("Worker started", map[string]interface{}{
		"worker_id":     workerID,
		"pid":           worker.PID,
		"assigned_slots": len(slots),
	})

	return nil
}

// waitForWorker 等待 Worker 进程退出
func (wpm *WorkerPoolManager) waitForWorker(worker *WorkerProcess, logFile *os.File) {
	defer logFile.Close()

	err := worker.Cmd.Wait()

	wpm.workersMu.Lock()
	defer wpm.workersMu.Unlock()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
	}

	// 更新状态
	if exitCode == 0 {
		worker.Status = "completed"
	} else {
		worker.Status = "crashed"
	}

	// 更新数据库
	wpm.db.UpdateWorkerStatus(wpm.taskID, worker.WorkerID, worker.Status)

	wpm.logger.Info("Worker exited", map[string]interface{}{
		"worker_id": worker.WorkerID,
		"pid":       worker.PID,
		"exit_code": exitCode,
		"status":    worker.Status,
	})

	// 如果崩溃，尝试重启（可选）
	if exitCode != 0 {
		wpm.onWorkerCrashed(worker.WorkerID)
	}
}

// onWorkerCrashed Worker 崩溃处理（自动重启）
func (wpm *WorkerPoolManager) onWorkerCrashed(workerID int) {
	wpm.logger.Warn("Worker crashed, checking auto-restart", map[string]interface{}{
		"worker_id": workerID,
	})

	// 检查是否开启自动重启
	if !wpm.autoRestart {
		wpm.logger.Info("Auto-restart disabled, manual intervention required", map[string]interface{}{
			"worker_id": workerID,
		})
		return
	}

	// 检查重启次数
	wpm.workersMu.Lock()
	restartCount := wpm.restartCounts[workerID]
	lastRestart := wpm.lastRestartTimes[workerID]
	wpm.workersMu.Unlock()

	if restartCount >= wpm.maxRestartCount {
		wpm.logger.Error("Worker exceeded max restart count, giving up", map[string]interface{}{
			"worker_id":     workerID,
			"restart_count": restartCount,
			"max_restarts":  wpm.maxRestartCount,
		})
		// 标记该 Worker 的 Slot 为失败
		wpm.markWorkerSlotsFailed(workerID)
		return
	}

	// 检查冷却时间
	if time.Since(lastRestart) < wpm.restartCooldown {
		wpm.logger.Warn("Worker restart in cooldown period", map[string]interface{}{
			"worker_id":        workerID,
			"cooldown_seconds": wpm.restartCooldown.Seconds(),
			"wait_seconds":     (wpm.restartCooldown - time.Since(lastRestart)).Seconds(),
		})
		// 等待冷却时间后重启
		go func() {
			waitTime := wpm.restartCooldown - time.Since(lastRestart)
			time.Sleep(waitTime)
			wpm.restartWorker(workerID)
		}()
		return
	}

	// 立即重启
	wpm.restartWorker(workerID)
}

// restartWorker 重启 Worker
func (wpm *WorkerPoolManager) restartWorker(workerID int) {
	wpm.workersMu.Lock()
	wpm.restartCounts[workerID]++
	wpm.lastRestartTimes[workerID] = time.Now()
	restartCount := wpm.restartCounts[workerID]
	wpm.workersMu.Unlock()

	wpm.logger.Info("Restarting worker", map[string]interface{}{
		"worker_id":     workerID,
		"restart_count": restartCount,
		"max_restarts":  wpm.maxRestartCount,
	})

	// 重置该 Worker 的 in_progress Slot 为 pending
	if err := wpm.resetWorkerSlots(workerID); err != nil {
		wpm.logger.Error("Failed to reset worker slots", map[string]interface{}{
			"worker_id": workerID,
			"error":     err.Error(),
		})
	}

	// 启动新 Worker
	if wpm.task != nil {
		if err := wpm.StartWorker(workerID, wpm.task); err != nil {
			wpm.logger.Error("Failed to restart worker", map[string]interface{}{
				"worker_id": workerID,
				"error":     err.Error(),
			})
		} else {
			wpm.logger.Info("Worker restarted successfully", map[string]interface{}{
				"worker_id":     workerID,
				"restart_count": restartCount,
			})
		}
	}
}

// resetWorkerSlots 重置 Worker 的 in_progress Slot
func (wpm *WorkerPoolManager) resetWorkerSlots(workerID int) error {
	slots := wpm.slotManager.GetWorkerSlots(workerID)
	for _, slot := range slots {
		status, err := wpm.db.GetSlotStatus(wpm.taskID, slot)
		if err != nil {
			continue
		}
		if status.Status == "in_progress" {
			wpm.db.UpdateSlotStatus(wpm.taskID, slot, "pending")
			wpm.logger.Debug("Reset slot to pending", map[string]interface{}{
				"slot":      slot,
				"worker_id": workerID,
			})
		}
	}
	return nil
}

// markWorkerSlotsFailed 标记 Worker 的所有未完成 Slot 为失败
func (wpm *WorkerPoolManager) markWorkerSlotsFailed(workerID int) {
	slots := wpm.slotManager.GetWorkerSlots(workerID)
	for _, slot := range slots {
		status, err := wpm.db.GetSlotStatus(wpm.taskID, slot)
		if err != nil {
			continue
		}
		if status.Status != "completed" {
			wpm.db.UpdateSlotStatus(wpm.taskID, slot, "failed")
			wpm.logger.Warn("Marked slot as failed", map[string]interface{}{
				"slot":      slot,
				"worker_id": workerID,
			})
		}
	}
}

// SetTask 设置任务信息（用于 Worker 重启）
func (wpm *WorkerPoolManager) SetTask(task *storage.Task) {
	wpm.task = task
}

// SetAutoRestart 设置自动重启配置
func (wpm *WorkerPoolManager) SetAutoRestart(enabled bool, maxCount int, cooldown time.Duration) {
	wpm.workersMu.Lock()
	defer wpm.workersMu.Unlock()
	wpm.autoRestart = enabled
	if maxCount > 0 {
		wpm.maxRestartCount = maxCount
	}
	if cooldown > 0 {
		wpm.restartCooldown = cooldown
	}
}

// ResetRestartCounts 重置所有 Worker 的重启计数
func (wpm *WorkerPoolManager) ResetRestartCounts() {
	wpm.workersMu.Lock()
	defer wpm.workersMu.Unlock()
	wpm.restartCounts = make(map[int]int)
	wpm.lastRestartTimes = make(map[int]time.Time)
}

// monitorWorkers 监控 Worker 健康状态
func (wpm *WorkerPoolManager) monitorWorkers() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-wpm.ctx.Done():
			return
		case <-ticker.C:
			wpm.checkWorkersHealth()
		}
	}
}

// checkWorkersHealth 检查 Worker 健康状态
func (wpm *WorkerPoolManager) checkWorkersHealth() {
	wpm.workersMu.RLock()
	defer wpm.workersMu.RUnlock()

	now := time.Now()
	for workerID, worker := range wpm.workers {
		if worker.Status != "running" {
			continue
		}

		// 检查心跳超时（30 秒无心跳）
		if now.Sub(worker.LastHeartbeat) > 30*time.Second {
			wpm.logger.Warn("Worker heartbeat timeout", map[string]interface{}{
				"worker_id":      workerID,
				"last_heartbeat": worker.LastHeartbeat.Format(time.RFC3339),
			})

			// 可以选择杀死僵尸进程
			// worker.Cmd.Process.Kill()
		}
	}
}

// UpdateWorkerHeartbeat 更新 Worker 心跳时间
func (wpm *WorkerPoolManager) UpdateWorkerHeartbeat(workerID int) {
	wpm.workersMu.Lock()
	defer wpm.workersMu.Unlock()

	if worker, exists := wpm.workers[workerID]; exists {
		worker.LastHeartbeat = time.Now()
	}
}

// StopWorker 停止单个 Worker
func (wpm *WorkerPoolManager) StopWorker(workerID int, graceful bool) error {
	wpm.workersMu.Lock()
	defer wpm.workersMu.Unlock()

	worker, exists := wpm.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %d not found", workerID)
	}

	if worker.Status != "running" {
		return fmt.Errorf("worker %d not running (status: %s)", workerID, worker.Status)
	}

	wpm.logger.Info("Stopping worker", map[string]interface{}{
		"worker_id": workerID,
		"pid":       worker.PID,
		"graceful":  graceful,
	})

	// 发送停止消息
	msg, _ := ipc.NewIPCMessage(ipc.MsgTypeShutdown, &ipc.MsgShutdown{
		TaskID:   wpm.taskID,
		Reason:   "manual_stop",
		Graceful: graceful,
	})
	wpm.ipcServer.SendMessage(workerID, msg)

	// 等待优雅退出
	if graceful {
		time.Sleep(5 * time.Second)
	}

	// 强制杀死
	if worker.Cmd.Process != nil {
		if graceful {
			worker.Cmd.Process.Signal(syscall.SIGTERM)
			time.Sleep(2 * time.Second)
		}
		worker.Cmd.Process.Kill()
	}

	worker.Status = "stopped"
	return nil
}

// StopAllWorkers 停止所有 Worker
func (wpm *WorkerPoolManager) StopAllWorkers(graceful bool) error {
	wpm.workersMu.RLock()
	workerIDs := make([]int, 0, len(wpm.workers))
	for workerID := range wpm.workers {
		workerIDs = append(workerIDs, workerID)
	}
	wpm.workersMu.RUnlock()

	for _, workerID := range workerIDs {
		if err := wpm.StopWorker(workerID, graceful); err != nil {
			wpm.logger.Warn("Failed to stop worker", map[string]interface{}{
				"worker_id": workerID,
				"error":     err.Error(),
			})
		}
	}

	wpm.cancel()
	return nil
}

// GetWorkerStatus 获取 Worker 状态
func (wpm *WorkerPoolManager) GetWorkerStatus(workerID int) *WorkerProcess {
	wpm.workersMu.RLock()
	defer wpm.workersMu.RUnlock()
	return wpm.workers[workerID]
}

// GetAllWorkersStatus 获取所有 Worker 状态
func (wpm *WorkerPoolManager) GetAllWorkersStatus() []*WorkerProcess {
	wpm.workersMu.RLock()
	defer wpm.workersMu.RUnlock()

	result := make([]*WorkerProcess, 0, len(wpm.workers))
	for _, worker := range wpm.workers {
		result = append(result, worker)
	}

	return result
}
