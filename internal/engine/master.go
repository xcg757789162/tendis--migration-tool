package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"tendis-migrate/internal/ipc"
	"tendis-migrate/internal/model"
	"tendis-migrate/internal/storage"
)

const (
	TotalSlots     = 16384
	SocketPath     = "/tmp/tendis-migrate/master.sock"
	WorkerBinary   = "./tendis-migrate-worker"
)

// MasterState Master状态
type MasterState int32

const (
	StateInit MasterState = iota
	StateRunning
	StatePaused
	StateStopping
	StateStopped
)

// Master 主进程
type Master struct {
	config       *Config
	store        *storage.SQLiteStore
	ipcServer    *ipc.Server
	scheduler    *Scheduler
	workerPool   *WorkerPool
	
	tasks        map[string]*TaskRunner
	tasksMu      sync.RWMutex
	
	state        atomic.Int32
	
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// Config Master配置
type Config struct {
	DataDir       string `json:"data_dir"`
	Port          int    `json:"port"`
	WorkerCount   int    `json:"worker_count"`
	SocketPath    string `json:"socket_path"`
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		DataDir:     "./data",
		Port:        8080,
		WorkerCount: 4,
		SocketPath:  SocketPath,
	}
}

// NewMaster 创建Master
func NewMaster(cfg *Config) (*Master, error) {
	// 确保数据目录存在
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}

	// 创建存储
	storeCfg := storage.DefaultSQLiteConfig
	storeCfg.Path = filepath.Join(cfg.DataDir, "migrate.db")
	store, err := storage.NewSQLiteStore(storeCfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	m := &Master{
		config:     cfg,
		store:      store,
		tasks:      make(map[string]*TaskRunner),
		ctx:        ctx,
		cancel:     cancel,
	}

	// 创建IPC服务
	m.ipcServer = ipc.NewServer(cfg.SocketPath)
	m.registerIPCHandlers()

	// 创建调度器
	m.scheduler = NewScheduler(m)

	// 创建Worker池
	m.workerPool = NewWorkerPool(cfg.WorkerCount, cfg.SocketPath)

	return m, nil
}

// registerIPCHandlers 注册IPC处理器
func (m *Master) registerIPCHandlers() {
	m.ipcServer.RegisterHandler(ipc.MsgCheckpointReport, m.handleCheckpointReport)
	m.ipcServer.RegisterHandler(ipc.MsgProgressReport, m.handleProgressReport)
	m.ipcServer.RegisterHandler(ipc.MsgError, m.handleWorkerError)
	m.ipcServer.RegisterHandler(ipc.MsgCompleted, m.handleWorkerCompleted)
	m.ipcServer.RegisterHandler(ipc.MsgPong, m.handlePong)
}

// Start 启动Master
func (m *Master) Start() error {
	m.state.Store(int32(StateRunning))

	// 启动IPC服务
	if err := m.ipcServer.Start(); err != nil {
		return err
	}

	// 启动Worker池
	if err := m.workerPool.Start(); err != nil {
		return err
	}

	// 启动心跳检测
	m.wg.Add(1)
	go m.heartbeatLoop()

	// 恢复未完成的任务
	if err := m.recoverTasks(); err != nil {
		log.Printf("Warning: recover tasks failed: %v", err)
	}

	return nil
}

// Stop 停止Master
func (m *Master) Stop() {
	m.state.Store(int32(StateStopping))
	m.cancel()
	
	// 停止所有任务
	m.tasksMu.RLock()
	for _, runner := range m.tasks {
		runner.Stop()
	}
	m.tasksMu.RUnlock()

	// 停止Worker池
	m.workerPool.Stop()

	// 停止IPC服务
	m.ipcServer.Stop()

	// 等待goroutine退出
	m.wg.Wait()

	// 关闭存储
	m.store.Close()

	m.state.Store(int32(StateStopped))
}

// heartbeatLoop 心跳检测
func (m *Master) heartbeatLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(ipc.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.ipcServer.BroadcastToWorkers(&ipc.Message{Type: ipc.MsgPing})
		case <-m.ctx.Done():
			return
		}
	}
}

// recoverTasks 恢复未完成的任务
func (m *Master) recoverTasks() error {
	tasks, _, err := m.store.ListTasks("running", 1, 100)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		log.Printf("Recovering task: %s", task.ID)
		// 暂停任务，等待用户手动恢复
		m.store.UpdateTaskStatus(task.ID, model.TaskStatusPaused)
	}

	return nil
}

// CreateTask 创建任务
func (m *Master) CreateTask(req *CreateTaskRequest) (*model.Task, error) {
	task := &model.Task{
		ID:            uuid.New().String(),
		Name:          req.Name,
		SourceCluster: storage.ToJSON(req.SourceCluster),
		TargetCluster: storage.ToJSON(req.TargetCluster),
		Status:        model.TaskStatusPending,
		Config:        storage.ToJSON(req.Options),
	}

	if err := m.store.CreateTask(task); err != nil {
		return nil, err
	}

	// 创建统计记录
	m.store.GetOrCreateStats(task.ID)

	return task, nil
}

// StartTask 启动任务
func (m *Master) StartTask(taskID string) error {
	task, err := m.store.GetTask(taskID)
	if err != nil {
		return err
	}
	if task == nil {
		return fmt.Errorf("task not found: %s", taskID)
	}

	if task.Status == model.TaskStatusRunning {
		return fmt.Errorf("task already running")
	}

	// 更新状态
	if err := m.store.UpdateTaskStarted(taskID); err != nil {
		return err
	}

	// 创建TaskRunner
	runner, err := NewTaskRunner(m, task)
	if err != nil {
		m.store.UpdateTaskStatus(taskID, model.TaskStatusFailed)
		return err
	}

	m.tasksMu.Lock()
	m.tasks[taskID] = runner
	m.tasksMu.Unlock()

	// 启动任务
	go runner.Run()

	return nil
}

// PauseTask 暂停任务
func (m *Master) PauseTask(taskID string) error {
	m.tasksMu.RLock()
	runner, ok := m.tasks[taskID]
	m.tasksMu.RUnlock()

	if !ok {
		return fmt.Errorf("task not running: %s", taskID)
	}

	runner.Pause()
	return m.store.UpdateTaskStatus(taskID, model.TaskStatusPaused)
}

// ResumeTask 恢复任务
func (m *Master) ResumeTask(taskID string) error {
	m.tasksMu.RLock()
	runner, ok := m.tasks[taskID]
	m.tasksMu.RUnlock()

	if ok {
		runner.Resume()
		return m.store.UpdateTaskStatus(taskID, model.TaskStatusRunning)
	}

	// 任务不在运行，重新启动
	return m.StartTask(taskID)
}

// StopTask 停止任务
func (m *Master) StopTask(taskID string) error {
	m.tasksMu.Lock()
	runner, ok := m.tasks[taskID]
	if ok {
		delete(m.tasks, taskID)
	}
	m.tasksMu.Unlock()

	if ok {
		runner.Stop()
	}

	return m.store.UpdateTaskStatus(taskID, model.TaskStatusPaused)
}

// DeleteTask 删除任务
func (m *Master) DeleteTask(taskID string) error {
	// 先停止任务
	m.StopTask(taskID)
	return m.store.DeleteTask(taskID)
}

// GetTask 获取任务
func (m *Master) GetTask(taskID string) (*model.Task, error) {
	return m.store.GetTask(taskID)
}

// ListTasks 获取任务列表
func (m *Master) ListTasks(status string, page, size int) ([]*model.Task, int, error) {
	return m.store.ListTasks(status, page, size)
}

// GetTaskProgress 获取任务进度
func (m *Master) GetTaskProgress(taskID string) (*model.Progress, error) {
	return m.store.GetTaskProgress(taskID)
}

// GetTaskStats 获取任务统计
func (m *Master) GetTaskStats(taskID string) (*model.MigrationStats, error) {
	return m.store.GetOrCreateStats(taskID)
}

// TriggerVerify 触发校验
func (m *Master) TriggerVerify(taskID string) (string, error) {
	m.tasksMu.RLock()
	runner, ok := m.tasks[taskID]
	m.tasksMu.RUnlock()

	if !ok {
		return "", fmt.Errorf("task not running")
	}

	return runner.TriggerVerify()
}

// GetVerifyResults 获取校验结果
func (m *Master) GetVerifyResults(taskID string) ([]*model.VerifyResult, error) {
	return m.store.GetVerifyResults(taskID)
}

// IPC Handlers

func (m *Master) handleCheckpointReport(conn net.Conn, msg *ipc.Message) error {
	data, _ := json.Marshal(msg.Data)
	var report ipc.CheckpointReport
	json.Unmarshal(data, &report)

	// 保存断点
	cp := &model.Checkpoint{
		WorkerID:      report.WorkerID,
		SlotID:        report.SlotID,
		Cursor:        report.Cursor,
		KeysMigrated:  report.Keys,
		BytesMigrated: report.Bytes,
		LastKey:       report.LastKey,
	}

	// 找到对应的任务
	m.tasksMu.RLock()
	for taskID, runner := range m.tasks {
		if runner.HasWorker(report.WorkerID) {
			cp.TaskID = taskID
			break
		}
	}
	m.tasksMu.RUnlock()

	if cp.TaskID != "" {
		m.store.SaveCheckpoint(cp)
	}

	return nil
}

func (m *Master) handleProgressReport(conn net.Conn, msg *ipc.Message) error {
	data, _ := json.Marshal(msg.Data)
	var report ipc.ProgressReport
	json.Unmarshal(data, &report)

	// 找到对应的任务
	m.tasksMu.RLock()
	for taskID, runner := range m.tasks {
		if runner.HasWorker(report.WorkerID) {
			m.store.IncrementStats(taskID, report.KeysProcessed, report.BytesSent)
			break
		}
	}
	m.tasksMu.RUnlock()

	return nil
}

func (m *Master) handleWorkerError(conn net.Conn, msg *ipc.Message) error {
	data, _ := json.Marshal(msg.Data)
	var report ipc.ErrorReport
	json.Unmarshal(data, &report)

	log.Printf("Worker error: worker=%s, slot=%d, error=%s", report.WorkerID, report.SlotID, report.Error)

	return nil
}

func (m *Master) handleWorkerCompleted(conn net.Conn, msg *ipc.Message) error {
	data, _ := json.Marshal(msg.Data)
	var info map[string]interface{}
	json.Unmarshal(data, &info)

	workerID, _ := info["worker_id"].(string)
	log.Printf("Worker completed: %s", workerID)

	return nil
}

func (m *Master) handlePong(conn net.Conn, msg *ipc.Message) error {
	// 更新Worker活跃时间
	return nil
}

// Store 获取存储
func (m *Master) Store() *storage.SQLiteStore {
	return m.store
}

// IPCServer 获取IPC服务
func (m *Master) IPCServer() *ipc.Server {
	return m.ipcServer
}

// CreateTaskRequest 创建任务请求
type CreateTaskRequest struct {
	Name          string                `json:"name"`
	SourceCluster *model.ClusterConfig  `json:"source_cluster"`
	TargetCluster *model.ClusterConfig  `json:"target_cluster"`
	Options       *model.MigrationOptions `json:"options,omitempty"`
}

// Scheduler 调度器
type Scheduler struct {
	master *Master
}

// NewScheduler 创建调度器
func NewScheduler(m *Master) *Scheduler {
	return &Scheduler{master: m}
}

// AssignSlots 分配Slots到Workers
func (s *Scheduler) AssignSlots(taskID string, workerCount int) ([]SlotRange, error) {
	slotsPerWorker := TotalSlots / workerCount
	ranges := make([]SlotRange, workerCount)

	for i := 0; i < workerCount; i++ {
		start := i * slotsPerWorker
		end := start + slotsPerWorker - 1
		if i == workerCount-1 {
			end = TotalSlots - 1
		}
		ranges[i] = SlotRange{Start: start, End: end}

		// 保存到数据库
		assignment := &model.SlotAssignment{
			TaskID:    taskID,
			WorkerID:  fmt.Sprintf("worker-%d", i),
			SlotStart: start,
			SlotEnd:   end,
			Status:    "assigned",
		}
		s.master.store.SaveSlotAssignment(assignment)
	}

	return ranges, nil
}

// SlotRange Slot范围
type SlotRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// WorkerPool Worker池
type WorkerPool struct {
	count      int
	socketPath string
	workers    []*WorkerProcess
	mu         sync.Mutex
}

// WorkerProcess Worker进程
type WorkerProcess struct {
	ID      string
	Slots   SlotRange
	Process *os.Process
	Status  string
}

// NewWorkerPool 创建Worker池
func NewWorkerPool(count int, socketPath string) *WorkerPool {
	return &WorkerPool{
		count:      count,
		socketPath: socketPath,
		workers:    make([]*WorkerProcess, 0, count),
	}
}

// Start 启动Worker池
func (p *WorkerPool) Start() error {
	// 在内嵌模式下，Worker作为goroutine运行，不需要启动外部进程
	// 实际部署时可以改为启动子进程
	for i := 0; i < p.count; i++ {
		worker := &WorkerProcess{
			ID:     fmt.Sprintf("worker-%d", i),
			Status: "ready",
		}
		p.workers = append(p.workers, worker)
	}
	return nil
}

// Stop 停止Worker池
func (p *WorkerPool) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, w := range p.workers {
		if w.Process != nil {
			w.Process.Kill()
		}
	}
	p.workers = nil
}

// SpawnWorker 启动Worker进程
func (p *WorkerPool) SpawnWorker(id string, slots SlotRange) (*WorkerProcess, error) {
	// 实际部署时启动子进程
	cmd := exec.Command(WorkerBinary,
		"-id", id,
		"-socket", p.socketPath,
		"-slot-start", fmt.Sprintf("%d", slots.Start),
		"-slot-end", fmt.Sprintf("%d", slots.End),
	)

	if err := cmd.Start(); err != nil {
		// 如果无法启动子进程，使用内嵌Worker
		log.Printf("Cannot start worker process, using embedded worker")
		return &WorkerProcess{
			ID:     id,
			Slots:  slots,
			Status: "embedded",
		}, nil
	}

	return &WorkerProcess{
		ID:      id,
		Slots:   slots,
		Process: cmd.Process,
		Status:  "running",
	}, nil
}

// GetWorker 获取Worker
func (p *WorkerPool) GetWorker(id string) *WorkerProcess {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, w := range p.workers {
		if w.ID == id {
			return w
		}
	}
	return nil
}

// GetAvailableWorker 获取可用Worker
func (p *WorkerPool) GetAvailableWorker() *WorkerProcess {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, w := range p.workers {
		if w.Status == "ready" {
			return w
		}
	}
	return nil
}
