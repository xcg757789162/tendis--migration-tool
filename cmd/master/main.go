package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"tendis-migrate/internal/ipc"
	"tendis-migrate/internal/master"
	"tendis-migrate/internal/storage"
	"tendis-migrate/pkg/logger"
)

var (
	port         = flag.Int("port", 8088, "Web server port")
	workers      = flag.Int("workers", 8, "Number of worker processes")
	workerBinary = flag.String("worker-binary", "./tendis-migrate-worker", "Worker binary path")
	dbPath       = flag.String("db", "./data/tasks.db", "SQLite database path")
	socketPath   = flag.String("socket", "/tmp/tendis-migrate-master.sock", "IPC socket path")
)

func main() {
	flag.Parse()

	log.Println("=== Tendis Migrate V2.0 Master Process ===")
	log.Printf("Workers: %d, Port: %d, Socket: %s\n", *workers, *port, *socketPath)

	// 创建数据目录
	os.MkdirAll("./data", 0755)
	os.MkdirAll("./logs", 0755)

	// 初始化 SQLite 数据库
	db, err := storage.NewSQLiteDB(*dbPath)
	if err != nil {
		log.Fatal("Failed to initialize database:", err)
	}
	defer db.Close()

	log.Println("✓ SQLite database initialized")

	// 创建 IPC 服务器
	ipcServer := ipc.NewServer(*socketPath, handleIPCMessage(db))
	if err := ipcServer.Start(); err != nil {
		log.Fatal("Failed to start IPC server:", err)
	}
	defer ipcServer.Stop()

	log.Printf("✓ IPC server listening on %s\n", *socketPath)

	// TODO: 启动 Web API 服务器
	// go startWebServer(*port, db)

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	log.Println("✓ Master process ready")
	log.Println("\nWaiting for tasks... (Press Ctrl+C to exit)")

	<-sigCh
	log.Println("\nShutting down...")
}

// handleIPCMessage 处理 IPC 消息
func handleIPCMessage(db *storage.SQLiteDB) ipc.MessageHandler {
	return func(msg *ipc.IPCMessage, conn interface{}) error {
		switch msg.Type {
		case ipc.MsgTypeWorkerReady:
			return handleWorkerReady(msg, db)
		case ipc.MsgTypeHeartbeat:
			return handleHeartbeat(msg, db)
		case ipc.MsgTypeCheckpoint:
			return handleCheckpoint(msg, db)
		case ipc.MsgTypeSlotCompleted:
			return handleSlotCompleted(msg, db)
		case ipc.MsgTypeSlotFailed:
			return handleSlotFailed(msg, db)
		default:
			log.Printf("Unknown message type: %s\n", msg.Type)
		}
		return nil
	}
}

// handleWorkerReady 处理 Worker 就绪消息
func handleWorkerReady(msg *ipc.IPCMessage, db *storage.SQLiteDB) error {
	var ready ipc.MsgWorkerReady
	if err := msg.DecodePayload(&ready); err != nil {
		return err
	}

	log.Printf("[Worker %d] Ready (PID: %d, Version: %s)\n", ready.WorkerID, ready.PID, ready.Version)

	// 更新数据库
	db.UpdateWorkerStatus(ready.TaskID, ready.WorkerID, "running")

	return nil
}

// handleHeartbeat 处理 Worker 心跳消息
func handleHeartbeat(msg *ipc.IPCMessage, db *storage.SQLiteDB) error {
	var hb ipc.MsgHeartbeat
	if err := msg.DecodePayload(&hb); err != nil {
		return err
	}

	// 更新 Worker 心跳
	db.UpdateWorkerHeartbeat(hb.TaskID, hb.WorkerID, hb.KeysMigrated, hb.BytesMigrated, hb.CurrentSlot)

	// 聚合任务进度
	updateTaskProgress(db, hb.TaskID)

	return nil
}

// handleCheckpoint 处理 Worker 断点消息
func handleCheckpoint(msg *ipc.IPCMessage, db *storage.SQLiteDB) error {
	var cp ipc.MsgCheckpoint
	if err := msg.DecodePayload(&cp); err != nil {
		return err
	}

	log.Printf("[Worker %d] Checkpoint: Slot %d, Cursor: %s, Keys: %d\n",
		cp.WorkerID, cp.Slot, cp.Cursor, cp.KeysMigrated)

	// 更新 Slot 断点
	db.UpdateSlotCheckpoint(cp.TaskID, cp.Slot, cp.Cursor, cp.KeysMigrated)

	return nil
}

// handleSlotCompleted 处理 Slot 完成消息
func handleSlotCompleted(msg *ipc.IPCMessage, db *storage.SQLiteDB) error {
	var sc ipc.MsgSlotCompleted
	if err := msg.DecodePayload(&sc); err != nil {
		return err
	}

	log.Printf("[Worker %d] Slot %d completed (Keys: %d, Bytes: %d, Duration: %dms)\n",
		sc.WorkerID, sc.Slot, sc.KeysMigrated, sc.BytesMigrated, sc.Duration)

	// 标记 Slot 完成
	db.UpdateSlotStatus(sc.TaskID, sc.Slot, "completed")

	// 检查是否所有 Slot 都完成
	checkTaskCompletion(db, sc.TaskID)

	return nil
}

// handleSlotFailed 处理 Slot 失败消息
func handleSlotFailed(msg *ipc.IPCMessage, db *storage.SQLiteDB) error {
	var sf ipc.MsgSlotFailed
	if err := msg.DecodePayload(&sf); err != nil {
		return err
	}

	log.Printf("[Worker %d] Slot %d FAILED: %s\n", sf.WorkerID, sf.Slot, sf.Error)

	// 标记 Slot 失败
	db.UpdateSlotStatus(sf.TaskID, sf.Slot, "failed")

	return nil
}

// updateTaskProgress 更新任务进度
func updateTaskProgress(db *storage.SQLiteDB, taskID string) error {
	// 聚合所有 Worker 的进度
	workers, err := db.ListWorkers(taskID)
	if err != nil {
		return err
	}

	var totalKeys, totalBytes int64
	for _, worker := range workers {
		totalKeys += worker.KeysMigrated
		totalBytes += worker.BytesMigrated
	}

	// 更新任务统计
	updates := map[string]interface{}{
		"keys_migrated":  totalKeys,
		"bytes_migrated": totalBytes,
	}

	return db.UpdateTask(taskID, updates)
}

// checkTaskCompletion 检查任务是否全部完成
func checkTaskCompletion(db *storage.SQLiteDB, taskID string) {
	completed, _ := db.CountSlotsByStatus(taskID, "completed")
	if completed == 16384 {
		log.Printf("[Task %s] All slots completed! (16384/16384)\n", taskID)

		// 更新任务状态
		db.UpdateTask(taskID, map[string]interface{}{
			"status":       "completed",
			"phase":        "completed",
			"completed_at": time.Now().Format(time.RFC3339),
		})

		// TODO: 如果是 full_and_incremental 模式，启动增量同步
	}
}

// startTask 启动任务（API 调用）
func startTask(taskID string, db *storage.SQLiteDB, ipcServer *ipc.Server) error {
	// 获取任务信息
	task, err := db.GetTask(taskID)
	if err != nil {
		return fmt.Errorf("get task failed: %w", err)
	}

	// 创建 Logger
	taskLogger := logger.WithTask(taskID)

	// 创建 Slot 管理器
	slotManager := master.NewSlotManager(db, taskID, task.NumWorkers)
	if err := slotManager.InitialAssignment(); err != nil {
		return fmt.Errorf("slot assignment failed: %w", err)
	}

	taskLogger.Info("Slot assignment completed", map[string]interface{}{
		"num_workers": task.NumWorkers,
		"total_slots": 16384,
	})

	// 创建 Worker 池管理器
	workerPool := master.NewWorkerPoolManager(
		taskID,
		task.NumWorkers,
		*workerBinary,
		*socketPath,
		db,
		slotManager,
		ipcServer,
		taskLogger,
	)

	// 启动所有 Worker
	if err := workerPool.StartAllWorkers(task); err != nil {
		return fmt.Errorf("start workers failed: %w", err)
	}

	// 更新任务状态
	db.UpdateTask(taskID, map[string]interface{}{
		"status":        "running",
		"phase":         "full",
		"full_start_at": time.Now().Format(time.RFC3339),
	})

	return nil
}

// createRedisClient 创建 Redis 客户端
func createRedisClient(cluster string, password string) (redis.UniversalClient, error) {
	addrs := []string{}
	for _, addr := range splitCluster(cluster) {
		addrs = append(addrs, addr)
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("invalid cluster config: %s", cluster)
	}

	// 检测是否是 Cluster 模式
	if len(addrs) > 1 || isClusterMode(addrs[0], password) {
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    addrs,
			Password: password,
		}), nil
	}

	// 单机模式
	return redis.NewClient(&redis.Options{
		Addr:     addrs[0],
		Password: password,
	}), nil
}

// isClusterMode 检测是否是 Cluster 模式
func isClusterMode(addr string, password string) bool {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 尝试执行 CLUSTER INFO
	_, err := client.ClusterInfo(ctx).Result()
	return err == nil
}

// splitCluster 分割集群地址
func splitCluster(cluster string) []string {
	addrs := []string{}
	for _, addr := range splitByComma(cluster) {
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func splitByComma(s string) []string {
	result := []string{}
	for _, part := range split(s, ',') {
		result = append(result, trim(part))
	}
	return result
}

func split(s string, sep rune) []string {
	result := []string{}
	start := 0
	for i, c := range s {
		if c == sep {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	result = append(result, s[start:])
	return result
}

func trim(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
