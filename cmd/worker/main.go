package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"tendis-migrate/internal/ipc"
	"tendis-migrate/internal/storage"
	"tendis-migrate/internal/worker"
	"tendis-migrate/pkg/logger"
)

var (
	taskID         = flag.String("task-id", "", "Task ID")
	workerID       = flag.Int("worker-id", 0, "Worker ID")
	slots          = flag.String("slots", "", "Assigned slots (comma-separated)")
	masterSocket   = flag.String("master-socket", "/tmp/tendis-migrate-master.sock", "Master IPC socket")
	sourceCluster  = flag.String("source-cluster", "", "Source cluster addresses")
	targetCluster  = flag.String("target-cluster", "", "Target cluster addresses")
	sourcePassword = flag.String("source-password", "", "Source password")
	targetPassword = flag.String("target-password", "", "Target password")
	options        = flag.String("options", "", "Task options (JSON)")
	dbPath         = flag.String("db", "./data/tasks.db", "SQLite database path")
)

func main() {
	flag.Parse()

	log.Printf("=== Tendis Migrate V2.0 Worker %d ===\n", *workerID)
	log.Printf("Task: %s, Slots: %s\n", *taskID, *slots)

	// 参数校验
	if *taskID == "" || *sourceCluster == "" || *targetCluster == "" {
		log.Fatal("Missing required parameters")
	}

	// 解析分配的 Slot
	assignedSlots, err := parseSlots(*slots)
	if err != nil {
		log.Fatal("Parse slots failed:", err)
	}

	log.Printf("Assigned %d slots\n", len(assignedSlots))

	// 解析任务选项
	var taskOptions TaskOptions
	if *options != "" {
		if err := json.Unmarshal([]byte(*options), &taskOptions); err != nil {
			log.Fatal("Parse options failed:", err)
		}
	} else {
		// 默认配置
		taskOptions = TaskOptions{
			WorkerThreads:  4,
			ScanBatchSize:  1000,
			ConflictPolicy: "skip_full_only",
			TargetQPSLimit: 10000,
		}
	}

	// 创建日志记录器
	taskLogger := logger.WithTask(*taskID)
	taskLogger.Info("Worker starting", map[string]interface{}{
		"worker_id":      *workerID,
		"assigned_slots": len(assignedSlots),
		"options":        taskOptions,
	})

	// 连接 SQLite 数据库
	db, err := storage.NewSQLiteDB(*dbPath)
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// 创建 Redis 客户端
	sourceClient, err := createRedisClient(*sourceCluster, *sourcePassword)
	if err != nil {
		log.Fatal("Failed to create source client:", err)
	}
	defer sourceClient.Close()

	targetClient, err := createRedisClient(*targetCluster, *targetPassword)
	if err != nil {
		log.Fatal("Failed to create target client:", err)
	}
	defer targetClient.Close()

	// 测试连接
	ctx := context.Background()
	if err := sourceClient.Ping(ctx).Err(); err != nil {
		log.Fatal("Source cluster ping failed:", err)
	}
	if err := targetClient.Ping(ctx).Err(); err != nil {
		log.Fatal("Target cluster ping failed:", err)
	}

	log.Println("✓ Redis clients connected")

	// 创建 IPC 客户端
	ipcClient := ipc.NewClient(*masterSocket, handleIPCMessage(taskLogger))
	if err := ipcClient.Connect(); err != nil {
		log.Fatal("Failed to connect to master:", err)
	}
	defer ipcClient.Close()

	log.Println("✓ Connected to master")

	// 发送 Worker Ready 消息
	readyMsg, _ := ipc.NewIPCMessage(ipc.MsgTypeWorkerReady, &ipc.MsgWorkerReady{
		WorkerID: *workerID,
		TaskID:   *taskID,
		PID:      os.Getpid(),
		Version:  "v2.0.0",
	})
	if err := ipcClient.Send(readyMsg); err != nil {
		log.Fatal("Failed to send ready message:", err)
	}

	// 启动心跳 goroutine
	go sendHeartbeat(ipcClient, *taskID, *workerID)

	// 创建 Slot 迁移器
	migrator := worker.NewSlotMigrator(
		*taskID,
		*workerID,
		assignedSlots,
		sourceClient,
		targetClient,
		db,
		ipcClient,
		taskLogger,
		taskOptions.ScanBatchSize,
		taskOptions.ConflictPolicy,
	)

	// 启动迁移
	log.Println("✓ Starting slot migration...")
	
	go func() {
		if err := migrator.MigrateAllSlots(); err != nil {
			log.Printf("Migration failed: %v\n", err)
			os.Exit(1)
		}
		log.Println("✓ All slots migrated successfully")
		os.Exit(0)
	}()

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("\nShutting down gracefully...")
}

// TaskOptions 任务配置选项
type TaskOptions struct {
	WorkerThreads  int    `json:"worker_threads"`
	ScanBatchSize  int    `json:"scan_batch_size"`
	ConflictPolicy string `json:"conflict_policy"`
	TargetQPSLimit int    `json:"target_qps_limit"`
}

// handleIPCMessage 处理 IPC 消息
func handleIPCMessage(logger *logger.TaskLogger) ipc.MessageHandler {
	return func(msg *ipc.IPCMessage, conn interface{}) error {
		switch msg.Type {
		case ipc.MsgTypeShutdown:
			var shutdown ipc.MsgShutdown
			if err := msg.DecodePayload(&shutdown); err == nil {
				logger.Info("Received shutdown command", map[string]interface{}{
					"reason":   shutdown.Reason,
					"graceful": shutdown.Graceful,
				})
				
				if shutdown.Graceful {
					// 等待当前 Slot 完成
					time.Sleep(2 * time.Second)
				}
				
				os.Exit(0)
			}
		case ipc.MsgTypePause:
			logger.Info("Received pause command", nil)
			// TODO: 实现暂停逻辑
		case ipc.MsgTypeResume:
			logger.Info("Received resume command", nil)
			// TODO: 实现恢复逻辑
		}
		return nil
	}
}

// sendHeartbeat 发送心跳
func sendHeartbeat(client *ipc.Client, taskID string, workerID int) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// TODO: 从迁移器获取实际统计数据
		msg, _ := ipc.NewIPCMessage(ipc.MsgTypeHeartbeat, &ipc.MsgHeartbeat{
			WorkerID:       workerID,
			TaskID:         taskID,
			KeysMigrated:   0,
			BytesMigrated:  0,
			MemoryUsageMB:  getMemoryUsage(),
			GoroutineCount: 10,
			CurrentSlot:    -1,
		})

		if err := client.Send(msg); err != nil {
			log.Printf("Failed to send heartbeat: %v\n", err)
		}
	}
}

// getMemoryUsage 获取内存使用量（MB）
func getMemoryUsage() int64 {
	// 简化实现，实际应该读取 /proc/self/status 或使用 runtime.ReadMemStats
	return 100
}

// parseSlots 解析 Slot 列表
func parseSlots(slotsStr string) ([]int, error) {
	if slotsStr == "" {
		return nil, fmt.Errorf("empty slots string")
	}

	result := []int{}
	for _, part := range strings.Split(slotsStr, ",") {
		slot, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("invalid slot: %s", part)
		}
		result = append(result, slot)
	}

	return result, nil
}

// createRedisClient 创建 Redis 客户端
func createRedisClient(cluster string, password string) (redis.UniversalClient, error) {
	addrs := strings.Split(cluster, ",")
	if len(addrs) == 0 {
		return nil, fmt.Errorf("invalid cluster config: %s", cluster)
	}

	// 清理地址
	for i, addr := range addrs {
		addrs[i] = strings.TrimSpace(addr)
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
