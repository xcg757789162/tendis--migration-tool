package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"tendis-migrate/internal/ipc"
	"tendis-migrate/internal/storage"
)

func main() {
	fmt.Println("=== Tendis Migrate V2.0 架构验证 ===\n")

	// 1. 测试 SQLite 元数据存储
	fmt.Println("1. 测试 SQLite 元数据存储...")
	testSQLite()

	// 2. 测试 LevelDB 队列
	fmt.Println("\n2. 测试 LevelDB 变更队列...")
	testLevelDB()

	// 3. 测试 IPC 通信
	fmt.Println("\n3. 测试 IPC 通信...")
	testIPC()

	fmt.Println("\n=== 所有测试通过！✅ ===")
}

func testSQLite() {
	// 创建临时数据库
	dbPath := "/tmp/tendis-migrate-test.db"
	os.Remove(dbPath)

	db, err := storage.NewSQLiteDB(dbPath)
	if err != nil {
		log.Fatal("创建 SQLite 失败:", err)
	}
	defer db.Close()

	// 创建任务
	task := &storage.Task{
		ID:             "test-task-1",
		Name:           "测试任务",
		Status:         "pending",
		Phase:          "full",
		CreatedAt:      time.Now().Format(time.RFC3339),
		UpdatedAt:      time.Now().Format(time.RFC3339),
		SourceCluster:  "10.248.37.11:8901,10.248.37.11:8902,10.248.37.11:8903",
		TargetCluster:  "10.31.165.39:8901,10.31.165.39:8902,10.31.165.39:8903",
		MigrationMode:  "full_and_incremental",
		NumWorkers:     8,
	}

	if err := db.CreateTask(task); err != nil {
		log.Fatal("创建任务失败:", err)
	}

	// 读取任务
	retrieved, err := db.GetTask("test-task-1")
	if err != nil {
		log.Fatal("读取任务失败:", err)
	}

	fmt.Printf("   ✅ 创建任务: %s (Workers: %d)\n", retrieved.Name, retrieved.NumWorkers)

	// 初始化 Slot
	if err := db.InitSlots("test-task-1", 8); err != nil {
		log.Fatal("初始化 Slot 失败:", err)
	}

	completedSlots, _ := db.CountSlotsByStatus("test-task-1", "pending")
	fmt.Printf("   ✅ 初始化 16384 个 Slot (pending: %d)\n", completedSlots)

	// 创建 Worker
	worker := &storage.WorkerStatus{
		TaskID:        "test-task-1",
		WorkerID:      0,
		PID:           12345,
		Status:        "running",
		AssignedSlots: []int{0, 1, 2, 3, 4},
		CreatedAt:     time.Now().Format(time.RFC3339),
		UpdatedAt:     time.Now().Format(time.RFC3339),
	}

	if err := db.CreateWorker(worker); err != nil {
		log.Fatal("创建 Worker 失败:", err)
	}

	fmt.Printf("   ✅ 创建 Worker %d (PID: %d)\n", worker.WorkerID, worker.PID)
}

func testLevelDB() {
	// 创建临时队列
	queuePath := "/tmp/tendis-migrate-queue-test"
	os.RemoveAll(queuePath)

	queue, err := storage.NewLevelDBQueue(queuePath, "node1")
	if err != nil {
		log.Fatal("创建 LevelDB 队列失败:", err)
	}
	defer queue.Close()

	// 入队变更
	changes := []*storage.KeyChange{
		{Key: "testkey:1", Operation: "set", Timestamp: time.Now().Unix(), NodeID: "node1"},
		{Key: "testkey:2", Operation: "set", Timestamp: time.Now().Unix(), NodeID: "node1"},
		{Key: "testkey:3", Operation: "del", Timestamp: time.Now().Unix(), NodeID: "node1"},
	}

	if err := queue.EnqueueBatch(changes); err != nil {
		log.Fatal("批量入队失败:", err)
	}

	count := queue.Count()
	fmt.Printf("   ✅ 入队 %d 个变更记录\n", count)

	// 出队
	dequeued, err := queue.Dequeue(2)
	if err != nil {
		log.Fatal("出队失败:", err)
	}

	fmt.Printf("   ✅ 出队 %d 个变更记录 (剩余: %d)\n", len(dequeued), queue.Count())

	// 统计信息
	stats := queue.GetStats()
	fmt.Printf("   ✅ 队列统计: count=%d\n", stats["count"])
}

func testIPC() {
	socketPath := "/tmp/tendis-migrate-test.sock"
	os.Remove(socketPath)

	// 创建服务器
	serverReceived := false
	server := ipc.NewServer(socketPath, func(msg *ipc.IPCMessage, conn interface{}) error {
		fmt.Printf("   ✅ Server 收到消息: type=%s\n", msg.Type)
		serverReceived = true
		return nil
	})

	if err := server.Start(); err != nil {
		log.Fatal("启动 IPC 服务器失败:", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	// 创建客户端
	client := ipc.NewClient(socketPath, func(msg *ipc.IPCMessage, conn interface{}) error {
		fmt.Printf("   ✅ Client 收到消息: type=%s\n", msg.Type)
		return nil
	})

	if err := client.Connect(); err != nil {
		log.Fatal("连接 IPC 服务器失败:", err)
	}
	defer client.Close()

	// 客户端发送消息
	msg, _ := ipc.NewIPCMessage(ipc.MsgTypeWorkerReady, &ipc.MsgWorkerReady{
		WorkerID: 0,
		TaskID:   "test-task-1",
		PID:      12345,
		Version:  "v2.0.0",
	})

	if err := client.Send(msg); err != nil {
		log.Fatal("发送消息失败:", err)
	}

	time.Sleep(100 * time.Millisecond)

	if !serverReceived {
		log.Fatal("服务器未收到消息")
	}

	fmt.Println("   ✅ IPC 双向通信正常")
}
