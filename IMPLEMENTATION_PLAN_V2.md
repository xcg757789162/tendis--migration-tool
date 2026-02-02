# Tendis 迁移工具 V2.0 完整实施计划

> **目标**：实现原始设计（V1.4），支持 40 亿 key 生产环境  
> **当前版本**：V1.4-simplified（简化版，支持 <1 亿 key）  
> **目标版本**：V2.0-enterprise（企业版，支持 ≤20 亿 key/任务）

---

## 一、当前状态评估

### 1.1 已完成（V1.4-simplified）✅

| 功能模块 | 状态 | 说明 |
|---------|------|------|
| 单任务全量迁移 | ✅ | 支持 Cluster 模式，使用 DUMP/RESTORE |
| 智能增量同步 | ✅ | 基于值变化检测（2秒轮询） |
| 过滤器 | ✅ | 支持 prefix/pattern/keys 三种模式 |
| 冲突策略 | ✅ | 支持 skip_full_only/replace/error |
| Web UI | ✅ | Vue3 前端，实时监控 |
| REST API | ✅ | 完整的任务管理接口 |
| 动态 Worker Pool | ✅ | 自动调整并发度（1-256 goroutines） |
| 背压控制 | ✅ | 目标端 QPS 限速 |

### 1.2 缺失的核心架构（原设计 V1.4）❌

| 架构组件 | 原设计 | 当前实现 | 影响 |
|---------|-------|---------|------|
| **进程架构** | Master + 多个 Worker 进程 | 单进程（多 goroutine） | 无法并行处理 Slot ❌ |
| **任务分片** | Slot 静态分配（16384 个 Slot） | 无分片（全局 SCAN） | 无法并发迁移 ❌ |
| **变更队列** | LevelDB 持久化（500MB/节点） | 内存 map | 无法承受大规模变更 ❌ |
| **元数据存储** | SQLite（WAL 模式） | 内存（丢失后重建） | 无持久化状态 ❌ |
| **IPC 通信** | Unix Socket（Master ↔ Worker） | 无（单进程） | - |
| **断点恢复** | Slot 级别（粒度细） | 任务级别（全局） | 重启代价大 ❌ |

### 1.3 关键数据对比

| 场景 | 原设计能力 | 当前实现能力 | 差距 |
|------|-----------|-------------|------|
| Key 数量上限 | 20 亿/任务 | 1 亿/任务 | **20 倍** ❌ |
| 全量迁移时间（40亿 key） | 22 小时 | 预估 88 小时 | **4 倍** ❌ |
| 增量变更队列容量 | 500 MB/节点（LevelDB） | 无上限（内存） | **OOM 风险** ❌ |
| 智能轮询覆盖率 | 无需轮询（Keyspace Notifications） | 11 小时/轮（40 亿 key） | **无法实时** ❌ |

---

## 二、架构升级方案

### 2.1 Master-Worker 多进程架构

#### 架构图
```
┌─────────────────────────────────────────────────────────────────┐
│                        迁移工具部署机                            │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Master Process                        │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐   │   │
│  │  │ Web/API  │  │ Scheduler│  │ Keyspace Listeners   │   │   │
│  │  │ Server   │  │          │  │ (per-node goroutine) │   │   │
│  │  └─────┬────┘  └────┬─────┘  └─────────┬────────────┘   │   │
│  │        │            │                   │                │   │
│  │        │      ┌─────▼─────┐      ┌──────▼─────┐         │   │
│  │        │      │  SQLite   │      │  LevelDB   │         │   │
│  │        │      │ (Metadata)│      │  (Queues)  │         │   │
│  │        │      └───────────┘      └────────────┘         │   │
│  └────────┼──────────────────────────────────────────────┬─┘   │
│           │                   Unix Socket IPC            │     │
│  ┌────────▼─────────┐   ┌──────────────┐   ┌────────────▼──┐  │
│  │   Worker 0       │   │   Worker 1   │   │   Worker N    │  │
│  │ Slots: 0-4095    │   │ Slots: 4096- │   │ Slots: ...    │  │
│  └──────────────────┘   └──────────────┘   └───────────────┘  │
└─────────────────────────────────────────────────────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
   ┌─────────┐            ┌─────────┐            ┌─────────┐
   │ Tendis  │            │ Tendis  │            │ Tendis  │
   │ Source  │            │ Source  │            │ Source  │
   │ Cluster │            │ Cluster │            │ Cluster │
   └─────────┘            └─────────┘            └─────────┘
        │                       │                       │
        └───────────────────────┴───────────────────────┘
                                │
                                ▼
                        ┌───────────────┐
                        │    Tendis     │
                        │    Target     │
                        │    Cluster    │
                        └───────────────┘
```

#### 进程职责划分

##### Master 进程
```go
// Master 核心职责
type MasterProcess struct {
    // 1. 任务调度
    Scheduler *TaskScheduler
    
    // 2. Slot 分配管理
    SlotManager *SlotManager
    
    // 3. Worker 生命周期管理
    WorkerPool *WorkerPoolManager
    
    // 4. Keyspace Notifications 监听（写入 LevelDB 队列）
    KeyspaceListeners map[string]*KeyspaceListener // node_id -> listener
    
    // 5. Web API 服务
    HTTPServer *gin.Engine
    
    // 6. IPC 服务器（接收 Worker 消息）
    IPCServer *UnixSocketServer
    
    // 7. 元数据持久化
    MetadataDB *SQLiteDB
    
    // 8. 变更队列（每个源节点一个）
    ChangeQueues map[string]*LevelDBQueue // node_id -> queue
}

// Master 主要流程
func (m *MasterProcess) Run() {
    // 1. 启动 Web 服务器
    go m.HTTPServer.Run(":8088")
    
    // 2. 启动 IPC 服务器（接收 Worker 消息）
    go m.IPCServer.Listen("/tmp/tendis-migrate-master.sock")
    
    // 3. 加载任务状态（如果是恢复启动）
    m.MetadataDB.LoadTasks()
    
    // 4. 启动任务调度器
    m.Scheduler.Start()
    
    // 5. 等待信号
    <-m.stopCh
}
```

##### Worker 进程
```go
// Worker 核心职责
type WorkerProcess struct {
    // 1. 身份信息
    WorkerID   int
    TaskID     string
    
    // 2. 分配的 Slot 区间
    AssignedSlots []int  // 例如: [0, 1, 2, ..., 4095]
    
    // 3. IPC 客户端（与 Master 通信）
    IPCClient *UnixSocketClient
    
    // 4. 迁移引擎
    MigrationEngine *MigrationEngine
    
    // 5. 断点状态（内存）
    Checkpoint *WorkerCheckpoint
}

// Worker 主要流程
func (w *WorkerProcess) Run() {
    // 1. 连接 Master
    w.IPCClient.Connect("/tmp/tendis-migrate-master.sock")
    
    // 2. 发送注册消息
    w.IPCClient.Send(MsgWorkerReady{
        WorkerID: w.WorkerID,
        TaskID:   w.TaskID,
    })
    
    // 3. 执行全量迁移（按分配的 Slot）
    for _, slot := range w.AssignedSlots {
        w.MigrationEngine.MigrateSlot(slot)
        
        // 上报断点（每完成一个 Slot）
        w.IPCClient.Send(MsgCheckpoint{
            WorkerID: w.WorkerID,
            Slot:     slot,
            Status:   "completed",
        })
    }
    
    // 4. 全量完成后，等待 Master 指令
    // （Master 协调所有 Worker 完成后，统一启动增量同步）
    w.WaitForIncrementalPhase()
    
    // 5. 从 LevelDB 队列消费增量变更
    w.ConsumeIncrementalChanges()
}
```

### 2.2 Slot 分片并行迁移

#### Slot 分配策略
```go
// Slot 静态分配（启动时一次性分配）
func (s *SlotManager) AssignSlots(numWorkers int) map[int][]int {
    totalSlots := 16384
    slotsPerWorker := totalSlots / numWorkers
    
    assignments := make(map[int][]int)
    for i := 0; i < numWorkers; i++ {
        start := i * slotsPerWorker
        end := start + slotsPerWorker
        if i == numWorkers-1 {
            end = totalSlots // 最后一个 Worker 处理剩余 Slot
        }
        
        for slot := start; slot < end; slot++ {
            assignments[i] = append(assignments[i], slot)
        }
    }
    
    return assignments
}

// 示例：8 个 Worker
// Worker 0: slots 0-2047
// Worker 1: slots 2048-4095
// Worker 2: slots 4096-6143
// ...
// Worker 7: slots 14336-16383
```

#### Worker 迁移逻辑（按 Slot）
```go
func (w *WorkerProcess) MigrateSlot(slot int) error {
    // 1. 获取该 Slot 对应的主节点
    nodeID := w.GetNodeForSlot(slot)
    sourceClient := w.GetSourceClient(nodeID)
    
    // 2. 使用 SCAN 扫描该 Slot 的所有 key
    cursor := "0"
    for {
        keys, newCursor, err := sourceClient.Scan(ctx, cursor, 
            fmt.Sprintf("*{%d}*", slot), // Cluster Slot Tag
            1000).Result()
        
        // 3. 批量迁移 keys
        for _, key := range keys {
            w.MigrationEngine.MigrateKey(key, sourceClient, targetClient)
        }
        
        cursor = newCursor
        if cursor == "0" {
            break
        }
        
        // 4. 定期上报进度
        if time.Since(lastReport) > 5*time.Second {
            w.ReportProgress(slot, len(keys))
        }
    }
    
    // 5. 标记 Slot 完成
    w.MetadataDB.MarkSlotCompleted(w.TaskID, slot)
    
    return nil
}
```

### 2.3 LevelDB 变更队列

#### 数据结构
```go
// LevelDB 队列设计（每个源节点一个实例）
type LevelDBQueue struct {
    db       *leveldb.DB
    nodeID   string
    basePath string // 例如: ./data/queue/node_10.248.37.11_8901/
}

// 队列键格式：timestamp_sequenceID
// 例如: "1735891200123_0000001" -> {"key": "testkey:123", "op": "set"}

// 写入（Master 进程，Keyspace Listener goroutine）
func (q *LevelDBQueue) Enqueue(change *KeyChange) error {
    key := fmt.Sprintf("%d_%07d", time.Now().UnixMilli(), q.nextSeqID())
    value, _ := json.Marshal(change)
    return q.db.Put([]byte(key), value, nil)
}

// 读取（Worker 进程，增量同步阶段）
func (q *LevelDBQueue) Dequeue(batchSize int) ([]*KeyChange, error) {
    iter := q.db.NewIterator(nil, nil)
    defer iter.Release()
    
    changes := make([]*KeyChange, 0, batchSize)
    for iter.Next() && len(changes) < batchSize {
        var change KeyChange
        json.Unmarshal(iter.Value(), &change)
        changes = append(changes, &change)
        
        // 删除已消费的记录
        q.db.Delete(iter.Key(), nil)
    }
    
    return changes, iter.Error()
}
```

#### 容量规划
```yaml
leveldb_queue_capacity:
  # 原设计估算（40亿key场景）
  write_qps_per_node: 10000  # 每个节点写入 QPS
  full_sync_duration: 55h    # 全量同步耗时
  total_changes: 1.98M       # 10k * 3600 * 55 = 198万变更
  storage_per_change: 100B   # key名(50B) + 元数据(50B)
  total_storage: 198MB       # 198万 * 100B ≈ 200MB
  
  # 保守估算（3倍冗余）
  recommended_storage: 500MB/node
  
  # 8节点集群
  total_storage_8_nodes: 4GB  # 500MB * 8
```

### 2.4 SQLite 元数据持久化

#### 表结构设计
```sql
-- 1. 任务表（主表）
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,  -- running, paused, completed, failed
    phase TEXT NOT NULL,   -- full, incremental, completed
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    -- 迁移配置
    source_cluster TEXT NOT NULL,
    target_cluster TEXT NOT NULL,
    migration_mode TEXT NOT NULL,  -- full_only, full_and_incremental
    num_workers INTEGER NOT NULL,
    -- 统计信息
    keys_total INTEGER DEFAULT 0,
    keys_migrated INTEGER DEFAULT 0,
    keys_failed INTEGER DEFAULT 0,
    bytes_migrated INTEGER DEFAULT 0
);

-- 2. Slot 状态表（核心：断点恢复）
CREATE TABLE slot_status (
    task_id TEXT NOT NULL,
    slot INTEGER NOT NULL,
    worker_id INTEGER NOT NULL,      -- 分配的 Worker ID
    status TEXT NOT NULL,             -- pending, migrating, completed, failed
    keys_total INTEGER DEFAULT 0,
    keys_migrated INTEGER DEFAULT 0,
    last_cursor TEXT,                 -- SCAN 游标（断点）
    updated_at TEXT NOT NULL,
    PRIMARY KEY (task_id, slot),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX idx_slot_status_task_status ON slot_status(task_id, status);

-- 3. Worker 状态表
CREATE TABLE worker_status (
    task_id TEXT NOT NULL,
    worker_id INTEGER NOT NULL,
    pid INTEGER,                      -- 进程 PID
    status TEXT NOT NULL,             -- idle, running, completed, crashed
    assigned_slots TEXT,              -- JSON数组: [0, 1, 2, ...]
    keys_migrated INTEGER DEFAULT 0,
    bytes_migrated INTEGER DEFAULT 0,
    last_heartbeat TEXT,
    PRIMARY KEY (task_id, worker_id),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);

-- 4. LevelDB 队列元数据（每个源节点一个记录）
CREATE TABLE queue_metadata (
    task_id TEXT NOT NULL,
    node_id TEXT NOT NULL,            -- 源节点标识: "10.248.37.11:8901"
    queue_path TEXT NOT NULL,         -- LevelDB 路径
    enqueued_count INTEGER DEFAULT 0, -- 累计入队数量
    dequeued_count INTEGER DEFAULT 0, -- 累计消费数量
    pending_count INTEGER DEFAULT 0,  -- 当前积压数量
    last_enqueue_time TEXT,
    last_dequeue_time TEXT,
    PRIMARY KEY (task_id, node_id),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);

-- 5. 进度快照表（用于 Web UI 图表）
CREATE TABLE progress_snapshots (
    task_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    phase TEXT NOT NULL,
    keys_migrated INTEGER NOT NULL,
    bytes_migrated INTEGER NOT NULL,
    speed INTEGER NOT NULL,  -- keys/s
    PRIMARY KEY (task_id, timestamp),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX idx_progress_snapshots_task_time ON progress_snapshots(task_id, timestamp);
```

#### WAL 模式配置
```go
// SQLite WAL 模式配置（支持并发读写）
func OpenSQLiteDB(dbPath string) (*sql.DB, error) {
    db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared&mode=rwc&_journal_mode=WAL", dbPath))
    if err != nil {
        return nil, err
    }
    
    // WAL 性能优化
    db.Exec("PRAGMA synchronous = NORMAL")    // 降低同步频率
    db.Exec("PRAGMA cache_size = -64000")     // 64MB 缓存
    db.Exec("PRAGMA temp_store = MEMORY")     // 临时表内存存储
    
    return db, nil
}
```

### 2.5 IPC 通信机制

#### Unix Socket 消息格式
```go
// 消息类型枚举
const (
    MsgTypeWorkerReady      = "worker_ready"       // Worker -> Master
    MsgTypeHeartbeat        = "heartbeat"          // Worker -> Master
    MsgTypeCheckpoint       = "checkpoint"         // Worker -> Master
    MsgTypeStartIncremental = "start_incremental"  // Master -> Worker
    MsgTypeShutdown         = "shutdown"           // Master -> Worker
)

// 消息基类
type IPCMessage struct {
    Type      string          `json:"type"`
    Timestamp int64           `json:"timestamp"`
    Payload   json.RawMessage `json:"payload"`
}

// Worker 注册消息
type MsgWorkerReady struct {
    WorkerID int    `json:"worker_id"`
    TaskID   string `json:"task_id"`
    PID      int    `json:"pid"`
}

// Worker 心跳消息（每 5 秒）
type MsgHeartbeat struct {
    WorkerID      int   `json:"worker_id"`
    TaskID        string `json:"task_id"`
    KeysMigrated  int64  `json:"keys_migrated"`
    BytesMigrated int64  `json:"bytes_migrated"`
    MemoryUsageMB int64  `json:"memory_usage_mb"`
    GoroutineCount int   `json:"goroutine_count"`
}

// Worker 断点消息（每完成一个 Slot）
type MsgCheckpoint struct {
    WorkerID      int    `json:"worker_id"`
    TaskID        string `json:"task_id"`
    Slot          int    `json:"slot"`
    Status        string `json:"status"` // completed, failed
    KeysMigrated  int64  `json:"keys_migrated"`
    LastCursor    string `json:"last_cursor"`
}

// Master 启动增量同步指令
type MsgStartIncremental struct {
    TaskID string `json:"task_id"`
}
```

#### 通信实现
```go
// Master 端 IPC 服务器
type UnixSocketServer struct {
    listener net.Listener
    handler  func(*IPCMessage, net.Conn)
}

func (s *UnixSocketServer) Listen(socketPath string) error {
    os.Remove(socketPath) // 清理旧 socket
    
    ln, err := net.Listen("unix", socketPath)
    if err != nil {
        return err
    }
    s.listener = ln
    
    for {
        conn, err := ln.Accept()
        if err != nil {
            continue
        }
        
        go s.handleConnection(conn)
    }
}

func (s *UnixSocketServer) handleConnection(conn net.Conn) {
    defer conn.Close()
    
    // 长度前缀协议: [4字节长度][JSON消息]
    for {
        var msgLen uint32
        if err := binary.Read(conn, binary.BigEndian, &msgLen); err != nil {
            return
        }
        
        msgData := make([]byte, msgLen)
        if _, err := io.ReadFull(conn, msgData); err != nil {
            return
        }
        
        var msg IPCMessage
        if err := json.Unmarshal(msgData, &msg); err != nil {
            continue
        }
        
        s.handler(&msg, conn)
    }
}

// Worker 端 IPC 客户端
type UnixSocketClient struct {
    conn net.Conn
    mu   sync.Mutex
}

func (c *UnixSocketClient) Send(msg interface{}) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    ipcMsg := IPCMessage{
        Type:      getMessageType(msg),
        Timestamp: time.Now().Unix(),
    }
    ipcMsg.Payload, _ = json.Marshal(msg)
    
    msgData, _ := json.Marshal(ipcMsg)
    msgLen := uint32(len(msgData))
    
    // 写入长度前缀
    if err := binary.Write(c.conn, binary.BigEndian, msgLen); err != nil {
        return err
    }
    
    // 写入消息体
    _, err := c.conn.Write(msgData)
    return err
}
```

---

## 三、实施计划

### 3.1 分阶段实施路线

#### Phase 1: 基础架构重构（Week 1-2）

**目标**：搭建 Master-Worker 框架骨架

| 任务 | 工作量 | 关键产出 |
|------|-------|---------|
| 1.1 项目结构重组 | 1 天 | 分离 Master/Worker 代码目录 |
| 1.2 IPC 通信框架 | 2 天 | Unix Socket 服务器/客户端 |
| 1.3 Worker 进程启动管理 | 1 天 | `os/exec` fork Worker |
| 1.4 SQLite 元数据层 | 2 天 | 5 张表 CRUD 接口 |
| 1.5 LevelDB 队列封装 | 2 天 | 入队/出队/持久化 |
| 1.6 单元测试 | 2 天 | IPC/SQLite/LevelDB 测试 |

**验收标准**：
- ✅ Master 能启动并 fork 8 个 Worker 进程
- ✅ Worker 能通过 Unix Socket 向 Master 发送心跳
- ✅ SQLite 能正确记录任务和 Slot 状态
- ✅ LevelDB 能读写变更队列

---

#### Phase 2: Slot 分片迁移（Week 3-4）

**目标**：实现 Slot 级别的全量迁移

| 任务 | 工作量 | 关键产出 |
|------|-------|---------|
| 2.1 Slot 分配算法 | 1 天 | 静态分配 16384 个 Slot 给 N 个 Worker |
| 2.2 Worker 迁移引擎重构 | 3 天 | 按 Slot 扫描和迁移逻辑 |
| 2.3 断点恢复机制 | 2 天 | Slot 级别断点保存和恢复 |
| 2.4 Master 进度聚合 | 1 天 | 汇总所有 Worker 的进度 |
| 2.5 集成测试 | 3 天 | 8 Worker 并行迁移 1 亿 key |

**验收标准**：
- ✅ 8 个 Worker 能并行迁移不同的 Slot
- ✅ 单个 Worker 崩溃后，Master 能将该 Slot 重新分配
- ✅ Master 重启后，能从断点恢复所有 Worker

---

#### Phase 3: 增量同步升级（Week 5-6）

**目标**：实现 LevelDB 队列 + Keyspace Notifications

| 任务 | 工作量 | 关键产出 |
|------|-------|---------|
| 3.1 Keyspace Notifications 监听 | 2 天 | Master 进程监听所有源节点 |
| 3.2 变更写入 LevelDB | 1 天 | 监听到的变更写入对应节点队列 |
| 3.3 Worker 增量消费 | 2 天 | 从 LevelDB 批量消费并迁移 |
| 3.4 全量-增量协调 | 2 天 | 全量完成后切换到增量阶段 |
| 3.5 收敛算法 | 2 天 | 时间窗口检查 + 趋势分析 |
| 3.6 降级逻辑 | 1 天 | Keyspace 不支持时回退到智能轮询 |

**验收标准**：
- ✅ 全量期间，变更能写入 LevelDB（不丢失）
- ✅ 全量完成后，Worker 能从队列消费增量变更
- ✅ 增量同步能正确收敛（队列为空且无新变更）

---

#### Phase 4: 性能优化（Week 7）

**目标**：优化到 ≥50k keys/s

| 任务 | 工作量 | 关键产出 |
|------|-------|---------|
| 4.1 Worker 动态调整 | 1 天 | 根据负载自动增减 Worker 数量 |
| 4.2 批量操作优化 | 2 天 | Pipeline DUMP/RESTORE |
| 4.3 LevelDB 批量写入 | 1 天 | WriteBatch 优化 |
| 4.4 性能基准测试 | 2 天 | 40 亿 key 压力测试 |
| 4.5 资源监控 | 1 天 | CPU/内存/网络监控 |

**验收标准**：
- ✅ 迁移速度 ≥50k keys/s（8 Worker, 1Gbps 网络）
- ✅ Master 进程内存 <2GB
- ✅ 单 Worker 内存 <512MB

---

#### Phase 5: 测试与交付（Week 8）

**目标**：全面测试和文档

| 任务 | 工作量 | 关键产出 |
|------|-------|---------|
| 5.1 功能测试 | 2 天 | 40 亿 key 端到端测试 |
| 5.2 异常测试 | 2 天 | 网络中断/进程崩溃/磁盘满 |
| 5.3 部署文档 | 1 天 | README + 安装指南 |
| 5.4 运维文档 | 1 天 | 故障排查 + 监控指标 |
| 5.5 性能报告 | 1 天 | 基准测试数据 + 优化建议 |

**验收标准**：
- ✅ 40 亿 key 迁移成功（<24 小时）
- ✅ 故障恢复测试通过（Master/Worker 崩溃）
- ✅ 文档齐全（用户手册 + 运维手册）

---

### 3.2 项目结构重组

#### 目录结构（V2.0）
```
tendis-migrate/
├── cmd/
│   ├── master/
│   │   └── main.go              # Master 进程入口
│   ├── worker/
│   │   └── main.go              # Worker 进程入口
│   └── simple/                  # 保留 V1.4-simplified 版本
│       └── main.go
├── internal/
│   ├── master/
│   │   ├── scheduler.go         # 任务调度器
│   │   ├── slot_manager.go      # Slot 分配管理
│   │   ├── worker_pool.go       # Worker 生命周期管理
│   │   ├── ipc_server.go        # IPC 服务器
│   │   └── keyspace_listener.go # Keyspace Notifications 监听
│   ├── worker/
│   │   ├── engine.go            # 迁移引擎
│   │   ├── slot_migrator.go     # Slot 迁移逻辑
│   │   ├── ipc_client.go        # IPC 客户端
│   │   └── incremental.go       # 增量同步消费
│   ├── storage/
│   │   ├── sqlite.go            # SQLite 封装
│   │   └── leveldb.go           # LevelDB 封装
│   ├── ipc/
│   │   ├── protocol.go          # 消息协议定义
│   │   └── codec.go             # 编解码
│   └── common/
│       ├── redis_client.go      # Redis 客户端封装
│       ├── migration.go         # 通用迁移逻辑
│       └── filter.go            # Key 过滤器
├── pkg/
│   └── logger/
│       └── logger.go
├── web/                         # 保持不变
├── data/
│   ├── tasks.db                 # SQLite 数据库
│   └── queues/                  # LevelDB 队列目录
│       ├── node_10.248.37.11_8901/
│       ├── node_10.248.37.11_8902/
│       └── ...
├── run-master.sh                # 启动 Master 脚本
├── run-worker.sh                # 手动启动 Worker 脚本（调试用）
└── README.md
```

---

## 四、关键技术细节

### 4.1 Master 进程启动 Worker

```go
// Master 启动 Worker 进程
func (m *MasterProcess) StartWorker(taskID string, workerID int, slots []int) error {
    // 1. 构建 Worker 命令行参数
    cmd := exec.Command(
        "./tendis-migrate-worker",
        "--task-id", taskID,
        "--worker-id", strconv.Itoa(workerID),
        "--slots", strings.Join(intSliceToStrings(slots), ","),
        "--master-socket", "/tmp/tendis-migrate-master.sock",
    )
    
    // 2. 设置环境变量
    cmd.Env = append(os.Environ(),
        fmt.Sprintf("WORKER_ID=%d", workerID),
        fmt.Sprintf("TASK_ID=%s", taskID),
    )
    
    // 3. 重定向日志
    logFile, _ := os.Create(fmt.Sprintf("./logs/worker-%d.log", workerID))
    cmd.Stdout = logFile
    cmd.Stderr = logFile
    
    // 4. 启动进程
    if err := cmd.Start(); err != nil {
        return err
    }
    
    // 5. 记录 PID
    m.MetadataDB.UpdateWorkerStatus(taskID, workerID, map[string]interface{}{
        "pid":    cmd.Process.Pid,
        "status": "running",
    })
    
    // 6. 监控进程退出
    go func() {
        cmd.Wait()
        m.OnWorkerExit(taskID, workerID, cmd.ProcessState.ExitCode())
    }()
    
    return nil
}
```

### 4.2 Worker 断点恢复

```go
// Worker 启动时加载断点
func (w *WorkerProcess) LoadCheckpoint() error {
    // 1. 从 SQLite 加载分配的 Slot
    rows, err := w.db.Query(`
        SELECT slot, status, last_cursor, keys_migrated
        FROM slot_status
        WHERE task_id = ? AND worker_id = ?
        ORDER BY slot
    `, w.TaskID, w.WorkerID)
    
    defer rows.Close()
    
    // 2. 恢复断点
    for rows.Next() {
        var slot int
        var status, lastCursor string
        var keysMigrated int64
        
        rows.Scan(&slot, &status, &lastCursor, &keysMigrated)
        
        if status != "completed" {
            // 恢复未完成的 Slot
            w.Checkpoint.PendingSlots[slot] = &SlotCheckpoint{
                Cursor:       lastCursor,
                KeysMigrated: keysMigrated,
            }
        }
    }
    
    return nil
}

// 迁移时保存断点（每 1000 个 key）
func (w *WorkerProcess) MigrateSlotWithCheckpoint(slot int) error {
    checkpoint := w.Checkpoint.PendingSlots[slot]
    cursor := checkpoint.Cursor
    
    for {
        keys, newCursor, _ := w.SourceClient.Scan(ctx, cursor, "*", 1000).Result()
        
        // 迁移 keys...
        
        cursor = newCursor
        checkpoint.KeysMigrated += int64(len(keys))
        
        // 每 1000 个 key 保存一次断点
        if checkpoint.KeysMigrated%1000 == 0 {
            w.db.Exec(`
                UPDATE slot_status
                SET last_cursor = ?, keys_migrated = ?, updated_at = ?
                WHERE task_id = ? AND worker_id = ? AND slot = ?
            `, cursor, checkpoint.KeysMigrated, time.Now().Format(time.RFC3339),
               w.TaskID, w.WorkerID, slot)
        }
        
        if cursor == "0" {
            break
        }
    }
    
    // 标记完成
    w.db.Exec(`
        UPDATE slot_status
        SET status = 'completed', updated_at = ?
        WHERE task_id = ? AND worker_id = ? AND slot = ?
    `, time.Now().Format(time.RFC3339), w.TaskID, w.WorkerID, slot)
    
    return nil
}
```

### 4.3 全量-增量协调

```go
// Master 协调全量完成后启动增量
func (m *MasterProcess) CoordinateIncrementalPhase(taskID string) {
    // 1. 等待所有 Worker 报告全量完成
    for {
        completedWorkers := m.MetadataDB.CountWorkers(taskID, "status = 'full_completed'")
        totalWorkers := m.MetadataDB.CountWorkers(taskID, "")
        
        if completedWorkers == totalWorkers {
            break
        }
        
        time.Sleep(1 * time.Second)
    }
    
    // 2. 回放 LevelDB 队列缓存的变更
    m.ReplayBufferedChanges(taskID)
    
    // 3. 向所有 Worker 发送启动增量同步指令
    workers := m.MetadataDB.GetWorkers(taskID)
    for _, worker := range workers {
        conn := m.IPCServer.GetConnection(worker.ID)
        m.SendMessage(conn, &MsgStartIncremental{TaskID: taskID})
    }
    
    // 4. 更新任务阶段
    m.MetadataDB.UpdateTask(taskID, map[string]interface{}{
        "phase":        "incremental",
        "incr_start_at": time.Now().Format(time.RFC3339),
    })
}

// Master 回放缓冲的变更
func (m *MasterProcess) ReplayBufferedChanges(taskID string) {
    nodes := m.MetadataDB.GetSourceNodes(taskID)
    
    for _, node := range nodes {
        queue := m.ChangeQueues[node.ID]
        
        // 统计队列积压
        pendingCount := queue.Count()
        log.Infof("Replaying buffered changes for node %s: %d changes", node.ID, pendingCount)
        
        // 批量消费队列（由各个 Worker 消费）
        // 注意：回放时不删除队列记录，等 Worker 确认后再删除
    }
}
```

### 4.4 Keyspace Notifications 监听

```go
// Master 启动 Keyspace Listener
func (m *MasterProcess) StartKeyspaceListener(taskID string, nodeAddr string) error {
    // 1. 连接源节点
    client := redis.NewClient(&redis.Options{
        Addr: nodeAddr,
    })
    
    // 2. 启用 Keyspace Notifications
    client.ConfigSet(ctx, "notify-keyspace-events", "AKE")
    
    // 3. 订阅所有数据库的 keyspace 事件
    pubsub := client.PSubscribe(ctx, "__keyspace@*__:*")
    
    // 4. 启动监听 goroutine
    go func() {
        for msg := range pubsub.Channel() {
            // 解析事件
            key := extractKeyFromChannel(msg.Channel)
            operation := msg.Payload
            
            // 检查是否匹配过滤规则
            task := m.GetTask(taskID)
            if !matchKeyFilter(key, task.Options) {
                continue
            }
            
            // 写入 LevelDB 队列
            queue := m.ChangeQueues[getNodeID(nodeAddr)]
            queue.Enqueue(&KeyChange{
                Key:       key,
                Operation: operation,
                Timestamp: time.Now().Unix(),
            })
            
            // 更新队列元数据
            m.MetadataDB.IncrementQueueEnqueued(taskID, getNodeID(nodeAddr))
        }
    }()
    
    return nil
}

// 辅助函数：从 channel 名称提取 key
func extractKeyFromChannel(channel string) string {
    // __keyspace@0__:testkey:123 -> testkey:123
    parts := strings.SplitN(channel, ":", 2)
    if len(parts) == 2 {
        return parts[1]
    }
    return ""
}
```

---

## 五、风险评估与缓解

### 5.1 技术风险

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| Keyspace Notifications 不支持 | 中 | 高 | 保留智能轮询降级方案 |
| LevelDB 多进程访问冲突 | 低 | 高 | 仅 Master 写入，Worker 只读 |
| SQLite 并发写入瓶颈 | 中 | 中 | 启用 WAL 模式 + 批量写入 |
| Worker 进程 OOM | 中 | 中 | 限制单 Worker 内存 <512MB |
| Unix Socket 通信阻塞 | 低 | 中 | 设置读写超时 + 重连机制 |

### 5.2 进度风险

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| 架构重构工作量超预期 | 中 | 高 | 保留 V1.4-simplified 作为备份 |
| Slot 分片测试不充分 | 中 | 中 | 提前进行小规模集成测试 |
| 文档编写时间不足 | 高 | 低 | 边开发边写文档（Markdown） |

---

## 六、成功标准

### 6.1 功能目标
- ✅ **支持 40 亿 key 迁移**（单任务 ≤20 亿 key，可分批）
- ✅ **迁移速度 ≥50k keys/s**（8 Worker, 1Gbps 网络）
- ✅ **增量同步收敛时间 <10 分钟**
- ✅ **断点恢复粒度：Slot 级别**（16384 个断点）
- ✅ **故障恢复时间 <5 分钟**（Master/Worker 崩溃）

### 6.2 性能目标
| 指标 | 目标值 | 测试场景 |
|------|-------|---------|
| 全量迁移速度 | ≥50k keys/s | 40 亿 key, 8 Worker |
| 增量同步延迟 | <5s | 10k writes/s |
| Master 内存占用 | <2GB | 40 亿 key 任务 |
| Worker 内存占用 | <512MB | 单 Worker |
| SQLite 写入 TPS | ≥1000 | 断点更新 |
| LevelDB 队列吞吐 | ≥10k ops/s | 增量变更写入 |

### 6.3 稳定性目标
- ✅ **零数据丢失**：全量 + 增量同步后数据一致性 100%
- ✅ **断点续传**：任意时刻中断后，重启能从断点恢复
- ✅ **进程崩溃恢复**：Master/Worker 崩溃后自动重建
- ✅ **网络异常容忍**：支持源端/目标端临时不可达

---

## 七、下一步行动

### 立即开始（本周）
1. **创建 V2.0 分支**
   ```bash
   cd /Users/chenguoxie/CodeBuddy/tendis-migrate
   git checkout -b feature/v2.0-master-worker
   ```

2. **搭建项目骨架**
   ```bash
   mkdir -p cmd/master cmd/worker internal/{master,worker,storage,ipc}
   ```

3. **编写 IPC 通信 POC**（验证 Unix Socket 可行性）
   ```bash
   # 目标：Master 和 Worker 能通过 Unix Socket 发送心跳
   ```

### 需要确认的问题
1. **资源预算**
   - 开发时间：预计 8 周，是否可接受？
   - 服务器资源：需要一台测试服务器（16 核 32GB 内存）

2. **优先级**
   - 是否优先实现 Master-Worker 架构？
   - 还是先优化当前版本（如使用 Pipeline 提速）？

3. **兼容性**
   - V2.0 是否需要兼容 V1.4-simplified 的任务数据？

---

## 八、附录

### A. 原设计文档关键章节
- `/Users/chenguoxie/CodeBuddy/20260123094903/tendis-migration-tool-technical-design-v1.4.md`
  - 第 2 节：架构设计（Master-Worker）
  - 第 3 节：核心模块设计（Slot 分配、LevelDB 队列）
  - 第 4 节：数据结构（SQLite 表结构）
  - 第 9 节：资源规划（40 亿 key 估算）

### B. 参考资料
- LevelDB Go 库：https://github.com/syndtr/goleveldb
- SQLite WAL 模式：https://www.sqlite.org/wal.html
- Redis Keyspace Notifications：https://redis.io/docs/manual/keyspace-notifications/
- Go Unix Socket：https://golang.org/pkg/net/#Dial

---

**生成时间**: 2026-02-02  
**版本**: V2.0 Plan (Draft)  
**状态**: 待审核
