package ipc

import (
	"encoding/json"
	"time"
)

// 消息类型常量
const (
	// Worker -> Master
	MsgTypeWorkerReady    = "worker_ready"
	MsgTypeHeartbeat      = "heartbeat"
	MsgTypeCheckpoint     = "checkpoint"
	MsgTypeSlotCompleted  = "slot_completed"
	MsgTypeSlotFailed     = "slot_failed"
	MsgTypeWorkerError    = "worker_error"

	// Master -> Worker
	MsgTypeStartFull        = "start_full"
	MsgTypeStartIncremental = "start_incremental"
	MsgTypeShutdown         = "shutdown"
	MsgTypePause            = "pause"
	MsgTypeResume           = "resume"
)

// IPCMessage 基础消息结构
type IPCMessage struct {
	Type      string          `json:"type"`
	Timestamp int64           `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// NewIPCMessage 创建新消息
func NewIPCMessage(msgType string, payload interface{}) (*IPCMessage, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &IPCMessage{
		Type:      msgType,
		Timestamp: time.Now().Unix(),
		Payload:   data,
	}, nil
}

// DecodePayload 解码消息载荷
func (m *IPCMessage) DecodePayload(v interface{}) error {
	return json.Unmarshal(m.Payload, v)
}

// ========== Worker -> Master 消息 ==========

// MsgWorkerReady Worker 启动就绪消息
type MsgWorkerReady struct {
	WorkerID int    `json:"worker_id"`
	TaskID   string `json:"task_id"`
	PID      int    `json:"pid"`
	Version  string `json:"version"`
}

// MsgHeartbeat Worker 心跳消息（每 5 秒）
type MsgHeartbeat struct {
	WorkerID       int   `json:"worker_id"`
	TaskID         string `json:"task_id"`
	KeysMigrated   int64 `json:"keys_migrated"`
	BytesMigrated  int64 `json:"bytes_migrated"`
	MemoryUsageMB  int64 `json:"memory_usage_mb"`
	GoroutineCount int   `json:"goroutine_count"`
	CurrentSlot    int   `json:"current_slot"` // 当前正在迁移的 Slot
}

// MsgCheckpoint Worker 断点消息（每完成 1000 个 key）
type MsgCheckpoint struct {
	WorkerID     int    `json:"worker_id"`
	TaskID       string `json:"task_id"`
	Slot         int    `json:"slot"`
	Cursor       string `json:"cursor"`         // SCAN 游标
	KeysMigrated int64  `json:"keys_migrated"`  // 该 Slot 已迁移 key 数
	UpdatedAt    string `json:"updated_at"`
}

// MsgSlotCompleted Slot 完成消息
type MsgSlotCompleted struct {
	WorkerID     int    `json:"worker_id"`
	TaskID       string `json:"task_id"`
	Slot         int    `json:"slot"`
	KeysMigrated int64  `json:"keys_migrated"`
	BytesMigrated int64 `json:"bytes_migrated"`
	Duration     int64  `json:"duration_ms"` // 耗时（毫秒）
}

// MsgSlotFailed Slot 失败消息
type MsgSlotFailed struct {
	WorkerID int    `json:"worker_id"`
	TaskID   string `json:"task_id"`
	Slot     int    `json:"slot"`
	Error    string `json:"error"`
}

// MsgWorkerError Worker 错误消息
type MsgWorkerError struct {
	WorkerID int    `json:"worker_id"`
	TaskID   string `json:"task_id"`
	Error    string `json:"error"`
	Fatal    bool   `json:"fatal"` // 是否致命错误（需要重启 Worker）
}

// ========== Master -> Worker 消息 ==========

// MsgStartFull 启动全量迁移消息
type MsgStartFull struct {
	TaskID         string   `json:"task_id"`
	SourceCluster  string   `json:"source_cluster"`
	TargetCluster  string   `json:"target_cluster"`
	SourcePassword string   `json:"source_password"`
	TargetPassword string   `json:"target_password"`
	AssignedSlots  []int    `json:"assigned_slots"` // 分配的 Slot 列表
	Options        *TaskOptions `json:"options"`
}

// TaskOptions 任务配置选项
type TaskOptions struct {
	WorkerThreads     int    `json:"worker_threads"`      // Worker 内部并发线程数
	ScanBatchSize     int    `json:"scan_batch_size"`     // SCAN 批次大小
	ConflictPolicy    string `json:"conflict_policy"`     // 冲突策略: skip_full_only, replace, error
	KeyFilterMode     string `json:"key_filter_mode"`     // 过滤模式: all, prefix, pattern, keys
	KeyFilterValue    string `json:"key_filter_value"`    // 过滤值
	TargetQPSLimit    int    `json:"target_qps_limit"`    // 目标端 QPS 限制
}

// MsgStartIncremental 启动增量同步消息
type MsgStartIncremental struct {
	TaskID string `json:"task_id"`
	NodeQueues map[string]string `json:"node_queues"` // node_id -> leveldb_path
}

// MsgShutdown 关闭 Worker 消息
type MsgShutdown struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason"`
	Graceful bool `json:"graceful"` // 是否优雅关闭
}

// MsgPause 暂停迁移消息
type MsgPause struct {
	TaskID string `json:"task_id"`
}

// MsgResume 恢复迁移消息
type MsgResume struct {
	TaskID string `json:"task_id"`
}
