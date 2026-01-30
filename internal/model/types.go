package model

import (
	"time"
)

// TaskStatus 任务状态
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusPaused    TaskStatus = "paused"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
)

// MigrationPhase 迁移阶段
type MigrationPhase int32

const (
	PhaseFullMigration    MigrationPhase = 1
	PhaseIncrementalSync  MigrationPhase = 2
	PhaseVerification     MigrationPhase = 3
)

func (p MigrationPhase) String() string {
	switch p {
	case PhaseFullMigration:
		return "full"
	case PhaseIncrementalSync:
		return "incremental"
	case PhaseVerification:
		return "verify"
	default:
		return "unknown"
	}
}

// ConflictPolicy 冲突策略
type ConflictPolicy string

const (
	ConflictPolicySkipFullOnly ConflictPolicy = "skip_full_only" // 全量跳过，增量replace
	ConflictPolicyReplace      ConflictPolicy = "replace"        // 直接覆盖
	ConflictPolicyError        ConflictPolicy = "error"          // 报错
	ConflictPolicySkip         ConflictPolicy = "skip"           // 跳过并记录，后续单独处理
)

// KeyFilterMode Key过滤模式
type KeyFilterMode string

const (
	KeyFilterModeAll     KeyFilterMode = "all"
	KeyFilterModePrefix  KeyFilterMode = "prefix"
	KeyFilterModePattern KeyFilterMode = "pattern"
	KeyFilterModeKeys    KeyFilterMode = "keys"
)

// Task 迁移任务
type Task struct {
	ID            string     `json:"id" db:"id"`
	Name          string     `json:"name" db:"name"`
	SourceCluster string     `json:"source_cluster" db:"source_cluster"` // JSON
	TargetCluster string     `json:"target_cluster" db:"target_cluster"` // JSON
	Status        TaskStatus `json:"status" db:"status"`
	Config        string     `json:"config" db:"config"` // JSON
	CreatedAt     int64      `json:"created_at" db:"created_at"`
	UpdatedAt     int64      `json:"updated_at" db:"updated_at"`
	StartedAt     *int64     `json:"started_at,omitempty" db:"started_at"`
	CompletedAt   *int64     `json:"completed_at,omitempty" db:"completed_at"`
}

// ClusterConfig 集群配置
type ClusterConfig struct {
	Addrs    []string `json:"addrs"`
	Password string   `json:"password,omitempty"`
}

// MigrationOptions 迁移选项
type MigrationOptions struct {
	WorkerCount       int               `json:"worker_count"`
	ScanBatchSize     int               `json:"scan_batch_size"`
	EnableCompression bool              `json:"enable_compression"`
	LargeKeyThreshold int64             `json:"large_key_threshold"`
	KeyFilter         *KeyFilterConfig  `json:"key_filter,omitempty"`
	ConflictPolicy    ConflictPolicy    `json:"conflict_policy"`
	RateLimit         *RateLimitConfig  `json:"rate_limit,omitempty"`
	SkipFullSync      bool              `json:"skip_full_sync"`      // 跳过全量同步阶段
	SkipIncremental   bool              `json:"skip_incremental"`    // 跳过增量同步阶段
}

// KeyFilterConfig Key过滤配置
type KeyFilterConfig struct {
	Mode            KeyFilterMode `json:"mode"`
	Prefixes        []string      `json:"prefixes,omitempty"`
	Patterns        []string      `json:"patterns,omitempty"`
	Keys            []string      `json:"keys,omitempty"`
	KeysFile        string        `json:"keys_file,omitempty"`
	ExcludePrefixes []string      `json:"exclude_prefixes,omitempty"`
	ExcludePatterns []string      `json:"exclude_patterns,omitempty"`
}

// RateLimitConfig 限速配置
type RateLimitConfig struct {
	SourceQPS         int `json:"source_qps"`
	SourceConnections int `json:"source_connections"`
	TargetQPS         int `json:"target_qps"`
	TargetConnections int `json:"target_connections"`
	PipelineSize      int `json:"pipeline_size"`
	PipelineTimeout   int `json:"pipeline_timeout_ms"`
	MaxBandwidthMbps  int `json:"max_bandwidth_mbps"`
}

// SlotAssignment Slot分配
type SlotAssignment struct {
	TaskID     string `json:"task_id" db:"task_id"`
	WorkerID   string `json:"worker_id" db:"worker_id"`
	SlotStart  int    `json:"slot_start" db:"slot_start"`
	SlotEnd    int    `json:"slot_end" db:"slot_end"`
	Status     string `json:"status" db:"status"`
	AssignedAt int64  `json:"assigned_at" db:"assigned_at"`
}

// Checkpoint 断点
type Checkpoint struct {
	TaskID       string `json:"task_id" db:"task_id"`
	WorkerID     string `json:"worker_id" db:"worker_id"`
	SlotID       int    `json:"slot_id" db:"slot_id"`
	Cursor       string `json:"cursor" db:"cursor"`
	KeysMigrated int64  `json:"keys_migrated" db:"keys_migrated"`
	BytesMigrated int64 `json:"bytes_migrated" db:"bytes_migrated"`
	LastKey      string `json:"last_key,omitempty" db:"last_key"`
	UpdatedAt    int64  `json:"updated_at" db:"updated_at"`
}

// MigrationStats 迁移统计
type MigrationStats struct {
	TaskID           string  `json:"task_id" db:"task_id"`
	TotalKeys        int64   `json:"total_keys" db:"total_keys"`
	MigratedKeys     int64   `json:"migrated_keys" db:"migrated_keys"`
	TotalBytes       int64   `json:"total_bytes" db:"total_bytes"`
	MigratedBytes    int64   `json:"migrated_bytes" db:"migrated_bytes"`
	LargeKeysCount   int64   `json:"large_keys_count" db:"large_keys_count"`
	SkippedKeysCount int64   `json:"skipped_keys_count" db:"skipped_keys_count"`
	ErrorCount       int64   `json:"error_count" db:"error_count"`
	StartTime        *int64  `json:"start_time,omitempty" db:"start_time"`
	EndTime          *int64  `json:"end_time,omitempty" db:"end_time"`
}

// Progress 进度
type Progress struct {
	Phase         MigrationPhase `json:"phase"`
	TotalKeys     int64          `json:"total_keys"`
	MigratedKeys  int64          `json:"migrated_keys"`
	TotalBytes    int64          `json:"total_bytes"`
	MigratedBytes int64          `json:"migrated_bytes"`
	Percentage    float64        `json:"percentage"`
	EstimatedETA  string         `json:"estimated_eta"`
	CurrentSpeed  int64          `json:"current_speed"` // keys/s
}

// TaskResponse API响应
type TaskResponse struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Status    TaskStatus `json:"status"`
	Progress  *Progress  `json:"progress,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	StartedAt *time.Time `json:"started_at,omitempty"`
}

// VerifyResult 校验结果
type VerifyResult struct {
	ID              int64     `json:"id" db:"id"`
	TaskID          string    `json:"task_id" db:"task_id"`
	BatchID         string    `json:"batch_id" db:"batch_id"`
	TotalKeys       int       `json:"total_keys" db:"total_keys"`
	MatchedKeys     int       `json:"matched_keys" db:"matched_keys"`
	MismatchedKeys  int       `json:"mismatched_keys" db:"mismatched_keys"`
	MissingKeys     int       `json:"missing_keys" db:"missing_keys"`
	ExtraKeys       int       `json:"extra_keys" db:"extra_keys"`
	ConsistencyRate float64   `json:"consistency_rate"`
	Details         string    `json:"details,omitempty" db:"details"` // JSON
	CreatedAt       int64     `json:"created_at" db:"created_at"`
}

// LargeKeyProgress 大Key进度
type LargeKeyProgress struct {
	TaskID           string `json:"task_id" db:"task_id"`
	KeyName          string `json:"key_name" db:"key_name"`
	KeyType          string `json:"key_type" db:"key_type"`
	TotalBytes       int64  `json:"total_bytes" db:"total_bytes"`
	TransferredBytes int64  `json:"transferred_bytes" db:"transferred_bytes"`
	Checksum         string `json:"checksum,omitempty" db:"checksum"`
	Status           string `json:"status" db:"status"`
	UpdatedAt        int64  `json:"updated_at" db:"updated_at"`
}

// ChangeEvent 变更事件
type ChangeEvent struct {
	NodeID    string `json:"node_id"`
	Key       string `json:"key"`
	Operation string `json:"op"`     // set/del/expire/rename
	Timestamp int64  `json:"ts"`
	Offset    int64  `json:"offset"`
}

// WorkerStats Worker统计
type WorkerStats struct {
	WorkerID         string  `json:"worker_id"`
	KeysProcessed    int64   `json:"keys_processed"`
	BytesTransferred int64   `json:"bytes_transferred"`
	ErrorCount       int64   `json:"error_count"`
	CurrentSpeed     float64 `json:"current_speed"`
	LastActiveAt     int64   `json:"last_active_at"`
}

// DefaultMigrationOptions 默认迁移选项
func DefaultMigrationOptions() *MigrationOptions {
	return &MigrationOptions{
		WorkerCount:       4,
		ScanBatchSize:     1000,
		EnableCompression: true,
		LargeKeyThreshold: 10 * 1024 * 1024, // 10MB
		ConflictPolicy:    ConflictPolicySkipFullOnly,
		RateLimit: &RateLimitConfig{
			SourceQPS:         10000,
			SourceConnections: 50,
			TargetQPS:         10000,
			TargetConnections: 50,
			PipelineSize:      100,
			PipelineTimeout:   5000,
			MaxBandwidthMbps:  0, // 不限制
		},
	}
}
