package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"tendis-migrate/internal/replication"
	"tendis-migrate/pkg/logger"
)

type Task struct {
	ID             string  `json:"id"`
	Name           string  `json:"name"`
	Status         string  `json:"status"`
	Progress       float64 `json:"progress"`
	SourceCluster  string  `json:"source_cluster"`
	TargetCluster  string  `json:"target_cluster"`
	SourcePassword string  `json:"-"`
	TargetPassword string  `json:"-"`
	MigrationMode  string  `json:"migration_mode"` // full_only, full_and_incremental
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
	FullStartAt    string  `json:"full_start_at,omitempty"`    // 全量迁移开始时间
	IncrStartAt    string  `json:"incr_start_at,omitempty"`    // 增量迁移开始时间
	StartedAt      string  `json:"started_at,omitempty"`       // 任务开始时间（用于计算已耗时间）
	CompletedAt    string  `json:"completed_at,omitempty"`     // 任务完成时间
	PausedAt       string  `json:"paused_at,omitempty"`        // 暂停时间
	PausedDuration int64   `json:"paused_duration,omitempty"`  // 累计暂停时长（秒）
	
	// ===== 全量阶段指标 =====
	KeysTotal      int64   `json:"keys_total"`       // 源端总Key数（DBSIZE，全量迁移时通过SCAN统计）
	KeysToMigrate  int64   `json:"keys_to_migrate"`  // 符合过滤条件的Key数（待迁移），边扫边统计
	KeysMigrated   int64   `json:"keys_migrated"`    // 全量阶段已迁移Key数
	KeysFailed     int64   `json:"keys_failed"`      // 全量阶段迁移失败Key数
	KeysSkipped    int64   `json:"keys_skipped"`     // 全量阶段冲突跳过Key数（目标端已存在）
	KeysFiltered   int64   `json:"keys_filtered"`    // 被排除Key数（不符合过滤条件，如前缀不匹配）
	BytesMigrated  int64   `json:"bytes_migrated"`   // 全量阶段已迁移字节数
	BytesTotal     int64   `json:"bytes_total"`      // 预估总字节数
	Speed          int64   `json:"speed"`            // 当前迁移速度（keys/s）
	Phase          string  `json:"phase"`            // 当前阶段: full(全量), incremental(增量), completed(完成)
	ActiveWorkers  int     `json:"active_workers,omitempty"`   // 当前活跃Worker数
	
	// ===== 增量阶段指标（Binlog/FakeSlave 模式）=====
	// 注意：增量阶段的指标与全量阶段独立计数，不累加
	IncrKeysSynced   int64  `json:"incr_keys_synced,omitempty"`   // 增量阶段已同步Key数（成功写入目标端）
	IncrKeysSkipped  int64  `json:"incr_keys_skipped,omitempty"`  // 增量阶段冲突跳过Key数（目标端已存在且策略为skip）
	IncrKeysFailed   int64  `json:"incr_keys_failed,omitempty"`   // 增量阶段同步失败Key数
	IncrKeysFiltered int64  `json:"incr_keys_filtered,omitempty"` // 增量阶段被过滤Key数（不符合前缀过滤条件）
	IncrBinlogPos    uint64 `json:"incr_binlog_pos,omitempty"`    // 当前Binlog位置（用于断点续传）
	IncrLagMs        int64  `json:"incr_lag_ms,omitempty"`        // 增量同步延迟（毫秒，从Binlog产生到应用完成）
	IncrHeartbeats   int64  `json:"incr_heartbeats,omitempty"`    // 收到的心跳数（监控连接健康）
	IncrReconnects   int64  `json:"incr_reconnects,omitempty"`    // 重连次数（监控连接稳定性）
	IncrSyncMode     string `json:"incr_sync_mode,omitempty"`     // 增量同步模式: binlog(推荐), time_window(备用)
	
	// 配置选项
	Options *TaskOptions `json:"options,omitempty"`
	// 内部字段（不序列化）
	workerPool   *DynamicWorkerPool `json:"-"`
	speedTracker *SpeedTracker      `json:"-"`  // 滑动窗口速度追踪器
	fakeSlaves   []*replication.FakeSlave `json:"-"` // Binlog 接收器（每个节点一个）
	cacheManager *replication.BinlogCacheManager `json:"-"` // Binlog 缓存管理器
}

// SpeedTracker 滑动窗口速度追踪器，用于计算实时速度
type SpeedTracker struct {
	mu           sync.Mutex
	samples      []speedSample
	windowSize   int           // 采样窗口大小
	maxSamples   int           // 最大保留采样数
}

type speedSample struct {
	timestamp time.Time
	count     int64
	bytes     int64
}

// NewSpeedTracker 创建速度追踪器
func NewSpeedTracker(windowSize int) *SpeedTracker {
	if windowSize <= 0 {
		windowSize = 10 // 默认10个采样点
	}
	return &SpeedTracker{
		samples:    make([]speedSample, 0, windowSize*2),
		windowSize: windowSize,
		maxSamples: windowSize * 2,
	}
}

// Record 记录一个采样点
func (st *SpeedTracker) Record(count, bytes int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	
	st.samples = append(st.samples, speedSample{
		timestamp: time.Now(),
		count:     count,
		bytes:     bytes,
	})
	
	// 清理过期采样（保留最近 maxSamples 个）
	if len(st.samples) > st.maxSamples {
		st.samples = st.samples[len(st.samples)-st.maxSamples:]
	}
}

// GetSpeed 获取实时速度（keys/s）
func (st *SpeedTracker) GetSpeed() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	
	if len(st.samples) < 2 {
		return 0
	}
	
	// 使用最近 windowSize 个样本计算速度
	startIdx := len(st.samples) - st.windowSize
	if startIdx < 0 {
		startIdx = 0
	}
	
	first := st.samples[startIdx]
	last := st.samples[len(st.samples)-1]
	
	elapsed := last.timestamp.Sub(first.timestamp).Seconds()
	if elapsed <= 0 {
		return 0
	}
	
	keysProcessed := last.count - first.count
	if keysProcessed < 0 {
		keysProcessed = 0
	}
	
	return int64(float64(keysProcessed) / elapsed)
}

// GetBytesSpeed 获取实时字节速度（bytes/s）
func (st *SpeedTracker) GetBytesSpeed() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	
	if len(st.samples) < 2 {
		return 0
	}
	
	startIdx := len(st.samples) - st.windowSize
	if startIdx < 0 {
		startIdx = 0
	}
	
	first := st.samples[startIdx]
	last := st.samples[len(st.samples)-1]
	
	elapsed := last.timestamp.Sub(first.timestamp).Seconds()
	if elapsed <= 0 {
		return 0
	}
	
	bytesTransferred := last.bytes - first.bytes
	if bytesTransferred < 0 {
		bytesTransferred = 0
	}
	
	return int64(float64(bytesTransferred) / elapsed)
}

// TaskTemplate 任务模板
type TaskTemplate struct {
	ID            string       `json:"id"`
	Name          string       `json:"name"`
	Description   string       `json:"description"`
	SourceCluster string       `json:"source_cluster"`
	TargetCluster string       `json:"target_cluster"`
	SourcePassword string      `json:"source_password,omitempty"`
	TargetPassword string      `json:"target_password,omitempty"`
	MigrationMode string       `json:"migration_mode"`
	Options       *TaskOptions `json:"options,omitempty"`
	CreatedAt     string       `json:"created_at"`
	UpdatedAt     string       `json:"updated_at"`
}

// TaskOptions 任务配置选项
type TaskOptions struct {
	WorkerCount          int                   `json:"worker_count"`
	ScanBatchSize        int                   `json:"scan_batch_size"`
	ConflictPolicy       string                `json:"conflict_policy"`       // skip, replace, error, skip_full_only
	LargeKeyThreshold    int64                 `json:"large_key_threshold"`   // 大 Key 阈值（字节），超过此值的 Key 会被记录，默认 10MB
	SkipFullSync         bool                  `json:"skip_full_sync"`        // 跳过全量同步阶段
	SkipIncremental      bool                  `json:"skip_incremental"`      // 跳过增量同步阶段
	ShadowMode           bool                  `json:"shadow_mode"`           // 影子模式：只读取源端数据，不写入目标端
	KeyFilter            *KeyFilter            `json:"key_filter,omitempty"`
	RateLimit            *RateLimit            `json:"rate_limit,omitempty"`
	RetryConfig          *RetryConfig          `json:"retry_config,omitempty"`
	FaultTolerance       *FaultToleranceConfig `json:"fault_tolerance,omitempty"`  // 问题5修复：容错配置
	SmartRetry           *SmartRetryConfig     `json:"smart_retry,omitempty"`      // 问题5修复：智能重试配置
	VerifyConfig         *VerifyConfig         `json:"verify_config,omitempty"`    // 数据校验配置
	KeyListFile          string                `json:"key_list_file,omitempty"`    // Key 清单文件路径（支持 CSV/JSON/TXT）
}

// VerifyConfig 数据校验配置
type VerifyConfig struct {
	Enabled    bool    `json:"enabled"`     // 是否启用采样校验
	SampleRate float64 `json:"sample_rate"` // 采样率，0.001 = 0.1%，默认 0.001
}

// ShadowModeStats 影子模式统计
type ShadowModeStats struct {
	KeysScanned      int64            `json:"keys_scanned"`       // 扫描的 Key 数量
	KeysMatched      int64            `json:"keys_matched"`       // 匹配过滤规则的 Key 数量
	KeysSkipped      int64            `json:"keys_skipped"`       // 被过滤跳过的 Key 数量
	BytesRead        int64            `json:"bytes_read"`         // 读取的总字节数
	LargeKeysFound   int64            `json:"large_keys_found"`   // 发现的大 Key 数量
	TypeDistribution map[string]int64 `json:"type_distribution"`  // 数据类型分布
	EstimatedTime    string           `json:"estimated_time"`     // 预估全量迁移耗时
	AvgKeySize       int64            `json:"avg_key_size"`       // 平均 Key 大小
}

// RetryConfig 重试配置
type RetryConfig struct {
	MaxRetries          int `json:"max_retries"`            // 最大重试次数，默认3
	FullRetryIntervalMs int `json:"full_retry_interval_ms"` // 全量迁移重试间隔基数(毫秒)，默认100
	IncrRetryIntervalMs int `json:"incr_retry_interval_ms"` // 增量同步重试间隔基数(毫秒)，默认1000
}

// FaultToleranceConfig 容错配置（问题5修复：现已集成到 TaskOptions）
type FaultToleranceConfig struct {
	AutoRetryFailedKeys   bool `json:"auto_retry_failed_keys"`    // 自动重试失败的 key，默认 true
	MaxKeyRetries         int  `json:"max_key_retries"`           // 每个 key 最大重试次数，默认 3
	RetryIntervalMs       int  `json:"retry_interval_ms"`         // 重试间隔(毫秒)，默认 1000
	EnableAutoResume      bool `json:"enable_auto_resume"`        // 启用自动恢复，默认 true
	BackupIntervalMinutes int  `json:"backup_interval_minutes"`   // 状态备份间隔(分钟)，默认 1
}

// SmartRetryConfig 智能重试配置
type SmartRetryConfig struct {
	EnableAutoRecovery      bool `json:"enable_auto_recovery"`       // 启用自动恢复（检测到集群恢复后自动继续任务）
	HealthCheckIntervalSec  int  `json:"health_check_interval_sec"`  // 健康检测间隔（秒），默认30
	EnablePeriodicRetry     bool `json:"enable_periodic_retry"`      // 启用定期重试失败 Key
	PeriodicRetryIntervalSec int `json:"periodic_retry_interval_sec"` // 定期重试间隔（秒），默认300
	PeriodicRetryBatchSize  int  `json:"periodic_retry_batch_size"`  // 每次定期重试的 Key 数量，默认100
	MaxAutoResumeAttempts   int  `json:"max_auto_resume_attempts"`   // 最大自动恢复尝试次数，默认10
}

// AutoRecoveryState 自动恢复状态
type AutoRecoveryState struct {
	TaskID             string    `json:"task_id"`
	PausedAt           time.Time `json:"paused_at"`
	PauseReason        string    `json:"pause_reason"`
	ResumeAttempts     int       `json:"resume_attempts"`
	LastResumeAttempt  time.Time `json:"last_resume_attempt"`
	LastHealthCheck    time.Time `json:"last_health_check"`
	SourceHealthy      bool      `json:"source_healthy"`
	TargetHealthy      bool      `json:"target_healthy"`
	AutoResumeEnabled  bool      `json:"auto_resume_enabled"`
}

// HealthStatus 健康状态
type HealthStatus struct {
	Status           string                 `json:"status"`
	SourceConnected  bool                   `json:"source_connected"`
	TargetConnected  bool                   `json:"target_connected"`
	ActiveWorkers    int                    `json:"active_workers"`
	TargetWorkers    int                    `json:"target_workers"`
	MemoryUsageMB    float64                `json:"memory_usage_mb"`
	Uptime           string                 `json:"uptime"`
	LastError        string                 `json:"last_error,omitempty"`
	Details          map[string]interface{} `json:"details,omitempty"`
}

// RateLimit 限速配置
type RateLimit struct {
	SourceQPS         int `json:"source_qps"`          // 源端QPS限制，0表示不限制
	TargetQPS         int `json:"target_qps"`          // 目标端QPS限制，0表示不限制
	SourceConnections int `json:"source_connections"`  // 源端连接数
	TargetConnections int `json:"target_connections"`  // 目标端连接数
}

// ClusterInfo 集群信息（用于推荐配置）
type ClusterInfo struct {
	Addrs            []string `json:"addrs"`
	IsCluster        bool     `json:"is_cluster"`
	MasterCount      int      `json:"master_count"`
	TotalKeys        int64    `json:"total_keys"`
	UsedMemory       int64    `json:"used_memory"`
	UsedMemoryHuman  string   `json:"used_memory_human"`
	MaxMemory        int64    `json:"max_memory"`
	MaxClients       int      `json:"max_clients"`
	ConnectedClients int      `json:"connected_clients"`
	InstantaneousOPS int64    `json:"instantaneous_ops"`
	Version          string   `json:"version"`
	AvgKeySize       int64    `json:"avg_key_size"`       // 估算的平均key大小
	LargeKeyCount    int64    `json:"large_key_count"`    // 大key数量估算
}

// RecommendedConfig 推荐配置
type RecommendedConfig struct {
	WorkerCount       int    `json:"worker_count"`
	ScanBatchSize     int    `json:"scan_batch_size"`
	SourceQPS         int    `json:"source_qps"`
	TargetQPS         int    `json:"target_qps"`
	SourceConnections int    `json:"source_connections"`
	TargetConnections int    `json:"target_connections"`
	LargeKeyThreshold int64  `json:"large_key_threshold"`
	EstimatedTime     string `json:"estimated_time"`      // 预计耗时
	EstimatedSpeed    int64  `json:"estimated_speed"`     // 预计速度 keys/s
	Reason            string `json:"reason"`              // 推荐理由
}

// HardwareInfo 用户提供的硬件参数
type HardwareInfo struct {
	BandwidthMbps int    `json:"bandwidth_mbps"`  // 网络带宽 Mbps (如 100, 1000, 10000)
	MemoryGB      int    `json:"memory_gb"`       // 可用内存 GB
	KeySizeLevel  string `json:"key_size_level"`  // 大部分 Key 大小: small(<1KB), medium(1-10KB), large(10-100KB), xlarge(>100KB)
}

// KeyFilter Key过滤配置
type KeyFilter struct {
	Mode            string   `json:"mode"` // all, prefix, pattern
	Prefixes        []string `json:"prefixes"`
	ExcludePrefixes []string `json:"exclude_prefixes"`
	Patterns        []string `json:"patterns"`
}

// ==================== Key 清单导入功能 ====================

// KeyListSource Key 清单来源类型
type KeyListSource struct {
	Keys       []string `json:"keys"`        // 内存中的 Key 列表
	TotalCount int      `json:"total_count"` // 总 Key 数量
	Source     string   `json:"source"`      // 来源：file, api, inline
	Format     string   `json:"format"`      // 格式：txt, csv, json
}

// LoadKeyListFromFile 从文件加载 Key 清单
// 支持格式：TXT（每行一个 Key）、CSV（第一列为 Key）、JSON（数组或对象数组）
func LoadKeyListFromFile(filePath string) (*KeyListSource, error) {
	if filePath == "" {
		return nil, nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read key list file failed: %w", err)
	}

	var keys []string
	format := detectKeyListFormat(filePath, data)

	switch format {
	case "json":
		keys, err = parseJSONKeyList(data)
	case "csv":
		keys, err = parseCSVKeyList(data)
	default: // txt
		keys, err = parseTXTKeyList(data)
	}

	if err != nil {
		return nil, fmt.Errorf("parse key list failed: %w", err)
	}

	// 去重
	uniqueKeys := make([]string, 0, len(keys))
	seen := make(map[string]bool)
	for _, key := range keys {
		if key != "" && !seen[key] {
			seen[key] = true
			uniqueKeys = append(uniqueKeys, key)
		}
	}

	return &KeyListSource{
		Keys:       uniqueKeys,
		TotalCount: len(uniqueKeys),
		Source:     "file",
		Format:     format,
	}, nil
}

// detectKeyListFormat 检测 Key 清单文件格式
func detectKeyListFormat(filePath string, data []byte) string {
	// 根据扩展名判断
	lowerPath := strings.ToLower(filePath)
	if strings.HasSuffix(lowerPath, ".json") {
		return "json"
	}
	if strings.HasSuffix(lowerPath, ".csv") {
		return "csv"
	}
	if strings.HasSuffix(lowerPath, ".txt") {
		return "txt"
	}

	// 根据内容判断
	content := strings.TrimSpace(string(data))
	if len(content) > 0 {
		// JSON 格式通常以 [ 或 { 开头
		if content[0] == '[' || content[0] == '{' {
			return "json"
		}
		// CSV 格式通常包含逗号
		if strings.Contains(content, ",") {
			lines := strings.Split(content, "\n")
			if len(lines) > 0 && strings.Contains(lines[0], ",") {
				return "csv"
			}
		}
	}

	return "txt"
}

// parseJSONKeyList 解析 JSON 格式的 Key 清单
// 支持格式：
// 1. 字符串数组：["key1", "key2", "key3"]
// 2. 对象数组：[{"key": "key1"}, {"key": "key2"}]
// 3. 对象数组（name 字段）：[{"name": "key1"}, {"name": "key2"}]
func parseJSONKeyList(data []byte) ([]string, error) {
	var keys []string

	// 尝试解析为字符串数组
	if err := json.Unmarshal(data, &keys); err == nil {
		return keys, nil
	}

	// 尝试解析为对象数组
	var objects []map[string]interface{}
	if err := json.Unmarshal(data, &objects); err == nil {
		for _, obj := range objects {
			// 尝试获取 key 字段
			if key, ok := obj["key"].(string); ok && key != "" {
				keys = append(keys, key)
				continue
			}
			// 尝试获取 name 字段
			if name, ok := obj["name"].(string); ok && name != "" {
				keys = append(keys, name)
				continue
			}
			// 尝试获取 Key 字段（大写）
			if key, ok := obj["Key"].(string); ok && key != "" {
				keys = append(keys, key)
			}
		}
		return keys, nil
	}

	return nil, fmt.Errorf("unsupported JSON format")
}

// parseCSVKeyList 解析 CSV 格式的 Key 清单
// 默认第一列为 Key，跳过标题行（如果有）
func parseCSVKeyList(data []byte) ([]string, error) {
	var keys []string
	lines := strings.Split(string(data), "\n")

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// 分割 CSV 行
		fields := strings.Split(line, ",")
		if len(fields) == 0 {
			continue
		}

		key := strings.TrimSpace(fields[0])
		// 去除可能的引号
		key = strings.Trim(key, "\"'")

		// 跳过标题行
		if i == 0 && (strings.ToLower(key) == "key" || strings.ToLower(key) == "name" || strings.ToLower(key) == "redis_key") {
			continue
		}

		if key != "" {
			keys = append(keys, key)
		}
	}

	return keys, nil
}

// parseTXTKeyList 解析 TXT 格式的 Key 清单（每行一个 Key）
func parseTXTKeyList(data []byte) ([]string, error) {
	var keys []string
	lines := strings.Split(string(data), "\n")

	for _, line := range lines {
		key := strings.TrimSpace(line)
		// 跳过空行和注释行
		if key == "" || strings.HasPrefix(key, "#") || strings.HasPrefix(key, "//") {
			continue
		}
		keys = append(keys, key)
	}

	return keys, nil
}

// ValidateKeyListInSource 验证 Key 清单中的 Key 在源端是否存在
// 返回：存在的 Key 列表、不存在的 Key 列表
func ValidateKeyListInSource(ctx context.Context, client redis.UniversalClient, keys []string, batchSize int) (existingKeys []string, missingKeys []string) {
	if batchSize <= 0 {
		batchSize = 1000
	}

	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]

		// 使用 Pipeline 批量检查 EXISTS
		pipe := client.Pipeline()
		cmds := make([]*redis.IntCmd, len(batch))
		for j, key := range batch {
			cmds[j] = pipe.Exists(ctx, key)
		}
		pipe.Exec(ctx)

		for j, cmd := range cmds {
			if cmd != nil && cmd.Val() > 0 {
				existingKeys = append(existingKeys, batch[j])
			} else {
				missingKeys = append(missingKeys, batch[j])
			}
		}
	}

	return
}

// getScanMatchPattern 根据 KeyFilter 配置构建 SCAN MATCH 模式
// 关键优化：利用 Redis SCAN MATCH 在服务端过滤，减少网络传输
// 评审建议：40亿 Key 场景下，服务端过滤可大幅减少需处理的 Key 数量
func getScanMatchPattern(filter *KeyFilter) string {
	if filter == nil || filter.Mode == "all" || filter.Mode == "" {
		return "*" // 不过滤，匹配所有
	}

	// 如果是前缀模式且只有一个前缀，使用 SCAN MATCH prefix*
	// 评审指出：SCAN MATCH 是服务端过滤，仅返回符合前缀的 Key
	if filter.Mode == "prefix" && len(filter.Prefixes) == 1 && len(filter.ExcludePrefixes) == 0 {
		return filter.Prefixes[0] + "*"
	}

	// 如果是 pattern 模式且只有一个模式，使用 SCAN MATCH
	if filter.Mode == "pattern" && len(filter.Patterns) == 1 {
		return "*" + filter.Patterns[0] + "*"
	}

	// 其他情况：服务端无法完全过滤，返回 * 后在客户端过滤
	// 但仍然会有优化：多前缀时可以分批 SCAN
	return "*"
}

// shouldUseMultiPrefixScan 检查是否应该使用多前缀分批扫描
// 评审建议：多前缀时，每个前缀独立 SCAN，避免扫描无关 Key
func shouldUseMultiPrefixScan(filter *KeyFilter) bool {
	if filter == nil || filter.Mode != "prefix" {
		return false
	}
	// 多个前缀且没有排除前缀时，建议使用多前缀分批扫描
	return len(filter.Prefixes) > 1 && len(filter.ExcludePrefixes) == 0
}

// getPrefixCheckpointKey 获取带前缀维度的断点 Key
// 评审建议：断点按"任务ID+前缀"存储，支持多前缀同时迁移
func getPrefixCheckpointKey(taskID string, nodeAddr string, prefix string) string {
	if prefix == "" || prefix == "*" {
		return fmt.Sprintf("checkpoint:%s:%s", taskID, nodeAddr)
	}
	// 对前缀进行简单编码，避免特殊字符问题
	safePrefix := strings.ReplaceAll(prefix, ":", "_")
	safePrefix = strings.ReplaceAll(safePrefix, "*", "_")
	return fmt.Sprintf("checkpoint:%s:%s:%s", taskID, nodeAddr, safePrefix)
}

// ErrorKey 记录迁移失败或跳过的Key
type ErrorKey struct {
	Key       string `json:"key"`
	Type      string `json:"type"`
	Reason    string `json:"reason"`
	Detail    string `json:"detail"`
	Timestamp string `json:"timestamp"`
}

// getRetryConfig 获取重试配置，返回默认值如果未配置
func getRetryConfig(opts *TaskOptions) (maxRetries int, fullIntervalMs int, incrIntervalMs int) {
	// 默认值
	maxRetries = 3
	fullIntervalMs = 100
	incrIntervalMs = 1000

	if opts != nil && opts.RetryConfig != nil {
		if opts.RetryConfig.MaxRetries > 0 {
			maxRetries = opts.RetryConfig.MaxRetries
		}
		if opts.RetryConfig.FullRetryIntervalMs > 0 {
			fullIntervalMs = opts.RetryConfig.FullRetryIntervalMs
		}
		if opts.RetryConfig.IncrRetryIntervalMs > 0 {
			incrIntervalMs = opts.RetryConfig.IncrRetryIntervalMs
		}
	}
	return
}

var (
	tasks      = make(map[string]*Task)
	tasksMu    sync.RWMutex
	templates  = make(map[string]*TaskTemplate)
	templateMu sync.RWMutex
	errorKeys  = make(map[string][]ErrorKey) // taskID -> error keys
	errorKeyMu sync.RWMutex
	startTime  time.Time
	
	// WebSocket 相关
	wsUpgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	wsClients   = make(map[*websocket.Conn]*WSClient)
	wsClientsMu sync.RWMutex
	
	// 容错相关
	taskRetryState = make(map[string]*TaskRetryState) // taskID -> retry state
	retryStateMu   sync.RWMutex
	lastBackupTime = time.Now()
	
	// 全量断点相关
	fullSyncCheckpoints   = make(map[string]*FullSyncCheckpoint) // taskID -> checkpoint
	fullSyncCheckpointsMu sync.RWMutex
	
	// 连续失败计数器
	consecutiveFailures   = make(map[string]*ConsecutiveFailureTracker)
	consecutiveFailuresMu sync.RWMutex
	
	// 自动恢复状态
	autoRecoveryStates   = make(map[string]*AutoRecoveryState)
	autoRecoveryStatesMu sync.RWMutex
	
	// 停止信号通道（用于优雅关闭）
	stopSmartRetry = make(chan struct{})
)

// TaskRetryState 任务重试状态
type TaskRetryState struct {
	FailedKeys      map[string]int // key -> retry count
	RetryQueue      []string       // 待重试的 key 队列
	TotalRetries    int64          // 总重试次数
	SuccessRetries  int64          // 成功重试次数
	LastRetryTime   time.Time      // 最后重试时间
	mu              sync.Mutex
}

// FullSyncCheckpoint 全量同步断点（包含 SCAN cursor）
type FullSyncCheckpoint struct {
	TaskID           string            `json:"task_id"`
	NodeCursors      map[string]uint64 `json:"node_cursors"`       // nodeAddr -> cursor
	ProcessedKeys    int64             `json:"processed_keys"`     // 已处理的 key 数量
	TotalScannedKeys int64             `json:"total_scanned_keys"` // 总共扫描的 key 数量
	Phase            string            `json:"phase"`              // full, incremental
	StartTime        string            `json:"start_time"`
	UpdatedAt        string            `json:"updated_at"`
	IsComplete       bool              `json:"is_complete"`        // 全量是否完成
}

// ConsecutiveFailureTracker 连续失败追踪器
type ConsecutiveFailureTracker struct {
	SourceFailures    int64     `json:"source_failures"`
	TargetFailures    int64     `json:"target_failures"`
	LastSourceFailure time.Time `json:"last_source_failure"`
	LastTargetFailure time.Time `json:"last_target_failure"`
	LastSourceSuccess time.Time `json:"last_source_success"`
	LastTargetSuccess time.Time `json:"last_target_success"`
	mu                sync.Mutex
}

func main() {
	startTime = time.Now()
	
	// 初始化日志系统
	if err := logger.Init("./logs", logger.DEBUG); err != nil {
		fmt.Printf("Failed to init logger: %v\n", err)
	}
	
	logger.Info("🚀 Tendis Migration Tool starting", map[string]interface{}{
		"port":    8088,
		"version": "1.0.0",
		"pid":     fmt.Sprintf("%d", getPID()),
	})

	// 初始化数据目录
	initDataDirectories()

	initDemoData()

	// 尝试恢复未完成的任务
	recoverUnfinishedTasks()

	// 启动智能重试后台服务
	startSmartRetryService()
	
	// 启动 WebSocket 广播服务
	startWSBroadcaster()

	// 使用自定义 handler 统一处理
	server := &http.Server{
		Addr:         ":8088",
		Handler:      http.HandlerFunc(mainHandler),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// 设置优雅关闭
	setupGracefulShutdown(server)

	logger.Info("Server listening on http://localhost:8088")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatal("Server failed to start", map[string]interface{}{"error": err.Error()})
	}
}

// setupGracefulShutdown 设置优雅关闭处理
// 当收到 SIGINT (Ctrl+C) 或 SIGTERM (kill) 信号时，保存所有状态后再退出
func setupGracefulShutdown(server *http.Server) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("⚠️ Received shutdown signal, starting graceful shutdown...", map[string]interface{}{
			"signal": sig.String(),
		})

		// 0. 停止智能重试服务
		logger.Info("Stopping smart retry service...")
		close(stopSmartRetry)

		// 1. 暂停所有运行中的任务并保存状态
		pauseAllRunningTasks()

		// 2. 保存所有任务状态
		logger.Info("Saving tasks state...")
		saveTasksState()

		// 3. 保存所有全量断点
		logger.Info("Saving full sync checkpoints...")
		saveAllFullSyncCheckpoints()

		// 4. 保存所有增量断点
		logger.Info("Saving incremental sync checkpoints...")
		saveAllIncrementalCheckpoints()

		// 5. 保存所有错误 Key
		logger.Info("Saving error keys...")
		saveAllErrorKeys()

		// 6. 关闭 HTTP 服务器
		logger.Info("Shutting down HTTP server...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			logger.Warn("HTTP server shutdown error", map[string]interface{}{"error": err.Error()})
		}

		logger.Info("✅ Graceful shutdown completed, all state saved")
		os.Exit(0)
	}()
}

// pauseAllRunningTasks 暂停所有运行中的任务
func pauseAllRunningTasks() {
	tasksMu.Lock()
	defer tasksMu.Unlock()

	pausedCount := 0
	now := time.Now()
	for _, task := range tasks {
		if task.Status == "running" {
			task.Status = "paused"
			task.PausedAt = now.Format(time.RFC3339)  // 记录暂停时间
			task.UpdatedAt = now.Format(time.RFC3339)
			pausedCount++
			logger.Info("Task paused for shutdown", map[string]interface{}{
				"task_id":   task.ID,
				"name":      task.Name,
				"paused_at": task.PausedAt,
			})
		}
	}

	if pausedCount > 0 {
		logger.Info("Tasks paused for graceful shutdown", map[string]interface{}{
			"paused_count": pausedCount,
		})
	}
}

// saveAllFullSyncCheckpoints 保存所有全量同步断点
func saveAllFullSyncCheckpoints() {
	fullSyncCheckpointsMu.RLock()
	defer fullSyncCheckpointsMu.RUnlock()

	for taskID, checkpoint := range fullSyncCheckpoints {
		if checkpoint != nil {
			checkpoint.UpdatedAt = time.Now().Format(time.RFC3339)
			checkpointDir := "./data/checkpoints"
			os.MkdirAll(checkpointDir, 0755)
			data, _ := json.MarshalIndent(checkpoint, "", "  ")
			os.WriteFile(fmt.Sprintf("%s/full-%s.json", checkpointDir, taskID), data, 0644)
		}
	}
}

// saveAllIncrementalCheckpoints 保存所有增量同步断点
func saveAllIncrementalCheckpoints() {
	// 保存 V2 断点
	incrCheckpointsV2Mu.RLock()
	for taskID, checkpoint := range incrCheckpointsV2 {
		if checkpoint != nil {
			checkpoint.UpdatedAt = time.Now().Format(time.RFC3339)
			checkpointDir := "./data/checkpoints"
			os.MkdirAll(checkpointDir, 0755)
			data, _ := json.MarshalIndent(checkpoint, "", "  ")
			os.WriteFile(fmt.Sprintf("%s/incr-v2-%s.json", checkpointDir, taskID), data, 0644)
		}
	}
	incrCheckpointsV2Mu.RUnlock()

	// 保存 Binlog 断点
	binlogCheckpointsMu.RLock()
	for taskID, checkpoint := range binlogCheckpoints {
		if checkpoint != nil {
			checkpoint.UpdatedAt = time.Now().Format(time.RFC3339)
			checkpointDir := "./data/checkpoints"
			data, _ := json.MarshalIndent(checkpoint, "", "  ")
			os.WriteFile(fmt.Sprintf("%s/binlog-%s.json", checkpointDir, taskID), data, 0644)
		}
	}
	binlogCheckpointsMu.RUnlock()
}



// initDataDirectories 初始化数据目录
func initDataDirectories() {
	dirs := []string{
		"./data",
		"./data/backups",
		"./data/checkpoints",
		"./logs",
	}
	for _, dir := range dirs {
		os.MkdirAll(dir, 0755)
	}
}

// recoverUnfinishedTasks 恢复未完成的任务
func recoverUnfinishedTasks() {
	// 从文件加载持久化的任务状态
	tasksFile := "./data/tasks-state.json"
	data, err := os.ReadFile(tasksFile)
	if err != nil {
		logger.Debug("No saved tasks state found", map[string]interface{}{"file": tasksFile})
		return
	}

	var savedTasks map[string]*Task
	if err := json.Unmarshal(data, &savedTasks); err != nil {
		logger.Warn("Failed to parse saved tasks", map[string]interface{}{"error": err.Error()})
		return
	}

	recoveredCount := 0
	for id, savedTask := range savedTasks {
		// 只恢复之前正在运行的任务
		if savedTask.Status == "running" || savedTask.Status == "paused" {
			// 创建新任务对象
			task := &Task{
				ID:             savedTask.ID,
				Name:           savedTask.Name,
				Status:         "paused", // 恢复后设为暂停状态，需要用户手动启动
				Progress:       savedTask.Progress,
				SourceCluster:  savedTask.SourceCluster,
				TargetCluster:  savedTask.TargetCluster,
				SourcePassword: savedTask.SourcePassword,
				TargetPassword: savedTask.TargetPassword,
				MigrationMode:  savedTask.MigrationMode,
				CreatedAt:      savedTask.CreatedAt,
				UpdatedAt:      time.Now().Format(time.RFC3339),
				StartedAt:      savedTask.StartedAt,
				FullStartAt:    savedTask.FullStartAt,
				IncrStartAt:    savedTask.IncrStartAt,
				KeysTotal:      savedTask.KeysTotal,
				KeysToMigrate:  savedTask.KeysToMigrate,  // 【新增】恢复待迁移Key数
				KeysMigrated:   savedTask.KeysMigrated,
				KeysFailed:     savedTask.KeysFailed,
				KeysSkipped:    savedTask.KeysSkipped,
				KeysFiltered:   savedTask.KeysFiltered,
				BytesMigrated:  savedTask.BytesMigrated,
				BytesTotal:     savedTask.BytesTotal,
				Phase:          savedTask.Phase,
				Options:        savedTask.Options,
			}

			tasksMu.Lock()
			tasks[id] = task
			tasksMu.Unlock()

			// 恢复错误 key 列表
			if keys := loadErrorKeysFromFile(id); keys != nil {
				logger.Info("Error keys recovered", map[string]interface{}{
					"task_id":    id,
					"error_keys": len(keys),
				})
			}

			// 加载全量同步断点
			if checkpoint := loadFullSyncCheckpoint(id); checkpoint != nil {
				logger.Info("Full sync checkpoint loaded", map[string]interface{}{
					"task_id":        id,
					"processed_keys": checkpoint.ProcessedKeys,
					"is_complete":    checkpoint.IsComplete,
					"nodes":          len(checkpoint.NodeCursors),
				})
			}

			// 加载增量同步断点
			if checkpoint := loadIncrementalCheckpoint(id); checkpoint != nil {
				logger.Info("Incremental checkpoint loaded", map[string]interface{}{
					"task_id":     id,
					"synced_keys": checkpoint.SyncedKeys,
				})
			}

			recoveredCount++
			logger.Info("Task recovered", map[string]interface{}{
				"task_id":         id,
				"task_name":       task.Name,
				"previous_status": savedTask.Status,
				"progress":        task.Progress,
				"phase":           task.Phase,
			})
		}
	}

	if recoveredCount > 0 {
		logger.Info("Tasks recovery completed", map[string]interface{}{
			"recovered_count": recoveredCount,
			"message":         "Recovered tasks are paused. Use resume API to continue.",
		})
	}
}

// saveTasksState 保存任务状态到文件
func saveTasksState() {
	tasksMu.RLock()
	tasksToSave := make(map[string]*Task)
	taskIDs := make([]string, 0)
	for id, task := range tasks {
		// 只保存运行中或暂停的任务
		if task.Status == "running" || task.Status == "paused" {
			tasksToSave[id] = task
			taskIDs = append(taskIDs, id)
		}
	}
	tasksMu.RUnlock()

	if len(tasksToSave) == 0 {
		return
	}

	data, err := json.MarshalIndent(tasksToSave, "", "  ")
	if err != nil {
		logger.Warn("Failed to marshal tasks state", map[string]interface{}{"error": err.Error()})
		return
	}

	tasksFile := "./data/tasks-state.json"
	if err := os.WriteFile(tasksFile, data, 0644); err != nil {
		logger.Warn("Failed to save tasks state", map[string]interface{}{"error": err.Error()})
	}

	// 同时保存所有任务的错误 key
	for _, id := range taskIDs {
		saveErrorKeysToFile(id)
	}
}

// startPeriodicStateSave 启动定期状态保存
func startPeriodicStateSave() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			saveTasksState()
		}
	}()
}

func mainHandler(w http.ResponseWriter, r *http.Request) {
	// 生成请求ID
	requestID := uuid.New().String()
	startTime := time.Now()
	
	// CORS
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("X-Request-ID", requestID)
	
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	path := r.URL.Path
	log := logger.WithRequest(requestID)
	
	// 记录请求日志
	log.Info("Request started", map[string]interface{}{
		"method": r.Method,
		"path":   path,
		"query":  r.URL.RawQuery,
		"remote": r.RemoteAddr,
		"ua":     r.UserAgent(),
	})

	// 包装 ResponseWriter 以捕获状态码
	rw := &responseWriter{ResponseWriter: w, statusCode: 200}

	// 路由处理
	switch {
	// WebSocket 端点 - 注意：必须传原始的 w，因为 WebSocket 需要 http.Hijacker 接口
	case path == "/ws":
		wsHandler(w, r, log)  // 使用原始 ResponseWriter，不能用包装后的 rw
	
	// 日志相关 API
	case path == "/api/v1/logs":
		logsHandler(rw, r, log)
	case path == "/api/v1/logs/export":
		logsExportHandler(rw, r, log)
	case path == "/api/v1/logs/clear":
		logsClearHandler(rw, r, log)
	case path == "/api/v1/logs/stats":
		logsStatsHandler(rw, r, log)
	case path == "/api/v1/logs/cleanup":
		logsCleanupHandler(rw, r, log)
		
	// 业务 API
	case path == "/api/v1/health":
		healthHandler(rw, r, log)
	case path == "/api/v1/health/detailed":
		healthDetailedHandler(rw, r, log)
	case path == "/api/v1/tasks":
		tasksHandler(rw, r, log)
	case strings.HasPrefix(path, "/api/v1/tasks/"):
		taskHandler(rw, r, log)
	case path == "/api/v1/system/status":
		systemHandler(rw, r, log)
	case path == "/api/v1/system/workers":
		systemWorkersHandler(rw, r, log)
	case path == "/api/v1/system/backup":
		systemBackupHandler(rw, r, log)
	case path == "/api/v1/test-connection":
		testConnectionHandler(rw, r, log)
	case path == "/api/v1/analyze-cluster":
		analyzeClusterHandler(rw, r, log)
	case path == "/api/v1/recommend-config":
		recommendConfigHandler(rw, r, log)
	case path == "/api/v1/templates":
		templatesHandler(rw, r, log)
	case strings.HasPrefix(path, "/api/v1/templates/"):
		templateHandler(rw, r, log)
	
	// 智能重试相关 API
	case path == "/api/v1/smart-retry/status":
		smartRetryStatusHandler(rw, r, log)
	
	// Key 清单上传 API
	case path == "/api/v1/upload-keylist":
		uploadKeyListHandler(rw, r, log)
	case path == "/api/v1/parse-keylist":
		parseKeyListHandler(rw, r, log)
		
	// 任务配置导入 API
	case path == "/api/v1/tasks/import":
		importTaskConfigHandler(rw, r, log)
		
	// 静态资源
	case strings.HasPrefix(path, "/assets/"):
		http.FileServer(http.Dir("./web/dist")).ServeHTTP(rw, r)
		
	// SPA 入口
	default:
		http.ServeFile(rw, r, "./web/dist/index.html")
	}

	// 记录响应日志
	duration := time.Since(startTime)
	log.Info("Request completed", map[string]interface{}{
		"status":      rw.statusCode,
		"duration_ms": duration.Milliseconds(),
	})
}

// responseWriter 包装器
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func getPID() int {
	return 1 // 简化处理
}

func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func initDemoData() {
	// 预置模板任务
	now := time.Now().Format("2006-01-02 15:04:05")
	templates["template-default"] = &TaskTemplate{
		ID:            "template-default",
		Name:          "Template",
		Description:   "预置迁移模板：源端到目标端的全量+增量迁移",
		SourceCluster: "10.248.37.11:8901,10.248.37.11:8902,10.248.37.11:8903",
		TargetCluster: "10.31.165.39:8901,10.31.165.39:8902,10.31.165.39:8903",
		MigrationMode: "full_and_incremental",
		Options: &TaskOptions{
			WorkerCount:       8,
			ScanBatchSize:     1000,
			ConflictPolicy:    "skip",
			LargeKeyThreshold: 10485760, // 10MB
			KeyFilter: &KeyFilter{
				Mode:     "prefix",
				Prefixes: []string{"testkey"},
			},
			RateLimit: &RateLimit{
				SourceQPS:         0,
				TargetQPS:         0,
				SourceConnections: 50,
				TargetConnections: 50,
			},
			RetryConfig: &RetryConfig{
				MaxRetries:          3,
				FullRetryIntervalMs: 100,
				IncrRetryIntervalMs: 1000,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	// 启动定期状态保存
	startPeriodicStateSave()

	logger.Info("System initialized", map[string]interface{}{
		"mode":      "production",
		"templates": len(templates),
	})
}

// ==================== 日志 API ====================

func logsHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if limit == 0 {
		limit = 100
	}

	filter := logger.LogFilter{
		Level:     q.Get("level"),
		RequestID: q.Get("request_id"),
		TaskID:    q.Get("task_id"),
		Keyword:   q.Get("keyword"),
		StartTime: q.Get("start_time"),
		EndTime:   q.Get("end_time"),
		Offset:    offset,
		Limit:     limit,
	}

	entries := logger.Default().GetEntries(filter)
	total := logger.Default().GetTotalCount(filter)

	log.Debug("Logs queried", map[string]interface{}{
		"filter": filter,
		"count":  len(entries),
		"total":  total,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"items":  entries,
			"total":  total,
			"offset": offset,
			"limit":  limit,
		},
	})
}

func logsExportHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	format := q.Get("format")
	if format == "" {
		format = "text"
	}

	taskID := q.Get("task_id")
	filter := logger.LogFilter{
		Level:     q.Get("level"),
		RequestID: q.Get("request_id"),
		TaskID:    taskID,
		Keyword:   q.Get("keyword"),
		StartTime: q.Get("start_time"),
		EndTime:   q.Get("end_time"),
	}

	// 如果指定了任务ID，获取任务名称用于文件名
	taskName := ""
	if taskID != "" {
		tasksMu.RLock()
		for _, t := range tasks {
			if t.ID == taskID || strings.HasPrefix(t.ID, taskID) {
				taskName = t.Name
				filter.TaskID = t.ID // 使用完整ID
				break
			}
		}
		tasksMu.RUnlock()
	}

	data, err := logger.Default().Export(filter, format)
	if err != nil {
		log.Error("Failed to export logs", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 500, "message": err.Error()})
		return
	}

	// 生成文件名
	var filename string
	timestamp := time.Now().Format("20060102-150405")
	ext := "txt"
	if format == "json" {
		ext = "json"
	}
	if taskID != "" {
		shortID := taskID
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}
		if taskName != "" {
			// 清理任务名中的特殊字符
			safeName := strings.Map(func(r rune) rune {
				if r == '/' || r == '\\' || r == ':' || r == '*' || r == '?' || r == '"' || r == '<' || r == '>' || r == '|' {
					return '-'
				}
				return r
			}, taskName)
			filename = fmt.Sprintf("task-%s-%s-%s.%s", shortID, safeName, timestamp, ext)
		} else {
			filename = fmt.Sprintf("task-%s-logs-%s.%s", shortID, timestamp, ext)
		}
	} else {
		filename = fmt.Sprintf("tendis-migrate-logs-%s.%s", timestamp, ext)
	}
	
	if format == "json" {
		w.Header().Set("Content-Type", "application/json")
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Write(data)

	log.Info("Logs exported", map[string]interface{}{
		"format":   format,
		"size":     len(data),
		"filename": filename,
		"task_id":  taskID,
	})
}

func logsClearHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	logger.Default().Clear()
	log.Info("Logs cleared")

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
	})
}

func logsCleanupHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 获取清理前的统计
	beforeStats := logger.Default().GetLogStats()

	// 执行清理
	logger.Default().CleanupNow()

	// 获取清理后的统计
	afterStats := logger.Default().GetLogStats()

	log.Info("Manual log cleanup executed", map[string]interface{}{
		"before_files": beforeStats["total_files"],
		"after_files":  afterStats["total_files"],
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"before": beforeStats,
			"after":  afterStats,
		},
	})
}

func logsStatsHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	l := logger.Default()
	
	// 统计各级别日志数量
	stats := map[string]int{
		"DEBUG": 0,
		"INFO":  0,
		"WARN":  0,
		"ERROR": 0,
		"FATAL": 0,
	}
	
	allEntries := l.GetEntries(logger.LogFilter{Limit: 0})
	for _, e := range allEntries {
		stats[e.Level]++
	}

	// 获取日志文件统计信息
	logStats := l.GetLogStats()

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"total":      len(allEntries),
			"by_level":   stats,
			"uptime":     time.Since(startTime).String(),
			"memory_mb":  getMemoryUsage(),
			"file_stats": logStats,
		},
	})
}

func getMemoryUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / 1024 / 1024
}

// ==================== WebSocket 支持 ====================

// WSClient WebSocket 客户端
type WSClient struct {
	conn       *websocket.Conn
	taskIDs    map[string]bool // 订阅的任务 ID
	mu         sync.RWMutex
	sendChan   chan []byte
	done       chan struct{}
}

// WSMessage WebSocket 消息格式
type WSMessage struct {
	Type    string      `json:"type"`    // subscribe, unsubscribe, ping, metrics, log, status
	TaskID  string      `json:"task_id,omitempty"`
	Payload interface{} `json:"payload,omitempty"`
}

// wsHandler WebSocket 连接处理
func wsHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("WebSocket upgrade failed", map[string]interface{}{"error": err.Error()})
		return
	}
	
	client := &WSClient{
		conn:     conn,
		taskIDs:  make(map[string]bool),
		sendChan: make(chan []byte, 256),
		done:     make(chan struct{}),
	}
	
	wsClientsMu.Lock()
	wsClients[conn] = client
	wsClientsMu.Unlock()
	
	log.Info("WebSocket client connected", map[string]interface{}{
		"remote": r.RemoteAddr,
		"total_clients": len(wsClients),
	})
	
	// 启动发送协程
	go client.writePump()
	
	// 主循环：读取消息
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Debug("WebSocket read error", map[string]interface{}{"error": err.Error()})
			}
			break
		}
		
		var msg WSMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}
		
		switch msg.Type {
		case "subscribe":
			client.subscribe(msg.TaskID, log)
		case "unsubscribe":
			client.unsubscribe(msg.TaskID)
		case "ping":
			client.sendMessage(&WSMessage{Type: "pong"})
		}
	}
	
	// 清理
	close(client.done)
	wsClientsMu.Lock()
	delete(wsClients, conn)
	wsClientsMu.Unlock()
	conn.Close()
	
	log.Info("WebSocket client disconnected", map[string]interface{}{
		"remote": r.RemoteAddr,
		"total_clients": len(wsClients),
	})
}

// subscribe 订阅任务更新
func (c *WSClient) subscribe(taskID string, log *logger.RequestLogger) {
	if taskID == "" {
		return
	}
	c.mu.Lock()
	c.taskIDs[taskID] = true
	c.mu.Unlock()
	
	log.Debug("Client subscribed to task", map[string]interface{}{"task_id": taskID})
	
	// 发送订阅确认
	c.sendMessage(&WSMessage{
		Type:   "subscribed",
		TaskID: taskID,
	})
	
	// 立即发送当前任务状态
	tasksMu.RLock()
	task, ok := tasks[taskID]
	tasksMu.RUnlock()
	
	if ok {
		c.sendTaskMetrics(task)
	}
}

// unsubscribe 取消订阅
func (c *WSClient) unsubscribe(taskID string) {
	c.mu.Lock()
	delete(c.taskIDs, taskID)
	c.mu.Unlock()
	
	c.sendMessage(&WSMessage{
		Type:   "unsubscribed",
		TaskID: taskID,
	})
}

// sendMessage 发送消息
func (c *WSClient) sendMessage(msg *WSMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	
	select {
	case c.sendChan <- data:
	default:
		// 通道满了，丢弃消息
	}
}

// writePump 发送消息协程
func (c *WSClient) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-c.done:
			return
		case message := <-c.sendChan:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// sendTaskMetrics 发送任务指标
func (c *WSClient) sendTaskMetrics(task *Task) {
	// 计算速度
	speed := task.Speed
	if task.speedTracker != nil {
		speed = task.speedTracker.GetSpeed()
	}
	
	c.sendMessage(&WSMessage{
		Type:   "metrics",
		TaskID: task.ID,
		Payload: map[string]interface{}{
			"status":         task.Status,
			"progress":       task.Progress,
			"processed_keys": task.KeysMigrated,
			"total_keys":     task.KeysTotal,
			"current_qps":    speed,
			"bytes_written":  task.BytesMigrated,
			"failed_keys":    task.KeysFailed,
			"skipped_keys":   task.KeysSkipped,  // 冲突跳过的 Key 数（目标端已存在）
			// 【修复】移除重复的 conflict_keys 字段，它和 skipped_keys 是同一个值
			"phase":          task.Phase,
		},
	})
}

// broadcastTaskUpdate 广播任务更新给所有订阅该任务的客户端
func broadcastTaskUpdate(taskID string) {
	tasksMu.RLock()
	task, ok := tasks[taskID]
	tasksMu.RUnlock()
	
	if !ok {
		return
	}
	
	wsClientsMu.RLock()
	defer wsClientsMu.RUnlock()
	
	for _, client := range wsClients {
		client.mu.RLock()
		subscribed := client.taskIDs[taskID]
		client.mu.RUnlock()
		
		if subscribed {
			client.sendTaskMetrics(task)
		}
	}
}

// broadcastTaskLog 广播任务日志
func broadcastTaskLog(taskID, level, message string) {
	wsClientsMu.RLock()
	defer wsClientsMu.RUnlock()
	
	msg := &WSMessage{
		Type:   "log",
		TaskID: taskID,
		Payload: map[string]interface{}{
			"level":     level,
			"message":   message,
			"timestamp": time.Now().Format(time.RFC3339Nano),
		},
	}
	
	for _, client := range wsClients {
		client.mu.RLock()
		subscribed := client.taskIDs[taskID]
		client.mu.RUnlock()
		
		if subscribed {
			client.sendMessage(msg)
		}
	}
}

// broadcastTaskStatus 广播任务状态变化
func broadcastTaskStatus(taskID, status string) {
	wsClientsMu.RLock()
	defer wsClientsMu.RUnlock()
	
	msg := &WSMessage{
		Type:   "status",
		TaskID: taskID,
		Payload: map[string]interface{}{
			"status": status,
		},
	}
	
	for _, client := range wsClients {
		client.mu.RLock()
		subscribed := client.taskIDs[taskID]
		client.mu.RUnlock()
		
		if subscribed {
			client.sendMessage(msg)
		}
	}
}

// startWSBroadcaster 启动 WebSocket 广播服务
func startWSBroadcaster() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		
		for range ticker.C {
			// 获取所有运行中的任务
			tasksMu.RLock()
			var runningTasks []string
			for id, t := range tasks {
				if t.Status == "running" {
					runningTasks = append(runningTasks, id)
				}
			}
			tasksMu.RUnlock()
			
			// 广播更新
			for _, taskID := range runningTasks {
				broadcastTaskUpdate(taskID)
			}
		}
	}()
	
	logger.Info("WebSocket broadcaster started")
}

// ==================== 业务 API ====================

func healthHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	log.Debug("Health check")
	jsonResponse(w, map[string]interface{}{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	})
}

func tasksHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	switch r.Method {
	case "GET":
		listTasksHandler(w, r, log)
	case "POST":
		createTaskHandler(w, r, log)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func listTasksHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	tasksMu.RLock()
	defer tasksMu.RUnlock()

	var items []map[string]interface{}
	for _, t := range tasks {
		items = append(items, map[string]interface{}{
			"id":         t.ID,
			"name":       t.Name,
			"status":     t.Status,
			"created_at": t.CreatedAt,
			"updated_at": t.UpdatedAt,
			"progress": map[string]interface{}{
				"percentage":    t.Progress,
				"keys_total":    t.KeysTotal,
				"keys_migrated": t.KeysMigrated,
				"speed":         t.Speed,
			},
		})
	}

	// 按创建时间倒序排序（最新的在前面）
	sort.Slice(items, func(i, j int) bool {
		timeI, _ := items[i]["created_at"].(string)
		timeJ, _ := items[j]["created_at"].(string)
		return timeI > timeJ
	})

	log.Debug("Tasks listed", map[string]interface{}{"count": len(items)})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"items": items,
			"total": len(items),
		},
	})
}

func createTaskHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	var req struct {
		Name          string `json:"name"`
		MigrationMode string `json:"migration_mode"`
		SourceCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"source_cluster"`
		TargetCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"target_cluster"`
		Options          *TaskOptions `json:"options"`
		AutoRecommend    bool         `json:"auto_recommend"` // 是否自动使用推荐配置
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Error("Failed to decode request", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 400, "message": err.Error()})
		return
	}

	mode := req.MigrationMode
	if mode == "" {
		mode = "full_and_incremental"
	}

	// 设置默认选项
	options := req.Options
	if options == nil {
		options = &TaskOptions{
			WorkerCount:       8, // 默认值提升到8
			ScanBatchSize:     1000,
			ConflictPolicy:    "skip_full_only",
			LargeKeyThreshold: 10485760,
			KeyFilter: &KeyFilter{
				Mode: "all",
			},
		}
	} else {
		// 确保 KeyFilter 有默认值
		if options.KeyFilter == nil {
			options.KeyFilter = &KeyFilter{Mode: "all"}
		} else if options.KeyFilter.Mode == "" {
			options.KeyFilter.Mode = "all"
		}
		// 确保其他选项有默认值
		if options.WorkerCount == 0 {
			options.WorkerCount = 8 // 默认值提升到8
		}
		if options.ScanBatchSize == 0 {
			options.ScanBatchSize = 1000
		}
		if options.ConflictPolicy == "" {
			options.ConflictPolicy = "skip_full_only"
		}
		if options.LargeKeyThreshold == 0 {
			options.LargeKeyThreshold = 10485760
		}
	}
	
	// 如果启用自动推荐配置，或者用户没有明确指定配置（options为nil或关键参数为默认值）
	// 则自动调用推荐配置逻辑计算最佳参数
	shouldAutoRecommend := req.AutoRecommend || req.Options == nil
	if shouldAutoRecommend && len(req.SourceCluster.Addrs) > 0 && len(req.TargetCluster.Addrs) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		
		sourceInfo, srcErr := analyzeCluster(ctx, req.SourceCluster.Addrs, req.SourceCluster.Password)
		targetInfo, tgtErr := analyzeCluster(ctx, req.TargetCluster.Addrs, req.TargetCluster.Password)
		
		if srcErr == nil && tgtErr == nil {
			recommended := generateRecommendedConfig(sourceInfo, targetInfo, nil)
			log.Info("Auto recommended config applied", map[string]interface{}{
				"worker_count":        recommended.WorkerCount,
				"scan_batch_size":     recommended.ScanBatchSize,
				"source_connections":  recommended.SourceConnections,
				"target_connections":  recommended.TargetConnections,
				"reason":              recommended.Reason,
			})
			
			// 应用推荐配置
			options.WorkerCount = recommended.WorkerCount
			options.ScanBatchSize = recommended.ScanBatchSize
			if options.RateLimit == nil {
				options.RateLimit = &RateLimit{}
			}
			options.RateLimit.SourceQPS = recommended.SourceQPS
			options.RateLimit.TargetQPS = recommended.TargetQPS
			options.RateLimit.SourceConnections = recommended.SourceConnections
			options.RateLimit.TargetConnections = recommended.TargetConnections
			if recommended.LargeKeyThreshold > 0 {
				options.LargeKeyThreshold = int64(recommended.LargeKeyThreshold)
			}
		} else {
			log.Warn("Failed to auto recommend config, using defaults", map[string]interface{}{
				"source_err": srcErr,
				"target_err": tgtErr,
			})
		}
	}

	task := &Task{
		ID:             uuid.New().String(),
		Name:           req.Name,
		Status:         "pending",
		Progress:       0,
		SourceCluster:  strings.Join(req.SourceCluster.Addrs, ","),
		TargetCluster:  strings.Join(req.TargetCluster.Addrs, ","),
		SourcePassword: req.SourceCluster.Password,
		TargetPassword: req.TargetCluster.Password,
		MigrationMode:  mode,
		CreatedAt:      time.Now().Format(time.RFC3339),
		UpdatedAt:      time.Now().Format(time.RFC3339),
		Phase:          "full",
		Options:        options,
	}

	tasksMu.Lock()
	tasks[task.ID] = task
	
	// 保留最近3个任务，清理旧任务
	cleanupOldTasks()
	tasksMu.Unlock()

	log.Info("Task created", map[string]interface{}{
		"task_id":        task.ID,
		"task_name":      task.Name,
		"migration_mode": mode,
	})

	// 同时记录任务日志
	logger.WithTask(task.ID).Info("Task created", map[string]interface{}{
		"name":           task.Name,
		"source":         task.SourceCluster,
		"target":         task.TargetCluster,
		"migration_mode": mode,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    map[string]string{"task_id": task.ID},
	})
}

// cleanupOldTasks 清理旧任务，保留最近3个（必须在tasksMu锁内调用）
func cleanupOldTasks() {
	const maxTasks = 3
	if len(tasks) <= maxTasks {
		return
	}
	
	// 获取所有任务并按创建时间排序
	taskList := make([]*Task, 0, len(tasks))
	for _, t := range tasks {
		taskList = append(taskList, t)
	}
	sort.Slice(taskList, func(i, j int) bool {
		return taskList[i].CreatedAt > taskList[j].CreatedAt // 降序，最新在前
	})
	
	// 删除超出限制的旧任务
	for i := maxTasks; i < len(taskList); i++ {
		oldTask := taskList[i]
		// 如果任务正在运行，跳过
		if oldTask.Status == "running" {
			continue
		}
		// 清理该任务的日志
		logger.Default().ClearTaskLogs(oldTask.ID)
		// 删除任务
		delete(tasks, oldTask.ID)
		logger.Default().Info("Old task cleaned up", map[string]interface{}{
			"task_id":   oldTask.ID,
			"task_name": oldTask.Name,
		})
	}
}

func taskHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/tasks/")
	parts := strings.Split(path, "/")

	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	id := parts[0]
	action := ""
	if len(parts) > 1 {
		action = strings.Join(parts[1:], "/") // 保留完整的子路径
	}

	taskLog := logger.WithTask(id)

	switch {
	case action == "" && r.Method == "GET":
		getTaskHandler(w, r, id, log)
	case action == "" && r.Method == "DELETE":
		deleteTaskHandler(w, r, id, log, taskLog)
	case action == "config" && r.Method == "PUT":
		updateTaskConfigHandler(w, r, id, log, taskLog)
	case action == "start" && r.Method == "POST":
		startTaskHandler(w, r, id, log, taskLog)
	case action == "pause" && r.Method == "POST":
		pauseTaskHandler(w, r, id, log, taskLog)
	case action == "resume" && r.Method == "POST":
		resumeTaskHandler(w, r, id, log, taskLog)
	case action == "restart" && r.Method == "POST":
		restartTaskHandler(w, r, id, log, taskLog)
	case action == "retry-failed" && r.Method == "POST":
		retryFailedKeysHandler(w, r, id, log, taskLog)
	case action == "progress" && r.Method == "GET":
		progressHandler(w, r, id, log)
	case action == "logs" && r.Method == "GET":
		taskLogsHandler(w, r, id, log)
	case action == "verify" && r.Method == "POST":
		triggerVerifyHandler(w, r, id, log)
	case strings.HasPrefix(action, "verify") && r.Method == "GET":
		verifyResultsHandler(w, r, id, log)
	case action == "error-keys" && r.Method == "GET":
		errorKeysHandler(w, r, id, log)
	case strings.HasPrefix(action, "error-keys/download") && r.Method == "GET":
		downloadErrorKeysHandler(w, r, id, log)
	case action == "health" && r.Method == "GET":
		taskHealthHandler(w, r, id, log)
	case action == "auto-recovery" && r.Method == "GET":
		autoRecoveryStatusHandler(w, r, id, log)
	case action == "auto-recovery" && r.Method == "POST":
		toggleAutoRecoveryHandler(w, r, id, log)
	case action == "shadow-stats" && r.Method == "GET":
		shadowStatsHandler(w, r, id, log)
	case action == "export" && r.Method == "GET":
		exportTaskConfigHandler(w, r, id, log)
	case action == "report" && r.Method == "GET":
		exportTaskReportHandler(w, r, id, log)
	case action == "stop-incremental" && r.Method == "POST":
		stopIncrementalHandler(w, r, id, log, taskLog)
	case action == "complete" && r.Method == "POST":
		completeTaskHandler(w, r, id, log, taskLog)
	case action == "metrics" && r.Method == "GET":
		taskMetricsHandler(w, r, id, log)
	case action == "conflicts" && r.Method == "GET":
		conflictKeysHandler(w, r, id, log)
	case strings.HasPrefix(action, "conflicts/") && r.Method == "GET":
		conflictKeysSubHandler(w, r, id, action, log)
	default:
		http.NotFound(w, r)
	}
}

func getTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		log.Warn("Task not found", map[string]interface{}{"task_id": id})
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	log.Debug("Task retrieved", map[string]interface{}{"task_id": id})

	phase := task.Phase
	if phase == "" {
		phase = "full"
	}

	// 构建配置信息
	configData := map[string]interface{}{
		"worker_count":       8,
		"scan_batch_size":    1000,
		"conflict_policy":    "skip",
		"large_key_threshold": 10485760,
		"rate_limit": map[string]interface{}{
			"source_qps":         0,
			"target_qps":         0,
			"source_connections": 50,
			"target_connections": 50,
		},
	}
	if task.Options != nil {
		configData["worker_count"] = task.Options.WorkerCount
		configData["scan_batch_size"] = task.Options.ScanBatchSize
		configData["conflict_policy"] = task.Options.ConflictPolicy
		configData["large_key_threshold"] = task.Options.LargeKeyThreshold
		if task.Options.RateLimit != nil {
			configData["rate_limit"] = map[string]interface{}{
				"source_qps":         task.Options.RateLimit.SourceQPS,
				"target_qps":         task.Options.RateLimit.TargetQPS,
				"source_connections": task.Options.RateLimit.SourceConnections,
				"target_connections": task.Options.RateLimit.TargetConnections,
			}
		}
		// 添加 key_filter 信息
		if task.Options.KeyFilter != nil {
			configData["key_filter"] = map[string]interface{}{
				"mode":             task.Options.KeyFilter.Mode,
				"prefixes":         task.Options.KeyFilter.Prefixes,
				"exclude_prefixes": task.Options.KeyFilter.ExcludePrefixes,
				"patterns":         task.Options.KeyFilter.Patterns,
			}
		}
	}

	// P2 改进：获取详细进度指标
	detailedProgress := getDetailedProgressMetrics(id, task)

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"id":             task.ID,
			"name":           task.Name,
			"status":         task.Status,
			"migration_mode": task.MigrationMode,
			"source_cluster": map[string]interface{}{"addrs": strings.Split(task.SourceCluster, ",")},
			"target_cluster": map[string]interface{}{"addrs": strings.Split(task.TargetCluster, ",")},
			"created_at":     task.CreatedAt,
			"updated_at":     task.UpdatedAt,
			"started_at":     task.StartedAt,
			"full_start_at":  task.FullStartAt,
			"incr_start_at":  task.IncrStartAt,
			"keys_filtered":  task.KeysFiltered,
			"config":         configData,
			"progress": map[string]interface{}{
				"percentage":      task.Progress,
				"total_keys":      task.KeysTotal,
				"keys_to_migrate": task.KeysToMigrate,  // 【新增】符合条件待迁移的Key数
				"migrated_keys":   task.KeysMigrated,
				"total_bytes":     task.BytesTotal,
				"migrated_bytes":  task.BytesMigrated,
				"current_speed":   task.Speed,
				"phase":           phase,
				"estimated_eta":   calculateETA(task),
				"elapsed_time":    calculateElapsedTime(task),
			},
			"stats": map[string]interface{}{
				"total_keys":      task.KeysTotal,
				"keys_to_migrate": task.KeysToMigrate,  // 【新增】符合条件待迁移的Key数
				"migrated_keys":   task.KeysMigrated,
				"failed_keys":     task.KeysFailed,
				"skipped_keys":    task.KeysSkipped,
				"filtered_keys":   task.KeysFiltered,
				"bytes_sent":      task.BytesMigrated,
			},
			// 增量同步指标（Binlog 模式）
			"incr_keys_synced":   task.IncrKeysSynced,
			"incr_keys_skipped":  task.IncrKeysSkipped,
			"incr_keys_failed":   task.IncrKeysFailed,
			"incr_keys_filtered": task.IncrKeysFiltered,
			"incr_binlog_pos":    task.IncrBinlogPos,
			"incr_lag_ms":        task.IncrLagMs,
			"incr_heartbeats":    task.IncrHeartbeats,
			"incr_reconnects":    task.IncrReconnects,
			"incr_sync_mode":     task.IncrSyncMode,
			// P2 改进：详细进度指标
			"detailed_progress": detailedProgress,
		},
	})
}

// ==================== P2 改进: 详细进度指标 ====================

// getDetailedProgressMetrics 获取详细进度指标（P2 改进）
func getDetailedProgressMetrics(taskID string, task *Task) map[string]interface{} {
	metrics := map[string]interface{}{
		"version": "v2",
	}

	// 全量同步断点信息
	fullCheckpoint := loadFullSyncCheckpointFromFile(taskID)
	if fullCheckpoint != nil {
		metrics["full_sync"] = map[string]interface{}{
			"is_complete":    fullCheckpoint.IsComplete,
			"processed_keys": fullCheckpoint.ProcessedKeys,
			"scanned_keys":   fullCheckpoint.TotalScannedKeys,
			"start_time":     fullCheckpoint.StartTime,
			"updated_at":     fullCheckpoint.UpdatedAt,
			"node_count":     len(fullCheckpoint.NodeCursors),
		}
	}

	// V2 增量同步断点信息
	incrCheckpointV2 := loadIncrementalCheckpointV2(taskID)
	if incrCheckpointV2 != nil {
		metrics["incremental_sync_v2"] = map[string]interface{}{
			"keys_synced":         incrCheckpointV2.KeysSynced,
			"keys_skipped":        incrCheckpointV2.KeysSkipped,
			"keys_failed":         incrCheckpointV2.KeysFailed,
			"scan_rounds":         incrCheckpointV2.ScanRounds,
			"sync_interval_sec":   incrCheckpointV2.SyncInterval,
			"last_sync_time":      incrCheckpointV2.LastSyncTime,
			"last_round_duration": incrCheckpointV2.LastRoundDuration,
			"last_round_synced":   incrCheckpointV2.LastRoundSynced,
			"avg_round_duration":  incrCheckpointV2.AvgRoundDuration,
			"estimated_lag":       incrCheckpointV2.EstimatedLag,
			"updated_at":          incrCheckpointV2.UpdatedAt,
		}
	}

	// 错误 Key 统计（P0 改进）
	metrics["error_keys"] = getErrorKeysStats(taskID)

	// Worker 状态
	if task.workerPool != nil {
		metrics["worker_pool"] = map[string]interface{}{
			"active_workers": task.workerPool.GetActiveWorkerCount(),
			"target_workers": atomic.LoadInt32(&task.workerPool.targetWorkers),
		}
	}

	// 内存使用
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	metrics["memory"] = map[string]interface{}{
		"alloc_mb":       float64(memStats.Alloc) / 1024 / 1024,
		"total_alloc_mb": float64(memStats.TotalAlloc) / 1024 / 1024,
		"sys_mb":         float64(memStats.Sys) / 1024 / 1024,
		"num_gc":         memStats.NumGC,
	}

	return metrics
}

// loadFullSyncCheckpointFromFile 从文件加载全量同步断点
func loadFullSyncCheckpointFromFile(taskID string) *FullSyncCheckpoint {
	// 先从内存加载
	fullSyncCheckpointsMu.RLock()
	if cp, ok := fullSyncCheckpoints[taskID]; ok {
		fullSyncCheckpointsMu.RUnlock()
		return cp
	}
	fullSyncCheckpointsMu.RUnlock()

	// 从文件加载
	checkpointFile := fmt.Sprintf("./data/checkpoints/full-%s.json", taskID)
	data, err := os.ReadFile(checkpointFile)
	if err != nil {
		return nil
	}

	var checkpoint FullSyncCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil
	}

	return &checkpoint
}

func calculateETA(task *Task) string {
	if task.Speed <= 0 || task.KeysTotal <= task.KeysMigrated {
		return "-"
	}
	remaining := task.KeysTotal - task.KeysMigrated
	seconds := remaining / task.Speed
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	} else if seconds < 3600 {
		return fmt.Sprintf("%dm %ds", seconds/60, seconds%60)
	}
	return fmt.Sprintf("%dh %dm", seconds/3600, (seconds%3600)/60)
}

// calculateElapsedTime 计算已耗时间（排除暂停时间）
func calculateElapsedTime(task *Task) string {
	if task.StartedAt == "" {
		return "-"
	}
	// 使用本地时区解析时间
	loc := time.Local
	startTime, err := time.ParseInLocation("2006-01-02 15:04:05", task.StartedAt, loc)
	if err != nil {
		// 尝试解析 RFC3339 格式
		startTime, err = time.Parse(time.RFC3339, task.StartedAt)
		if err != nil {
			return "-"
		}
	}
	
	// 确定结束时间
	var endTime time.Time
	if task.Status == "completed" && task.CompletedAt != "" {
		// 已完成：使用完成时间
		endTime, err = time.ParseInLocation("2006-01-02 15:04:05", task.CompletedAt, loc)
		if err != nil {
			endTime, err = time.Parse(time.RFC3339, task.CompletedAt)
			if err != nil {
				endTime = time.Now()
			}
		}
	} else if task.Status == "paused" && task.PausedAt != "" {
		// 已暂停：使用暂停时间作为结束时间（不再增加）
		endTime, err = time.ParseInLocation("2006-01-02 15:04:05", task.PausedAt, loc)
		if err != nil {
			endTime, err = time.Parse(time.RFC3339, task.PausedAt)
			if err != nil {
				endTime = time.Now()
			}
		}
	} else {
		// 运行中：使用当前时间
		endTime = time.Now()
	}
	
	// 计算总耗时
	elapsed := endTime.Sub(startTime)
	seconds := int64(elapsed.Seconds())
	
	// 减去累计暂停时长
	seconds -= task.PausedDuration
	
	if seconds < 0 {
		return "-"
	}
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	} else if seconds < 3600 {
		return fmt.Sprintf("%dm %ds", seconds/60, seconds%60)
	} else if seconds < 86400 {
		return fmt.Sprintf("%dh %dm", seconds/3600, (seconds%3600)/60)
	}
	return fmt.Sprintf("%dd %dh", seconds/86400, (seconds%86400)/3600)
}

func deleteTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	delete(tasks, id)
	tasksMu.Unlock()
	
	log.Info("Task deleted", map[string]interface{}{"task_id": id})
	taskLog.Info("Task deleted")
	
	jsonResponse(w, map[string]interface{}{"code": 0, "message": "success"})
}

// updateTaskConfigHandler 动态调整任务配置（优雅调整）
func updateTaskConfigHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	tasksMu.Unlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 解析请求体
	var req struct {
		WorkerCount    int `json:"worker_count"`
		ScanBatchSize  int `json:"scan_batch_size"`
		RateLimit      *RateLimit `json:"rate_limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Invalid request body"})
		return
	}

	// 更新配置
	tasksMu.Lock()
	if task.Options == nil {
		task.Options = &TaskOptions{}
	}
	
	oldWorkerCount := task.Options.WorkerCount
	oldScanBatchSize := task.Options.ScanBatchSize
	
	if req.WorkerCount > 0 {
		task.Options.WorkerCount = req.WorkerCount
	}
	if req.ScanBatchSize > 0 {
		task.Options.ScanBatchSize = req.ScanBatchSize
	}
	if req.RateLimit != nil {
		if task.Options.RateLimit == nil {
			task.Options.RateLimit = &RateLimit{}
		}
		task.Options.RateLimit.SourceQPS = req.RateLimit.SourceQPS
		task.Options.RateLimit.TargetQPS = req.RateLimit.TargetQPS
	}
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	tasksMu.Unlock()

	log.Info("Task config updated", map[string]interface{}{
		"task_id":            id,
		"old_worker_count":   oldWorkerCount,
		"new_worker_count":   task.Options.WorkerCount,
		"old_scan_batch":     oldScanBatchSize,
		"new_scan_batch":     task.Options.ScanBatchSize,
	})
	
	// 记录Worker动态调整信息
	adjustMsg := "will take effect dynamically"
	if task.workerPool != nil {
		currentActive := task.workerPool.GetActiveWorkerCount()
		if oldWorkerCount != task.Options.WorkerCount {
			if task.Options.WorkerCount > oldWorkerCount {
				adjustMsg = fmt.Sprintf("increasing workers from %d to %d (current active: %d)", oldWorkerCount, task.Options.WorkerCount, currentActive)
			} else {
				adjustMsg = fmt.Sprintf("decreasing workers from %d to %d gracefully (current active: %d)", oldWorkerCount, task.Options.WorkerCount, currentActive)
			}
		}
	}
	
	// 获取QPS值用于日志（处理空指针）
	sourceQPS := 0
	targetQPS := 0
	if task.Options.RateLimit != nil {
		sourceQPS = task.Options.RateLimit.SourceQPS
		targetQPS = task.Options.RateLimit.TargetQPS
	}
	
	taskLog.Info("Config updated (dynamic adjustment)", map[string]interface{}{
		"worker_count":    task.Options.WorkerCount,
		"scan_batch_size": task.Options.ScanBatchSize,
		"source_qps":      sourceQPS,
		"target_qps":      targetQPS,
		"adjustment":      adjustMsg,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "Config updated dynamically, worker adjustment in progress",
		"data": map[string]interface{}{
			"worker_count":    task.Options.WorkerCount,
			"scan_batch_size": task.Options.ScanBatchSize,
			"rate_limit":      task.Options.RateLimit,
			"adjustment":      adjustMsg,
		},
	})
}

func startTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	if ok {
		task.Status = "running"
		task.UpdatedAt = time.Now().Format(time.RFC3339)
		task.StartedAt = time.Now().Format("2006-01-02 15:04:05")
		go simulateProgress(task)
	}
	tasksMu.Unlock()
	
	if ok {
		log.Info("Task started", map[string]interface{}{"task_id": id})
		taskLog.Info("Task started", map[string]interface{}{
			"keys_total": task.KeysTotal,
		})
	} else {
		log.Warn("Task not found for start", map[string]interface{}{"task_id": id})
	}
	
	jsonResponse(w, map[string]interface{}{"code": 0, "message": "success"})
}

func pauseTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	if ok {
		task.Status = "paused"
		task.Speed = 0
		now := time.Now()
		task.PausedAt = now.Format(time.RFC3339)  // 记录暂停时间
		task.UpdatedAt = now.Format(time.RFC3339)
	}
	tasksMu.Unlock()
	
	if ok {
		log.Info("Task paused", map[string]interface{}{"task_id": id})
		taskLog.Info("Task paused", map[string]interface{}{
			"progress":  task.Progress,
			"paused_at": task.PausedAt,
		})
	}
	
	jsonResponse(w, map[string]interface{}{"code": 0, "message": "success"})
}

func resumeTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	if ok {
		// 计算本次暂停的时长并累加
		if task.PausedAt != "" {
			loc := time.Local
			pausedTime, err := time.ParseInLocation(time.RFC3339, task.PausedAt, loc)
			if err != nil {
				pausedTime, _ = time.ParseInLocation("2006-01-02 15:04:05", task.PausedAt, loc)
			}
			if !pausedTime.IsZero() {
				pausedSeconds := int64(time.Since(pausedTime).Seconds())
				task.PausedDuration += pausedSeconds  // 累计暂停时长
			}
		}
		task.Status = "running"
		task.PausedAt = ""  // 清空暂停时间
		task.UpdatedAt = time.Now().Format(time.RFC3339)
		go simulateProgress(task)
	}
	tasksMu.Unlock()
	
	if ok {
		log.Info("Task resumed", map[string]interface{}{"task_id": id})
		taskLog.Info("Task resumed", map[string]interface{}{
			"paused_duration": task.PausedDuration,
		})
	}
	
	jsonResponse(w, map[string]interface{}{"code": 0, "message": "success"})
}

func progressHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		log.Warn("Task not found for progress", map[string]interface{}{"task_id": id})
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"percentage":    task.Progress,
			"keys_total":    task.KeysTotal,
			"keys_migrated": task.KeysMigrated,
			"speed":         task.Speed,
			"phase":         "full",
			"eta":           int64((100 - task.Progress) / 0.5 * 2),
		},
	})
}

func taskLogsHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if limit == 0 {
		limit = 100
	}

	filter := logger.LogFilter{
		TaskID:  id,
		Level:   q.Get("level"),
		Keyword: q.Get("keyword"),
		Offset:  offset,
		Limit:   limit,
	}

	entries := logger.Default().GetEntries(filter)
	total := logger.Default().GetTotalCount(filter)

	log.Debug("Task logs queried", map[string]interface{}{
		"task_id": id,
		"count":   len(entries),
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"items":  entries,
			"total":  total,
			"offset": offset,
			"limit":  limit,
		},
	})
}

// VerifyResult 验证结果
type VerifyResult struct {
	BatchID       string                 `json:"batch_id"`
	TaskID        string                 `json:"task_id"`
	Status        string                 `json:"status"`  // running, completed, failed
	StartTime     string                 `json:"start_time"`
	EndTime       string                 `json:"end_time,omitempty"`
	TotalKeys     int64                  `json:"total_keys"`
	SampledKeys   int64                  `json:"sampled_keys"`
	MatchedKeys   int64                  `json:"matched_keys"`
	MismatchKeys  int64                  `json:"mismatch_keys"`
	MissingKeys   int64                  `json:"missing_keys"`    // 源端有但目标端没有
	ExtraKeys     int64                  `json:"extra_keys"`      // 目标端有但源端没有（仅全量验证）
	TTLMismatch   int64                  `json:"ttl_mismatch"`    // TTL 不匹配
	ValueMismatch int64                  `json:"value_mismatch"`  // 值不匹配
	Details       []VerifyMismatchDetail `json:"details,omitempty"`
	SampleRate    float64                `json:"sample_rate"`     // 采样率 (0.0-1.0)
	VerifyMode    string                 `json:"verify_mode"`     // sample, full
}

// VerifyMismatchDetail 验证不匹配详情
type VerifyMismatchDetail struct {
	Key          string `json:"key"`
	Type         string `json:"type"`  // missing, extra, value_mismatch, ttl_mismatch
	SourceValue  string `json:"source_value,omitempty"`
	TargetValue  string `json:"target_value,omitempty"`
	SourceTTL    int64  `json:"source_ttl,omitempty"`
	TargetTTL    int64  `json:"target_ttl,omitempty"`
}

// 验证结果存储
var (
	verifyResults   = make(map[string]*VerifyResult) // batchID -> result
	verifyResultsMu sync.RWMutex
)

// ==================== 问题4修复: 大 Key 监控功能 ====================

// LargeKeyRecord 大 Key 记录
type LargeKeyRecord struct {
	Key       string `json:"key"`
	Size      int64  `json:"size"`       // 字节数
	Type      string `json:"type"`       // Key 类型
	Migrated  bool   `json:"migrated"`   // 是否迁移成功
	Timestamp string `json:"timestamp"`  // 记录时间
}

// 大 Key 存储（按任务 ID）
var (
	largeKeys   = make(map[string][]LargeKeyRecord) // taskID -> []LargeKeyRecord
	largeKeysMu sync.RWMutex
)

// recordLargeKey 记录大 Key（线程安全）
func recordLargeKey(taskID string, key string, size int64, keyType string, migrated bool) {
	largeKeysMu.Lock()
	defer largeKeysMu.Unlock()

	record := LargeKeyRecord{
		Key:       key,
		Size:      size,
		Type:      keyType,
		Migrated:  migrated,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	if largeKeys[taskID] == nil {
		largeKeys[taskID] = make([]LargeKeyRecord, 0)
	}

	// 最多保存 1000 条大 Key 记录
	if len(largeKeys[taskID]) < 1000 {
		largeKeys[taskID] = append(largeKeys[taskID], record)
	}
}

// getLargeKeys 获取任务的大 Key 列表
func getLargeKeys(taskID string) []LargeKeyRecord {
	largeKeysMu.RLock()
	defer largeKeysMu.RUnlock()

	if records, ok := largeKeys[taskID]; ok {
		result := make([]LargeKeyRecord, len(records))
		copy(result, records)
		return result
	}
	return nil
}

// getLargeKeyStats 获取大 Key 统计
func getLargeKeyStats(taskID string) map[string]interface{} {
	largeKeysMu.RLock()
	defer largeKeysMu.RUnlock()

	records := largeKeys[taskID]
	if len(records) == 0 {
		return map[string]interface{}{
			"count":         0,
			"total_size":    int64(0),
			"migrated":      0,
			"failed":        0,
		}
	}

	var totalSize int64
	var migrated, failed int
	for _, r := range records {
		totalSize += r.Size
		if r.Migrated {
			migrated++
		} else {
			failed++
		}
	}

	return map[string]interface{}{
		"count":         len(records),
		"total_size":    totalSize,
		"total_size_mb": float64(totalSize) / 1024 / 1024,
		"migrated":      migrated,
		"failed":        failed,
	}
}

func triggerVerifyHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 解析请求参数
	var req struct {
		Mode       string  `json:"mode"`        // sample（采样验证）或 full（全量验证），默认 sample
		SampleRate float64 `json:"sample_rate"` // 采样率，0.001-1.0，默认从任务配置获取
		MaxKeys    int64   `json:"max_keys"`    // 最大验证 Key 数量，默认 10000
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// 使用默认值
		req.Mode = "sample"
		req.MaxKeys = 10000
	}

	// 参数校验和默认值
	if req.Mode == "" {
		req.Mode = "sample"
	}
	
	// 优先使用请求参数，其次使用任务配置，最后使用默认值
	if req.SampleRate <= 0 || req.SampleRate > 1.0 {
		// 尝试从任务配置获取
		if task.Options != nil && task.Options.VerifyConfig != nil && task.Options.VerifyConfig.SampleRate > 0 {
			req.SampleRate = task.Options.VerifyConfig.SampleRate
		} else {
			req.SampleRate = 0.001 // 默认 0.1%
		}
	}
	
	if req.MaxKeys <= 0 {
		req.MaxKeys = 10000
	}

	batchID := uuid.New().String()
	
	// 创建验证结果记录
	result := &VerifyResult{
		BatchID:    batchID,
		TaskID:     id,
		Status:     "running",
		StartTime:  time.Now().Format(time.RFC3339),
		SampleRate: req.SampleRate,
		VerifyMode: req.Mode,
	}
	
	verifyResultsMu.Lock()
	verifyResults[batchID] = result
	verifyResultsMu.Unlock()

	log.Info("Verify triggered", map[string]interface{}{
		"task_id":     id,
		"batch_id":    batchID,
		"mode":        req.Mode,
		"sample_rate": req.SampleRate,
		"max_keys":    req.MaxKeys,
	})

	// 异步执行验证
	go runDataVerification(task, result, req.SampleRate, req.MaxKeys, log)

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "Verification started",
		"data": map[string]interface{}{
			"batch_id":    batchID,
			"mode":        req.Mode,
			"sample_rate": req.SampleRate,
			"max_keys":    req.MaxKeys,
		},
	})
}

// runDataVerification 执行数据验证
func runDataVerification(task *Task, result *VerifyResult, sampleRate float64, maxKeys int64, log *logger.RequestLogger) {
	ctx := context.Background()
	taskLog := logger.WithTask(task.ID)

	defer func() {
		result.EndTime = time.Now().Format(time.RFC3339)
		if result.Status == "running" {
			result.Status = "completed"
		}
		verifyResultsMu.Lock()
		verifyResults[result.BatchID] = result
		verifyResultsMu.Unlock()
	}()

	// 连接源端和目标端
	sourceAddrs := strings.Split(task.SourceCluster, ",")
	targetAddrs := strings.Split(task.TargetCluster, ",")
	for i := range sourceAddrs {
		sourceAddrs[i] = strings.TrimSpace(sourceAddrs[i])
	}
	for i := range targetAddrs {
		targetAddrs[i] = strings.TrimSpace(targetAddrs[i])
	}

	sourceClient, sourceIsCluster, err := connectRedis(ctx, sourceAddrs, task.SourcePassword)
	if err != nil {
		taskLog.Error("Verify: Failed to connect source", map[string]interface{}{"error": err.Error()})
		result.Status = "failed"
		return
	}
	defer sourceClient.Close()

	targetClient, _, err := connectRedis(ctx, targetAddrs, task.TargetPassword)
	if err != nil {
		taskLog.Error("Verify: Failed to connect target", map[string]interface{}{"error": err.Error()})
		result.Status = "failed"
		return
	}
	defer targetClient.Close()

	taskLog.Info("Verify: Starting data verification", map[string]interface{}{
		"batch_id":    result.BatchID,
		"sample_rate": sampleRate,
		"max_keys":    maxKeys,
	})

	// 采样验证逻辑
	var keysToVerify []string
	var scannedKeys int64

	// SCAN 获取 Key 并采样
	if sourceIsCluster {
		clusterClient := sourceClient.(*redis.ClusterClient)
		var mu sync.Mutex
		
		clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			var cursor uint64
			for {
				mu.Lock()
				currentSampled := int64(len(keysToVerify))
				mu.Unlock()
				
				if currentSampled >= maxKeys {
					return nil
				}

				keys, newCursor, err := node.Scan(ctx, cursor, "*", 1000).Result()
				if err != nil {
					return err
				}

				mu.Lock()
				for _, key := range keys {
					scannedKeys++
					// 根据采样率决定是否采样
					if rand.Float64() < sampleRate && int64(len(keysToVerify)) < maxKeys {
						// 检查是否匹配 Key 过滤器
						if matchKeyFilter(key, task.Options) {
							keysToVerify = append(keysToVerify, key)
						}
					}
				}
				mu.Unlock()

				cursor = newCursor
				if cursor == 0 {
					break
				}
			}
			return nil
		})
	} else {
		var cursor uint64
		for {
			if int64(len(keysToVerify)) >= maxKeys {
				break
			}

			keys, newCursor, err := sourceClient.Scan(ctx, cursor, "*", 1000).Result()
			if err != nil {
				taskLog.Error("Verify: SCAN failed", map[string]interface{}{"error": err.Error()})
				break
			}

			for _, key := range keys {
				scannedKeys++
				if rand.Float64() < sampleRate && int64(len(keysToVerify)) < maxKeys {
					if matchKeyFilter(key, task.Options) {
						keysToVerify = append(keysToVerify, key)
					}
				}
			}

			cursor = newCursor
			if cursor == 0 {
				break
			}
		}
	}

	result.TotalKeys = scannedKeys
	result.SampledKeys = int64(len(keysToVerify))

	taskLog.Info("Verify: Sampled keys", map[string]interface{}{
		"scanned": scannedKeys,
		"sampled": len(keysToVerify),
	})

	// 批量验证 Key
	const batchSize = 100
	var mismatches []VerifyMismatchDetail

	for i := 0; i < len(keysToVerify); i += batchSize {
		end := i + batchSize
		if end > len(keysToVerify) {
			end = len(keysToVerify)
		}
		batchKeys := keysToVerify[i:end]

		// Pipeline 获取源端数据
		sourcePipe := sourceClient.Pipeline()
		sourceTypeCmds := make([]*redis.StatusCmd, len(batchKeys))
		sourceTTLCmds := make([]*redis.DurationCmd, len(batchKeys))
		sourceDumpCmds := make([]*redis.StringCmd, len(batchKeys))

		for j, key := range batchKeys {
			sourceTypeCmds[j] = sourcePipe.Type(ctx, key)
			sourceTTLCmds[j] = sourcePipe.TTL(ctx, key)
			sourceDumpCmds[j] = sourcePipe.Dump(ctx, key)
		}
		sourcePipe.Exec(ctx)

		// Pipeline 获取目标端数据
		targetPipe := targetClient.Pipeline()
		targetExistsCmds := make([]*redis.IntCmd, len(batchKeys))
		targetTTLCmds := make([]*redis.DurationCmd, len(batchKeys))
		targetDumpCmds := make([]*redis.StringCmd, len(batchKeys))

		for j, key := range batchKeys {
			targetExistsCmds[j] = targetPipe.Exists(ctx, key)
			targetTTLCmds[j] = targetPipe.TTL(ctx, key)
			targetDumpCmds[j] = targetPipe.Dump(ctx, key)
		}
		targetPipe.Exec(ctx)

		// 比对结果
		for j, key := range batchKeys {
			sourceType, _ := sourceTypeCmds[j].Result()
			sourceTTL, _ := sourceTTLCmds[j].Result()
			sourceDump, sourceErr := sourceDumpCmds[j].Result()

			targetExists, _ := targetExistsCmds[j].Result()
			targetTTL, _ := targetTTLCmds[j].Result()
			targetDump, targetErr := targetDumpCmds[j].Result()

			// 源端 Key 不存在（可能已被删除）
			if sourceErr == redis.Nil || sourceType == "none" {
				continue
			}

			// 目标端 Key 不存在
			if targetExists == 0 || targetErr == redis.Nil {
				result.MissingKeys++
				if len(mismatches) < 100 { // 最多保存 100 条详情
					mismatches = append(mismatches, VerifyMismatchDetail{
						Key:  key,
						Type: "missing",
					})
				}
				continue
			}

			result.MatchedKeys++

			// 比较值
			if sourceDump != targetDump {
				result.ValueMismatch++
				if len(mismatches) < 100 {
					mismatches = append(mismatches, VerifyMismatchDetail{
						Key:         key,
						Type:        "value_mismatch",
						SourceValue: fmt.Sprintf("[%s] %d bytes", sourceType, len(sourceDump)),
						TargetValue: fmt.Sprintf("%d bytes", len(targetDump)),
					})
				}
				continue
			}

			// 比较 TTL（允许 5 秒误差）
			ttlDiff := sourceTTL - targetTTL
			if ttlDiff < 0 {
				ttlDiff = -ttlDiff
			}
			if ttlDiff > 5*time.Second && sourceTTL > 0 {
				result.TTLMismatch++
				if len(mismatches) < 100 {
					mismatches = append(mismatches, VerifyMismatchDetail{
						Key:       key,
						Type:      "ttl_mismatch",
						SourceTTL: int64(sourceTTL.Seconds()),
						TargetTTL: int64(targetTTL.Seconds()),
					})
				}
			}
		}
	}

	result.MismatchKeys = result.MissingKeys + result.ValueMismatch + result.TTLMismatch
	result.Details = mismatches

	taskLog.Info("Verify: Completed", map[string]interface{}{
		"batch_id":       result.BatchID,
		"sampled_keys":   result.SampledKeys,
		"matched_keys":   result.MatchedKeys,
		"missing_keys":   result.MissingKeys,
		"value_mismatch": result.ValueMismatch,
		"ttl_mismatch":   result.TTLMismatch,
	})
}

func verifyResultsHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	log.Debug("Verify results queried", map[string]interface{}{"task_id": id})

	// 获取该任务的所有验证结果
	var results []*VerifyResult
	verifyResultsMu.RLock()
	for _, result := range verifyResults {
		if result.TaskID == id {
			results = append(results, result)
		}
	}
	verifyResultsMu.RUnlock()

	// 按时间倒序排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].StartTime > results[j].StartTime
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    results,
	})
}

func simulateProgress(task *Task) {
	taskLog := logger.WithTask(task.ID)
	taskLog.Info("Migration started - connecting to clusters")

	ctx := context.Background()

	// 解析源端和目标端地址
	sourceAddrs := strings.Split(task.SourceCluster, ",")
	targetAddrs := strings.Split(task.TargetCluster, ",")

	for i := range sourceAddrs {
		sourceAddrs[i] = strings.TrimSpace(sourceAddrs[i])
	}
	for i := range targetAddrs {
		targetAddrs[i] = strings.TrimSpace(targetAddrs[i])
	}

	// 获取连接数配置
	sourcePoolSize := 50 // 默认50连接
	targetPoolSize := 50 // 默认50连接
	if task.Options != nil && task.Options.RateLimit != nil {
		if task.Options.RateLimit.SourceConnections > 0 {
			sourcePoolSize = task.Options.RateLimit.SourceConnections
		}
		if task.Options.RateLimit.TargetConnections > 0 {
			targetPoolSize = task.Options.RateLimit.TargetConnections
		}
	}

	taskLog.Info("Connection pool config", map[string]interface{}{
		"source_pool_size": sourcePoolSize,
		"target_pool_size": targetPoolSize,
	})

	// 尝试连接源端（使用配置的连接池大小）
	sourceClient, sourceIsCluster, err := connectRedisWithPoolSize(ctx, sourceAddrs, task.SourcePassword, sourcePoolSize)
	if err != nil {
		taskLog.Error("Failed to connect source cluster", map[string]interface{}{"error": err.Error()})
		tasksMu.Lock()
		task.Status = "failed"
		task.UpdatedAt = time.Now().Format(time.RFC3339)
		tasksMu.Unlock()
		return
	}
	defer sourceClient.Close()

	// 尝试连接目标端（使用配置的连接池大小）
	targetClient, targetIsCluster, err := connectRedisWithPoolSize(ctx, targetAddrs, task.TargetPassword, targetPoolSize)
	if err != nil {
		taskLog.Error("Failed to connect target cluster", map[string]interface{}{"error": err.Error()})
		tasksMu.Lock()
		task.Status = "failed"
		task.UpdatedAt = time.Now().Format(time.RFC3339)
		tasksMu.Unlock()
		return
	}
	defer targetClient.Close()

	taskLog.Info("Connected to clusters", map[string]interface{}{
		"source_mode": map[bool]string{true: "cluster", false: "standalone"}[sourceIsCluster],
		"target_mode": map[bool]string{true: "cluster", false: "standalone"}[targetIsCluster],
	})

	// ==================== 时间差校准检测 ====================
	// 检测源端和目标端的时间差，用于 TTL 精度告警
	checkClusterTimeSkew(ctx, sourceClient, targetClient, taskLog)

	// 记录Key过滤配置
	if task.Options != nil && task.Options.KeyFilter != nil {
		taskLog.Info("Key filter config", map[string]interface{}{
			"mode":             task.Options.KeyFilter.Mode,
			"prefixes":         task.Options.KeyFilter.Prefixes,
			"exclude_prefixes": task.Options.KeyFilter.ExcludePrefixes,
			"patterns":         task.Options.KeyFilter.Patterns,
		})
	}

	// 获取源端Key总数
	totalKeys, err := getDBSize(ctx, sourceClient, sourceIsCluster)
	if err != nil {
		taskLog.Warn("Failed to get source DB size, using estimate", map[string]interface{}{"error": err.Error()})
		totalKeys = 10000 // 默认估算值
	}

	tasksMu.Lock()
	task.KeysTotal = totalKeys
	task.BytesTotal = totalKeys * 256 // 估算平均每个key 256 bytes
	task.FullStartAt = time.Now().Format("2006-01-02 15:04:05")
	// 【问题1修复】开始新的全量迁移时，清空旧的增量开始时间
	// 避免断点恢复时显示不合理的时间（增量开始早于全量开始）
	task.IncrStartAt = ""
	tasksMu.Unlock()

	// ==================== 问题2修复: 检查 SkipFullSync 配置 ====================
	skipFullSync := task.Options != nil && task.Options.SkipFullSync
	skipIncremental := task.Options != nil && task.Options.SkipIncremental
	needIncremental := !skipIncremental && task.MigrationMode == "full_and_incremental"

	// ==================== 【关键修复】全量+增量模式下，先启动 FakeSlave ====================
	// 问题描述：之前的实现在全量完成后才启动 FakeSlave，导致全量期间的数据变更丢失
	// 解决方案：
	// 1. 全量开始前，先启动 FakeSlave 并设置 CacheMode=true
	// 2. 全量期间，FakeSlave 将接收到的 Binlog 缓存到本地文件
	// 3. 全量完成后，先回放缓存的 Binlog，再切换到实时同步
	var fakeSlaves []*replication.FakeSlave
	var cacheManager *replication.BinlogCacheManager
	var binlogCtx context.Context
	var binlogCancel context.CancelFunc

	if needIncremental && !skipFullSync {
		// 检查是否支持 INCRSYNC 协议
		if checkTendisIncrSyncSupport(ctx, sourceClient, taskLog) {
			taskLog.Info("【关键】Starting FakeSlave BEFORE full migration to capture all changes")
			
			// 创建 Binlog 缓存管理器
			cacheConfig := replication.BinlogCacheConfig{
				CacheDir:    "./data/binlog_cache",
				TaskID:      task.ID,
				MaxFileSize: 1 << 30, // 1GB 单文件上限
			}
			cacheManager = replication.NewBinlogCacheManager(cacheConfig)
			cacheManager.StartCaching() // 开启缓存模式
			
			// 保存到 task
			tasksMu.Lock()
			task.cacheManager = cacheManager
			task.IncrSyncMode = "binlog"
			tasksMu.Unlock()
			
			// 启动 FakeSlave（缓存模式）
			binlogCtx, binlogCancel = context.WithCancel(ctx)
			fakeSlaves, err = startFakeSlavesWithCache(binlogCtx, task, sourceClient, targetClient, sourceIsCluster, cacheManager, taskLog)
			if err != nil {
				taskLog.Error("Failed to start FakeSlaves, will use time-window mode", map[string]interface{}{
					"error": err.Error(),
				})
				binlogCancel()
				cacheManager = nil
				fakeSlaves = nil
			} else {
				taskLog.Info("FakeSlaves started in cache mode, binlog will be captured during full migration", map[string]interface{}{
					"node_count": len(fakeSlaves),
				})
			}
		} else {
			taskLog.Warn("Tendis INCRSYNC not supported, will use time-window mode for incremental sync")
			tasksMu.Lock()
			task.IncrSyncMode = "time_window"
			tasksMu.Unlock()
		}
	}

	// ==================== 执行全量迁移 ====================
	if skipFullSync {
		taskLog.Info("Full migration SKIPPED (skip_full_sync=true)", map[string]interface{}{
			"total_keys": totalKeys,
		})
		// 直接标记全量阶段完成
		tasksMu.Lock()
		task.Phase = "full_skipped"
		task.Progress = 100
		tasksMu.Unlock()
	} else {
		taskLog.Info("Starting full migration", map[string]interface{}{
			"total_keys": totalKeys,
			"binlog_caching": cacheManager != nil,
		})

		// 执行全量迁移
		doFullMigration(ctx, task, sourceClient, targetClient, sourceIsCluster, targetIsCluster, taskLog)
	}

	// 检查是否需要增量迁移
	tasksMu.RLock()
	status := task.Status
	mode := task.MigrationMode
	tasksMu.RUnlock()

	// ==================== 问题2修复: 检查 SkipIncremental 配置 ====================
	if skipIncremental {
		taskLog.Info("Incremental sync SKIPPED (skip_incremental=true)")
		// 如果跳过增量，停止 FakeSlave 并清理
		if binlogCancel != nil {
			binlogCancel()
		}
		if cacheManager != nil {
			cacheManager.Close()
			cacheManager.CleanupCache()
		}
		// 停止所有 FakeSlave
		for _, fs := range fakeSlaves {
			fs.Stop()
		}
		if status == "running" {
			tasksMu.Lock()
			task.Status = "completed"
			task.Phase = "completed"
			task.Progress = 100
			task.UpdatedAt = time.Now().Format(time.RFC3339)
			tasksMu.Unlock()
			taskLog.Info("Task completed (incremental skipped)")
		}
		return
	}

	if status == "running" && mode == "full_and_incremental" {
		taskLog.Info("Starting incremental sync phase")
		tasksMu.Lock()
		task.Phase = "incremental"
		task.IncrStartAt = time.Now().Format("2006-01-02 15:04:05")
		tasksMu.Unlock()
		
		// 根据是否有 FakeSlave 选择同步模式
		if cacheManager != nil && len(fakeSlaves) > 0 {
			// ==================== 【关键】回放缓存的 Binlog ====================
			taskLog.Info("Stopping cache mode, preparing to replay cached binlogs")
			cacheManager.StopCaching() // 停止缓存，后续 Binlog 直接应用
			
			// 获取缓存统计
			cacheStats := cacheManager.GetAllStats()
			var totalCached int64
			for storeID, stats := range cacheStats {
				totalCached += stats["total_records"]
				taskLog.Info("Cache stats for store", map[string]interface{}{
					"store_id":      storeID,
					"total_records": stats["total_records"],
					"total_bytes":   stats["total_bytes"],
					"files_created": stats["files_created"],
				})
			}
			
			if totalCached > 0 {
				taskLog.Info("Replaying cached binlogs captured during full migration", map[string]interface{}{
					"total_cached": totalCached,
				})
				
				// 回放缓存的 Binlog
				for _, fs := range fakeSlaves {
					cacheConfig := replication.BinlogCacheConfig{
						CacheDir: "./data/binlog_cache",
						TaskID:   task.ID,
					}
					if err := fs.ReplayCachedBinlogs(ctx, cacheConfig); err != nil {
						taskLog.Error("Failed to replay cached binlogs", map[string]interface{}{
							"error": err.Error(),
						})
					}
				}
				taskLog.Info("Cached binlog replay completed")
			} else {
				taskLog.Info("No cached binlogs to replay")
			}
			
			// 清理缓存文件（已回放完成）
			if err := cacheManager.CleanupCache(); err != nil {
				taskLog.Warn("Failed to cleanup cache", map[string]interface{}{"error": err.Error()})
			}
			
			// ==================== 切换到实时 Binlog 同步 ====================
			taskLog.Info("Switching to real-time binlog sync mode")
			
			// 保存 FakeSlaves 到 task
			tasksMu.Lock()
			task.fakeSlaves = fakeSlaves
			tasksMu.Unlock()
			
			// 等待 FakeSlave 完成或被取消（实时同步模式）
			waitForFakeSlaves(binlogCtx, binlogCancel, task, fakeSlaves, taskLog)
		} else {
			// 使用时间窗口模式（非 Tendis 或 FakeSlave 启动失败）
			taskLog.Info("Using time-window mode for incremental sync (Tendis INCRSYNC not available)")
			doIncrementalSync(ctx, task, sourceClient, targetClient, sourceIsCluster, targetIsCluster, taskLog)
		}
	} else {
		// 不需要增量同步，清理资源
		if binlogCancel != nil {
			binlogCancel()
		}
		if cacheManager != nil {
			cacheManager.Close()
			cacheManager.CleanupCache()
		}
		for _, fs := range fakeSlaves {
			fs.Stop()
		}
	}
}

// connectRedis 连接Redis，返回通用客户端接口
func connectRedis(ctx context.Context, addrs []string, password string) (redis.UniversalClient, bool, error) {
	return connectRedisWithPoolSize(ctx, addrs, password, 0)
}

// ==================== 时间差校准检测 ====================

// checkClusterTimeSkew 检测源端和目标端的时间差
// 如果时间差超过阈值，记录警告日志
func checkClusterTimeSkew(ctx context.Context, sourceClient, targetClient redis.UniversalClient, taskLog *logger.TaskLogger) {
	// 获取源端时间
	sourceTime, err := getRedisTime(ctx, sourceClient)
	if err != nil {
		taskLog.Warn("Failed to get source cluster time", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// 获取目标端时间
	targetTime, err := getRedisTime(ctx, targetClient)
	if err != nil {
		taskLog.Warn("Failed to get target cluster time", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// 计算时间差
	skew := sourceTime.Sub(targetTime)
	if skew < 0 {
		skew = -skew
	}

	// 记录时间信息
	taskLog.Info("Cluster time check", map[string]interface{}{
		"source_time":  sourceTime.Format("2006-01-02 15:04:05"),
		"target_time":  targetTime.Format("2006-01-02 15:04:05"),
		"time_skew_ms": skew.Milliseconds(),
	})

	// 时间差超过 5 秒，发出警告
	if skew > 5*time.Second {
		taskLog.Warn("⚠️ Large time skew detected between source and target clusters", map[string]interface{}{
			"time_skew":   skew.String(),
			"source_time": sourceTime.Format("2006-01-02 15:04:05"),
			"target_time": targetTime.Format("2006-01-02 15:04:05"),
			"impact":      "TTL precision may be affected, keys may expire earlier/later than expected",
			"suggestion":  "Please ensure NTP is configured correctly on both clusters",
		})
	} else if skew > 1*time.Second {
		taskLog.Info("Moderate time skew detected (acceptable)", map[string]interface{}{
			"time_skew": skew.String(),
		})
	} else {
		taskLog.Info("Time synchronization is good", map[string]interface{}{
			"time_skew": skew.String(),
		})
	}
}

// getRedisTime 获取 Redis 服务器时间
func getRedisTime(ctx context.Context, client redis.UniversalClient) (time.Time, error) {
	result, err := client.Time(ctx).Result()
	if err != nil {
		return time.Time{}, err
	}
	return result, nil
}

// connectRedisWithPoolSize 连接Redis，支持自定义连接池大小
func connectRedisWithPoolSize(ctx context.Context, addrs []string, password string, poolSize int) (redis.UniversalClient, bool, error) {
	// 设置默认连接池大小
	if poolSize <= 0 {
		poolSize = 10 // 默认10个连接
	}
	
	// 先尝试集群模式
	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        addrs,
		Password:     password,
		PoolSize:     poolSize,           // 每个节点的连接池大小
		MinIdleConns: poolSize / 4,       // 最小空闲连接数
		PoolTimeout:  30 * time.Second,   // 等待连接的超时时间
	})
	if err := clusterClient.Ping(ctx).Err(); err == nil {
		return clusterClient, true, nil
	}
	clusterClient.Close()

	// 尝试单机模式
	standaloneClient := redis.NewClient(&redis.Options{
		Addr:         addrs[0],
		Password:     password,
		PoolSize:     poolSize,           // 连接池大小
		MinIdleConns: poolSize / 4,       // 最小空闲连接数
		PoolTimeout:  30 * time.Second,   // 等待连接的超时时间
	})
	if err := standaloneClient.Ping(ctx).Err(); err != nil {
		standaloneClient.Close()
		return nil, false, err
	}
	return standaloneClient, false, nil
}

// getDBSize 获取数据库Key数量
func getDBSize(ctx context.Context, client redis.UniversalClient, isCluster bool) (int64, error) {
	if !isCluster {
		return client.DBSize(ctx).Result()
	}

	// 集群模式需要遍历所有节点
	clusterClient := client.(*redis.ClusterClient)
	var total int64
	var mu sync.Mutex

	err := clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		size, err := node.DBSize(ctx).Result()
		if err != nil {
			return err
		}
		mu.Lock()
		total += size
		mu.Unlock()
		return nil
	})
	return total, err
}

// WorkerInfo Worker信息
type WorkerInfo struct {
	ID            int
	StopChan      chan struct{}       // 停止信号
	StoppedChan   chan struct{}       // 已停止确认
	ProcessingKey string              // 当前正在处理的Key（用于日志）
	IsProcessing  int32               // 是否正在处理Key（原子操作）
	CreatedAt     time.Time           // 创建时间（用于LIFO）
}

// DynamicWorkerPool 动态Worker池，支持运行时调整Worker数量
type DynamicWorkerPool struct {
	task           *Task
	ctx            context.Context
	keyChan        chan string
	wg             sync.WaitGroup
	mu             sync.RWMutex
	activeWorkers  int32                    // 当前活跃Worker数量
	targetWorkers  int32                    // 目标Worker数量
	workers        []*WorkerInfo            // Worker列表（有序，用于LIFO）
	nextWorkerID   int                      // 下一个Worker ID
	taskLog        *logger.TaskLogger
	
	// 迁移相关参数
	sourceClient   redis.UniversalClient
	targetClient   redis.UniversalClient
	conflictPolicy string
	rateLimiter    *RateLimiter             // 源端限速器
	targetLimiter  *RateLimiter             // 目标端限速器
	rateLimiterMu  sync.RWMutex             // 限速器锁（支持动态更新）
	shadowMode     bool                     // 影子模式：只读取不写入
	// processedKeys 已移除：40 亿 Key 场景下会导致 OOM（80-150 GB 内存）
	// Redis SCAN 返回重复 Key 是正常的，重复迁移不影响数据正确性
	
	// 统计计数器
	migratedCount     *int64
	migratedBytes     *int64
	failedCount       *int64
	skippedCount      *int64
	filteredCount     *int64
	largeKeyThreshold int64  // 大 Key 阈值（问题4修复）
	
	// 影子模式统计
	shadowStats      *ShadowModeStats
	shadowStatsMu    sync.Mutex
}

// NewDynamicWorkerPool 创建动态Worker池
// 注意：已移除 processedKeys 参数，40 亿 Key 场景下 sync.Map 会导致 OOM
func NewDynamicWorkerPool(ctx context.Context, task *Task, keyChan chan string, taskLog *logger.TaskLogger,
	sourceClient, targetClient redis.UniversalClient, conflictPolicy string, 
	sourceLimiter, targetLimiter *RateLimiter,
	migratedCount, migratedBytes, failedCount, skippedCount, filteredCount *int64) *DynamicWorkerPool {
	
	// 获取大 Key 阈值配置
	var largeKeyThreshold int64 = 10 * 1024 * 1024 // 默认 10MB
	if task.Options != nil && task.Options.LargeKeyThreshold > 0 {
		largeKeyThreshold = task.Options.LargeKeyThreshold
	}

	// 检查是否为影子模式
	shadowMode := false
	var shadowStats *ShadowModeStats
	if task.Options != nil && task.Options.ShadowMode {
		shadowMode = true
		shadowStats = &ShadowModeStats{
			TypeDistribution: make(map[string]int64),
		}
		taskLog.Info("Shadow mode enabled: data will be read but NOT written to target", nil)
	}

	return &DynamicWorkerPool{
		task:              task,
		ctx:               ctx,
		keyChan:           keyChan,
		workers:           make([]*WorkerInfo, 0),
		nextWorkerID:      0,
		taskLog:           taskLog,
		sourceClient:      sourceClient,
		targetClient:      targetClient,
		conflictPolicy:    conflictPolicy,
		rateLimiter:       sourceLimiter,
		targetLimiter:     targetLimiter,
		shadowMode:        shadowMode,
		shadowStats:       shadowStats,
		migratedCount:     migratedCount,
		migratedBytes:     migratedBytes,
		failedCount:       failedCount,
		skippedCount:      skippedCount,
		filteredCount:     filteredCount,
		largeKeyThreshold: largeKeyThreshold,
	}
}

// SetWorkerCount 动态调整Worker数量
func (p *DynamicWorkerPool) SetWorkerCount(count int) {
	atomic.StoreInt32(&p.targetWorkers, int32(count))
}

// GetActiveWorkerCount 获取当前活跃Worker数量
func (p *DynamicWorkerPool) GetActiveWorkerCount() int {
	return int(atomic.LoadInt32(&p.activeWorkers))
}

// UpdateRateLimiter 动态更新源端限速器
func (p *DynamicWorkerPool) UpdateRateLimiter(newQPS int) {
	p.rateLimiterMu.Lock()
	defer p.rateLimiterMu.Unlock()
	
	oldQPS := 0
	if p.rateLimiter != nil {
		oldQPS = p.rateLimiter.qps
	}
	
	// QPS 没有变化，无需更新
	if oldQPS == newQPS {
		return
	}
	
	// 停止旧的限速器
	if p.rateLimiter != nil {
		p.rateLimiter.Stop()
		p.rateLimiter = nil
	}
	
	// 创建新的限速器（如果 QPS > 0）
	if newQPS > 0 {
		p.rateLimiter = NewRateLimiter(newQPS)
	}
	
	p.taskLog.Info("Source rate limiter updated dynamically", map[string]interface{}{
		"old_qps": oldQPS,
		"new_qps": newQPS,
	})
}

// UpdateTargetRateLimiter 动态更新目标端限速器
func (p *DynamicWorkerPool) UpdateTargetRateLimiter(newQPS int) {
	p.rateLimiterMu.Lock()
	defer p.rateLimiterMu.Unlock()
	
	oldQPS := 0
	if p.targetLimiter != nil {
		oldQPS = p.targetLimiter.qps
	}
	
	// QPS 没有变化，无需更新
	if oldQPS == newQPS {
		return
	}
	
	// 停止旧的限速器
	if p.targetLimiter != nil {
		p.targetLimiter.Stop()
		p.targetLimiter = nil
	}
	
	// 创建新的限速器（如果 QPS > 0）
	if newQPS > 0 {
		p.targetLimiter = NewRateLimiter(newQPS)
	}
	
	p.taskLog.Info("Target rate limiter updated dynamically", map[string]interface{}{
		"old_qps": oldQPS,
		"new_qps": newQPS,
	})
}

// GetTargetRateLimiter 获取目标端限速器（线程安全）
func (p *DynamicWorkerPool) GetTargetRateLimiter() *RateLimiter {
	p.rateLimiterMu.RLock()
	defer p.rateLimiterMu.RUnlock()
	return p.targetLimiter
}

// GetRateLimiter 获取当前限速器（线程安全）
func (p *DynamicWorkerPool) GetRateLimiter() *RateLimiter {
	p.rateLimiterMu.RLock()
	defer p.rateLimiterMu.RUnlock()
	return p.rateLimiter
}

// Start 启动指定数量的Worker
func (p *DynamicWorkerPool) Start(initialCount int) {
	atomic.StoreInt32(&p.targetWorkers, int32(initialCount))
	for i := 0; i < initialCount; i++ {
		p.addWorker()
	}
}

// addWorker 添加一个新Worker
func (p *DynamicWorkerPool) addWorker() {
	p.mu.Lock()
	workerID := p.nextWorkerID
	p.nextWorkerID++
	
	workerInfo := &WorkerInfo{
		ID:          workerID,
		StopChan:    make(chan struct{}),
		StoppedChan: make(chan struct{}),
		CreatedAt:   time.Now(),
	}
	p.workers = append(p.workers, workerInfo)
	p.mu.Unlock()
	
	atomic.AddInt32(&p.activeWorkers, 1)
	p.wg.Add(1)
	
	go p.workerLoop(workerInfo)
	
	p.taskLog.Info("Worker started", map[string]interface{}{
		"worker_id":      workerID,
		"active_workers": atomic.LoadInt32(&p.activeWorkers),
	})
}

// removeWorkerSmart 智能选择Worker停止：优先空闲Worker，其次LIFO（优雅停止）
func (p *DynamicWorkerPool) removeWorkerSmart() bool {
	p.mu.Lock()
	
	if len(p.workers) == 0 {
		p.mu.Unlock()
		return false
	}
	
	// 策略：优先找空闲的Worker（从后往前找，保持LIFO倾向）
	var targetIdx = -1
	var selectionReason string
	
	for i := len(p.workers) - 1; i >= 0; i-- {
		if atomic.LoadInt32(&p.workers[i].IsProcessing) == 0 {
			targetIdx = i
			selectionReason = "idle"
			break
		}
	}
	
	// 如果没有空闲的，选最后一个（LIFO兜底）
	if targetIdx == -1 {
		targetIdx = len(p.workers) - 1
		selectionReason = "LIFO (all busy)"
	}
	
	// 移除选中的Worker
	workerInfo := p.workers[targetIdx]
	p.workers = append(p.workers[:targetIdx], p.workers[targetIdx+1:]...)
	p.mu.Unlock()
	
	// 记录Worker当前状态
	isProcessing := atomic.LoadInt32(&workerInfo.IsProcessing) == 1
	processingKey := workerInfo.ProcessingKey
	
	p.taskLog.Info("Stopping worker (smart selection)", map[string]interface{}{
		"worker_id":        workerInfo.ID,
		"selection_reason": selectionReason,
		"is_processing":    isProcessing,
		"processing_key":   processingKey,
		"created_at":       workerInfo.CreatedAt.Format("15:04:05"),
	})
	
	// 发送停止信号
	close(workerInfo.StopChan)
	
	// 等待Worker确认停止（空闲Worker应该立即停止，忙碌Worker需要等待）
	timeout := 5 * time.Second
	if isProcessing {
		timeout = 30 * time.Second // 忙碌Worker给更长时间
	}
	
	select {
	case <-workerInfo.StoppedChan:
		p.taskLog.Info("Worker stopped confirmed", map[string]interface{}{
			"worker_id":        workerInfo.ID,
			"selection_reason": selectionReason,
		})
	case <-time.After(timeout):
		p.taskLog.Warn("Worker stop timeout, force continue", map[string]interface{}{
			"worker_id": workerInfo.ID,
			"timeout":   timeout.String(),
		})
	}
	
	return true
}

// workerLoop Worker的主循环（批量模式：收集多个Key后使用Pipeline批量处理）
// 性能优化：从 4N 次网络往返 → 3 次网络往返，提升 10-20 倍性能
func (p *DynamicWorkerPool) workerLoop(info *WorkerInfo) {
	defer func() {
		atomic.AddInt32(&p.activeWorkers, -1)
		p.wg.Done()
		// 发送已停止确认
		close(info.StoppedChan)
		p.taskLog.Debug("Worker exited", map[string]interface{}{
			"worker_id":      info.ID,
			"active_workers": atomic.LoadInt32(&p.activeWorkers),
		})
	}()
	
	shouldStop := false
	
	// 批量处理配置
	// 性能优化：进一步增大批量大小以最大化 Pipeline 效率
	// 注意：批量大小过大可能导致单次 Pipeline 超时或内存压力
	const batchSize = 1000                       // 每批处理 1000 个 Key
	const batchTimeout = 200 * time.Millisecond  // 收集超时时间
	
	keyBatch := make([]string, 0, batchSize)
	
	for {
		// 先检查是否收到停止信号（非阻塞）
		select {
		case <-info.StopChan:
			shouldStop = true
		default:
		}
		
		// 如果已收到停止信号且当前没有在处理Key，则退出
		if shouldStop && atomic.LoadInt32(&info.IsProcessing) == 0 && len(keyBatch) == 0 {
			p.taskLog.Debug("Worker stopping gracefully (no pending work)", map[string]interface{}{
				"worker_id": info.ID,
			})
			return
		}
		
		// 收集一批 Key
		collectStart := time.Now()
		for len(keyBatch) < batchSize {
			// 检查是否超时
			if time.Since(collectStart) > batchTimeout && len(keyBatch) > 0 {
				break // 有 Key 且超时，开始处理
			}
			
			select {
			case <-info.StopChan:
				shouldStop = true
				if len(keyBatch) == 0 {
					p.taskLog.Debug("Worker stopping gracefully (stop signal received)", map[string]interface{}{
						"worker_id": info.ID,
					})
					return
				}
				// 有待处理的 Key，继续处理完
				goto processBatch
				
			case key, ok := <-p.keyChan:
				if !ok {
					// 通道关闭，处理剩余 Key 后退出
					if len(keyBatch) > 0 {
						goto processBatch
					}
					return
				}
				
				// 如果已标记停止，将key放回通道让其他worker处理
				if shouldStop && len(keyBatch) == 0 {
					select {
					case p.keyChan <- key:
					default:
					}
					return
				}
				
				keyBatch = append(keyBatch, key)
				
			case <-time.After(10 * time.Millisecond):
				// 短超时，检查是否需要处理已收集的 Key
				if len(keyBatch) > 0 && time.Since(collectStart) > batchTimeout {
					goto processBatch
				}
				continue
			}
		}
		
	processBatch:
		if len(keyBatch) == 0 {
			continue
		}
		
		// 标记正在处理
		atomic.StoreInt32(&info.IsProcessing, 1)
		info.ProcessingKey = fmt.Sprintf("batch[%d keys]", len(keyBatch))
		
		// 批量处理 Key（使用 Pipeline）
		p.processKeyBatch(info.ID, keyBatch)
		
		// 清空批次
		keyBatch = keyBatch[:0]
		
		// 标记处理完成
		info.ProcessingKey = ""
		atomic.StoreInt32(&info.IsProcessing, 0)
		
		// 处理完成后检查是否需要停止
		if shouldStop {
			p.taskLog.Info("Worker completed batch and stopping", map[string]interface{}{
				"worker_id": info.ID,
			})
			return
		}
	}
}

// processKeyBatch 批量处理 Key（使用 Pipeline，高性能模式）
// 性能优化：100 个 Key 批量处理，从 400 次网络往返 → 3 次网络往返
func (p *DynamicWorkerPool) processKeyBatch(workerID int, keys []string) {
	if len(keys) == 0 {
		return
	}
	
	// 检查任务状态
	tasksMu.RLock()
	status := p.task.Status
	keyFilter := p.task.Options.KeyFilter
	tasksMu.RUnlock()
	if status != "running" {
		return
	}

	// 批量模式下的限速优化：只等待一次，不逐个等待
	// 因为 Pipeline 已经是批量操作，逐个等待会严重降低吞吐量
	// 如果设置了 QPS 限速，这里按批次大小进行粗粒度控制
	if rl := p.GetRateLimiter(); rl != nil {
		rl.Wait() // 仅等待一次令牌
	}

	// 影子模式：只读取源端数据，不写入目标端
	if p.shadowMode {
		scanned, matched, skippedKeys, bytesRead, largeKeys, typeDistribution := p.processShadowBatch(keys, keyFilter)
		
		// 更新统计（影子模式下 migrated 表示成功读取的 Key）
		atomic.AddInt64(p.migratedCount, matched)
		atomic.AddInt64(p.migratedBytes, bytesRead)
		atomic.AddInt64(p.skippedCount, skippedKeys)
		atomic.AddInt64(p.filteredCount, scanned-matched-skippedKeys)
		
		// 更新影子模式统计
		p.shadowStatsMu.Lock()
		if p.shadowStats != nil {
			p.shadowStats.KeysScanned += scanned
			p.shadowStats.KeysMatched += matched
			p.shadowStats.KeysSkipped += skippedKeys
			p.shadowStats.BytesRead += bytesRead
			p.shadowStats.LargeKeysFound += largeKeys
			for k, v := range typeDistribution {
				p.shadowStats.TypeDistribution[k] += v
			}
			// 计算平均 Key 大小
			if p.shadowStats.KeysMatched > 0 {
				p.shadowStats.AvgKeySize = p.shadowStats.BytesRead / p.shadowStats.KeysMatched
			}
		}
		p.shadowStatsMu.Unlock()
		return
	}

	// 正常模式：写入目标端
	if tl := p.GetTargetRateLimiter(); tl != nil {
		tl.Wait() // 仅等待一次令牌
	}

	// 使用 Pipeline 批量迁移（问题4修复：添加任务ID和大Key阈值）
	migrated, skipped, failed, filtered, totalBytes := MigrateBatchWithPipelineAndFilter(
		p.ctx, p.sourceClient, p.targetClient, keys, p.conflictPolicy, keyFilter,
		p.task.ID, p.largeKeyThreshold,
	)
	
	// 更新统计计数
	atomic.AddInt64(p.migratedCount, migrated)
	atomic.AddInt64(p.migratedBytes, totalBytes)
	atomic.AddInt64(p.skippedCount, skipped)
	atomic.AddInt64(p.failedCount, failed)
	atomic.AddInt64(p.filteredCount, filtered)
	
	// 记录目标端成功/失败（用于自动暂停检测）
	// 批量模式优化：只有当失败比例超过 50% 时才记录为连续失败
	// 因为批量处理中部分 key 失败是正常的（可能是 SCAN 期间被删除）
	totalProcessed := migrated + skipped + failed
	if totalProcessed > 0 {
		failureRatio := float64(failed) / float64(totalProcessed)
		if failureRatio < 0.5 {
			// 失败比例低于 50%，记录为成功批次
			recordTargetSuccess(p.task.ID)
		} else {
			// 失败比例高于 50%，可能有严重问题，记录失败
			shouldPause := recordTargetFailure(p.task.ID, p.taskLog)
			if shouldPause {
				saveErrorKeysToFile(p.task.ID)
				autoStopTask(p.task.ID, "Too many consecutive target failures", p.taskLog)
				return
			}
		}
	}
}

// processShadowBatch 影子模式批量处理：只读取源端数据进行分析，不写入目标端
// 返回：扫描数、匹配数、跳过数、读取字节数、大 Key 数、类型分布
func (p *DynamicWorkerPool) processShadowBatch(keys []string, keyFilter *KeyFilter) (scanned, matched, skippedKeys, bytesRead, largeKeys int64, typeDistribution map[string]int64) {
	typeDistribution = make(map[string]int64)
	scanned = int64(len(keys))
	
	// 先过滤 Key
	filteredKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if matchKeyFilterV2(key, keyFilter) {
			filteredKeys = append(filteredKeys, key)
		}
	}
	
	if len(filteredKeys) == 0 {
		return
	}
	
	// Pipeline 批量获取 Key 类型
	typePipe := p.sourceClient.Pipeline()
	typeCmds := make([]*redis.StatusCmd, len(filteredKeys))
	for i, key := range filteredKeys {
		typeCmds[i] = typePipe.Type(p.ctx, key)
	}
	typePipe.Exec(p.ctx)
	
	// Pipeline 批量 DUMP（获取序列化数据大小）
	dumpPipe := p.sourceClient.Pipeline()
	dumpCmds := make([]*redis.StringCmd, len(filteredKeys))
	for i, key := range filteredKeys {
		dumpCmds[i] = dumpPipe.Dump(p.ctx, key)
	}
	dumpPipe.Exec(p.ctx)
	
	// 统计结果
	for i, key := range filteredKeys {
		// 获取类型
		keyType := "unknown"
		if typeCmds[i] != nil && typeCmds[i].Err() == nil {
			keyType = typeCmds[i].Val()
		}
		
		// 获取大小
		var keySize int64 = 0
		if dumpCmds[i] != nil && dumpCmds[i].Err() == nil {
			data := dumpCmds[i].Val()
			keySize = int64(len(data))
			bytesRead += keySize
			matched++
			typeDistribution[keyType]++
			
			// 检查是否为大 Key
			if keySize >= p.largeKeyThreshold {
				largeKeys++
				// 记录大 Key（影子模式）
				recordLargeKey(p.task.ID, key, keySize, "shadow_scan", true)
			}
		} else {
			// Key 可能已被删除
			skippedKeys++
		}
	}
	
	return
}

// GetShadowStats 获取影子模式统计（线程安全）
func (p *DynamicWorkerPool) GetShadowStats() *ShadowModeStats {
	p.shadowStatsMu.Lock()
	defer p.shadowStatsMu.Unlock()
	if p.shadowStats == nil {
		return nil
	}
	// 返回副本
	stats := &ShadowModeStats{
		KeysScanned:      p.shadowStats.KeysScanned,
		KeysMatched:      p.shadowStats.KeysMatched,
		KeysSkipped:      p.shadowStats.KeysSkipped,
		BytesRead:        p.shadowStats.BytesRead,
		LargeKeysFound:   p.shadowStats.LargeKeysFound,
		AvgKeySize:       p.shadowStats.AvgKeySize,
		TypeDistribution: make(map[string]int64),
	}
	for k, v := range p.shadowStats.TypeDistribution {
		stats.TypeDistribution[k] = v
	}
	return stats
}

// processKey 处理单个Key的迁移（保留用于兼容）
func (p *DynamicWorkerPool) processKey(workerID int, key string) {
	// 检查任务状态
	tasksMu.RLock()
	status := p.task.Status
	tasksMu.RUnlock()
	if status != "running" {
		return
	}

	// 注意：已移除 processedKeys 检查
	// 原因：40 亿 Key 场景下，sync.Map 存储所有 Key 会导致 OOM（80-150 GB）
	// Redis SCAN 返回重复 Key 是正常的，重复迁移不影响数据正确性：
	// - replace 模式：覆盖，结果正确
	// - skip 模式：跳过已存在的 Key，结果正确

	// 源端限速（读取操作）
	if rl := p.GetRateLimiter(); rl != nil {
		rl.Wait()
	}

	// 检查Key是否匹配过滤规则
	if !matchKeyFilter(key, p.task.Options) {
		atomic.AddInt64(p.filteredCount, 1)
		return
	}

	// 迁移Key（带重试机制）
	var migrated bool
	var bytes int64
	var reason string
	maxRetries, fullIntervalMs, _ := getRetryConfig(p.task.Options)
	
	for retry := 0; retry < maxRetries; retry++ {
		// 目标端限速（写入操作）- 在每次重试前都进行限速
		if tl := p.GetTargetRateLimiter(); tl != nil {
			tl.Wait()
		}
		
		migrated, bytes, reason = migrateKeyWithPolicy(p.ctx, p.sourceClient, p.targetClient, key, p.conflictPolicy)
		if migrated || reason == "skipped" || reason == "filtered" || reason == "" {
			// 成功，重置目标端失败计数
			recordTargetSuccess(p.task.ID)
			break
		}
		
		// 检测是否是目标端错误
		if strings.Contains(reason, "RESTORE") || strings.Contains(reason, "target") {
			shouldPause := recordTargetFailure(p.task.ID, p.taskLog)
			if shouldPause {
				// 保存状态后自动暂停
				saveErrorKeysToFile(p.task.ID)
				autoStopTask(p.task.ID, "Too many consecutive target failures", p.taskLog)
				return
			}
		}
		
		if retry < maxRetries-1 {
			time.Sleep(time.Duration((retry+1)*fullIntervalMs) * time.Millisecond)
		}
	}

	if migrated {
		atomic.AddInt64(p.migratedCount, 1)
		atomic.AddInt64(p.migratedBytes, bytes)
	} else if reason == "skipped" {
		atomic.AddInt64(p.skippedCount, 1)
	} else if reason == "filtered" {
		atomic.AddInt64(p.filteredCount, 1)
	} else {
		atomic.AddInt64(p.failedCount, 1)
		addErrorKey(p.task.ID, key, "string", "failed", reason+" (after "+fmt.Sprintf("%d", maxRetries)+" retries)")
		
		// 定期保存错误 key（每 100 个失败保存一次）
		failedCount := atomic.LoadInt64(p.failedCount)
		if failedCount%100 == 0 {
			go saveErrorKeysToFile(p.task.ID)
		}
	}
}

// AdjustWorkers 调整Worker数量到目标值
func (p *DynamicWorkerPool) AdjustWorkers() {
	target := int(atomic.LoadInt32(&p.targetWorkers))
	current := int(atomic.LoadInt32(&p.activeWorkers))
	
	if target > current {
		// 需要增加Worker
		toAdd := target - current
		for i := 0; i < toAdd; i++ {
			p.addWorker()
		}
		p.taskLog.Info("Workers increased", map[string]interface{}{
			"from":  current,
			"to":    target,
			"added": toAdd,
		})
	} else if target < current {
		// 需要减少Worker（智能选择：优先空闲，其次LIFO）
		toRemove := current - target
		for i := 0; i < toRemove; i++ {
			p.removeWorkerSmart()
		}
		p.taskLog.Info("Workers decreased (smart selection)", map[string]interface{}{
			"from":    current,
			"to":      target,
			"removed": toRemove,
		})
	}
}

// Wait 等待所有Worker完成
func (p *DynamicWorkerPool) Wait() {
	p.wg.Wait()
}

// StopAll 停止所有Worker
func (p *DynamicWorkerPool) StopAll() {
	p.mu.Lock()
	workers := make([]*WorkerInfo, len(p.workers))
	copy(workers, p.workers)
	p.workers = p.workers[:0]
	p.mu.Unlock()
	
	// 并行发送停止信号
	for _, w := range workers {
		close(w.StopChan)
	}
	
	// 等待所有Worker确认停止
	for _, w := range workers {
		select {
		case <-w.StoppedChan:
		case <-time.After(30 * time.Second):
			p.taskLog.Warn("Worker stop timeout during StopAll", map[string]interface{}{
				"worker_id": w.ID,
			})
		}
	}
}

// GetWorkerStatus 获取所有Worker的状态（用于监控）
func (p *DynamicWorkerPool) GetWorkerStatus() []map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	status := make([]map[string]interface{}, len(p.workers))
	for i, w := range p.workers {
		status[i] = map[string]interface{}{
			"id":            w.ID,
			"created_at":    w.CreatedAt.Format("2006-01-02 15:04:05"),
			"is_processing": atomic.LoadInt32(&w.IsProcessing) == 1,
			"current_key":   w.ProcessingKey,
		}
	}
	return status
}

// doFullMigration 执行全量迁移（并行Worker模式 - 支持动态调整）
func doFullMigration(ctx context.Context, task *Task, sourceClient, targetClient redis.UniversalClient, sourceIsCluster, targetIsCluster bool, taskLog *logger.TaskLogger) {
	// 获取配置参数
	batchSize := int64(1000)
	workerCount := 4
	var sourceLimiter *RateLimiter
	var targetLimiter *RateLimiter

	if task.Options != nil {
		if task.Options.ScanBatchSize > 0 {
			batchSize = int64(task.Options.ScanBatchSize)
		}
		if task.Options.WorkerCount > 0 {
			workerCount = task.Options.WorkerCount
		}
		// 初始化源端限速器
		if task.Options.RateLimit != nil && task.Options.RateLimit.SourceQPS > 0 {
			sourceLimiter = NewRateLimiter(task.Options.RateLimit.SourceQPS)
		}
		// 初始化目标端限速器
		if task.Options.RateLimit != nil && task.Options.RateLimit.TargetQPS > 0 {
			targetLimiter = NewRateLimiter(task.Options.RateLimit.TargetQPS)
		}
	}

	// 获取冲突策略
	conflictPolicy := "skip_full_only"
	if task.Options != nil && task.Options.ConflictPolicy != "" {
		conflictPolicy = task.Options.ConflictPolicy
	}

	// 获取QPS配置用于日志
	sourceQPS := 0
	targetQPS := 0
	if task.Options != nil && task.Options.RateLimit != nil {
		sourceQPS = task.Options.RateLimit.SourceQPS
		targetQPS = task.Options.RateLimit.TargetQPS
	}

	taskLog.Info("Starting parallel migration with dynamic worker pool", map[string]interface{}{
		"worker_count": workerCount,
		"batch_size":   batchSize,
		"policy":       conflictPolicy,
		"source_qps":   sourceQPS,
		"target_qps":   targetQPS,
	})

	// 统计计数器（使用原子操作）
	var migratedCount int64
	var migratedBytes int64
	var failedCount int64
	var skippedCount int64
	var filteredCount int64
	startTime := time.Now()
	lastLogTime := time.Now()
	var lastLogMu sync.Mutex

	// 创建滑动窗口速度追踪器
	speedTracker := NewSpeedTracker(20) // 使用20个采样点（约10秒窗口）

	// 已移除 processedKeys：40 亿 Key 场景下 sync.Map 会导致 OOM（80-150 GB 内存）
	// Redis SCAN 返回重复 Key 是正常的，重复迁移（replace 覆盖 / skip 跳过）不影响正确性

	// 创建Key通道（缓冲区大小动态调整）
	keyChan := make(chan string, workerCount*100)

	// 创建动态Worker池（无 processedKeys 参数）
	workerPool := NewDynamicWorkerPool(ctx, task, keyChan, taskLog,
		sourceClient, targetClient, conflictPolicy, sourceLimiter, targetLimiter,
		&migratedCount, &migratedBytes, &failedCount, &skippedCount, &filteredCount)
	
	// 启动初始Worker
	workerPool.Start(workerCount)
	
	// 注册Worker池和速度追踪器到任务（用于动态调整和实时速度）
	tasksMu.Lock()
	task.workerPool = workerPool
	task.speedTracker = speedTracker
	tasksMu.Unlock()

	// 进度更新协程（包含Worker动态调整检查和实时速度计算）
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopProgress:
				return
			case <-ticker.C:
				mc := atomic.LoadInt64(&migratedCount)
				mb := atomic.LoadInt64(&migratedBytes)
				fc := atomic.LoadInt64(&failedCount)
				sc := atomic.LoadInt64(&skippedCount)
				ftc := atomic.LoadInt64(&filteredCount)
				
				// 记录速度采样点
				speedTracker.Record(mc, mb)
				
				// 检查是否需要动态调整配置
				tasksMu.RLock()
				targetWorkerCount := 4
				targetSourceQPS := 0
				targetTargetQPS := 0
				if task.Options != nil {
					if task.Options.WorkerCount > 0 {
						targetWorkerCount = task.Options.WorkerCount
					}
					if task.Options.RateLimit != nil {
						targetSourceQPS = task.Options.RateLimit.SourceQPS
						targetTargetQPS = task.Options.RateLimit.TargetQPS
					}
				}
				tasksMu.RUnlock()
				
				// 动态调整Worker
				workerPool.SetWorkerCount(targetWorkerCount)
				workerPool.AdjustWorkers()
				
				// 动态调整源端QPS限速器
				workerPool.UpdateRateLimiter(targetSourceQPS)
				
				// 动态调整目标端QPS限速器
				workerPool.UpdateTargetRateLimiter(targetTargetQPS)
				
				currentWorkers := workerPool.GetActiveWorkerCount()
				
				// 使用滑动窗口计算实时速度
				realTimeSpeed := speedTracker.GetSpeed()
				bytesSpeed := speedTracker.GetBytesSpeed()

				tasksMu.Lock()
				task.KeysMigrated = mc
				task.BytesMigrated = mb
				task.KeysFailed = fc
				task.KeysSkipped = sc
				task.KeysFiltered = ftc
				task.ActiveWorkers = currentWorkers  // 更新当前活跃Worker数
				
				// 动态调整 KeysTotal：确保 KeysTotal >= 已处理Key数量
				// 修复：初始 DBSIZE 可能不准确，且增量阶段会有新 Key
				processedKeys := mc + sc + ftc
				if processedKeys > task.KeysTotal {
					task.KeysTotal = processedKeys
					task.BytesTotal = processedKeys * 256 // 同步更新估算字节数
				}
				
				if task.KeysTotal > 0 {
					task.Progress = float64(processedKeys) / float64(task.KeysTotal) * 100
					if task.Progress > 100 {
						task.Progress = 100
					}
				}
				// 使用实时速度（滑动窗口），而不是平均速度
				task.Speed = realTimeSpeed
				task.UpdatedAt = time.Now().Format(time.RFC3339)
				tasksMu.Unlock()

				// 每10秒记录一次详细日志（包含性能分析信息）
				lastLogMu.Lock()
				if time.Since(lastLogTime) > 10*time.Second {
					elapsed := time.Since(startTime).Seconds()
					avgSpeed := int64(0)
					if elapsed > 0 {
						avgSpeed = int64(float64(mc) / elapsed)
					}
					
					taskLog.Info("Migration progress", map[string]interface{}{
						"progress":        fmt.Sprintf("%.1f%%", task.Progress),
						"migrated_keys":   mc,
						"failed_keys":     fc,
						"skipped_keys":    sc,
						"filtered_keys":   ftc,
						"realtime_speed":  realTimeSpeed,
						"average_speed":   avgSpeed,
						"bytes_speed_mb":  fmt.Sprintf("%.2f MB/s", float64(bytesSpeed)/1024/1024),
						"active_workers":  currentWorkers,
						"target_workers":  targetWorkerCount,
						"elapsed":         fmt.Sprintf("%.0fs", elapsed),
						"speed_per_worker": realTimeSpeed / int64(max(currentWorkers, 1)),
					})
					lastLogTime = time.Now()
				}
				lastLogMu.Unlock()
			}
		}
	}()

	// ==================== Key 清单模式 ====================
	// 如果配置了 Key 清单文件，则从清单迁移，不使用 SCAN
	if task.Options != nil && task.Options.KeyListFile != "" {
		taskLog.Info("Key list mode: loading keys from file", map[string]interface{}{
			"file": task.Options.KeyListFile,
		})

		keyList, err := LoadKeyListFromFile(task.Options.KeyListFile)
		if err != nil {
			taskLog.Error("Failed to load key list file", map[string]interface{}{
				"error": err.Error(),
			})
			tasksMu.Lock()
			task.Status = "failed"
			tasksMu.Unlock()
			close(keyChan)
			workerPool.Wait()
			close(stopProgress)
			return
		}

		taskLog.Info("Key list loaded successfully", map[string]interface{}{
			"total_keys": keyList.TotalCount,
			"format":     keyList.Format,
			"source":     keyList.Source,
		})

		// 更新任务总 Key 数
		tasksMu.Lock()
		task.KeysTotal = int64(keyList.TotalCount)
		tasksMu.Unlock()

		// 验证 Key 在源端是否存在（可选，默认验证）
		taskLog.Info("Validating keys exist in source...", nil)
		existingKeys, missingKeys := ValidateKeyListInSource(ctx, sourceClient, keyList.Keys, 1000)
		taskLog.Info("Key validation completed", map[string]interface{}{
			"existing_keys": len(existingKeys),
			"missing_keys":  len(missingKeys),
		})

		if len(missingKeys) > 0 && len(missingKeys) <= 100 {
			taskLog.Warn("Some keys not found in source", map[string]interface{}{
				"count":        len(missingKeys),
				"missing_keys": missingKeys,
			})
		} else if len(missingKeys) > 100 {
			taskLog.Warn("Many keys not found in source", map[string]interface{}{
				"count": len(missingKeys),
				"first_10": missingKeys[:10],
			})
		}

		// 更新实际需要迁移的 Key 数
		tasksMu.Lock()
		task.KeysTotal = int64(len(existingKeys))
		tasksMu.Unlock()

		// 分发 Key 到 Worker
		go func() {
			defer close(keyChan)
			for _, key := range existingKeys {
				tasksMu.RLock()
				status := task.Status
				tasksMu.RUnlock()
				if status != "running" {
					return
				}
				keyChan <- key
			}
			taskLog.Info("All keys from list dispatched to workers", map[string]interface{}{
				"total": len(existingKeys),
			})
		}()

		// 等待所有 Worker 完成
		workerPool.Wait()
		close(stopProgress)

		taskLog.Info("Key list migration completed", map[string]interface{}{
			"migrated": atomic.LoadInt64(&migratedCount),
			"failed":   atomic.LoadInt64(&failedCount),
			"skipped":  atomic.LoadInt64(&skippedCount),
		})
		return
	}

	// ==================== SCAN 模式（默认）====================
	// SCAN并分发Key到Worker
	// 辅助函数：动态获取批次大小
	getBatchSize := func() int64 {
		tasksMu.RLock()
		defer tasksMu.RUnlock()
		if task.Options != nil && task.Options.ScanBatchSize > 0 {
			return int64(task.Options.ScanBatchSize)
		}
		return 1000
	}

	// 加载已有的全量断点
	existingCheckpoint := loadFullSyncCheckpoint(task.ID)
	if existingCheckpoint != nil && !existingCheckpoint.IsComplete {
		taskLog.Info("Resuming from existing checkpoint", map[string]interface{}{
			"processed_keys": existingCheckpoint.ProcessedKeys,
			"node_cursors":   len(existingCheckpoint.NodeCursors),
		})
	}

	// 初始化全量断点
	fullCheckpoint := &FullSyncCheckpoint{
		TaskID:      task.ID,
		NodeCursors: make(map[string]uint64),
		StartTime:   time.Now().Format(time.RFC3339),
		Phase:       "full",
	}
	if existingCheckpoint != nil && !existingCheckpoint.IsComplete {
		fullCheckpoint = existingCheckpoint
	}

	// 断点保存计数器
	var scannedKeysCount int64
	checkpointSaveInterval := int64(10000) // 每扫描 10000 个 key 保存一次断点
	lastCheckpointSave := time.Now()
	
	// 【优化】获取 SCAN MATCH 模式 - 利用服务端过滤
	// 评审建议：SCAN MATCH 是服务端过滤，40亿 Key 场景下可大幅减少网络传输
	var scanMatchPattern string
	if task.Options != nil {
		scanMatchPattern = getScanMatchPattern(task.Options.KeyFilter)
	} else {
		scanMatchPattern = "*"
	}
	
	if scanMatchPattern != "*" {
		taskLog.Info("🚀 Using server-side SCAN MATCH filter (optimized)", map[string]interface{}{
			"pattern": scanMatchPattern,
			"benefit": "Reduces network transfer, only matching keys returned",
		})
	}
	
	if sourceIsCluster {
		// 集群模式：并行遍历所有master节点
		clusterClient := sourceClient.(*redis.ClusterClient)
		var scanWg sync.WaitGroup
		var nodeCursorsMu sync.Mutex

		clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			scanWg.Add(1)
			go func(nodeClient *redis.Client) {
				defer scanWg.Done()
				
				// 获取节点地址
				nodeAddr := nodeClient.Options().Addr
				
				// 从断点恢复 cursor（支持前缀维度）
				var cursor uint64
				nodeCursorsMu.Lock()
				checkpointKey := getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)
				if savedCursor, ok := fullCheckpoint.NodeCursors[checkpointKey]; ok {
					cursor = savedCursor
					taskLog.Info("Resuming node scan from cursor", map[string]interface{}{
						"node":         nodeAddr,
						"cursor":       cursor,
						"match_pattern": scanMatchPattern,
					})
				} else if savedCursor, ok := fullCheckpoint.NodeCursors[nodeAddr]; ok {
					// 兼容旧断点格式
					cursor = savedCursor
					taskLog.Info("Resuming node scan from cursor (legacy)", map[string]interface{}{
						"node":   nodeAddr,
						"cursor": cursor,
					})
				}
				nodeCursorsMu.Unlock()
				
				consecutiveScanFailures := 0
				
				for {
					tasksMu.RLock()
					status := task.Status
					tasksMu.RUnlock()
					if status != "running" {
						// 保存当前 cursor 再退出（使用前缀维度的 key）
						nodeCursorsMu.Lock()
						fullCheckpoint.NodeCursors[getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)] = cursor
						fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
						nodeCursorsMu.Unlock()
						saveFullSyncCheckpoint(task.ID, fullCheckpoint)
						return
					}

					// 动态获取批次大小
					currentBatchSize := getBatchSize()
					// 【优化】使用 SCAN MATCH 服务端过滤
					keys, newCursor, err := nodeClient.Scan(ctx, cursor, scanMatchPattern, currentBatchSize).Result()
					if err != nil {
						consecutiveScanFailures++
						taskLog.Warn("SCAN failed on node", map[string]interface{}{
							"error":                err.Error(),
							"node":                 nodeAddr,
							"consecutive_failures": consecutiveScanFailures,
						})
						
						// 检查是否需要自动暂停
						if consecutiveScanFailures >= MaxConsecutiveFailures {
							shouldPause := recordSourceFailure(task.ID, taskLog)
							if shouldPause {
								// 保存断点后暂停
								nodeCursorsMu.Lock()
								fullCheckpoint.NodeCursors[nodeAddr] = cursor
								fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
								nodeCursorsMu.Unlock()
								saveFullSyncCheckpoint(task.ID, fullCheckpoint)
								saveErrorKeysToFile(task.ID)
								autoStopTask(task.ID, "Too many consecutive source failures", taskLog)
								return
							}
						}
						
						time.Sleep(time.Second)
						continue
					}
					
					// 成功，重置失败计数
					consecutiveScanFailures = 0
					recordSourceSuccess(task.ID)

					for _, key := range keys {
						tasksMu.RLock()
						status := task.Status
						tasksMu.RUnlock()
						if status != "running" {
							// 保存断点再退出（使用前缀维度 key）
							nodeCursorsMu.Lock()
							fullCheckpoint.NodeCursors[getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)] = cursor
							fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
							nodeCursorsMu.Unlock()
							saveFullSyncCheckpoint(task.ID, fullCheckpoint)
							return
						}
						keyChan <- key
					}

					cursor = newCursor
					atomic.AddInt64(&scannedKeysCount, int64(len(keys)))
					
					// 定期保存断点（每 10000 个 key 或每 30 秒）- 使用前缀维度 key
					currentScanned := atomic.LoadInt64(&scannedKeysCount)
					if currentScanned%checkpointSaveInterval == 0 || time.Since(lastCheckpointSave) > 30*time.Second {
						nodeCursorsMu.Lock()
						fullCheckpoint.NodeCursors[getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)] = cursor
						fullCheckpoint.TotalScannedKeys = currentScanned
						fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
						nodeCursorsMu.Unlock()
						saveFullSyncCheckpoint(task.ID, fullCheckpoint)
						lastCheckpointSave = time.Now()
					}
					
					if cursor == 0 {
						// 该节点扫描完成
						nodeCursorsMu.Lock()
						fullCheckpoint.NodeCursors[getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)] = 0 // 标记完成
						fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
						nodeCursorsMu.Unlock()
						break
					}
				}
			}(node)
			return nil
		})

		scanWg.Wait()
	} else {
		// 单机模式
		nodeAddr := "standalone"
		
		// 从断点恢复 cursor（支持前缀维度）
		var cursor uint64
		checkpointKey := getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)
		if savedCursor, ok := fullCheckpoint.NodeCursors[checkpointKey]; ok {
			cursor = savedCursor
			taskLog.Info("Resuming scan from cursor", map[string]interface{}{
				"cursor":        cursor,
				"match_pattern": scanMatchPattern,
			})
		} else if savedCursor, ok := fullCheckpoint.NodeCursors[nodeAddr]; ok {
			// 兼容旧断点格式
			cursor = savedCursor
			taskLog.Info("Resuming scan from cursor (legacy)", map[string]interface{}{
				"cursor": cursor,
			})
		}
		
		consecutiveScanFailures := 0
		
		for {
			tasksMu.RLock()
			status := task.Status
			tasksMu.RUnlock()
			if status != "running" {
				// 保存断点再退出（使用前缀维度 key）
				fullCheckpoint.NodeCursors[checkpointKey] = cursor
				fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
				saveFullSyncCheckpoint(task.ID, fullCheckpoint)
				break
			}

			// 动态获取批次大小
			currentBatchSize := getBatchSize()
			// 【优化】使用 SCAN MATCH 服务端过滤
			keys, newCursor, err := sourceClient.Scan(ctx, cursor, scanMatchPattern, currentBatchSize).Result()
			if err != nil {
				consecutiveScanFailures++
				taskLog.Error("SCAN failed", map[string]interface{}{
					"error":                err.Error(),
					"consecutive_failures": consecutiveScanFailures,
				})
				
				// 检查是否需要自动暂停
				if consecutiveScanFailures >= MaxConsecutiveFailures {
					shouldPause := recordSourceFailure(task.ID, taskLog)
					if shouldPause {
						fullCheckpoint.NodeCursors[checkpointKey] = cursor
						fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
						saveFullSyncCheckpoint(task.ID, fullCheckpoint)
						saveErrorKeysToFile(task.ID)
						autoStopTask(task.ID, "Too many consecutive source failures", taskLog)
						break
					}
				}
				
				time.Sleep(time.Second)
				continue
			}
			
			// 成功，重置失败计数
			consecutiveScanFailures = 0
			recordSourceSuccess(task.ID)

			for _, key := range keys {
				tasksMu.RLock()
				status := task.Status
				tasksMu.RUnlock()
				if status != "running" {
					// 保存断点再退出（使用前缀维度 key）
					fullCheckpoint.NodeCursors[checkpointKey] = cursor
					fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
					saveFullSyncCheckpoint(task.ID, fullCheckpoint)
					break
				}
				keyChan <- key
			}

			cursor = newCursor
			atomic.AddInt64(&scannedKeysCount, int64(len(keys)))
			
			// 定期保存断点（使用前缀维度 key）
			currentScanned := atomic.LoadInt64(&scannedKeysCount)
			if currentScanned%checkpointSaveInterval == 0 || time.Since(lastCheckpointSave) > 30*time.Second {
				fullCheckpoint.NodeCursors[checkpointKey] = cursor
				fullCheckpoint.TotalScannedKeys = currentScanned
				fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
				saveFullSyncCheckpoint(task.ID, fullCheckpoint)
				lastCheckpointSave = time.Now()
			}
			
			if cursor == 0 {
				break
			}
		}
	}

	// 关闭通道，等待所有Worker完成
	close(keyChan)
	workerPool.Wait()
	close(stopProgress)
	
	// 清理Worker池引用
	tasksMu.Lock()
	task.workerPool = nil
	tasksMu.Unlock()

	// 标记全量完成
	markFullSyncComplete(task.ID)
	
	// 保存错误 key
	saveErrorKeysToFile(task.ID)

	// 最终更新统计
	mc := atomic.LoadInt64(&migratedCount)
	fc := atomic.LoadInt64(&failedCount)
	sc := atomic.LoadInt64(&skippedCount)
	ftc := atomic.LoadInt64(&filteredCount)

	tasksMu.Lock()
	task.KeysMigrated = mc
	task.KeysFailed = fc
	task.KeysSkipped = sc
	task.KeysFiltered = ftc
	
	// 【修复】计算待迁移 Key 数 = 符合过滤条件的 Key（已迁移 + 失败）
	// 注意：KeysSkipped 是"目标端已存在而跳过的 Key"，不是需要迁移的 Key
	// 真正"待迁移"的是：SCAN 到的总数 - 被过滤的 = 已迁移 + 失败 + 冲突跳过
	// 但从用户视角，"待迁移"指的是"需要新迁移"，所以不包含冲突跳过的
	// 这里使用：全量 SCAN 匹配数 = 已迁移 + 失败 + 冲突跳过（不含被前缀过滤的）
	task.KeysToMigrate = mc + fc + sc
	
	// 修复：全量完成时，用实际处理的 Key 数量更新 KeysTotal
	// 这样可以确保 KeysTotal 准确反映实际处理的数量
	processedKeys := mc + sc + ftc
	if processedKeys > task.KeysTotal {
		task.KeysTotal = processedKeys
		task.BytesTotal = processedKeys * 256
	}
	
	if task.Status == "running" {
		if task.MigrationMode == "full_only" {
			task.Status = "completed"
			task.Progress = 100
			task.Phase = "completed"
		} else {
			// 全量迁移完成，准备进入增量同步
			task.Progress = 100
			task.Phase = "incremental"
		}
	}
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	tasksMu.Unlock()

	taskLog.Info("Full migration completed", map[string]interface{}{
		"migrated_keys": mc,
		"failed_keys":   fc,
		"skipped_keys":  sc,
		"filtered_keys": ftc,
		"duration":      time.Since(startTime).String(),
		"avg_speed":     int64(float64(mc) / time.Since(startTime).Seconds()),
	})
}

// RateLimiter 简单的限速器
type RateLimiter struct {
	qps      int                // QPS值（用于比较是否需要更新）
	ticker   *time.Ticker
	tokens   chan struct{}
	stopChan chan struct{}
}

// NewRateLimiter 创建限速器
func NewRateLimiter(qps int) *RateLimiter {
	if qps <= 0 {
		return nil
	}
	interval := time.Second / time.Duration(qps)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}

	rl := &RateLimiter{
		qps:      qps,
		ticker:   time.NewTicker(interval),
		tokens:   make(chan struct{}, qps),
		stopChan: make(chan struct{}),
	}

	// 预填充tokens
	for i := 0; i < qps/10+1; i++ {
		select {
		case rl.tokens <- struct{}{}:
		default:
		}
	}

	// 持续填充tokens
	go func() {
		for {
			select {
			case <-rl.stopChan:
				return
			case <-rl.ticker.C:
				select {
				case rl.tokens <- struct{}{}:
				default:
				}
			}
		}
	}()

	return rl
}

// Wait 等待获取令牌
func (rl *RateLimiter) Wait() {
	if rl == nil {
		return
	}
	<-rl.tokens
}

// Stop 停止限速器
func (rl *RateLimiter) Stop() {
	if rl == nil {
		return
	}
	close(rl.stopChan)
	rl.ticker.Stop()
}

// matchKeyFilter 检查Key是否匹配过滤规则
func matchKeyFilter(key string, options *TaskOptions) bool {
	if options == nil || options.KeyFilter == nil {
		return true
	}

	filter := options.KeyFilter
	switch filter.Mode {
	case "prefix":
		// 检查排除前缀
		for _, prefix := range filter.ExcludePrefixes {
			if strings.HasPrefix(key, prefix) {
				return false
			}
		}
		// 如果设置了包含前缀，只迁移匹配的
		if len(filter.Prefixes) > 0 {
			for _, prefix := range filter.Prefixes {
				if strings.HasPrefix(key, prefix) {
					return true
				}
			}
			return false
		}
		return true
	case "pattern":
		// 正则匹配（简化实现，使用 strings.Contains）
		if len(filter.Patterns) > 0 {
			for _, pattern := range filter.Patterns {
				if strings.Contains(key, pattern) {
					return true
				}
			}
			return false
		}
		return true
	default:
		return true
	}
}

// migrateKeyWithPolicy 根据冲突策略迁移Key
func migrateKeyWithPolicy(ctx context.Context, sourceClient, targetClient redis.UniversalClient, key string, policy string) (bool, int64, string) {
	// 获取Key的TTL
	ttl, err := sourceClient.TTL(ctx, key).Result()
	if err != nil {
		return false, 0, "get TTL failed: " + err.Error()
	}

	// 使用DUMP+RESTORE迁移
	dump, err := sourceClient.Dump(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, 0, "skipped" // Key不存在，跳过
		}
		return false, 0, "dump failed: " + err.Error()
	}

	bytes := int64(len(dump))

	// 检查目标是否存在
	exists, err := targetClient.Exists(ctx, key).Result()
	if err != nil {
		return false, 0, "check exists failed: " + err.Error()
	}

	if exists > 0 {
		switch policy {
		case "skip", "skip_full_only":
			return false, 0, "skipped"
		case "replace":
			// 先删除目标Key
			if err := targetClient.Del(ctx, key).Err(); err != nil {
				return false, 0, "delete failed: " + err.Error()
			}
		case "error":
			return false, 0, "key conflict"
		default:
			return false, 0, "skipped"
		}
	}

	// RESTORE到目标
	if ttl < 0 {
		ttl = 0 // 无过期时间
	}
	err = targetClient.Restore(ctx, key, ttl, dump).Err()
	if err != nil {
		return false, 0, "restore failed: " + err.Error()
	}

	return true, bytes, ""
}

// ==================== P2 改进: Pipeline 批量 DUMP/RESTORE ====================

// PipelineMigrateResult 批量迁移结果
type PipelineMigrateResult struct {
	Key       string
	Migrated  bool
	Bytes     int64
	Reason    string
}

// MigrateBatchWithPipeline 使用 Pipeline 批量迁移 Key（P2 改进：提高迁移效率）
// 对比单个迁移：减少网络往返次数，提高吞吐量
func MigrateBatchWithPipeline(ctx context.Context, sourceClient, targetClient redis.UniversalClient, keys []string, policy string) []PipelineMigrateResult {
	if len(keys) == 0 {
		return nil
	}

	results := make([]PipelineMigrateResult, len(keys))
	for i, key := range keys {
		results[i] = PipelineMigrateResult{Key: key}
	}

	// 阶段 1: 批量 DUMP（从源端获取数据）
	sourcePipe := sourceClient.Pipeline()
	ttlCmds := make([]*redis.DurationCmd, len(keys))
	dumpCmds := make([]*redis.StringCmd, len(keys))

	for i, key := range keys {
		ttlCmds[i] = sourcePipe.TTL(ctx, key)
		dumpCmds[i] = sourcePipe.Dump(ctx, key)
	}

	_, err := sourcePipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		// Pipeline 执行失败，所有 Key 都标记为失败
		for i := range results {
			results[i].Reason = "source pipeline failed: " + err.Error()
		}
		return results
	}

	// 收集 DUMP 结果
	type dumpResult struct {
		TTL  time.Duration
		Data string
	}
	dumpResults := make(map[int]*dumpResult)

	for i := range keys {
		ttl, _ := ttlCmds[i].Result()
		dump, err := dumpCmds[i].Result()
		if err != nil {
			if err == redis.Nil {
				results[i].Reason = "skipped" // Key 不存在
			} else {
				results[i].Reason = "dump failed: " + err.Error()
			}
			continue
		}

		if ttl < 0 {
			ttl = 0
		}
		dumpResults[i] = &dumpResult{TTL: ttl, Data: dump}
		results[i].Bytes = int64(len(dump))
	}

	// 阶段 2: 批量检查目标端是否存在（对于 skip 策略）
	if policy == "skip" || policy == "skip_full_only" {
		targetPipe := targetClient.Pipeline()
		existsCmds := make([]*redis.IntCmd, len(keys))

		for i, key := range keys {
			if dumpResults[i] != nil { // 只检查有数据的 Key
				existsCmds[i] = targetPipe.Exists(ctx, key)
			}
		}

		targetPipe.Exec(ctx)

		// 处理 skip 策略
		for i := range keys {
			if dumpResults[i] == nil {
				continue
			}
			if existsCmds[i] != nil {
				exists, _ := existsCmds[i].Result()
				if exists > 0 {
					results[i].Reason = "skipped"
					delete(dumpResults, i) // 移除，不需要 RESTORE
				}
			}
		}
	} else if policy == "replace" {
		// replace 策略：使用 RESTORE REPLACE 选项，无需先删除
		// RESTORE REPLACE 是 Redis 3.0+ 支持的原子操作
	}

	// 阶段 3: 批量 RESTORE 到目标端
	restorePipe := targetClient.Pipeline()
	restoreCmds := make([]*redis.StatusCmd, len(keys))

	for i, key := range keys {
		if dr, ok := dumpResults[i]; ok {
			if policy == "replace" {
				// 使用 RESTORE REPLACE 原子替换
				restoreCmds[i] = restorePipe.RestoreReplace(ctx, key, dr.TTL, dr.Data)
			} else {
				restoreCmds[i] = restorePipe.Restore(ctx, key, dr.TTL, dr.Data)
			}
		}
	}

	_, err = restorePipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		// 部分失败，需要检查每个命令的结果
		for i := range keys {
			if restoreCmds[i] != nil {
				if err := restoreCmds[i].Err(); err != nil {
					results[i].Reason = "restore failed: " + err.Error()
				} else {
					results[i].Migrated = true
				}
			}
		}
	} else {
		// 全部成功
		for i := range keys {
			if restoreCmds[i] != nil && results[i].Reason == "" {
				results[i].Migrated = true
			}
		}
	}

	return results
}

// MigrateBatchWithPipelineAndFilter 带过滤的批量迁移
// 问题4修复：添加 taskID 和 largeKeyThreshold 参数用于大 Key 监控
func MigrateBatchWithPipelineAndFilter(ctx context.Context, sourceClient, targetClient redis.UniversalClient, keys []string, policy string, keyFilter *KeyFilter, taskID string, largeKeyThreshold int64) (migrated, skipped, failed, filtered int64, totalBytes int64) {
	// 先过滤 Key
	filteredKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if matchKeyFilterV2(key, keyFilter) {
			filteredKeys = append(filteredKeys, key)
		} else {
			filtered++
		}
	}

	if len(filteredKeys) == 0 {
		return
	}

	// 默认大 Key 阈值 10MB
	if largeKeyThreshold <= 0 {
		largeKeyThreshold = 10 * 1024 * 1024
	}

	// 批量迁移
	results := MigrateBatchWithPipeline(ctx, sourceClient, targetClient, filteredKeys, policy)

	for _, r := range results {
		if r.Migrated {
			migrated++
			totalBytes += r.Bytes

			// 大 Key 监控：记录超过阈值的 Key
			if r.Bytes >= largeKeyThreshold && taskID != "" {
				recordLargeKey(taskID, r.Key, r.Bytes, "migrated", true)
			}
		} else if r.Reason == "skipped" {
			skipped++
		} else if r.Reason != "" {
			failed++
			// 【问题2修复】记录详细的失败原因
			if taskID != "" {
				addErrorKey(taskID, r.Key, "unknown", "failed", r.Reason+" (batch pipeline)")
			}
			// 记录迁移失败的大 Key
			if r.Bytes >= largeKeyThreshold && taskID != "" {
				recordLargeKey(taskID, r.Key, r.Bytes, "failed", false)
			}
		}
	}

	return
}

// matchKeyFilterV2 检查 Key 是否匹配过滤规则（支持 KeyFilter 结构）
func matchKeyFilterV2(key string, filter *KeyFilter) bool {
	if filter == nil {
		return true
	}

	// 检查排除前缀
	for _, prefix := range filter.ExcludePrefixes {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}

	// 检查模式
	switch filter.Mode {
	case "all", "":
		return true
	case "prefix":
		if len(filter.Prefixes) == 0 {
			return true
		}
		for _, prefix := range filter.Prefixes {
			if strings.HasPrefix(key, prefix) {
				return true
			}
		}
		return false
	case "pattern":
		// 简单模式匹配（支持 * 通配符）
		if len(filter.Patterns) == 0 {
			return true
		}
		for _, pattern := range filter.Patterns {
			if matchSimplePattern(key, pattern) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

// matchSimplePattern 简单模式匹配（支持 * 通配符）
func matchSimplePattern(key, pattern string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(key, pattern[:len(pattern)-1])
	}
	if strings.HasPrefix(pattern, "*") {
		return strings.HasSuffix(key, pattern[1:])
	}
	return key == pattern
}

// migrateKey 迁移单个Key（保留旧函数供兼容）
func migrateKey(ctx context.Context, sourceClient, targetClient redis.UniversalClient, key string) (bool, int64, string) {
	return migrateKeyWithPolicy(ctx, sourceClient, targetClient, key, "skip_full_only")
}

// scanAllKeys 扫描所有key（支持集群模式）
func scanAllKeys(ctx context.Context, client redis.UniversalClient, isCluster bool) (map[string]bool, error) {
	knownKeys := make(map[string]bool)

	if !isCluster {
		// 单机模式：直接SCAN
		var cursor uint64
		for {
			keys, newCursor, err := client.Scan(ctx, cursor, "*", 1000).Result()
			if err != nil {
				return knownKeys, err
			}
			for _, key := range keys {
				knownKeys[key] = true
			}
			cursor = newCursor
			if cursor == 0 {
				break
			}
		}
		return knownKeys, nil
	}

	// 集群模式：遍历所有master节点扫描
	clusterClient := client.(*redis.ClusterClient)
	var mu sync.Mutex

	err := clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		var cursor uint64
		for {
			keys, newCursor, err := node.Scan(ctx, cursor, "*", 1000).Result()
			if err != nil {
				return err
			}
			mu.Lock()
			for _, key := range keys {
				knownKeys[key] = true
			}
			mu.Unlock()
			cursor = newCursor
			if cursor == 0 {
				break
			}
		}
		return nil
	})

	return knownKeys, err
}

// ==================== FakeSlave Binlog 增量同步（真正的 40 亿 Key 解决方案）====================

// doIncrementalSyncWithFakeSlave 使用 FakeSlave 模拟 Tendis Slave 进行增量同步
// 这是唯一适用于 40 亿 Key 场景的增量同步方案
// 工作原理：
// 1. 伪装成 Tendis Slave，使用 INCRSYNC 协议连接到 Master
// 2. 实时接收 Master 推送的 Binlog
// 3. 解析 Binlog 并将变更应用到目标端
func doIncrementalSyncWithFakeSlave(
	ctx context.Context,
	task *Task,
	sourceClient, targetClient redis.UniversalClient,
	sourceIsCluster bool,
	taskLog *logger.TaskLogger,
) {
	taskLog.Info("Starting incremental sync with FakeSlave Binlog mode (40B key safe)")

	// 更新任务状态
	tasksMu.Lock()
	task.IncrSyncMode = "binlog"
	tasksMu.Unlock()

	// 首先检查是否是 Tendis（支持 INCRSYNC 协议）
	if !checkTendisIncrSyncSupport(ctx, sourceClient, taskLog) {
		taskLog.Warn("Tendis INCRSYNC not supported, falling back to time-window mode")
		tasksMu.Lock()
		task.IncrSyncMode = "time_window"
		tasksMu.Unlock()
		// 回退到时间窗口模式（注意：40亿Key场景下此模式性能极差）
		doIncrementalSync(ctx, task, sourceClient, targetClient, sourceIsCluster, false, taskLog)
		return
	}

	// 创建 Key 过滤函数
	keyFilter := func(key string) bool {
		return matchKeyFilter(key, task.Options)
	}

	// 获取冲突策略
	conflictPolicy := "skip"
	if task.Options != nil && task.Options.ConflictPolicy != "" {
		conflictPolicy = task.Options.ConflictPolicy
	}

	// 创建 Binlog 处理回调
	binlogHandler := func(entries []replication.BinlogEntry) error {
		return processBinlogEntries(ctx, task, entries, targetClient, conflictPolicy, taskLog)
	}

	// 收集所有源端节点地址
	var sourceNodes []string
	if sourceIsCluster {
		clusterClient := sourceClient.(*redis.ClusterClient)
		err := clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			sourceNodes = append(sourceNodes, node.Options().Addr)
			return nil
		})
		if err != nil {
			taskLog.Error("Failed to get cluster nodes", map[string]interface{}{"error": err.Error()})
			return
		}
	} else {
		// 单机模式
		sourceNodes = []string{sourceClient.(*redis.Client).Options().Addr}
	}

	// 获取 kvstorecount（Tendis 每个节点的 store 数量，默认 10）
	kvstorecount := 10
	if len(sourceNodes) > 0 {
		nodeClient := redis.NewClient(&redis.Options{
			Addr:     sourceNodes[0],
			Password: task.SourcePassword,
		})
		result, err := nodeClient.Do(ctx, "CONFIG", "GET", "kvstorecount").Result()
		nodeClient.Close()
		if err == nil {
			if arr, ok := result.([]interface{}); ok && len(arr) >= 2 {
				if countStr, ok := arr[1].(string); ok {
					if count, err := strconv.Atoi(countStr); err == nil && count > 0 {
						kvstorecount = count
					}
				}
			}
		}
	}

	taskLog.Info("FakeSlave will connect to nodes", map[string]interface{}{
		"nodes":        sourceNodes,
		"node_count":   len(sourceNodes),
		"kvstorecount": kvstorecount,
		"total_slaves": len(sourceNodes) * kvstorecount,
	})

	// 为每个节点的每个 store 创建 FakeSlave
	var wg sync.WaitGroup
	fakeSlaves := make([]*replication.FakeSlave, 0, len(sourceNodes)*kvstorecount)
	var fakeSlavesMu sync.Mutex

	for nodeIdx, nodeAddr := range sourceNodes {
		nodeClient := redis.NewClient(&redis.Options{
			Addr:     nodeAddr,
			Password: task.SourcePassword,
		})

		for storeID := 0; storeID < kvstorecount; storeID++ {
			// 获取当前 store 的 binlog 位置
			var startBinlogPos uint64
			binlogPosResult, err := nodeClient.Do(ctx, "binlogpos", fmt.Sprintf("%d", storeID)).Result()

			if err == nil {
				switch v := binlogPosResult.(type) {
				case int64:
					startBinlogPos = uint64(v)
				case string:
					fmt.Sscanf(v, "%d", &startBinlogPos)
				}
				taskLog.Debug("Got binlog position for store", map[string]interface{}{
					"node":             nodeAddr,
					"store_id":         storeID,
					"start_binlog_pos": startBinlogPos,
				})
			} else {
				taskLog.Warn("Failed to get binlog position for store, using 0", map[string]interface{}{
					"node":     nodeAddr,
					"store_id": storeID,
					"error":    err.Error(),
				})
			}

			config := replication.FakeSlaveConfig{
				SourceAddr:       nodeAddr,
				SourcePassword:   task.SourcePassword,
				StoreID:          uint32(storeID),
				StartBinlogPos:   startBinlogPos,
				FakeListenIP:     "127.0.0.1",
				FakeListenPort:   uint16(16379 + nodeIdx*kvstorecount + storeID),
				ReadTimeout:      30 * time.Second,
				HeartbeatTimeout: 60 * time.Second,
				KeyFilter:        keyFilter,
			}

			fs := replication.NewFakeSlave(config, targetClient)
			fs.SetBinlogHandler(binlogHandler)

			fakeSlavesMu.Lock()
			fakeSlaves = append(fakeSlaves, fs)
			fakeSlavesMu.Unlock()

			wg.Add(1)
			go func(fs *replication.FakeSlave, nodeAddr string, storeID int) {
				defer wg.Done()

				// 启动 FakeSlave
				if err := fs.Start(ctx); err != nil {
					if ctx.Err() == nil {
						taskLog.Error("FakeSlave stopped with error", map[string]interface{}{
							"node":     nodeAddr,
							"store_id": storeID,
							"error":    err.Error(),
						})
					}
				}
			}(fs, nodeAddr, storeID)
		}

		nodeClient.Close()
	}

	// 保存 FakeSlaves 到 task（用于获取统计信息）
	tasksMu.Lock()
	task.fakeSlaves = fakeSlaves
	tasksMu.Unlock()

	// 定时更新统计信息
	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTicker.C:
				updateFakeSlaveStats(task, fakeSlaves, taskLog)
			}
		}
	}()

	// 等待所有 FakeSlave 完成或被取消
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		taskLog.Info("Incremental sync cancelled, stopping FakeSlaves...")
		// 停止所有 FakeSlave
		for _, fs := range fakeSlaves {
			fs.Stop()
		}
		<-done
	case <-done:
		taskLog.Info("All FakeSlaves stopped")
	}

	// 最终统计
	updateFakeSlaveStats(task, fakeSlaves, taskLog)
	taskLog.Info("FakeSlave incremental sync completed", map[string]interface{}{
		"keys_synced":   task.IncrKeysSynced,
		"keys_filtered": task.IncrKeysFiltered,
		"keys_failed":   task.IncrKeysFailed,
	})
}

// ==================== 【新增】FakeSlave 缓存模式启动函数 ====================

// startFakeSlavesWithCache 启动 FakeSlave 并开启缓存模式
// 用于全量迁移前启动，将 Binlog 缓存到本地文件
func startFakeSlavesWithCache(
	ctx context.Context,
	task *Task,
	sourceClient, targetClient redis.UniversalClient,
	sourceIsCluster bool,
	cacheManager *replication.BinlogCacheManager,
	taskLog *logger.TaskLogger,
) ([]*replication.FakeSlave, error) {
	taskLog.Info("Starting FakeSlaves with cache mode for capturing binlogs during full migration")

	// 创建 Key 过滤函数
	keyFilter := func(key string) bool {
		return matchKeyFilter(key, task.Options)
	}

	// 获取冲突策略（用于后续实时同步）
	conflictPolicy := "skip"
	if task.Options != nil && task.Options.ConflictPolicy != "" {
		conflictPolicy = task.Options.ConflictPolicy
	}

	// 创建 Binlog 处理回调（非缓存模式下使用）
	binlogHandler := func(entries []replication.BinlogEntry) error {
		return processBinlogEntries(ctx, task, entries, targetClient, conflictPolicy, taskLog)
	}

	// 收集所有源端节点地址
	var sourceNodes []string
	if sourceIsCluster {
		clusterClient := sourceClient.(*redis.ClusterClient)
		err := clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			sourceNodes = append(sourceNodes, node.Options().Addr)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get cluster nodes: %w", err)
		}
	} else {
		// 单机模式
		sourceNodes = []string{sourceClient.(*redis.Client).Options().Addr}
	}

	// 获取 kvstorecount（Tendis 每个节点的 store 数量，默认 10）
	kvstorecount := 10
	if len(sourceNodes) > 0 {
		nodeClient := redis.NewClient(&redis.Options{
			Addr:     sourceNodes[0],
			Password: task.SourcePassword,
		})
		result, err := nodeClient.Do(ctx, "CONFIG", "GET", "kvstorecount").Result()
		nodeClient.Close()
		if err == nil {
			if arr, ok := result.([]interface{}); ok && len(arr) >= 2 {
				if countStr, ok := arr[1].(string); ok {
					if count, err := strconv.Atoi(countStr); err == nil && count > 0 {
						kvstorecount = count
					}
				}
			}
		}
	}

	taskLog.Info("FakeSlave will connect to nodes (cache mode)", map[string]interface{}{
		"nodes":        sourceNodes,
		"node_count":   len(sourceNodes),
		"kvstorecount": kvstorecount,
		"total_slaves": len(sourceNodes) * kvstorecount,
	})

	// 为每个节点的每个 store 创建 FakeSlave
	// Tendis 每个节点有 kvstorecount 个 store，每个 store 有独立的 binlog
	fakeSlaves := make([]*replication.FakeSlave, 0, len(sourceNodes)*kvstorecount)

	for nodeIdx, nodeAddr := range sourceNodes {
		nodeClient := redis.NewClient(&redis.Options{
			Addr:     nodeAddr,
			Password: task.SourcePassword,
		})

		for storeID := 0; storeID < kvstorecount; storeID++ {
			// 获取当前 store 的 binlog 位置
			var startBinlogPos uint64
			binlogPosResult, err := nodeClient.Do(ctx, "binlogpos", fmt.Sprintf("%d", storeID)).Result()

			if err == nil {
				switch v := binlogPosResult.(type) {
				case int64:
					startBinlogPos = uint64(v)
				case string:
					fmt.Sscanf(v, "%d", &startBinlogPos)
				}
				taskLog.Debug("Got binlog position for store", map[string]interface{}{
					"node":             nodeAddr,
					"store_id":         storeID,
					"start_binlog_pos": startBinlogPos,
				})
			} else {
				taskLog.Warn("Failed to get binlog position for store, using 0", map[string]interface{}{
					"node":     nodeAddr,
					"store_id": storeID,
					"error":    err.Error(),
				})
			}

			config := replication.FakeSlaveConfig{
				SourceAddr:       nodeAddr,
				SourcePassword:   task.SourcePassword,
				StoreID:          uint32(storeID),
				StartBinlogPos:   startBinlogPos,
				FakeListenIP:     "127.0.0.1",
				FakeListenPort:   uint16(16379 + nodeIdx*kvstorecount + storeID),
				ReadTimeout:      30 * time.Second,
				HeartbeatTimeout: 60 * time.Second,
				KeyFilter:        keyFilter,
				// 【关键】启用缓存模式
				CacheMode:    true,
				CacheManager: cacheManager,
			}

			fs := replication.NewFakeSlave(config, targetClient)
			fs.SetBinlogHandler(binlogHandler)
			fakeSlaves = append(fakeSlaves, fs)

			// 启动 FakeSlave goroutine
			go func(fs *replication.FakeSlave, nodeAddr string, storeID int) {
				if err := fs.Start(ctx); err != nil {
					if ctx.Err() == nil {
						taskLog.Error("FakeSlave stopped with error", map[string]interface{}{
							"node":     nodeAddr,
							"store_id": storeID,
							"error":    err.Error(),
						})
					}
				}
			}(fs, nodeAddr, storeID)
		}

		nodeClient.Close()
	}

	taskLog.Info("All FakeSlaves started", map[string]interface{}{
		"total_slaves": len(fakeSlaves),
	})

	// 等待所有 FakeSlave 连接成功（最多等待 30 秒）
	taskLog.Info("Waiting for all FakeSlaves to connect...")
	var connectedCount int
	for _, fs := range fakeSlaves {
		if err := fs.WaitConnected(30 * time.Second); err != nil {
			taskLog.Warn("FakeSlave connection failed or timeout", map[string]interface{}{
				"error": err.Error(),
			})
		} else {
			connectedCount++
		}
	}

	if connectedCount == 0 {
		// 所有连接都失败，停止并返回错误
		for _, fs := range fakeSlaves {
			fs.Stop()
		}
		return nil, fmt.Errorf("all FakeSlave connections failed")
	}

	taskLog.Info("FakeSlaves connected and ready", map[string]interface{}{
		"connected": connectedCount,
		"total":     len(fakeSlaves),
	})

	return fakeSlaves, nil
}

// waitForFakeSlaves 等待 FakeSlave 完成或被取消（实时同步模式）
func waitForFakeSlaves(
	ctx context.Context,
	cancel context.CancelFunc,
	task *Task,
	fakeSlaves []*replication.FakeSlave,
	taskLog *logger.TaskLogger,
) {
	// 定时更新统计信息
	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTicker.C:
				updateFakeSlaveStats(task, fakeSlaves, taskLog)
			}
		}
	}()

	// 等待所有 FakeSlave 完成
	var wg sync.WaitGroup
	for _, fs := range fakeSlaves {
		wg.Add(1)
		go func(fs *replication.FakeSlave) {
			defer wg.Done()
			// FakeSlave 已经在运行，这里只是等待它停止
			for fs.IsConnected() {
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
				}
			}
		}(fs)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		taskLog.Info("Incremental sync cancelled, stopping FakeSlaves...")
		for _, fs := range fakeSlaves {
			fs.Stop()
		}
		<-done
	case <-done:
		taskLog.Info("All FakeSlaves stopped")
	}

	// 最终统计
	updateFakeSlaveStats(task, fakeSlaves, taskLog)
	taskLog.Info("FakeSlave real-time sync completed", map[string]interface{}{
		"keys_synced":   task.IncrKeysSynced,
		"keys_filtered": task.IncrKeysFiltered,
		"keys_failed":   task.IncrKeysFailed,
	})
}

// checkTendisIncrSyncSupport 检查 Tendis 是否支持 INCRSYNC 协议
// Tendis 2.7.0 的正确检测方式：
// 1. 检查 binlogpos 命令（获取 binlog 位置）
// 2. 检查 binlog-enabled 配置
// 3. 检查 INCRSYNC 命令是否存在
// 注意：Tendis 不支持 "binlog getlatestoffset" 命令，应使用 "binlogpos <storeId>"
func checkTendisIncrSyncSupport(ctx context.Context, client redis.UniversalClient, taskLog *logger.TaskLogger) bool {
	// 尝试获取 Tendis 版本信息
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		taskLog.Warn("Failed to get server info", map[string]interface{}{"error": err.Error()})
		return false
	}

	// 检查是否是 Tendis（多种检测方式）
	// 1. 包含 tendis_version 或 tendisplus_version
	// 2. 包含 TendisPlus
	// 3. 包含 TENDIS_DEBUG（Tendis 2.7.0 特征）
	// 4. 版本号包含 rocksdb（Tendis 使用 RocksDB 存储引擎）
	isTendis := strings.Contains(info, "tendis_version") ||
		strings.Contains(info, "tendisplus_version") ||
		strings.Contains(info, "TendisPlus") ||
		strings.Contains(info, "TENDIS_DEBUG") ||
		strings.Contains(info, "rocksdb")

	if isTendis {
		taskLog.Info("Tendis detected, checking INCRSYNC protocol support", map[string]interface{}{
			"server_info_snippet": info[:min(300, len(info))],
		})

		// 方法1: 检查 binlogpos 命令（Tendis 2.7.0 正确的获取 binlog 位置方式）
		// 命令格式: binlogpos <storeId>
		binlogPosResult, err := client.Do(ctx, "binlogpos", "0").Result()
		if err == nil {
			taskLog.Info("Tendis binlogpos command works", map[string]interface{}{
				"store_0_binlog_pos": fmt.Sprintf("%v", binlogPosResult),
			})

			// 检查 binlog-enabled 配置
			configResult, err := client.Do(ctx, "CONFIG", "GET", "binlog-enabled").Result()
			binlogEnabled := false
			if err == nil {
				configStr := fmt.Sprintf("%v", configResult)
				binlogEnabled = strings.Contains(configStr, "yes")
				taskLog.Info("Tendis binlog config", map[string]interface{}{
					"binlog-enabled": binlogEnabled,
					"config_result":  configStr,
				})
			}

			if binlogEnabled {
				// 检查 INCRSYNC 命令是否存在
				cmdList, err := client.Do(ctx, "COMMAND").Result()
				if err == nil {
					cmdStr := fmt.Sprintf("%v", cmdList)
					if strings.Contains(strings.ToLower(cmdStr), "incrsync") {
						taskLog.Info("Tendis INCRSYNC protocol fully supported", map[string]interface{}{
							"binlog_pos":     fmt.Sprintf("%v", binlogPosResult),
							"binlog_enabled": true,
							"incrsync_cmd":   true,
						})
						return true
					}
				}
				// 即使没找到 INCRSYNC 命令，binlogpos 可用也说明可以尝试
				taskLog.Info("Tendis binlog available (binlogpos works, binlog enabled)", map[string]interface{}{
					"binlog_pos": fmt.Sprintf("%v", binlogPosResult),
				})
				return true
			} else {
				taskLog.Warn("Tendis binlog not enabled, INCRSYNC may not work properly", map[string]interface{}{
					"binlog-enabled": false,
					"hint":           "Run 'CONFIG SET binlog-enabled yes' on source Tendis to enable",
				})
				// binlog 未启用，但命令存在，返回 true 让后续尝试
				return true
			}
		}

		// 方法2: 检查 INFO BinlogInfo（备选方案）
		binlogInfo, err := client.Info(ctx, "BinlogInfo").Result()
		if err == nil && binlogInfo != "" && strings.Contains(binlogInfo, "rocksdb") {
			taskLog.Info("Tendis BinlogInfo available", map[string]interface{}{
				"binlog_info_snippet": binlogInfo[:min(300, len(binlogInfo))],
			})

			// 检查 INCRSYNC 命令
			cmdList, err := client.Do(ctx, "COMMAND").Result()
			if err == nil {
				cmdStr := fmt.Sprintf("%v", cmdList)
				if strings.Contains(strings.ToLower(cmdStr), "incrsync") {
					taskLog.Info("Tendis INCRSYNC supported (via BinlogInfo)")
					return true
				}
			}
		}

		// 方法3: 直接检查 COMMAND 列表中的 incrsync 命令
		cmdList, err := client.Do(ctx, "COMMAND").Result()
		if err == nil {
			cmdStr := fmt.Sprintf("%v", cmdList)
			if strings.Contains(strings.ToLower(cmdStr), "incrsync") {
				taskLog.Info("INCRSYNC command found in COMMAND list, attempting to use it")
				return true
			}
		}

		taskLog.Warn("Tendis detected but INCRSYNC protocol not available", map[string]interface{}{
			"binlogpos_error": err.Error(),
		})
		return false
	}

	// 非 Tendis，尝试检测 binlogpos 命令（兼容其他 Tendis 兼容系统）
	binlogPosResult, err := client.Do(ctx, "binlogpos", "0").Result()
	if err == nil {
		taskLog.Info("binlogpos command supported, assuming Tendis compatible", map[string]interface{}{
			"binlog_pos": fmt.Sprintf("%v", binlogPosResult),
		})
		return true
	}

	taskLog.Info("Not Tendis, INCRSYNC not supported", map[string]interface{}{
		"server_info_snippet": info[:min(200, len(info))],
	})
	return false
}

// parseRESPCommand 解析 RESP 格式的 Redis 命令
// 格式：*N\r\n$len1\r\narg1\r\n$len2\r\narg2\r\n...
// 【重要】正确处理二进制数据，不能使用 strings.Split，必须根据长度读取
func parseRESPCommand(cmdStr string) []string {
	if len(cmdStr) == 0 || cmdStr[0] != '*' {
		return nil
	}

	data := []byte(cmdStr)
	offset := 1 // 跳过 '*'

	// 读取数组长度
	argCountEnd := offset
	for argCountEnd < len(data) && data[argCountEnd] != '\r' {
		argCountEnd++
	}
	if argCountEnd >= len(data)-1 {
		return nil
	}
	argCount, err := strconv.Atoi(string(data[offset:argCountEnd]))
	if err != nil || argCount <= 0 {
		return nil
	}
	offset = argCountEnd + 2 // 跳过 "\r\n"

	var args []string
	for j := 0; j < argCount && offset < len(data); j++ {
		// 期望 '$'
		if data[offset] != '$' {
			break
		}
		offset++ // 跳过 '$'

		// 读取长度
		lenEnd := offset
		for lenEnd < len(data) && data[lenEnd] != '\r' {
			lenEnd++
		}
		if lenEnd >= len(data)-1 {
			break
		}
		argLen, err := strconv.Atoi(string(data[offset:lenEnd]))
		if err != nil || argLen < 0 {
			break
		}
		offset = lenEnd + 2 // 跳过 "\r\n"

		// 读取实际参数（根据长度，正确处理二进制数据）
		if offset+argLen > len(data) {
			break
		}
		args = append(args, string(data[offset:offset+argLen]))
		offset += argLen

		// 跳过参数后的 "\r\n"
		if offset+2 <= len(data) && data[offset] == '\r' && data[offset+1] == '\n' {
			offset += 2
		}
	}

	return args
}

// processBinlogEntries 处理 Binlog 条目并应用到目标端
func processBinlogEntries(
	ctx context.Context,
	task *Task,
	entries []replication.BinlogEntry,
	targetClient redis.UniversalClient,
	conflictPolicy string,
	taskLog *logger.TaskLogger,
) error {
	if len(entries) == 0 {
		return nil
	}

	var synced, skipped, failed int64

	for _, entry := range entries {
		// Key 已经在 FakeSlave 中过滤过了，这里直接处理
		switch entry.OpType {
		case "CMD":
			// CMD 类型：entry.Value 是 RESP 格式的 Redis 命令字符串
			// 例如：*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
			cmdStr := string(entry.Value)
			args := parseRESPCommand(cmdStr)
			if len(args) > 0 {
				taskLog.Debug("Executing CMD binlog", map[string]interface{}{
					"command": args[0],
					"args":    args[1:],
				})
				// 将字符串数组转为 interface{} 数组
				iargs := make([]interface{}, len(args))
				for i, v := range args {
					iargs[i] = v
				}
				err := targetClient.Do(ctx, iargs...).Err()
				if err != nil {
					taskLog.Debug("Binlog CMD failed", map[string]interface{}{
						"command": args,
						"error":   err.Error(),
					})
					failed++
					continue
				}
				synced++
			} else {
				taskLog.Debug("Failed to parse CMD binlog", map[string]interface{}{
					"value_len": len(entry.Value),
				})
				failed++
			}

		case "SET":
			// SET 类型：entry.Value 是 RocksDB 格式的 DUMP 数据
			// 使用 RESTORE 命令同步
			if len(entry.Value) > 0 {
				ttl := time.Duration(entry.TTL) * time.Millisecond
				// 根据冲突策略决定是否替换
				if conflictPolicy == "replace" || conflictPolicy == "skip_full_only" {
					err := targetClient.RestoreReplace(ctx, entry.Key, ttl, string(entry.Value)).Err()
					if err != nil {
						taskLog.Debug("Binlog RESTORE failed", map[string]interface{}{
							"key":   entry.Key,
							"error": err.Error(),
						})
						failed++
						continue
					}
					synced++
				} else {
					// skip 策略：先检查目标端是否存在
					exists, _ := targetClient.Exists(ctx, entry.Key).Result()
					if exists > 0 {
						skipped++
						continue
					}
					err := targetClient.Restore(ctx, entry.Key, ttl, string(entry.Value)).Err()
					if err != nil {
						failed++
						continue
					}
					synced++
				}
			}

		case "DEL", "UNLINK", "EXPIRED", "EVICTED":
			// 删除操作
			err := targetClient.Del(ctx, entry.Key).Err()
			if err != nil {
				taskLog.Debug("Binlog DEL failed", map[string]interface{}{
					"key":   entry.Key,
					"error": err.Error(),
				})
				// 删除失败不计入 failed（目标端可能本来就不存在）
			} else {
				synced++
			}

		default:
			// 其他操作类型：尝试通用同步
			taskLog.Debug("Unknown binlog op type", map[string]interface{}{
				"op_type": entry.OpType,
				"key":     entry.Key,
			})
		}
	}

	// 更新任务统计
	tasksMu.Lock()
	task.IncrKeysSynced += synced
	task.IncrKeysSkipped += skipped
	task.IncrKeysFailed += failed
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	tasksMu.Unlock()

	return nil
}

// updateFakeSlaveStats 更新 FakeSlave 统计信息到任务
func updateFakeSlaveStats(task *Task, fakeSlaves []*replication.FakeSlave, taskLog *logger.TaskLogger) {
	var totalBinlogs, appliedBinlogs, filteredBinlogs, cachedBinlogs, heartbeats, reconnects, errors int64
	var maxBinlogPos uint64
	nodeOffsets := make(map[string]uint64)

	for i, fs := range fakeSlaves {
		stats := fs.GetStats()
		totalBinlogs += stats["total_binlogs"]
		appliedBinlogs += stats["applied_binlogs"]
		filteredBinlogs += stats["filtered_binlogs"]
		cachedBinlogs += stats["cached_binlogs"]
		heartbeats += stats["heartbeats"]
		reconnects += stats["reconnects"]
		errors += stats["errors"]

		pos := fs.GetCurrentBinlogPos()
		if pos > maxBinlogPos {
			maxBinlogPos = pos
		}
		// 记录每个节点的 Binlog 位置（用于断点恢复）
		nodeOffsets[fmt.Sprintf("store_%d", i)] = pos
	}

	tasksMu.Lock()
	task.IncrKeysFiltered = filteredBinlogs
	task.IncrBinlogPos = maxBinlogPos
	task.IncrHeartbeats = heartbeats
	task.IncrReconnects = reconnects
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	keysSynced := task.IncrKeysSynced
	keysSkipped := task.IncrKeysSkipped
	keysFailed := task.IncrKeysFailed
	tasksMu.Unlock()

	// 【断点保存】定期保存 FakeSlave 断点信息
	saveFakeSlaveCheckpoint(task.ID, nodeOffsets, keysSynced, keysSkipped, keysFailed, filteredBinlogs)

	taskLog.Debug("FakeSlave stats updated", map[string]interface{}{
		"total_binlogs":    totalBinlogs,
		"applied_binlogs":  appliedBinlogs,
		"filtered_binlogs": filteredBinlogs,
		"cached_binlogs":   cachedBinlogs,
		"heartbeats":       heartbeats,
		"reconnects":       reconnects,
		"binlog_pos":       maxBinlogPos,
	})
}

// saveFakeSlaveCheckpoint 保存 FakeSlave 断点（用于崩溃恢复）
func saveFakeSlaveCheckpoint(taskID string, nodeOffsets map[string]uint64, keysSynced, keysSkipped, keysFailed, keysFiltered int64) {
	checkpoint := &BinlogCheckpoint{
		TaskID:          taskID,
		NodeOffsets:     nodeOffsets,
		LastSyncTime:    time.Now().Format(time.RFC3339),
		KeysSynced:      keysSynced,
		KeysSkipped:     keysSkipped,
		KeysFailed:      keysFailed,
		KeysFiltered:    keysFiltered,
		UpdatedAt:       time.Now().Format(time.RFC3339),
	}

	binlogCheckpointsMu.Lock()
	binlogCheckpoints[taskID] = checkpoint
	binlogCheckpointsMu.Unlock()

	// 保存到文件
	checkpointDir := "./data/checkpoints"
	os.MkdirAll(checkpointDir, 0755)
	data, _ := json.MarshalIndent(checkpoint, "", "  ")
	os.WriteFile(fmt.Sprintf("%s/fakeslave-%s.json", checkpointDir, taskID), data, 0644)
}

// min 返回两个整数的最小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max 返回两个整数的最大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ==================== P1 改进: 时间窗口增量同步（备用方案，40亿Key场景下性能极差）====================

// doIncrementalSync 增量同步 V2（时间窗口模式，无 OOM 风险）
// 核心改进：不再存储全量 Key 到内存，而是通过 OBJECT IDLETIME 检测最近修改的 Key
func doIncrementalSync(ctx context.Context, task *Task, sourceClient, targetClient redis.UniversalClient, sourceIsCluster, targetIsCluster bool, taskLog *logger.TaskLogger) {
	taskLog.Info("Starting incremental sync V2 (time-window mode, no OOM risk)")

	// 加载 V2 断点（如果存在）
	checkpointV2 := loadIncrementalCheckpointV2(task.ID)

	// 配置参数
	syncIntervalSec := 30 // 默认同步间隔 30 秒
	if checkpointV2 != nil && checkpointV2.SyncInterval > 0 {
		syncIntervalSec = checkpointV2.SyncInterval
	}
	syncInterval := time.Duration(syncIntervalSec) * time.Second

	// 各节点的 SCAN cursor（用于断点续传）- 这里只保存 cursor，不保存全量 Key
	nodeCursors := make(map[string]uint64)
	if checkpointV2 != nil && checkpointV2.NodeCursors != nil {
		for k, v := range checkpointV2.NodeCursors {
			nodeCursors[k] = v
		}
	}

	// 累计统计
	keysSynced := int64(0)
	keysSkipped := int64(0)
	keysFailed := int64(0)
	scanRounds := int64(0)
	var totalRoundDuration time.Duration

	if checkpointV2 != nil {
		keysSynced = checkpointV2.KeysSynced
		keysSkipped = checkpointV2.KeysSkipped
		keysFailed = checkpointV2.KeysFailed
		scanRounds = checkpointV2.ScanRounds
		taskLog.Info("Resuming from V2 checkpoint", map[string]interface{}{
			"last_sync_time": checkpointV2.LastSyncTime,
			"keys_synced":    keysSynced,
			"keys_skipped":   keysSkipped,
			"scan_rounds":    scanRounds,
		})
	}

	// 扫描间隔 ticker
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	// 断点保存 ticker（每 30 秒保存一次）
	checkpointTicker := time.NewTicker(30 * time.Second)
	defer checkpointTicker.Stop()

	taskLog.Info("Incremental sync V2 configuration", map[string]interface{}{
		"sync_interval_sec": syncIntervalSec,
		"is_cluster":        sourceIsCluster,
	})

	// 创建一个用于取消的 context
	incrCtx, incrCancel := context.WithCancel(ctx)
	defer incrCancel()

	for {
		select {
		case <-incrCtx.Done():
			// 保存最终断点
			saveIncrementalCheckpointV2Final(task.ID, nodeCursors, keysSynced, keysSkipped, keysFailed, scanRounds, syncIntervalSec, totalRoundDuration)
			taskLog.Info("Incremental sync V2 stopped (context done)", map[string]interface{}{
				"keys_synced":  keysSynced,
				"keys_skipped": keysSkipped,
				"keys_failed":  keysFailed,
				"scan_rounds":  scanRounds,
			})
			return

		case <-checkpointTicker.C:
			// 定期保存断点
			avgDuration := ""
			if scanRounds > 0 {
				avgDuration = (totalRoundDuration / time.Duration(scanRounds)).String()
			}
			saveIncrementalCheckpointV2(task.ID, &IncrementalCheckpointV2{
				TaskID:           task.ID,
				NodeCursors:      nodeCursors,
				LastSyncTime:     time.Now().Format(time.RFC3339),
				SyncInterval:     syncIntervalSec,
				KeysSynced:       keysSynced,
				KeysSkipped:      keysSkipped,
				KeysFailed:       keysFailed,
				ScanRounds:       scanRounds,
				AvgRoundDuration: avgDuration,
			})

		case <-ticker.C:
			// 检查任务状态
			tasksMu.RLock()
			status := task.Status
			tasksMu.RUnlock()

			if status != "running" {
				saveIncrementalCheckpointV2Final(task.ID, nodeCursors, keysSynced, keysSkipped, keysFailed, scanRounds, syncIntervalSec, totalRoundDuration)
				taskLog.Info("Incremental sync V2 stopped (task not running)", map[string]interface{}{
					"status":       status,
					"keys_synced":  keysSynced,
					"keys_skipped": keysSkipped,
					"scan_rounds":  scanRounds,
				})
				return
			}

			// 执行一轮时间窗口扫描
			roundStart := time.Now()
			roundSynced, roundSkipped, roundFailed := doIncrementalScanRoundV2(
				incrCtx, task, sourceClient, targetClient, sourceIsCluster,
				syncInterval, nodeCursors, taskLog,
			)

			roundDuration := time.Since(roundStart)
			totalRoundDuration += roundDuration
			scanRounds++
			keysSynced += roundSynced
			keysSkipped += roundSkipped
			keysFailed += roundFailed

			// 计算本轮的实时速度（keys/s）
			roundSpeed := int64(0)
			if roundDuration.Seconds() > 0 {
				roundSpeed = int64(float64(roundSynced+roundSkipped) / roundDuration.Seconds())
			}

			// 更新任务统计（包括速度）
			tasksMu.Lock()
			task.KeysMigrated += roundSynced
			task.KeysSkipped += roundSkipped
			task.KeysFailed += roundFailed
			task.UpdatedAt = time.Now().Format(time.RFC3339)
			// 增量阶段更新速度：显示本轮扫描速度
			if roundSpeed > 0 {
				task.Speed = roundSpeed
			} else if roundSynced == 0 && roundSkipped == 0 {
				// 如果本轮没有变化，显示状态为"监听中"，速度保持之前值或显示为0
				// 这里不更新速度，保持上次的速度值，让用户知道还在运行
			}
			tasksMu.Unlock()

			// 更新断点中的上一轮统计
			avgDuration := (totalRoundDuration / time.Duration(scanRounds)).String()
			saveIncrementalCheckpointV2(task.ID, &IncrementalCheckpointV2{
				TaskID:            task.ID,
				NodeCursors:       nodeCursors,
				LastSyncTime:      time.Now().Format(time.RFC3339),
				SyncInterval:      syncIntervalSec,
				KeysSynced:        keysSynced,
				KeysSkipped:       keysSkipped,
				KeysFailed:        keysFailed,
				ScanRounds:        scanRounds,
				LastRoundDuration: roundDuration.String(),
				LastRoundSynced:   roundSynced,
				LastRoundSkipped:  roundSkipped,
				AvgRoundDuration:  avgDuration,
				EstimatedLag:      avgDuration, // 同步延迟约等于一轮扫描时间
			})

			// 只在有变化时打印日志
			if roundSynced > 0 || roundSkipped > 0 || roundFailed > 0 {
				taskLog.Info("Incremental sync V2 round completed", map[string]interface{}{
					"round":           scanRounds,
					"round_synced":    roundSynced,
					"round_skipped":   roundSkipped,
					"round_failed":    roundFailed,
					"round_duration":  roundDuration.String(),
					"round_speed":     roundSpeed,
					"total_synced":    keysSynced,
					"total_skipped":   keysSkipped,
					"total_migrated":  task.KeysMigrated,
				})
			} else {
				// 每 5 轮打一条"监听中"的日志，让用户知道系统在运行
				if scanRounds%5 == 0 {
					taskLog.Info("Incremental sync V2 listening (no changes detected)", map[string]interface{}{
						"round":          scanRounds,
						"round_duration": roundDuration.String(),
						"total_migrated": task.KeysMigrated,
						"status":         "listening",
					})
				} else {
					taskLog.Debug("Incremental sync V2 round completed (no changes)", map[string]interface{}{
						"round":          scanRounds,
						"round_duration": roundDuration.String(),
					})
				}
			}
		}
	}
}

// doIncrementalScanRoundV2 执行一轮增量扫描（时间窗口模式）
// 核心：通过 OBJECT IDLETIME 检测最近修改的 Key，不存储全量 Key 到内存
func doIncrementalScanRoundV2(
	ctx context.Context,
	task *Task,
	sourceClient, targetClient redis.UniversalClient,
	sourceIsCluster bool,
	syncInterval time.Duration,
	nodeCursors map[string]uint64,
	taskLog *logger.TaskLogger,
) (synced, skipped, failed int64) {

	// 时间阈值：syncInterval + 5 秒容错
	idleTimeThreshold := syncInterval + 5*time.Second

	if !sourceIsCluster {
		// 单机模式
		synced, skipped, failed = scanNodeModifiedKeysV2(
			ctx, sourceClient.(*redis.Client), targetClient,
			"standalone", task, idleTimeThreshold, nodeCursors, taskLog,
		)
		return
	}

	// 集群模式：并行扫描各主节点
	clusterClient := sourceClient.(*redis.ClusterClient)
	var wg sync.WaitGroup
	var mu sync.Mutex

	err := clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		nodeAddr := node.Options().Addr

		wg.Add(1)
		go func() {
			defer wg.Done()

			nodeSynced, nodeSkipped, nodeFailed := scanNodeModifiedKeysV2(
				ctx, node, targetClient,
				nodeAddr, task, idleTimeThreshold, nodeCursors, taskLog,
			)

			mu.Lock()
			synced += nodeSynced
			skipped += nodeSkipped
			failed += nodeFailed
			mu.Unlock()
		}()

		return nil
	})

	wg.Wait()

	if err != nil {
		taskLog.Warn("Error iterating cluster masters", map[string]interface{}{"error": err.Error()})
	}

	return
}

// scanNodeModifiedKeysV2 扫描单个节点最近修改的 Key（核心函数）
// 使用 OBJECT IDLETIME 检测最近修改的 Key，无需存储全量 Key
func scanNodeModifiedKeysV2(
	ctx context.Context,
	node *redis.Client,
	targetClient redis.UniversalClient,
	nodeAddr string,
	task *Task,
	idleTimeThreshold time.Duration,
	nodeCursors map[string]uint64,
	taskLog *logger.TaskLogger,
) (synced, skipped, failed int64) {

	// 获取该节点的起始 cursor（用于断点续传）
	cursor := nodeCursors[nodeAddr]

	// 批量大小
	batchSize := int64(10000)
	if task.Options != nil && task.Options.ScanBatchSize > 0 {
		batchSize = int64(task.Options.ScanBatchSize)
	}

	// 【优化】获取 SCAN MATCH 模式
	var scanMatchPattern string
	if task.Options != nil {
		scanMatchPattern = getScanMatchPattern(task.Options.KeyFilter)
	} else {
		scanMatchPattern = "*"
	}

	// 获取重试配置
	maxRetries, _, incrIntervalMs := getRetryConfig(task.Options)

	// 用于 Pipeline 批量检查 IDLETIME
	const pipelineBatchSize = 100

	for {
		// 检查上下文是否已取消
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 【优化】SCAN 使用 MATCH 模式服务端过滤
		keys, newCursor, err := node.Scan(ctx, cursor, scanMatchPattern, batchSize).Result()
		if err != nil {
			taskLog.Warn("SCAN failed in incremental V2", map[string]interface{}{
				"node":          nodeAddr,
				"cursor":        cursor,
				"match_pattern": scanMatchPattern,
				"error":         err.Error(),
			})
			break
		}

		// 批量检查 IDLETIME 并处理
		for i := 0; i < len(keys); i += pipelineBatchSize {
			end := i + pipelineBatchSize
			if end > len(keys) {
				end = len(keys)
			}
			batchKeys := keys[i:end]

			// 使用 Pipeline 批量获取 IDLETIME
			pipe := node.Pipeline()
			idleTimeCmds := make([]*redis.DurationCmd, len(batchKeys))
			for j, key := range batchKeys {
				idleTimeCmds[j] = pipe.ObjectIdleTime(ctx, key)
			}
			_, _ = pipe.Exec(ctx)

			// 处理每个 Key
			for j, key := range batchKeys {
				// 1. 先检查 Key 是否匹配过滤规则（双重校验，避免极端场景漏筛）
				if !matchKeyFilter(key, task.Options) {
					continue
				}

				// 2. 检查 IDLETIME
				idleTime, err := idleTimeCmds[j].Result()
				if err != nil {
					// Key 可能已被删除或不存在，跳过
					continue
				}

				// 3. 如果空闲时间 < 阈值，说明最近被修改过，需要同步
				if idleTime < idleTimeThreshold {
					// 迁移这个 Key（带重试）
					var migrated bool
					var reason string

					for retry := 0; retry < maxRetries; retry++ {
						migrated, _, reason = migrateKeyWithPolicy(ctx, node, targetClient, key, "replace")
						if migrated || reason == "skipped" || reason == "" {
							break
						}
						if retry < maxRetries-1 {
							time.Sleep(time.Duration((retry+1)*incrIntervalMs) * time.Millisecond)
						}
					}

					if migrated {
						synced++
					} else if reason == "skipped" {
						skipped++
						addErrorKey(task.ID, key, "string", "skipped", "Key exists in target (incremental V2)")
					} else if reason != "" {
						failed++
						addErrorKey(task.ID, key, "string", "failed", reason+" (incremental V2)")
					}
				}
			}
		}

		// 更新 cursor
		cursor = newCursor
		nodeCursors[nodeAddr] = cursor

		// cursor 为 0 表示扫描完成一轮
		if cursor == 0 {
			break
		}
	}

	return
}

// saveIncrementalCheckpointV2Final 保存最终断点（任务停止时调用）
func saveIncrementalCheckpointV2Final(taskID string, nodeCursors map[string]uint64, keysSynced, keysSkipped, keysFailed, scanRounds int64, syncIntervalSec int, totalRoundDuration time.Duration) {
	avgDuration := ""
	if scanRounds > 0 {
		avgDuration = (totalRoundDuration / time.Duration(scanRounds)).String()
	}
	saveIncrementalCheckpointV2(taskID, &IncrementalCheckpointV2{
		TaskID:           taskID,
		NodeCursors:      nodeCursors,
		LastSyncTime:     time.Now().Format(time.RFC3339),
		SyncInterval:     syncIntervalSec,
		KeysSynced:       keysSynced,
		KeysSkipped:      keysSkipped,
		KeysFailed:       keysFailed,
		ScanRounds:       scanRounds,
		AvgRoundDuration: avgDuration,
	})
}

// ==================== P3 改进: Tendis Binlog 支持（可选，如果 Tendis 支持）====================

// BinlogCheckpoint Binlog 增量同步断点
type BinlogCheckpoint struct {
	TaskID          string            `json:"task_id"`
	NodeOffsets     map[string]uint64 `json:"node_offsets"`     // 各节点的 Binlog 偏移量
	LastSyncTime    string            `json:"last_sync_time"`   // 上次同步时间
	KeysSynced      int64             `json:"keys_synced"`      // 累计同步的 Key 数
	KeysSkipped     int64             `json:"keys_skipped"`     // 累计跳过的 Key 数
	KeysFailed      int64             `json:"keys_failed"`      // 累计失败的 Key 数
	KeysFiltered    int64             `json:"keys_filtered"`    // 累计过滤的 Key 数（不符合前缀条件）
	BinlogSupported bool              `json:"binlog_supported"` // Tendis 是否支持 Binlog
	UpdatedAt       string            `json:"updated_at"`       // 更新时间
}

var (
	binlogCheckpoints   = make(map[string]*BinlogCheckpoint)
	binlogCheckpointsMu sync.RWMutex
)

// CheckTendisBinlogSupport 检查 Tendis 是否支持 Binlog 命令
// Tendis 2.7.0 使用 binlogpos <storeId> 命令获取 binlog 位置
func CheckTendisBinlogSupport(ctx context.Context, client redis.UniversalClient) (bool, string) {
	// 尝试执行 binlogpos 命令（Tendis 2.7.0 正确的命令格式）
	result, err := client.Do(ctx, "binlogpos", "0").Result()
	if err != nil {
		return false, fmt.Sprintf("Binlog not supported: %v", err)
	}

	// 检查返回值类型
	switch v := result.(type) {
	case int64:
		return true, fmt.Sprintf("Binlog supported, store 0 binlog pos: %d", v)
	case string:
		return true, fmt.Sprintf("Binlog supported, response: %s", v)
	default:
		return true, fmt.Sprintf("Binlog may be supported, response type: %T", result)
	}
}

// GetBinlogLatestOffset 获取 Binlog 最新偏移量
// Tendis 2.7.0 使用 binlogpos <storeId> 命令
// storeId 默认使用 0（可以通过 INFO BinlogInfo 获取所有 store 的位置）
func GetBinlogLatestOffset(ctx context.Context, client *redis.Client) (uint64, error) {
	result, err := client.Do(ctx, "binlogpos", "0").Result()
	if err != nil {
		return 0, err
	}

	switch v := result.(type) {
	case int64:
		return uint64(v), nil
	case string:
		var offset uint64
		_, err := fmt.Sscanf(v, "%d", &offset)
		return offset, err
	default:
		return 0, fmt.Errorf("unexpected result type: %T", result)
	}
}

// GetBinlogPosForStore 获取指定 store 的 Binlog 位置
// Tendis 2.7.0 支持多个 store（通常 0-9）
func GetBinlogPosForStore(ctx context.Context, client redis.UniversalClient, storeID int) (uint64, error) {
	result, err := client.Do(ctx, "binlogpos", fmt.Sprintf("%d", storeID)).Result()
	if err != nil {
		return 0, err
	}

	switch v := result.(type) {
	case int64:
		return uint64(v), nil
	case string:
		var offset uint64
		_, err := fmt.Sscanf(v, "%d", &offset)
		return offset, err
	default:
		return 0, fmt.Errorf("unexpected result type: %T", result)
	}
}

// GetAllStoresBinlogPos 获取所有 store 的 Binlog 位置
// 返回 map[storeId]binlogPos
func GetAllStoresBinlogPos(ctx context.Context, client redis.UniversalClient) (map[int]uint64, error) {
	// 尝试从 INFO BinlogInfo 获取所有 store 的信息
	info, err := client.Info(ctx, "BinlogInfo").Result()
	if err != nil {
		// 如果 INFO BinlogInfo 不可用，尝试逐个获取
		result := make(map[int]uint64)
		for i := 0; i < 10; i++ {
			pos, err := GetBinlogPosForStore(ctx, client, i)
			if err != nil {
				break // 没有更多 store
			}
			result[i] = pos
		}
		if len(result) == 0 {
			return nil, fmt.Errorf("failed to get binlog positions")
		}
		return result, nil
	}

	// 解析 INFO BinlogInfo 输出
	// 格式: rocksdbN:min=xxx,save=xxx,BLWM=xxx,BHWM=xxx,remain=xxx
	result := make(map[int]uint64)
	lines := strings.Split(info, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "rocksdb") {
			// 解析 rocksdbN:min=xxx,save=xxx,BLWM=xxx,BHWM=xxx
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue
			}
			// 提取 store ID
			storeIDStr := strings.TrimPrefix(parts[0], "rocksdb")
			storeID, err := strconv.Atoi(storeIDStr)
			if err != nil {
				continue
			}
			// 解析 BHWM（高水位，即当前位置）
			fields := strings.Split(parts[1], ",")
			for _, field := range fields {
				if strings.HasPrefix(field, "BHWM=") {
					posStr := strings.TrimPrefix(field, "BHWM=")
					pos, err := strconv.ParseUint(posStr, 10, 64)
					if err == nil {
						result[storeID] = pos
					}
					break
				}
			}
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no binlog info found in INFO BinlogInfo output")
	}
	return result, nil
}

// BinlogEntry Binlog 条目
type BinlogEntry struct {
	Operation string // SET, DEL, HSET, etc.
	Key       string
	Value     string
	Field     string // for HSET
	Offset    uint64
}

// ReadBinlog 读取 Binlog 条目
func ReadBinlog(ctx context.Context, client *redis.Client, offset uint64, count int) ([]BinlogEntry, uint64, error) {
	result, err := client.Do(ctx, "binlog", "read", offset, count).Result()
	if err != nil {
		return nil, offset, err
	}

	entries := make([]BinlogEntry, 0)
	newOffset := offset

	// 解析 Binlog 返回值（格式可能因 Tendis 版本而异）
	switch v := result.(type) {
	case []interface{}:
		for _, item := range v {
			if entry, ok := parseBinlogEntry(item); ok {
				entries = append(entries, entry)
				if entry.Offset > newOffset {
					newOffset = entry.Offset
				}
			}
		}
	case string:
		// 可能是错误消息
		return nil, offset, fmt.Errorf("binlog read error: %s", v)
	}

	return entries, newOffset, nil
}

// parseBinlogEntry 解析单个 Binlog 条目
func parseBinlogEntry(item interface{}) (BinlogEntry, bool) {
	entry := BinlogEntry{}

	switch v := item.(type) {
	case []interface{}:
		if len(v) >= 3 {
			if op, ok := v[0].(string); ok {
				entry.Operation = op
			}
			if key, ok := v[1].(string); ok {
				entry.Key = key
			}
			if val, ok := v[2].(string); ok {
				entry.Value = val
			}
			if len(v) >= 4 {
				if offset, ok := v[3].(int64); ok {
					entry.Offset = uint64(offset)
				}
			}
			if len(v) >= 5 {
				if field, ok := v[4].(string); ok {
					entry.Field = field
				}
			}
			return entry, true
		}
	}

	return entry, false
}

// doIncrementalSyncWithBinlog 使用 Binlog 进行增量同步（P3 改进，如果 Tendis 支持）
// 相比时间窗口模式，Binlog 模式延迟更低，但需要 Tendis 支持
func doIncrementalSyncWithBinlog(
	ctx context.Context,
	task *Task,
	sourceClient, targetClient redis.UniversalClient,
	sourceIsCluster bool,
	taskLog *logger.TaskLogger,
) {
	taskLog.Info("Starting incremental sync with Binlog mode (P3)")

	// 首先检查是否支持 Binlog
	supported, msg := CheckTendisBinlogSupport(ctx, sourceClient)
	taskLog.Info("Binlog support check", map[string]interface{}{
		"supported": supported,
		"message":   msg,
	})

	if !supported {
		taskLog.Warn("Binlog not supported, falling back to time-window mode (V2)")
		// 回退到时间窗口模式
		doIncrementalSync(ctx, task, sourceClient, targetClient, sourceIsCluster, false, taskLog)
		return
	}

	// 加载 Binlog 断点
	checkpoint := loadBinlogCheckpoint(task.ID)
	nodeOffsets := make(map[string]uint64)
	keysSynced := int64(0)
	keysSkipped := int64(0)
	keysFailed := int64(0)

	if checkpoint != nil {
		nodeOffsets = checkpoint.NodeOffsets
		keysSynced = checkpoint.KeysSynced
		keysSkipped = checkpoint.KeysSkipped
		keysFailed = checkpoint.KeysFailed
		taskLog.Info("Resuming from Binlog checkpoint", map[string]interface{}{
			"keys_synced":  keysSynced,
			"node_offsets": nodeOffsets,
		})
	}

	// 同步间隔（Binlog 模式可以更短）
	syncInterval := 1 * time.Second
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	checkpointTicker := time.NewTicker(10 * time.Second)
	defer checkpointTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			saveBinlogCheckpoint(task.ID, nodeOffsets, keysSynced, keysSkipped, keysFailed)
			return

		case <-checkpointTicker.C:
			saveBinlogCheckpoint(task.ID, nodeOffsets, keysSynced, keysSkipped, keysFailed)

		case <-ticker.C:
			tasksMu.RLock()
			status := task.Status
			tasksMu.RUnlock()

			if status != "running" {
				saveBinlogCheckpoint(task.ID, nodeOffsets, keysSynced, keysSkipped, keysFailed)
				return
			}

			// 读取并处理 Binlog
			roundSynced, roundSkipped, roundFailed := processBinlogRound(
				ctx, task, sourceClient, targetClient, sourceIsCluster,
				nodeOffsets, taskLog,
			)

			keysSynced += roundSynced
			keysSkipped += roundSkipped
			keysFailed += roundFailed

			if roundSynced > 0 || roundSkipped > 0 || roundFailed > 0 {
				tasksMu.Lock()
				task.KeysMigrated += roundSynced
				task.KeysSkipped += roundSkipped
				task.KeysFailed += roundFailed
				task.UpdatedAt = time.Now().Format(time.RFC3339)
				tasksMu.Unlock()

				taskLog.Debug("Binlog sync round", map[string]interface{}{
					"synced":  roundSynced,
					"skipped": roundSkipped,
					"failed":  roundFailed,
				})
			}
		}
	}
}

// processBinlogRound 处理一轮 Binlog 读取
func processBinlogRound(
	ctx context.Context,
	task *Task,
	sourceClient, targetClient redis.UniversalClient,
	sourceIsCluster bool,
	nodeOffsets map[string]uint64,
	taskLog *logger.TaskLogger,
) (synced, skipped, failed int64) {

	if !sourceIsCluster {
		// 单机模式
		return processSingleNodeBinlog(ctx, task, sourceClient.(*redis.Client), targetClient, "standalone", nodeOffsets, taskLog)
	}

	// 集群模式
	clusterClient := sourceClient.(*redis.ClusterClient)
	var wg sync.WaitGroup
	var mu sync.Mutex

	clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		nodeAddr := node.Options().Addr

		wg.Add(1)
		go func() {
			defer wg.Done()

			nodeSynced, nodeSkipped, nodeFailed := processSingleNodeBinlog(
				ctx, task, node, targetClient, nodeAddr, nodeOffsets, taskLog,
			)

			mu.Lock()
			synced += nodeSynced
			skipped += nodeSkipped
			failed += nodeFailed
			mu.Unlock()
		}()

		return nil
	})

	wg.Wait()
	return
}

// processSingleNodeBinlog 处理单个节点的 Binlog
func processSingleNodeBinlog(
	ctx context.Context,
	task *Task,
	node *redis.Client,
	targetClient redis.UniversalClient,
	nodeAddr string,
	nodeOffsets map[string]uint64,
	taskLog *logger.TaskLogger,
) (synced, skipped, failed int64) {

	// 获取当前偏移量
	offset := nodeOffsets[nodeAddr]
	if offset == 0 {
		// 首次运行，获取最新偏移量
		var err error
		offset, err = GetBinlogLatestOffset(ctx, node)
		if err != nil {
			taskLog.Warn("Failed to get binlog offset", map[string]interface{}{
				"node":  nodeAddr,
				"error": err.Error(),
			})
			return
		}
		nodeOffsets[nodeAddr] = offset
	}

	// 读取 Binlog
	entries, newOffset, err := ReadBinlog(ctx, node, offset, 1000)
	if err != nil {
		taskLog.Warn("Failed to read binlog", map[string]interface{}{
			"node":   nodeAddr,
			"offset": offset,
			"error":  err.Error(),
		})
		return
	}

	// 处理 Binlog 条目
	for _, entry := range entries {
		// 【评审建议】RENAME/RENAMENX 边界处理
		// 如果源 Key 或目标 Key 任意一个匹配前缀，就同步该操作
		// 否则可能导致目标端数据不一致
		if entry.Operation == "RENAME" || entry.Operation == "RENAMENX" {
			srcKey := entry.Key
			dstKey := entry.Value // RENAME 的目标 Key 存储在 Value 字段
			srcMatch := matchKeyFilter(srcKey, task.Options)
			dstMatch := matchKeyFilter(dstKey, task.Options)
			
			if srcMatch || dstMatch {
				// 执行 RENAME
				err := targetClient.Rename(ctx, srcKey, dstKey).Err()
				if err != nil {
					// 如果目标端没有源 Key，先迁移再 rename
					if strings.Contains(err.Error(), "no such key") || strings.Contains(err.Error(), "ERR") {
						// 从源端获取 dstKey（因为源端已经 rename 完成了）
						migrated, _, reason := migrateKeyWithPolicy(ctx, node, targetClient, dstKey, "replace")
						if migrated {
							synced++
							taskLog.Debug("RENAME handled by migration", map[string]interface{}{
								"src_key": srcKey,
								"dst_key": dstKey,
							})
						} else if reason != "" && reason != "skipped" {
							failed++
							addErrorKey(task.ID, dstKey, "string", "failed", reason+" (rename)")
						}
					} else {
						taskLog.Warn("Failed to rename key", map[string]interface{}{
							"src_key": srcKey,
							"dst_key": dstKey,
							"error":   err.Error(),
						})
						failed++
					}
				} else {
					synced++
					taskLog.Debug("Key renamed", map[string]interface{}{
						"src_key": srcKey,
						"dst_key": dstKey,
					})
				}
			}
			continue // 处理完 RENAME，跳过后续
		}

		// 检查 Key 过滤（非 RENAME 操作）
		if !matchKeyFilter(entry.Key, task.Options) {
			continue
		}

		switch entry.Operation {
		case "SET", "SETEX", "SETNX", "HSET", "HMSET", "LPUSH", "RPUSH", "SADD", "ZADD":
			// 写操作：同步到目标端
			migrated, _, reason := migrateKeyWithPolicy(ctx, node, targetClient, entry.Key, "replace")
			if migrated {
				synced++
			} else if reason == "skipped" {
				skipped++
			} else if reason != "" {
				failed++
				addErrorKey(task.ID, entry.Key, "string", "failed", reason+" (binlog)")
			}

		case "DEL", "UNLINK":
			// 删除操作：在目标端也删除
			if err := targetClient.Del(ctx, entry.Key).Err(); err != nil {
				taskLog.Warn("Failed to delete key in target", map[string]interface{}{
					"key":   entry.Key,
					"error": err.Error(),
				})
			} else {
				synced++
			}

		case "EXPIRE", "EXPIREAT", "PEXPIRE", "PEXPIREAT":
			// 过期操作：同步到目标端
			migrated, _, reason := migrateKeyWithPolicy(ctx, node, targetClient, entry.Key, "replace")
			if migrated {
				synced++
			} else if reason != "" && reason != "skipped" {
				failed++
			}
		}
	}

	// 更新偏移量
	if newOffset > offset {
		nodeOffsets[nodeAddr] = newOffset
	}

	return
}

// saveBinlogCheckpoint 保存 Binlog 断点
func saveBinlogCheckpoint(taskID string, nodeOffsets map[string]uint64, keysSynced, keysSkipped, keysFailed int64) {
	checkpoint := &BinlogCheckpoint{
		TaskID:          taskID,
		NodeOffsets:     nodeOffsets,
		LastSyncTime:    time.Now().Format(time.RFC3339),
		KeysSynced:      keysSynced,
		KeysSkipped:     keysSkipped,
		KeysFailed:      keysFailed,
		BinlogSupported: true,
		UpdatedAt:       time.Now().Format(time.RFC3339),
	}

	binlogCheckpointsMu.Lock()
	binlogCheckpoints[taskID] = checkpoint
	binlogCheckpointsMu.Unlock()

	// 保存到文件
	checkpointDir := "./data/checkpoints"
	os.MkdirAll(checkpointDir, 0755)

	data, _ := json.MarshalIndent(checkpoint, "", "  ")
	os.WriteFile(fmt.Sprintf("%s/binlog-%s.json", checkpointDir, taskID), data, 0644)
}

// loadBinlogCheckpoint 加载 Binlog 断点
func loadBinlogCheckpoint(taskID string) *BinlogCheckpoint {
	binlogCheckpointsMu.RLock()
	if cp, ok := binlogCheckpoints[taskID]; ok {
		binlogCheckpointsMu.RUnlock()
		return cp
	}
	binlogCheckpointsMu.RUnlock()

	checkpointFile := fmt.Sprintf("./data/checkpoints/binlog-%s.json", taskID)
	data, err := os.ReadFile(checkpointFile)
	if err != nil {
		return nil
	}

	var checkpoint BinlogCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil
	}

	binlogCheckpointsMu.Lock()
	binlogCheckpoints[taskID] = &checkpoint
	binlogCheckpointsMu.Unlock()

	return &checkpoint
}

// ==================== P0 改进: ErrorKeys 上限提升 + 落盘机制 ====================

const (
	// P0 改进：提高内存上限到 10 万，总上限 100 万
	MaxErrorKeysInMemory = 100000   // 内存中最多存 10 万条
	MaxErrorKeysTotal    = 1000000  // 总共最多记录 100 万条（含落盘）
)

// ErrorKeysFileTracker 追踪每个任务的落盘文件
type ErrorKeysFileTracker struct {
	TaskID        string   `json:"task_id"`
	FileCount     int      `json:"file_count"`      // 已落盘文件数
	TotalInFiles  int64    `json:"total_in_files"`  // 文件中的总记录数
	Files         []string `json:"files"`           // 落盘文件列表
	LastFlushTime string   `json:"last_flush_time"` // 最后落盘时间
}

var (
	errorKeysTrackers   = make(map[string]*ErrorKeysFileTracker) // taskID -> tracker
	errorKeysTrackersMu sync.RWMutex
)

// addErrorKey 添加错误Key记录（P0 改进版：支持落盘）
func addErrorKey(taskID, key, keyType, reason, detail string) {
	errorKeyMu.Lock()
	defer errorKeyMu.Unlock()

	if errorKeys[taskID] == nil {
		errorKeys[taskID] = []ErrorKey{}
	}

	// 检查是否需要落盘（内存中超过上限）
	if len(errorKeys[taskID]) >= MaxErrorKeysInMemory {
		// 异步落盘，不阻塞当前操作
		keysToFlush := errorKeys[taskID]
		errorKeys[taskID] = make([]ErrorKey, 0, MaxErrorKeysInMemory/10)
		go flushErrorKeysBatch(taskID, keysToFlush)
	}

	// 检查是否超过总上限
	tracker := getOrCreateErrorKeysTracker(taskID)
	if tracker.TotalInFiles+int64(len(errorKeys[taskID])) >= MaxErrorKeysTotal {
		// 超过总上限，只记录到日志，不存储
		logger.Warn("Error keys exceeded total limit, only logging", map[string]interface{}{
			"task_id":        taskID,
			"key":            key,
			"reason":         reason,
			"total_in_files": tracker.TotalInFiles,
			"in_memory":      len(errorKeys[taskID]),
			"max_total":      MaxErrorKeysTotal,
		})
		return
	}

	// 添加到内存
	errorKeys[taskID] = append(errorKeys[taskID], ErrorKey{
		Key:       key,
		Type:      keyType,
		Reason:    reason,
		Detail:    detail,
		Timestamp: time.Now().Format(time.RFC3339),
	})
}

// getOrCreateErrorKeysTracker 获取或创建错误Key追踪器
func getOrCreateErrorKeysTracker(taskID string) *ErrorKeysFileTracker {
	errorKeysTrackersMu.Lock()
	defer errorKeysTrackersMu.Unlock()

	if tracker, ok := errorKeysTrackers[taskID]; ok {
		return tracker
	}

	tracker := &ErrorKeysFileTracker{
		TaskID:       taskID,
		FileCount:    0,
		TotalInFiles: 0,
		Files:        make([]string, 0),
	}
	errorKeysTrackers[taskID] = tracker

	// 尝试从磁盘加载已有的 tracker
	loadErrorKeysTracker(taskID, tracker)

	return tracker
}

// loadErrorKeysTracker 从磁盘加载 tracker
func loadErrorKeysTracker(taskID string, tracker *ErrorKeysFileTracker) {
	trackerFile := fmt.Sprintf("./data/error-keys/%s_tracker.json", taskID)
	data, err := os.ReadFile(trackerFile)
	if err != nil {
		return
	}

	var loaded ErrorKeysFileTracker
	if err := json.Unmarshal(data, &loaded); err != nil {
		return
	}

	tracker.FileCount = loaded.FileCount
	tracker.TotalInFiles = loaded.TotalInFiles
	tracker.Files = loaded.Files
	tracker.LastFlushTime = loaded.LastFlushTime
}

// saveErrorKeysTracker 保存 tracker 到磁盘
func saveErrorKeysTracker(taskID string, tracker *ErrorKeysFileTracker) {
	trackerFile := fmt.Sprintf("./data/error-keys/%s_tracker.json", taskID)
	os.MkdirAll("./data/error-keys", 0755)

	data, err := json.MarshalIndent(tracker, "", "  ")
	if err != nil {
		logger.Warn("Failed to marshal error keys tracker", map[string]interface{}{
			"task_id": taskID,
			"error":   err.Error(),
		})
		return
	}

	if err := os.WriteFile(trackerFile, data, 0644); err != nil {
		logger.Warn("Failed to save error keys tracker", map[string]interface{}{
			"task_id": taskID,
			"error":   err.Error(),
		})
	}
}

// flushErrorKeysBatch 批量落盘错误 Key（异步调用）
func flushErrorKeysBatch(taskID string, keys []ErrorKey) {
	if len(keys) == 0 {
		return
	}

	errorDir := "./data/error-keys"
	os.MkdirAll(errorDir, 0755)

	// 生成唯一文件名
	filename := fmt.Sprintf("%s/%s_batch_%d.json", errorDir, taskID, time.Now().UnixNano())

	data, err := json.Marshal(keys)
	if err != nil {
		logger.Warn("Failed to marshal error keys batch", map[string]interface{}{
			"task_id": taskID,
			"error":   err.Error(),
		})
		return
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		logger.Warn("Failed to write error keys batch", map[string]interface{}{
			"task_id":  taskID,
			"filename": filename,
			"error":    err.Error(),
		})
		return
	}

	// 更新 tracker
	errorKeysTrackersMu.Lock()
	tracker := errorKeysTrackers[taskID]
	if tracker == nil {
		tracker = &ErrorKeysFileTracker{
			TaskID: taskID,
			Files:  make([]string, 0),
		}
		errorKeysTrackers[taskID] = tracker
	}
	tracker.FileCount++
	tracker.TotalInFiles += int64(len(keys))
	tracker.Files = append(tracker.Files, filename)
	tracker.LastFlushTime = time.Now().Format(time.RFC3339)
	errorKeysTrackersMu.Unlock()

	// 保存 tracker
	saveErrorKeysTracker(taskID, tracker)

	logger.Info("Error keys batch flushed to disk", map[string]interface{}{
		"task_id":        taskID,
		"filename":       filename,
		"count":          len(keys),
		"total_in_files": tracker.TotalInFiles,
	})
}

// getErrorKeysStats 获取错误 Key 统计信息
func getErrorKeysStats(taskID string) map[string]interface{} {
	errorKeyMu.RLock()
	inMemory := len(errorKeys[taskID])
	errorKeyMu.RUnlock()

	errorKeysTrackersMu.RLock()
	tracker := errorKeysTrackers[taskID]
	var totalInFiles int64
	var fileCount int
	if tracker != nil {
		totalInFiles = tracker.TotalInFiles
		fileCount = tracker.FileCount
	}
	errorKeysTrackersMu.RUnlock()

	return map[string]interface{}{
		"in_memory":      inMemory,
		"total_in_files": totalInFiles,
		"file_count":     fileCount,
		"total":          int64(inMemory) + totalInFiles,
		"max_total":      MaxErrorKeysTotal,
	}
}

// getAllErrorKeys 获取所有错误 Key（包括落盘的）
func getAllErrorKeys(taskID string, limit int) []ErrorKey {
	errorKeyMu.RLock()
	memoryKeys := errorKeys[taskID]
	errorKeyMu.RUnlock()

	// 如果内存中的数据足够，直接返回
	if limit > 0 && len(memoryKeys) >= limit {
		return memoryKeys[:limit]
	}

	// 需要从文件加载更多
	allKeys := make([]ErrorKey, 0, len(memoryKeys))
	allKeys = append(allKeys, memoryKeys...)

	errorKeysTrackersMu.RLock()
	tracker := errorKeysTrackers[taskID]
	var files []string
	if tracker != nil {
		files = tracker.Files
	}
	errorKeysTrackersMu.RUnlock()

	// 从文件加载（按时间倒序，最新的先加载）
	for i := len(files) - 1; i >= 0 && (limit <= 0 || len(allKeys) < limit); i-- {
		data, err := os.ReadFile(files[i])
		if err != nil {
			continue
		}

		var fileKeys []ErrorKey
		if err := json.Unmarshal(data, &fileKeys); err != nil {
			continue
		}

		// 计算还需要多少
		need := limit - len(allKeys)
		if limit <= 0 {
			need = len(fileKeys)
		}
		if need > len(fileKeys) {
			need = len(fileKeys)
		}

		allKeys = append(allKeys, fileKeys[:need]...)
	}

	return allKeys
}

// ==================== 增量同步断点 ====================

// IncrementalSyncCheckpoint 增量同步断点（旧版本，保留兼容）
type IncrementalSyncCheckpoint struct {
	TaskID         string `json:"task_id"`
	LastSyncTime   string `json:"last_sync_time"`
	SyncedKeys     int64  `json:"synced_keys"`
	SkippedKeys    int64  `json:"skipped_keys"`
	TotalKnownKeys int64  `json:"total_known_keys"`
	UpdatedAt      string `json:"updated_at"`
}

// ==================== P1 改进: 时间窗口增量同步 V2 ====================

// IncrementalCheckpointV2 增量同步断点 V2（时间窗口模式，无需存储全量 Key）
type IncrementalCheckpointV2 struct {
	TaskID        string            `json:"task_id"`
	Version       int               `json:"version"`         // 版本号，V2 = 2
	NodeCursors   map[string]uint64 `json:"node_cursors"`    // 各节点的 SCAN cursor（用于断点续传）
	LastSyncTime  string            `json:"last_sync_time"`  // 上次同步完成时间
	SyncInterval  int               `json:"sync_interval"`   // 同步间隔（秒）
	KeysSynced    int64             `json:"keys_synced"`     // 累计同步的 Key 数
	KeysSkipped   int64             `json:"keys_skipped"`    // 累计跳过的 Key 数
	KeysFailed    int64             `json:"keys_failed"`     // 累计失败的 Key 数
	ScanRounds    int64             `json:"scan_rounds"`     // 已完成的扫描轮数
	UpdatedAt     string            `json:"updated_at"`      // 更新时间
	// P2 改进：详细进度指标
	LastRoundDuration  string `json:"last_round_duration"`   // 上一轮扫描耗时
	LastRoundSynced    int64  `json:"last_round_synced"`     // 上一轮同步的 Key 数
	LastRoundSkipped   int64  `json:"last_round_skipped"`    // 上一轮跳过的 Key 数
	AvgRoundDuration   string `json:"avg_round_duration"`    // 平均每轮扫描耗时
	EstimatedLag       string `json:"estimated_lag"`         // 估计的同步延迟
}

var (
	incrCheckpointsV2   = make(map[string]*IncrementalCheckpointV2)
	incrCheckpointsV2Mu sync.RWMutex
)

// saveIncrementalCheckpointV2 保存增量同步断点 V2
func saveIncrementalCheckpointV2(taskID string, checkpoint *IncrementalCheckpointV2) {
	checkpoint.Version = 2
	checkpoint.UpdatedAt = time.Now().Format(time.RFC3339)

	incrCheckpointsV2Mu.Lock()
	incrCheckpointsV2[taskID] = checkpoint
	incrCheckpointsV2Mu.Unlock()

	// 同时保存到文件
	checkpointDir := "./data/checkpoints"
	os.MkdirAll(checkpointDir, 0755)

	data, _ := json.MarshalIndent(checkpoint, "", "  ")
	os.WriteFile(fmt.Sprintf("%s/incr-v2-%s.json", checkpointDir, taskID), data, 0644)
}

// loadIncrementalCheckpointV2 加载增量同步断点 V2
func loadIncrementalCheckpointV2(taskID string) *IncrementalCheckpointV2 {
	// 先从内存加载
	incrCheckpointsV2Mu.RLock()
	if cp, ok := incrCheckpointsV2[taskID]; ok {
		incrCheckpointsV2Mu.RUnlock()
		return cp
	}
	incrCheckpointsV2Mu.RUnlock()

	// 从文件加载
	checkpointFile := fmt.Sprintf("./data/checkpoints/incr-v2-%s.json", taskID)
	data, err := os.ReadFile(checkpointFile)
	if err != nil {
		return nil
	}

	var checkpoint IncrementalCheckpointV2
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil
	}

	// 缓存到内存
	incrCheckpointsV2Mu.Lock()
	incrCheckpointsV2[taskID] = &checkpoint
	incrCheckpointsV2Mu.Unlock()

	return &checkpoint
}

var (
	incrCheckpoints   = make(map[string]*IncrementalSyncCheckpoint)
	incrCheckpointsMu sync.RWMutex
)

// saveIncrementalCheckpoint 保存增量同步断点
func saveIncrementalCheckpoint(taskID string, checkpoint *IncrementalSyncCheckpoint) {
	incrCheckpointsMu.Lock()
	defer incrCheckpointsMu.Unlock()
	incrCheckpoints[taskID] = checkpoint

	// 同时保存到文件
	checkpointDir := "./data/checkpoints"
	os.MkdirAll(checkpointDir, 0755)

	data, _ := json.Marshal(checkpoint)
	os.WriteFile(fmt.Sprintf("%s/incr-%s.json", checkpointDir, taskID), data, 0644)
}

// loadIncrementalCheckpoint 加载增量同步断点
func loadIncrementalCheckpoint(taskID string) *IncrementalSyncCheckpoint {
	// 先从内存加载
	incrCheckpointsMu.RLock()
	if cp, ok := incrCheckpoints[taskID]; ok {
		incrCheckpointsMu.RUnlock()
		return cp
	}
	incrCheckpointsMu.RUnlock()

	// 从文件加载
	checkpointFile := fmt.Sprintf("./data/checkpoints/incr-%s.json", taskID)
	data, err := os.ReadFile(checkpointFile)
	if err != nil {
		return nil
	}

	var checkpoint IncrementalSyncCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil
	}

	// 缓存到内存
	incrCheckpointsMu.Lock()
	incrCheckpoints[taskID] = &checkpoint
	incrCheckpointsMu.Unlock()

	return &checkpoint
}

// getIncrementalCheckpoint 获取增量同步断点（API 用）
func getIncrementalCheckpoint(taskID string) *IncrementalSyncCheckpoint {
	return loadIncrementalCheckpoint(taskID)
}

// ==================== 全量同步断点（SCAN cursor 持久化）====================

// saveFullSyncCheckpoint 保存全量同步断点
func saveFullSyncCheckpoint(taskID string, checkpoint *FullSyncCheckpoint) {
	fullSyncCheckpointsMu.Lock()
	fullSyncCheckpoints[taskID] = checkpoint
	fullSyncCheckpointsMu.Unlock()

	// 保存到文件
	checkpointDir := "./data/checkpoints"
	os.MkdirAll(checkpointDir, 0755)

	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		logger.Warn("Failed to marshal full sync checkpoint", map[string]interface{}{"error": err.Error()})
		return
	}
	os.WriteFile(fmt.Sprintf("%s/full-%s.json", checkpointDir, taskID), data, 0644)
}

// loadFullSyncCheckpoint 加载全量同步断点
func loadFullSyncCheckpoint(taskID string) *FullSyncCheckpoint {
	// 先从内存加载
	fullSyncCheckpointsMu.RLock()
	if cp, ok := fullSyncCheckpoints[taskID]; ok {
		fullSyncCheckpointsMu.RUnlock()
		return cp
	}
	fullSyncCheckpointsMu.RUnlock()

	// 从文件加载
	checkpointFile := fmt.Sprintf("./data/checkpoints/full-%s.json", taskID)
	data, err := os.ReadFile(checkpointFile)
	if err != nil {
		return nil
	}

	var checkpoint FullSyncCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil
	}

	// 缓存到内存
	fullSyncCheckpointsMu.Lock()
	fullSyncCheckpoints[taskID] = &checkpoint
	fullSyncCheckpointsMu.Unlock()

	return &checkpoint
}

// updateFullSyncCursor 更新单个节点的 cursor
func updateFullSyncCursor(taskID, nodeAddr string, cursor uint64, processedKeys int64) {
	fullSyncCheckpointsMu.Lock()
	defer fullSyncCheckpointsMu.Unlock()

	cp, ok := fullSyncCheckpoints[taskID]
	if !ok {
		cp = &FullSyncCheckpoint{
			TaskID:      taskID,
			NodeCursors: make(map[string]uint64),
			StartTime:   time.Now().Format(time.RFC3339),
			Phase:       "full",
		}
		fullSyncCheckpoints[taskID] = cp
	}

	cp.NodeCursors[nodeAddr] = cursor
	cp.ProcessedKeys = processedKeys
	cp.UpdatedAt = time.Now().Format(time.RFC3339)
}

// markFullSyncComplete 标记全量同步完成
func markFullSyncComplete(taskID string) {
	fullSyncCheckpointsMu.Lock()
	if cp, ok := fullSyncCheckpoints[taskID]; ok {
		cp.IsComplete = true
		cp.Phase = "incremental"
		cp.UpdatedAt = time.Now().Format(time.RFC3339)
	}
	fullSyncCheckpointsMu.Unlock()

	// 保存到文件
	if cp := fullSyncCheckpoints[taskID]; cp != nil {
		saveFullSyncCheckpoint(taskID, cp)
	}
}

// ==================== ErrorKeys 持久化 ====================

// saveErrorKeysToFile 保存错误 key 到文件
func saveErrorKeysToFile(taskID string) {
	errorKeyMu.RLock()
	keys := errorKeys[taskID]
	errorKeyMu.RUnlock()

	if len(keys) == 0 {
		return
	}

	errorDir := "./data/error-keys"
	os.MkdirAll(errorDir, 0755)

	data, err := json.MarshalIndent(keys, "", "  ")
	if err != nil {
		logger.Warn("Failed to marshal error keys", map[string]interface{}{"error": err.Error(), "task_id": taskID})
		return
	}

	os.WriteFile(fmt.Sprintf("%s/%s.json", errorDir, taskID), data, 0644)
}

// loadErrorKeysFromFile 从文件加载错误 key
func loadErrorKeysFromFile(taskID string) []ErrorKey {
	errorFile := fmt.Sprintf("./data/error-keys/%s.json", taskID)
	data, err := os.ReadFile(errorFile)
	if err != nil {
		return nil
	}

	var keys []ErrorKey
	if err := json.Unmarshal(data, &keys); err != nil {
		return nil
	}

	// 缓存到内存
	errorKeyMu.Lock()
	errorKeys[taskID] = keys
	errorKeyMu.Unlock()

	return keys
}

// saveAllErrorKeys 保存所有任务的错误 key（定期调用）
func saveAllErrorKeys() {
	errorKeyMu.RLock()
	taskIDs := make([]string, 0, len(errorKeys))
	for taskID := range errorKeys {
		taskIDs = append(taskIDs, taskID)
	}
	errorKeyMu.RUnlock()

	for _, taskID := range taskIDs {
		saveErrorKeysToFile(taskID)
	}
}

// ==================== 连续失败追踪和自动暂停 ====================

const (
	MaxConsecutiveFailures = 10  // 连续失败次数阈值
	FailureCooldownSeconds = 60  // 失败冷却时间（秒）
)

// getFailureTracker 获取或创建失败追踪器
func getFailureTracker(taskID string) *ConsecutiveFailureTracker {
	consecutiveFailuresMu.Lock()
	defer consecutiveFailuresMu.Unlock()

	if tracker, ok := consecutiveFailures[taskID]; ok {
		return tracker
	}

	tracker := &ConsecutiveFailureTracker{}
	consecutiveFailures[taskID] = tracker
	return tracker
}

// recordSourceFailure 记录源端失败
func recordSourceFailure(taskID string, taskLog *logger.TaskLogger) bool {
	tracker := getFailureTracker(taskID)
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	tracker.SourceFailures++
	tracker.LastSourceFailure = time.Now()

	// 检查是否需要自动暂停
	if tracker.SourceFailures >= MaxConsecutiveFailures {
		taskLog.Error("Too many consecutive source failures, auto-pausing task", map[string]interface{}{
			"consecutive_failures": tracker.SourceFailures,
			"threshold":            MaxConsecutiveFailures,
		})
		return true // 需要暂停
	}
	return false
}

// recordSourceSuccess 记录源端成功（重置计数器）
func recordSourceSuccess(taskID string) {
	tracker := getFailureTracker(taskID)
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	tracker.SourceFailures = 0
	tracker.LastSourceSuccess = time.Now()
}

// recordTargetFailure 记录目标端失败
func recordTargetFailure(taskID string, taskLog *logger.TaskLogger) bool {
	tracker := getFailureTracker(taskID)
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	tracker.TargetFailures++
	tracker.LastTargetFailure = time.Now()

	// 检查是否需要自动暂停
	if tracker.TargetFailures >= MaxConsecutiveFailures {
		taskLog.Error("Too many consecutive target failures, auto-pausing task", map[string]interface{}{
			"consecutive_failures": tracker.TargetFailures,
			"threshold":            MaxConsecutiveFailures,
		})
		return true // 需要暂停
	}
	return false
}

// recordTargetSuccess 记录目标端成功（重置计数器）
func recordTargetSuccess(taskID string) {
	tracker := getFailureTracker(taskID)
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	tracker.TargetFailures = 0
	tracker.LastTargetSuccess = time.Now()
}

// autoStopTask 自动暂停任务
func autoStopTask(taskID string, reason string, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[taskID]
	if ok && task.Status == "running" {
		now := time.Now()
		task.Status = "paused"
		task.PausedAt = now.Format(time.RFC3339)  // 记录暂停时间
		task.UpdatedAt = now.Format(time.RFC3339)
	}
	tasksMu.Unlock()

	if ok {
		taskLog.Error("Task auto-paused due to failures", map[string]interface{}{
			"reason":    reason,
			"paused_at": task.PausedAt,
		})

		// 启用自动恢复（当检测到集群恢复时自动继续任务）
		enableAutoRecoveryForTask(taskID, reason)

		// 保存状态
		saveTasksState()
		saveErrorKeysToFile(taskID)
	}
}

// getFailureStats 获取失败统计
func getFailureStats(taskID string) map[string]interface{} {
	tracker := getFailureTracker(taskID)
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	stats := map[string]interface{}{
		"source_consecutive_failures": tracker.SourceFailures,
		"target_consecutive_failures": tracker.TargetFailures,
		"threshold":                   MaxConsecutiveFailures,
	}

	if !tracker.LastSourceFailure.IsZero() {
		stats["last_source_failure"] = tracker.LastSourceFailure.Format(time.RFC3339)
	}
	if !tracker.LastTargetFailure.IsZero() {
		stats["last_target_failure"] = tracker.LastTargetFailure.Format(time.RFC3339)
	}
	if !tracker.LastSourceSuccess.IsZero() {
		stats["last_source_success"] = tracker.LastSourceSuccess.Format(time.RFC3339)
	}
	if !tracker.LastTargetSuccess.IsZero() {
		stats["last_target_success"] = tracker.LastTargetSuccess.Format(time.RFC3339)
	}

	return stats
}

// errorKeysHandler 获取错误Key列表
func errorKeysHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	errorKeyMu.RLock()
	keys := errorKeys[id]
	errorKeyMu.RUnlock()

	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 统计
	stats := map[string]int64{
		"total":      int64(len(keys)),
		"failed":     task.KeysFailed,
		"skipped":    task.KeysSkipped,
		"large_keys": 0,
	}

	// 只返回前100条
	items := keys
	if len(items) > 100 {
		items = items[:100]
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"stats": stats,
			"items": items,
		},
	})
}

// downloadErrorKeysHandler 下载错误Key CSV
func downloadErrorKeysHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	errorKeyMu.RLock()
	keys := errorKeys[id]
	errorKeyMu.RUnlock()

	// 生成CSV
	var sb strings.Builder
	sb.WriteString("Key,Type,Reason,Detail,Timestamp\n")
	for _, k := range keys {
		sb.WriteString(fmt.Sprintf("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n",
			strings.ReplaceAll(k.Key, "\"", "\"\""),
			k.Type,
			k.Reason,
			strings.ReplaceAll(k.Detail, "\"", "\"\""),
			k.Timestamp,
		))
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"error-keys-%s.csv\"", id[:8]))
	w.Write([]byte(sb.String()))

	log.Info("Error keys downloaded", map[string]interface{}{"task_id": id, "count": len(keys)})
}

func systemHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	running := 0
	totalActiveWorkers := 0
	totalTargetWorkers := 0

	// 问题6修复：统计真实的 Worker 数量
	tasksMu.RLock()
	for _, t := range tasks {
		if t.Status == "running" {
			running++
			// 获取 Worker 池的实际 Worker 数量
			if t.workerPool != nil {
				totalActiveWorkers += t.workerPool.GetActiveWorkerCount()
			}
			// 获取配置的目标 Worker 数量
			if t.Options != nil {
				totalTargetWorkers += t.Options.WorkerCount
			}
		}
	}
	tasksMu.RUnlock()

	log.Debug("System status queried")

	// 返回更详细的系统状态信息
	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"status":              "running",
			"active_workers":      totalActiveWorkers,  // 实际活跃 Worker 数量
			"target_workers":      totalTargetWorkers,  // 配置的目标 Worker 数量
			"worker_count":        totalActiveWorkers,  // 兼容旧前端
			"running_tasks":       running,
			"total_tasks":         len(tasks),
			"uptime":              time.Since(startTime).String(),
			"memory_usage":        int64(getMemoryUsage() * 1024 * 1024), // 转为字节
			"memory_mb":           getMemoryUsage(),
		},
	})
}

// ==================== 容错增强 API ====================

// healthDetailedHandler 详细健康检查
func healthDetailedHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	health := &HealthStatus{
		Status:        "healthy",
		MemoryUsageMB: getMemoryUsage(),
		Uptime:        time.Since(startTime).String(),
		Details:       make(map[string]interface{}),
	}

	// 统计运行中的任务
	var runningTasks []*Task
	tasksMu.RLock()
	for _, t := range tasks {
		if t.Status == "running" {
			runningTasks = append(runningTasks, t)
		}
	}
	tasksMu.RUnlock()

	// 检查每个运行中任务的连接状态
	var totalActiveWorkers, totalTargetWorkers int
	taskHealths := make([]map[string]interface{}, 0)

	for _, task := range runningTasks {
		taskHealth := map[string]interface{}{
			"task_id": task.ID,
			"name":    task.Name,
			"phase":   task.Phase,
		}

		// 检查源端连接
		sourceAddrs := strings.Split(task.SourceCluster, ",")
		sourceConnected := checkRedisConnection(ctx, sourceAddrs, task.SourcePassword)
		taskHealth["source_connected"] = sourceConnected

		// 检查目标端连接
		targetAddrs := strings.Split(task.TargetCluster, ",")
		targetConnected := checkRedisConnection(ctx, targetAddrs, task.TargetPassword)
		taskHealth["target_connected"] = targetConnected

		// Worker 状态
		if task.workerPool != nil {
			activeWorkers := task.workerPool.GetActiveWorkerCount()
			taskHealth["active_workers"] = activeWorkers
			totalActiveWorkers += activeWorkers
			if task.Options != nil {
				totalTargetWorkers += task.Options.WorkerCount
			}
		}

		if !sourceConnected || !targetConnected {
			health.Status = "degraded"
		}

		taskHealths = append(taskHealths, taskHealth)
	}

	health.SourceConnected = len(runningTasks) == 0 || health.Status != "unhealthy"
	health.TargetConnected = len(runningTasks) == 0 || health.Status != "unhealthy"
	health.ActiveWorkers = totalActiveWorkers
	health.TargetWorkers = totalTargetWorkers
	health.Details["running_tasks"] = len(runningTasks)
	health.Details["task_health"] = taskHealths

	log.Debug("Detailed health check", map[string]interface{}{
		"status":         health.Status,
		"running_tasks":  len(runningTasks),
		"active_workers": totalActiveWorkers,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    health,
	})
}

// checkRedisConnection 检查 Redis 连接
func checkRedisConnection(ctx context.Context, addrs []string, password string) bool {
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	
	// 尝试集群模式
	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		Password: password,
	})
	if err := clusterClient.Ping(ctx).Err(); err == nil {
		clusterClient.Close()
		return true
	}
	clusterClient.Close()

	// 尝试单机模式
	standaloneClient := redis.NewClient(&redis.Options{
		Addr:     addrs[0],
		Password: password,
	})
	if err := standaloneClient.Ping(ctx).Err(); err == nil {
		standaloneClient.Close()
		return true
	}
	standaloneClient.Close()

	return false
}

// systemBackupHandler 系统状态备份
func systemBackupHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 创建备份目录
	backupDir := "./data/backups"
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		log.Error("Failed to create backup directory", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 500, "message": "Failed to create backup directory"})
		return
	}

	timestamp := time.Now().Format("20060102-150405")
	backupFile := fmt.Sprintf("%s/tasks-backup-%s.json", backupDir, timestamp)

	// 导出任务状态
	tasksMu.RLock()
	tasksBackup := make(map[string]interface{})
	for id, task := range tasks {
		tasksBackup[id] = map[string]interface{}{
			"id":             task.ID,
			"name":           task.Name,
			"status":         task.Status,
			"phase":          task.Phase,
			"progress":       task.Progress,
			"source_cluster": task.SourceCluster,
			"target_cluster": task.TargetCluster,
			"migration_mode": task.MigrationMode,
			"keys_total":     task.KeysTotal,
			"keys_migrated":  task.KeysMigrated,
			"keys_failed":    task.KeysFailed,
			"keys_skipped":   task.KeysSkipped,
			"keys_filtered":  task.KeysFiltered,
			"bytes_migrated": task.BytesMigrated,
			"created_at":     task.CreatedAt,
			"updated_at":     task.UpdatedAt,
			"started_at":     task.StartedAt,
			"options":        task.Options,
		}
	}
	tasksMu.RUnlock()

	// 导出错误 key
	errorKeyMu.RLock()
	errorKeysBackup := make(map[string][]ErrorKey)
	for taskID, keys := range errorKeys {
		errorKeysBackup[taskID] = keys
	}
	errorKeyMu.RUnlock()

	backup := map[string]interface{}{
		"version":     "1.0",
		"timestamp":   time.Now().Format(time.RFC3339),
		"tasks":       tasksBackup,
		"error_keys":  errorKeysBackup,
		"uptime":      time.Since(startTime).String(),
	}

	data, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		log.Error("Failed to marshal backup", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 500, "message": "Failed to marshal backup"})
		return
	}

	if err := os.WriteFile(backupFile, data, 0644); err != nil {
		log.Error("Failed to write backup file", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 500, "message": "Failed to write backup file"})
		return
	}

	lastBackupTime = time.Now()

	log.Info("System backup created", map[string]interface{}{
		"backup_file": backupFile,
		"size":        len(data),
		"tasks":       len(tasksBackup),
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"backup_file":      backupFile,
			"size":             len(data),
			"tasks_count":      len(tasksBackup),
			"error_keys_count": len(errorKeysBackup),
		},
	})
}

// stopIncrementalHandler 停止增量同步（任务进入准备完成状态）
func stopIncrementalHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	if !ok {
		tasksMu.Unlock()
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	if task.Status != "running" || task.Phase != "incremental" {
		tasksMu.Unlock()
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": "Task is not in incremental sync phase",
		})
		return
	}

	// 设置任务状态为增量已停止
	task.Status = "incremental_stopped"
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	tasksMu.Unlock()

	taskLog.Info("Incremental sync stopped manually", map[string]interface{}{
		"task_id": id,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "Incremental sync stopped",
		"data": map[string]interface{}{
			"task_id":      id,
			"status":       task.Status,
			"next_step":    "Execute verify or mark as complete",
		},
	})
}

// completeTaskHandler 完成任务（停止增量同步并标记完成）
func completeTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	if !ok {
		tasksMu.Unlock()
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 只允许从 running(incremental)、incremental_stopped 或 paused 状态完成任务
	allowedStates := map[string]bool{
		"running":              task.Phase == "incremental",
		"incremental_stopped":  true,
		"paused":               true,
	}

	if !allowedStates[task.Status] {
		tasksMu.Unlock()
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": fmt.Sprintf("Cannot complete task in '%s' status", task.Status),
		})
		return
	}

	// 检查是否跳过校验
	skipVerify := r.URL.Query().Get("skip_verify") == "true"

	// 更新任务状态
	task.Status = "completed"
	task.Phase = "completed"
	task.Progress = 100
	task.CompletedAt = time.Now().Format(time.RFC3339)
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	tasksMu.Unlock()

	taskLog.Info("Task completed", map[string]interface{}{
		"task_id":      id,
		"skip_verify":  skipVerify,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "Task completed successfully",
		"data": map[string]interface{}{
			"task_id":      id,
			"status":       task.Status,
			"completed_at": task.CompletedAt,
			"skip_verify":  skipVerify,
		},
	})
}

// taskMetricsHandler 获取任务实时指标
func taskMetricsHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 获取详细进度指标
	metrics := getDetailedProgressMetrics(id, task)

	// 添加实时指标
	metrics["status"] = task.Status
	metrics["phase"] = task.Phase
	metrics["progress"] = task.Progress
	metrics["total_keys"] = task.KeysTotal
	metrics["processed_keys"] = task.KeysMigrated
	metrics["failed_keys"] = task.KeysFailed
	metrics["skipped_keys"] = task.KeysSkipped
	metrics["current_qps"] = task.Speed
	metrics["bytes_written"] = task.BytesMigrated

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    metrics,
	})
}

// systemWorkersHandler 获取系统 Worker 状态
func systemWorkersHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 收集所有任务的 Worker 信息
	tasksMu.RLock()
	var workers []map[string]interface{}
	for id, task := range tasks {
		if task.Status == "running" {
			workerInfo := map[string]interface{}{
				"task_id":        id,
				"task_name":      task.Name,
				"active_workers": task.ActiveWorkers,
				"status":         task.Status,
				"phase":          task.Phase,
			}
			
			if task.Options != nil {
				workerInfo["configured_workers"] = task.Options.WorkerCount
			}
			
			workers = append(workers, workerInfo)
		}
	}
	tasksMu.RUnlock()

	// 系统总体 Worker 统计
	totalActive := 0
	for _, w := range workers {
		if active, ok := w["active_workers"].(int); ok {
			totalActive += active
		}
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"total_active_workers": totalActive,
			"running_tasks":        len(workers),
			"workers":              workers,
		},
	})
}

// restartTaskHandler 重启任务（从失败状态恢复）
func restartTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	if !ok {
		tasksMu.Unlock()
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 只能重启失败或已完成的任务
	if task.Status != "failed" && task.Status != "completed" && task.Status != "paused" {
		tasksMu.Unlock()
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": fmt.Sprintf("Cannot restart task in '%s' status. Only 'failed', 'completed', or 'paused' tasks can be restarted.", task.Status),
		})
		return
	}

	// 解析请求体，检查是否只重试失败的 key
	var req struct {
		RetryFailedOnly bool `json:"retry_failed_only"`
		ResetProgress   bool `json:"reset_progress"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	previousStatus := task.Status
	previousProgress := task.Progress

	if req.ResetProgress {
		// 完全重置，从头开始
		task.KeysMigrated = 0
		task.KeysFailed = 0
		task.KeysSkipped = 0
		task.KeysFiltered = 0
		task.BytesMigrated = 0
		task.Progress = 0
		task.Phase = "full"

		// 清除错误 key 记录
		errorKeyMu.Lock()
		delete(errorKeys, id)
		errorKeyMu.Unlock()
	}

	task.Status = "running"
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	if task.StartedAt == "" {
		task.StartedAt = time.Now().Format("2006-01-02 15:04:05")
	}
	tasksMu.Unlock()

	log.Info("Task restarted", map[string]interface{}{
		"task_id":           id,
		"previous_status":   previousStatus,
		"previous_progress": previousProgress,
		"retry_failed_only": req.RetryFailedOnly,
		"reset_progress":    req.ResetProgress,
	})

	taskLog.Info("Task restarted", map[string]interface{}{
		"previous_status":   previousStatus,
		"retry_failed_only": req.RetryFailedOnly,
		"reset_progress":    req.ResetProgress,
	})

	// 启动迁移
	go simulateProgress(task)

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"task_id":         id,
			"previous_status": previousStatus,
			"new_status":      "running",
		},
	})
}

// retryFailedKeysHandler 重试失败的 key
func retryFailedKeysHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 获取失败的 key 列表
	errorKeyMu.RLock()
	failedKeys := errorKeys[id]
	errorKeyMu.RUnlock()

	if len(failedKeys) == 0 {
		jsonResponse(w, map[string]interface{}{
			"code":    0,
			"message": "No failed keys to retry",
			"data": map[string]interface{}{
				"retried": 0,
			},
		})
		return
	}

	// 解析请求参数
	var req struct {
		MaxRetries int `json:"max_retries"`
		BatchSize  int `json:"batch_size"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	if req.MaxRetries <= 0 {
		req.MaxRetries = 3
	}
	if req.BatchSize <= 0 {
		req.BatchSize = 100
	}

	// 限制批次大小
	keysToRetry := failedKeys
	if len(keysToRetry) > req.BatchSize {
		keysToRetry = keysToRetry[:req.BatchSize]
	}

	log.Info("Starting retry of failed keys", map[string]interface{}{
		"task_id":      id,
		"total_failed": len(failedKeys),
		"batch_size":   len(keysToRetry),
		"max_retries":  req.MaxRetries,
	})

	taskLog.Info("Retrying failed keys", map[string]interface{}{
		"count":       len(keysToRetry),
		"max_retries": req.MaxRetries,
	})

	// 异步重试
	go func() {
		retryFailedKeysAsync(task, keysToRetry, req.MaxRetries, taskLog)
	}()

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "Retry started",
		"data": map[string]interface{}{
			"keys_to_retry": len(keysToRetry),
			"total_failed":  len(failedKeys),
		},
	})
}

// retryFailedKeysAsync 异步重试失败的 key
func retryFailedKeysAsync(task *Task, keysToRetry []ErrorKey, maxRetries int, taskLog *logger.TaskLogger) {
	ctx := context.Background()

	// 连接 Redis
	sourceAddrs := strings.Split(task.SourceCluster, ",")
	targetAddrs := strings.Split(task.TargetCluster, ",")

	for i := range sourceAddrs {
		sourceAddrs[i] = strings.TrimSpace(sourceAddrs[i])
	}
	for i := range targetAddrs {
		targetAddrs[i] = strings.TrimSpace(targetAddrs[i])
	}

	sourceClient, _, err := connectRedis(ctx, sourceAddrs, task.SourcePassword)
	if err != nil {
		taskLog.Error("Failed to connect source for retry", map[string]interface{}{"error": err.Error()})
		return
	}
	defer sourceClient.Close()

	targetClient, _, err := connectRedis(ctx, targetAddrs, task.TargetPassword)
	if err != nil {
		taskLog.Error("Failed to connect target for retry", map[string]interface{}{"error": err.Error()})
		return
	}
	defer targetClient.Close()

	var successCount, failCount int64

	for _, errorKey := range keysToRetry {
		key := errorKey.Key
		var migrated bool
		var reason string

		for retry := 0; retry < maxRetries; retry++ {
			migrated, _, reason = migrateKeyWithPolicy(ctx, sourceClient, targetClient, key, "replace")
			if migrated {
				break
			}
			if reason == "skipped" {
				break
			}
			time.Sleep(time.Duration((retry+1)*100) * time.Millisecond)
		}

		if migrated {
			successCount++
			// 从错误列表中移除
			removeErrorKey(task.ID, key)

			tasksMu.Lock()
			task.KeysMigrated++
			task.KeysFailed--
			tasksMu.Unlock()
		} else {
			failCount++
			taskLog.Warn("Retry failed", map[string]interface{}{
				"key":    key,
				"reason": reason,
			})
		}
	}

	taskLog.Info("Retry completed", map[string]interface{}{
		"success": successCount,
		"failed":  failCount,
		"total":   len(keysToRetry),
	})
}

// removeErrorKey 从错误列表中移除 key
func removeErrorKey(taskID, key string) {
	errorKeyMu.Lock()
	defer errorKeyMu.Unlock()

	if keys, ok := errorKeys[taskID]; ok {
		newKeys := make([]ErrorKey, 0, len(keys))
		for _, k := range keys {
			if k.Key != key {
				newKeys = append(newKeys, k)
			}
		}
		errorKeys[taskID] = newKeys
	}
}

// taskHealthHandler 获取任务健康状态
func taskHealthHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	health := map[string]interface{}{
		"task_id":  id,
		"status":   task.Status,
		"phase":    task.Phase,
		"progress": task.Progress,
	}

	// 检查源端连接
	sourceAddrs := strings.Split(task.SourceCluster, ",")
	for i := range sourceAddrs {
		sourceAddrs[i] = strings.TrimSpace(sourceAddrs[i])
	}
	health["source_connected"] = checkRedisConnection(ctx, sourceAddrs, task.SourcePassword)

	// 检查目标端连接
	targetAddrs := strings.Split(task.TargetCluster, ",")
	for i := range targetAddrs {
		targetAddrs[i] = strings.TrimSpace(targetAddrs[i])
	}
	health["target_connected"] = checkRedisConnection(ctx, targetAddrs, task.TargetPassword)

	// Worker 状态
	if task.workerPool != nil {
		health["active_workers"] = task.workerPool.GetActiveWorkerCount()
		health["worker_status"] = task.workerPool.GetWorkerStatus()
	} else {
		health["active_workers"] = 0
	}

	// 错误统计
	errorKeyMu.RLock()
	errorCount := len(errorKeys[id])
	errorKeyMu.RUnlock()
	health["error_keys_count"] = errorCount

	// 重试状态
	retryStateMu.RLock()
	if retryState, ok := taskRetryState[id]; ok {
		health["retry_state"] = map[string]interface{}{
			"total_retries":   retryState.TotalRetries,
			"success_retries": retryState.SuccessRetries,
			"pending_retries": len(retryState.RetryQueue),
			"last_retry_time": retryState.LastRetryTime.Format(time.RFC3339),
		}
	}
	retryStateMu.RUnlock()

	// 连续失败统计
	health["failure_stats"] = getFailureStats(id)

	// 全量断点信息
	if checkpoint := loadFullSyncCheckpoint(id); checkpoint != nil {
		health["full_sync_checkpoint"] = map[string]interface{}{
			"is_complete":        checkpoint.IsComplete,
			"processed_keys":     checkpoint.ProcessedKeys,
			"total_scanned_keys": checkpoint.TotalScannedKeys,
			"nodes":              len(checkpoint.NodeCursors),
			"updated_at":         checkpoint.UpdatedAt,
		}
	}

	// 增量断点信息
	if checkpoint := loadIncrementalCheckpoint(id); checkpoint != nil {
		health["incr_sync_checkpoint"] = map[string]interface{}{
			"synced_keys":      checkpoint.SyncedKeys,
			"skipped_keys":     checkpoint.SkippedKeys,
			"total_known_keys": checkpoint.TotalKnownKeys,
			"last_sync_time":   checkpoint.LastSyncTime,
		}
	}

	log.Debug("Task health checked", map[string]interface{}{"task_id": id})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    health,
	})
}

// ==================== 测试连接 API ====================

func testConnectionHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Addrs    []string `json:"addrs"`
		Password string   `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Error("Failed to decode request", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Invalid request: " + err.Error()})
		return
	}

	// 过滤空地址
	var addrs []string
	for _, addr := range req.Addrs {
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "至少需要一个集群地址"})
		return
	}

	log.Info("Testing connection", map[string]interface{}{
		"addrs": addrs,
	})

	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 先尝试集群模式连接
	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		Password: req.Password,
	})
	defer clusterClient.Close()

	if err := clusterClient.Ping(ctx).Err(); err == nil {
		// 集群模式连接成功
		info := getClusterInfo(ctx, clusterClient)
		info["mode"] = "cluster"

		log.Info("Cluster connection successful", map[string]interface{}{
			"mode":       "cluster",
			"node_count": info["node_count"],
			"latency_ms": time.Since(startTime).Milliseconds(),
		})

		jsonResponse(w, map[string]interface{}{
			"code":    0,
			"message": "success",
			"data": map[string]interface{}{
				"success":      true,
				"message":      "集群连接成功",
				"cluster_info": info,
				"latency_ms":   time.Since(startTime).Milliseconds(),
			},
		})
		return
	}

	// 尝试单机模式连接
	standaloneClient := redis.NewClient(&redis.Options{
		Addr:     addrs[0],
		Password: req.Password,
	})
	defer standaloneClient.Close()

	if err := standaloneClient.Ping(ctx).Err(); err != nil {
		log.Error("Connection failed", map[string]interface{}{
			"error":      err.Error(),
			"latency_ms": time.Since(startTime).Milliseconds(),
		})

		jsonResponse(w, map[string]interface{}{
			"code":    0,
			"message": "success",
			"data": map[string]interface{}{
				"success":    false,
				"message":    "连接失败: " + err.Error(),
				"latency_ms": time.Since(startTime).Milliseconds(),
			},
		})
		return
	}

	// 单机模式连接成功
	info := getStandaloneInfo(ctx, standaloneClient, addrs[0])
	info["mode"] = "standalone"

	log.Info("Standalone connection successful", map[string]interface{}{
		"mode":       "standalone",
		"latency_ms": time.Since(startTime).Milliseconds(),
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"success":      true,
			"message":      "单机模式连接成功",
			"cluster_info": info,
			"latency_ms":   time.Since(startTime).Milliseconds(),
		},
	})
}

func getClusterInfo(ctx context.Context, client *redis.ClusterClient) map[string]interface{} {
	info := map[string]interface{}{
		"nodes":        []map[string]interface{}{},
		"node_count":   0,
		"total_keys":   int64(0),
		"total_memory": int64(0),
		"version":      "unknown",
	}

	var nodes []map[string]interface{}
	var totalKeys int64
	var totalMemory int64
	var version string
	var mu sync.Mutex

	// 获取集群节点信息
	client.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		nodeInfo := map[string]interface{}{
			"role": "master",
		}

		// 获取节点地址
		opts := node.Options()
		nodeInfo["addr"] = opts.Addr

		// 获取DBSize
		if dbsize, err := node.DBSize(ctx).Result(); err == nil {
			nodeInfo["keys"] = dbsize
			mu.Lock()
			totalKeys += dbsize
			mu.Unlock()
		}

		// 获取内存使用
		if memInfo, err := node.Info(ctx, "memory").Result(); err == nil {
			mem := parseMemoryFromInfo(memInfo)
			nodeInfo["memory"] = mem
			mu.Lock()
			totalMemory += mem
			mu.Unlock()
		}

		// 获取Redis版本（只需获取一次）
		mu.Lock()
		if version == "" {
			if serverInfo, err := node.Info(ctx, "server").Result(); err == nil {
				version = parseVersionFromInfo(serverInfo)
			}
		}
		mu.Unlock()

		mu.Lock()
		nodes = append(nodes, nodeInfo)
		mu.Unlock()
		return nil
	})

	info["nodes"] = nodes
	info["node_count"] = len(nodes)
	info["total_keys"] = totalKeys
	info["total_memory"] = totalMemory
	info["version"] = version

	return info
}

func getStandaloneInfo(ctx context.Context, client *redis.Client, addr string) map[string]interface{} {
	info := map[string]interface{}{
		"node_count":   1,
		"total_keys":   int64(0),
		"total_memory": int64(0),
		"version":      "unknown",
	}

	nodeInfo := map[string]interface{}{
		"addr": addr,
		"role": "master",
	}

	// 获取DBSize
	if dbsize, err := client.DBSize(ctx).Result(); err == nil {
		nodeInfo["keys"] = dbsize
		info["total_keys"] = dbsize
	}

	// 获取内存和版本信息
	if serverInfo, err := client.Info(ctx, "server").Result(); err == nil {
		info["version"] = parseVersionFromInfo(serverInfo)
	}
	if memInfo, err := client.Info(ctx, "memory").Result(); err == nil {
		mem := parseMemoryFromInfo(memInfo)
		nodeInfo["memory"] = mem
		info["total_memory"] = mem
	}

	info["nodes"] = []map[string]interface{}{nodeInfo}
	return info
}

func parseVersionFromInfo(info string) string {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "redis_version:") {
			return strings.TrimPrefix(line, "redis_version:")
		}
	}
	return "unknown"
}

func parseMemoryFromInfo(info string) int64 {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "used_memory:") {
			memStr := strings.TrimPrefix(line, "used_memory:")
			mem, _ := strconv.ParseInt(memStr, 10, 64)
			return mem
		}
	}
	return 0
}

// analyzeClusterHandler 分析集群详细信息
func analyzeClusterHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Addrs    []string `json:"addrs"`
		Password string   `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Invalid request"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	info, err := analyzeCluster(ctx, req.Addrs, req.Password)
	if err != nil {
		log.Error("Failed to analyze cluster", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 500, "message": err.Error()})
		return
	}

	log.Info("Cluster analyzed", map[string]interface{}{
		"total_keys": info.TotalKeys,
		"is_cluster": info.IsCluster,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    info,
	})
}

// analyzeCluster 分析集群详细信息
func analyzeCluster(ctx context.Context, addrs []string, password string) (*ClusterInfo, error) {
	info := &ClusterInfo{
		Addrs: addrs,
	}

	// 尝试集群模式
	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		Password: password,
	})

	if err := clusterClient.Ping(ctx).Err(); err == nil {
		info.IsCluster = true
		defer clusterClient.Close()

		var mu sync.Mutex
		var totalKeys int64
		var totalMemory int64
		var maxMemory int64
		var masterCount int
		var connectedClients int
		var instantaneousOPS int64
		var maxClients int

		clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			masterCount++

			// DBSize
			if dbsize, err := node.DBSize(ctx).Result(); err == nil {
				mu.Lock()
				totalKeys += dbsize
				mu.Unlock()
			}

			// Memory info
			if memInfo, err := node.Info(ctx, "memory").Result(); err == nil {
				mem := parseMemoryFromInfo(memInfo)
				maxMem := parseMaxMemoryFromInfo(memInfo)
				mu.Lock()
				totalMemory += mem
				if maxMem > maxMemory {
					maxMemory = maxMem
				}
				mu.Unlock()
			}

			// Stats info
			if statsInfo, err := node.Info(ctx, "stats").Result(); err == nil {
				ops := parseOPSFromInfo(statsInfo)
				mu.Lock()
				instantaneousOPS += ops
				mu.Unlock()
			}

			// Clients info
			if clientInfo, err := node.Info(ctx, "clients").Result(); err == nil {
				clients := parseConnectedClientsFromInfo(clientInfo)
				mu.Lock()
				connectedClients += clients
				mu.Unlock()
			}

			// Server info (version, maxclients)
			if serverInfo, err := node.Info(ctx, "server").Result(); err == nil {
				mu.Lock()
				if info.Version == "" {
					info.Version = parseVersionFromInfo(serverInfo)
				}
				mu.Unlock()
			}

			// Config maxclients
			if result, err := node.ConfigGet(ctx, "maxclients").Result(); err == nil && len(result) >= 2 {
				if mcStr, ok := result[1].(string); ok {
					mc, _ := strconv.Atoi(mcStr)
					mu.Lock()
					if mc > maxClients {
						maxClients = mc
					}
					mu.Unlock()
				}
			}

			return nil
		})

		info.MasterCount = masterCount
		info.TotalKeys = totalKeys
		info.UsedMemory = totalMemory
		info.UsedMemoryHuman = formatBytes(totalMemory)
		info.MaxMemory = maxMemory
		info.MaxClients = maxClients
		info.ConnectedClients = connectedClients
		info.InstantaneousOPS = instantaneousOPS

		// 估算平均key大小
		if totalKeys > 0 {
			info.AvgKeySize = totalMemory / totalKeys
		}

		return info, nil
	}
	clusterClient.Close()

	// 尝试单机模式
	standaloneClient := redis.NewClient(&redis.Options{
		Addr:     addrs[0],
		Password: password,
	})
	defer standaloneClient.Close()

	if err := standaloneClient.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	info.IsCluster = false
	info.MasterCount = 1

	if dbsize, err := standaloneClient.DBSize(ctx).Result(); err == nil {
		info.TotalKeys = dbsize
	}

	if memInfo, err := standaloneClient.Info(ctx, "memory").Result(); err == nil {
		info.UsedMemory = parseMemoryFromInfo(memInfo)
		info.UsedMemoryHuman = formatBytes(info.UsedMemory)
		info.MaxMemory = parseMaxMemoryFromInfo(memInfo)
	}

	if statsInfo, err := standaloneClient.Info(ctx, "stats").Result(); err == nil {
		info.InstantaneousOPS = parseOPSFromInfo(statsInfo)
	}

	if clientInfo, err := standaloneClient.Info(ctx, "clients").Result(); err == nil {
		info.ConnectedClients = parseConnectedClientsFromInfo(clientInfo)
	}

	if serverInfo, err := standaloneClient.Info(ctx, "server").Result(); err == nil {
		info.Version = parseVersionFromInfo(serverInfo)
	}

	if result, err := standaloneClient.ConfigGet(ctx, "maxclients").Result(); err == nil && len(result) >= 2 {
		if mcStr, ok := result[1].(string); ok {
			info.MaxClients, _ = strconv.Atoi(mcStr)
		}
	}

	if info.TotalKeys > 0 {
		info.AvgKeySize = info.UsedMemory / info.TotalKeys
	}

	return info, nil
}

// recommendConfigHandler 推荐配置
func recommendConfigHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SourceCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"source_cluster"`
		TargetCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"target_cluster"`
		// 用户提供的硬件参数（可选）
		HardwareInfo *HardwareInfo `json:"hardware_info,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Invalid request"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 分析源端集群
	sourceInfo, err := analyzeCluster(ctx, req.SourceCluster.Addrs, req.SourceCluster.Password)
	if err != nil {
		jsonResponse(w, map[string]interface{}{"code": 500, "message": "分析源端集群失败: " + err.Error()})
		return
	}

	// 分析目标端集群
	targetInfo, err := analyzeCluster(ctx, req.TargetCluster.Addrs, req.TargetCluster.Password)
	if err != nil {
		jsonResponse(w, map[string]interface{}{"code": 500, "message": "分析目标端集群失败: " + err.Error()})
		return
	}

	// 生成推荐配置
	config := generateRecommendedConfig(sourceInfo, targetInfo, req.HardwareInfo)

	log.Info("Config recommended", map[string]interface{}{
		"worker_count":    config.WorkerCount,
		"estimated_speed": config.EstimatedSpeed,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"source_info": sourceInfo,
			"target_info": targetInfo,
			"recommended": config,
		},
	})
}

// generateRecommendedConfig 生成推荐配置
func generateRecommendedConfig(source, target *ClusterInfo, hardware *HardwareInfo) *RecommendedConfig {
	config := &RecommendedConfig{
		ScanBatchSize:     1000,
		LargeKeyThreshold: 10 * 1024 * 1024, // 10MB
	}

	var reasons []string

	// 解析用户输入的 Key 大小级别
	// small(<1KB), medium(1-10KB), large(10-100KB), xlarge(>100KB)
	keySizeLevel := "small"
	if hardware != nil && hardware.KeySizeLevel != "" {
		keySizeLevel = hardware.KeySizeLevel
	}
	
	// 根据 Key 大小级别计算相关参数
	var avgKeySizeBytes int64   // 估算的平均 Key 大小（字节）
	var memPerWorkerMB int64    // 每个 Worker 预估内存消耗（MB）
	var maxWorkersByKeySize int64 = 512 // Key 大小对 Worker 数的限制
	var keySizeDesc string
	
	switch keySizeLevel {
	case "small":
		avgKeySizeBytes = 512       // 估算 0.5KB
		memPerWorkerMB = 10         // 小Key场景：每Worker约10MB
		maxWorkersByKeySize = 512   // 小Key可高并发
		keySizeDesc = "小Key(<1KB)"
	case "medium":
		avgKeySizeBytes = 5 * 1024  // 估算 5KB
		memPerWorkerMB = 20         // 中Key场景：每Worker约20MB
		maxWorkersByKeySize = 256   // 中Key适度并发
		keySizeDesc = "中Key(1-10KB)"
	case "large":
		avgKeySizeBytes = 50 * 1024 // 估算 50KB
		memPerWorkerMB = 50         // 大Key场景：每Worker约50MB
		maxWorkersByKeySize = 128   // 大Key限制并发
		keySizeDesc = "大Key(10-100KB)"
	case "xlarge":
		avgKeySizeBytes = 500 * 1024 // 估算 500KB
		memPerWorkerMB = 100         // 超大Key场景：每Worker约100MB
		maxWorkersByKeySize = 64     // 超大Key严格限制并发
		keySizeDesc = "超大Key(>100KB)"
	default:
		avgKeySizeBytes = 512
		memPerWorkerMB = 10
		maxWorkersByKeySize = 512
		keySizeDesc = "小Key(<1KB，默认)"
	}
	
	reasons = append(reasons, fmt.Sprintf("【Key大小】%s，每Worker约%dMB内存，最多%d个Worker", 
		keySizeDesc, memPerWorkerMB, maxWorkersByKeySize))

	// 1. 计算Worker数量（多因素综合考虑）
	// 因素1: 连接数限制（硬性瓶颈）
	sourceMaxConns := source.MaxClients - source.ConnectedClients
	targetMaxConns := target.MaxClients - target.ConnectedClients
	if sourceMaxConns < 100 {
		sourceMaxConns = 100
	}
	if targetMaxConns < 100 {
		targetMaxConns = 100
	}
	// 每个 Worker 需要约 3 个连接，预留 50% 给业务
	sourceWorkerLimit := sourceMaxConns / 6  // /3 连接 × /2 预留
	targetWorkerLimit := targetMaxConns / 6
	maxWorkersByConn := min(sourceWorkerLimit, targetWorkerLimit)
	reasons = append(reasons, fmt.Sprintf("【连接约束】源端可用%d÷6=%d，目标端可用%d÷6=%d，连接数允许最多%d个Worker",
		sourceMaxConns, sourceWorkerLimit, targetMaxConns, targetWorkerLimit, maxWorkersByConn))
	
	// 因素2: 内存约束（根据用户输入的 Key 大小动态估算）
	var maxWorkersByMemory int64 = 512 // 默认不限制
	if hardware != nil && hardware.MemoryGB > 0 {
		// 预留 2GB 给系统和 Go runtime
		availableMemMB := int64((hardware.MemoryGB * 1024) - 2048)
		if availableMemMB < 512 {
			availableMemMB = 512
		}
		
		maxWorkersByMemory = availableMemMB / memPerWorkerMB
		if maxWorkersByMemory < 16 {
			maxWorkersByMemory = 16
		}
		if maxWorkersByMemory > 512 {
			maxWorkersByMemory = 512
		}
		reasons = append(reasons, fmt.Sprintf("【内存约束】%dGB可用，每Worker约%dMB，最多%d个Worker", 
			hardware.MemoryGB, memPerWorkerMB, maxWorkersByMemory))
	}
	
	// 因素3: 带宽约束（根据用户输入的带宽和 Key 大小计算）
	var maxWorkersByBandwidth int64 = 512 // 默认不限制
	if hardware != nil && hardware.BandwidthMbps > 0 {
		// 带宽单位: Mbps -> MB/s (除以8)
		bandwidthMBps := int64(hardware.BandwidthMbps / 8)
		
		// 根据 Key 大小计算单 Worker 吞吐
		// 每个 Worker 处理 Key 的速度假设为 200~1000 keys/s（取决于 Key 大小）
		var keysPerSecond int64
		switch keySizeLevel {
		case "small":
			keysPerSecond = 1000
		case "medium":
			keysPerSecond = 500
		case "large":
			keysPerSecond = 200
		case "xlarge":
			keysPerSecond = 50
		default:
			keysPerSecond = 500
		}
		
		// 每个 Worker 带宽消耗 ≈ avgKeySize × keysPerSecond
		workerBandwidthKBps := (avgKeySizeBytes / 1024) * keysPerSecond
		if workerBandwidthKBps < 1 {
			workerBandwidthKBps = 1
		}
		workerBandwidthMBps := workerBandwidthKBps / 1024
		if workerBandwidthMBps < 1 {
			workerBandwidthMBps = 1
		}
		
		// 预留 30% 带宽给其他业务
		usableBandwidthMBps := bandwidthMBps * 70 / 100
		if usableBandwidthMBps < 1 {
			usableBandwidthMBps = 1
		}
		maxWorkersByBandwidth = usableBandwidthMBps / workerBandwidthMBps
		if maxWorkersByBandwidth < 8 {
			maxWorkersByBandwidth = 8
		}
		if maxWorkersByBandwidth > 512 {
			maxWorkersByBandwidth = 512
		}
		
		reasons = append(reasons, fmt.Sprintf("【带宽约束】%dMbps带宽，70%%可用=%dMB/s，每Worker约%dMB/s，最多%d个Worker", 
			hardware.BandwidthMbps, usableBandwidthMBps, workerBandwidthMBps, maxWorkersByBandwidth))
	}
	
	// 综合取最小值（只考虑真正的约束因素）
	recommendedWorkers := int64(maxWorkersByConn)
	limitingFactor := "连接数"
	
	if maxWorkersByKeySize < recommendedWorkers {
		recommendedWorkers = maxWorkersByKeySize
		limitingFactor = "Key大小"
	}
	if maxWorkersByMemory < recommendedWorkers {
		recommendedWorkers = maxWorkersByMemory
		limitingFactor = "可用内存"
	}
	if maxWorkersByBandwidth < recommendedWorkers {
		recommendedWorkers = maxWorkersByBandwidth
		limitingFactor = "网络带宽"
	}
	
	// 数据规模只用于参考显示，不参与 Worker 数计算
	reasons = append(reasons, fmt.Sprintf("【数据规模】%s Keys（仅供参考，不影响Worker推荐）", formatKeyCount(source.TotalKeys)))
	
	// 最小值保底
	if recommendedWorkers < 8 {
		recommendedWorkers = 8
	}
	// 最大值上限（系统支持1024，但默认推荐不超过512）
	if recommendedWorkers > 512 {
		recommendedWorkers = 512
	}
	
	config.WorkerCount = int(recommendedWorkers)
	reasons = append(reasons, fmt.Sprintf("【推荐Worker】%d（受限于%s，可手动调整，系统支持8~1024）", config.WorkerCount, limitingFactor))

	// 2. 计算QPS限制（通过QPS控制对源端/目标端的压力，而不是限制Worker数量）
	// 源端：预留资源给业务
	if source.InstantaneousOPS < 100 {
		// 几乎无业务，不限制
		config.SourceQPS = 0
		reasons = append(reasons, fmt.Sprintf("【源端QPS】当前OPS=%d(<100无业务)，不限制", source.InstantaneousOPS))
	} else {
		// 估算最大容量（假设当前业务只用了50%容量）
		estimatedMaxOPS := source.InstantaneousOPS * 2
		if estimatedMaxOPS < 50000 {
			estimatedMaxOPS = 50000 // 最低假设5万
		}
		config.SourceQPS = int(estimatedMaxOPS * 30 / 100) // 迁移使用30%
		reasons = append(reasons, fmt.Sprintf("【源端QPS】当前OPS=%d，预估容量=OPS×2=%d，迁移用30%%=%d（保护业务）", 
			source.InstantaneousOPS, estimatedMaxOPS, config.SourceQPS))
	}

	// 目标端：通常可以更激进
	if target.InstantaneousOPS < 100 {
		config.TargetQPS = 0
		reasons = append(reasons, fmt.Sprintf("【目标QPS】当前OPS=%d(<100无业务)，不限制", target.InstantaneousOPS))
	} else {
		estimatedMaxOPS := target.InstantaneousOPS * 2
		if estimatedMaxOPS < 50000 {
			estimatedMaxOPS = 50000
		}
		config.TargetQPS = int(estimatedMaxOPS * 50 / 100) // 使用50%
		reasons = append(reasons, fmt.Sprintf("【目标QPS】当前OPS=%d，预估容量=%d，迁移用50%%=%d",
			target.InstantaneousOPS, estimatedMaxOPS, config.TargetQPS))
	}

	// 3. 计算 ScanBatchSize（基于用户输入的 Key 大小）
	switch keySizeLevel {
	case "small":
		config.ScanBatchSize = 5000
		reasons = append(reasons, "【批次大小】小Key(<1KB)，用5000")
	case "medium":
		config.ScanBatchSize = 2000
		reasons = append(reasons, "【批次大小】中Key(1-10KB)，用2000")
	case "large":
		config.ScanBatchSize = 1000
		reasons = append(reasons, "【批次大小】大Key(10-100KB)，用1000")
	case "xlarge":
		config.ScanBatchSize = 500
		reasons = append(reasons, "【批次大小】超大Key(>100KB)，用500")
	default:
		config.ScanBatchSize = 2000
		reasons = append(reasons, "【批次大小】默认用2000")
	}

	// 4. 计算连接数
	// 每个Worker需要约3个连接
	config.SourceConnections = config.WorkerCount * 3
	config.TargetConnections = config.WorkerCount * 3

	// 确保不超过可用连接数的50%
	if config.SourceConnections > sourceMaxConns/2 {
		config.SourceConnections = sourceMaxConns / 2
	}
	if config.TargetConnections > targetMaxConns/2 {
		config.TargetConnections = targetMaxConns / 2
	}

	// 最小连接数
	if config.SourceConnections < 10 {
		config.SourceConnections = 10
	}
	if config.TargetConnections < 10 {
		config.TargetConnections = 10
	}

	// 5. 估算迁移速度和时间（基于用户输入的 Key 大小）
	var singleWorkerSpeed int64
	switch keySizeLevel {
	case "small":
		singleWorkerSpeed = 500
	case "medium":
		singleWorkerSpeed = 200
	case "large":
		singleWorkerSpeed = 50
	case "xlarge":
		singleWorkerSpeed = 10
	default:
		singleWorkerSpeed = 200
	}

	config.EstimatedSpeed = singleWorkerSpeed * int64(config.WorkerCount)

	// QPS限制可能成为瓶颈
	if config.SourceQPS > 0 && int64(config.SourceQPS) < config.EstimatedSpeed*4 {
		// 每个key需要约4次操作，QPS限制可能影响速度
		config.EstimatedSpeed = int64(config.SourceQPS) / 4
	}

	// 估算时间
	if config.EstimatedSpeed > 0 && source.TotalKeys > 0 {
		seconds := source.TotalKeys / config.EstimatedSpeed
		config.EstimatedTime = formatDuration(seconds)
	} else {
		config.EstimatedTime = "无法估算"
	}

	// 6. 大Key阈值（基于用户输入的 Key 大小）
	if keySizeLevel == "xlarge" {
		config.LargeKeyThreshold = 5 * 1024 * 1024 // 5MB
		reasons = append(reasons, "【大Key阈值】超大Key场景，阈值调为5MB")
	}

	config.Reason = strings.Join(reasons, "；")

	return config
}

// 辅助函数
func parseMaxMemoryFromInfo(info string) int64 {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "maxmemory:") {
			memStr := strings.TrimPrefix(line, "maxmemory:")
			mem, _ := strconv.ParseInt(memStr, 10, 64)
			return mem
		}
	}
	return 0
}

func parseOPSFromInfo(info string) int64 {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "instantaneous_ops_per_sec:") {
			opsStr := strings.TrimPrefix(line, "instantaneous_ops_per_sec:")
			ops, _ := strconv.ParseInt(opsStr, 10, 64)
			return ops
		}
	}
	return 0
}

func parseConnectedClientsFromInfo(info string) int {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "connected_clients:") {
			clientsStr := strings.TrimPrefix(line, "connected_clients:")
			clients, _ := strconv.Atoi(clientsStr)
			return clients
		}
	}
	return 0
}

func formatBytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	} else if bytes < 1024*1024 {
		return fmt.Sprintf("%.2f KB", float64(bytes)/1024)
	} else if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
	}
	return fmt.Sprintf("%.2f GB", float64(bytes)/(1024*1024*1024))
}

func formatDuration(seconds int64) string {
	if seconds < 60 {
		return fmt.Sprintf("%d秒", seconds)
	} else if seconds < 3600 {
		return fmt.Sprintf("%d分%d秒", seconds/60, seconds%60)
	} else if seconds < 86400 {
		return fmt.Sprintf("%d小时%d分", seconds/3600, (seconds%3600)/60)
	}
	return fmt.Sprintf("%d天%d小时", seconds/86400, (seconds%86400)/3600)
}

func formatKeyCount(count int64) string {
	if count < 10000 {
		return fmt.Sprintf("%d", count)
	} else if count < 10000000 {
		return fmt.Sprintf("%.1f万", float64(count)/10000)
	} else if count < 100000000 {
		return fmt.Sprintf("%.0f万", float64(count)/10000)
	} else if count < 10000000000 {
		return fmt.Sprintf("%.2f亿", float64(count)/100000000)
	}
	return fmt.Sprintf("%.1f亿", float64(count)/100000000)
}

func templatesHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	switch r.Method {
	case "GET":
		listTemplates(w, r, log)
	case "POST":
		createTemplate(w, r, log)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// listTemplates 获取模板列表
func listTemplates(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	templateMu.RLock()
	defer templateMu.RUnlock()

	items := make([]*TaskTemplate, 0, len(templates))
	for _, t := range templates {
		items = append(items, t)
	}

	// 按创建时间排序
	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt > items[j].CreatedAt
	})

	log.Debug("Templates listed", map[string]interface{}{"count": len(items)})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"items": items,
			"total": len(items),
		},
	})
}

// createTemplate 创建模板
func createTemplate(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	var req struct {
		Name          string `json:"name"`
		Description   string `json:"description"`
		SourceCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"source_cluster"`
		TargetCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"target_cluster"`
		MigrationMode string       `json:"migration_mode"`
		Options       *TaskOptions `json:"options"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Error("Failed to decode request", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 400, "message": err.Error()})
		return
	}

	if req.Name == "" {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "模板名称不能为空"})
		return
	}

	template := &TaskTemplate{
		ID:             uuid.New().String(),
		Name:           req.Name,
		Description:    req.Description,
		SourceCluster:  strings.Join(req.SourceCluster.Addrs, ","),
		TargetCluster:  strings.Join(req.TargetCluster.Addrs, ","),
		SourcePassword: req.SourceCluster.Password,
		TargetPassword: req.TargetCluster.Password,
		MigrationMode:  req.MigrationMode,
		Options:        req.Options,
		CreatedAt:      time.Now().Format(time.RFC3339),
		UpdatedAt:      time.Now().Format(time.RFC3339),
	}

	templateMu.Lock()
	templates[template.ID] = template
	templateMu.Unlock()

	log.Info("Template created", map[string]interface{}{
		"template_id":   template.ID,
		"template_name": template.Name,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    map[string]string{"template_id": template.ID},
	})
}

// templateHandler 处理单个模板请求
func templateHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/templates/")
	parts := strings.Split(path, "/")

	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	id := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	switch r.Method {
	case "GET":
		getTemplate(w, r, id, log)
	case "PUT":
		updateTemplate(w, r, id, log)
	case "DELETE":
		deleteTemplate(w, r, id, log)
	case "POST":
		if action == "create-task" {
			createTaskFromTemplate(w, r, id, log)
		} else {
			http.NotFound(w, r)
		}
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// getTemplate 获取模板详情
func getTemplate(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	templateMu.RLock()
	template, ok := templates[id]
	templateMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Template not found"})
		return
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    template,
	})
}

// updateTemplate 更新模板
func updateTemplate(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	templateMu.Lock()
	template, ok := templates[id]
	if !ok {
		templateMu.Unlock()
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Template not found"})
		return
	}

	var req struct {
		Name          string `json:"name"`
		Description   string `json:"description"`
		SourceCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"source_cluster"`
		TargetCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"target_cluster"`
		MigrationMode string       `json:"migration_mode"`
		Options       *TaskOptions `json:"options"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		templateMu.Unlock()
		jsonResponse(w, map[string]interface{}{"code": 400, "message": err.Error()})
		return
	}

	if req.Name != "" {
		template.Name = req.Name
	}
	if req.Description != "" {
		template.Description = req.Description
	}
	if len(req.SourceCluster.Addrs) > 0 {
		template.SourceCluster = strings.Join(req.SourceCluster.Addrs, ",")
		template.SourcePassword = req.SourceCluster.Password
	}
	if len(req.TargetCluster.Addrs) > 0 {
		template.TargetCluster = strings.Join(req.TargetCluster.Addrs, ",")
		template.TargetPassword = req.TargetCluster.Password
	}
	if req.MigrationMode != "" {
		template.MigrationMode = req.MigrationMode
	}
	if req.Options != nil {
		template.Options = req.Options
	}
	template.UpdatedAt = time.Now().Format(time.RFC3339)
	templateMu.Unlock()

	log.Info("Template updated", map[string]interface{}{"template_id": id})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
	})
}

// deleteTemplate 删除模板
func deleteTemplate(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	templateMu.Lock()
	_, ok := templates[id]
	if !ok {
		templateMu.Unlock()
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Template not found"})
		return
	}
	delete(templates, id)
	templateMu.Unlock()

	log.Info("Template deleted", map[string]interface{}{"template_id": id})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
	})
}

// createTaskFromTemplate 从模板创建任务
func createTaskFromTemplate(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	templateMu.RLock()
	template, ok := templates[id]
	templateMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Template not found"})
		return
	}

	// 可选：允许覆盖部分参数
	var req struct {
		Name string `json:"name"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	taskName := req.Name
	if taskName == "" {
		taskName = template.Name + "-" + time.Now().Format("0102-1504")
	}

	mode := template.MigrationMode
	if mode == "" {
		mode = "full_and_incremental"
	}

	options := template.Options
	if options == nil {
		options = &TaskOptions{
			WorkerCount:       4,
			ScanBatchSize:     1000,
			ConflictPolicy:    "skip_full_only",
			LargeKeyThreshold: 10485760,
			KeyFilter:         &KeyFilter{Mode: "all"},
		}
	} else if options.KeyFilter == nil || options.KeyFilter.Mode == "" {
		if options.KeyFilter == nil {
			options.KeyFilter = &KeyFilter{Mode: "all"}
		} else {
			options.KeyFilter.Mode = "all"
		}
	}

	task := &Task{
		ID:             uuid.New().String(),
		Name:           taskName,
		Status:         "pending",
		Progress:       0,
		SourceCluster:  template.SourceCluster,
		TargetCluster:  template.TargetCluster,
		SourcePassword: template.SourcePassword,
		TargetPassword: template.TargetPassword,
		MigrationMode:  mode,
		CreatedAt:      time.Now().Format(time.RFC3339),
		UpdatedAt:      time.Now().Format(time.RFC3339),
		Phase:          "full",
		Options:        options,
	}

	tasksMu.Lock()
	tasks[task.ID] = task
	tasksMu.Unlock()

	log.Info("Task created from template", map[string]interface{}{
		"task_id":     task.ID,
		"template_id": id,
		"task_name":   taskName,
	})

	logger.WithTask(task.ID).Info("Task created from template", map[string]interface{}{
		"name":        task.Name,
		"template_id": id,
		"source":      task.SourceCluster,
		"target":      task.TargetCluster,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    map[string]string{"task_id": task.ID},
	})
}

// ==================== 智能重试服务 ====================

const (
	// 默认配置
	DefaultHealthCheckIntervalSec   = 30  // 默认健康检测间隔（秒）
	DefaultPeriodicRetryIntervalSec = 300 // 默认定期重试间隔（秒）= 5 分钟
	DefaultPeriodicRetryBatchSize   = 100 // 默认每次定期重试的 Key 数量
	DefaultMaxAutoResumeAttempts    = 10  // 默认最大自动恢复尝试次数
)

// startSmartRetryService 启动智能重试后台服务
func startSmartRetryService() {
	logger.Info("🔄 Starting smart retry service", map[string]interface{}{
		"health_check_interval_sec":   DefaultHealthCheckIntervalSec,
		"periodic_retry_interval_sec": DefaultPeriodicRetryIntervalSec,
		"periodic_retry_batch_size":   DefaultPeriodicRetryBatchSize,
	})

	// 启动健康检测和自动恢复 goroutine
	go autoRecoveryLoop()

	// 启动定期重试失败 Key 的 goroutine
	go periodicRetryLoop()
}

// autoRecoveryLoop 自动恢复循环
// 定期检测暂停任务的集群健康状态，恢复后自动继续任务
func autoRecoveryLoop() {
	ticker := time.NewTicker(time.Duration(DefaultHealthCheckIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopSmartRetry:
			logger.Info("Auto recovery loop stopped")
			return
		case <-ticker.C:
			checkAndRecoverPausedTasks()
		}
	}
}

// periodicRetryLoop 定期重试循环
// 定期重试失败的 Key
func periodicRetryLoop() {
	ticker := time.NewTicker(time.Duration(DefaultPeriodicRetryIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopSmartRetry:
			logger.Info("Periodic retry loop stopped")
			return
		case <-ticker.C:
			retryFailedKeysForRunningTasks()
		}
	}
}

// checkAndRecoverPausedTasks 检查并恢复暂停的任务
func checkAndRecoverPausedTasks() {
	// 获取所有暂停的任务
	var pausedTasks []*Task
	tasksMu.RLock()
	for _, task := range tasks {
		if task.Status == "paused" {
			pausedTasks = append(pausedTasks, task)
		}
	}
	tasksMu.RUnlock()

	if len(pausedTasks) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, task := range pausedTasks {
		// 获取或创建自动恢复状态
		state := getOrCreateAutoRecoveryState(task.ID)

		// 检查是否启用自动恢复
		if !state.AutoResumeEnabled {
			continue
		}

		// 检查是否超过最大尝试次数
		if state.ResumeAttempts >= DefaultMaxAutoResumeAttempts {
			logger.Debug("Task exceeded max auto resume attempts", map[string]interface{}{
				"task_id":  task.ID,
				"attempts": state.ResumeAttempts,
				"max":      DefaultMaxAutoResumeAttempts,
			})
			continue
		}

		// 检查源端健康状态
		sourceAddrs := strings.Split(task.SourceCluster, ",")
		for i := range sourceAddrs {
			sourceAddrs[i] = strings.TrimSpace(sourceAddrs[i])
		}
		sourceHealthy := checkRedisConnection(ctx, sourceAddrs, task.SourcePassword)

		// 检查目标端健康状态
		targetAddrs := strings.Split(task.TargetCluster, ",")
		for i := range targetAddrs {
			targetAddrs[i] = strings.TrimSpace(targetAddrs[i])
		}
		targetHealthy := checkRedisConnection(ctx, targetAddrs, task.TargetPassword)

		// 更新健康状态
		autoRecoveryStatesMu.Lock()
		state.SourceHealthy = sourceHealthy
		state.TargetHealthy = targetHealthy
		state.LastHealthCheck = time.Now()
		autoRecoveryStatesMu.Unlock()

		// 如果两端都健康，尝试自动恢复
		if sourceHealthy && targetHealthy {
			logger.Info("🔄 Attempting auto recovery", map[string]interface{}{
				"task_id":        task.ID,
				"task_name":      task.Name,
				"resume_attempt": state.ResumeAttempts + 1,
			})

			// 增加尝试次数
			autoRecoveryStatesMu.Lock()
			state.ResumeAttempts++
			state.LastResumeAttempt = time.Now()
			autoRecoveryStatesMu.Unlock()

			// 尝试恢复任务
			if err := autoResumeTask(task); err != nil {
				logger.Warn("Auto recovery failed", map[string]interface{}{
					"task_id": task.ID,
					"error":   err.Error(),
				})
			} else {
				logger.Info("✅ Auto recovery succeeded", map[string]interface{}{
					"task_id":   task.ID,
					"task_name": task.Name,
				})

				// 重置自动恢复状态
				autoRecoveryStatesMu.Lock()
				state.ResumeAttempts = 0
				state.AutoResumeEnabled = true
				autoRecoveryStatesMu.Unlock()

				// 重置连续失败计数器
				resetFailureTracker(task.ID)
			}
		} else {
			logger.Debug("Task cluster not healthy yet", map[string]interface{}{
				"task_id":        task.ID,
				"source_healthy": sourceHealthy,
				"target_healthy": targetHealthy,
			})
		}
	}
}

// getOrCreateAutoRecoveryState 获取或创建自动恢复状态
func getOrCreateAutoRecoveryState(taskID string) *AutoRecoveryState {
	autoRecoveryStatesMu.Lock()
	defer autoRecoveryStatesMu.Unlock()

	if state, ok := autoRecoveryStates[taskID]; ok {
		return state
	}

	state := &AutoRecoveryState{
		TaskID:            taskID,
		PausedAt:          time.Now(),
		AutoResumeEnabled: true, // 默认启用自动恢复
	}
	autoRecoveryStates[taskID] = state
	return state
}

// autoResumeTask 自动恢复任务
func autoResumeTask(task *Task) error {
	tasksMu.Lock()
	defer tasksMu.Unlock()

	if task.Status != "paused" {
		return fmt.Errorf("task is not paused, current status: %s", task.Status)
	}

	task.Status = "running"
	task.UpdatedAt = time.Now().Format(time.RFC3339)

	taskLog := logger.WithTask(task.ID)
	taskLog.Info("Task auto-resumed", map[string]interface{}{
		"task_name": task.Name,
		"phase":     task.Phase,
		"progress":  task.Progress,
	})

	// 启动迁移（在锁外执行）
	go func() {
		simulateProgress(task)
	}()

	return nil
}

// resetFailureTracker 重置失败追踪器
func resetFailureTracker(taskID string) {
	tracker := getFailureTracker(taskID)
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	tracker.SourceFailures = 0
	tracker.TargetFailures = 0
	tracker.LastSourceSuccess = time.Now()
	tracker.LastTargetSuccess = time.Now()
}

// retryFailedKeysForRunningTasks 为运行中的任务重试失败的 Key
func retryFailedKeysForRunningTasks() {
	// 获取所有运行中的任务
	var runningTasks []*Task
	tasksMu.RLock()
	for _, task := range tasks {
		if task.Status == "running" {
			runningTasks = append(runningTasks, task)
		}
	}
	tasksMu.RUnlock()

	if len(runningTasks) == 0 {
		return
	}

	for _, task := range runningTasks {
		// 获取失败的 Key 列表
		errorKeyMu.RLock()
		failedKeys := errorKeys[task.ID]
		errorKeyMu.RUnlock()

		if len(failedKeys) == 0 {
			continue
		}

		// 限制每次重试的数量
		keysToRetry := failedKeys
		if len(keysToRetry) > DefaultPeriodicRetryBatchSize {
			keysToRetry = keysToRetry[:DefaultPeriodicRetryBatchSize]
		}

		taskLog := logger.WithTask(task.ID)
		taskLog.Info("🔄 Starting periodic retry of failed keys", map[string]interface{}{
			"total_failed": len(failedKeys),
			"batch_size":   len(keysToRetry),
		})

		// 异步重试
		go func(t *Task, keys []ErrorKey) {
			retryFailedKeysAsync(t, keys, 3, logger.WithTask(t.ID))
		}(task, keysToRetry)
	}
}

// enableAutoRecoveryForTask 为任务启用自动恢复
func enableAutoRecoveryForTask(taskID string, pauseReason string) {
	autoRecoveryStatesMu.Lock()
	defer autoRecoveryStatesMu.Unlock()

	state := &AutoRecoveryState{
		TaskID:            taskID,
		PausedAt:          time.Now(),
		PauseReason:       pauseReason,
		AutoResumeEnabled: true,
		ResumeAttempts:    0,
	}
	autoRecoveryStates[taskID] = state

	logger.Info("Auto recovery enabled for task", map[string]interface{}{
		"task_id":      taskID,
		"pause_reason": pauseReason,
	})
}

// disableAutoRecoveryForTask 禁用任务的自动恢复
func disableAutoRecoveryForTask(taskID string) {
	autoRecoveryStatesMu.Lock()
	defer autoRecoveryStatesMu.Unlock()

	if state, ok := autoRecoveryStates[taskID]; ok {
		state.AutoResumeEnabled = false
	}
}

// getAutoRecoveryStatus 获取自动恢复状态
func getAutoRecoveryStatus(taskID string) map[string]interface{} {
	autoRecoveryStatesMu.RLock()
	defer autoRecoveryStatesMu.RUnlock()

	if state, ok := autoRecoveryStates[taskID]; ok {
		result := map[string]interface{}{
			"enabled":             state.AutoResumeEnabled,
			"paused_at":           state.PausedAt.Format(time.RFC3339),
			"pause_reason":        state.PauseReason,
			"resume_attempts":     state.ResumeAttempts,
			"max_resume_attempts": DefaultMaxAutoResumeAttempts,
			"source_healthy":      state.SourceHealthy,
			"target_healthy":      state.TargetHealthy,
		}
		if !state.LastResumeAttempt.IsZero() {
			result["last_resume_attempt"] = state.LastResumeAttempt.Format(time.RFC3339)
		}
		if !state.LastHealthCheck.IsZero() {
			result["last_health_check"] = state.LastHealthCheck.Format(time.RFC3339)
		}
		return result
	}

	return map[string]interface{}{
		"enabled": false,
		"message": "Auto recovery not configured for this task",
	}
}

// ==================== 自动恢复 API ====================

// autoRecoveryStatusHandler 获取自动恢复状态
func autoRecoveryStatusHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	_, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	status := getAutoRecoveryStatus(id)
	status["task_id"] = id

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    status,
	})
}

// toggleAutoRecoveryHandler 启用/禁用自动恢复
func toggleAutoRecoveryHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": err.Error()})
		return
	}

	if req.Enabled {
		enableAutoRecoveryForTask(task.ID, "manual enable")
	} else {
		disableAutoRecoveryForTask(task.ID)
	}

	log.Info("Auto recovery toggled", map[string]interface{}{
		"task_id": id,
		"enabled": req.Enabled,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"task_id": id,
			"enabled": req.Enabled,
		},
	})
}

// ==================== 影子模式 API ====================

// shadowStatsHandler 获取影子模式统计
func shadowStatsHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 检查是否为影子模式
	if task.Options == nil || !task.Options.ShadowMode {
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": "Task is not in shadow mode",
		})
		return
	}

	// 获取影子模式统计
	var shadowStats *ShadowModeStats
	if task.workerPool != nil {
		shadowStats = task.workerPool.GetShadowStats()
	}

	if shadowStats == nil {
		shadowStats = &ShadowModeStats{
			TypeDistribution: make(map[string]int64),
		}
	}

	// 计算预估迁移时间（基于当前速度）
	if task.Speed > 0 && task.KeysTotal > 0 {
		remainingKeys := task.KeysTotal - shadowStats.KeysMatched
		if remainingKeys > 0 {
			estimatedSeconds := remainingKeys / task.Speed
			hours := estimatedSeconds / 3600
			minutes := (estimatedSeconds % 3600) / 60
			shadowStats.EstimatedTime = fmt.Sprintf("%dh %dm", hours, minutes)
		} else {
			shadowStats.EstimatedTime = "已完成"
		}
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    shadowStats,
	})
}

// ==================== 任务配置导出/导入 API ====================

// exportTaskReportHandler 导出任务迁移报告
func exportTaskReportHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	format := r.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}

	// 构建报告数据
	report := map[string]interface{}{
		"task_id":         task.ID,
		"task_name":       task.Name,
		"status":          task.Status,
		"source_cluster":  task.SourceCluster,
		"target_cluster":  task.TargetCluster,
		"migration_mode":  task.MigrationMode,
		"created_at":      task.CreatedAt,
		"updated_at":      task.UpdatedAt,
		"report_time":     time.Now().Format("2006-01-02 15:04:05"),
		"progress": map[string]interface{}{
			"phase":          task.Phase,
			"total_keys":     task.KeysTotal,
			"migrated_keys":  task.KeysMigrated,
			"skipped_keys":   task.KeysSkipped,
			"failed_keys":    task.KeysFailed,
			"filtered_keys":  task.KeysFiltered,
			"speed":          task.Speed,
			"progress":       task.Progress,
			"bytes_migrated": task.BytesMigrated,
			"bytes_total":    task.BytesTotal,
		},
	}

	// 获取错误 Key 信息
	errorKeyMu.RLock()
	errorKeyList := errorKeys[id]
	errorKeyMu.RUnlock()

	if len(errorKeyList) > 0 {
		sampleSize := len(errorKeyList)
		if sampleSize > 10 {
			sampleSize = 10
		}
		report["error_summary"] = map[string]interface{}{
			"total_errors":  len(errorKeyList),
			"sample_errors": errorKeyList[:sampleSize],
		}
	}

	switch format {
	case "csv":
		exportTaskReportCSV(w, id, task, log)
	case "json":
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"task-report-%s.json\"", id[:8]))
		json.NewEncoder(w).Encode(report)
	default:
		jsonResponse(w, map[string]interface{}{
			"code":    0,
			"message": "success",
			"data":    report,
		})
	}
}

// exportTaskReportCSV 导出 CSV 格式报告
func exportTaskReportCSV(w http.ResponseWriter, id string, task *Task, log *logger.RequestLogger) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"task-report-%s.csv\"", id[:8]))

	// 写入 BOM 头（支持 Excel 正确识别 UTF-8）
	w.Write([]byte{0xEF, 0xBB, 0xBF})

	var sb strings.Builder

	// 基本信息
	sb.WriteString("=== 任务基本信息 ===\n")
	sb.WriteString("字段,值\n")
	sb.WriteString(fmt.Sprintf("任务ID,%s\n", task.ID))
	sb.WriteString(fmt.Sprintf("任务名称,%s\n", escapeCSV(task.Name)))
	sb.WriteString(fmt.Sprintf("状态,%s\n", task.Status))
	sb.WriteString(fmt.Sprintf("源集群,%s\n", escapeCSV(task.SourceCluster)))
	sb.WriteString(fmt.Sprintf("目标集群,%s\n", escapeCSV(task.TargetCluster)))
	sb.WriteString(fmt.Sprintf("迁移模式,%s\n", task.MigrationMode))
	sb.WriteString(fmt.Sprintf("创建时间,%s\n", task.CreatedAt))
	sb.WriteString(fmt.Sprintf("更新时间,%s\n", task.UpdatedAt))
	sb.WriteString(fmt.Sprintf("报告生成时间,%s\n", time.Now().Format("2006-01-02 15:04:05")))
	sb.WriteString("\n")

	// 进度信息
	sb.WriteString("=== 迁移进度 ===\n")
	sb.WriteString("字段,值\n")
	sb.WriteString(fmt.Sprintf("当前阶段,%s\n", task.Phase))
	sb.WriteString(fmt.Sprintf("总 Key 数,%d\n", task.KeysTotal))
	sb.WriteString(fmt.Sprintf("已迁移 Key,%d\n", task.KeysMigrated))
	sb.WriteString(fmt.Sprintf("跳过 Key,%d\n", task.KeysSkipped))
	sb.WriteString(fmt.Sprintf("失败 Key,%d\n", task.KeysFailed))
	sb.WriteString(fmt.Sprintf("过滤 Key,%d\n", task.KeysFiltered))
	sb.WriteString(fmt.Sprintf("迁移速度,%d keys/s\n", task.Speed))
	sb.WriteString(fmt.Sprintf("进度百分比,%.2f%%\n", task.Progress))
	sb.WriteString(fmt.Sprintf("已迁移数据量,%s\n", formatBytesSize(task.BytesMigrated)))
	sb.WriteString(fmt.Sprintf("总数据量,%s\n", formatBytesSize(task.BytesTotal)))
	sb.WriteString("\n")

	// 错误 Key 列表
	errorKeyMu.RLock()
	errorKeyList := errorKeys[id]
	errorKeyMu.RUnlock()

	if len(errorKeyList) > 0 {
		sb.WriteString("=== 错误 Key 列表 ===\n")
		sb.WriteString("序号,Key,类型,原因,详情,时间\n")
		for i, ek := range errorKeyList {
			if i >= 1000 {
				sb.WriteString(fmt.Sprintf("...还有 %d 个错误 Key 未列出\n", len(errorKeyList)-1000))
				break
			}
			sb.WriteString(fmt.Sprintf("%d,%s,%s,%s,%s,%s\n", 
				i+1, 
				escapeCSV(ek.Key), 
				escapeCSV(ek.Type),
				escapeCSV(ek.Reason), 
				escapeCSV(ek.Detail),
				ek.Timestamp))
		}
		sb.WriteString("\n")
	}

	// 配置信息
	if task.Options != nil {
		sb.WriteString("=== 迁移配置 ===\n")
		sb.WriteString("字段,值\n")
		sb.WriteString(fmt.Sprintf("并发数,%d\n", task.Options.WorkerCount))
		sb.WriteString(fmt.Sprintf("批次大小,%d\n", task.Options.ScanBatchSize))
		sb.WriteString(fmt.Sprintf("冲突策略,%s\n", task.Options.ConflictPolicy))
		sb.WriteString(fmt.Sprintf("影子模式,%v\n", task.Options.ShadowMode))
		if task.Options.KeyFilter != nil {
			sb.WriteString(fmt.Sprintf("包含前缀,%s\n", strings.Join(task.Options.KeyFilter.Prefixes, ";")))
			sb.WriteString(fmt.Sprintf("排除前缀,%s\n", strings.Join(task.Options.KeyFilter.ExcludePrefixes, ";")))
		}
	}

	w.Write([]byte(sb.String()))

	log.Info("Task report exported as CSV", map[string]interface{}{
		"task_id": id,
	})
}

// escapeCSV 转义 CSV 中的特殊字符
func escapeCSV(s string) string {
	if strings.ContainsAny(s, ",\"\n\r") {
		return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\""
	}
	return s
}

// formatBytesSize 格式化字节大小
func formatBytesSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// exportTaskConfigHandler 导出任务配置
func exportTaskConfigHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 构建导出配置
	exportConfig := map[string]interface{}{
		"name":            task.Name,
		"source_cluster":  task.SourceCluster,
		"target_cluster":  task.TargetCluster,
		"migration_mode":  task.MigrationMode,
		"options":         task.Options,
		"exported_at":     time.Now().Format("2006-01-02 15:04:05"),
		"export_version":  "1.0",
	}

	// 根据格式返回
	format := r.URL.Query().Get("format")
	if format == "file" {
		// 作为文件下载
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"task-config-%s.json\"", id[:8]))
		json.NewEncoder(w).Encode(exportConfig)
		return
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    exportConfig,
	})
}

// ==================== Key 清单上传/解析 API ====================

// uploadKeyListHandler 上传 Key 清单文件
func uploadKeyListHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 限制上传大小为 100MB
	r.Body = http.MaxBytesReader(w, r.Body, 100*1024*1024)

	// 解析 multipart form
	if err := r.ParseMultipartForm(100 * 1024 * 1024); err != nil {
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": "Failed to parse form: " + err.Error(),
		})
		return
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": "Failed to get file: " + err.Error(),
		})
		return
	}
	defer file.Close()

	// 读取文件内容
	data, err := io.ReadAll(file)
	if err != nil {
		jsonResponse(w, map[string]interface{}{
			"code":    500,
			"message": "Failed to read file: " + err.Error(),
		})
		return
	}

	// 保存到临时目录
	uploadDir := "./data/keylists"
	os.MkdirAll(uploadDir, 0755)
	
	// 生成唯一文件名
	filename := fmt.Sprintf("%s_%s", time.Now().Format("20060102150405"), handler.Filename)
	filePath := filepath.Join(uploadDir, filename)
	
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		jsonResponse(w, map[string]interface{}{
			"code":    500,
			"message": "Failed to save file: " + err.Error(),
		})
		return
	}

	// 尝试解析预览
	keyList, err := LoadKeyListFromFile(filePath)
	if err != nil {
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": "Failed to parse key list: " + err.Error(),
		})
		return
	}

	// 返回预览信息
	previewKeys := keyList.Keys
	if len(previewKeys) > 10 {
		previewKeys = previewKeys[:10]
	}

	log.Info("Key list uploaded", map[string]interface{}{
		"filename":   handler.Filename,
		"size":       len(data),
		"total_keys": keyList.TotalCount,
		"format":     keyList.Format,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"file_path":    filePath,
			"filename":     handler.Filename,
			"size":         len(data),
			"total_keys":   keyList.TotalCount,
			"format":       keyList.Format,
			"preview_keys": previewKeys,
		},
	})
}

// parseKeyListHandler 解析 Key 清单内容（不保存文件）
func parseKeyListHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Content string `json:"content"` // Key 清单内容
		Format  string `json:"format"`  // 格式：txt, csv, json（可选，自动检测）
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": "Invalid request: " + err.Error(),
		})
		return
	}

	if req.Content == "" {
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": "Content is required",
		})
		return
	}

	data := []byte(req.Content)
	format := req.Format
	if format == "" {
		format = detectKeyListFormat("", data)
	}

	var keys []string
	var err error
	switch format {
	case "json":
		keys, err = parseJSONKeyList(data)
	case "csv":
		keys, err = parseCSVKeyList(data)
	default:
		keys, err = parseTXTKeyList(data)
	}

	if err != nil {
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": "Failed to parse: " + err.Error(),
		})
		return
	}

	// 去重
	uniqueKeys := make([]string, 0, len(keys))
	seen := make(map[string]bool)
	for _, key := range keys {
		if key != "" && !seen[key] {
			seen[key] = true
			uniqueKeys = append(uniqueKeys, key)
		}
	}

	previewKeys := uniqueKeys
	if len(previewKeys) > 20 {
		previewKeys = previewKeys[:20]
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"total_keys":   len(uniqueKeys),
			"format":       format,
			"preview_keys": previewKeys,
		},
	})
}

// importTaskConfigHandler 导入任务配置
func importTaskConfigHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var importConfig struct {
		Name           string       `json:"name"`
		SourceCluster  string       `json:"source_cluster"`
		TargetCluster  string       `json:"target_cluster"`
		MigrationMode  string       `json:"migration_mode"`
		Options        *TaskOptions `json:"options"`
	}

	if err := json.NewDecoder(r.Body).Decode(&importConfig); err != nil {
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": "Invalid config: " + err.Error(),
		})
		return
	}

	// 创建新任务
	taskID := uuid.New().String()
	now := time.Now().Format(time.RFC3339)

	task := &Task{
		ID:            taskID,
		Name:          importConfig.Name,
		Status:        "pending",
		SourceCluster: importConfig.SourceCluster,
		TargetCluster: importConfig.TargetCluster,
		MigrationMode: importConfig.MigrationMode,
		Options:       importConfig.Options,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	tasksMu.Lock()
	tasks[taskID] = task
	tasksMu.Unlock()

	log.Info("Task imported from config", map[string]interface{}{
		"task_id":   taskID,
		"task_name": task.Name,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"task_id":   taskID,
			"task_name": task.Name,
			"status":    task.Status,
		},
	})
}

// smartRetryStatusHandler 获取智能重试服务状态
func smartRetryStatusHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	// 统计各状态任务数量
	var runningCount, pausedCount, pendingCount int
	var tasksWithAutoRecovery []map[string]interface{}

	tasksMu.RLock()
	for _, task := range tasks {
		switch task.Status {
		case "running":
			runningCount++
		case "paused":
			pausedCount++
			// 获取自动恢复状态
			status := getAutoRecoveryStatus(task.ID)
			status["task_id"] = task.ID
			status["task_name"] = task.Name
			tasksWithAutoRecovery = append(tasksWithAutoRecovery, status)
		case "pending":
			pendingCount++
		}
	}
	tasksMu.RUnlock()

	// 统计失败 Key 数量
	var totalFailedKeys int
	errorKeyMu.RLock()
	for _, keys := range errorKeys {
		totalFailedKeys += len(keys)
	}
	errorKeyMu.RUnlock()

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"service_status": "running",
			"config": map[string]interface{}{
				"health_check_interval_sec":   DefaultHealthCheckIntervalSec,
				"periodic_retry_interval_sec": DefaultPeriodicRetryIntervalSec,
				"periodic_retry_batch_size":   DefaultPeriodicRetryBatchSize,
				"max_auto_resume_attempts":    DefaultMaxAutoResumeAttempts,
			},
			"task_stats": map[string]interface{}{
				"running": runningCount,
				"paused":  pausedCount,
				"pending": pendingCount,
			},
			"total_failed_keys":          totalFailedKeys,
			"paused_tasks_auto_recovery": tasksWithAutoRecovery,
		},
	})
}

// conflictKeysHandler 查询冲突 Key 列表
// GET /api/v1/tasks/:id/conflicts?page=1&size=100
func conflictKeysHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		log.Warn("Task not found", map[string]interface{}{"task_id": id})
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 解析分页参数
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	size, _ := strconv.Atoi(r.URL.Query().Get("size"))
	if page < 1 {
		page = 1
	}
	if size <= 0 {
		size = 100
	}
	if size > 1000 {
		size = 1000
	}

	// 获取跳过的 Key 数量作为冲突数
	skippedKeys := task.KeysSkipped

	log.Debug("Conflict keys queried", map[string]interface{}{
		"task_id":      id,
		"skipped_keys": skippedKeys,
	})

	// 目前简单返回统计信息，不存储具体的冲突 Key
	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"total": skippedKeys,
			"page":  page,
			"size":  size,
			"keys":  []interface{}{}, // 暂不存储具体 Key
			"note":  "Conflict keys are counted but not individually stored for memory efficiency",
		},
	})
}

// conflictKeysSubHandler 处理冲突 Key 子路由
// GET /api/v1/tasks/:id/conflicts/summary
// GET /api/v1/tasks/:id/conflicts/export
func conflictKeysSubHandler(w http.ResponseWriter, r *http.Request, id, action string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		log.Warn("Task not found", map[string]interface{}{"task_id": id})
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	subAction := strings.TrimPrefix(action, "conflicts/")
	skippedKeys := task.KeysSkipped

	switch subAction {
	case "summary":
		log.Debug("Conflict keys summary queried", map[string]interface{}{"task_id": id})
		jsonResponse(w, map[string]interface{}{
			"code":    0,
			"message": "success",
			"data": map[string]interface{}{
				"total_count":  skippedKeys,
				"memory_count": 0,
				"disk_count":   0,
				"by_phase": map[string]int64{
					"full":        skippedKeys,
					"incremental": 0,
				},
				"by_action": map[string]int64{
					"skip": skippedKeys,
				},
				"by_type": map[string]int64{},
			},
		})
	case "export":
		format := r.URL.Query().Get("format")
		if format == "" {
			format = "jsonl"
		}
		log.Debug("Conflict keys export requested", map[string]interface{}{
			"task_id": id,
			"format":  format,
		})
		// 设置下载头
		filename := fmt.Sprintf("conflict-keys-%s.%s", id[:8], format)
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
		w.Header().Set("Content-Type", "application/octet-stream")
		// 返回空数据（因为没有存储具体 Key）
		w.Write([]byte(fmt.Sprintf("# Conflict keys for task %s\n", id)))
		w.Write([]byte(fmt.Sprintf("# Total skipped: %d\n", skippedKeys)))
		w.Write([]byte("# Note: Individual conflict keys are not stored for memory efficiency\n"))
	default:
		http.NotFound(w, r)
	}
}
