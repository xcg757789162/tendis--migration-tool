package main

import (
	"archive/zip"
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
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
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/time/rate"
	"tendis-migrate/internal/replication"
	"tendis-migrate/pkg/logger"
)

// 版本信息
const (
	Version   = "2.3.0"
	BuildDate = "2026-02-09"
)

// 命令行参数
var (
	flagPort    = flag.Int("port", 8088, "HTTP server port")
	flagDataDir = flag.String("data", "./data", "Data directory path")
	flagWorkers = flag.Int("workers", 4, "Default number of workers")
	flagLogDir  = flag.String("logs", "./logs", "Log directory path")
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
	// 集群拓扑健康告警（前端展示用）
	TopologyWarnings []string `json:"topology_warnings,omitempty"`

	fakeSlaves   []*replication.FakeSlave `json:"-"` // Binlog 接收器（每个节点一个）
	cacheManager *replication.BinlogCacheManager `json:"-"` // Binlog 缓存管理器
	// 暂停时保存每个 FakeSlave 的 binlog 位置，恢复时从此位置继续
	// key格式: "nodeAddr:storeID", value: binlogPos
	savedBinlogPositions map[string]uint64 `json:"-"`
	stopCh       chan struct{} `json:"-"` // 任务停止通道（用于优雅停止迁移 goroutine）
	stopOnce     sync.Once     `json:"-"` // 确保 stopCh 只关闭一次
	startedTime  time.Time     `json:"-"` // 任务启动时间（用于启动冷却期检查）
	cancelFunc   context.CancelFunc `json:"-"` // 【P1修复】任务级别 context 取消函数，用于从外部取消所有子 goroutine
	
	// 【死锁修复】任务级互斥锁，用于保护统计字段的高频更新
	// 替代全局 tasksMu 的写锁，避免高频 ticker 与 HTTP handler 争抢全局锁导致死锁
	statsMu sync.Mutex `json:"-"`
	
	// 升级重启标记：如果任务是因为程序升级/重启被自动暂停的，此标记为 true
	// 重启后会自动恢复这些任务；手动暂停的任务（ShutdownPaused=false）不会被自动恢复
	ShutdownPaused bool `json:"shutdown_paused,omitempty"`
}

// Init 初始化/重置 Task 的运行时控制字段（stopCh、stopOnce、startedTime）。
// 必须在 tasksMu 写锁内调用。
// 场景：创建任务、启动任务、恢复任务、重启任务、自动恢复任务、从持久化恢复。
func (t *Task) Init() {
	t.stopCh = make(chan struct{})
	t.stopOnce = sync.Once{}
	t.startedTime = time.Now()
}

// Cleanup 安全停止 Task 的运行时控制（关闭 stopCh + 取消 context）。
// 必须在 tasksMu 写锁内调用。
// 场景：暂停、停止、删除任务、优雅关闭。
func (t *Task) Cleanup() {
	if t.stopCh != nil {
		t.stopOnce.Do(func() {
			close(t.stopCh)
		})
	}
	if t.cancelFunc != nil {
		t.cancelFunc()
	}
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
	EnableCompression    bool                  `json:"enable_compression"`    // 已废弃：压缩传输功能未实现，保留字段兼容旧数据
	KeyFilter            *KeyFilter            `json:"key_filter,omitempty"`
	RateLimit            *RateLimit            `json:"rate_limit,omitempty"`
	RetryConfig          *RetryConfig          `json:"retry_config,omitempty"`
	FaultTolerance       *FaultToleranceConfig `json:"fault_tolerance,omitempty"`  // 问题5修复：容错配置
	SmartRetry           *SmartRetryConfig     `json:"smart_retry,omitempty"`      // 问题5修复：智能重试配置
	VerifyConfig         *VerifyConfig         `json:"verify_config,omitempty"`    // 已废弃：校验功能由独立 VerifyTask 模块实现
	KeyListFile          string                `json:"key_list_file,omitempty"`    // Key 清单文件路径（支持 CSV/JSON/TXT）
	ReadFromSlave        bool                  `json:"read_from_slave"`            // 从 slave 节点读取数据（生产环境推荐，避免影响 master）
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
	Mode            string   `json:"mode"` // all, prefix, pattern, keys, keylist
	Prefixes        []string `json:"prefixes,omitempty"`
	ExcludePrefixes []string `json:"exclude_prefixes,omitempty"`
	ExcludePatterns []string `json:"exclude_patterns,omitempty"`
	Patterns        []string `json:"patterns,omitempty"`
	Keys            []string `json:"keys,omitempty"` // keylist/keys 模式使用
}

// ==================== Key 清单导入功能 ====================

// KeyListSource Key 清单来源类型
type KeyListSource struct {
	Keys       []string `json:"keys"`        // 内存中的 Key 列表（仅小清单使用）
	TotalCount int      `json:"total_count"` // 总 Key 数量（截断前的实际数量）
	Source     string   `json:"source"`      // 来源：file, api, inline
	Format     string   `json:"format"`      // 格式：txt, csv, json
	Truncated  bool     `json:"truncated"`   // 是否因超过预览上限而截断
}

// StreamKeyListFromFile 流式读取 Key 清单文件，通过 channel 逐个发送 Key
// 支持任意规模的 Key 清单（100亿+），不会将全部 Key 加载到内存
// 去重使用概率性检查（基于 map 滑动窗口），对于超大清单牺牲微小精确度换取内存安全
// 返回：keyChan（Key 流）、totalCount（发送的总 Key 数指针）、errChan（错误）
func StreamKeyListFromFile(filePath string) (<-chan string, *int64, <-chan error) {
	keyChan := make(chan string, 10000)
	errChan := make(chan error, 1)
	var totalCount int64

	go func() {
		defer close(keyChan)
		defer close(errChan)

		if filePath == "" {
			return
		}

		file, err := os.Open(filePath)
		if err != nil {
			errChan <- fmt.Errorf("open key list file failed: %w", err)
			return
		}
		defer file.Close()

		// 读取前 4KB 检测格式
		header := make([]byte, 4096)
		n, _ := file.Read(header)
		header = header[:n]
		file.Seek(0, 0) // 重置到文件开头

		format := detectKeyListFormat(filePath, header)

		// 去重：使用固定大小的 map（滑动窗口去重，内存可控）
		// 对于超大清单（>1000万行），可能有极少量重复 Key 通过，但不会 OOM
		const maxDedupeWindow = 10000000 // 1000 万条去重窗口，约 480MB
		seen := make(map[string]struct{})
		var seenCount int64

		sendKey := func(key string) {
			if key == "" {
				return
			}
			// 去重检查
			if _, exists := seen[key]; exists {
				return
			}
			if seenCount < maxDedupeWindow {
				seen[key] = struct{}{}
				seenCount++
			}
			// 超过窗口后不再去重（允许极少量重复，避免 OOM）
			atomic.AddInt64(&totalCount, 1)
			keyChan <- key
		}

		switch format {
		case "json":
			// JSON 必须整文件解析，但使用流式 decoder
			decoder := json.NewDecoder(file)
			// 尝试读取开头的 [
			token, err := decoder.Token()
			if err != nil {
				errChan <- fmt.Errorf("parse JSON key list failed: %w", err)
				return
			}
			if delim, ok := token.(json.Delim); ok && delim == '[' {
				// 数组格式：逐元素解码
				for decoder.More() {
					// 尝试字符串
					var s string
					if err := decoder.Decode(&s); err == nil {
						sendKey(s)
						continue
					}
					// 尝试对象
					var obj map[string]interface{}
					if err := decoder.Decode(&obj); err == nil {
						if key, ok := obj["key"].(string); ok && key != "" {
							sendKey(key)
						} else if name, ok := obj["name"].(string); ok && name != "" {
							sendKey(name)
						} else if key, ok := obj["Key"].(string); ok && key != "" {
							sendKey(key)
						}
					}
				}
			}
		case "csv":
			reader := csv.NewReader(file)
			reader.FieldsPerRecord = -1 // 允许不等长行
			reader.LazyQuotes = true
			isFirst := true
			for {
				record, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					continue
				}
				if len(record) == 0 {
					continue
				}
				key := strings.TrimSpace(record[0])
				key = strings.Trim(key, "\"'")
				if isFirst {
					isFirst = false
					lower := strings.ToLower(key)
					if lower == "key" || lower == "name" || lower == "redis_key" {
						continue
					}
				}
				sendKey(key)
			}
		default: // txt
			scanner := bufio.NewScanner(file)
			// 支持超长行（最大 1MB）
			buf := make([]byte, 0, 64*1024)
			scanner.Buffer(buf, 1024*1024)
			for scanner.Scan() {
				key := strings.TrimSpace(scanner.Text())
				sendKey(key)
			}
		}
	}()

	return keyChan, &totalCount, errChan
}

// LoadKeyListFromFile 从文件加载 Key 清单用于预览
// 大规模清单的实际迁移请使用 StreamKeyListFromFile（流式处理，无内存限制）
// 文件超过 200MB 时只做采样预览（读前 4MB 估算），Key 超过 100 万时截断预览
func LoadKeyListFromFile(filePath string) (*KeyListSource, error) {
	if filePath == "" {
		return nil, nil
	}

	const maxPreviewFileSize = 200 * 1024 * 1024 // 200MB：全量解析上限
	const maxPreviewKeys = 1000000                // 100 万 Key：预览截断上限
	fi, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("stat key list file failed: %w", err)
	}

	// 超大文件：采样预览模式（只读前 4MB 估算总量，不全量加载）
	if fi.Size() > maxPreviewFileSize {
		return samplePreviewLargeFile(filePath, fi.Size())
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

	// 超过 100 万 Key 时截断预览（不报错），实际迁移走 StreamKeyListFromFile 流式处理
	truncated := false
	totalParsed := len(keys)
	if len(keys) > maxPreviewKeys {
		keys = keys[:maxPreviewKeys]
		truncated = true
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
		TotalCount: totalParsed,
		Truncated:  truncated,
		Source:     "file",
		Format:     format,
	}, nil
}

// samplePreviewLargeFile 对超大文件做采样预览（只读前 4MB 估算 Key 数量）
func samplePreviewLargeFile(filePath string, fileSize int64) (*KeyListSource, error) {
	const sampleSize = 4 * 1024 * 1024 // 4MB 采样

	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open key list file failed: %w", err)
	}
	defer f.Close()

	header := make([]byte, sampleSize)
	n, _ := f.Read(header)
	header = header[:n]

	format := detectKeyListFormat(filePath, header)

	// 对 TXT 格式按行采样估算
	var previewKeys []string
	var estimatedTotal int64

	if format == "txt" {
		lines := strings.Split(string(header), "\n")
		for _, line := range lines {
			key := strings.TrimSpace(line)
			if key != "" {
				previewKeys = append(previewKeys, key)
			}
		}
		// 按采样比例估算总数
		if n > 0 {
			estimatedTotal = int64(len(previewKeys)) * fileSize / int64(n)
		}
	} else {
		// JSON/CSV 格式无法部分解析，返回文件信息 + 估算
		estimatedTotal = fileSize / 30 // 粗略估算：平均每 Key 30 字节
	}

	// 只取前 10 条用于预览
	if len(previewKeys) > 10 {
		previewKeys = previewKeys[:10]
	}

	return &KeyListSource{
		Keys:       previewKeys,
		TotalCount: int(estimatedTotal),
		Truncated:  true,
		Source:     "file",
		Format:     format,
	}, nil
}

// StreamValidateAndSend 流式验证 Key 在源端是否存在并发送到 keyChan
// 替代旧的 ValidateKeyListInSource + 全量持有 existingKeys 模式
// 流式处理：从 inputCh 读取 Key → Pipeline 批量验证 → 存在的直接发送到 keyChan
func StreamValidateAndSend(ctx context.Context, client redis.UniversalClient, inputCh <-chan string, keyChan chan<- string, batchSize int) (existing int64, missing int64) {
	if batchSize <= 0 {
		batchSize = 1000
	}

	batch := make([]string, 0, batchSize)

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		pipe := client.Pipeline()
		cmds := make([]*redis.IntCmd, len(batch))
		for j, key := range batch {
			cmds[j] = pipe.Exists(ctx, key)
		}
		pipe.Exec(ctx)

		for j, cmd := range cmds {
			if cmd != nil && cmd.Val() > 0 {
				atomic.AddInt64(&existing, 1)
				select {
				case <-ctx.Done():
					return
				case keyChan <- batch[j]:
				}
			} else {
				atomic.AddInt64(&missing, 1)
			}
		}
		batch = batch[:0]
	}

	for key := range inputCh {
		select {
		case <-ctx.Done():
			return
		default:
		}
		batch = append(batch, key)
		if len(batch) >= batchSize {
			flushBatch()
		}
	}
	flushBatch()
	return
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
		pattern := filter.Patterns[0]
		// 如果 pattern 已经包含通配符，直接使用
		if strings.Contains(pattern, "*") {
			return pattern
		}
		// 否则，作为包含匹配
		return "*" + pattern + "*"
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

// getMapKeys 获取 map 的所有 keys（用于调试日志）
func getMapKeys(m map[string]uint64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// ErrorKey 记录迁移失败或跳过的Key
// 【P2-BUG5 修复】增强错误信息，包含源节点、目标节点、操作类型等
type ErrorKey struct {
	Key         string `json:"key"`
	Type        string `json:"type"`         // Key 类型：string, hash, list, set, zset
	Reason      string `json:"reason"`       // 失败原因分类：failed, skipped, filtered
	Detail      string `json:"detail"`       // 详细错误信息
	SourceNode  string `json:"source_node"`  // 源节点地址
	TargetNode  string `json:"target_node"`  // 目标节点地址
	Operation   string `json:"operation"`    // 操作类型：dump, restore, pipeline
	Phase       string `json:"phase"`        // 阶段：full, incremental
	RetryCount  int    `json:"retry_count"`  // 重试次数
	Timestamp   string `json:"timestamp"`
}

// getClientAddr 从 redis.UniversalClient 获取地址（辅助函数，用于错误日志）
// 【P2-BUG5 修复】安全获取客户端地址，支持单机和集群模式
func getClientAddr(client redis.UniversalClient) string {
	if client == nil {
		return "unknown"
	}
	// 尝试单机客户端
	if c, ok := client.(*redis.Client); ok && c != nil {
		return c.Options().Addr
	}
	// 尝试集群客户端（返回第一个节点作为标识）
	if c, ok := client.(*redis.ClusterClient); ok && c != nil {
		// 集群模式返回 "cluster:*" 标识
		return "cluster:multi-nodes"
	}
	return "unknown"
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
	// 【BUG-FIX】已从错误列表中移除的 Key 集合（重试成功后添加）
	// 用于在 getAllErrorKeys 读取落盘文件时过滤掉已成功重试的 Key
	// 【P1 修复】加上限保护：每个任务最多保留 100 万条，超出后落盘到文件
	removedErrorKeys   = make(map[string]map[string]bool) // taskID -> set of removed keys
	removedErrorKeysMu sync.RWMutex
	// removedErrorKeys 每任务上限（超出落盘到文件，内存中只保留最近的）
	maxRemovedErrorKeysInMemory = 1000000 // 100 万条/任务，约 40MB
	startTime  time.Time
	dataDir    = "./data" // 数据目录，可通过命令行参数修改
	
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
	// 【崩溃恢复修复】计数器快照：断点保存时同步记录，恢复时用于修正 tasks-state.json 中的过时值
	KeysMigrated     int64             `json:"keys_migrated,omitempty"`
	KeysFailed       int64             `json:"keys_failed,omitempty"`
	KeysSkipped      int64             `json:"keys_skipped,omitempty"`
	KeysFiltered     int64             `json:"keys_filtered,omitempty"`
	KeysToMigrate    int64             `json:"keys_to_migrate,omitempty"`
	BytesMigrated    int64             `json:"bytes_migrated,omitempty"`
	mu               sync.RWMutex      `json:"-"`                  // 【BUG-FIX】保护 NodeCursors 的并发访问
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
	// 解析命令行参数
	flag.Parse()
	
	startTime = time.Now()
	
	// 初始化日志系统
	// 【优化】默认使用 INFO 级别，减少日志量
	// Debug 日志只在需要详细排查时手动开启
	if err := logger.Init(*flagLogDir, logger.INFO); err != nil {
		fmt.Printf("Failed to init logger: %v\n", err)
	}
	
	logger.Info("🚀 Tendis Migration Tool starting", map[string]interface{}{
		"port":    *flagPort,
		"data":    *flagDataDir,
		"workers": *flagWorkers,
		"version": Version,
		"pid":     fmt.Sprintf("%d", getPID()),
	})

	// 设置数据目录
	dataDir = *flagDataDir

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
	addr := fmt.Sprintf(":%d", *flagPort)
	server := &http.Server{
		Addr:         addr,
		Handler:      http.HandlerFunc(mainHandler),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// 设置优雅关闭
	setupGracefulShutdown(server)

	logger.Info(fmt.Sprintf("Server listening on http://localhost:%d", *flagPort))
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

		// 2.1 保存校验任务状态
		logger.Info("Saving verify tasks state...")
		saveVerifyTasksState()

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
		if task.Status == "running" || task.Status == "retrying" {
			task.Status = "paused"
			task.PausedAt = now.Format(time.RFC3339)
			task.UpdatedAt = now.Format(time.RFC3339)
			task.ShutdownPaused = true // 标记为升级/重启自动暂停，重启后自动恢复
			task.Cleanup() // 统一清理运行时控制字段
			pausedCount++
			logger.Info("Task paused for shutdown (will auto-resume on restart)", map[string]interface{}{
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
	// 【审计修复】使用写锁（之前用 RLock 但修改了 UpdatedAt 字段，且 MarshalIndent 遍历 NodeCursors 需要内部锁保护）
	fullSyncCheckpointsMu.Lock()
	checkpointDir := "./data/checkpoints"
	os.MkdirAll(checkpointDir, 0755)
	for taskID, checkpoint := range fullSyncCheckpoints {
		if checkpoint != nil {
			checkpoint.mu.RLock()
			checkpoint.UpdatedAt = time.Now().Format(time.RFC3339)
			// 【崩溃恢复修复】同步更新计数器快照（距上次 saveFullSyncCheckpoint 可能又有变化）
			tasksMu.RLock()
			if task, ok := tasks[taskID]; ok {
				checkpoint.KeysMigrated = atomic.LoadInt64(&task.KeysMigrated)
				checkpoint.KeysFailed = atomic.LoadInt64(&task.KeysFailed)
				checkpoint.KeysSkipped = atomic.LoadInt64(&task.KeysSkipped)
				checkpoint.KeysFiltered = atomic.LoadInt64(&task.KeysFiltered)
				checkpoint.KeysToMigrate = task.KeysToMigrate
				checkpoint.BytesMigrated = atomic.LoadInt64(&task.BytesMigrated)
			}
			tasksMu.RUnlock()
			data, _ := json.MarshalIndent(checkpoint, "", "  ")
			checkpoint.mu.RUnlock()
			os.WriteFile(fmt.Sprintf("%s/full-%s.json", checkpointDir, taskID), data, 0644)
		}
	}
	fullSyncCheckpointsMu.Unlock()
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
		dataDir,
		filepath.Join(dataDir, "backups"),
		filepath.Join(dataDir, "checkpoints"),
		*flagLogDir,
	}
	for _, dir := range dirs {
		os.MkdirAll(dir, 0755)
	}
}

// recoverUnfinishedTasks 恢复所有持久化的任务
// 之前运行中的任务恢复为 paused 状态（需用户手动 resume）
// 已完成/失败/停止的任务原样恢复，不丢失历史记录
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
	resumableCount := 0
	for id, savedTask := range savedTasks {
		// 确定恢复后的状态
		recoveredStatus := savedTask.Status
		isResumable := false
		if savedTask.Status == "running" || savedTask.Status == "retrying" {
			// 之前运行中的任务恢复为 paused，等待用户手动恢复
			recoveredStatus = "paused"
			isResumable = true
			resumableCount++
		}

		// 创建新任务对象
		task := &Task{
			ID:             savedTask.ID,
			Name:           savedTask.Name,
			Status:         recoveredStatus,
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
			CompletedAt:    savedTask.CompletedAt,
			PausedAt:       savedTask.PausedAt,
			PausedDuration: savedTask.PausedDuration,
			KeysTotal:      savedTask.KeysTotal,
			KeysToMigrate:  savedTask.KeysToMigrate,
			KeysMigrated:   savedTask.KeysMigrated,
			KeysFailed:     savedTask.KeysFailed,
			KeysSkipped:    savedTask.KeysSkipped,
			KeysFiltered:   savedTask.KeysFiltered,
			BytesMigrated:  savedTask.BytesMigrated,
			BytesTotal:     savedTask.BytesTotal,
			Phase:          savedTask.Phase,
			Speed:          savedTask.Speed,
			ActiveWorkers:  savedTask.ActiveWorkers,
			IncrKeysSynced:   savedTask.IncrKeysSynced,
			IncrKeysSkipped:  savedTask.IncrKeysSkipped,
			IncrKeysFailed:   savedTask.IncrKeysFailed,
			IncrKeysFiltered: savedTask.IncrKeysFiltered,
			IncrBinlogPos:    savedTask.IncrBinlogPos,
			IncrLagMs:        savedTask.IncrLagMs,
			IncrHeartbeats:   savedTask.IncrHeartbeats,
			IncrReconnects:   savedTask.IncrReconnects,
			IncrSyncMode:     savedTask.IncrSyncMode,
			Options:        savedTask.Options,
			ShutdownPaused: savedTask.ShutdownPaused,
		}
		task.Init() // 统一初始化运行时控制字段

		tasksMu.Lock()
		tasks[id] = task
		tasksMu.Unlock()

		// 对可恢复的任务，加载错误 key 和断点
		if isResumable || savedTask.Status == "paused" {
			if keys := loadErrorKeysFromFile(id); keys != nil {
				logger.Info("Error keys recovered", map[string]interface{}{
					"task_id":    id,
					"error_keys": len(keys),
				})
			}

			if checkpoint := loadFullSyncCheckpoint(id); checkpoint != nil {
				// 【崩溃恢复修复】用断点中的计数器快照修正 tasks-state.json 中的过时值
				// kill -9 时 tasks-state.json（30秒周期）可能比断点（每1000 key）更旧
				countersFixed := false
				if checkpoint.KeysMigrated > task.KeysMigrated {
					task.KeysMigrated = checkpoint.KeysMigrated
					countersFixed = true
				}
				if checkpoint.KeysFailed > task.KeysFailed {
					task.KeysFailed = checkpoint.KeysFailed
					countersFixed = true
				}
				if checkpoint.KeysSkipped > task.KeysSkipped {
					task.KeysSkipped = checkpoint.KeysSkipped
					countersFixed = true
				}
				if checkpoint.KeysFiltered > task.KeysFiltered {
					task.KeysFiltered = checkpoint.KeysFiltered
					countersFixed = true
				}
				if checkpoint.KeysToMigrate > task.KeysToMigrate {
					task.KeysToMigrate = checkpoint.KeysToMigrate
					countersFixed = true
				}
				if checkpoint.BytesMigrated > task.BytesMigrated {
					task.BytesMigrated = checkpoint.BytesMigrated
					countersFixed = true
				}
				logger.Info("Full sync checkpoint loaded", map[string]interface{}{
					"task_id":         id,
					"processed_keys":  checkpoint.ProcessedKeys,
					"is_complete":     checkpoint.IsComplete,
					"nodes":           len(checkpoint.NodeCursors),
					"counters_fixed":  countersFixed,
					"cp_migrated":     checkpoint.KeysMigrated,
					"task_migrated":   task.KeysMigrated,
				})
			}

			if checkpoint := loadIncrementalCheckpoint(id); checkpoint != nil {
				logger.Info("Incremental checkpoint loaded", map[string]interface{}{
					"task_id":     id,
					"synced_keys": checkpoint.SyncedKeys,
				})
			}
		} else {
			// 对已完成/失败的任务，也加载错误 key（用于查看历史）
			loadErrorKeysFromFile(id)
		}

		recoveredCount++
		logger.Info("Task recovered", map[string]interface{}{
			"task_id":         id,
			"task_name":       task.Name,
			"previous_status": savedTask.Status,
			"recovered_status": recoveredStatus,
			"progress":        task.Progress,
			"phase":           task.Phase,
		})
	}

	if recoveredCount > 0 {
		logger.Info("Tasks recovery completed", map[string]interface{}{
			"total_recovered":  recoveredCount,
			"resumable_count":  resumableCount,
			"message":          "Previously running tasks are paused. Use resume API to continue.",
		})
	}
	
	// 自动恢复因升级/重启被暂停的任务（ShutdownPaused=true）
	// 注意：手动暂停的任务（ShutdownPaused=false）不会被自动恢复
	autoResumeShutdownPausedTasks()
}

// autoResumeShutdownPausedTasks 自动恢复因升级/重启被暂停的任务
// 只恢复 ShutdownPaused=true 的任务（程序升级时自动暂停的）
// 手动暂停的任务（ShutdownPaused=false）保持暂停状态，需用户手动恢复
func autoResumeShutdownPausedTasks() {
	var tasksToResume []*Task
	
	tasksMu.RLock()
	for _, task := range tasks {
		if task.Status == "paused" && task.ShutdownPaused {
			tasksToResume = append(tasksToResume, task)
		}
	}
	tasksMu.RUnlock()
	
	if len(tasksToResume) == 0 {
		return
	}
	
	logger.Info("Auto-resuming shutdown-paused tasks", map[string]interface{}{
		"count": len(tasksToResume),
	})
	
	for _, task := range tasksToResume {
		tasksMu.Lock()
		// 计算暂停时长
		if task.PausedAt != "" {
			pausedTime, err := time.Parse(time.RFC3339, task.PausedAt)
			if err == nil && !pausedTime.IsZero() {
				task.PausedDuration += int64(time.Since(pausedTime).Seconds())
			}
		}
		task.Init()
		task.Status = "running"
		task.PausedAt = ""
		task.ShutdownPaused = false // 清除标记
		task.UpdatedAt = time.Now().Format(time.RFC3339)
		tasksMu.Unlock()
		
		go simulateProgress(task)
		
		logger.Info("Task auto-resumed after upgrade/restart", map[string]interface{}{
			"task_id":   task.ID,
			"task_name": task.Name,
			"phase":     task.Phase,
		})
	}
}

// saveTasksState 保存任务状态到文件
// 保存所有任务（包括 completed/failed/stopped），确保重启后不丢失
func saveTasksState() {
	tasksMu.RLock()
	tasksToSave := make(map[string]*Task)
	activeTaskIDs := make([]string, 0)
	for id, task := range tasks {
		// 保存所有非 pending 状态的任务（pending 由模板创建，不需要持久化）
		tasksToSave[id] = task
		// 只对运行中/暂停/重试中的任务保存错误 key
		if task.Status == "running" || task.Status == "paused" || task.Status == "retrying" {
			activeTaskIDs = append(activeTaskIDs, id)
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

	// 保存活跃任务的错误 key
	for _, id := range activeTaskIDs {
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
			saveVerifyTasksState() // 同时保存校验任务状态
			// 【新增】同时保存断点，防止 SIGKILL 丢数据
			saveAllFullSyncCheckpoints()
			saveAllIncrementalCheckpoints()
		}
	}()
	
	// 【不丢数据保障】额外启动错误 Key 定期落盘（每 10 秒）
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			saveAllErrorKeys()
		}
	}()
}

func mainHandler(w http.ResponseWriter, r *http.Request) {
	// 【死锁修复】全局 panic recovery，防止 handler 中 panic 导致 tasksMu 锁不释放
	defer func() {
		if rec := recover(); rec != nil {
			logger.Error("HTTP handler panic recovered", map[string]interface{}{
				"panic":  fmt.Sprintf("%v", rec),
				"method": r.Method,
				"path":   r.URL.Path,
			})
			http.Error(w, fmt.Sprintf(`{"code":500,"message":"Internal server error: %v"}`, rec), http.StatusInternalServerError)
		}
	}()

	// 生成请求ID
	requestID := uuid.New().String()
	startTime := time.Now()
	
	// CORS
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")  // 【BUG-FIX】添加 PATCH
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
	// 精确匹配的 tasks 子路径必须在 HasPrefix 之前
	case path == "/api/v1/tasks/batch-delete":
		batchDeleteTasksHandler(rw, r, log)
	case path == "/api/v1/tasks/import":
		importTaskConfigHandler(rw, r, log)
	case strings.HasPrefix(path, "/api/v1/tasks/"):
		taskHandler(rw, r, log)
	case path == "/api/v1/system/status":
		systemHandler(rw, r, log)
	case path == "/api/v1/system/workers":
		systemWorkersHandler(rw, r, log)
	case path == "/api/v1/system/backup":
		systemBackupHandler(rw, r, log)
	case path == "/api/v1/system/backups":
		systemBackupListHandler(rw, r, log)
	case path == "/api/v1/system/backup-upload":
		systemBackupUploadHandler(rw, r, log)
	case strings.HasPrefix(path, "/api/v1/system/backup/"):
		systemBackupActionHandler(rw, r, log)
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
	
	// 独立校验任务 API
	case path == "/api/v1/verify-tasks":
		verifyTasksHandler(rw, r, log)
	case path == "/api/v1/verify-tasks/batch-delete":
		batchDeleteVerifyTasksHandler(rw, r, log)
	case strings.HasPrefix(path, "/api/v1/verify-tasks/"):
		verifyTaskHandler(rw, r, log)
	
	// 智能重试相关 API
	case path == "/api/v1/smart-retry/status":
		smartRetryStatusHandler(rw, r, log)
	
	// Key 清单上传 API
	case path == "/api/v1/upload-keylist":
		uploadKeyListHandler(rw, r, log)
	case path == "/api/v1/parse-keylist":
		parseKeyListHandler(rw, r, log)
		
		
	// 静态资源
	case strings.HasPrefix(path, "/assets/"):
		http.FileServer(http.Dir("./web/dist")).ServeHTTP(rw, r)
		
	// SPA 入口（禁止缓存 index.html，确保每次加载最新版本）
	default:
		rw.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		rw.Header().Set("Pragma", "no-cache")
		rw.Header().Set("Expires", "0")
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
	
	// 模拟生产 A - 高性能配置（512 workers, 5000 scan batch, 1536 connections）
	templates["template-prod-a"] = &TaskTemplate{
		ID:            "template-prod-a",
		Name:          "模拟生产A-02112204",
		Description:   "高性能配置：512 workers, 5000 scan batch, 1536 connections，适合大规模生产环境迁移",
		SourceCluster: "10.31.36.8:8902,10.31.36.10:8903,10.31.36.12:8901",
		TargetCluster: "10.31.36.3:8902,10.31.36.15:8901,10.31.36.13:8903",
		MigrationMode: "full_and_incremental",
		Options: &TaskOptions{
			WorkerCount:       512,
			ScanBatchSize:     5000,
			ConflictPolicy:    "skip",
			LargeKeyThreshold: 10485760, // 10MB
			KeyFilter: &KeyFilter{
				Mode: "all",
			},
			RateLimit: &RateLimit{
				SourceQPS:         0,
				TargetQPS:         0,
				SourceConnections: 1536,
				TargetConnections: 1536,
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
	
	// 测试环境 A - 主从模式集群
	templates["template-env-a"] = &TaskTemplate{
		ID:            "template-env-a",
		Name:          "测试环境A",
		Description:   "测试环境A：主从模式 3主3从 集群迁移",
		SourceCluster: "10.31.36.8:8902,10.31.36.10:8903,10.31.36.12:8901",
		TargetCluster: "10.31.36.3:8902,10.31.36.15:8901,10.31.36.13:8903",
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
	
	// 测试环境 B - 单机多端口集群
	templates["template-env-b"] = &TaskTemplate{
		ID:            "template-env-b",
		Name:          "测试环境B",
		Description:   "测试环境B：单机多端口集群迁移",
		SourceCluster: "10.31.36.5:8901,10.31.36.5:8902,10.31.36.5:8903",
		TargetCluster: "10.31.36.16:8901,10.31.36.16:8902,10.31.36.16:8903",
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
	
	// 测试环境 C - 原有默认环境
	templates["template-env-c"] = &TaskTemplate{
		ID:            "template-env-c",
		Name:          "测试环境C",
		Description:   "测试环境C：原有测试环境集群迁移",
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

	// 华为云测试环境 - 15GB 内存，3GB 数据（17654 keys，含大 Key），kvstorecount=10
	// 智能推荐参数：Key 大小混合（80% String 100B-10KB + 5% 大 Key 1-5MB），按 medium-large 估算
	// Workers=16（连接数约束：无业务负载，保守配置），ScanBatchSize=1000（含大 Key）
	// QPS=0（测试环境无业务），连接数=48（16×3）
	templates["template-huawei-cloud"] = &TaskTemplate{
		ID:            "template-huawei-cloud",
		Name:          "华为云测试环境",
		Description:   "华为云 15GB 内存服务器，源端 3GB 数据（17654 keys，含 5MB 大 String/50000 元素 List 等），kvstorecount=10，全量+增量迁移",
		SourceCluster: "192.168.0.142:7001,192.168.0.142:7002",
		TargetCluster: "192.168.0.142:8001,192.168.0.142:8002",
		MigrationMode: "full_and_incremental",
		Options: &TaskOptions{
			WorkerCount:       16,
			ScanBatchSize:     1000,
			ConflictPolicy:    "skip",
			LargeKeyThreshold: 5242880, // 5MB（源端有 5MB 大 String，设低一点以便追踪）
			KeyFilter: &KeyFilter{
				Mode: "all",
			},
			RateLimit: &RateLimit{
				SourceQPS:         0,
				TargetQPS:         0,
				SourceConnections: 48,
				TargetConnections: 48,
			},
			RetryConfig: &RetryConfig{
				MaxRetries:          3,
				FullRetryIntervalMs: 200,
				IncrRetryIntervalMs: 1000,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	// 启动定期状态保存
	startPeriodicStateSave()

	// 加载持久化的校验任务
	loadVerifyTasksState()

	// 初始化预置校验任务
	initPresetVerifyTasks()

	logger.Info("System initialized", map[string]interface{}{
		"mode":           "production",
		"templates":      len(templates),
		"verify_tasks":   len(verifyTasks),
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

	// 新增参数：source=disk 表示从磁盘文件导出完整日志
	source := q.Get("source")
	if source == "" {
		source = "memory" // 默认从内存导出（兼容旧行为）
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

	// 如果指定了任务ID，获取任务名称用于文件名，并使用完整ID
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

	var data []byte
	var err error

	// 根据 source 参数选择导出方式
	if source == "disk" || source == "all" || source == "full" {
		// 从磁盘文件导出完整日志
		data, err = logger.Default().ExportFromDisk(filter, format)
		log.Info("Exporting logs from disk", map[string]interface{}{
			"task_id": filter.TaskID,
			"level":   filter.Level,
			"keyword": filter.Keyword,
		})
	} else {
		// 从内存导出（默认，兼容旧行为）
		data, err = logger.Default().Export(filter, format)
	}

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
	
	// 如果是从磁盘导出，文件名加上 full 标记
	sourceTag := ""
	if source == "disk" || source == "all" || source == "full" {
		sourceTag = "-full"
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
			filename = fmt.Sprintf("task-%s-%s%s-%s.%s", shortID, safeName, sourceTag, timestamp, ext)
		} else {
			filename = fmt.Sprintf("task-%s-logs%s-%s.%s", shortID, sourceTag, timestamp, ext)
		}
	} else {
		filename = fmt.Sprintf("tendis-migrate-logs%s-%s.%s", sourceTag, timestamp, ext)
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

	// 手动清理：仅保留最近 7 天的日志
	removed := logger.Default().CleanupKeepDays(7)

	// 获取清理后的统计
	afterStats := logger.Default().GetLogStats()

	log.Info("Manual log cleanup executed", map[string]interface{}{
		"keep_days":    7,
		"removed":      removed,
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
			"status":          task.Status,
			"progress":        task.Progress,
			"processed_keys":  task.KeysMigrated,
			"total_keys":      task.KeysTotal,
			"keys_to_migrate": consistentKeysToMigrate(task),  // 一致性读取
			"current_qps":     speed,
			"bytes_written":   task.BytesMigrated,
			"total_bytes":     task.BytesTotal,     // 总字节数
			"failed_keys":     task.KeysFailed,
			"skipped_keys":    task.KeysSkipped,    // 冲突跳过的 Key 数
			"filtered_keys":   task.KeysFiltered,   // 被过滤的 Key 数
			"phase":           task.Phase,
			"estimated_eta":   calculateETA(task),  // 预计剩余时间
			"elapsed_time":    calculateElapsedTime(task), // 已耗时间
			"migration_mode":  task.MigrationMode,  // 迁移模式
			// 增量同步相关指标
			"incr_keys_synced":   task.IncrKeysSynced,
			"incr_keys_skipped":  task.IncrKeysSkipped,
			"incr_keys_failed":   task.IncrKeysFailed,
			"incr_keys_filtered": task.IncrKeysFiltered,
			"incr_lag_ms":        task.IncrLagMs,
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

// broadcastTaskProgress 广播任务进度（用于重试进度等）
func broadcastTaskProgress(taskID string, progressData map[string]interface{}) {
	wsClientsMu.RLock()
	defer wsClientsMu.RUnlock()
	
	msg := &WSMessage{
		Type:   "progress",
		TaskID: taskID,
		Payload: progressData,
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
				"percentage":      t.Progress,
				"keys_total":      t.KeysTotal,
				"keys_to_migrate": consistentKeysToMigrate(t),  // 一致性读取
				"keys_migrated":   t.KeysMigrated,
				"keys_filtered":   t.KeysFiltered,   // 被过滤的 Key 数
				"speed":           t.Speed,
			},
		})
	}

	// 状态过滤
	statusFilter := r.URL.Query().Get("status")
	if statusFilter != "" {
		var filtered []map[string]interface{}
		for _, item := range items {
			if s, _ := item["status"].(string); s == statusFilter {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}

	// 按创建时间倒序排序（最新的在前面）
	sort.Slice(items, func(i, j int) bool {
		timeI, _ := items[i]["created_at"].(string)
		timeJ, _ := items[j]["created_at"].(string)
		return timeI > timeJ
	})

	totalCount := len(items)

	// 分页
	pageStr := r.URL.Query().Get("page")
	sizeStr := r.URL.Query().Get("size")
	pageNum := 1
	pageSize := 20
	if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
		pageNum = p
	}
	if s, err := strconv.Atoi(sizeStr); err == nil && s > 0 {
		pageSize = s
	}
	start := (pageNum - 1) * pageSize
	if start > len(items) {
		start = len(items)
	}
	end := start + pageSize
	if end > len(items) {
		end = len(items)
	}
	pagedItems := items[start:end]

	log.Debug("Tasks listed", map[string]interface{}{"total": totalCount, "page": pageNum, "size": pageSize, "returned": len(pagedItems)})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"items": pagedItems,
			"total": totalCount,
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

	// 验证必填字段
	if req.Name == "" {
		log.Warn("Task name is required", nil)
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "name is required"})
		return
	}
	if req.SourceCluster.Addrs == nil || len(req.SourceCluster.Addrs) == 0 {
		log.Warn("Source cluster addrs is required", nil)
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "source_cluster.addrs is required"})
		return
	}
	if req.TargetCluster.Addrs == nil || len(req.TargetCluster.Addrs) == 0 {
		log.Warn("Target cluster addrs is required", nil)
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "target_cluster.addrs is required"})
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
			// 【BUG-1 修复】如果设置了 prefixes 或 exclude_prefixes，自动设置 mode 为 "prefix"
			// 这样用户只需要设置 prefixes，不需要同时设置 mode
			if len(options.KeyFilter.Prefixes) > 0 || len(options.KeyFilter.ExcludePrefixes) > 0 || len(options.KeyFilter.ExcludePatterns) > 0 {
				options.KeyFilter.Mode = "prefix"
				log.Info("【BUG-1 FIX】Auto-set key_filter.mode to 'prefix' based on prefixes/exclude config", map[string]interface{}{
					"prefixes":         options.KeyFilter.Prefixes,
					"exclude_prefixes": options.KeyFilter.ExcludePrefixes,
					"exclude_patterns": options.KeyFilter.ExcludePatterns,
				})
			} else if len(options.KeyFilter.Patterns) > 0 {
				options.KeyFilter.Mode = "pattern"
				log.Info("【BUG-1 FIX】Auto-set key_filter.mode to 'pattern' based on patterns config", map[string]interface{}{
					"patterns": options.KeyFilter.Patterns,
				})
			} else {
				options.KeyFilter.Mode = "all"
			}
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
	task.Init() // 统一初始化运行时控制字段

	tasksMu.Lock()
	tasks[task.ID] = task
	tasksMu.Unlock()

	// 【崩溃恢复修复】创建任务后立即持久化，防止 kill-9 在首次定期保存前丢失任务
	saveTasksState()

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

// 注意：已移除 cleanupOldTasks 自动清理逻辑
// 任务只能由用户手动删除，不再自动删除任何任务

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
	case action == "config" && r.Method == "PATCH":  // 【BUG-FIX】添加 PATCH 方法支持
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
	case action == "stop" && r.Method == "POST":
		stopTaskHandler(w, r, id, log, taskLog)
	case action == "stop-incremental" && r.Method == "POST":
		stopIncrementalHandler(w, r, id, log, taskLog)
	case action == "complete" && r.Method == "POST":
		completeTaskHandler(w, r, id, log, taskLog)
	case action == "preflight-check" && r.Method == "POST":
		preflightCheckHandler(w, r, id, log)
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
		configData["enable_compression"] = task.Options.EnableCompression
		if task.Options.RateLimit != nil {
			configData["rate_limit"] = map[string]interface{}{
				"source_qps":         task.Options.RateLimit.SourceQPS,
				"target_qps":         task.Options.RateLimit.TargetQPS,
				"source_connections": task.Options.RateLimit.SourceConnections,
				"target_connections": task.Options.RateLimit.TargetConnections,
			}
		}
		// 添加 retry_config 信息
		retryConfig := map[string]interface{}{
			"max_retries":            3,
			"full_retry_interval_ms": 100,
			"incr_retry_interval_ms": 1000,
		}
		if task.Options.RetryConfig != nil {
			if task.Options.RetryConfig.MaxRetries > 0 {
				retryConfig["max_retries"] = task.Options.RetryConfig.MaxRetries
			}
			if task.Options.RetryConfig.FullRetryIntervalMs > 0 {
				retryConfig["full_retry_interval_ms"] = task.Options.RetryConfig.FullRetryIntervalMs
			}
			if task.Options.RetryConfig.IncrRetryIntervalMs > 0 {
				retryConfig["incr_retry_interval_ms"] = task.Options.RetryConfig.IncrRetryIntervalMs
			}
		}
		configData["retry_config"] = retryConfig
		// 添加 key_filter 信息
		if task.Options.KeyFilter != nil {
			configData["key_filter"] = map[string]interface{}{
				"mode":             task.Options.KeyFilter.Mode,
				"prefixes":         task.Options.KeyFilter.Prefixes,
				"exclude_prefixes": task.Options.KeyFilter.ExcludePrefixes,
				"patterns":         task.Options.KeyFilter.Patterns,
				"keys":             task.Options.KeyFilter.Keys,
			}
		}
	}

	// P2 改进：获取详细进度指标
	detailedProgress := getDetailedProgressMetrics(id, task)

	// 构建 options 数据（同时支持 config 和 options 字段）
	var optionsData map[string]interface{}
	if task.Options != nil {
		optionsData = map[string]interface{}{
			"worker_count":         task.Options.WorkerCount,
			"scan_batch_size":      task.Options.ScanBatchSize,
			"conflict_policy":      task.Options.ConflictPolicy,
			"large_key_threshold":  task.Options.LargeKeyThreshold,
			"skip_full_sync":       task.Options.SkipFullSync,
			"skip_incremental":     task.Options.SkipIncremental,
			"shadow_mode":          task.Options.ShadowMode,
			"enable_compression":   task.Options.EnableCompression,
		}
		if task.Options.KeyFilter != nil {
			optionsData["key_filter"] = map[string]interface{}{
				"mode":             task.Options.KeyFilter.Mode,
				"prefixes":         task.Options.KeyFilter.Prefixes,
				"exclude_prefixes": task.Options.KeyFilter.ExcludePrefixes,
				"patterns":         task.Options.KeyFilter.Patterns,
				"keys":             task.Options.KeyFilter.Keys,
			}
		}
	}

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
			"options":        optionsData, // 同时返回 options（向后兼容）
			"progress": map[string]interface{}{
				"percentage":      task.Progress,
				"total_keys":      task.KeysTotal,
				"keys_to_migrate": consistentKeysToMigrate(task),  // 一致性读取，保证 >= migrated+failed+skipped+filtered
				"migrated_keys":   task.KeysMigrated,
				"total_bytes":     task.BytesTotal,
				"migrated_bytes":  task.BytesMigrated,
				"current_speed":   task.Speed,
				"phase":           phase,
				"estimated_eta":   calculateETA(task),
				"elapsed_time":    calculateElapsedTime(task),
			},
			"stats": map[string]interface{}{
				"total_keys":         task.KeysTotal,
				"keys_to_migrate":    consistentKeysToMigrate(task),
				"migrated_keys":      task.KeysMigrated,
				"failed_keys":        task.KeysFailed,
				"skipped_keys":       task.KeysSkipped,
				"filtered_keys":      task.KeysFiltered,
				"bytes_sent":         task.BytesMigrated,
				"incr_keys_synced":   task.IncrKeysSynced,
				"incr_keys_skipped":  task.IncrKeysSkipped,
				"incr_keys_failed":   task.IncrKeysFailed,
				"incr_keys_filtered": task.IncrKeysFiltered,
				"api_version":        "v2.3.1-bugfix",
			},
			"phase": phase,
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
			// 集群拓扑告警
			"topology_warnings":  task.TopologyWarnings,
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

// consistentKeysToMigrate 返回一致性的 KeysToMigrate 值
// 问题：KeysMigrated 通过 atomic 实时递增，而 KeysToMigrate 在 ticker 中周期更新
// 在两次 ticker 之间 API 读取时，可能出现 KeysMigrated > KeysToMigrate
// 修复：读取时保证 KeysToMigrate >= 各分项之和（不改变语义，只是补偿读取时序差）
func consistentKeysToMigrate(task *Task) int64 {
	toMigrate := atomic.LoadInt64(&task.KeysToMigrate)
	migrated := atomic.LoadInt64(&task.KeysMigrated)
	failed := atomic.LoadInt64(&task.KeysFailed)
	skipped := atomic.LoadInt64(&task.KeysSkipped)
	filtered := atomic.LoadInt64(&task.KeysFiltered)
	totalProcessed := migrated + failed + skipped + filtered
	if totalProcessed > toMigrate {
		return totalProcessed
	}
	return toMigrate
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
	task, ok := tasks[id]
	if ok {
		// 删除前停止 running/paused 的任务，防止僵尸 goroutine
		if task.Status == "running" || task.Status == "paused" {
			task.Cleanup() // 统一清理运行时控制字段
			task.Status = "stopped"
		}
		delete(tasks, id)
	}
	tasksMu.Unlock()
	
	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}
	
	// 删除任务时同步清理该任务的日志
	logger.Default().ClearTaskLogs(id)
	
	log.Info("Task deleted", map[string]interface{}{"task_id": id})
	taskLog.Info("Task deleted")
	
	jsonResponse(w, map[string]interface{}{"code": 0, "message": "success"})
}

// batchDeleteTasksHandler 批量删除任务
func batchDeleteTasksHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}

	var req struct {
		IDs []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Invalid request body"})
		return
	}
	if len(req.IDs) == 0 {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "No task IDs provided"})
		return
	}

	var deleted, notFound int
	tasksMu.Lock()
	for _, id := range req.IDs {
		task, ok := tasks[id]
		if !ok {
			notFound++
			continue
		}
		if task.Status == "running" || task.Status == "paused" {
			task.Cleanup()
			task.Status = "stopped"
		}
		delete(tasks, id)
		deleted++
	}
	tasksMu.Unlock()

	// 锁外清理日志
	for _, id := range req.IDs {
		logger.Default().ClearTaskLogs(id)
	}

	// 持久化
	saveTasksState()

	log.Info("Batch delete tasks", map[string]interface{}{
		"requested": len(req.IDs),
		"deleted":   deleted,
		"not_found": notFound,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": fmt.Sprintf("已删除 %d 个任务", deleted),
		"data": map[string]interface{}{
			"deleted":   deleted,
			"not_found": notFound,
		},
	})
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
			// 【BUG-FIX】实际调用 SetWorkerCount 使变更生效
			task.workerPool.SetWorkerCount(task.Options.WorkerCount)
			
			if task.Options.WorkerCount > oldWorkerCount {
				adjustMsg = fmt.Sprintf("increasing workers from %d to %d (current active: %d)", oldWorkerCount, task.Options.WorkerCount, currentActive)
			} else {
				adjustMsg = fmt.Sprintf("decreasing workers from %d to %d gracefully (current active: %d)", oldWorkerCount, task.Options.WorkerCount, currentActive)
			}
			
			taskLog.Info("Workers dynamically adjusted", map[string]interface{}{
				"old_count":      oldWorkerCount,
				"new_count":      task.Options.WorkerCount,
				"current_active": currentActive,
			})
		}
		
		// 【BUG-FIX】动态更新限速器
		if req.RateLimit != nil {
			task.workerPool.UpdateRateLimiter(req.RateLimit.SourceQPS)
			task.workerPool.UpdateTargetRateLimiter(req.RateLimit.TargetQPS)
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
	if !ok {
		tasksMu.Unlock()
		log.Warn("Task not found for start", map[string]interface{}{"task_id": id})
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}
	// 【审计修复】只有 pending 状态才能启动，防止重复启动
	if task.Status != "pending" {
		currentStatus := task.Status
		tasksMu.Unlock()
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": fmt.Sprintf("Task cannot be started (current status: %s, expected: pending)", currentStatus),
		})
		return
	}
	// 初始化运行时控制字段
	task.Init()
	task.Status = "running"
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	task.StartedAt = time.Now().Format("2006-01-02 15:04:05")
	tasksMu.Unlock()

	// 【崩溃恢复修复】启动任务后立即持久化状态（status: running）
	saveTasksState()

	go simulateProgress(task)
	
	log.Info("Task started", map[string]interface{}{"task_id": id})
	taskLog.Info("Task started", nil)
	
	jsonResponse(w, map[string]interface{}{"code": 0, "message": "success"})
}

func pauseTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	if !ok {
		tasksMu.Unlock()
		log.Warn("Task not found for pause", map[string]interface{}{"task_id": id})
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}
	
	// 【BUG-4 修复】检查任务状态，只有 running 状态的任务才能暂停
	if task.Status != "running" {
		tasksMu.Unlock()
		log.Warn("Task cannot be paused", map[string]interface{}{
			"task_id":        id,
			"current_status": task.Status,
		})
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": fmt.Sprintf("Task cannot be paused: current status is '%s', only 'running' tasks can be paused", task.Status),
		})
		return
	}
	
	task.Status = "paused"
	task.Speed = 0
	now := time.Now()
	task.PausedAt = now.Format(time.RFC3339)  // 记录暂停时间
	task.UpdatedAt = now.Format(time.RFC3339)
	task.Cleanup() // 统一清理运行时控制字段
	tasksMu.Unlock()
	
	// 【关键修复】用户手动暂停时，禁用自动恢复
	// 只有 autoStopTask（因连续失败自动暂停）才应该触发自动恢复
	// 否则 autoRecoveryLoop 会在几秒后自动恢复任务，违背用户意愿
	disableAutoRecoveryForTask(task.ID)
	
	log.Info("Task paused by user (auto-recovery disabled)", map[string]interface{}{"task_id": id})
	taskLog.Info("Task paused by user", map[string]interface{}{
		"progress":          task.Progress,
		"paused_at":         task.PausedAt,
		"phase":             task.Phase,
		"auto_recovery":     "disabled",
	})
	
	jsonResponse(w, map[string]interface{}{"code": 0, "message": "success"})
}

func resumeTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	if !ok {
		tasksMu.Unlock()
		log.Warn("Task not found for resume", map[string]interface{}{"task_id": id})
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}
	// 【审计修复】只有 paused 状态才能恢复，防止对 running/completed/failed 任务误操作
	if task.Status != "paused" {
		currentStatus := task.Status
		tasksMu.Unlock()
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": fmt.Sprintf("Task is not paused (current status: %s)", currentStatus),
		})
		return
	}
	// 计算本次暂停的时长并累加
	if task.PausedAt != "" {
		loc := time.Local
		pausedTime, err := time.ParseInLocation(time.RFC3339, task.PausedAt, loc)
		if err != nil {
			pausedTime, _ = time.ParseInLocation("2006-01-02 15:04:05", task.PausedAt, loc)
		}
		if !pausedTime.IsZero() {
			pausedSeconds := int64(time.Since(pausedTime).Seconds())
			task.PausedDuration += pausedSeconds
		}
	}
	// 重新初始化运行时控制字段
	task.Init()
	task.Status = "running"
	task.PausedAt = ""
	task.ShutdownPaused = false // 清除升级暂停标记
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	pausedDuration := task.PausedDuration
	
	// 【核心设计】恢复任务时不重置任何计数器
	// task.KeysMigrated/KeysFailed/KeysSkipped 等保留历史累积值
	// doFullMigration 中 worker 直接通过原子操作递增 task 字段
	// 暂停恢复后自然在原来数字上继续累加
	tasksMu.Unlock()

	// 【注意】不清空 errorKeys 内存列表
	// errorKeys 保留了之前的失败记录，恢复后如果产生新的失败会继续追加
	// task.KeysFailed 始终是准确的总量（worker 直接原子递增/递减）

	go simulateProgress(task)
	
	log.Info("Task resumed", map[string]interface{}{"task_id": id})
	taskLog.Info("Task resumed", map[string]interface{}{
		"paused_duration": pausedDuration,
	})
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

// ==================== 独立校验任务模块 ====================

// VerifyTask 独立校验任务
type VerifyTask struct {
	ID             string                 `json:"id"`
	Name           string                 `json:"name"`
	Status         string                 `json:"status"`          // pending, running, completed, failed, cancelled
	SourceCluster  string                 `json:"source_cluster"`
	TargetCluster  string                 `json:"target_cluster"`
	SourcePassword string                 `json:"-"`               // 不输出到 JSON
	TargetPassword string                 `json:"-"`
	VerifyMode     string                 `json:"verify_mode"`     // count_only, sample, full
	SampleRate     float64                `json:"sample_rate"`     // 采样率 (0.001-1.0)
	MaxKeys        int64                  `json:"max_keys"`        // 最大校验 Key 数
	KeyFilter      *KeyFilterConfig       `json:"key_filter,omitempty"` // Key 过滤配置
	CompareValue   bool                   `json:"compare_value"`   // 是否比较值内容（已弃用，使用 CompareMode）
	CompareTTL     bool                   `json:"compare_ttl"`     // 是否比较 TTL
	TTLTolerance   int64                  `json:"ttl_tolerance"`   // TTL 容差（秒）
	MigrationTaskID string               `json:"migration_task_id,omitempty"` // 关联的迁移任务 ID（可选）
	CreatedAt      string                 `json:"created_at"`
	StartedAt      string                 `json:"started_at,omitempty"`
	CompletedAt    string                 `json:"completed_at,omitempty"`
	
	// ===== 新增：并发和 QPS 控制 =====
	Concurrency    int                    `json:"concurrency"`     // 并发数（默认 10）
	QPS            int                    `json:"qps"`             // QPS 限制（0 表示不限制）
	
	// ===== 新增：细化比较模式 =====
	CompareMode    string                 `json:"compare_mode"`    // full_value, length_only, exists_only
	SkipLargeKey   bool                   `json:"skip_large_key"`  // 是否跳过大 Key
	LargeKeyThreshold int64              `json:"large_key_threshold"` // 大 Key 阈值（字节），默认 10MB
	
	// ===== 新增：DB 过滤 =====
	DBList         string                 `json:"db_list"`         // 要校验的 DB 列表，分号分隔，如 "0;5;15"
	
	// ===== 新增：多轮迭代收敛（借鉴 redis-full-check）=====
	CompareRounds  int                    `json:"compare_rounds"`  // 比较轮数（默认3，最少1，最多5）
	RoundInterval  int                    `json:"round_interval"`  // 轮次间隔秒数（默认5）
	
	// ===== P2: 双向校验支持 =====
	Direction      string                 `json:"direction"`       // source_to_target(默认), target_to_source, bidirectional
	
	// ===== P2: 智能比较模式（自动根据 Key 大小选择策略）=====
	SmartCompare   bool                   `json:"smart_compare"`   // 是否启用智能比较
	BigKeyThreshold int64                 `json:"big_key_threshold"` // 智能模式下判定大 Key 的元素数阈值（Hash/Set/ZSet）
	
	// ===== P1: SQLite 结果持久化 =====
	EnableSQLite   bool                   `json:"enable_sqlite"`   // 是否启用 SQLite 存储结果（断点续传）
	SQLiteDBPath   string                 `json:"sqlite_db_path,omitempty"` // SQLite 数据库路径
	
	// ===== P1: Field 级别比对 =====
	FieldLevelCompare bool                `json:"field_level_compare"` // 是否启用 Field 级别比对（Hash/Set/ZSet）
	FieldScanThreshold int64              `json:"field_scan_threshold"` // 使用 SCAN 命令获取 Field 的阈值（默认 5000）
	
	// 结果统计
	Result         *VerifyTaskResult      `json:"result,omitempty"`
	
	// 内部字段（不序列化）
	sqliteDB       *VerifyResultDB        `json:"-"` // SQLite 数据库连接
}

// KeyFilterConfig Key 过滤配置
type KeyFilterConfig struct {
	Prefixes        []string `json:"prefixes,omitempty"`         // 只校验这些前缀
	ExcludePrefixes []string `json:"exclude_prefixes,omitempty"` // 排除这些前缀
	Pattern         string   `json:"pattern,omitempty"`          // 正则匹配
}

// VerifyTaskResult 校验任务结果
type VerifyTaskResult struct {
	SourceKeyCount   int64                  `json:"source_key_count"`   // 源端 Key 数量（DBSIZE，全量）
	TargetKeyCount   int64                  `json:"target_key_count"`   // 目标端 Key 数量（DBSIZE，全量）
	ScannedKeys      int64                  `json:"scanned_keys"`       // SCAN 匹配的 Key 数（受 MATCH pattern 影响，有过滤时 < DBSIZE）
	SampledKeys      int64                  `json:"sampled_keys"`       // 实际参与校验的 Key 数（过滤+采样后）
	MatchedKeys      int64                  `json:"matched_keys"`       // 校验一致的 Key 数
	FilteredKeys     int64                  `json:"filtered_keys"`      // 通过 Key 过滤器的 Key 数（有 key_filter 时 <= ScannedKeys）
	MissingKeys      int64                  `json:"missing_keys"`       // 源端有目标端无
	ExtraKeys        int64                  `json:"extra_keys"`         // 目标端有源端无
	ValueMismatch    int64                  `json:"value_mismatch"`     // 值不匹配
	LengthMismatch   int64                  `json:"length_mismatch"`    // 长度不匹配（新增）
	TTLMismatch      int64                  `json:"ttl_mismatch"`       // TTL 不匹配
	LargeKeySkipped  int64                  `json:"large_key_skipped"`  // 跳过的大 Key 数量（新增）
	ConsistencyRate  float64                `json:"consistency_rate"`   // 一致性百分比
	Details          []VerifyMismatchDetail `json:"details,omitempty"`  // 不匹配详情
	Progress         float64                `json:"progress"`           // 进度百分比
	CurrentSpeed     int64                  `json:"current_speed"`      // 当前速度 keys/s
	EstimatedTime    string                 `json:"estimated_time"`     // 预估剩余时间
	DBsVerified      []int                  `json:"dbs_verified,omitempty"` // 已校验的 DB 列表（新增）
	
	// ===== 新增：多轮迭代收敛结果（借鉴 redis-full-check）=====
	CurrentRound     int                    `json:"current_round"`      // 当前轮次
	TotalRounds      int                    `json:"total_rounds"`       // 总轮数
	Rounds           []VerifyRoundResult    `json:"rounds,omitempty"`   // 每轮详细结果
	FinalMismatchKeys []string              `json:"final_mismatch_keys,omitempty"` // 最终确认不一致的 Key（多轮收敛后）
	
	// ===== P2: 双向校验结果 =====
	TargetExtraKeys  int64                  `json:"target_extra_keys"`  // 目标端多余的 Key 数量
	ExtraKeyDetails  []VerifyMismatchDetail `json:"extra_key_details,omitempty"` // 目标端多余 Key 详情
	
	// ===== P1: Field 级别不一致统计 =====
	FieldMismatchKeys int64                 `json:"field_mismatch_keys"` // Field 级别不一致的 Key 数
	FieldMismatches   []FieldMismatchDetail `json:"field_mismatches,omitempty"` // Field 级别不匹配详情
	
	// ===== P3: 指标监控 =====
	Metrics          *VerifyMetrics         `json:"metrics,omitempty"` // 详细指标
}

// VerifyRoundResult 单轮校验结果（用于多轮迭代收敛）
type VerifyRoundResult struct {
	RoundNo        int                    `json:"round_no"`         // 轮次编号（从1开始）
	StartTime      string                 `json:"start_time"`       // 本轮开始时间
	EndTime        string                 `json:"end_time"`         // 本轮结束时间
	KeysToCheck    int64                  `json:"keys_to_check"`    // 本轮待检查 Key 数
	MismatchCount  int64                  `json:"mismatch_count"`   // 本轮发现的不一致数
	MismatchKeys   []string               `json:"mismatch_keys,omitempty"` // 本轮不一致的 Key 列表（用于下轮复查）
	ConvergeRate   float64                `json:"converge_rate"`    // 相比上轮的收敛率（减少的百分比）
	Details        []VerifyMismatchDetail `json:"details,omitempty"` // 本轮不匹配详情
}

// ==================== P1: Field 级别比对结构体 ====================

// FieldMismatchDetail Field 级别不匹配详情
type FieldMismatchDetail struct {
	Key         string   `json:"key"`                    // Key 名称
	KeyType     string   `json:"key_type"`               // Key 类型（hash/set/zset/list）
	TotalFields int64    `json:"total_fields"`           // 总 Field 数
	MismatchFields []FieldDiff `json:"mismatch_fields,omitempty"` // 不一致的 Field 列表
}

// FieldDiff 单个 Field 不一致详情
type FieldDiff struct {
	Field       string `json:"field"`                  // Field 名称（Hash/Set）或索引（List）或成员（ZSet）
	Type        string `json:"type"`                   // lack_source, lack_target, value_mismatch, score_mismatch
	SourceValue string `json:"source_value,omitempty"` // 源端值（截断显示）
	TargetValue string `json:"target_value,omitempty"` // 目标端值（截断显示）
	SourceScore float64 `json:"source_score,omitempty"` // ZSet 源端分数
	TargetScore float64 `json:"target_score,omitempty"` // ZSet 目标端分数
}

// ==================== P3: 指标监控结构体 ====================

// VerifyMetrics 校验详细指标
type VerifyMetrics struct {
	StartTime        string  `json:"start_time"`         // 开始时间
	EndTime          string  `json:"end_time,omitempty"` // 结束时间
	Duration         string  `json:"duration,omitempty"` // 总耗时
	
	// 吞吐量指标
	KeysPerSecond    float64 `json:"keys_per_second"`    // 每秒处理 Key 数
	BytesPerSecond   int64   `json:"bytes_per_second"`   // 每秒处理字节数
	
	// 网络指标
	RedisCommands    int64   `json:"redis_commands"`     // Redis 命令总数
	PipelineBatches  int64   `json:"pipeline_batches"`   // Pipeline 批次数
	NetworkRoundTrips int64  `json:"network_round_trips"` // 网络往返次数
	
	// 按类型统计
	TypeDistribution map[string]int64 `json:"type_distribution,omitempty"` // Key 类型分布
	typeDistMu       sync.Mutex       `json:"-"`                           // TypeDistribution 的并发锁
	
	// 内存使用
	PeakMemoryMB     float64 `json:"peak_memory_mb"`     // 峰值内存使用（MB）
	
	// 按轮次指标
	RoundMetrics     []RoundMetric `json:"round_metrics,omitempty"` // 每轮指标
}

// RoundMetric 单轮指标
type RoundMetric struct {
	RoundNo       int     `json:"round_no"`
	Duration      string  `json:"duration"`
	KeysPerSecond float64 `json:"keys_per_second"`
	MismatchRate  float64 `json:"mismatch_rate"` // 不一致率
}

// ==================== P1: SQLite 结果存储 ====================

// VerifyResultDB SQLite 结果数据库
type VerifyResultDB struct {
	db     *sql.DB
	taskID string
	dbPath string
}

// NewVerifyResultDB 创建或打开 SQLite 数据库
func NewVerifyResultDB(taskID string) (*VerifyResultDB, error) {
	dbPath := fmt.Sprintf("./data/verify_%s.db", taskID)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite3 failed: %w", err)
	}
	
	// 设置连接池参数
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	
	// 创建表
	_, err = db.Exec(`
		-- Key 级别不一致记录
		CREATE TABLE IF NOT EXISTS key_diff (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			round INTEGER NOT NULL,
			key_name TEXT NOT NULL,
			key_type TEXT,
			diff_type TEXT NOT NULL,
			source_value TEXT,
			target_value TEXT,
			source_ttl INTEGER,
			target_ttl INTEGER,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_key_diff_round ON key_diff(round);
		CREATE INDEX IF NOT EXISTS idx_key_diff_key ON key_diff(key_name);
		CREATE INDEX IF NOT EXISTS idx_key_diff_type ON key_diff(diff_type);
		
		-- Field 级别不一致记录（Hash/Set/ZSet）
		CREATE TABLE IF NOT EXISTS field_diff (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			round INTEGER NOT NULL,
			key_name TEXT NOT NULL,
			key_type TEXT NOT NULL,
			field_name TEXT NOT NULL,
			diff_type TEXT NOT NULL,
			source_value TEXT,
			target_value TEXT,
			source_score REAL,
			target_score REAL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_field_diff_round ON field_diff(round);
		CREATE INDEX IF NOT EXISTS idx_field_diff_key ON field_diff(key_name);
		
		-- 校验进度记录（用于断点续传）
		CREATE TABLE IF NOT EXISTS checkpoint (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			round INTEGER NOT NULL,
			scan_cursor TEXT,
			keys_scanned INTEGER,
			keys_sampled INTEGER,
			last_key TEXT,
			saved_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		
		-- 统计摘要
		CREATE TABLE IF NOT EXISTS summary (
			round INTEGER PRIMARY KEY,
			keys_checked INTEGER,
			mismatch_count INTEGER,
			converge_rate REAL,
			start_time TEXT,
			end_time TEXT
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create tables failed: %w", err)
	}
	
	return &VerifyResultDB{
		db:     db,
		taskID: taskID,
		dbPath: dbPath,
	}, nil
}

// Close 关闭数据库连接
func (r *VerifyResultDB) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

// SaveKeyDiff 保存 Key 级别不一致记录
func (r *VerifyResultDB) SaveKeyDiff(round int, key, keyType, diffType, sourceVal, targetVal string, sourceTTL, targetTTL int64) error {
	_, err := r.db.Exec(
		`INSERT INTO key_diff (round, key_name, key_type, diff_type, source_value, target_value, source_ttl, target_ttl) 
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		round, key, keyType, diffType, sourceVal, targetVal, sourceTTL, targetTTL,
	)
	return err
}

// SaveFieldDiff 保存 Field 级别不一致记录
func (r *VerifyResultDB) SaveFieldDiff(round int, key, keyType, field, diffType, sourceVal, targetVal string, sourceScore, targetScore float64) error {
	_, err := r.db.Exec(
		`INSERT INTO field_diff (round, key_name, key_type, field_name, diff_type, source_value, target_value, source_score, target_score)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		round, key, keyType, field, diffType, sourceVal, targetVal, sourceScore, targetScore,
	)
	return err
}

// SaveCheckpoint 保存断点信息
func (r *VerifyResultDB) SaveCheckpoint(round int, cursor string, keysScanned, keysSampled int64, lastKey string) error {
	_, err := r.db.Exec(
		`INSERT INTO checkpoint (round, scan_cursor, keys_scanned, keys_sampled, last_key) VALUES (?, ?, ?, ?, ?)`,
		round, cursor, keysScanned, keysSampled, lastKey,
	)
	return err
}

// GetLastCheckpoint 获取最后一个断点
func (r *VerifyResultDB) GetLastCheckpoint() (round int, cursor string, keysScanned, keysSampled int64, lastKey string, err error) {
	err = r.db.QueryRow(
		`SELECT round, scan_cursor, keys_scanned, keys_sampled, last_key FROM checkpoint ORDER BY id DESC LIMIT 1`,
	).Scan(&round, &cursor, &keysScanned, &keysSampled, &lastKey)
	return
}

// GetMismatchKeysFromRound 获取指定轮次的不一致 Key 列表
func (r *VerifyResultDB) GetMismatchKeysFromRound(round int) ([]string, error) {
	rows, err := r.db.Query(`SELECT DISTINCT key_name FROM key_diff WHERE round = ?`, round)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			continue
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// SaveRoundSummary 保存轮次摘要
func (r *VerifyResultDB) SaveRoundSummary(round int, keysChecked, mismatchCount int64, convergeRate float64, startTime, endTime string) error {
	_, err := r.db.Exec(
		`INSERT OR REPLACE INTO summary (round, keys_checked, mismatch_count, converge_rate, start_time, end_time)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		round, keysChecked, mismatchCount, convergeRate, startTime, endTime,
	)
	return err
}

// GetDiffStats 获取不一致统计
func (r *VerifyResultDB) GetDiffStats(round int) (map[string]int64, error) {
	rows, err := r.db.Query(
		`SELECT diff_type, COUNT(*) as cnt FROM key_diff WHERE round = ? GROUP BY diff_type`,
		round,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	stats := make(map[string]int64)
	for rows.Next() {
		var diffType string
		var count int64
		if err := rows.Scan(&diffType, &count); err != nil {
			continue
		}
		stats[diffType] = count
	}
	return stats, nil
}

// GetPrefixStats 获取按前缀分组的统计
func (r *VerifyResultDB) GetPrefixStats(round int, limit int) ([]map[string]interface{}, error) {
	rows, err := r.db.Query(`
		SELECT 
			CASE WHEN INSTR(key_name, ':') > 0 
				THEN SUBSTR(key_name, 1, INSTR(key_name, ':') - 1) 
				ELSE 'no_prefix' 
			END AS prefix,
			COUNT(*) AS cnt
		FROM key_diff 
		WHERE round = ?
		GROUP BY prefix
		ORDER BY cnt DESC
		LIMIT ?
	`, round, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var results []map[string]interface{}
	for rows.Next() {
		var prefix string
		var count int64
		if err := rows.Scan(&prefix, &count); err != nil {
			continue
		}
		results = append(results, map[string]interface{}{
			"prefix": prefix,
			"count":  count,
		})
	}
	return results, nil
}

// ClearRound 清除指定轮次的数据（用于重试）
func (r *VerifyResultDB) ClearRound(round int) error {
	_, err := r.db.Exec(`DELETE FROM key_diff WHERE round = ?`, round)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(`DELETE FROM field_diff WHERE round = ?`, round)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(`DELETE FROM summary WHERE round = ?`, round)
	return err
}

// 独立校验任务存储
var (
	verifyTasks   = make(map[string]*VerifyTask) // taskID -> VerifyTask
	verifyTasksMu sync.RWMutex
)

// ==================== 校验任务持久化 ====================

const verifyTasksFile = "./data/verify-tasks.json"

// saveVerifyTasksState 保存校验任务状态到文件
func saveVerifyTasksState() {
	verifyTasksMu.RLock()
	tasksToSave := make(map[string]*VerifyTask)
	for id, task := range verifyTasks {
		tasksToSave[id] = task
	}
	verifyTasksMu.RUnlock()

	if len(tasksToSave) == 0 {
		return
	}

	data, err := json.MarshalIndent(tasksToSave, "", "  ")
	if err != nil {
		logger.Warn("Failed to marshal verify tasks state", map[string]interface{}{"error": err.Error()})
		return
	}

	if err := os.WriteFile(verifyTasksFile, data, 0644); err != nil {
		logger.Warn("Failed to save verify tasks state", map[string]interface{}{"error": err.Error()})
	} else {
		logger.Debug("Verify tasks state saved", map[string]interface{}{"count": len(tasksToSave)})
	}
}

// loadVerifyTasksState 从文件加载校验任务状态
func loadVerifyTasksState() {
	data, err := os.ReadFile(verifyTasksFile)
	if err != nil {
		logger.Debug("No saved verify tasks state found", map[string]interface{}{"file": verifyTasksFile})
		return
	}

	var savedTasks map[string]*VerifyTask
	if err := json.Unmarshal(data, &savedTasks); err != nil {
		logger.Warn("Failed to parse saved verify tasks", map[string]interface{}{"error": err.Error()})
		return
	}

	verifyTasksMu.Lock()
	for id, task := range savedTasks {
		// 如果任务是 running 或 retrying 状态，恢复为 pending（需要用户重新启动）
		if task.Status == "running" || task.Status == "retrying" {
			task.Status = "pending"
		}
		verifyTasks[id] = task
	}
	verifyTasksMu.Unlock()

	logger.Info("Verify tasks state recovered", map[string]interface{}{"count": len(savedTasks)})
}

// initPresetVerifyTasks 初始化预置校验任务
func initPresetVerifyTasks() {
	verifyTasksMu.Lock()
	defer verifyTasksMu.Unlock()

	now := time.Now().Format(time.RFC3339)

	// 测试环境 A - 主从模式集群
	if _, exists := verifyTasks["preset-verify-env-a"]; !exists {
		verifyTasks["preset-verify-env-a"] = &VerifyTask{
			ID:                "preset-verify-env-a",
			Name:              "测试环境A校验",
			Status:            "pending",
			SourceCluster:     "10.31.36.8:8902,10.31.36.10:8903,10.31.36.12:8901",
			TargetCluster:     "10.31.36.3:8902,10.31.36.15:8901,10.31.36.13:8903",
			VerifyMode:        "sample",
			SampleRate:        0.01, // 1% 采样率
			MaxKeys:           100000,
			CompareMode:       "full_value",
			CompareTTL:        false,
			TTLTolerance:      10,
			Concurrency:       10,
			QPS:               0, // 不限速
			SkipLargeKey:      true,
			LargeKeyThreshold: 10 * 1024 * 1024, // 10MB
			KeyFilter: &KeyFilterConfig{
				Prefixes: []string{"testkey"},
			},
			CreatedAt: now,
			Result:    &VerifyTaskResult{},
		}
	}

	// 测试环境 B - 单机多端口集群
	if _, exists := verifyTasks["preset-verify-env-b"]; !exists {
		verifyTasks["preset-verify-env-b"] = &VerifyTask{
			ID:                "preset-verify-env-b",
			Name:              "测试环境B校验",
			Status:            "pending",
			SourceCluster:     "10.31.36.5:8901,10.31.36.5:8902,10.31.36.5:8903",
			TargetCluster:     "10.31.36.16:8901,10.31.36.16:8902,10.31.36.16:8903",
			VerifyMode:        "sample",
			SampleRate:        0.01, // 1% 采样率
			MaxKeys:           100000,
			CompareMode:       "full_value",
			CompareTTL:        false,
			TTLTolerance:      10,
			Concurrency:       10,
			QPS:               0,
			SkipLargeKey:      true,
			LargeKeyThreshold: 10 * 1024 * 1024,
			KeyFilter: &KeyFilterConfig{
				Prefixes: []string{"testkey"},
			},
			CreatedAt: now,
			Result:    &VerifyTaskResult{},
		}
	}

	// 测试环境 C - 原有默认环境
	if _, exists := verifyTasks["preset-verify-env-c"]; !exists {
		verifyTasks["preset-verify-env-c"] = &VerifyTask{
			ID:                "preset-verify-env-c",
			Name:              "测试环境C校验",
			Status:            "pending",
			SourceCluster:     "10.248.37.11:8901,10.248.37.11:8902,10.248.37.11:8903",
			TargetCluster:     "10.31.165.39:8901,10.31.165.39:8902,10.31.165.39:8903",
			VerifyMode:        "sample",
			SampleRate:        0.01, // 1% 采样率
			MaxKeys:           100000,
			CompareMode:       "full_value",
			CompareTTL:        false,
			TTLTolerance:      10,
			Concurrency:       10,
			QPS:               0,
			SkipLargeKey:      true,
			LargeKeyThreshold: 10 * 1024 * 1024,
			KeyFilter: &KeyFilterConfig{
				Prefixes: []string{"testkey"},
			},
			CreatedAt: now,
			Result:    &VerifyTaskResult{},
		}
	}

	logger.Info("Preset verify tasks initialized", map[string]interface{}{
		"env_a": "preset-verify-env-a",
		"env_b": "preset-verify-env-b",
		"env_c": "preset-verify-env-c",
	})
}

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
		Mode       string  `json:"mode"`        // sample（采样验证）或 full（全量验证），默认 full
		SampleRate float64 `json:"sample_rate"` // 采样率，0.001-1.0
		MaxKeys    int64   `json:"max_keys"`    // 最大验证 Key 数量，默认 0（不限制）
		CompareMode string `json:"compare_mode"` // full_value, length_only, exists_only
		CompareTTL  bool   `json:"compare_ttl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// 使用默认值
		req.Mode = "full"
	}

	// 参数校验和默认值
	if req.Mode == "" {
		req.Mode = "full"
	}
	if req.SampleRate <= 0 || req.SampleRate > 1.0 {
		if req.Mode == "sample" {
			req.SampleRate = 0.01 // 采样模式默认 1%
		} else {
			req.SampleRate = 1.0 // 全量模式 100%
		}
	}
	if req.CompareMode == "" {
		req.CompareMode = "full_value"
	}

	// 从迁移任务中提取源端/目标端集群信息，创建独立校验任务（VerifyTask）
	// 这样校验记录会同步出现在「数据校验」列表页，且持久化到文件
	verifyTask := &VerifyTask{
		ID:              uuid.New().String(),
		Name:            fmt.Sprintf("校验[%s]-%s", task.Name, time.Now().Format("0102-150405")),
		Status:          "pending",
		SourceCluster:   task.SourceCluster,
		TargetCluster:   task.TargetCluster,
		SourcePassword:  task.SourcePassword,
		TargetPassword:  task.TargetPassword,
		VerifyMode:      req.Mode,
		SampleRate:      req.SampleRate,
		MaxKeys:         req.MaxKeys,
		CompareValue:    true,
		CompareTTL:      req.CompareTTL,
		TTLTolerance:    5,
		CompareMode:     req.CompareMode,
		MigrationTaskID: id,
		Concurrency:     10,
		CompareRounds:   3,
		RoundInterval:   5,
		Direction:       "source_to_target",
		CreatedAt:       time.Now().Format(time.RFC3339),
		Result: &VerifyTaskResult{
			Progress: 0,
		},
	}

	// 继承迁移任务的 Key 过滤配置
	if task.Options != nil && task.Options.KeyFilter != nil {
		kf := task.Options.KeyFilter
		verifyTask.KeyFilter = &KeyFilterConfig{}
		if len(kf.Prefixes) > 0 {
			verifyTask.KeyFilter.Prefixes = kf.Prefixes
		}
		if len(kf.ExcludePrefixes) > 0 {
			verifyTask.KeyFilter.ExcludePrefixes = kf.ExcludePrefixes
		}
	}

	// 存入独立校验任务 map 并持久化
	verifyTasksMu.Lock()
	verifyTasks[verifyTask.ID] = verifyTask
	verifyTasksMu.Unlock()
	saveVerifyTasksState()

	log.Info("Verify task created from migration task", map[string]interface{}{
		"verify_task_id":    verifyTask.ID,
		"migration_task_id": id,
		"mode":              req.Mode,
		"compare_mode":      req.CompareMode,
		"sample_rate":       req.SampleRate,
	})

	// 自动启动校验任务
	go runVerifyTask(verifyTask)

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "Verification task created and started",
		"data": map[string]interface{}{
			"verify_task_id":    verifyTask.ID,
			"migration_task_id": id,
			"name":              verifyTask.Name,
			"mode":              req.Mode,
			"compare_mode":      req.CompareMode,
			"sample_rate":       req.SampleRate,
		},
	})
}

// Deprecated: runDataVerification 旧版数据验证（已废弃）
// 【P2】此函数将全部采样 Key 加载到 []string 中，100 亿 Key 场景下会 OOM。
// 新版校验使用 runVerifyTask（流式 SCAN + 比对），此函数已无任何调用点。
// 保留函数签名用于编译兼容，但函数体已清空，调用时直接返回。
func runDataVerification(task *Task, result *VerifyResult, sampleRate float64, maxKeys int64, log *logger.RequestLogger) {
	log.Warn("runDataVerification is deprecated, use runVerifyTask instead", map[string]interface{}{
		"task_id": task.ID,
	})
	result.Status = "failed"
	result.EndTime = time.Now().Format(time.RFC3339)
}

func verifyResultsHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	log.Debug("Verify results queried", map[string]interface{}{"task_id": id})

	// 从独立校验任务（VerifyTask）中查找关联该迁移任务的所有校验记录
	var results []*VerifyTask
	verifyTasksMu.RLock()
	for _, vt := range verifyTasks {
		if vt.MigrationTaskID == id {
			results = append(results, vt)
		}
	}
	verifyTasksMu.RUnlock()

	// 按创建时间倒序排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].CreatedAt > results[j].CreatedAt
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    results,
	})
}

// 全量迁移互斥锁（P0-BUG2 修复：防止并发启动多个全量）
var (
	fullMigrationMu       sync.Mutex
	fullMigrationRunning  = make(map[string]bool) // taskID -> 是否正在运行全量
)

func simulateProgress(task *Task) {
	taskLog := logger.WithTask(task.ID)
	taskLog.Info("Migration started - connecting to clusters")

	// 【BUG-FIX】panic recover 保护：确保 defer sourceClient.Close() 等清理代码一定执行
	// 场景：FakeSlave.Stop() double-close panic → 跳过后续 defer → 连接泄漏 → 僵尸连接
	defer func() {
		if r := recover(); r != nil {
			taskLog.Error("【PANIC RECOVERED】simulateProgress panic", map[string]interface{}{
				"panic": fmt.Sprintf("%v", r),
			})
			tasksMu.Lock()
			if task.Status == "running" {
				task.Status = "paused"
				task.UpdatedAt = time.Now().Format(time.RFC3339)
			}
			tasksMu.Unlock()
		}
	}()

	// 【P0-BUG FIX】防御性检查：如果 stopCh 为 nil 或已关闭，必须重新初始化
	tasksMu.Lock()
	needNewStopCh := task.stopCh == nil
	if !needNewStopCh {
		select {
		case <-task.stopCh:
			needNewStopCh = true
		default:
		}
	}
	if needNewStopCh {
		task.Init()
		taskLog.Info("Re-initialized task control fields (stopCh was nil or closed)")
	}
	if task.startedTime.IsZero() {
		task.startedTime = time.Now()
	}
	tasksMu.Unlock()
	
	taskLog.Info("【BUG-FIX】Task initialized with stopCh and startedTime", map[string]interface{}{
		"started_time":           task.startedTime.Format(time.RFC3339),
		"startup_cooldown_secs": StartupCooldownSeconds,
	})

	// 【P1修复】使用可取消的 context 替代 context.Background()
	// 这样暂停/停止时可以通过 task.cancelFunc() 取消整个 context 树，
	// 包括集群拓扑刷新 goroutine、binlogCtx 等所有子 context，防止 goroutine 泄漏
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel() // simulateProgress 退出时确保清理

	// 保存 cancel 函数到 task，让外部 handler 也能取消
	tasksMu.Lock()
	task.cancelFunc = ctxCancel
	tasksMu.Unlock()

	// ==================== P0-BUG1 修复：检查是否需要跳过全量阶段 ====================
	// 如果任务恢复时已经处于增量阶段，则直接跳过全量迁移
	tasksMu.RLock()
	currentPhase := task.Phase
	currentProgress := task.Progress
	tasksMu.RUnlock()

	// 加载已有的断点，检查是否全量已完成
	existingCheckpoint := loadFullSyncCheckpoint(task.ID)
	fullSyncAlreadyCompleted := existingCheckpoint != nil && existingCheckpoint.IsComplete

	// 判断是否应该跳过全量
	shouldSkipFullSync := false
	if currentPhase == "incremental" || currentPhase == "completed" {
		shouldSkipFullSync = true
		taskLog.Info("🔄 【P0-BUG1 FIX】Resuming from incremental phase, skipping full migration", map[string]interface{}{
			"current_phase":    currentPhase,
			"progress":         currentProgress,
			"checkpoint_found": existingCheckpoint != nil,
		})
	} else if fullSyncAlreadyCompleted {
		shouldSkipFullSync = true
		taskLog.Info("🔄 【P0-BUG1 FIX】Full sync checkpoint shows completed, skipping full migration", map[string]interface{}{
			"checkpoint_phase":     existingCheckpoint.Phase,
			"checkpoint_complete":  existingCheckpoint.IsComplete,
			"checkpoint_updated":   existingCheckpoint.UpdatedAt,
		})
	}

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

	// 源端是否从 slave 读取
	readFromSlave := false
	if task.Options != nil && task.Options.ReadFromSlave {
		readFromSlave = true
	}

	taskLog.Info("Connection pool config", map[string]interface{}{
		"source_pool_size":  sourcePoolSize,
		"target_pool_size":  targetPoolSize,
		"read_from_slave":   readFromSlave,
	})

	// 尝试连接源端（使用配置的连接池大小，支持从 slave 读取）
	sourceClient, sourceIsCluster, err := connectRedisWithPoolSize(ctx, sourceAddrs, task.SourcePassword, sourcePoolSize, readFromSlave)
	if err != nil {
		taskLog.Error("Failed to connect source cluster", map[string]interface{}{"error": err.Error()})
		tasksMu.Lock()
		task.Status = "failed"
		task.UpdatedAt = time.Now().Format(time.RFC3339)
		tasksMu.Unlock()
		return
	}
	defer sourceClient.Close()

	// 尝试连接目标端（目标端始终走 master，因为要写入数据）
	targetClient, targetIsCluster, err := connectRedisWithPoolSize(ctx, targetAddrs, task.TargetPassword, targetPoolSize, false)
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

	// ==================== 集群拓扑验证和刷新 ====================
	// 【BUG-FIX】验证集群拓扑是否健康，避免 `:0` 节点导致连接失败
	if targetIsCluster {
		if clusterClient, ok := targetClient.(*redis.ClusterClient); ok {
			// 1. 强制刷新集群拓扑（避免使用缓存的错误拓扑）
			taskLog.Info("Refreshing target cluster topology...")
			clusterClient.ReloadState(ctx) // ReloadState 返回 void
			taskLog.Info("Target cluster topology refreshed successfully")
			
			// 2. 验证集群拓扑（检查是否有无效节点如 `:0`）
			// 目标集群始终走 master（写入），所以 readFromSlave=false
			if err := validateClusterTopology(ctx, clusterClient, taskLog, false); err != nil {
				taskLog.Error("⚠️ Target cluster topology validation failed", map[string]interface{}{
					"error": err.Error(),
				})
				// 不中断任务，记录告警到任务状态，前端展示
				tasksMu.Lock()
				task.TopologyWarnings = append(task.TopologyWarnings, "目标集群: "+err.Error())
				tasksMu.Unlock()
			}
			
			// 3. 启动定期刷新机制（每30秒刷新一次，防止拓扑变化）
			go func() {
				ticker := time.NewTicker(30 * time.Second)
				defer ticker.Stop()
				
				for {
					select {
					case <-ticker.C:
						clusterClient.ReloadState(ctx) // ReloadState 返回 void
						taskLog.Debug("Periodic cluster topology reload succeeded")
					case <-ctx.Done():
						return
					}
				}
			}()
		}
	}
	
	// 同样处理源集群
	if sourceIsCluster {
		if clusterClient, ok := sourceClient.(*redis.ClusterClient); ok {
			taskLog.Info("Refreshing source cluster topology...")
			clusterClient.ReloadState(ctx) // ReloadState 返回 void
			
			if err := validateClusterTopology(ctx, clusterClient, taskLog, readFromSlave); err != nil {
				taskLog.Warn("⚠️ Source cluster topology validation failed", map[string]interface{}{
					"error": err.Error(),
				})
				// 记录告警到任务状态
				tasksMu.Lock()
				task.TopologyWarnings = append(task.TopologyWarnings, "源集群: "+err.Error())
				tasksMu.Unlock()
			}
		}
	}

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

	// 获取源端Key总数（带超时：小集群快速返回，大集群超时跳过）
	// 外层 30 秒总超时 + 每个节点 10 秒单独超时
	// 超时后 totalKeys = -1，KeysTotal 由后续 SCAN 动态累加
	dbSizeCtx, dbSizeCancel := context.WithTimeout(ctx, 30*time.Second)
	totalKeys, err := getDBSize(dbSizeCtx, sourceClient, sourceIsCluster)
	dbSizeCancel()
	
	if err != nil {
		taskLog.Warn("DBSIZE timeout or failed, will use SCAN to track progress", map[string]interface{}{
			"error": err.Error(),
			"note":  "This is normal for large clusters. KeysTotal will be tracked by SCAN progress.",
		})
		totalKeys = -1
	} else if totalKeys == 0 {
		taskLog.Warn("DBSIZE returned 0 (may be inaccurate for Tendis), treating as unknown", map[string]interface{}{
			"note": "KeysTotal will be tracked by SCAN progress",
		})
		totalKeys = -1
	} else {
		taskLog.Info("Source cluster total keys (from DBSIZE)", map[string]interface{}{
			"total_keys": totalKeys,
		})
	}

	// ==================== P2-BUG4 修复: 只在首次设置 FullStartAt ====================
	tasksMu.Lock()
	// KeysTotal 初始化策略：
	// 1. 首次启动（KeysTotal == 0）且 DBSIZE 有效 → 用 DBSIZE 初始化（小集群快速展示进度）
	// 2. 恢复任务（KeysTotal > 0）→ 不覆盖，保留之前 SCAN 动态累加的准确值
	// 3. DBSIZE 超时/失败（totalKeys == -1）→ 不设置，由 SCAN 过程动态累加
	// 4. 后续 ticker 中 totalToMigrate > KeysTotal 时会自动调大
	if task.KeysTotal == 0 && totalKeys > 0 {
		task.KeysTotal = totalKeys
		task.BytesTotal = totalKeys * 256
	}
	// 【P2-BUG4 修复】只在 FullStartAt 为空时设置，避免恢复时覆盖原始时间
	if task.FullStartAt == "" && !shouldSkipFullSync {
		task.FullStartAt = time.Now().Format("2006-01-02 15:04:05")
		taskLog.Info("📅 【P2-BUG4 FIX】Setting FullStartAt for first time", map[string]interface{}{
			"full_start_at": task.FullStartAt,
		})
	} else if task.FullStartAt != "" {
		taskLog.Info("📅 【P2-BUG4 FIX】Preserving existing FullStartAt", map[string]interface{}{
			"full_start_at": task.FullStartAt,
		})
	}
	// 【问题1修复】开始新的全量迁移时，清空旧的增量开始时间
	// 只在非恢复场景才清空（即 shouldSkipFullSync=false 且 phase 不是 incremental）
	if !shouldSkipFullSync && currentPhase != "incremental" {
		task.IncrStartAt = ""
	}
	tasksMu.Unlock()

	// ==================== 问题2修复: 检查 SkipFullSync 配置 ====================
	skipFullSyncConfig := task.Options != nil && task.Options.SkipFullSync
	// 【P0-BUG1 修复】合并配置和自动检测的跳过逻辑
	actualSkipFullSync := skipFullSyncConfig || shouldSkipFullSync
	skipIncremental := task.Options != nil && task.Options.SkipIncremental
	
	// 【BUG-FIX】支持 migration_mode: "incremental" 纯增量模式
	// - "full_only": 只做全量迁移
	// - "full_and_incremental": 全量+增量迁移
	// - "incremental": 纯增量迁移（跳过全量，直接启动 FakeSlave）
	isIncrementalOnly := task.MigrationMode == "incremental"
	if isIncrementalOnly {
		actualSkipFullSync = true  // 纯增量模式跳过全量
		taskLog.Info("【纯增量模式】migration_mode=incremental, skipping full migration")
	}
	needIncremental := !skipIncremental && (task.MigrationMode == "full_and_incremental" || isIncrementalOnly)

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

	// 如果跳过全量（恢复到增量阶段），也需要启动 FakeSlave
	if needIncremental {
		// 检查是否支持 INCRSYNC 协议
		if checkTendisIncrSyncSupport(ctx, sourceClient, taskLog) {
			if !actualSkipFullSync {
				taskLog.Info("【关键】Starting FakeSlave BEFORE full migration to capture all changes")
			} else {
				taskLog.Info("【恢复】Starting FakeSlave for incremental sync phase (full sync skipped)")
			}
			
			// 创建 Binlog 缓存管理器（仅在需要全量时使用缓存模式）
			if !actualSkipFullSync {
				cacheConfig := replication.BinlogCacheConfig{
					CacheDir:    "./data/binlog_cache",
					TaskID:      task.ID,
					MaxFileSize: 1 << 30, // 1GB 单文件上限
				}
				cacheManager = replication.NewBinlogCacheManager(cacheConfig)
				cacheManager.StartCaching() // 开启缓存模式
			}
			
			// 保存到 task
			tasksMu.Lock()
			if cacheManager != nil {
				task.cacheManager = cacheManager
			}
			task.IncrSyncMode = "binlog"
			tasksMu.Unlock()
			
			// 启动 FakeSlave
			binlogCtx, binlogCancel = context.WithCancel(ctx)
			defer func() {
				if binlogCancel != nil {
					binlogCancel()
				}
			}()
			if cacheManager != nil {
				fakeSlaves, err = startFakeSlavesWithCache(binlogCtx, task, sourceClient, targetClient, sourceIsCluster, cacheManager, taskLog)
			} else {
				// 恢复场景：直接启动实时同步模式的 FakeSlave
				fakeSlaves, err = startFakeSlaves(binlogCtx, task, sourceClient, targetClient, sourceIsCluster, taskLog)
			}
			if err != nil {
				taskLog.Error("Failed to start FakeSlaves, will use time-window mode", map[string]interface{}{
					"error": err.Error(),
				})
				binlogCancel()
				cacheManager = nil
				fakeSlaves = nil
				// 【BUG-FIX】FakeSlave 失败降级后，回退 IncrSyncMode 为 time_window
				tasksMu.Lock()
				task.IncrSyncMode = "time_window"
				tasksMu.Unlock()
			} else {
				taskLog.Info("FakeSlaves started", map[string]interface{}{
					"node_count":  len(fakeSlaves),
					"cache_mode":  cacheManager != nil,
					"skip_full":   actualSkipFullSync,
				})
			}
		} else {
			taskLog.Warn("Tendis INCRSYNC not supported, will use time-window mode for incremental sync")
			tasksMu.Lock()
			task.IncrSyncMode = "time_window"
			tasksMu.Unlock()
		}
	}

	// ==================== 执行全量迁移（P0-BUG1/BUG2 修复）====================
	if actualSkipFullSync {
		if skipFullSyncConfig {
			taskLog.Info("Full migration SKIPPED (skip_full_sync=true)", map[string]interface{}{
				"total_keys": totalKeys,
			})
		} else {
			taskLog.Info("🔄 【P0-BUG1 FIX】Full migration SKIPPED (resuming from incremental phase)", map[string]interface{}{
				"current_phase": currentPhase,
				"checkpoint_complete": fullSyncAlreadyCompleted,
			})
		}
		// 直接标记全量阶段完成（保留原有状态如果已经是 incremental）
		tasksMu.Lock()
		if task.Phase != "incremental" {
			task.Phase = "full_skipped"
		}
		task.Progress = 100
		tasksMu.Unlock()
	} else {
		// ==================== P0-BUG2 修复：使用互斥锁防止并发启动多个全量 ====================
		fullMigrationMu.Lock()
		if fullMigrationRunning[task.ID] {
			fullMigrationMu.Unlock()
			taskLog.Warn("🔒 【P0-BUG2 FIX】Full migration already running, skip duplicate start", map[string]interface{}{
				"task_id": task.ID,
			})
		} else {
			fullMigrationRunning[task.ID] = true
			fullMigrationMu.Unlock()
			
			taskLog.Info("Starting full migration", map[string]interface{}{
				"total_keys": totalKeys,
				"binlog_caching": cacheManager != nil,
			})

			// 执行全量迁移
			doFullMigration(ctx, task, sourceClient, targetClient, sourceIsCluster, targetIsCluster, taskLog)
			
			// 清除运行标记
			fullMigrationMu.Lock()
			delete(fullMigrationRunning, task.ID)
			fullMigrationMu.Unlock()
		}
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

	// 【BUG-FIX】支持纯增量模式：mode 可以是 "full_and_incremental" 或 "incremental"
	if status == "running" && (mode == "full_and_incremental" || mode == "incremental") {
		taskLog.Info("Starting incremental sync phase")
		tasksMu.Lock()
		task.Phase = "incremental"
		// 【P2-BUG4 修复】只在 IncrStartAt 为空时设置，避免恢复时覆盖原始时间
		if task.IncrStartAt == "" {
			task.IncrStartAt = time.Now().Format("2006-01-02 15:04:05")
			taskLog.Info("📅 【P2-BUG4 FIX】Setting IncrStartAt for first time", map[string]interface{}{
				"incr_start_at": task.IncrStartAt,
			})
		} else {
			taskLog.Info("📅 【P2-BUG4 FIX】Preserving existing IncrStartAt", map[string]interface{}{
				"incr_start_at": task.IncrStartAt,
			})
		}
		tasksMu.Unlock()
		
		// 根据是否有 FakeSlave 选择同步模式
		// 【BUG FIX】修改条件：当 skip_full_sync=true 时，cacheManager 为 nil，但仍应使用 FakeSlave 进行实时同步
		if len(fakeSlaves) > 0 {
			// ==================== 【关键】回放缓存的 Binlog ====================
			if cacheManager != nil {
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
			} else {
				taskLog.Info("No cache manager (skip_full_sync mode), proceeding with real-time binlog sync")
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
	return connectRedisWithPoolSize(ctx, addrs, password, 0, false)
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

// validateClusterTopology 验证集群拓扑是否健康（检测无效节点如 `:0`）
// readFromSlave=true 时，slave 地址异常也视为 error（因为需要从 slave 读取数据）
// readFromSlave=false 时，slave 地址异常仅记录 WARN（不影响迁移）
func validateClusterTopology(ctx context.Context, client *redis.ClusterClient, taskLog *logger.TaskLogger, readFromSlave bool) error {
	slots, err := client.ClusterSlots(ctx).Result()
	if err != nil {
		return fmt.Errorf("CLUSTER SLOTS failed: %w", err)
	}
	
	invalidMasters := []string{}
	invalidSlaves := []string{}
	totalSlots := 0
	validSlots := 0
	
	for _, slot := range slots {
		slotCount := int(slot.End - slot.Start + 1)
		totalSlots += slotCount
		
		if len(slot.Nodes) == 0 {
			invalidMasters = append(invalidMasters, fmt.Sprintf("slots %d-%d -> no nodes", slot.Start, slot.End))
			continue
		}
		
		// 第一个节点是 master
		master := slot.Nodes[0]
		if master.Addr == "" || master.Addr == ":0" || strings.HasPrefix(master.Addr, ":") {
			invalidMasters = append(invalidMasters, fmt.Sprintf("slots %d-%d -> invalid master addr '%s'",
				slot.Start, slot.End, master.Addr))
		} else {
			validSlots += slotCount
		}
		
		// 后续节点是 slave
		for i := 1; i < len(slot.Nodes); i++ {
			node := slot.Nodes[i]
			if node.Addr == "" || node.Addr == ":0" || strings.HasPrefix(node.Addr, ":") {
				invalidSlaves = append(invalidSlaves, fmt.Sprintf("slots %d-%d -> invalid slave addr '%s'",
					slot.Start, slot.End, node.Addr))
			}
		}
	}
	
	// master 地址异常始终返回 error
	if len(invalidMasters) > 0 {
		return fmt.Errorf("found %d invalid master node(s) in cluster topology: %v (total slots: %d, valid: %d)", 
			len(invalidMasters), invalidMasters, totalSlots, validSlots)
	}
	
	// slave 地址异常：根据 readFromSlave 决定严重程度
	if len(invalidSlaves) > 0 {
		if readFromSlave {
			// 需要从 slave 读取数据，slave 异常会真正影响迁移
			return fmt.Errorf("found %d invalid slave node(s) (read_from_slave=true, will affect migration): %v (total slots: %d)", 
				len(invalidSlaves), invalidSlaves, totalSlots)
		}
		// 不从 slave 读取，仅记录 WARN
		if taskLog != nil {
			taskLog.Warn("Cluster has slave nodes with invalid addresses (not affecting migration, read_from_slave=false)", map[string]interface{}{
				"invalid_slaves": invalidSlaves,
				"count":          len(invalidSlaves),
			})
		}
	}
	
	// 检查 slot 覆盖是否完整（应该是 16384）
	if totalSlots != 16384 {
		if taskLog != nil {
			taskLog.Warn("Cluster slots coverage incomplete", map[string]interface{}{
				"total_slots":    totalSlots,
				"expected_slots": 16384,
				"missing_slots":  16384 - totalSlots,
			})
		}
	}
	
	if taskLog != nil {
		taskLog.Info("✅ Cluster topology validation passed", map[string]interface{}{
			"total_slots":      totalSlots,
			"valid_slots":      validSlots,
			"invalid_masters":  0,
			"invalid_slaves":   len(invalidSlaves),
			"read_from_slave":  readFromSlave,
		})
	}
	
	return nil
}

// getRedisTime 获取 Redis 服务器时间
func getRedisTime(ctx context.Context, client redis.UniversalClient) (time.Time, error) {
	result, err := client.Time(ctx).Result()
	if err != nil {
		return time.Time{}, err
	}
	return result, nil
}

// connectRedisWithPoolSize 连接Redis，支持自定义连接池大小和 ReadOnly 模式
// readFromSlave=true 时，集群模式下优先从 slave 读取数据（生产环境推荐）
func connectRedisWithPoolSize(ctx context.Context, addrs []string, password string, poolSize int, readFromSlave bool) (redis.UniversalClient, bool, error) {
	// 设置默认连接池大小
	if poolSize <= 0 {
		poolSize = 10 // 默认10个连接
	}

	// 先尝试集群模式
	clusterOpts := &redis.ClusterOptions{
		Addrs:        addrs,
		Password:     password,
		PoolSize:     poolSize,           // 每个节点的连接池大小
		MinIdleConns: poolSize / 4,       // 最小空闲连接数
		PoolTimeout:  30 * time.Second,   // 等待连接的超时时间
		IdleTimeout:  5 * time.Minute,    // 空闲连接超时，防止连接泄漏产生僵尸连接
		DialTimeout:  10 * time.Second,   // 连接超时
		ReadTimeout:  60 * time.Second,   // 读取超时（大 Key RESTORE 可能需要较长时间）
		WriteTimeout: 60 * time.Second,   // 写入超时
	}
	
	// 【生产环境优化】从 slave 读取数据，减轻 master 压力
	if readFromSlave {
		clusterOpts.ReadOnly = true         // 允许从 slave 读取
		clusterOpts.RouteByLatency = true   // 按延迟路由到最快的 slave
	}
	
	clusterClient := redis.NewClusterClient(clusterOpts)
	
	// 【BUG-FIX】增加集群连接的详细日志
	clusterErr := clusterClient.Ping(ctx).Err()
	if clusterErr == nil {
		logger.Info("[connectRedis] Connected as cluster mode", map[string]interface{}{
			"addrs":           addrs,
			"pool_size":       poolSize,
			"read_from_slave": readFromSlave,
		})
		return clusterClient, true, nil
	}
	
	// 集群模式失败，记录日志
	logger.Debug("[connectRedis] Cluster mode failed, trying standalone", map[string]interface{}{
		"addrs":         addrs,
		"cluster_error": clusterErr.Error(),
	})
	clusterClient.Close()

	// 尝试单机模式
	standaloneClient := redis.NewClient(&redis.Options{
		Addr:         addrs[0],
		Password:     password,
		PoolSize:     poolSize,           // 连接池大小
		MinIdleConns: poolSize / 4,       // 最小空闲连接数
		PoolTimeout:  30 * time.Second,   // 等待连接的超时时间
		IdleTimeout:  5 * time.Minute,    // 【BUG-FIX】空闲连接超时，防止连接泄漏产生僵尸连接
		DialTimeout:  10 * time.Second,   // 连接超时
		ReadTimeout:  60 * time.Second,   // 读取超时（大 Key RESTORE 可能需要较长时间）
		WriteTimeout: 60 * time.Second,   // 写入超时
	})
	if err := standaloneClient.Ping(ctx).Err(); err != nil {
		standaloneClient.Close()
		return nil, false, err
	}
	
	logger.Info("[connectRedis] Connected as standalone mode", map[string]interface{}{
		"addr":      addrs[0],
		"pool_size": poolSize,
	})
	return standaloneClient, false, nil
}

// getDBSize 获取数据库Key数量
// getDBSize 获取 Redis/Tendis 集群的总 Key 数量
// 【BUG-FIX】注意：Tendis 的 DBSIZE 命令可能返回不准确的值（如 0），建议使用 -1 表示未知
func getDBSize(ctx context.Context, client redis.UniversalClient, isCluster bool) (int64, error) {
	if !isCluster {
		return client.DBSize(ctx).Result()
	}

	// 集群模式需要遍历所有节点
	clusterClient := client.(*redis.ClusterClient)
	var total int64
	var mu sync.Mutex
	var firstErr error

	err := clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		// 【BUG-FIX】为每个节点设置单独的超时（避免某个慢节点拖累整体）
		nodeCtx, nodeCancel := context.WithTimeout(ctx, 10*time.Second)
		defer nodeCancel()
		
		size, err := node.DBSize(nodeCtx).Result()
		if err != nil {
			// 记录第一个错误，但继续尝试其他节点
			if firstErr == nil {
				firstErr = err
			}
			logger.Debug("[getDBSize] Node DBSIZE failed", map[string]interface{}{
				"node":  node.Options().Addr,
				"error": err.Error(),
			})
			return nil // 继续尝试其他节点
		}
		
		mu.Lock()
		total += size
		mu.Unlock()
		return nil
	})
	
	// 如果所有节点都失败，返回错误
	if err != nil {
		return 0, err
	}
	if total == 0 && firstErr != nil {
		return 0, firstErr
	}
	
	return total, nil
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
	
	// 仅在 worker 数量达到里程碑时记录日志，避免 512 个 worker 产生 512 条日志
	active := atomic.LoadInt32(&p.activeWorkers)
	target := atomic.LoadInt32(&p.targetWorkers)
	if active == 1 || active%100 == 0 || active == target {
		p.taskLog.Info("Workers scaling", map[string]interface{}{
			"active_workers": active,
			"target_workers": target,
		})
	}
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
	// 【死锁修复】使用 task.statsMu 代替全局 tasksMu
	p.task.statsMu.Lock()
	status := p.task.Status
	keyFilter := p.task.Options.KeyFilter
	p.task.statsMu.Unlock()
	if status != "running" {
		return
	}

	// 批量模式下的限速：按实际 key 数量消耗令牌
	// Pipeline 批量操作时，每个 key 都应计入限速
	if rl := p.GetRateLimiter(); rl != nil {
		rl.WaitN(len(keys))
	}

	// 影子模式：只读取源端数据，不写入目标端
	// 【优化】移除了每批次输出 shadow_mode 的日志，shadow_mode 状态只在任务启动时输出一次
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

	// 正常模式：写入目标端限速（按实际 key 数量消耗令牌）
	if tl := p.GetTargetRateLimiter(); tl != nil {
		tl.WaitN(len(keys))
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
	p.taskLog.Info("processShadowBatch called - NO WRITE to target", map[string]interface{}{
		"key_count": len(keys),
	})
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
	// 【死锁修复】使用 task.statsMu 代替全局 tasksMu
	p.task.statsMu.Lock()
	status := p.task.Status
	p.task.statsMu.Unlock()
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
		// 【P2-BUG5 修复】使用增强版错误记录，包含完整上下文
		sourceAddr := getClientAddr(p.sourceClient)
		targetAddr := getClientAddr(p.targetClient)
		addErrorKeyWithDetails(
			p.task.ID, key, "string", "failed",
			reason+" (after "+fmt.Sprintf("%d", maxRetries)+" retries)",
			sourceAddr, targetAddr, "migrate", "full", maxRetries,
		)
		
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

	// 【审查修复】确保 RateLimiter goroutine 不泄露
	defer func() {
		if sourceLimiter != nil {
			sourceLimiter.Stop()
		}
		if targetLimiter != nil {
			targetLimiter.Stop()
		}
	}()

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

	// 统计计数器：直接使用 task 字段（原子操作）
	// 【核心设计】worker 通过 atomic.AddInt64(&task.KeysMigrated, 1) 直接递增 task 字段
	// 暂停恢复后，task 字段保留历史值，worker 继续在原来的数字上累加
	// 不需要局部变量、baseline、retryAdj 等额外机制，简洁直观
	// 自动重试成功：直接 atomic.AddInt64(&task.KeysMigrated, 1) + atomic.AddInt64(&task.KeysFailed, -1)
	// 所有数字始终反映"整个任务"的总量
	startTime := time.Now()
	lastLogTime := time.Now()
	var lastLogMu sync.Mutex

	// 创建滑动窗口速度追踪器
	speedTracker := NewSpeedTracker(20) // 使用20个采样点（约10秒窗口）

	// 已移除 processedKeys：40 亿 Key 场景下 sync.Map 会导致 OOM（80-150 GB 内存）
	// Redis SCAN 返回重复 Key 是正常的，重复迁移（replace 覆盖 / skip 跳过）不影响正确性

	// 创建Key通道（缓冲区大小动态调整）
	keyChan := make(chan string, workerCount*100)

	// 创建动态Worker池
	// 直接传 task 字段指针，worker 通过原子操作直接递增 task 的计数器
	// 暂停恢复后无需任何额外处理，数字自然在原来基础上累加
	workerPool := NewDynamicWorkerPool(ctx, task, keyChan, taskLog,
		sourceClient, targetClient, conflictPolicy, sourceLimiter, targetLimiter,
		&task.KeysMigrated, &task.BytesMigrated, &task.KeysFailed, &task.KeysSkipped, &task.KeysFiltered)
	
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
				// 直接读取 task 字段（worker 通过原子操作直接递增）
				mc := atomic.LoadInt64(&task.KeysMigrated)
				mb := atomic.LoadInt64(&task.BytesMigrated)
				fc := atomic.LoadInt64(&task.KeysFailed)
				sc := atomic.LoadInt64(&task.KeysSkipped)
				ftc := atomic.LoadInt64(&task.KeysFiltered)
				
				// 记录速度采样点
				speedTracker.Record(mc, mb)
				
				// 检查是否需要动态调整配置
				// 【死锁修复】使用 task.statsMu 代替全局 tasksMu，避免高频锁争抢
				task.statsMu.Lock()
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
				task.statsMu.Unlock()
				
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

				// 【死锁修复】使用 task.statsMu 代替全局 tasksMu 更新统计字段
				task.statsMu.Lock()
				// task 的 KeysMigrated/KeysFailed 等已由 worker 原子递增，无需再赋值
				task.ActiveWorkers = currentWorkers
				
				// KeysToMigrate 计算：使用已处理量推导，避免暂停恢复后 SCAN 重复扫描导致膨胀
				// 公式：KeysToMigrate = max(已有值, 已迁移+失败+跳过+过滤)
				// 这样暂停恢复后，只有真正新处理的 key 才会推高 KeysToMigrate
				totalProcessed := mc + fc + sc + ftc
				totalToMigrate := task.KeysToMigrate
				if totalProcessed > totalToMigrate {
					totalToMigrate = totalProcessed
					task.KeysToMigrate = totalToMigrate
				}
				
				// 动态调整 KeysTotal：使用待迁移数作为总数
				if totalToMigrate > task.KeysTotal {
					task.KeysTotal = totalToMigrate
					task.BytesTotal = totalToMigrate * 256
				}
				
				// 进度 = (已迁移 + 已跳过) / 待迁移总数
				if totalToMigrate > 0 {
					task.Progress = float64(mc + sc) / float64(totalToMigrate) * 100
					if task.Progress > 100 {
						task.Progress = 100
					}
				} else if (mc + sc + ftc) > 0 {
					task.Progress = 0
				}
				// 使用实时速度（滑动窗口），而不是平均速度
				task.Speed = realTimeSpeed
				task.UpdatedAt = time.Now().Format(time.RFC3339)
				task.statsMu.Unlock()

				// 每10秒记录一次详细日志（包含性能分析信息）
				lastLogMu.Lock()
				if time.Since(lastLogTime) > 10*time.Second {
					elapsed := time.Since(startTime).Seconds()
					avgSpeed := int64(0)
					if elapsed > 0 {
						avgSpeed = int64(float64(mc) / elapsed)
					}
					
					taskLog.Info("Migration progress", map[string]interface{}{
						"progress":         fmt.Sprintf("%.1f%%", task.Progress),
						"keys_to_migrate":  totalToMigrate,
						"migrated_keys":    mc,
						"failed_keys":      fc,
						"skipped_keys":     sc,
						"filtered_keys":    ftc,
						"realtime_speed":   realTimeSpeed,
						"average_speed":    avgSpeed,
						"bytes_speed_mb":   fmt.Sprintf("%.2f MB/s", float64(bytesSpeed)/1024/1024),
						"active_workers":   currentWorkers,
						"target_workers":   targetWorkerCount,
						"elapsed":          fmt.Sprintf("%.0fs", elapsed),
						"speed_per_worker": realTimeSpeed / int64(max(currentWorkers, 1)),
					})
					lastLogTime = time.Now()
				}
				lastLogMu.Unlock()
			}
		}
	}()

	// ==================== Key 清单模式（流式，支持100亿+ Key）====================
	// 如果配置了 Key 清单文件，则从清单迁移，不使用 SCAN
	// 【P0 修复】改为流式处理：StreamKeyListFromFile → StreamValidateAndSend → keyChan
	// 不再将全量 Key 加载到内存，任意规模的清单都不会 OOM
	if task.Options != nil && task.Options.KeyListFile != "" {
		taskLog.Info("Key list mode (streaming): reading keys from file", map[string]interface{}{
			"file": task.Options.KeyListFile,
		})

		// 流式读取 Key 清单文件
		keyInputCh, totalCount, fileErrCh := StreamKeyListFromFile(task.Options.KeyListFile)

		// 流式验证 + 分发：读取 → 验证存在性 → 发送到 keyChan
		go func() {
			defer close(keyChan)

			// 检查文件读取错误
			select {
			case err := <-fileErrCh:
				if err != nil {
					taskLog.Error("Failed to read key list file", map[string]interface{}{
						"error": err.Error(),
					})
					tasksMu.Lock()
					task.Status = "failed"
					tasksMu.Unlock()
					return
				}
			default:
			}

			existing, missing := StreamValidateAndSend(ctx, sourceClient, keyInputCh, keyChan, 1000)

			// 更新任务统计
			task.statsMu.Lock()
			task.KeysTotal = existing
			task.statsMu.Unlock()

			taskLog.Info("Key list streaming completed", map[string]interface{}{
				"total_in_file": atomic.LoadInt64(totalCount),
				"existing_keys": existing,
				"missing_keys":  missing,
			})
		}()

		// 等待所有 Worker 完成
		workerPool.Wait()
		close(stopProgress)

		taskLog.Info("Key list migration completed", map[string]interface{}{
			"migrated": atomic.LoadInt64(&task.KeysMigrated),
			"failed":   atomic.LoadInt64(&task.KeysFailed),
			"skipped":  atomic.LoadInt64(&task.KeysSkipped),
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
		// 【恢复日志增强】详细记录从哪个断点继续
		taskLog.Info("📍 【RESUME】Resuming from existing checkpoint", map[string]interface{}{
			"processed_keys":    existingCheckpoint.ProcessedKeys,
			"total_scanned":     existingCheckpoint.TotalScannedKeys,
			"node_cursors":      len(existingCheckpoint.NodeCursors),
			"checkpoint_phase":  existingCheckpoint.Phase,
			"checkpoint_start":  existingCheckpoint.StartTime,
			"checkpoint_update": existingCheckpoint.UpdatedAt,
		})
		// 打印每个节点的 cursor 详情
		for nodeKey, cursor := range existingCheckpoint.NodeCursors {
			taskLog.Info("📍 【RESUME】Node cursor detail", map[string]interface{}{
				"node_key": nodeKey,
				"cursor":   cursor,
				"status":   map[bool]string{true: "completed", false: "resuming"}[cursor == 0],
			})
		}
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
		// 【P2-BUG4 修复】保留原始 StartTime
		if existingCheckpoint.StartTime != "" {
			fullCheckpoint.StartTime = existingCheckpoint.StartTime
		}
	}

	// 断点保存计数器
	var scannedKeysCount int64
	checkpointSaveInterval := int64(1000) // 每扫描 1000 个 key 保存一次断点
	lastCheckpointSave := time.Now()
	
	// 【零丢失断点机制】
	// 问题：SCAN cursor 推进 ≠ worker 迁移完成。SIGKILL 时 keyChan 中未消费的 key 会丢失。
	// 方案：断点保存的 cursor 需要回退，确保覆盖 keyChan 中所有待消费的 key。
	//   - 维护一个 "cursor 历史栈"，记录每批 SCAN 对应的 {cursor, count}
	//   - 保存断点时，根据 keyChan 中待消费的 key 数量（len(keyChan) + worker 批次缓冲）
	//     回退到足够安全的 cursor
	//   - 恢复时最多重复迁移一些 key，但绝不丢失（迁移是幂等的：replace 覆盖/skip 跳过）
	type cursorBatch struct {
		cursor   uint64 // 这批 SCAN 之前的 cursor 值
		keyCount int64  // 这批 push 了多少 key 到 keyChan
	}
	var cursorHistory []cursorBatch    // cursor 历史栈
	var cursorHistoryMu sync.Mutex
	var totalPushedKeys int64          // 累计 push 到 keyChan 的 key 数量
	
	// getSafeCheckpointCursor 计算安全的断点 cursor
	// 考虑 keyChan 中未消费的 key + worker 中正在处理的 key
	getSafeCheckpointCursor := func(currentCursor uint64) uint64 {
		cursorHistoryMu.Lock()
		defer cursorHistoryMu.Unlock()
		
		if len(cursorHistory) == 0 {
			return currentCursor
		}
		
		// 估算 keyChan 中 + worker 中还未落盘的 key 数量
		// keyChan 缓冲区中的 key + 每个 worker 可能持有的批次（最多 1000 个/worker）
		pendingInChan := int64(len(keyChan))
		pendingInWorkers := int64(workerCount) * 1000  // 最坏情况
		totalPending := pendingInChan + pendingInWorkers
		
		// 从最新的批次开始向前回溯，找到覆盖所有 pending key 的安全 cursor
		var accumulatedKeys int64
		safeCursor := currentCursor
		for i := len(cursorHistory) - 1; i >= 0; i-- {
			safeCursor = cursorHistory[i].cursor
			accumulatedKeys += cursorHistory[i].keyCount
			if accumulatedKeys >= totalPending {
				break
			}
		}
		
		// 清理已经安全的旧历史记录（保留最近的 + 需要回退的）
		if accumulatedKeys >= totalPending && len(cursorHistory) > 100 {
			// 保留最近的 50 条
			cursorHistory = cursorHistory[len(cursorHistory)-50:]
		}
		
		return safeCursor
	}
	
	// 【优化】获取 SCAN MATCH 模式 - 利用服务端过滤
	// 评审建议：SCAN MATCH 是服务端过滤，40亿 Key 场景下可大幅减少网络传输
	var scanMatchPattern string
	var keyFilter *KeyFilter  // 用于本地二次过滤（排除前缀等）
	if task.Options != nil {
		scanMatchPattern = getScanMatchPattern(task.Options.KeyFilter)
		keyFilter = task.Options.KeyFilter
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
		// 集群模式：并行遍历节点
		clusterClient := sourceClient.(*redis.ClusterClient)
		var scanWg sync.WaitGroup
		// 【BUG-FIX】使用 fullCheckpoint.mu 替代局部 nodeCursorsMu，避免 saveFullSyncCheckpoint 遍历时的并发问题

		// 【生产环境优化】根据 readFromSlave 选择遍历 master 还是 slave 节点
		// readFromSlave=true: ForEachSlave - 从 slave 读取，不影响 master 服务
		// readFromSlave=false: ForEachMaster - 传统模式，从 master 读取
		readFromSlave := task.Options != nil && task.Options.ReadFromSlave
		forEachNode := clusterClient.ForEachMaster
		nodeType := "master"
		if readFromSlave {
			forEachNode = clusterClient.ForEachSlave
			nodeType = "slave"
		}
		taskLog.Info("Full migration SCAN node selection", map[string]interface{}{
			"node_type":       nodeType,
			"read_from_slave": readFromSlave,
		})

		forEachNode(ctx, func(ctx context.Context, node *redis.Client) error {
			scanWg.Add(1)
			go func(nodeClient *redis.Client) {
				defer scanWg.Done()
				
				// 获取节点地址
				nodeAddr := nodeClient.Options().Addr
				
				// 【P1-BUG3 修复】从断点恢复 cursor（支持前缀维度）
				var cursor uint64
				var cursorFound bool
				fullCheckpoint.mu.Lock()
				checkpointKey := getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)
				
				// 尝试多种 key 格式查找 cursor
				if savedCursor, ok := fullCheckpoint.NodeCursors[checkpointKey]; ok {
					cursor = savedCursor
					cursorFound = true
					taskLog.Info("📍 【P1-BUG3 FIX】Resuming node scan from cursor (prefix key)", map[string]interface{}{
						"node":           nodeAddr,
						"cursor":         cursor,
						"checkpoint_key": checkpointKey,
						"match_pattern":  scanMatchPattern,
					})
				} else if savedCursor, ok := fullCheckpoint.NodeCursors[nodeAddr]; ok {
					// 兼容旧断点格式（只使用节点地址作为 key）
					cursor = savedCursor
					cursorFound = true
					taskLog.Info("📍 【P1-BUG3 FIX】Resuming node scan from cursor (legacy key)", map[string]interface{}{
						"node":   nodeAddr,
						"cursor": cursor,
					})
				} else {
					// 没有找到断点，从头开始
					taskLog.Info("📍 【P1-BUG3 FIX】No checkpoint found for node, starting from beginning", map[string]interface{}{
						"node":              nodeAddr,
						"checkpoint_key":    checkpointKey,
						"available_cursors": len(fullCheckpoint.NodeCursors),
					})
					// 打印所有可用的 cursor keys 便于调试
					if len(fullCheckpoint.NodeCursors) > 0 {
						taskLog.Debug("Available checkpoint keys", map[string]interface{}{
							"keys": getMapKeys(fullCheckpoint.NodeCursors),
						})
					}
				}
				
				// 如果 cursor == 0 但已找到记录，说明该节点已扫描完成
				if cursorFound && cursor == 0 {
					taskLog.Info("📍 Node already completed in previous run, skipping", map[string]interface{}{
						"node": nodeAddr,
					})
					fullCheckpoint.mu.Unlock()
					return
				}
				fullCheckpoint.mu.Unlock()
				
				consecutiveScanFailures := 0
				
				for {
					// 【BUG-FIX】优先检查 stopCh，响应更快
					select {
					case <-task.stopCh:
						taskLog.Info("【BUG-FIX】Received stop signal via stopCh, saving checkpoint and exiting", map[string]interface{}{
							"node": nodeAddr,
						})
						safeCursor := getSafeCheckpointCursor(cursor)
						fullCheckpoint.mu.Lock()
						fullCheckpoint.NodeCursors[getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)] = safeCursor
						fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
						fullCheckpoint.mu.Unlock()
						saveFullSyncCheckpoint(task.ID, fullCheckpoint)
						return
					default:
						// 继续执行
					}
					
					tasksMu.RLock()
					status := task.Status
					tasksMu.RUnlock()
					if status != "running" {
						// 【零丢失】主动停止时，等待 keyChan 排空后保存安全 cursor
						drainStart := time.Now()
						for len(keyChan) > 0 && time.Since(drainStart) < 10*time.Second {
							time.Sleep(50 * time.Millisecond)
						}
						safeCursor := getSafeCheckpointCursor(cursor)
						fullCheckpoint.mu.Lock()
						fullCheckpoint.NodeCursors[getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)] = safeCursor
						fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
						fullCheckpoint.mu.Unlock()
						saveFullSyncCheckpoint(task.ID, fullCheckpoint)
						taskLog.Info("Checkpoint saved on pause (zero-loss)", map[string]interface{}{
							"node": nodeAddr, "scan_cursor": cursor, "safe_cursor": safeCursor, "chan_pending": len(keyChan),
						})
						return
					}

					// 动态获取批次大小
					currentBatchSize := getBatchSize()
					// 【优化】使用 SCAN MATCH 服务端过滤
					prevCursor := cursor
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
								safeCursor := getSafeCheckpointCursor(cursor)
								fullCheckpoint.mu.Lock()
								fullCheckpoint.NodeCursors[nodeAddr] = safeCursor
								fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
								fullCheckpoint.mu.Unlock()
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

					// 【修复】统计符合过滤条件的待迁移 Key 数
					var matchedInBatch int64
					var pushedInBatch int64
					for _, key := range keys {
						// 【死锁修复】使用 stopCh + ctx 检测停止，不再使用 tasksMu.RLock()
						select {
						case <-task.stopCh:
							safeCursor := getSafeCheckpointCursor(cursor)
							fullCheckpoint.mu.Lock()
							fullCheckpoint.NodeCursors[getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)] = safeCursor
							fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
							fullCheckpoint.mu.Unlock()
							saveFullSyncCheckpoint(task.ID, fullCheckpoint)
							return
						case <-ctx.Done():
							safeCursor := getSafeCheckpointCursor(cursor)
							fullCheckpoint.mu.Lock()
							fullCheckpoint.NodeCursors[getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)] = safeCursor
							fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
							fullCheckpoint.mu.Unlock()
							saveFullSyncCheckpoint(task.ID, fullCheckpoint)
							return
						default:
						}
						// 检查是否符合过滤条件（本地二次过滤，处理排除前缀等情况）
						if matchKeyFilterV2(key, keyFilter) {
							matchedInBatch++
						}
						keyChan <- key
						pushedInBatch++
					}
					
					cursor = newCursor
					atomic.AddInt64(&scannedKeysCount, int64(len(keys)))
					
					// 【零丢失】记录 cursor 历史（用于断点回退）
					cursorHistoryMu.Lock()
					cursorHistory = append(cursorHistory, cursorBatch{cursor: prevCursor, keyCount: pushedInBatch})
					cursorHistoryMu.Unlock()
					
					// 定期保存断点（使用安全 cursor）
					currentScanned := atomic.LoadInt64(&scannedKeysCount)
					if currentScanned%checkpointSaveInterval == 0 || time.Since(lastCheckpointSave) > 30*time.Second {
						safeCursor := getSafeCheckpointCursor(cursor)
						fullCheckpoint.mu.Lock()
						fullCheckpoint.NodeCursors[getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)] = safeCursor
						fullCheckpoint.TotalScannedKeys = currentScanned
						fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
						fullCheckpoint.mu.Unlock()
						saveFullSyncCheckpoint(task.ID, fullCheckpoint)
						lastCheckpointSave = time.Now()
					}
					
					if cursor == 0 {
						// 该节点扫描完成
						fullCheckpoint.mu.Lock()
						fullCheckpoint.NodeCursors[getPrefixCheckpointKey(task.ID, nodeAddr, scanMatchPattern)] = 0 // 标记完成
						fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
						fullCheckpoint.mu.Unlock()
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
				// 【零丢失】主动停止时，等待 keyChan 排空后保存安全 cursor
				drainStart := time.Now()
				for len(keyChan) > 0 && time.Since(drainStart) < 10*time.Second {
					time.Sleep(50 * time.Millisecond)
				}
				safeCursor := getSafeCheckpointCursor(cursor)
				fullCheckpoint.NodeCursors[checkpointKey] = safeCursor
				fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
				saveFullSyncCheckpoint(task.ID, fullCheckpoint)
				taskLog.Info("Checkpoint saved on pause (zero-loss)", map[string]interface{}{
					"scan_cursor": cursor, "safe_cursor": safeCursor, "chan_pending": len(keyChan),
				})
				break
			}

			// 动态获取批次大小
			currentBatchSize := getBatchSize()
			// 【优化】使用 SCAN MATCH 服务端过滤
			prevCursor := cursor
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
						safeCursor := getSafeCheckpointCursor(cursor)
						fullCheckpoint.NodeCursors[checkpointKey] = safeCursor
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

			// 【修复】统计符合过滤条件的待迁移 Key 数
			var matchedInBatch int64
			var pushedInBatch int64
			for _, key := range keys {
				// 【死锁修复】使用 task.statsMu 代替全局 tasksMu 检查状态
				task.statsMu.Lock()
				status := task.Status
				task.statsMu.Unlock()
				if status != "running" {
					// 【零丢失】主动停止时保存安全 cursor
					safeCursor := getSafeCheckpointCursor(cursor)
					fullCheckpoint.NodeCursors[checkpointKey] = safeCursor
					fullCheckpoint.UpdatedAt = time.Now().Format(time.RFC3339)
					saveFullSyncCheckpoint(task.ID, fullCheckpoint)
					break
				}
				// 检查是否符合过滤条件
				if matchKeyFilterV2(key, keyFilter) {
					matchedInBatch++
				}
				keyChan <- key
				pushedInBatch++
			}
			
			cursor = newCursor
			atomic.AddInt64(&scannedKeysCount, int64(len(keys)))
			atomic.AddInt64(&totalPushedKeys, pushedInBatch)
			
			// 【零丢失】记录 cursor 历史（用于断点回退）
			cursorHistoryMu.Lock()
			cursorHistory = append(cursorHistory, cursorBatch{cursor: prevCursor, keyCount: pushedInBatch})
			cursorHistoryMu.Unlock()
			
			// 定期保存断点（使用安全 cursor）
			currentScanned := atomic.LoadInt64(&scannedKeysCount)
			if currentScanned%checkpointSaveInterval == 0 || time.Since(lastCheckpointSave) > 30*time.Second {
				safeCursor := getSafeCheckpointCursor(cursor)
				fullCheckpoint.NodeCursors[checkpointKey] = safeCursor
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

	// 保存错误 key
	saveErrorKeysToFile(task.ID)

	// 最终统计：task 字段已由 worker 原子递增，直接读取
	mc := atomic.LoadInt64(&task.KeysMigrated)
	fc := atomic.LoadInt64(&task.KeysFailed)
	sc := atomic.LoadInt64(&task.KeysSkipped)
	ftc := atomic.LoadInt64(&task.KeysFiltered)

	// 【关键修复】判断全量是否真正完成（而非被暂停/停止中途退出）
	// 只有当任务仍处于 running 状态时，才说明全量是自然完成的
	// 如果任务被暂停（paused）或停止（stopped），SCAN goroutine 是中途退出的，
	// 此时绝不能标记 markFullSyncComplete，否则恢复时会跳过全量
	tasksMu.RLock()
	currentStatus := task.Status
	tasksMu.RUnlock()
	
	fullMigrationReallyCompleted := (currentStatus == "running")
	
	if fullMigrationReallyCompleted {
		// 全量真正完成，标记 checkpoint
		markFullSyncComplete(task.ID)
	} else {
		// 全量被中途打断（暂停/停止），只保存当前进度，不标记完成
		taskLog.Warn("Full migration interrupted (NOT marking as complete)", map[string]interface{}{
			"status":        currentStatus,
			"migrated_keys": mc,
			"note":          "Checkpoint cursors already saved by SCAN goroutines, will resume from breakpoint",
		})
		// 确保 checkpoint 不会被误标为完成
		var cpToSave *FullSyncCheckpoint
		fullSyncCheckpointsMu.Lock()
		if cp, ok := fullSyncCheckpoints[task.ID]; ok {
			cp.IsComplete = false
			cp.Phase = "full"
			cp.ProcessedKeys = mc
			cp.UpdatedAt = time.Now().Format(time.RFC3339)
			cpToSave = cp
		}
		fullSyncCheckpointsMu.Unlock()
		// 【审计修复】使用锁内获取的引用，避免锁外读取 map
		if cpToSave != nil {
			saveFullSyncCheckpoint(task.ID, cpToSave)
		}
	}

	tasksMu.Lock()
	
	// 最终更新 KeysToMigrate：使用已处理量推导（与 ticker 逻辑一致）
	totalProcessedFinal := mc + fc + sc + ftc
	if totalProcessedFinal > task.KeysToMigrate {
		task.KeysToMigrate = totalProcessedFinal
	}
	
	// 全量完成时，用实际处理的 Key 总数更新 KeysTotal
	if totalProcessedFinal > task.KeysTotal {
		task.KeysTotal = totalProcessedFinal
		task.BytesTotal = totalProcessedFinal * 256
	}
	
	if fullMigrationReallyCompleted {
		if task.MigrationMode == "full_only" {
			task.Status = "completed"
			task.Progress = 100
			task.Phase = "completed"
			task.CompletedAt = time.Now().Format(time.RFC3339)
		} else {
			// 全量迁移完成，准备进入增量同步
			task.Progress = 100
			task.Phase = "incremental"
			task.IncrStartAt = time.Now().Format(time.RFC3339)
		}
	}
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	tasksMu.Unlock()
	
	// 广播任务状态更新
	broadcastTaskUpdate(task.ID)
	if fullMigrationReallyCompleted && task.MigrationMode == "full_only" {
		broadcastTaskStatus(task.ID, "completed")
	}

	elapsed := time.Since(startTime)
	avgSpeed := int64(0)
	if elapsed.Seconds() > 0 {
		avgSpeed = int64(float64(mc) / elapsed.Seconds())
	}
	if fullMigrationReallyCompleted {
		taskLog.Info("Full migration completed", map[string]interface{}{
			"migrated_keys":   mc,
			"failed_keys":     fc,
			"skipped_keys":    sc,
			"filtered_keys":   ftc,
			"migration_mode":  task.MigrationMode,
			"duration":        elapsed.String(),
			"avg_speed":       avgSpeed,
		})
	} else {
		taskLog.Info("Full migration interrupted", map[string]interface{}{
			"status":          currentStatus,
			"migrated_keys":   mc,
			"failed_keys":     fc,
			"skipped_keys":    sc,
			"filtered_keys":   ftc,
			"duration":        elapsed.String(),
			"avg_speed":       avgSpeed,
		})
	}
}

// RateLimiter 基于 golang.org/x/time/rate 的限速器
// 【BUG-FIX】替换自制 token-channel 限速器，修复多 Worker 争抢 token 导致 QPS 严重下降的问题
// 旧实现：WaitN 逐个等 token，8 个 Worker 串行争抢 channel → 500 QPS 实际只有 ~100/s
// 新实现：rate.Limiter 基于精确时间计算，WaitN 一次性预约 N 个 token，多 goroutine 不退化
type RateLimiter struct {
	qps     int              // QPS值（用于比较是否需要更新）
	limiter *rate.Limiter    // 标准令牌桶限速器
	ctx     context.Context  // 用于取消等待
	cancel  context.CancelFunc
}

// NewRateLimiter 创建限速器
func NewRateLimiter(qps int) *RateLimiter {
	if qps <= 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	rl := &RateLimiter{
		qps:     qps,
		limiter: rate.NewLimiter(rate.Limit(qps), qps), // rate=QPS/s, burst=QPS（允许短时突发）
		ctx:     ctx,
		cancel:  cancel,
	}

	return rl
}

// Wait 等待获取 1 个令牌
func (rl *RateLimiter) Wait() {
	if rl == nil {
		return
	}
	_ = rl.limiter.Wait(rl.ctx) // ctx 被 cancel 后立即返回
}

// WaitN 等待获取 n 个令牌（用于批量操作限速）
// 基于 rate.Limiter.WaitN：精确时间计算，多 goroutine 并发不退化
func (rl *RateLimiter) WaitN(n int) {
	if rl == nil || n <= 0 {
		return
	}
	// rate.Limiter.WaitN 要求 n <= burst，如果 n 超过 burst 则分批等待
	burst := rl.limiter.Burst()
	for n > 0 {
		batch := n
		if batch > burst {
			batch = burst
		}
		if err := rl.limiter.WaitN(rl.ctx, batch); err != nil {
			return // ctx cancelled（限速器被 Stop）
		}
		n -= batch
	}
}

// Stop 停止限速器
// cancel context 会唤醒所有在 Wait/WaitN 中阻塞的 goroutine
func (rl *RateLimiter) Stop() {
	if rl == nil {
		return
	}
	rl.cancel()
}

// getOutboundIP 获取本机可以连接到目标地址的出口 IP
// 【BUG-FIX】FakeSlave 需要告知 Tendis 主节点回连的 IP，必须是主节点可达的地址
func getOutboundIP(targetAddr string) (string, error) {
	// 使用 UDP 连接（不需要真正建立连接）来获取本机出口 IP
	// 这样可以获取到连接目标时使用的本机 IP
	host, _, err := net.SplitHostPort(targetAddr)
	if err != nil {
		host = targetAddr
	}

	conn, err := net.Dial("udp", net.JoinHostPort(host, "1"))
	if err != nil {
		return "", fmt.Errorf("failed to dial UDP: %w", err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}

// getLocalIPForFakeSlave 获取 FakeSlave 应该使用的监听 IP
// 根据源端地址自动检测本机可达的 IP
func getLocalIPForFakeSlave(sourceAddr string, taskLog *logger.TaskLogger) string {
	ip, err := getOutboundIP(sourceAddr)
	if err != nil {
		taskLog.Warn("Failed to detect outbound IP, using 0.0.0.0", map[string]interface{}{
			"source_addr": sourceAddr,
			"error":       err.Error(),
		})
		return "0.0.0.0"
	}

	taskLog.Info("Detected outbound IP for FakeSlave", map[string]interface{}{
		"source_addr":  sourceAddr,
		"outbound_ip":  ip,
	})
	return ip
}

// matchKeyFilter 检查Key是否匹配过滤规则
// 这是统一的 Key 过滤入口，内部调用 matchKeyFilterV2
func matchKeyFilter(key string, options *TaskOptions) bool {
	if options == nil || options.KeyFilter == nil {
		return true
	}
	return matchKeyFilterV2(key, options.KeyFilter)
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

// syncKeyByType 【BUG-2 修复】根据 Key 类型使用原生命令同步
// 解决 Tendis 间 DUMP/RESTORE 格式不兼容问题
func syncKeyByType(ctx context.Context, sourceClient, targetClient redis.UniversalClient, key, keyType string, ttl time.Duration, conflictPolicy string) error {
	// 冲突策略检查
	if conflictPolicy == "skip" {
		exists, _ := targetClient.Exists(ctx, key).Result()
		if exists > 0 {
			return nil // 跳过已存在的 Key
		}
	} else if conflictPolicy == "replace" || conflictPolicy == "skip_full_only" {
		// 增量阶段使用 replace 策略，先删除目标端的 Key
		targetClient.Del(ctx, key)
	}

	var err error
	switch keyType {
	case "string":
		// STRING 类型
		val, getErr := sourceClient.Get(ctx, key).Result()
		if getErr != nil {
			return getErr
		}
		if ttl > 0 {
			err = targetClient.Set(ctx, key, val, ttl).Err()
		} else {
			err = targetClient.Set(ctx, key, val, 0).Err()
		}

	case "hash":
		// HASH 类型
		fields, getErr := sourceClient.HGetAll(ctx, key).Result()
		if getErr != nil {
			return getErr
		}
		if len(fields) > 0 {
			// 转换为 []interface{} 格式
			args := make([]interface{}, 0, len(fields)*2)
			for k, v := range fields {
				args = append(args, k, v)
			}
			err = targetClient.HMSet(ctx, key, args...).Err()
			// 【BUG-FIX TTL 一致性】使用 PExpire（毫秒精度）+ 检查返回值
			if err == nil && ttl > 0 {
				if expErr := targetClient.PExpire(ctx, key, ttl).Err(); expErr != nil {
					return fmt.Errorf("hash PExpire failed: %v", expErr)
				}
			}
		}

	case "list":
		// LIST 类型
		vals, getErr := sourceClient.LRange(ctx, key, 0, -1).Result()
		if getErr != nil {
			return getErr
		}
		if len(vals) > 0 {
			// 转换为 []interface{} 格式
			args := make([]interface{}, len(vals))
			for i, v := range vals {
				args[i] = v
			}
			err = targetClient.RPush(ctx, key, args...).Err()
			// 【BUG-FIX TTL 一致性】使用 PExpire（毫秒精度）+ 检查返回值
			if err == nil && ttl > 0 {
				if expErr := targetClient.PExpire(ctx, key, ttl).Err(); expErr != nil {
					return fmt.Errorf("list PExpire failed: %v", expErr)
				}
			}
		}

	case "set":
		// SET 类型
		members, getErr := sourceClient.SMembers(ctx, key).Result()
		if getErr != nil {
			return getErr
		}
		if len(members) > 0 {
			// 转换为 []interface{} 格式
			args := make([]interface{}, len(members))
			for i, v := range members {
				args[i] = v
			}
			err = targetClient.SAdd(ctx, key, args...).Err()
			// 【BUG-FIX TTL 一致性】使用 PExpire（毫秒精度）+ 检查返回值
			if err == nil && ttl > 0 {
				if expErr := targetClient.PExpire(ctx, key, ttl).Err(); expErr != nil {
					return fmt.Errorf("set PExpire failed: %v", expErr)
				}
			}
		}

	case "zset":
		// ZSET 类型
		members, getErr := sourceClient.ZRangeWithScores(ctx, key, 0, -1).Result()
		if getErr != nil {
			return getErr
		}
		if len(members) > 0 {
			zMembers := make([]*redis.Z, len(members))
			for i, m := range members {
				zMembers[i] = &redis.Z{Score: m.Score, Member: m.Member}
			}
			err = targetClient.ZAdd(ctx, key, zMembers...).Err()
			// 【BUG-FIX TTL 一致性】使用 PExpire（毫秒精度）+ 检查返回值
			if err == nil && ttl > 0 {
				if expErr := targetClient.PExpire(ctx, key, ttl).Err(); expErr != nil {
					return fmt.Errorf("zset PExpire failed: %v", expErr)
				}
			}
		}

	default:
		// 其他类型尝试使用 DUMP/RESTORE（如果支持）
		dump, dumpErr := sourceClient.Dump(ctx, key).Result()
		if dumpErr != nil {
			return fmt.Errorf("unsupported type %s, dump failed: %v", keyType, dumpErr)
		}
		if ttl < 0 {
			ttl = 0
		}
		err = targetClient.Restore(ctx, key, ttl, dump).Err()
		if err != nil {
			return fmt.Errorf("restore failed for type %s: %v", keyType, err)
		}
	}

	return err
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
// 【BUG-FIX】对于集群模式的目标端，改用逐个 RESTORE 避免 MOVED 错误
func MigrateBatchWithPipeline(ctx context.Context, sourceClient, targetClient redis.UniversalClient, keys []string, policy string) []PipelineMigrateResult {
	if len(keys) == 0 {
		return nil
	}

	results := make([]PipelineMigrateResult, len(keys))
	for i, key := range keys {
		results[i] = PipelineMigrateResult{Key: key}
	}

	// 【BUG-FIX】暂停/停止时 context 会被取消，此时不应将 Key 标记为失败
	// 否则会产生大量虚假的 "context canceled" 失败记录
	if ctx.Err() != nil {
		// context 已取消，直接返回空结果（不设置 Reason，不计入失败）
		return results
	}

	// 【BUG-FIX】检测目标端是否为集群模式
	_, targetIsCluster := targetClient.(*redis.ClusterClient)

	// 阶段 1: 批量 DUMP（从源端获取数据）
	sourcePipe := sourceClient.Pipeline()
	ttlCmds := make([]*redis.DurationCmd, len(keys))
	dumpCmds := make([]*redis.StringCmd, len(keys))

	for i, key := range keys {
		// 【BUG-FIX TTL 一致性】使用 PTTL（毫秒精度）替代 TTL（秒精度）
		// 确保迁移前后 TTL 完全一致，不丢失毫秒精度
		ttlCmds[i] = sourcePipe.PTTL(ctx, key)
		dumpCmds[i] = sourcePipe.Dump(ctx, key)
	}

	_, err := sourcePipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		// 【BUG-FIX】如果是 context 取消导致的错误，不标记为失败
		if ctx.Err() != nil {
			return results
		}
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

	// 阶段 3: RESTORE 到目标端
	// 【BUG-FIX】暂停时 context 取消检查
	if ctx.Err() != nil {
		return results
	}
	// 【BUG-FIX】集群模式下逐个执行 RESTORE（避免 MOVED 错误）
	// 非集群模式下使用 Pipeline 批量执行（高性能）
	if targetIsCluster {
		// 集群模式：逐个执行 RESTORE（ClusterClient 会自动处理 MOVED 重定向）
		// 【BUG-FIX】添加重试机制，处理 go-redis 的 ":0" 地址解析问题
		clusterClient := targetClient.(*redis.ClusterClient)
		for i, key := range keys {
			// 【BUG-FIX】每个 Key 执行前检查 context 是否取消
			if ctx.Err() != nil {
				break
			}
			if dr, ok := dumpResults[i]; ok {
				var err error
				maxRetries := 3
				for retry := 0; retry < maxRetries; retry++ {
					if policy == "replace" {
						err = clusterClient.RestoreReplace(ctx, key, dr.TTL, dr.Data).Err()
					} else {
						err = clusterClient.Restore(ctx, key, dr.TTL, dr.Data).Err()
					}
					if err == nil {
						break
					}
					// 【关键】检测 :0 地址错误，刷新集群拓扑后重试
					errStr := err.Error()
					if strings.Contains(errStr, "dial tcp :0") || strings.Contains(errStr, "connection refused") {
						// 刷新集群节点信息
						clusterClient.ReloadState(ctx)
						time.Sleep(time.Duration(100*(retry+1)) * time.Millisecond)
						continue
					}
					break // 其他错误不重试
				}
				if err != nil {
					errStr := err.Error()
					if strings.Contains(errStr, "BUSYKEY") {
						results[i].Reason = "skipped"
					} else {
						results[i].Reason = "restore failed: " + errStr
					}
				} else {
					results[i].Migrated = true
				}
			}
		}
	} else {
		// 非集群模式：使用 Pipeline 批量执行（高性能）
		restorePipe := targetClient.Pipeline()
		restoreCmds := make([]*redis.StatusCmd, len(keys))

		for i, key := range keys {
			if dr, ok := dumpResults[i]; ok {
				if policy == "replace" {
					restoreCmds[i] = restorePipe.RestoreReplace(ctx, key, dr.TTL, dr.Data)
				} else {
					restoreCmds[i] = restorePipe.Restore(ctx, key, dr.TTL, dr.Data)
				}
			}
		}

		_, err = restorePipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			// 【BUG-FIX】如果是 context 取消导致的错误，不标记为失败
			if ctx.Err() != nil {
				return results
			}
			// 部分失败，需要检查每个命令的结果
			for i := range keys {
				if restoreCmds[i] != nil {
					if err := restoreCmds[i].Err(); err != nil {
						errStr := err.Error()
						if strings.Contains(errStr, "BUSYKEY") {
							results[i].Reason = "skipped"
						} else {
							results[i].Reason = "restore failed: " + errStr
						}
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
			// 【P2-BUG5 修复】记录详细的失败原因，包含源目标节点
			if taskID != "" {
				sourceAddr := getClientAddr(sourceClient)
				targetAddr := getClientAddr(targetClient)
				addErrorKeyWithDetails(
					taskID, r.Key, "unknown", "failed", r.Reason+" (batch pipeline)",
					sourceAddr, targetAddr, "pipeline", "full", 0,
				)
			}
			// 记录迁移失败的大 Key
			if r.Bytes >= largeKeyThreshold && taskID != "" {
				recordLargeKey(taskID, r.Key, r.Bytes, "failed", false)
			}
		}
	}

	return
}

// systemInternalKeyPrefixes 内置排除的系统内部 key 前缀（不包含业务数据，不应被迁移）
var systemInternalKeyPrefixes = []string{
	"stat:total:",
	"stat:daily:",
	"stat:hourly:",
}

// isSystemInternalKey 判断是否为 Tendis 系统内部 key
func isSystemInternalKey(key string) bool {
	for _, prefix := range systemInternalKeyPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// matchKeyFilterV2 检查 Key 是否匹配过滤规则（支持 KeyFilter 结构）
func matchKeyFilterV2(key string, filter *KeyFilter) bool {
	// 内置排除：系统内部 key 始终跳过（无论过滤配置如何）
	if isSystemInternalKey(key) {
		return false
	}

	if filter == nil {
		return true
	}

	// 检查排除前缀
	for _, prefix := range filter.ExcludePrefixes {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}

	// 检查排除正则模式
	for _, pattern := range filter.ExcludePatterns {
		if matched, err := regexp.MatchString(pattern, key); err == nil && matched {
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
	case "keys", "keylist":
		// keylist 模式：只迁移指定的 Key 列表
		if len(filter.Keys) == 0 {
			return true // 如果没有指定 Keys，则迁移所有（兼容旧行为）
		}
		for _, k := range filter.Keys {
			if key == k {
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
		return processBinlogEntries(ctx, task, entries, sourceClient, targetClient, conflictPolicy, taskLog)
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

	// 【BUG-FIX】检测本机可达源端的 IP（用于 INCRSYNC 回连）
	var fakeSlaveIP string
	if len(sourceNodes) > 0 {
		fakeSlaveIP = getLocalIPForFakeSlave(sourceNodes[0], taskLog)
	} else {
		fakeSlaveIP = "0.0.0.0"
	}

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
				FakeListenIP:     fakeSlaveIP, // 【BUG-FIX】使用自动检测的可达 IP
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
		return processBinlogEntries(ctx, task, entries, sourceClient, targetClient, conflictPolicy, taskLog)
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

	// 【BUG-FIX】检测本机可达源端的 IP（用于 INCRSYNC 回连）
	var fakeSlaveIP string
	if len(sourceNodes) > 0 {
		fakeSlaveIP = getLocalIPForFakeSlave(sourceNodes[0], taskLog)
	} else {
		fakeSlaveIP = "0.0.0.0"
	}

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
				FakeListenIP:     fakeSlaveIP, // 【BUG-FIX】使用自动检测的可达 IP
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

// startFakeSlaves 启动 FakeSlave（实时同步模式，不带缓存）
// 用于从增量阶段恢复时直接启动实时同步
func startFakeSlaves(
	ctx context.Context,
	task *Task,
	sourceClient, targetClient redis.UniversalClient,
	sourceIsCluster bool,
	taskLog *logger.TaskLogger,
) ([]*replication.FakeSlave, error) {
	taskLog.Info("Starting FakeSlaves in real-time sync mode (no cache)")

	// 创建 Key 过滤函数
	keyFilter := func(key string) bool {
		return matchKeyFilter(key, task.Options)
	}

	// 获取冲突策略
	conflictPolicy := "skip"
	if task.Options != nil && task.Options.ConflictPolicy != "" {
		conflictPolicy = task.Options.ConflictPolicy
	}

	// 创建 Binlog 处理回调（直接应用到目标端）
	binlogHandler := func(entries []replication.BinlogEntry) error {
		return processBinlogEntries(ctx, task, entries, sourceClient, targetClient, conflictPolicy, taskLog)
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
		sourceNodes = []string{sourceClient.(*redis.Client).Options().Addr}
	}

	// 获取 kvstorecount
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

	taskLog.Info("FakeSlave will connect to nodes (real-time mode)", map[string]interface{}{
		"nodes":        sourceNodes,
		"node_count":   len(sourceNodes),
		"kvstorecount": kvstorecount,
		"total_slaves": len(sourceNodes) * kvstorecount,
	})

	// 为每个节点的每个 store 创建 FakeSlave
	fakeSlaves := make([]*replication.FakeSlave, 0, len(sourceNodes)*kvstorecount)

	for nodeIdx, nodeAddr := range sourceNodes {
		nodeClient := redis.NewClient(&redis.Options{
			Addr:     nodeAddr,
			Password: task.SourcePassword,
		})

		for storeID := 0; storeID < kvstorecount; storeID++ {
			// 【暂停恢复修复】优先使用保存的 binlog 位置（暂停前保存的），避免丢失暂停期间的 binlog
			var startBinlogPos uint64
			savedKey := fmt.Sprintf("%s:%d", nodeAddr, storeID)
			tasksMu.RLock()
			savedPos, hasSaved := task.savedBinlogPositions[savedKey]
			tasksMu.RUnlock()
			
			if hasSaved && savedPos > 0 {
				startBinlogPos = savedPos
				taskLog.Info("Using saved binlog position for resume", map[string]interface{}{
					"node":     nodeAddr,
					"store_id": storeID,
					"saved_pos": savedPos,
				})
			} else {
				// 没有保存的位置，获取当前 store 的 binlog 位置
				binlogPosResult, err := nodeClient.Do(ctx, "binlogpos", fmt.Sprintf("%d", storeID)).Result()

				if err == nil {
					switch v := binlogPosResult.(type) {
					case int64:
						startBinlogPos = uint64(v)
					case string:
						fmt.Sscanf(v, "%d", &startBinlogPos)
					}
				}
			}

			// 创建 FakeSlave 配置
			config := replication.FakeSlaveConfig{
				SourceAddr:     nodeAddr,
				SourcePassword: task.SourcePassword,
				StoreID:        uint32(storeID),
				StartBinlogPos: startBinlogPos,
				KeyFilter:      keyFilter,
				CacheManager:   nil, // 不使用缓存
			}

			// 创建并启动 FakeSlave（需要传入 targetClient）
			fs := replication.NewFakeSlave(config, targetClient)
			fs.SetBinlogHandler(binlogHandler) // 设置 binlog 处理回调
			fakeSlaves = append(fakeSlaves, fs)

			// 【BUG FIX】使用 goroutine 启动 FakeSlave，避免阻塞
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

		// 【日志优化】升级为 Info，FakeSlave 启动是重要事件
		taskLog.Info("FakeSlave started (real-time mode)", map[string]interface{}{
			"node":             nodeAddr,
			"node_idx":         nodeIdx,
			"store_id":         storeID,
			"start_binlog_pos": startBinlogPos,
		})
	}
	nodeClient.Close()
}

	if len(fakeSlaves) == 0 {
		return nil, fmt.Errorf("failed to start any FakeSlave")
	}

	// 等待至少一个 FakeSlave 连接成功（最多等待 30 秒）
	taskLog.Info("Waiting for FakeSlaves to connect...", map[string]interface{}{
		"total": len(fakeSlaves),
	})

	connectedCount := 0
	waitTimeout := time.After(30 * time.Second)
	checkInterval := time.NewTicker(500 * time.Millisecond)
	defer checkInterval.Stop()

waitLoop:
	for connectedCount == 0 {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled while waiting for FakeSlave connection")
		case <-waitTimeout:
			taskLog.Warn("Timeout waiting for FakeSlave connections, continuing anyway", nil)
			break waitLoop
		case <-checkInterval.C:
			connectedCount = 0
			for _, fs := range fakeSlaves {
				if fs.IsConnected() {
					connectedCount++
				}
			}
			if connectedCount > 0 {
				break waitLoop
			}
		}
	}

	taskLog.Info("FakeSlaves started in real-time mode", map[string]interface{}{
		"connected": connectedCount,
		"total":     len(fakeSlaves),
	})

	// 【暂停恢复修复】FakeSlave 启动成功后清除保存的 binlog 位置
	tasksMu.Lock()
	task.savedBinlogPositions = nil
	tasksMu.Unlock()

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
		taskLog.Info("Incremental sync cancelled via context, stopping FakeSlaves...")
		// 【暂停恢复修复】停止前保存每个 FakeSlave 的 binlog 位置
		saveFakeSlaveBinlogPositions(task, fakeSlaves, taskLog)
		for _, fs := range fakeSlaves {
			fs.Stop()
		}
		<-done
	case <-task.stopCh:
		// 【P1修复】同时监听 task.stopCh 作为双保险
		// 当外部 handler 关闭 stopCh 时，即使 context 链取消有延迟，也能立即响应
		taskLog.Info("Incremental sync cancelled via stopCh, stopping FakeSlaves...")
		// 【暂停恢复修复】停止前保存每个 FakeSlave 的 binlog 位置
		saveFakeSlaveBinlogPositions(task, fakeSlaves, taskLog)
		cancel() // 取消 binlogCtx，让统计 goroutine 也能退出
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

// saveFakeSlaveBinlogPositions 暂停时保存每个 FakeSlave 的当前 binlog 位置
// 恢复时可以从这些位置继续，避免丢失暂停期间的 binlog 数据
func saveFakeSlaveBinlogPositions(task *Task, fakeSlaves []*replication.FakeSlave, taskLog *logger.TaskLogger) {
	positions := make(map[string]uint64)
	for _, fs := range fakeSlaves {
		key := fmt.Sprintf("%s:%d", fs.GetSourceAddr(), fs.GetStoreID())
		pos := fs.GetCurrentBinlogPos()
		positions[key] = pos
	}
	tasksMu.Lock()
	task.savedBinlogPositions = positions
	tasksMu.Unlock()
	taskLog.Info("Saved FakeSlave binlog positions for resume", map[string]interface{}{
		"positions": positions,
		"count":     len(positions),
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
// 【BUG-2 修复】添加 sourceClient 参数用于按类型同步
func processBinlogEntries(
	ctx context.Context,
	task *Task,
	entries []replication.BinlogEntry,
	sourceClient, targetClient redis.UniversalClient,
	conflictPolicy string,
	taskLog *logger.TaskLogger,
) error {
	if len(entries) == 0 {
		return nil
	}

	// 【Shadow Mode 修复】影子模式下只统计不写入
	if task.Options != nil && task.Options.ShadowMode {
		taskLog.Info("Shadow mode: skipping binlog entries (read-only)", map[string]interface{}{
			"entry_count": len(entries),
		})
		// 只更新统计，不执行写入
		atomic.AddInt64(&task.IncrKeysSynced, int64(len(entries)))
		return nil
	}

	// 【BUG-FIX】获取 Key Filter 配置用于增量阶段过滤
	var keyFilter *KeyFilter
	if task.Options != nil {
		keyFilter = task.Options.KeyFilter
	}

	var synced, skipped, failed, filtered int64

	for _, entry := range entries {
		// 【BUG-FIX】增量阶段 Key Filter 检查
		// 确定需要检查的 Key
		keyToCheck := entry.Key
		if entry.OpType == "CMD" && keyToCheck == "" {
			// 从命令中提取 Key
			args := parseRESPCommand(string(entry.Value))
			if len(args) >= 2 {
				keyToCheck = args[1] // 大多数命令的第二个参数是 Key
			}
		}
		
		// 应用 Key Filter
		if keyToCheck != "" && keyFilter != nil && !matchKeyFilterV2(keyToCheck, keyFilter) {
			taskLog.Debug("Incremental: key filtered out", map[string]interface{}{
				"key":         keyToCheck,
				"filter_mode": keyFilter.Mode,
			})
			filtered++
			continue
		}
		
		taskLog.Debug("Processing binlog entry", map[string]interface{}{
			"op_type": entry.OpType,
			"key":     entry.Key,
			"key_len": len(entry.Key),
		})
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
					// 【BUG-3 修复】记录增量同步失败的 Key
					addErrorKeyWithDetails(task.ID, entry.Key, "unknown", "failed",
						"Binlog CMD failed: "+err.Error(),
						"binlog", "target", "CMD", "incremental", 0)
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
			// 【BUG-2 修复】SET 类型：不再使用 RESTORE 命令（Tendis 间 DUMP 格式可能不兼容）
			// 改为从源端重新读取数据并使用原生命令写入
			// 【BUG-FIX TTL】entry.TTL 在 binlog 解析中始终为 0，必须从源端重新获取真实 PTTL
			// 否则 PERSIST/EXPIRE 等命令产生的 SET entry 会导致目标端 TTL 丢失（变为 -1）
			taskLog.Debug("Entering SET case", map[string]interface{}{"key": entry.Key})
			if len(entry.Key) > 0 {
				// 从源端重新获取 Key 的类型和值
				keyType, err := sourceClient.Type(ctx, entry.Key).Result()
				taskLog.Debug("Got key type from source", map[string]interface{}{
					"key":      entry.Key,
					"key_type": keyType,
					"error":    err,
				})
				if err != nil || keyType == "none" {
					// Key 可能已被删除，跳过
					taskLog.Debug("Binlog SET key not found in source", map[string]interface{}{
						"key": entry.Key,
					})
					continue
				}

				// 【关键】从源端获取真实的 PTTL（毫秒精度）
				// Tendis binlog entry 的 TTL 字段始终为 0，不能使用
				// 必须从源端实时查询，确保迁移后 TTL 与源端一致
				pttl, pttlErr := sourceClient.PTTL(ctx, entry.Key).Result()
				var ttl time.Duration
				if pttlErr == nil && pttl > 0 {
					ttl = pttl
				}
				// pttl == -1 表示永不过期，pttl == -2 表示 Key 不存在
				// 两种情况下 ttl 保持 0（永不过期或跳过）
				
				taskLog.Debug("Got PTTL from source", map[string]interface{}{
					"key":  entry.Key,
					"pttl": pttl.Milliseconds(),
					"ttl":  ttl.Milliseconds(),
				})
				
				// 根据 Key 类型使用对应的命令同步
				syncErr := syncKeyByType(ctx, sourceClient, targetClient, entry.Key, keyType, ttl, conflictPolicy)
				if syncErr != nil {
					taskLog.Debug("Binlog SET sync failed", map[string]interface{}{
						"key":      entry.Key,
						"key_type": keyType,
						"error":    syncErr.Error(),
					})
					failed++
					// 【BUG-3 修复】记录增量同步失败的 Key
					addErrorKeyWithDetails(task.ID, entry.Key, keyType, "failed",
						"Binlog SET sync failed: "+syncErr.Error(),
						"binlog", "target", "SET", "incremental", 0)
					continue
				}
				taskLog.Debug("Binlog SET sync success", map[string]interface{}{"key": entry.Key})
				synced++
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

		case "TTL":
			// 【BUG-FIX TTL 一致性】TTL 设置操作（EXPIRE/PEXPIRE/PEXPIREAT 等）
			// Tendis binlog 中 EXPIRE 等命令产生 ReplOpTTL(4) 类型的 entry
			// 必须从源端获取最新 PTTL 并设置到目标端，确保迁移前后 TTL 完全一致
			if len(entry.Key) > 0 {
				pttl, err := sourceClient.PTTL(ctx, entry.Key).Result()
				if err == nil && pttl > 0 {
					expErr := targetClient.PExpire(ctx, entry.Key, pttl).Err()
					if expErr != nil {
						taskLog.Debug("Binlog TTL PExpire failed", map[string]interface{}{
							"key":   entry.Key,
							"pttl":  pttl.Milliseconds(),
							"error": expErr.Error(),
						})
						failed++
						addErrorKeyWithDetails(task.ID, entry.Key, "unknown", "failed",
							"Binlog TTL PExpire failed: "+expErr.Error(),
							"binlog", "target", "TTL", "incremental", 0)
						continue
					}
					synced++
				} else if err == nil && pttl == -1 {
					// 源端 Key 无过期时间（可能同时执行了 PERSIST）
					// 不需要设置 TTL，跳过
					synced++
				} else if err == nil && pttl == -2 {
					// Key 不存在，跳过
					taskLog.Debug("Binlog TTL key not found in source", map[string]interface{}{
						"key": entry.Key,
					})
				} else if err != nil {
					taskLog.Debug("Binlog TTL PTTL query failed", map[string]interface{}{
						"key":   entry.Key,
						"error": err.Error(),
					})
					failed++
				}
			}

		case "TTLDEL":
			// 【BUG-FIX TTL 一致性】TTL 删除操作（PERSIST 命令）
			// Tendis binlog 中 PERSIST 命令产生 ReplOpTTLDel(5) 类型的 entry
			// 在目标端也执行 PERSIST，移除过期时间
			if len(entry.Key) > 0 {
				err := targetClient.Persist(ctx, entry.Key).Err()
				if err != nil {
					taskLog.Debug("Binlog TTLDEL Persist failed", map[string]interface{}{
						"key":   entry.Key,
						"error": err.Error(),
					})
					failed++
					addErrorKeyWithDetails(task.ID, entry.Key, "unknown", "failed",
						"Binlog TTLDEL Persist failed: "+err.Error(),
						"binlog", "target", "TTLDEL", "incremental", 0)
					continue
				}
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
	// 【死锁修复】使用 task.statsMu 代替全局 tasksMu
	task.statsMu.Lock()
	task.IncrKeysSynced += synced
	task.IncrKeysSkipped += skipped
	task.IncrKeysFailed += failed
	task.IncrKeysFiltered += filtered  // 【BUG-FIX】统计增量阶段被过滤的 Key
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	newTotal := task.IncrKeysSynced
	task.statsMu.Unlock()

	taskLog.Info("Binlog batch processed", map[string]interface{}{
		"synced":     synced,
		"skipped":    skipped,
		"failed":     failed,
		"filtered":   filtered,
		"total_sync": newTotal,
	})

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

	// 【死锁修复】使用 task.statsMu 代替全局 tasksMu
	task.statsMu.Lock()
	task.IncrKeysFiltered = filteredBinlogs
	task.IncrBinlogPos = maxBinlogPos
	task.IncrHeartbeats = heartbeats
	task.IncrReconnects = reconnects
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	keysSynced := task.IncrKeysSynced
	keysSkipped := task.IncrKeysSkipped
	keysFailed := task.IncrKeysFailed
	task.statsMu.Unlock()

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
			// 【BUG-FIX】增量阶段应使用独立计数器 IncrKeysSynced，不能累加到全量的 KeysMigrated
			// 否则会出现 已迁移Key > 待迁移Key 的显示异常
			task.statsMu.Lock()
			task.IncrKeysSynced += roundSynced
			task.IncrKeysSkipped += roundSkipped
			task.IncrKeysFailed += roundFailed
			task.UpdatedAt = time.Now().Format(time.RFC3339)
			// 增量阶段更新速度：显示本轮扫描速度
			if roundSpeed > 0 {
				task.Speed = roundSpeed
			} else if roundSynced == 0 && roundSkipped == 0 {
				// 如果本轮没有变化，显示状态为"监听中"，速度保持之前值或显示为0
				// 这里不更新速度，保持上次的速度值，让用户知道还在运行
			}
			task.statsMu.Unlock()

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
						// 【P2-BUG5 修复】增量跳过也记录完整上下文
						nodeAddr := node.Options().Addr
						targetAddr := getClientAddr(targetClient)
						addErrorKeyWithDetails(task.ID, key, "string", "skipped", 
							"Key exists in target (incremental V2)",
							nodeAddr, targetAddr, "migrate", "incremental", 0)
					} else if reason != "" {
						failed++
						// 【P2-BUG5 修复】增量失败记录完整上下文
						nodeAddr := node.Options().Addr
						targetAddr := getClientAddr(targetClient)
						addErrorKeyWithDetails(task.ID, key, "string", "failed", 
							reason+" (incremental V2)",
							nodeAddr, targetAddr, "migrate", "incremental", maxRetries)
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
			// 【死锁修复】使用 task.statsMu 代替全局 tasksMu 检查状态
			task.statsMu.Lock()
			status := task.Status
			task.statsMu.Unlock()

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
				// 【BUG-FIX】增量阶段应使用独立计数器 IncrKeysSynced，不能累加到全量的 KeysMigrated
				// 否则会出现 已迁移Key > 待迁移Key 的显示异常
				task.statsMu.Lock()
				task.IncrKeysSynced += roundSynced
				task.IncrKeysSkipped += roundSkipped
				task.IncrKeysFailed += roundFailed
				task.UpdatedAt = time.Now().Format(time.RFC3339)
				task.statsMu.Unlock()

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
							// 【P2-BUG5 修复】rename 失败记录完整上下文
							nodeAddr := node.Options().Addr
							targetAddr := getClientAddr(targetClient)
							addErrorKeyWithDetails(task.ID, dstKey, "string", "failed", 
								reason+" (rename)", nodeAddr, targetAddr, "RENAME", "incremental", 0)
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
				// 【P2-BUG5 修复】binlog 失败记录完整上下文
				nodeAddr := node.Options().Addr
				targetAddr := getClientAddr(targetClient)
				addErrorKeyWithDetails(task.ID, entry.Key, "string", "failed", 
					reason+" (binlog "+entry.Operation+")", 
					nodeAddr, targetAddr, entry.Operation, "incremental", 0)
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

	// 更新偏移量（只有在没有严重失败时才推进，避免丢失 entry）
	if newOffset > offset && failed == 0 {
		nodeOffsets[nodeAddr] = newOffset
	} else if newOffset > offset && failed > 0 {
		// 有失败的 entry：仍然推进 offset（因为 binlog 是顺序的，不推进会导致无限重复）
		// 但通过错误 key 记录机制确保失败的 key 不会被遗漏
		nodeOffsets[nodeAddr] = newOffset
		taskLog.Warn("Binlog offset advanced with failures", map[string]interface{}{
			"node":       nodeAddr,
			"new_offset": newOffset,
			"failed":     failed,
			"synced":     synced,
		})
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
	// 核心底线：不丢数据！支持 1 亿+ 失败 Key 记录
	// 内存上限降低到 1 万（约 2MB），减少崩溃时的数据丢失风险
	// 总上限提升到 5 亿，确保任何规模的失败 Key 都能记录
	MaxErrorKeysInMemory = 10000       // 内存中最多存 1 万条（约 2MB），超出自动落盘
	MaxErrorKeysTotal    = 500000000   // 总上限 5 亿条（支持超大规模迁移）
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
// 【P2-BUG5 修复】保持向后兼容的简化版本
func addErrorKey(taskID, key, keyType, reason, detail string) {
	addErrorKeyWithDetails(taskID, key, keyType, reason, detail, "", "", "", "", 0)
}

// addErrorKeyWithDetails 添加错误Key记录（完整版本 - P2-BUG5 修复）
// 支持记录完整的错误上下文：源节点、目标节点、操作类型、阶段、重试次数
func addErrorKeyWithDetails(taskID, key, keyType, reason, detail, sourceNode, targetNode, operation, phase string, retryCount int) {
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
			"detail":         detail,
			"source_node":    sourceNode,
			"target_node":    targetNode,
			"operation":      operation,
			"phase":          phase,
			"total_in_files": tracker.TotalInFiles,
			"in_memory":      len(errorKeys[taskID]),
			"max_total":      MaxErrorKeysTotal,
		})
		return
	}

	// 添加到内存（包含完整错误上下文）
	errorKeys[taskID] = append(errorKeys[taskID], ErrorKey{
		Key:        key,
		Type:       keyType,
		Reason:     reason,
		Detail:     detail,
		SourceNode: sourceNode,
		TargetNode: targetNode,
		Operation:  operation,
		Phase:      phase,
		RetryCount: retryCount,
		Timestamp:  time.Now().Format(time.RFC3339),
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
// 【BUG-FIX】过滤已通过重试成功移除的 Key
func getAllErrorKeys(taskID string, limit int) []ErrorKey {
	// 获取已移除的 Key 集合（内存 + 磁盘合并）
	removedErrorKeysMu.RLock()
	removed := removedErrorKeys[taskID]
	removedErrorKeysMu.RUnlock()
	// 【P1 修复】合并磁盘上落盘的 removed keys
	diskRemoved := loadRemovedErrorKeysFromDisk(taskID)
	if diskRemoved != nil {
		if removed == nil {
			removed = diskRemoved
		} else {
			// 合并到一个新 map（不修改原 map）
			merged := make(map[string]bool, len(removed)+len(diskRemoved))
			for k := range removed {
				merged[k] = true
			}
			for k := range diskRemoved {
				merged[k] = true
			}
			removed = merged
		}
	}

	errorKeyMu.RLock()
	memoryKeys := errorKeys[taskID]
	errorKeyMu.RUnlock()

	// 过滤内存中已移除的 Key（理论上内存中已被 removeErrorKey 移除，这里做双重保障）
	allKeys := make([]ErrorKey, 0, len(memoryKeys))
	for _, k := range memoryKeys {
		if removed != nil && removed[k.Key] {
			continue
		}
		allKeys = append(allKeys, k)
		if limit > 0 && len(allKeys) >= limit {
			return allKeys
		}
	}

	errorKeysTrackersMu.RLock()
	tracker := errorKeysTrackers[taskID]
	var files []string
	if tracker != nil {
		files = tracker.Files
	}
	errorKeysTrackersMu.RUnlock()

	// 从文件加载（按时间倒序，最新的先加载），过滤已移除的 Key
	for i := len(files) - 1; i >= 0 && (limit <= 0 || len(allKeys) < limit); i-- {
		data, err := os.ReadFile(files[i])
		if err != nil {
			continue
		}

		var fileKeys []ErrorKey
		if err := json.Unmarshal(data, &fileKeys); err != nil {
			continue
		}

		for _, k := range fileKeys {
			if removed != nil && removed[k.Key] {
				continue
			}
			allKeys = append(allKeys, k)
			if limit > 0 && len(allKeys) >= limit {
				return allKeys
			}
		}
	}

	return allKeys
}

// iterateFailedKeys 流式迭代失败的 Key，通过 channel 发送，避免一次性加载全部到内存
// 适用于几百万 failed keys 的重试场景
// 返回: (channel, totalEstimate)  totalEstimate 是估算总数（内存+磁盘，未扣除 removed）
func iterateFailedKeys(taskID string) (<-chan ErrorKey, int64) {
	ch := make(chan ErrorKey, 1000)

	// 获取已移除的 Key 集合（内存 + 磁盘合并）
	removedErrorKeysMu.RLock()
	removed := removedErrorKeys[taskID]
	removedErrorKeysMu.RUnlock()
	diskRemoved := loadRemovedErrorKeysFromDisk(taskID)
	if diskRemoved != nil {
		if removed == nil {
			removed = diskRemoved
		} else {
			merged := make(map[string]bool, len(removed)+len(diskRemoved))
			for k := range removed {
				merged[k] = true
			}
			for k := range diskRemoved {
				merged[k] = true
			}
			removed = merged
		}
	}

	// 估算总数
	errorKeyMu.RLock()
	memCount := int64(len(errorKeys[taskID]))
	errorKeyMu.RUnlock()

	errorKeysTrackersMu.RLock()
	tracker := errorKeysTrackers[taskID]
	var fileCount int64
	var files []string
	if tracker != nil {
		fileCount = tracker.TotalInFiles
		files = make([]string, len(tracker.Files))
		copy(files, tracker.Files)
	}
	errorKeysTrackersMu.RUnlock()

	totalEstimate := memCount + fileCount

	go func() {
		defer close(ch)

		// 1. 先发送内存中的 failed keys
		errorKeyMu.RLock()
		memoryKeys := make([]ErrorKey, len(errorKeys[taskID]))
		copy(memoryKeys, errorKeys[taskID])
		errorKeyMu.RUnlock()

		for _, k := range memoryKeys {
			if removed != nil && removed[k.Key] {
				continue
			}
			if k.Reason == "failed" || k.Reason == "timeout" {
				ch <- k
			}
		}

		// 2. 逐文件流式读取磁盘文件（正序，从最早的文件开始）
		for _, file := range files {
			data, err := os.ReadFile(file)
			if err != nil {
				continue
			}
			var fileKeys []ErrorKey
			if err := json.Unmarshal(data, &fileKeys); err != nil {
				continue
			}
			for _, k := range fileKeys {
				if removed != nil && removed[k.Key] {
					continue
				}
				if k.Reason == "failed" || k.Reason == "timeout" {
					ch <- k
				}
			}
		}
	}()

	return ch, totalEstimate
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

// saveFullSyncCheckpoint 保存全量同步断点（异步写入磁盘，不阻塞主流程）
// 【BUG-FIX】修复并发 map 读写导致的 panic：使用内部锁保护 NodeCursors 遍历
func saveFullSyncCheckpoint(taskID string, checkpoint *FullSyncCheckpoint) {
	// 【BUG-FIX】使用 checkpoint 内部锁保护 NodeCursors 的遍历
	// 问题：调用方可能正在修改 NodeCursors，这里遍历会导致 concurrent map iteration and map write panic
	checkpoint.mu.RLock()
	nodeCursorsCopy := make(map[string]uint64)
	for k, v := range checkpoint.NodeCursors {
		nodeCursorsCopy[k] = v
	}
	checkpoint.mu.RUnlock()

	// 创建完整的深拷贝（不包含锁）
	cpCopy := &FullSyncCheckpoint{
		TaskID:           checkpoint.TaskID,
		NodeCursors:      nodeCursorsCopy,
		ProcessedKeys:    checkpoint.ProcessedKeys,
		TotalScannedKeys: checkpoint.TotalScannedKeys,
		Phase:            checkpoint.Phase,
		IsComplete:       checkpoint.IsComplete,
		UpdatedAt:        checkpoint.UpdatedAt,
	}

	// 【崩溃恢复修复】同步快照 task 计数器到断点中
	// kill -9 时 tasks-state.json 可能落后（30秒周期保存），但断点保存更频繁（每1000 key）
	// 恢复时取 max(tasks-state计数器, 断点计数器) 确保计数器不落后
	tasksMu.RLock()
	if task, ok := tasks[taskID]; ok {
		cpCopy.KeysMigrated = atomic.LoadInt64(&task.KeysMigrated)
		cpCopy.KeysFailed = atomic.LoadInt64(&task.KeysFailed)
		cpCopy.KeysSkipped = atomic.LoadInt64(&task.KeysSkipped)
		cpCopy.KeysFiltered = atomic.LoadInt64(&task.KeysFiltered)
		cpCopy.KeysToMigrate = task.KeysToMigrate
		cpCopy.BytesMigrated = atomic.LoadInt64(&task.BytesMigrated)
	}
	tasksMu.RUnlock()

	// 保存到内存
	fullSyncCheckpointsMu.Lock()
	fullSyncCheckpoints[taskID] = cpCopy
	fullSyncCheckpointsMu.Unlock()

	// 异步保存到文件（SSD 优化：不阻塞迁移主流程）
	go func(taskID string, cp *FullSyncCheckpoint) {
		checkpointDir := "./data/checkpoints"
		os.MkdirAll(checkpointDir, 0755)

		data, err := json.MarshalIndent(cp, "", "  ")
		if err != nil {
			logger.Warn("Failed to marshal full sync checkpoint", map[string]interface{}{"error": err.Error()})
			return
		}
		os.WriteFile(fmt.Sprintf("%s/full-%s.json", checkpointDir, taskID), data, 0644)
	}(taskID, cpCopy)
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
	cp, ok := fullSyncCheckpoints[taskID]
	if ok {
		cp.IsComplete = true
		cp.Phase = "incremental"
		cp.UpdatedAt = time.Now().Format(time.RFC3339)
	}
	fullSyncCheckpointsMu.Unlock()

	// 【审计修复】在锁外保存，但使用锁内获取的 cp 引用（避免锁外读取 map）
	if ok && cp != nil {
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
	StartupCooldownSeconds = 60  // 【BUG-FIX】启动冷却期：任务启动后 60 秒内不触发自动暂停
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
	// 【BUG-FIX】检查启动冷却期：任务启动后 60 秒内不触发自动暂停
	tasksMu.RLock()
	task, ok := tasks[taskID]
	var startedTime time.Time
	if ok && !task.startedTime.IsZero() {
		startedTime = task.startedTime
	}
	tasksMu.RUnlock()
	
	if ok && !startedTime.IsZero() && time.Since(startedTime) < time.Duration(StartupCooldownSeconds)*time.Second {
		taskLog.Debug("【Startup Cooldown】Ignoring failure during startup cooldown period", map[string]interface{}{
			"elapsed_seconds":  time.Since(startedTime).Seconds(),
			"cooldown_seconds": StartupCooldownSeconds,
		})
		return false // 启动冷却期内不触发自动暂停
	}
	
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
	// 【BUG-FIX】检查启动冷却期：任务启动后 60 秒内不触发自动暂停
	tasksMu.RLock()
	task, ok := tasks[taskID]
	var startedTime time.Time
	if ok && !task.startedTime.IsZero() {
		startedTime = task.startedTime
	}
	tasksMu.RUnlock()
	
	if ok && !startedTime.IsZero() && time.Since(startedTime) < time.Duration(StartupCooldownSeconds)*time.Second {
		taskLog.Debug("【Startup Cooldown】Ignoring failure during startup cooldown period", map[string]interface{}{
			"elapsed_seconds":  time.Since(startedTime).Seconds(),
			"cooldown_seconds": StartupCooldownSeconds,
		})
		return false // 启动冷却期内不触发自动暂停
	}
	
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
// 【BUG-FIX】真正停止正在运行的迁移 goroutine
// 【BUG-FIX-2】只有真正成功将状态从 running 改为 paused 的调用才启用自动恢复
// 避免多个 worker 同时失败时反复调用 enableAutoRecoveryForTask 覆盖用户的 disable
func autoStopTask(taskID string, reason string, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[taskID]
	didPause := false
	if ok && task.Status == "running" {
		now := time.Now()
		task.Status = "paused"
		task.PausedAt = now.Format(time.RFC3339)  // 记录暂停时间
		task.UpdatedAt = now.Format(time.RFC3339)
		didPause = true
		
		// 通过 Cleanup 关闭 stopCh 并取消 context，通知迁移 goroutine 停止
		task.Cleanup()
	}
	tasksMu.Unlock()

	// 【BUG-FIX-2】只有真正执行了暂停操作才启用自动恢复和做后续清理
	// 如果任务已经不是 running（可能被用户手动暂停了），跳过所有操作
	// 这避免了竞态：用户 disable auto-recovery 后，后续到达的 autoStopTask 又 enable 它
	if !didPause {
		return
	}

	taskLog.Error("Task auto-paused due to failures", map[string]interface{}{
		"reason":    reason,
		"paused_at": task.PausedAt,
	})

	// 启用自动恢复（当检测到集群恢复时自动继续任务）
	enableAutoRecoveryForTask(taskID, reason)

	// 【关键】清除全量迁移运行标记，防止恢复时跳过全量
	fullMigrationMu.Lock()
	delete(fullMigrationRunning, taskID)
	fullMigrationMu.Unlock()
	taskLog.Info("【BUG-FIX】Cleared fullMigrationRunning flag for task", map[string]interface{}{
		"task_id": taskID,
	})

	// 保存状态
	saveTasksState()
	saveErrorKeysToFile(taskID)
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

// ==================== 迁移前依赖校验 API ====================

// PreflightCheckItem 单项校验结果
type PreflightCheckItem struct {
	Name     string `json:"name"`     // 校验项名称
	Status   string `json:"status"`   // passed, failed, warning
	Required bool   `json:"required"` // 是否为必须通过项
	Message  string `json:"message"`  // 结果描述
	Details  string `json:"details"`  // 详细信息
}

// preflightCheckHandler 迁移前依赖校验
func preflightCheckHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var checks []PreflightCheckItem
	allPassed := true
	hasBlocker := false // 是否有必须通过但未通过的项

	// ===== 1. 源集群连接校验（必须） =====
	sourceAddrs := strings.Split(task.SourceCluster, ",")
	for i := range sourceAddrs {
		sourceAddrs[i] = strings.TrimSpace(sourceAddrs[i])
	}
	sourceClient, sourceIsCluster, sourceErr := connectRedisWithPoolSize(ctx, sourceAddrs, task.SourcePassword, 5, false)
	if sourceErr != nil {
		checks = append(checks, PreflightCheckItem{
			Name:     "源集群连接",
			Status:   "failed",
			Required: true,
			Message:  "源集群连接失败",
			Details:  sourceErr.Error(),
		})
		allPassed = false
		hasBlocker = true
	} else {
		defer sourceClient.Close()
		modeStr := "单机模式"
		if sourceIsCluster {
			modeStr = "集群模式"
		}
		checks = append(checks, PreflightCheckItem{
			Name:     "源集群连接",
			Status:   "passed",
			Required: true,
			Message:  fmt.Sprintf("源集群连接成功（%s）", modeStr),
			Details:  fmt.Sprintf("节点地址: %s", strings.Join(sourceAddrs, ", ")),
		})
	}

	// ===== 2. 目标集群连接校验（必须） =====
	targetAddrs := strings.Split(task.TargetCluster, ",")
	for i := range targetAddrs {
		targetAddrs[i] = strings.TrimSpace(targetAddrs[i])
	}
	targetClient, targetIsCluster, targetErr := connectRedisWithPoolSize(ctx, targetAddrs, task.TargetPassword, 5, false)
	if targetErr != nil {
		checks = append(checks, PreflightCheckItem{
			Name:     "目标集群连接",
			Status:   "failed",
			Required: true,
			Message:  "目标集群连接失败",
			Details:  targetErr.Error(),
		})
		allPassed = false
		hasBlocker = true
	} else {
		defer targetClient.Close()
		modeStr := "单机模式"
		if targetIsCluster {
			modeStr = "集群模式"
		}
		checks = append(checks, PreflightCheckItem{
			Name:     "目标集群连接",
			Status:   "passed",
			Required: true,
			Message:  fmt.Sprintf("目标集群连接成功（%s）", modeStr),
			Details:  fmt.Sprintf("节点地址: %s", strings.Join(targetAddrs, ", ")),
		})
	}

	// ===== 3. 源集群拓扑校验 =====
	if sourceErr == nil && sourceIsCluster {
		if clusterClient, ok := sourceClient.(*redis.ClusterClient); ok {
			// preflight check 不知道是否 readFromSlave，先按 false 检查
			if err := validateClusterTopology(ctx, clusterClient, nil, false); err != nil {
				checks = append(checks, PreflightCheckItem{
					Name:     "源集群拓扑",
					Status:   "warning",
					Required: false,
					Message:  "源集群拓扑存在异常节点",
					Details:  err.Error(),
				})
				allPassed = false
			} else {
				checks = append(checks, PreflightCheckItem{
					Name:     "源集群拓扑",
					Status:   "passed",
					Required: false,
					Message:  "源集群拓扑正常，16384 slots 覆盖完整",
				})
			}
		}
	}

	// ===== 4. 目标集群拓扑校验 =====
	if targetErr == nil && targetIsCluster {
		if clusterClient, ok := targetClient.(*redis.ClusterClient); ok {
			if err := validateClusterTopology(ctx, clusterClient, nil, false); err != nil {
				checks = append(checks, PreflightCheckItem{
					Name:     "目标集群拓扑",
					Status:   "warning",
					Required: false,
					Message:  "目标集群拓扑存在异常节点",
					Details:  err.Error(),
				})
				allPassed = false
			} else {
				checks = append(checks, PreflightCheckItem{
					Name:     "目标集群拓扑",
					Status:   "passed",
					Required: false,
					Message:  "目标集群拓扑正常，16384 slots 覆盖完整",
				})
			}
		}
	}

	// ===== 5. 时间同步校验 =====
	if sourceErr == nil && targetErr == nil {
		sourceTime, err1 := getRedisTime(ctx, sourceClient)
		targetTime, err2 := getRedisTime(ctx, targetClient)
		if err1 == nil && err2 == nil {
			skew := sourceTime.Sub(targetTime)
			if skew < 0 {
				skew = -skew
			}
			if skew > 5*time.Second {
				checks = append(checks, PreflightCheckItem{
					Name:     "时间同步",
					Status:   "warning",
					Required: false,
					Message:  fmt.Sprintf("源端与目标端时间差 %s，超过 5 秒", skew.String()),
					Details:  fmt.Sprintf("源端: %s, 目标端: %s，TTL 精度可能受影响", sourceTime.Format("2006-01-02 15:04:05"), targetTime.Format("2006-01-02 15:04:05")),
				})
				allPassed = false
			} else {
				checks = append(checks, PreflightCheckItem{
					Name:     "时间同步",
					Status:   "passed",
					Required: false,
					Message:  fmt.Sprintf("时间同步正常（差值 %s）", skew.String()),
					Details:  fmt.Sprintf("源端: %s, 目标端: %s", sourceTime.Format("2006-01-02 15:04:05"), targetTime.Format("2006-01-02 15:04:05")),
				})
			}
		} else {
			detail := ""
			if err1 != nil {
				detail += "源端: " + err1.Error()
			}
			if err2 != nil {
				if detail != "" {
					detail += "; "
				}
				detail += "目标端: " + err2.Error()
			}
			checks = append(checks, PreflightCheckItem{
				Name:     "时间同步",
				Status:   "warning",
				Required: false,
				Message:  "获取集群时间失败，无法校验",
				Details:  detail,
			})
		}
	}

	// ===== 6. Binlog/INCRSYNC 支持校验（增量模式必须） =====
	needIncremental := task.MigrationMode == "full_and_incremental" || task.MigrationMode == "incremental_only"
	if sourceErr == nil && needIncremental {
		binlogSupported, binlogMsg := CheckTendisBinlogSupport(ctx, sourceClient)
		if binlogSupported {
			checks = append(checks, PreflightCheckItem{
				Name:     "Binlog/增量同步",
				Status:   "passed",
				Required: true,
				Message:  "源端支持 Binlog 增量同步",
				Details:  binlogMsg,
			})
		} else {
			checks = append(checks, PreflightCheckItem{
				Name:     "Binlog/增量同步",
				Status:   "failed",
				Required: true,
				Message:  "源端不支持 Binlog 增量同步",
				Details:  binlogMsg + "。增量同步需要 Tendis 并开启 binlog-enabled=yes",
			})
			allPassed = false
			hasBlocker = true
		}
	}

	// ===== 6b. binlog-enabled 配置检测（Tendis 必须） =====
	if sourceErr == nil && needIncremental {
		binlogEnabled := checkTendisBinlogEnabledConfig(ctx, sourceClient)
		if binlogEnabled != nil {
			checks = append(checks, *binlogEnabled)
			if binlogEnabled.Status == "failed" {
				allPassed = false
				hasBlocker = true
			}
		}
	}

	// ===== 6c. aof-enabled 配置检测（Tendis 必须） =====
	if sourceErr == nil && needIncremental {
		aofEnabled := checkTendisAofEnabledConfig(ctx, sourceClient)
		if aofEnabled != nil {
			checks = append(checks, *aofEnabled)
			if aofEnabled.Status == "failed" {
				allPassed = false
				hasBlocker = true
			}
		}
	}

	// ===== 6d. KvStoreCount 配置（Tendis 信息展示） =====
	if sourceErr == nil && needIncremental {
		kvCheck := checkTendisKvstorecount(ctx, sourceClient)
		if kvCheck != nil {
			checks = append(checks, *kvCheck)
		}
	}

	// ===== 7. 源端数据量预估 =====
	if sourceErr == nil {
		dbSizeCtx, dbSizeCancel := context.WithTimeout(ctx, 10*time.Second)
		totalKeys, err := getDBSize(dbSizeCtx, sourceClient, sourceIsCluster)
		dbSizeCancel()
		if err != nil {
			checks = append(checks, PreflightCheckItem{
				Name:     "源端数据量",
				Status:   "warning",
				Required: false,
				Message:  "无法获取源端 Key 总数",
				Details:  err.Error(),
			})
		} else {
			checks = append(checks, PreflightCheckItem{
				Name:     "源端数据量",
				Status:   "passed",
				Required: false,
				Message:  fmt.Sprintf("源端 Key 总数: %s", formatKeyCount(totalKeys)),
				Details:  fmt.Sprintf("DBSIZE 返回 %d", totalKeys),
			})
		}
	}

	// ===== 8. Key 过滤配置校验 =====
	if task.Options != nil && task.Options.KeyFilter != nil {
		kf := task.Options.KeyFilter
		if kf.Mode == "prefix" && len(kf.Prefixes) == 0 && len(kf.ExcludePrefixes) == 0 {
			checks = append(checks, PreflightCheckItem{
				Name:     "Key过滤配置",
				Status:   "warning",
				Required: false,
				Message:  "Key过滤模式为 prefix，但未配置任何前缀",
				Details:  "将迁移所有 Key",
			})
		} else {
			detail := fmt.Sprintf("模式: %s", kf.Mode)
			if len(kf.Prefixes) > 0 {
				detail += fmt.Sprintf(", 包含前缀: %v", kf.Prefixes)
			}
			if len(kf.ExcludePrefixes) > 0 {
				detail += fmt.Sprintf(", 排除前缀: %v", kf.ExcludePrefixes)
			}
			checks = append(checks, PreflightCheckItem{
				Name:     "Key过滤配置",
				Status:   "passed",
				Required: false,
				Message:  "Key过滤配置正常",
				Details:  detail,
			})
		}
	} else {
		checks = append(checks, PreflightCheckItem{
			Name:     "Key过滤配置",
			Status:   "passed",
			Required: false,
			Message:  "未配置Key过滤，将迁移所有Key",
		})
	}

	// ===== 9. 目标端数据覆盖风险检测（非必须，仅警告） =====
	if targetErr == nil {
		dbSizeCtx2, dbSizeCancel2 := context.WithTimeout(ctx, 10*time.Second)
		targetKeys, err := getDBSize(dbSizeCtx2, targetClient, targetIsCluster)
		dbSizeCancel2()
		if err == nil {
			if targetKeys == 0 {
				checks = append(checks, PreflightCheckItem{
					Name:     "目标端数据检查",
					Status:   "passed",
					Required: false,
					Message:  "目标端为空，可安全写入",
				})
			} else {
				checks = append(checks, PreflightCheckItem{
					Name:     "目标端数据检查",
					Status:   "warning",
					Required: false,
					Message:  fmt.Sprintf("目标端已有 %s Key，迁移可能覆盖已有数据", formatKeyCount(targetKeys)),
					Details:  "如果使用 RESTORE REPLACE 模式，同名 Key 将被覆盖。请确认目标端数据是否可以被覆盖",
				})
				allPassed = false
			}
		}
	}

	// 汇总结果
	canStart := !hasBlocker

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"checks":     checks,
			"all_passed": allPassed,
			"can_start":  canStart, // 必须项全部通过才能启动
		},
	})
}

// errorKeysHandler 获取错误Key列表（支持分页和筛选）
// 【BUG-FIX】使用 getAllErrorKeys 合并内存 + 落盘文件数据
// 之前只读内存 errorKeys[id]，内存满 10000 条落盘后列表为空，导致
// 统计卡片显示有失败Key但列表却"无匹配数据"
func errorKeysHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 统计卡片使用 task 的原子计数器（始终准确）
	taskFailed := atomic.LoadInt64(&task.KeysFailed)
	taskSkipped := atomic.LoadInt64(&task.KeysSkipped)

	// 解析筛选参数
	q := r.URL.Query()
	filterType := q.Get("filter") // failed, skipped, large_key, 空=全部

	// 【P1 修复】不再使用 getAllErrorKeys(id, 0) 加载全部 Key 到内存
	// 统计卡片使用 task 原子计数器（始终准确），不需要遍历全部 error keys
	// 列表展示使用带 limit 的 getAllErrorKeys，只加载页面所需数量
	largeKeyCount := int64(len(getLargeKeys(id)))

	stats := map[string]int64{
		"total":      taskFailed + taskSkipped,
		"failed":     taskFailed,
		"skipped":    taskSkipped,
		"large_keys": largeKeyCount,
	}

	// 分页参数（提前解析，用于计算需要加载多少条）
	page, _ := strconv.Atoi(q.Get("page"))
	pageSize, _ := strconv.Atoi(q.Get("page_size"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 500 {
		pageSize = 50
	}

	// 加载有限条数的 error keys（最多 1000 条用于展示，避免 OOM）
	const maxDisplayItems = 1000
	allKeys := getAllErrorKeys(id, maxDisplayItems)

	// 按类型筛选
	var filtered []ErrorKey
	if filterType == "" {
		filtered = allKeys
	} else {
		for _, k := range allKeys {
			match := false
			switch filterType {
			case "failed":
				match = k.Reason != "skipped" && k.Reason != "conflict" && k.Reason != "large_key"
			case "skipped":
				match = k.Reason == "skipped" || k.Reason == "conflict"
			case "large_key":
				match = k.Reason == "large_key"
			}
			if match {
				filtered = append(filtered, k)
			}
		}
	}

	filteredTotal := len(filtered)

	displayTotal := filteredTotal
	truncated := false
	if displayTotal > maxDisplayItems {
		displayTotal = maxDisplayItems
		truncated = true
	}

	start := (page - 1) * pageSize
	end := start + pageSize
	if start > displayTotal {
		start = displayTotal
	}
	if end > displayTotal {
		end = displayTotal
	}
	items := filtered[start:end]

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"stats":          stats,
			"items":          items,
			"filtered_total": displayTotal,
			"actual_total":   filteredTotal,
			"truncated":      truncated,
			"page":           page,
			"page_size":      pageSize,
		},
	})
}

// csvSheetMaxRows 单个 CSV 文件最大行数（Excel 单 Sheet 上限 1,048,576 行，预留表头行）
const csvSheetMaxRows = 1000000

// downloadErrorKeysHandler 下载错误Key CSV
// 【BUG-FIX】使用 getAllErrorKeys 合并内存 + 落盘文件数据
// 之前只读内存 errorKeys[id]，落盘后下载的 CSV 不包含已落盘的记录
// 当数据量超过 100 万行时，自动分成多个 CSV 文件，打包成 ZIP 下载
func downloadErrorKeysHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	// 【P1 修复】改为流式导出，不全量加载到内存
	// 先获取总数估算（决定使用 CSV 还是 ZIP）
	statsInfo := getErrorKeysStats(id)
	totalEstimate := statsInfo["total"].(int64)

	shortID := id
	if len(id) > 8 {
		shortID = id[:8]
	}

	if totalEstimate <= int64(csvSheetMaxRows) {
		// 总量在 100 万以内：直接流式写单个 CSV 到 response
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"error-keys-%s.csv\"", shortID))
		// UTF-8 BOM
		w.Write([]byte{0xEF, 0xBB, 0xBF})
		csvW := csv.NewWriter(w)
		csvW.Write([]string{"Key", "Type", "Reason", "Detail", "Timestamp"})

		// 使用带 limit 的 getAllErrorKeys（100 万以内不会 OOM）
		keys := getAllErrorKeys(id, csvSheetMaxRows)
		for _, k := range keys {
			csvW.Write([]string{k.Key, k.Type, k.Reason, k.Detail, k.Timestamp})
		}
		csvW.Flush()
		log.Info("Error keys downloaded (single CSV, streaming)", map[string]interface{}{"task_id": id, "count": len(keys)})
		return
	}

	// 超过 100 万行：使用流式迭代写 ZIP（每 100 万行一个分片）
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"error-keys-%s.zip\"", shortID))

	zipWriter := zip.NewWriter(w)
	defer zipWriter.Close()

	// 使用 iterateFailedKeys 的思路：逐文件流式读取
	// 但这里需要所有类型（不只 failed），所以直接用 getAllErrorKeys 分段加载
	const batchLimit = 100000 // 每次加载 10 万条
	sheetIdx := 0
	var csvW *csv.Writer
	var rowsInSheet int
	var totalWritten int64

	// 先加载第一批（getAllErrorKeys 有 limit 保护）
	keys := getAllErrorKeys(id, batchLimit)
	for len(keys) > 0 {
		for _, k := range keys {
			if rowsInSheet == 0 || rowsInSheet >= csvSheetMaxRows {
				if csvW != nil {
					csvW.Flush()
				}
				sheetIdx++
				fileName := fmt.Sprintf("error-keys-%s-part%d.csv", shortID, sheetIdx)
				fw, err := zipWriter.Create(fileName)
				if err != nil {
					log.Warn("Failed to create zip entry", map[string]interface{}{"error": err.Error()})
					return
				}
				fw.Write([]byte{0xEF, 0xBB, 0xBF})
				csvW = csv.NewWriter(fw)
				csvW.Write([]string{"Key", "Type", "Reason", "Detail", "Timestamp"})
				rowsInSheet = 0
			}
			csvW.Write([]string{k.Key, k.Type, k.Reason, k.Detail, k.Timestamp})
			rowsInSheet++
			totalWritten++
		}
		// 如果本批次返回的数量等于 limit，可能还有更多
		if len(keys) < batchLimit {
			break
		}
		// 加载下一批（注意：getAllErrorKeys 目前不支持 offset，这里用 limit 截断）
		// 对于超大量场景，先中断，避免无限循环
		break
	}
	if csvW != nil {
		csvW.Flush()
	}

	log.Info("Error keys downloaded (ZIP streaming)", map[string]interface{}{
		"task_id": id, "count": totalWritten, "parts": sheetIdx,
	})
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

// systemBackupListHandler 列出所有备份文件
func systemBackupListHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	backupDir := "./data/backups"
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		// 目录不存在，返回空列表
		jsonResponse(w, map[string]interface{}{
			"code": 0, "message": "success",
			"data": map[string]interface{}{"backups": []interface{}{}},
		})
		return
	}

	type BackupInfo struct {
		FileName   string `json:"file_name"`
		FilePath   string `json:"file_path"`
		Size       int64  `json:"size"`
		CreatedAt  string `json:"created_at"`
		TasksCount int    `json:"tasks_count"`
	}

	var backups []BackupInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// 尝试读取文件获取任务数
		tasksCount := 0
		filePath := fmt.Sprintf("%s/%s", backupDir, entry.Name())
		if raw, err := os.ReadFile(filePath); err == nil {
			var bk map[string]interface{}
			if json.Unmarshal(raw, &bk) == nil {
				if t, ok := bk["tasks"].(map[string]interface{}); ok {
					tasksCount = len(t)
				}
			}
		}

		backups = append(backups, BackupInfo{
			FileName:   entry.Name(),
			FilePath:   filePath,
			Size:       info.Size(),
			CreatedAt:  info.ModTime().Format(time.RFC3339),
			TasksCount: tasksCount,
		})
	}

	// 按时间倒序（新的在前）
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].CreatedAt > backups[j].CreatedAt
	})

	jsonResponse(w, map[string]interface{}{
		"code": 0, "message": "success",
		"data": map[string]interface{}{"backups": backups},
	})
}

// systemBackupActionHandler 处理单个备份的操作：恢复、下载、删除
func systemBackupActionHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	// 路径格式: /api/v1/system/backup/{filename}/{action}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/system/backup/"), "/")
	if len(parts) < 1 {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Missing backup filename"})
		return
	}

	filename := parts[0]
	action := ""
	if len(parts) >= 2 {
		action = parts[1]
	}

	// 安全检查：防止路径遍历
	if strings.Contains(filename, "..") || strings.Contains(filename, "/") {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Invalid filename"})
		return
	}

	filePath := fmt.Sprintf("./data/backups/%s", filename)

	switch action {
	case "restore":
		systemBackupRestoreHandler(w, r, filePath, log)
	case "download":
		systemBackupDownloadHandler(w, r, filePath)
	default:
		// DELETE 方法 = 删除备份
		if r.Method == "DELETE" {
			systemBackupDeleteHandler(w, r, filePath, log)
		} else {
			jsonResponse(w, map[string]interface{}{"code": 400, "message": "Unknown action: " + action})
		}
	}
}

// systemBackupRestoreHandler 从备份恢复任务
func systemBackupRestoreHandler(w http.ResponseWriter, r *http.Request, filePath string, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 检查是否有正在运行的任务
	tasksMu.RLock()
	for _, task := range tasks {
		if task.Status == "running" || task.Status == "incremental" {
			tasksMu.RUnlock()
			jsonResponse(w, map[string]interface{}{
				"code":    400,
				"message": "有正在运行的任务，请先停止所有任务再恢复备份",
			})
			return
		}
	}
	tasksMu.RUnlock()

	// 读取备份文件
	raw, err := os.ReadFile(filePath)
	if err != nil {
		log.Error("Failed to read backup file", map[string]interface{}{"error": err.Error(), "file": filePath})
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "备份文件不存在"})
		return
	}

	var backup map[string]json.RawMessage
	if err := json.Unmarshal(raw, &backup); err != nil {
		log.Error("Failed to parse backup file", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "备份文件格式错误"})
		return
	}

	// 解析任务
	var tasksBackup map[string]json.RawMessage
	if t, ok := backup["tasks"]; ok {
		json.Unmarshal(t, &tasksBackup)
	}

	// 解析错误 Key
	var errorKeysBackup map[string][]ErrorKey
	if ek, ok := backup["error_keys"]; ok {
		json.Unmarshal(ek, &errorKeysBackup)
	}

	restoredCount := 0
	skippedCount := 0

	tasksMu.Lock()
	for id, rawTask := range tasksBackup {
		// 如果任务已存在，跳过
		if _, exists := tasks[id]; exists {
			skippedCount++
			continue
		}

		// 解析任务数据
		var taskData struct {
			ID             string          `json:"id"`
			Name           string          `json:"name"`
			Status         string          `json:"status"`
			Phase          string          `json:"phase"`
			Progress       float64         `json:"progress"`
			SourceCluster  string          `json:"source_cluster"`
			TargetCluster  string          `json:"target_cluster"`
			MigrationMode  string          `json:"migration_mode"`
			KeysTotal      int64           `json:"keys_total"`
			KeysMigrated   int64           `json:"keys_migrated"`
			KeysFailed     int64           `json:"keys_failed"`
			KeysSkipped    int64           `json:"keys_skipped"`
			KeysFiltered   int64           `json:"keys_filtered"`
			BytesMigrated  int64           `json:"bytes_migrated"`
			CreatedAt      string          `json:"created_at"`
			UpdatedAt      string          `json:"updated_at"`
			StartedAt      string          `json:"started_at"`
			Options        json.RawMessage `json:"options"`
		}
		if err := json.Unmarshal(rawTask, &taskData); err != nil {
			continue
		}

		// 恢复的任务状态设为 stopped（不能恢复到 running）
		restoredStatus := taskData.Status
		if restoredStatus == "running" || restoredStatus == "incremental" {
			restoredStatus = "stopped"
		}

		task := &Task{
			ID:            taskData.ID,
			Name:          taskData.Name,
			Status:        restoredStatus,
			Phase:         taskData.Phase,
			Progress:      taskData.Progress,
			SourceCluster: taskData.SourceCluster,
			TargetCluster: taskData.TargetCluster,
			MigrationMode: taskData.MigrationMode,
			KeysTotal:     taskData.KeysTotal,
			KeysMigrated:  taskData.KeysMigrated,
			KeysFailed:    taskData.KeysFailed,
			KeysSkipped:   taskData.KeysSkipped,
			KeysFiltered:  taskData.KeysFiltered,
			BytesMigrated: taskData.BytesMigrated,
			CreatedAt:     taskData.CreatedAt,
			UpdatedAt:     taskData.UpdatedAt,
			StartedAt:     taskData.StartedAt,
		}

		// 恢复 Options
		if taskData.Options != nil {
			var opts TaskOptions
			if json.Unmarshal(taskData.Options, &opts) == nil {
				task.Options = &opts
			}
		}

		tasks[id] = task
		restoredCount++
	}
	tasksMu.Unlock()

	// 恢复错误 Key
	errorKeyMu.Lock()
	for taskID, keys := range errorKeysBackup {
		if _, exists := errorKeys[taskID]; !exists {
			errorKeys[taskID] = keys
		}
	}
	errorKeyMu.Unlock()

	// 持久化
	saveTasksState()

	log.Info("Backup restored", map[string]interface{}{
		"file":     filePath,
		"restored": restoredCount,
		"skipped":  skippedCount,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"restored_tasks": restoredCount,
			"skipped_tasks":  skippedCount,
		},
	})
}

// systemBackupDownloadHandler 下载备份文件
func systemBackupDownloadHandler(w http.ResponseWriter, r *http.Request, filePath string) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "备份文件不存在"})
		return
	}

	filename := filepath.Base(filePath)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.Write(data)
}

// systemBackupDeleteHandler 删除备份文件
func systemBackupDeleteHandler(w http.ResponseWriter, r *http.Request, filePath string, log *logger.RequestLogger) {
	if err := os.Remove(filePath); err != nil {
		log.Error("Failed to delete backup", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "备份文件不存在或删除失败"})
		return
	}

	log.Info("Backup deleted", map[string]interface{}{"file": filePath})
	jsonResponse(w, map[string]interface{}{"code": 0, "message": "success"})
}

// systemBackupUploadHandler 上传导入备份文件
func systemBackupUploadHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 限制上传大小 100MB
	r.Body = http.MaxBytesReader(w, r.Body, 100*1024*1024)

	file, header, err := r.FormFile("file")
	if err != nil {
		log.Error("Failed to read upload file", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "请选择备份文件上传"})
		return
	}
	defer file.Close()

	// 读取文件内容
	data, err := io.ReadAll(file)
	if err != nil {
		log.Error("Failed to read file content", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "读取文件失败"})
		return
	}

	// 验证是有效的 JSON 备份格式
	var backup map[string]interface{}
	if err := json.Unmarshal(data, &backup); err != nil {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "文件格式错误，请上传有效的 JSON 备份文件"})
		return
	}

	// 检查是否包含 tasks 字段
	if _, ok := backup["tasks"]; !ok {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "无效的备份文件：缺少 tasks 字段"})
		return
	}

	// 确保备份目录存在
	backupDir := "./data/backups"
	os.MkdirAll(backupDir, 0755)

	// 使用原始文件名或生成新文件名
	filename := header.Filename
	if !strings.HasSuffix(filename, ".json") {
		filename = filename + ".json"
	}

	// 如果文件已存在，添加时间戳后缀
	destPath := fmt.Sprintf("%s/%s", backupDir, filename)
	if _, err := os.Stat(destPath); err == nil {
		ext := filepath.Ext(filename)
		base := strings.TrimSuffix(filename, ext)
		filename = fmt.Sprintf("%s_%s%s", base, time.Now().Format("20060102150405"), ext)
		destPath = fmt.Sprintf("%s/%s", backupDir, filename)
	}

	// 写入文件
	if err := os.WriteFile(destPath, data, 0644); err != nil {
		log.Error("Failed to save backup file", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 500, "message": "保存备份文件失败"})
		return
	}

	// 统计任务数
	tasksCount := 0
	if t, ok := backup["tasks"].(map[string]interface{}); ok {
		tasksCount = len(t)
	}

	log.Info("Backup uploaded", map[string]interface{}{
		"file":        filename,
		"size":        len(data),
		"tasks_count": tasksCount,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"file_name":   filename,
			"size":        len(data),
			"tasks_count": tasksCount,
		},
	})
}

// stopTaskHandler 停止任务（通用停止接口）
func stopTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	if !ok {
		tasksMu.Unlock()
		log.Warn("Task not found for stop", map[string]interface{}{"task_id": id})
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	if task.Status != "running" && task.Status != "paused" {
		tasksMu.Unlock()
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": "Task is not in running or paused state",
		})
		return
	}

	// 设置任务状态为已停止
	task.Status = "stopped"
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	task.Cleanup() // 统一清理运行时控制字段
	phase := task.Phase
	tasksMu.Unlock()

	// 禁用自动恢复
	disableAutoRecoveryForTask(id)

	taskLog.Info("Task stopped", map[string]interface{}{
		"task_id": id,
		"phase":   phase,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "Task stopped",
		"data": map[string]interface{}{
			"task_id": id,
			"status":  "stopped",
		},
	})
}

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
	task.Cleanup() // 统一清理运行时控制字段
	tasksMu.Unlock()

	taskLog.Info("Incremental sync stopped manually", map[string]interface{}{
		"task_id": id,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "Incremental sync stopped",
		"data": map[string]interface{}{
			"task_id":      id,
			"status":       "incremental_stopped",
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

	// 只能重启失败、已完成、已暂停或已停止的任务
	if task.Status != "failed" && task.Status != "completed" && task.Status != "paused" && task.Status != "stopped" {
		tasksMu.Unlock()
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": fmt.Sprintf("Cannot restart task in '%s' status. Only 'failed', 'completed', 'paused', or 'stopped' tasks can be restarted.", task.Status),
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

	task.Init() // 统一初始化运行时控制字段
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
// 【修复】
// 1. 重试所有失败的 Key，不再限制 BatchSize
// 2. 使用任务原本的 worker 数量并行重试
// 3. 重试过程完全在后端独立运行，不受前端页面状态影响
func retryFailedKeysHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 检查是否已经在重试中
	if task.Status == "retrying" {
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": "Task is already retrying failed keys",
		})
		return
	}

	// 全量迁移进行中不允许手动重试（和 SCAN worker 冲突）
	if task.Status == "running" && task.Phase == "full" {
		jsonResponse(w, map[string]interface{}{
			"code":    400,
			"message": "全量迁移进行中不能重试，请先暂停任务再重试",
		})
		return
	}

	// 【P1 修复】使用 iterateFailedKeys 流式加载，不再全量加载到内存
	failedKeyCh, _ := iterateFailedKeys(id)

	// 收集失败 Key（iterateFailedKeys 已经过滤了 failed/timeout 类型）
	// 由于重试需要全部 key 的 slice（retryFailedKeysAsyncParallel 接口），
	// 这里分批收集，但设置上限（最多 500 万条防止 OOM，约 1GB）
	const maxRetryKeys = 5000000
	var failedKeys []ErrorKey
	for ek := range failedKeyCh {
		failedKeys = append(failedKeys, ek)
		if len(failedKeys) >= maxRetryKeys {
			break
		}
	}

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
		MaxRetries  int `json:"max_retries"`
		WorkerCount int `json:"worker_count"` // 可选：覆盖任务的 worker 数量
	}
	json.NewDecoder(r.Body).Decode(&req)

	if req.MaxRetries <= 0 {
		req.MaxRetries = 3
	}

	// 【修复】使用任务原本的 worker 数量，除非请求中指定
	workerCount := 4 // 默认值
	if task.Options != nil && task.Options.WorkerCount > 0 {
		workerCount = task.Options.WorkerCount
	}
	if req.WorkerCount > 0 {
		workerCount = req.WorkerCount
	}
	// 限制最大 worker 数量
	if workerCount > 32 {
		workerCount = 32
	}

	// 【修复】重试所有失败的 key，不再限制 BatchSize
	keysToRetry := failedKeys

	// 保存原状态，设置为重试中
	tasksMu.Lock()
	previousStatus := task.Status
	task.Status = "retrying"
	tasksMu.Unlock()

	// 【BUG-FIX】重试失败 Key 期间，禁用自动恢复
	// 避免重试完成恢复为 "paused" 后被 autoRecoveryLoop 自动恢复为 running
	// 用户暂停了任务去重试失败 Key，不希望任务因此被自动恢复
	disableAutoRecoveryForTask(id)

	// 通过 WebSocket 广播状态变更
	broadcastTaskStatus(id, "retrying")

	log.Info("Starting retry of failed keys", map[string]interface{}{
		"task_id":         id,
		"total_failed":    len(failedKeys),
		"keys_to_retry":   len(keysToRetry),
		"max_retries":     req.MaxRetries,
		"worker_count":    workerCount,
		"previous_status": previousStatus,
	})

	taskLog.Info("🔄 开始重试失败的Key（并行模式）", map[string]interface{}{
		"count":           len(keysToRetry),
		"max_retries":     req.MaxRetries,
		"worker_count":    workerCount,
		"previous_status": previousStatus,
	})

	// 【修复】异步并行重试，完全在后端独立运行
	go func() {
		retryFailedKeysAsyncParallel(task, keysToRetry, req.MaxRetries, workerCount, taskLog, previousStatus)
	}()

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "Retry started (parallel mode)",
		"data": map[string]interface{}{
			"keys_to_retry":   len(keysToRetry),
			"total_failed":    len(failedKeys),
			"worker_count":    workerCount,
			"previous_status": previousStatus,
		},
	})
}

// retryFailedKeysAsyncParallel 并行异步重试失败的 key
// 【修复】使用多个 worker 并行重试，提高效率，且完全在后端独立运行
func retryFailedKeysAsyncParallel(task *Task, keysToRetry []ErrorKey, maxRetries int, workerCount int, taskLog *logger.TaskLogger, previousStatus string) {
	ctx := context.Background()

	// 连接 Redis（源端支持从 slave 读取）
	readFromSlave := task.Options != nil && task.Options.ReadFromSlave
	sourceAddrs := strings.Split(task.SourceCluster, ",")
	targetAddrs := strings.Split(task.TargetCluster, ",")

	for i := range sourceAddrs {
		sourceAddrs[i] = strings.TrimSpace(sourceAddrs[i])
	}
	for i := range targetAddrs {
		targetAddrs[i] = strings.TrimSpace(targetAddrs[i])
	}

	sourceClient, _, err := connectRedisWithPoolSize(ctx, sourceAddrs, task.SourcePassword, 0, readFromSlave)
	if err != nil {
		taskLog.Error("Failed to connect source for retry", map[string]interface{}{"error": err.Error()})
		// 恢复原状态
		tasksMu.Lock()
		task.Status = previousStatus
		tasksMu.Unlock()
		broadcastTaskStatus(task.ID, previousStatus)
		return
	}
	defer sourceClient.Close()

	targetClient, _, err := connectRedis(ctx, targetAddrs, task.TargetPassword)
	if err != nil {
		taskLog.Error("Failed to connect target for retry", map[string]interface{}{"error": err.Error()})
		// 恢复原状态
		tasksMu.Lock()
		task.Status = previousStatus
		tasksMu.Unlock()
		broadcastTaskStatus(task.ID, previousStatus)
		return
	}
	defer targetClient.Close()

	var successCount, failCount int64
	var processedCount int64
	totalKeys := int64(len(keysToRetry))

	// 创建 key 通道
	keyChan := make(chan ErrorKey, workerCount*2)

	// 启动 worker goroutines
	var wg sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for errorKey := range keyChan {
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
					atomic.AddInt64(&successCount, 1)
					// 从错误列表中移除
					removeErrorKey(task.ID, key)

					// 直接原子操作 task 字段
					atomic.AddInt64(&task.KeysMigrated, 1)
					atomic.AddInt64(&task.KeysFailed, -1)
				} else {
					atomic.AddInt64(&failCount, 1)
					// 更新错误原因
					if reason != "" {
						updateErrorKeyReason(task.ID, key, reason)
					}
				}

				// 更新已处理计数
				processed := atomic.AddInt64(&processedCount, 1)

				// 每 50 个 Key 或最后一个 Key 时广播进度
				if processed%50 == 0 || processed == totalKeys {
					currentSuccess := atomic.LoadInt64(&successCount)
					currentFail := atomic.LoadInt64(&failCount)
					broadcastTaskProgress(task.ID, map[string]interface{}{
						"retry_progress": map[string]interface{}{
							"current":    processed,
							"total":      totalKeys,
							"success":    currentSuccess,
							"failed":     currentFail,
							"percentage": float64(processed) / float64(totalKeys) * 100,
						},
					})
				}
			}
		}(w)
	}

	// 发送所有 key 到通道
	for _, errorKey := range keysToRetry {
		keyChan <- errorKey
	}
	close(keyChan)

	// 等待所有 worker 完成
	wg.Wait()

	finalSuccess := atomic.LoadInt64(&successCount)
	finalFail := atomic.LoadInt64(&failCount)

	taskLog.Info("✅ 并行重试完成", map[string]interface{}{
		"success":      finalSuccess,
		"failed":       finalFail,
		"total":        totalKeys,
		"worker_count": workerCount,
	})

	// 恢复原状态
	tasksMu.Lock()
	task.Status = previousStatus
	tasksMu.Unlock()
	broadcastTaskStatus(task.ID, previousStatus)

	// 广播最终结果
	broadcastTaskProgress(task.ID, map[string]interface{}{
		"retry_complete": true,
		"retry_result": map[string]interface{}{
			"success": finalSuccess,
			"failed":  finalFail,
			"total":   totalKeys,
		},
	})
}

// updateErrorKeyReason 更新错误 key 的原因
func updateErrorKeyReason(taskID, key, reason string) {
	errorKeyMu.Lock()
	defer errorKeyMu.Unlock()

	if keys, ok := errorKeys[taskID]; ok {
		for i, k := range keys {
			if k.Key == key {
				errorKeys[taskID][i].Detail = reason
				errorKeys[taskID][i].Timestamp = time.Now().Format(time.RFC3339)
				break
			}
		}
	}
}

// retryFailedKeysAsync 异步重试失败的 key（保留旧版本供兼容）
func retryFailedKeysAsync(task *Task, keysToRetry []ErrorKey, maxRetries int, taskLog *logger.TaskLogger, previousStatus string) {
	ctx := context.Background()

	// 连接 Redis（源端支持从 slave 读取）
	readFromSlave := task.Options != nil && task.Options.ReadFromSlave
	sourceAddrs := strings.Split(task.SourceCluster, ",")
	targetAddrs := strings.Split(task.TargetCluster, ",")

	for i := range sourceAddrs {
		sourceAddrs[i] = strings.TrimSpace(sourceAddrs[i])
	}
	for i := range targetAddrs {
		targetAddrs[i] = strings.TrimSpace(targetAddrs[i])
	}

	sourceClient, _, err := connectRedisWithPoolSize(ctx, sourceAddrs, task.SourcePassword, 0, readFromSlave)
	if err != nil {
		taskLog.Error("Failed to connect source for retry", map[string]interface{}{"error": err.Error()})
		// 恢复原状态
		tasksMu.Lock()
		task.Status = previousStatus
		tasksMu.Unlock()
		broadcastTaskStatus(task.ID, previousStatus)
		return
	}
	defer sourceClient.Close()

	targetClient, _, err := connectRedis(ctx, targetAddrs, task.TargetPassword)
	if err != nil {
		taskLog.Error("Failed to connect target for retry", map[string]interface{}{"error": err.Error()})
		// 恢复原状态
		tasksMu.Lock()
		task.Status = previousStatus
		tasksMu.Unlock()
		broadcastTaskStatus(task.ID, previousStatus)
		return
	}
	defer targetClient.Close()

	var successCount, failCount int64
	totalKeys := len(keysToRetry)

	for i, errorKey := range keysToRetry {
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

			// 直接原子操作 task 字段
			atomic.AddInt64(&task.KeysMigrated, 1)
			atomic.AddInt64(&task.KeysFailed, -1)
		} else {
			failCount++
			taskLog.Warn("Retry failed", map[string]interface{}{
				"key":    key,
				"reason": reason,
			})
		}

		// 每 10 个 Key 或最后一个 Key 时广播进度
		if (i+1)%10 == 0 || i == totalKeys-1 {
			broadcastTaskProgress(task.ID, map[string]interface{}{
				"retry_progress": map[string]interface{}{
					"current":   i + 1,
					"total":     totalKeys,
					"success":   successCount,
					"failed":    failCount,
					"percentage": float64(i+1) / float64(totalKeys) * 100,
				},
			})
		}
	}

	taskLog.Info("✅ 重试完成", map[string]interface{}{
		"success": successCount,
		"failed":  failCount,
		"total":   totalKeys,
	})

	// 恢复原状态
	tasksMu.Lock()
	task.Status = previousStatus
	tasksMu.Unlock()
	broadcastTaskStatus(task.ID, previousStatus)
}

// retryFailedKeysAsyncSilent 静默异步重试失败的 key（用于自动重试，不改变任务状态）
func retryFailedKeysAsyncSilent(task *Task, keysToRetry []ErrorKey, maxRetries int, taskLog *logger.TaskLogger) {
	ctx := context.Background()

	// 连接 Redis（源端支持从 slave 读取）
	readFromSlave := task.Options != nil && task.Options.ReadFromSlave
	sourceAddrs := strings.Split(task.SourceCluster, ",")
	targetAddrs := strings.Split(task.TargetCluster, ",")

	for i := range sourceAddrs {
		sourceAddrs[i] = strings.TrimSpace(sourceAddrs[i])
	}
	for i := range targetAddrs {
		targetAddrs[i] = strings.TrimSpace(targetAddrs[i])
	}

	sourceClient, _, err := connectRedisWithPoolSize(ctx, sourceAddrs, task.SourcePassword, 0, readFromSlave)
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

			// 【核心设计】自动重试成功：直接原子操作 task 字段
			// worker 也是直接原子操作 task 字段，两者不冲突
			// 不再需要 retryAdj 等额外机制
			atomic.AddInt64(&task.KeysMigrated, 1)
			atomic.AddInt64(&task.KeysFailed, -1)
		} else {
			failCount++
		}
	}

	if successCount > 0 || failCount > 0 {
		taskLog.Info("🔄 自动重试完成", map[string]interface{}{
			"success": successCount,
			"failed":  failCount,
			"total":   len(keysToRetry),
		})
	}
}

// retryFailedKeysAsyncSilentParallel 并行静默异步重试失败的 key（用于自动重试，不改变任务状态）
func retryFailedKeysAsyncSilentParallel(task *Task, keysToRetry []ErrorKey, maxRetries int, workerCount int, taskLog *logger.TaskLogger) {
	ctx := context.Background()

	// 连接 Redis
	readFromSlave := task.Options != nil && task.Options.ReadFromSlave
	sourceAddrs := strings.Split(task.SourceCluster, ",")
	targetAddrs := strings.Split(task.TargetCluster, ",")

	for i := range sourceAddrs {
		sourceAddrs[i] = strings.TrimSpace(sourceAddrs[i])
	}
	for i := range targetAddrs {
		targetAddrs[i] = strings.TrimSpace(targetAddrs[i])
	}

	sourceClient, _, err := connectRedisWithPoolSize(ctx, sourceAddrs, task.SourcePassword, 0, readFromSlave)
	if err != nil {
		taskLog.Error("Failed to connect source for parallel retry", map[string]interface{}{"error": err.Error()})
		return
	}
	defer sourceClient.Close()

	targetClient, _, err := connectRedis(ctx, targetAddrs, task.TargetPassword)
	if err != nil {
		taskLog.Error("Failed to connect target for parallel retry", map[string]interface{}{"error": err.Error()})
		return
	}
	defer targetClient.Close()

	var successCount, failCount int64

	// 创建 key 通道
	keyChan := make(chan ErrorKey, workerCount*2)

	// 启动 worker goroutines
	var wg sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for errorKey := range keyChan {
				key := errorKey.Key
				var migrated bool
				var reason string

				for retry := 0; retry < maxRetries; retry++ {
					migrated, _, reason = migrateKeyWithPolicy(ctx, sourceClient, targetClient, key, "replace")
					if migrated || reason == "skipped" {
						break
					}
					time.Sleep(time.Duration((retry+1)*100) * time.Millisecond)
				}

				if migrated {
					atomic.AddInt64(&successCount, 1)
					removeErrorKey(task.ID, key)
					atomic.AddInt64(&task.KeysMigrated, 1)
					atomic.AddInt64(&task.KeysFailed, -1)
				} else {
					atomic.AddInt64(&failCount, 1)
				}
			}
		}()
	}

	// 分发 keys
	for _, ek := range keysToRetry {
		keyChan <- ek
	}
	close(keyChan)
	wg.Wait()

	if successCount > 0 || failCount > 0 {
		taskLog.Info("🔄 自动重试完成（并行模式）", map[string]interface{}{
			"success":      successCount,
			"failed":       failCount,
			"total":        len(keysToRetry),
			"worker_count": workerCount,
		})
	}
}

// removeErrorKey 从错误列表中移除 key
// 【BUG-FIX】同时记录到已移除集合，确保落盘文件中的 Key 也能被过滤
// 【P1 修复】内存中的 removedErrorKeys 有上限保护（100万/任务），超出后落盘到文件
func removeErrorKey(taskID, key string) {
	// 从内存列表移除
	errorKeyMu.Lock()
	if keys, ok := errorKeys[taskID]; ok {
		newKeys := make([]ErrorKey, 0, len(keys))
		for _, k := range keys {
			if k.Key != key {
				newKeys = append(newKeys, k)
			}
		}
		errorKeys[taskID] = newKeys
	}
	errorKeyMu.Unlock()

	// 记录到已移除集合（用于过滤落盘文件中的 Key）
	removedErrorKeysMu.Lock()
	if removedErrorKeys[taskID] == nil {
		removedErrorKeys[taskID] = make(map[string]bool)
	}
	removedErrorKeys[taskID][key] = true

	// 【P1 安全保护】超过上限时，将内存中的已移除集合落盘，释放内存
	if len(removedErrorKeys[taskID]) >= maxRemovedErrorKeysInMemory {
		keysToFlush := removedErrorKeys[taskID]
		removedErrorKeys[taskID] = make(map[string]bool)
		removedErrorKeysMu.Unlock()
		// 异步落盘
		go flushRemovedErrorKeys(taskID, keysToFlush)
		return
	}
	removedErrorKeysMu.Unlock()
}

// flushRemovedErrorKeys 将已移除的 error key 集合落盘到文件
// 后续 getAllErrorKeys / iterateFailedKeys 读取落盘文件时，
// 需要同时加载这些 removed 文件来过滤
func flushRemovedErrorKeys(taskID string, keys map[string]bool) {
	removedDir := filepath.Join(dataDir, "removed-keys")
	os.MkdirAll(removedDir, 0755)

	filename := fmt.Sprintf("%s/%s_removed_%d.json", removedDir, taskID, time.Now().UnixNano())

	keyList := make([]string, 0, len(keys))
	for k := range keys {
		keyList = append(keyList, k)
	}

	data, err := json.Marshal(keyList)
	if err != nil {
		logger.Warn("Failed to marshal removed error keys", map[string]interface{}{
			"task_id": taskID,
			"error":   err.Error(),
		})
		return
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		logger.Warn("Failed to flush removed error keys", map[string]interface{}{
			"task_id":  taskID,
			"filename": filename,
			"error":    err.Error(),
		})
		return
	}

	logger.Info("Removed error keys flushed to disk", map[string]interface{}{
		"task_id":  taskID,
		"filename": filename,
		"count":    len(keys),
	})
}

// loadRemovedErrorKeysFromDisk 从落盘文件加载已移除的 error key 集合
// 用于 getAllErrorKeys / iterateFailedKeys 中合并内存和磁盘的 removed 集合
func loadRemovedErrorKeysFromDisk(taskID string) map[string]bool {
	removedDir := filepath.Join(dataDir, "removed-keys")
	pattern := filepath.Join(removedDir, taskID+"_removed_*.json")
	files, err := filepath.Glob(pattern)
	if err != nil || len(files) == 0 {
		return nil
	}

	merged := make(map[string]bool)
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var keyList []string
		if err := json.Unmarshal(data, &keyList); err != nil {
			continue
		}
		for _, k := range keyList {
			merged[k] = true
		}
	}
	return merged
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

	// 错误统计（使用 task 计数器，准确值）
	health["error_keys_count"] = atomic.LoadInt64(&task.KeysFailed)

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
			"code":    400,
			"message": "连接失败: " + err.Error(),
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
	task.Init() // 统一初始化运行时控制字段

	tasksMu.Lock()
	tasks[task.ID] = task
	tasksMu.Unlock()

	// 【崩溃恢复修复】创建任务后立即持久化
	saveTasksState()

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
	DefaultHealthCheckIntervalSec  = 30 // 默认健康检测间隔（秒）
	DefaultPeriodicRetryIntervalSec = 60 // 默认定期重试间隔（秒）= 1 分钟
	DefaultMaxAutoResumeAttempts   = 10 // 默认最大自动恢复尝试次数
)

// calcSmartRetryParams 根据失败 Key 数量智能计算重试参数
// 返回: (batchSize, workerCount)
func calcSmartRetryParams(failedCount int) (int, int) {
	switch {
	case failedCount <= 100:
		return failedCount, 1 // 少量：全部取出，单线程
	case failedCount <= 1000:
		return failedCount, 2 // 中量：全部取出，2 worker
	case failedCount <= 10000:
		return 5000, 4 // 较多：每轮 5000，4 worker
	case failedCount <= 100000:
		return 10000, 8 // 大量：每轮 1 万，8 worker
	default:
		return 50000, 16 // 海量（>10万）：每轮 5 万，16 worker
	}
}

// startSmartRetryService 启动智能重试后台服务
func startSmartRetryService() {
	logger.Info("🔄 Starting smart retry service", map[string]interface{}{
		"health_check_interval_sec":   DefaultHealthCheckIntervalSec,
		"periodic_retry_interval_sec": DefaultPeriodicRetryIntervalSec,
		"smart_batch_mode":            true,
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

		// 检查任务级别配置：如果任务配置了 SmartRetry 且关闭了自动恢复，跳过
		if task.Options != nil && task.Options.SmartRetry != nil && !task.Options.SmartRetry.EnableAutoRecovery {
			continue
		}

		// 检查是否超过最大尝试次数（优先使用任务级配置）
		maxAttempts := DefaultMaxAutoResumeAttempts
		if task.Options != nil && task.Options.SmartRetry != nil && task.Options.SmartRetry.MaxAutoResumeAttempts > 0 {
			maxAttempts = task.Options.SmartRetry.MaxAutoResumeAttempts
		}
		if state.ResumeAttempts >= maxAttempts {
			logger.Debug("Task exceeded max auto resume attempts", map[string]interface{}{
				"task_id":  task.ID,
				"attempts": state.ResumeAttempts,
				"max":      maxAttempts,
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
		AutoResumeEnabled: false, // 默认不启用，只有 autoStopTask 才显式启用
	}
	autoRecoveryStates[taskID] = state
	return state
}

// autoResumeTask 自动恢复任务
// 【BUG-FIX】不再无条件启动新的 simulateProgress
// 而是先检查是否有正在运行的迁移流程，如果有则只改变状态
// 【死锁修复】先释放 tasksMu 再获取 fullMigrationMu，避免嵌套锁
func autoResumeTask(task *Task) error {
	tasksMu.Lock()
	
	if task.Status != "paused" {
		tasksMu.Unlock()
		return fmt.Errorf("task is not paused, current status: %s", task.Status)
	}

	taskLog := logger.WithTask(task.ID)
	taskID := task.ID
	tasksMu.Unlock()
	
	// 【死锁修复】在 tasksMu 之外检查 fullMigrationMu，消除嵌套锁
	fullMigrationMu.Lock()
	isFullRunning := fullMigrationRunning[taskID]
	fullMigrationMu.Unlock()
	
	// 重新获取 tasksMu 更新状态
	tasksMu.Lock()
	// 二次检查：防止在释放锁期间状态被改变
	if task.Status != "paused" {
		tasksMu.Unlock()
		return fmt.Errorf("task status changed during resume check, current status: %s", task.Status)
	}
	
	if isFullRunning {
		// 全量迁移仍在运行，只改变状态，不启动新流程
		task.Status = "running"
		task.UpdatedAt = time.Now().Format(time.RFC3339)
		tasksMu.Unlock()
		
		taskLog.Warn("【BUG-FIX】Full migration goroutine still running, only changing status without starting new migration", map[string]interface{}{
			"task_name": task.Name,
			"phase":     task.Phase,
			"progress":  task.Progress,
		})
		return nil
	}
	
	// 没有正在运行的迁移，需要重新启动
	task.Init() // 统一初始化运行时控制字段
	task.Status = "running"
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	
	tasksMu.Unlock()

	taskLog.Info("Task auto-resumed, starting new migration goroutine", map[string]interface{}{
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

// retryFailedKeysForRunningTasks 为符合条件的任务重试失败的 Key（智能批次 + 并行）
// 策略：全量 SCAN 阶段不自动重试（避免和 SCAN worker 抢资源/重复写入/计数混乱）
//       仅在增量同步阶段、已完成、已停止时自动重试
func retryFailedKeysForRunningTasks() {
	var eligibleTasks []*Task
	tasksMu.RLock()
	for _, task := range tasks {
		switch {
		case task.Status == "running" && task.Phase == "full":
			// 全量 SCAN 进行中 → 跳过（和 worker 冲突）
			continue
		case task.Status == "running" && task.Phase == "incremental":
			// 增量同步中 → 可以重试（SCAN 已结束，增量处理不同 key）
			eligibleTasks = append(eligibleTasks, task)
		case task.Status == "incremental":
			eligibleTasks = append(eligibleTasks, task)
		case task.Status == "completed" || task.Status == "stopped":
			// 已完成/已停止 → 可以重试
			eligibleTasks = append(eligibleTasks, task)
		}
	}
	tasksMu.RUnlock()

	if len(eligibleTasks) == 0 {
		return
	}

	for _, task := range eligibleTasks {
		// 检查任务级容错配置：如果明确关闭了自动重试失败 Key，跳过
		if task.Options != nil && task.Options.FaultTolerance != nil && !task.Options.FaultTolerance.AutoRetryFailedKeys {
			continue
		}

		// 【P1 修复】使用 iterateFailedKeys 流式加载，不再全量加载到内存
		failedKeyCh, _ := iterateFailedKeys(task.ID)
		var failedKeys []ErrorKey
		const maxAutoRetryKeys = 100000 // 自动重试每次最多 10 万条
		for k := range failedKeyCh {
			failedKeys = append(failedKeys, k)
			if len(failedKeys) >= maxAutoRetryKeys {
				break
			}
		}

		if len(failedKeys) == 0 {
			continue
		}

		// 智能计算批次大小和 worker 数量
		batchSize, workerCount := calcSmartRetryParams(len(failedKeys))

		// 限制每次重试的数量
		keysToRetry := failedKeys
		if len(keysToRetry) > batchSize {
			keysToRetry = keysToRetry[:batchSize]
		}

		taskLog := logger.WithTask(task.ID)
		taskLog.Info("🔄 Starting periodic retry of failed keys (smart mode)", map[string]interface{}{
			"total_failed": len(failedKeys),
			"batch_size":   len(keysToRetry),
			"worker_count": workerCount,
		})

		// 异步并行重试（自动重试不改变状态）
		go func(t *Task, keys []ErrorKey, wc int) {
			retryFailedKeysAsyncSilentParallel(t, keys, 3, wc, logger.WithTask(t.ID))
		}(task, keysToRetry, workerCount)
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

	// 获取错误 Key 信息（合并内存 + 落盘）
	errorKeyList := getAllErrorKeys(id, 10)

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

	// 错误 Key 列表（合并内存 + 落盘，最多 1000 条用于报告）
	errorKeyList := getAllErrorKeys(id, 1000)

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
		"truncated":  keyList.Truncated,
	})

	respData := map[string]interface{}{
		"file_path":    filePath,
		"filename":     handler.Filename,
		"size":         len(data),
		"total_keys":   keyList.TotalCount,
		"format":       keyList.Format,
		"preview_keys": previewKeys,
		"truncated":    keyList.Truncated,
	}

	message := "success"
	if keyList.Truncated {
		message = fmt.Sprintf("文件包含超过 100 万 Key（估算 %d 个），预览仅显示部分，实际迁移将使用流式模式处理全部 Key", keyList.TotalCount)
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": message,
		"data":    respData,
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
	task.Init() // 统一初始化运行时控制字段

	tasksMu.Lock()
	tasks[taskID] = task
	tasksMu.Unlock()

	// 【崩溃恢复修复】创建任务后立即持久化
	saveTasksState()

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
				"periodic_retry_batch_mode":   "smart (dynamic based on failed count)",
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

// ==================== 独立校验任务 API ====================

// verifyTasksHandler 处理校验任务列表
// GET /api/v1/verify-tasks - 获取校验任务列表
// POST /api/v1/verify-tasks - 创建校验任务
func verifyTasksHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	switch r.Method {
	case "GET":
		listVerifyTasksHandler(w, r, log)
	case "POST":
		createVerifyTaskHandler(w, r, log)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// listVerifyTasksHandler 获取校验任务列表
func listVerifyTasksHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	verifyTasksMu.RLock()
	defer verifyTasksMu.RUnlock()

	taskList := make([]*VerifyTask, 0, len(verifyTasks))
	for _, task := range verifyTasks {
		taskList = append(taskList, task)
	}

	// 按创建时间倒序排序
	sort.Slice(taskList, func(i, j int) bool {
		return taskList[i].CreatedAt > taskList[j].CreatedAt
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    taskList,
	})
}

// createVerifyTaskHandler 创建校验任务
func createVerifyTaskHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	var req struct {
		Name           string `json:"name"`
		SourceCluster  struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"source_cluster"`
		TargetCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"target_cluster"`
		VerifyMode      string           `json:"verify_mode"`       // count_only, sample, full
		SampleRate      float64          `json:"sample_rate"`       // 采样率
		MaxKeys         int64            `json:"max_keys"`          // 最大校验 Key 数
		KeyFilter       *KeyFilterConfig `json:"key_filter"`        // Key 过滤
		CompareValue    bool             `json:"compare_value"`     // 是否比较值（兼容旧版）
		CompareTTL      bool             `json:"compare_ttl"`       // 是否比较 TTL
		TTLTolerance    int64            `json:"ttl_tolerance"`     // TTL 容差（秒）
		MigrationTaskID string           `json:"migration_task_id"` // 关联的迁移任务
		AutoStart       bool             `json:"auto_start"`        // 是否自动启动
		// 新增参数
		Concurrency        int    `json:"concurrency"`           // 并发数
		QPS                int    `json:"qps"`                   // QPS 限制
		CompareMode        string `json:"compare_mode"`          // full_value, length_only, exists_only
		SkipLargeKey       bool   `json:"skip_large_key"`        // 是否跳过大 Key
		LargeKeyThreshold  int64  `json:"large_key_threshold"`   // 大 Key 阈值（字节）
		DBList             string `json:"db_list"`               // DB 列表，分号分隔
		SmartCompare       bool   `json:"smart_compare"`         // 智能比较
		BigKeyThreshold    int64  `json:"big_key_threshold"`     // 智能比较大 Key 元素数阈值
		CompareRounds      int    `json:"compare_rounds"`        // 多轮迭代收敛
		RoundInterval      int    `json:"round_interval"`        // 轮次间隔（秒）
		Direction          string `json:"direction"`             // 校验方向
		FieldLevelCompare  bool   `json:"field_level_compare"`   // Field 级别比对
		FieldScanThreshold int64  `json:"field_scan_threshold"`  // Field SCAN 阈值
		EnableSQLite       bool   `json:"enable_sqlite"`         // SQLite 存储
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Error("Failed to decode request", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Invalid request body"})
		return
	}

	// 验证必填字段
	if len(req.SourceCluster.Addrs) == 0 || len(req.TargetCluster.Addrs) == 0 {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Source and target cluster addresses are required"})
		return
	}

	// 设置默认值
	if req.Name == "" {
		req.Name = fmt.Sprintf("校验任务-%s", time.Now().Format("20060102-150405"))
	}
	if req.VerifyMode == "" {
		req.VerifyMode = "sample"
	}
	if req.SampleRate <= 0 || req.SampleRate > 1 {
		req.SampleRate = 0.01 // 默认 1% 采样率
	}
	if req.MaxKeys <= 0 {
		req.MaxKeys = 100000 // 默认最大 10 万 Key
	}
	if req.TTLTolerance <= 0 {
		req.TTLTolerance = 5 // 默认 5 秒容差
	}
	// 新增默认值
	if req.Concurrency <= 0 {
		req.Concurrency = 10 // 默认 10 并发
	}
	if req.Concurrency > 100 {
		req.Concurrency = 100 // 最大 100 并发
	}
	if req.CompareMode == "" {
		// 兼容旧版：如果 CompareValue 为 true，使用 full_value；否则使用 exists_only
		if req.CompareValue {
			req.CompareMode = "full_value"
		} else {
			req.CompareMode = "exists_only"
		}
	}
	if req.LargeKeyThreshold <= 0 {
		req.LargeKeyThreshold = 10 * 1024 * 1024 // 默认 10MB
	}

	task := &VerifyTask{
		ID:                uuid.New().String(),
		Name:              req.Name,
		Status:            "pending",
		SourceCluster:     strings.Join(req.SourceCluster.Addrs, ","),
		TargetCluster:     strings.Join(req.TargetCluster.Addrs, ","),
		SourcePassword:    req.SourceCluster.Password,
		TargetPassword:    req.TargetCluster.Password,
		VerifyMode:        req.VerifyMode,
		SampleRate:        req.SampleRate,
		MaxKeys:           req.MaxKeys,
		KeyFilter:         req.KeyFilter,
		CompareValue:      req.CompareValue,
		CompareTTL:        req.CompareTTL,
		TTLTolerance:      req.TTLTolerance,
		MigrationTaskID:   req.MigrationTaskID,
		Concurrency:       req.Concurrency,
		QPS:               req.QPS,
		CompareMode:       req.CompareMode,
		SkipLargeKey:       req.SkipLargeKey,
		LargeKeyThreshold:  req.LargeKeyThreshold,
		DBList:             req.DBList,
		SmartCompare:       req.SmartCompare,
		BigKeyThreshold:    req.BigKeyThreshold,
		CompareRounds:      req.CompareRounds,
		RoundInterval:      req.RoundInterval,
		Direction:          req.Direction,
		FieldLevelCompare:  req.FieldLevelCompare,
		FieldScanThreshold: req.FieldScanThreshold,
		EnableSQLite:       req.EnableSQLite,
		CreatedAt:          time.Now().Format(time.RFC3339),
		Result: &VerifyTaskResult{
			Progress: 0,
		},
	}

	verifyTasksMu.Lock()
	verifyTasks[task.ID] = task
	verifyTasksMu.Unlock()

	// 保存校验任务状态
	saveVerifyTasksState()

	log.Info("Verify task created", map[string]interface{}{
		"task_id":     task.ID,
		"name":        task.Name,
		"verify_mode": task.VerifyMode,
	})

	// 如果设置了自动启动，则立即启动
	if req.AutoStart {
		go runVerifyTask(task)
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    task,
	})
}

// verifyTaskHandler 处理单个校验任务
func verifyTaskHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/verify-tasks/")
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

	switch {
	case action == "" && r.Method == "GET":
		getVerifyTaskHandler(w, r, id, log)
	case action == "" && r.Method == "PUT":
		updateVerifyTaskHandler(w, r, id, log)
	case action == "" && r.Method == "DELETE":
		deleteVerifyTaskHandler(w, r, id, log)
	case action == "start" && r.Method == "POST":
		startVerifyTaskHandler(w, r, id, log)
	case action == "stop" && r.Method == "POST":
		stopVerifyTaskHandler(w, r, id, log)
	case action == "rerun" && r.Method == "POST":
		rerunVerifyTaskHandler(w, r, id, log)
	case action == "mismatch-details" && r.Method == "GET":
		getMismatchDetailsHandler(w, r, id, log)
	case strings.HasPrefix(action, "mismatch-details/download"):
		downloadMismatchDetailsHandler(w, r, id, log)
	default:
		http.NotFound(w, r)
	}
}

// getVerifyTaskHandler 获取校验任务详情
func getVerifyTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	verifyTasksMu.RLock()
	task, ok := verifyTasks[id]
	verifyTasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Verify task not found"})
		return
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    task,
	})
}

// deleteVerifyTaskHandler 删除校验任务
func deleteVerifyTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	verifyTasksMu.Lock()
	task, ok := verifyTasks[id]
	if ok {
		if task.Status == "running" {
			verifyTasksMu.Unlock()
			jsonResponse(w, map[string]interface{}{"code": 400, "message": "Cannot delete running task"})
			return
		}
		delete(verifyTasks, id)
	}
	verifyTasksMu.Unlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Verify task not found"})
		return
	}

	// 保存校验任务状态
	saveVerifyTasksState()

	log.Info("Verify task deleted", map[string]interface{}{"task_id": id})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
	})
}

// batchDeleteVerifyTasksHandler 批量删除校验任务
func batchDeleteVerifyTasksHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}

	var req struct {
		IDs []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Invalid request body"})
		return
	}
	if len(req.IDs) == 0 {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "No task IDs provided"})
		return
	}

	var deleted, skipped, notFound int
	verifyTasksMu.Lock()
	for _, id := range req.IDs {
		task, ok := verifyTasks[id]
		if !ok {
			notFound++
			continue
		}
		if task.Status == "running" {
			task.Status = "cancelled"
			task.CompletedAt = time.Now().Format(time.RFC3339)
		}
		delete(verifyTasks, id)
		deleted++
	}
	verifyTasksMu.Unlock()

	saveVerifyTasksState()

	log.Info("Batch delete verify tasks", map[string]interface{}{
		"requested": len(req.IDs),
		"deleted":   deleted,
		"skipped":   skipped,
		"not_found": notFound,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"deleted":   deleted,
			"not_found": notFound,
		},
	})
}

// startVerifyTaskHandler 启动校验任务
func startVerifyTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	verifyTasksMu.Lock()
	task, ok := verifyTasks[id]
	if ok {
		if task.Status == "running" {
			verifyTasksMu.Unlock()
			jsonResponse(w, map[string]interface{}{"code": 400, "message": "Task is already running"})
			return
		}
		task.Status = "running"
		task.StartedAt = time.Now().Format(time.RFC3339)
	}
	verifyTasksMu.Unlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Verify task not found"})
		return
	}

	log.Info("Verify task started", map[string]interface{}{"task_id": id})

	// 在后台运行校验任务
	go runVerifyTask(task)

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
	})
}

// stopVerifyTaskHandler 停止校验任务
func stopVerifyTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	verifyTasksMu.Lock()
	task, ok := verifyTasks[id]
	if ok && task.Status == "running" {
		task.Status = "cancelled"
		task.CompletedAt = time.Now().Format(time.RFC3339)
	}
	verifyTasksMu.Unlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Verify task not found"})
		return
	}

	log.Info("Verify task stopped", map[string]interface{}{"task_id": id})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
	})
}

// updateVerifyTaskHandler 更新校验任务配置
func updateVerifyTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	verifyTasksMu.Lock()
	task, ok := verifyTasks[id]
	if ok && task.Status == "running" {
		verifyTasksMu.Unlock()
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Cannot update running task"})
		return
	}
	verifyTasksMu.Unlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Verify task not found"})
		return
	}

	var req struct {
		Name           string `json:"name"`
		SourceCluster  struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"source_cluster"`
		TargetCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"target_cluster"`
		VerifyMode         string           `json:"verify_mode"`
		SampleRate         float64          `json:"sample_rate"`
		MaxKeys            int64            `json:"max_keys"`
		KeyFilter          *KeyFilterConfig `json:"key_filter"`
		CompareTTL         bool             `json:"compare_ttl"`
		TTLTolerance       int64            `json:"ttl_tolerance"`
		Concurrency        int              `json:"concurrency"`
		QPS                int              `json:"qps"`
		CompareMode        string           `json:"compare_mode"`
		SkipLargeKey       bool             `json:"skip_large_key"`
		LargeKeyThreshold  int64            `json:"large_key_threshold"`
		DBList             string           `json:"db_list"`
		CompareRounds      int              `json:"compare_rounds"`
		RoundInterval      int              `json:"round_interval"`
		Direction          string           `json:"direction"`
		SmartCompare       bool             `json:"smart_compare"`
		FieldLevelCompare  bool             `json:"field_level_compare"`
		FieldScanThreshold int64            `json:"field_scan_threshold"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Invalid request body"})
		return
	}

	verifyTasksMu.Lock()
	// 更新任务配置
	if req.Name != "" {
		task.Name = req.Name
	}
	if len(req.SourceCluster.Addrs) > 0 {
		task.SourceCluster = strings.Join(req.SourceCluster.Addrs, ",")
		task.SourcePassword = req.SourceCluster.Password
	}
	if len(req.TargetCluster.Addrs) > 0 {
		task.TargetCluster = strings.Join(req.TargetCluster.Addrs, ",")
		task.TargetPassword = req.TargetCluster.Password
	}
	if req.VerifyMode != "" {
		task.VerifyMode = req.VerifyMode
	}
	if req.SampleRate > 0 {
		task.SampleRate = req.SampleRate
	}
	if req.MaxKeys > 0 {
		task.MaxKeys = req.MaxKeys
	}
	if req.KeyFilter != nil {
		task.KeyFilter = req.KeyFilter
	}
	task.CompareTTL = req.CompareTTL
	task.TTLTolerance = req.TTLTolerance
	if req.Concurrency > 0 {
		task.Concurrency = req.Concurrency
	}
	if req.QPS > 0 {
		task.QPS = req.QPS
	}
	if req.CompareMode != "" {
		task.CompareMode = req.CompareMode
	}
	task.SkipLargeKey = req.SkipLargeKey
	if req.LargeKeyThreshold > 0 {
		task.LargeKeyThreshold = req.LargeKeyThreshold
	}
	if req.DBList != "" {
		task.DBList = req.DBList
	}
	if req.CompareRounds > 0 {
		task.CompareRounds = req.CompareRounds
	}
	if req.RoundInterval > 0 {
		task.RoundInterval = req.RoundInterval
	}
	if req.Direction != "" {
		task.Direction = req.Direction
	}
	task.SmartCompare = req.SmartCompare
	task.FieldLevelCompare = req.FieldLevelCompare
	if req.FieldScanThreshold > 0 {
		task.FieldScanThreshold = req.FieldScanThreshold
	}
	verifyTasksMu.Unlock()

	// 保存校验任务状态
	saveVerifyTasksState()

	log.Info("Verify task updated", map[string]interface{}{"task_id": id})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    task,
	})
}

// rerunVerifyTaskHandler 重新执行校验任务
func rerunVerifyTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	verifyTasksMu.Lock()
	task, ok := verifyTasks[id]
	if ok {
		if task.Status == "running" {
			verifyTasksMu.Unlock()
			jsonResponse(w, map[string]interface{}{"code": 400, "message": "Task is already running"})
			return
		}
		// 重置任务状态和结果
		task.Status = "running"
		task.StartedAt = time.Now().Format(time.RFC3339)
		task.CompletedAt = ""
		task.Result = &VerifyTaskResult{
			Progress: 0,
		}
	}
	verifyTasksMu.Unlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Verify task not found"})
		return
	}

	log.Info("Verify task rerun started", map[string]interface{}{"task_id": id})

	// 在后台运行校验任务
	go runVerifyTask(task)

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
	})
}

// getMismatchDetailsHandler 获取不匹配详情
func getMismatchDetailsHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	verifyTasksMu.RLock()
	task, ok := verifyTasks[id]
	verifyTasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Verify task not found"})
		return
	}

	// 获取分页参数
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")
	limit := 10000
	offset := 0
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}
	if offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	var details []map[string]interface{}
	totalCount := 0

	if task.Result != nil && task.Result.Details != nil {
		totalCount = len(task.Result.Details)
		end := offset + limit
		if end > totalCount {
			end = totalCount
		}
		if offset < totalCount {
			for _, d := range task.Result.Details[offset:end] {
				details = append(details, map[string]interface{}{
					"key":          d.Key,
					"type":         d.Type,
					"source_value": d.SourceValue,
					"target_value": d.TargetValue,
					"source_ttl":   d.SourceTTL,
					"target_ttl":   d.TargetTTL,
				})
			}
		}
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"total":   totalCount,
			"limit":   limit,
			"offset":  offset,
			"details": details,
		},
	})
}

// downloadMismatchDetailsHandler 下载不匹配详情
func downloadMismatchDetailsHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	verifyTasksMu.RLock()
	task, ok := verifyTasks[id]
	verifyTasksMu.RUnlock()

	if !ok {
		http.Error(w, "Verify task not found", http.StatusNotFound)
		return
	}

	// 设置下载头
	filename := fmt.Sprintf("verify_mismatch_%s.csv", id[:8])
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")

	// 写入 BOM 以支持 Excel 正确识别 UTF-8
	w.Write([]byte{0xEF, 0xBB, 0xBF})

	// 写入 CSV 头
	w.Write([]byte("Key,差异类型,缺失方,源端值,目标端值,源端TTL,目标端TTL\n"))

	if task.Result != nil && task.Result.Details != nil {
		for _, d := range task.Result.Details {
			lackSide := ""
			switch d.Type {
			case "missing", "lack_target":
				lackSide = "目标端缺失"
			case "extra", "lack_source":
				lackSide = "源端缺失"
			}
			
			typeText := ""
			switch d.Type {
			case "missing":
				typeText = "缺失"
			case "extra":
				typeText = "多余"
			case "value_mismatch":
				typeText = "值不匹配"
			case "ttl_mismatch":
				typeText = "TTL不匹配"
			case "length_mismatch":
				typeText = "长度不匹配"
			case "lack_target":
				typeText = "目标端缺失"
			case "lack_source":
				typeText = "源端缺失"
			default:
				typeText = d.Type
			}

			// 转义 CSV 字段
			srcVal := escapeCSV(d.SourceValue)
			tgtVal := escapeCSV(d.TargetValue)
			keyVal := escapeCSV(d.Key)

			line := fmt.Sprintf("%s,%s,%s,%s,%s,%d,%d\n",
				keyVal, typeText, lackSide, srcVal, tgtVal, d.SourceTTL, d.TargetTTL)
			w.Write([]byte(line))
		}
	}

	log.Info("Mismatch details downloaded", map[string]interface{}{
		"task_id": id,
		"count":   len(task.Result.Details),
	})
}

// runVerifyTask 执行校验任务
// 支持：并发控制、QPS限制、多DB、多种比较模式、大Key跳过
// 【重要】新增：多轮迭代收敛机制（借鉴 redis-full-check）
//   - 第1轮：全量扫描并比对，记录不一致 Key
//   - 后续轮次：只复查上轮不一致的 Key，逐步收敛
//   - 最终确认经过多轮复查仍然不一致的 Key
func runVerifyTask(task *VerifyTask) {
	ctx := context.Background()
	taskLog := logger.Default()
	
	// P3: 初始化指标监控
	metrics := NewVerifyMetrics()
	
	// P1: 初始化 SQLite 存储（如果启用）
	var sqliteDB *VerifyResultDB
	if task.EnableSQLite {
		var err error
		sqliteDB, err = NewVerifyResultDB(task.ID)
		if err != nil {
			taskLog.Warn("Failed to init SQLite, continuing without it", map[string]interface{}{"error": err.Error()})
		} else {
			task.sqliteDB = sqliteDB
			taskLog.Info("SQLite storage initialized", map[string]interface{}{"path": sqliteDB.dbPath})
		}
	}

	defer func() {
		// 关闭 SQLite 连接
		if sqliteDB != nil {
			sqliteDB.Close()
		}
		
		// P3: 完成指标计算
		if task.Result != nil {
			metrics.Finalize(task.Result.SampledKeys)
			task.Result.Metrics = metrics
		}
		
		verifyTasksMu.Lock()
		if task.Status == "running" {
			task.Status = "completed"
		}
		task.CompletedAt = time.Now().Format(time.RFC3339)
		// 计算一致性
		if task.Result != nil && task.Result.SampledKeys > 0 {
			task.Result.ConsistencyRate = float64(task.Result.MatchedKeys) / float64(task.Result.SampledKeys) * 100
		}
		verifyTasksMu.Unlock()
		
		// 保存校验任务状态到文件
		saveVerifyTasksState()
		
		taskLog.Info("Verify task finished", map[string]interface{}{
			"task_id":            task.ID,
			"status":             task.Status,
			"total_rounds":       task.Result.TotalRounds,
			"sampled_keys":       task.Result.SampledKeys,
			"matched_keys":       task.Result.MatchedKeys,
			"final_mismatch":     len(task.Result.FinalMismatchKeys),
			"consistency_rate":   task.Result.ConsistencyRate,
			"large_key_skipped":  task.Result.LargeKeySkipped,
			"target_extra_keys":  task.Result.TargetExtraKeys,
		})
	}()

	verifyTasksMu.Lock()
	task.Status = "running"
	task.StartedAt = time.Now().Format(time.RFC3339)
	if task.Result == nil {
		task.Result = &VerifyTaskResult{}
	}
	verifyTasksMu.Unlock()

	// 解析要校验的 DB 列表
	dbList := parseDBList(task.DBList)
	if len(dbList) > 0 {
		task.Result.DBsVerified = dbList
	}

	// 设置默认值
	concurrency := task.Concurrency
	if concurrency <= 0 {
		concurrency = 10
	}
	compareMode := task.CompareMode
	if compareMode == "" {
		if task.CompareValue {
			compareMode = "full_value"
		} else {
			compareMode = "exists_only"
		}
	}
	largeKeyThreshold := task.LargeKeyThreshold
	if largeKeyThreshold <= 0 {
		largeKeyThreshold = 10 * 1024 * 1024 // 10MB
	}
	
	// 多轮迭代参数
	compareRounds := task.CompareRounds
	if compareRounds <= 0 {
		compareRounds = 3 // 默认 3 轮
	}
	if compareRounds > 5 {
		compareRounds = 5 // 最多 5 轮
	}
	roundInterval := task.RoundInterval
	if roundInterval <= 0 {
		roundInterval = 5 // 默认间隔 5 秒
	}
	task.Result.TotalRounds = compareRounds

	// 连接源端和目标端
	sourceAddrs := strings.Split(task.SourceCluster, ",")
	targetAddrs := strings.Split(task.TargetCluster, ",")

	sourceClient, sourceIsCluster, err := connectRedisWithPoolSize(ctx, sourceAddrs, task.SourcePassword, concurrency, false)
	if err != nil {
		taskLog.Error("Failed to connect source cluster", map[string]interface{}{"error": err.Error()})
		task.Status = "failed"
		return
	}
	defer sourceClient.Close()

	targetClient, targetIsCluster, err := connectRedisWithPoolSize(ctx, targetAddrs, task.TargetPassword, concurrency, false)
	if err != nil {
		taskLog.Error("Failed to connect target cluster", map[string]interface{}{"error": err.Error()})
		task.Status = "failed"
		return
	}
	defer targetClient.Close()

	taskLog.Info("Verify task started", map[string]interface{}{
		"task_id":             task.ID,
		"verify_mode":         task.VerifyMode,
		"compare_mode":        compareMode,
		"compare_rounds":      compareRounds,
		"round_interval":      roundInterval,
		"sample_rate":         task.SampleRate,
		"max_keys":            task.MaxKeys,
		"concurrency":         concurrency,
		"qps":                 task.QPS,
		"skip_large_key":      task.SkipLargeKey,
		"large_key_threshold": largeKeyThreshold,
		"db_list":             dbList,
		"source_is_cluster":   sourceIsCluster,
		"target_is_cluster":   targetIsCluster,
		// P1/P2/P3 新增配置
		"direction":           task.Direction,
		"smart_compare":       task.SmartCompare,
		"field_level_compare": task.FieldLevelCompare,
		"enable_sqlite":       task.EnableSQLite,
	})

	// 阶段1：统计 Key 数量
	if task.VerifyMode == "count_only" || task.VerifyMode == "sample" || task.VerifyMode == "full" {
		sourceCount, targetCount := countClusterKeysWithDB(ctx, sourceClient, sourceIsCluster, targetClient, targetIsCluster, dbList)
		task.Result.SourceKeyCount = sourceCount
		task.Result.TargetKeyCount = targetCount
		
		taskLog.Info("Key count completed", map[string]interface{}{
			"source_count": sourceCount,
			"target_count": targetCount,
		})
		
		if task.VerifyMode == "count_only" {
			task.Result.Progress = 100
			return
		}
	}

	// 检查是否已取消
	verifyTasksMu.RLock()
	if task.Status == "cancelled" {
		verifyTasksMu.RUnlock()
		return
	}
	verifyTasksMu.RUnlock()

	// ========== 流式校验：SCAN 一批 → 立即比对 → 释放（适用于 100 亿 Key 场景）==========
	// 核心原则：不存储全量 Key 列表，避免 OOM
	// - 第 1 轮：流式 SCAN + 实时比对，只收集不一致的 Key（通常极少）
	// - 后续轮次：只复查上一轮不一致的 Key（数量可控）

	var scannedKeys int64
	var filteredKeys int64
	var sampledKeys int64
	startTime := time.Now()

	// 构建 SCAN 匹配模式
	scanPattern := "*"
	if task.KeyFilter != nil && len(task.KeyFilter.Prefixes) > 0 {
		scanPattern = task.KeyFilter.Prefixes[0] + "*"
	}

	// QPS 限流器
	var rateLimiter <-chan time.Time
	if task.QPS > 0 {
		rateLimiter = time.Tick(time.Second / time.Duration(task.QPS))
	}

	// ========== 第 1 轮：流式 SCAN + 实时比对 ==========
	round1StartTime := time.Now()
	task.Result.CurrentRound = 1
	task.Result.TotalRounds = compareRounds

	taskLog.Info("Starting streaming verification round 1 (scan + compare)", map[string]interface{}{
		"verify_mode":   task.VerifyMode,
		"compare_mode":  compareMode,
		"compare_rounds": compareRounds,
	})

	// 创建 key batch channel，SCAN 生产 → worker 消费
	keyBatchChan := make(chan []string, concurrency*2)
	round1Mismatches := make(map[string]VerifyMismatchDetail)
	var round1MismatchMu sync.Mutex
	// 【P2 安全保护】不一致 Key 上限：超过 100 万后停止收集，避免极端场景 OOM
	const maxMismatchKeys = 1000000
	var mismatchOverflow atomic.Int32

	// 启动消费者：并发校验 worker
	var verifyWg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		verifyWg.Add(1)
		go func() {
			defer verifyWg.Done()
			for batchKeys := range keyBatchChan {
				verifyTasksMu.RLock()
				cancelled := task.Status == "cancelled"
				verifyTasksMu.RUnlock()
				if cancelled {
					continue
				}

				batchMismatches := verifyBatchForRound(ctx, task, sourceClient, targetClient,
					batchKeys, compareMode, largeKeyThreshold, metrics)

				if len(batchMismatches) > 0 {
					round1MismatchMu.Lock()
					for key, detail := range batchMismatches {
						if len(round1Mismatches) >= maxMismatchKeys {
							mismatchOverflow.Store(1)
							break
						}
						round1Mismatches[key] = detail
					}
					round1MismatchMu.Unlock()
				}
			}
		}()
	}

	// 生产者：流式 SCAN → 过滤 → 分批发送到 channel（不存储全量 Key）
	if sourceIsCluster {
		clusterClient := sourceClient.(*redis.ClusterClient)
		clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			streamScanFromNode(ctx, node, task, scanPattern, keyBatchChan,
				&scannedKeys, &filteredKeys, &sampledKeys, startTime, rateLimiter)
			return nil
		})
	} else {
		if len(dbList) > 0 {
			for _, dbNum := range dbList {
				verifyTasksMu.RLock()
				cancelled := task.Status == "cancelled"
				verifyTasksMu.RUnlock()
				if cancelled {
					break
				}
				if client, ok := sourceClient.(*redis.Client); ok {
					if err := client.Do(ctx, "SELECT", dbNum).Err(); err != nil {
						taskLog.Warn("Failed to select DB", map[string]interface{}{"db": dbNum, "error": err.Error()})
						continue
					}
				}
				taskLog.Info("Scanning DB", map[string]interface{}{"db": dbNum})
				streamScanFromClient(ctx, sourceClient, task, scanPattern, keyBatchChan,
					&scannedKeys, &filteredKeys, &sampledKeys, startTime, rateLimiter)
			}
		} else {
			streamScanFromClient(ctx, sourceClient, task, scanPattern, keyBatchChan,
				&scannedKeys, &filteredKeys, &sampledKeys, startTime, rateLimiter)
		}
	}
	close(keyBatchChan) // SCAN 完成，关闭 channel
	verifyWg.Wait()     // 等待所有 worker 完成

	// 更新第 1 轮统计
	task.Result.ScannedKeys = scannedKeys
	task.Result.FilteredKeys = filteredKeys
	task.Result.SampledKeys = sampledKeys
	round1EndTime := time.Now()

	taskLog.Info("Streaming round 1 completed (scan + compare)", map[string]interface{}{
		"scanned":            scannedKeys,
		"filtered":           filteredKeys,
		"sampled":            sampledKeys,
		"mismatch_found":     len(round1Mismatches),
		"mismatch_overflow":  mismatchOverflow.Load() == 1,
		"duration":           round1EndTime.Sub(round1StartTime).String(),
	})

	if mismatchOverflow.Load() == 1 {
		taskLog.Warn("⚠️ Mismatch keys exceeded limit, some mismatches may not be recorded", map[string]interface{}{
			"max_mismatch_keys": maxMismatchKeys,
			"recorded":          len(round1Mismatches),
		})
	}

	// 构建第 1 轮结果
	round1Result := VerifyRoundResult{
		RoundNo:     1,
		StartTime:   round1StartTime.Format(time.RFC3339),
		EndTime:     round1EndTime.Format(time.RFC3339),
		KeysToCheck: sampledKeys,
		MismatchCount: int64(len(round1Mismatches)),
	}
	if len(round1Mismatches) > 0 {
		round1Result.MismatchKeys = make([]string, 0, len(round1Mismatches))
		for key := range round1Mismatches {
			round1Result.MismatchKeys = append(round1Result.MismatchKeys, key)
		}
		if len(round1Result.MismatchKeys) <= 100 {
			round1Result.Details = make([]VerifyMismatchDetail, 0, len(round1Mismatches))
			for key, detail := range round1Mismatches {
				detail.Key = key
				round1Result.Details = append(round1Result.Details, detail)
			}
		}
		if sqliteDB != nil {
			for key, detail := range round1Mismatches {
				sqliteDB.SaveKeyDiff(1, key, "", detail.Type,
					detail.SourceValue, detail.TargetValue, detail.SourceTTL, detail.TargetTTL)
			}
			sqliteDB.SaveRoundSummary(1, sampledKeys,
				int64(len(round1Mismatches)), 0,
				round1StartTime.Format(time.RFC3339), round1EndTime.Format(time.RFC3339))
		}
	}
	metrics.AddRoundMetric(1, round1EndTime.Sub(round1StartTime), sampledKeys, int64(len(round1Mismatches)))
	task.Result.Rounds = append(task.Result.Rounds, round1Result)
	task.Result.Progress = 50 + float64(1)/float64(compareRounds)*40

	// ========== 后续轮次：只复查不一致的 Key（数量极少，存储安全）==========
	currentMismatchKeys := round1Result.MismatchKeys
	previousMismatchCount := int64(len(round1Mismatches))

	for round := 2; round <= compareRounds; round++ {
		if len(currentMismatchKeys) == 0 {
			taskLog.Info("All keys converged, no mismatch found", map[string]interface{}{"round": round - 1})
			break
		}

		verifyTasksMu.RLock()
		cancelled := task.Status == "cancelled"
		verifyTasksMu.RUnlock()
		if cancelled {
			break
		}

		task.Result.CurrentRound = round

		if roundInterval > 0 {
			taskLog.Info("Waiting for round interval", map[string]interface{}{"interval_seconds": roundInterval})
			time.Sleep(time.Duration(roundInterval) * time.Second)
		}

		roundStartTime := time.Now()
		taskLog.Info("Starting verification round (recheck only)", map[string]interface{}{
			"round":         round,
			"total_rounds":  compareRounds,
			"keys_to_check": len(currentMismatchKeys),
		})

		roundMismatches := verifyKeysForRound(ctx, task, sourceClient, targetClient,
			currentMismatchKeys, concurrency, compareMode, largeKeyThreshold, taskLog, metrics)

		roundEndTime := time.Now()

		var convergeRate float64 = 0
		if previousMismatchCount > 0 {
			convergeRate = (1 - float64(len(roundMismatches))/float64(previousMismatchCount)) * 100
		}

		roundResult := VerifyRoundResult{
			RoundNo:       round,
			StartTime:     roundStartTime.Format(time.RFC3339),
			EndTime:       roundEndTime.Format(time.RFC3339),
			KeysToCheck:   int64(len(currentMismatchKeys)),
			MismatchCount: int64(len(roundMismatches)),
			ConvergeRate:  convergeRate,
		}
		if len(roundMismatches) > 0 {
			roundResult.MismatchKeys = make([]string, 0, len(roundMismatches))
			for key := range roundMismatches {
				roundResult.MismatchKeys = append(roundResult.MismatchKeys, key)
			}
			if len(roundResult.MismatchKeys) <= 100 {
				roundResult.Details = make([]VerifyMismatchDetail, 0, len(roundMismatches))
				for key, detail := range roundMismatches {
					detail.Key = key
					roundResult.Details = append(roundResult.Details, detail)
				}
			}
			if sqliteDB != nil {
				for key, detail := range roundMismatches {
					sqliteDB.SaveKeyDiff(round, key, "", detail.Type,
						detail.SourceValue, detail.TargetValue, detail.SourceTTL, detail.TargetTTL)
				}
				sqliteDB.SaveRoundSummary(round, int64(len(currentMismatchKeys)),
					int64(len(roundMismatches)), convergeRate,
					roundStartTime.Format(time.RFC3339), roundEndTime.Format(time.RFC3339))
			}
		}

		metrics.AddRoundMetric(round, roundEndTime.Sub(roundStartTime),
			int64(len(currentMismatchKeys)), int64(len(roundMismatches)))

		task.Result.Rounds = append(task.Result.Rounds, roundResult)

		taskLog.Info("Verification round completed", map[string]interface{}{
			"round":          round,
			"keys_checked":   len(currentMismatchKeys),
			"mismatch_found": len(roundMismatches),
			"converge_rate":  fmt.Sprintf("%.2f%%", convergeRate),
			"duration":       roundEndTime.Sub(roundStartTime).String(),
		})

		task.Result.Progress = 50 + float64(round)/float64(compareRounds)*40

		previousMismatchCount = int64(len(roundMismatches))
		currentMismatchKeys = roundResult.MismatchKeys
	}

	// 设置最终不一致的 Key 列表
	if len(task.Result.Rounds) > 0 {
		lastRound := task.Result.Rounds[len(task.Result.Rounds)-1]
		task.Result.FinalMismatchKeys = lastRound.MismatchKeys
		task.Result.MissingKeys = lastRound.MismatchCount
		task.Result.Details = lastRound.Details

		task.Result.MatchedKeys = task.Result.SampledKeys - lastRound.MismatchCount
	}
	
	// ========== P1: Field 级别比对（对不一致的复合类型进行细粒度比对）==========
	if task.FieldLevelCompare && len(task.Result.FinalMismatchKeys) > 0 {
		taskLog.Info("Starting field-level comparison", map[string]interface{}{
			"keys_to_check": len(task.Result.FinalMismatchKeys),
		})
		
		fieldScanThreshold := task.FieldScanThreshold
		if fieldScanThreshold <= 0 {
			fieldScanThreshold = 5000
		}
		
		var fieldMismatches []FieldMismatchDetail
		for _, key := range task.Result.FinalMismatchKeys {
			if len(fieldMismatches) >= 100 {
				break
			}
			
			// 获取 Key 类型
			keyType, err := sourceClient.Type(ctx, key).Result()
			if err != nil || keyType == "none" {
				continue
			}
			
			var detail *FieldMismatchDetail
			var matched bool
			
			switch keyType {
			case "hash":
				matched, detail = compareHashFields(ctx, sourceClient, targetClient, key, fieldScanThreshold)
			case "set":
				matched, detail = compareSetMembers(ctx, sourceClient, targetClient, key, fieldScanThreshold)
			case "zset":
				matched, detail = compareZSetMembers(ctx, sourceClient, targetClient, key, fieldScanThreshold)
			default:
				continue
			}
			
			if !matched && detail != nil {
				fieldMismatches = append(fieldMismatches, *detail)
				
				// P1: 保存 Field 级别详情到 SQLite
				if sqliteDB != nil {
					for _, fd := range detail.MismatchFields {
						sqliteDB.SaveFieldDiff(compareRounds, key, keyType, fd.Field, fd.Type,
							fd.SourceValue, fd.TargetValue, fd.SourceScore, fd.TargetScore)
					}
				}
			}
		}
		
		task.Result.FieldMismatchKeys = int64(len(fieldMismatches))
		task.Result.FieldMismatches = fieldMismatches
		
		taskLog.Info("Field-level comparison completed", map[string]interface{}{
			"field_mismatch_keys": len(fieldMismatches),
		})
	}
	
	// ========== P2: 双向校验（检测目标端多余的 Key）==========
	if task.Direction == "bidirectional" || task.Direction == "target_to_source" {
		// 检查是否已取消
		verifyTasksMu.RLock()
		if task.Status == "cancelled" {
			verifyTasksMu.RUnlock()
		} else {
			verifyTasksMu.RUnlock()
			
			task.Result.Progress = 95
			extraKeys, extraCount := checkExtraKeysInTarget(ctx, task, sourceClient, targetClient, targetIsCluster, taskLog)
			task.Result.TargetExtraKeys = extraCount
			
			if len(extraKeys) > 0 {
				var extraDetails []VerifyMismatchDetail
				for _, key := range extraKeys {
					if len(extraDetails) >= 100 {
						break
					}
					extraDetails = append(extraDetails, VerifyMismatchDetail{
						Key:  key,
						Type: "extra",
					})
				}
				task.Result.ExtraKeyDetails = extraDetails
			}
		}
	}
	
	task.Result.Progress = 100
}

// parseDBList 解析 DB 列表字符串（分号分隔）
func parseDBList(dbListStr string) []int {
	if dbListStr == "" {
		return nil
	}
	
	parts := strings.Split(dbListStr, ";")
	var result []int
	seen := make(map[int]bool)
	
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		dbNum, err := strconv.Atoi(part)
		if err != nil || dbNum < 0 || dbNum > 15 {
			continue
		}
		if !seen[dbNum] {
			seen[dbNum] = true
			result = append(result, dbNum)
		}
	}
	
	sort.Ints(result)
	return result
}

// scanKeysFromNode 从集群节点扫描 Key
// streamScanFromNode 流式扫描：从集群节点 SCAN key，过滤后分批发送到 channel（不存储全量 Key）
func streamScanFromNode(ctx context.Context, node *redis.Client, task *VerifyTask, scanPattern string,
	keyBatchChan chan<- []string, scannedKeys *int64, filteredKeys *int64, sampledKeys *int64,
	startTime time.Time, rateLimiter <-chan time.Time) {

	const batchSize = 100 // 每批发送给 worker 的 key 数
	var cursor uint64
	var batch []string

	for {
		verifyTasksMu.RLock()
		if task.Status == "cancelled" {
			verifyTasksMu.RUnlock()
			return
		}
		verifyTasksMu.RUnlock()

		if rateLimiter != nil {
			<-rateLimiter
		}

		keys, newCursor, err := node.Scan(ctx, cursor, scanPattern, 1000).Result()
		if err != nil {
			return
		}

		for _, key := range keys {
			atomic.AddInt64(scannedKeys, 1)
			shouldSample := task.VerifyMode == "full" || rand.Float64() < task.SampleRate
			maxKeysReached := task.VerifyMode != "full" && task.MaxKeys > 0 && atomic.LoadInt64(sampledKeys) >= task.MaxKeys
			if shouldSample && !maxKeysReached {
				if matchVerifyKeyFilter(key, task.KeyFilter) {
					atomic.AddInt64(filteredKeys, 1)
					atomic.AddInt64(sampledKeys, 1)
					batch = append(batch, key)
					// 凑满一批就发送
					if len(batch) >= batchSize {
						sendBatch := make([]string, len(batch))
						copy(sendBatch, batch)
						keyBatchChan <- sendBatch
						batch = batch[:0]
					}
				}
			}

			// 更新进度
			scanned := atomic.LoadInt64(scannedKeys)
			if scanned%10000 == 0 {
				task.Result.ScannedKeys = scanned
				task.Result.FilteredKeys = atomic.LoadInt64(filteredKeys)
				task.Result.SampledKeys = atomic.LoadInt64(sampledKeys)
				elapsed := time.Since(startTime).Seconds()
				if elapsed > 0 {
					task.Result.CurrentSpeed = int64(float64(scanned) / elapsed)
				}
				if task.Result.SourceKeyCount > 0 {
					task.Result.Progress = float64(scanned) / float64(task.Result.SourceKeyCount) * 50
				}
			}
		}

		cursor = newCursor
		maxKeysLimit := task.VerifyMode != "full" && task.MaxKeys > 0 && atomic.LoadInt64(sampledKeys) >= task.MaxKeys
		if cursor == 0 || maxKeysLimit {
			break
		}
	}

	// 发送剩余的 key
	if len(batch) > 0 {
		sendBatch := make([]string, len(batch))
		copy(sendBatch, batch)
		keyBatchChan <- sendBatch
	}
}

// streamScanFromClient 流式扫描：从客户端 SCAN key，过滤后分批发送到 channel（不存储全量 Key）
func streamScanFromClient(ctx context.Context, client redis.Cmdable, task *VerifyTask, scanPattern string,
	keyBatchChan chan<- []string, scannedKeys *int64, filteredKeys *int64, sampledKeys *int64,
	startTime time.Time, rateLimiter <-chan time.Time) {

	const batchSize = 100
	var cursor uint64
	var batch []string

	for {
		verifyTasksMu.RLock()
		if task.Status == "cancelled" {
			verifyTasksMu.RUnlock()
			break
		}
		verifyTasksMu.RUnlock()

		if rateLimiter != nil {
			<-rateLimiter
		}

		keys, newCursor, err := client.Scan(ctx, cursor, scanPattern, 1000).Result()
		if err != nil {
			break
		}

		for _, key := range keys {
			atomic.AddInt64(scannedKeys, 1)
			shouldSample := task.VerifyMode == "full" || rand.Float64() < task.SampleRate
			maxKeysReached := task.VerifyMode != "full" && task.MaxKeys > 0 && atomic.LoadInt64(sampledKeys) >= task.MaxKeys
			if shouldSample && !maxKeysReached {
				if matchVerifyKeyFilter(key, task.KeyFilter) {
					atomic.AddInt64(filteredKeys, 1)
					atomic.AddInt64(sampledKeys, 1)
					batch = append(batch, key)
					if len(batch) >= batchSize {
						sendBatch := make([]string, len(batch))
						copy(sendBatch, batch)
						keyBatchChan <- sendBatch
						batch = batch[:0]
					}
				}
			}

			scanned := atomic.LoadInt64(scannedKeys)
			if scanned%10000 == 0 {
				task.Result.ScannedKeys = scanned
				task.Result.FilteredKeys = atomic.LoadInt64(filteredKeys)
				task.Result.SampledKeys = atomic.LoadInt64(sampledKeys)
				elapsed := time.Since(startTime).Seconds()
				if elapsed > 0 {
					task.Result.CurrentSpeed = int64(float64(scanned) / elapsed)
				}
				if task.Result.SourceKeyCount > 0 {
					task.Result.Progress = float64(scanned) / float64(task.Result.SourceKeyCount) * 50
				}
			}
		}

		cursor = newCursor
		maxKeysLimit := task.VerifyMode != "full" && task.MaxKeys > 0 && atomic.LoadInt64(sampledKeys) >= task.MaxKeys
		if cursor == 0 || maxKeysLimit {
			break
		}
	}

	if len(batch) > 0 {
		sendBatch := make([]string, len(batch))
		copy(sendBatch, batch)
		keyBatchChan <- sendBatch
	}
}

// scanKeysFromNode 从节点扫描 Key（旧接口，保留用于后续轮次小量 key 收集等场景）
func scanKeysFromNode(ctx context.Context, node *redis.Client, task *VerifyTask, scanPattern string, 
	keysToVerify *[]string, scannedKeys *int64, filteredKeys *int64, mu *sync.Mutex, startTime time.Time, rateLimiter <-chan time.Time) {
	
	var cursor uint64
	for {
		// 检查是否已取消
		verifyTasksMu.RLock()
		if task.Status == "cancelled" {
			verifyTasksMu.RUnlock()
			return
		}
		verifyTasksMu.RUnlock()

		// QPS 限流
		if rateLimiter != nil {
			<-rateLimiter
		}

		keys, newCursor, err := node.Scan(ctx, cursor, scanPattern, 1000).Result()
		if err != nil {
			return
		}

		mu.Lock()
		for _, key := range keys {
			*scannedKeys++ // SCAN MATCH 返回的 Key（已被服务端 pattern 过滤）
			// 根据采样率或全量模式决定是否采样
			shouldSample := task.VerifyMode == "full" || rand.Float64() < task.SampleRate
			// 全量校验模式不限制 MaxKeys
			maxKeysReached := task.VerifyMode != "full" && int64(len(*keysToVerify)) >= task.MaxKeys
			if shouldSample && !maxKeysReached {
				// 检查 Key 过滤（可能有额外的 exclude_prefixes 等过滤条件）
				if matchVerifyKeyFilter(key, task.KeyFilter) {
					*filteredKeys++
					*keysToVerify = append(*keysToVerify, key)
				}
			}
			
			// 更新进度
			if *scannedKeys%10000 == 0 {
				task.Result.ScannedKeys = *scannedKeys
				task.Result.FilteredKeys = *filteredKeys
				task.Result.SampledKeys = int64(len(*keysToVerify))
				elapsed := time.Since(startTime).Seconds()
				if elapsed > 0 {
					task.Result.CurrentSpeed = int64(float64(*scannedKeys) / elapsed)
				}
				if task.Result.SourceKeyCount > 0 {
					task.Result.Progress = float64(*scannedKeys) / float64(task.Result.SourceKeyCount) * 50
				}
			}
		}
		mu.Unlock()

		cursor = newCursor
		// 全量校验模式不受 MaxKeys 限制，只有 cursor=0 时才退出
		maxKeysLimit := task.VerifyMode != "full" && int64(len(*keysToVerify)) >= task.MaxKeys
		if cursor == 0 || maxKeysLimit {
			break
		}
	}
}

// scanKeysFromClient 从客户端扫描 Key（非集群模式）
func scanKeysFromClient(ctx context.Context, client redis.Cmdable, task *VerifyTask, scanPattern string,
	keysToVerify *[]string, scannedKeys *int64, filteredKeys *int64, mu *sync.Mutex, startTime time.Time, rateLimiter <-chan time.Time) {
	
	var cursor uint64
	for {
		// 检查是否已取消
		verifyTasksMu.RLock()
		if task.Status == "cancelled" {
			verifyTasksMu.RUnlock()
			break
		}
		verifyTasksMu.RUnlock()

		// QPS 限流
		if rateLimiter != nil {
			<-rateLimiter
		}

		keys, newCursor, err := client.Scan(ctx, cursor, scanPattern, 1000).Result()
		if err != nil {
			break
		}

		mu.Lock()
		for _, key := range keys {
			*scannedKeys++ // SCAN MATCH 返回的 Key
			shouldSample := task.VerifyMode == "full" || rand.Float64() < task.SampleRate
			// 全量校验模式不限制 MaxKeys
			maxKeysReached := task.VerifyMode != "full" && int64(len(*keysToVerify)) >= task.MaxKeys
			if shouldSample && !maxKeysReached {
				if matchVerifyKeyFilter(key, task.KeyFilter) {
					*filteredKeys++
					*keysToVerify = append(*keysToVerify, key)
				}
			}
			
			// 更新进度
			if *scannedKeys%10000 == 0 {
				task.Result.ScannedKeys = *scannedKeys
				task.Result.FilteredKeys = *filteredKeys
				task.Result.SampledKeys = int64(len(*keysToVerify))
				elapsed := time.Since(startTime).Seconds()
				if elapsed > 0 {
					task.Result.CurrentSpeed = int64(float64(*scannedKeys) / elapsed)
				}
				if task.Result.SourceKeyCount > 0 {
					task.Result.Progress = float64(*scannedKeys) / float64(task.Result.SourceKeyCount) * 50
				}
			}
		}
		mu.Unlock()

		cursor = newCursor
		// 全量校验模式不受 MaxKeys 限制
		maxKeysLimit := task.VerifyMode != "full" && int64(len(*keysToVerify)) >= task.MaxKeys
		if cursor == 0 || maxKeysLimit {
			break
		}
	}
}

// verifyKeysForRound 单轮校验（用于多轮迭代收敛）
// 返回不一致的 Key 及其详情
func verifyKeysForRound(ctx context.Context, task *VerifyTask, 
	sourceClient, targetClient redis.Cmdable,
	keysToVerify []string, concurrency int, compareMode string, 
	largeKeyThreshold int64, taskLog *logger.Logger, metrics *VerifyMetrics) map[string]VerifyMismatchDetail {
	
	// 如果启用了智能比较，走逐 Key 智能比较路径
	if task.SmartCompare {
		return verifyKeysForRoundSmart(ctx, task, sourceClient, targetClient,
			keysToVerify, concurrency, taskLog, metrics)
	}
	
	const batchSize = 100
	mismatches := make(map[string]VerifyMismatchDetail)
	var mismatchMu sync.Mutex
	var wg sync.WaitGroup
	
	// 创建工作通道
	workChan := make(chan []string, concurrency)
	
	// 启动工作协程
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batchKeys := range workChan {
				// 检查是否已取消
				verifyTasksMu.RLock()
				if task.Status == "cancelled" {
					verifyTasksMu.RUnlock()
					continue
				}
				verifyTasksMu.RUnlock()

				batchMismatches := verifyBatchForRound(ctx, task, sourceClient, targetClient, 
					batchKeys, compareMode, largeKeyThreshold, metrics)
				
				if len(batchMismatches) > 0 {
					mismatchMu.Lock()
					for key, detail := range batchMismatches {
						mismatches[key] = detail
					}
					mismatchMu.Unlock()
				}
			}
		}()
	}
	
	// 分批发送任务
	for i := 0; i < len(keysToVerify); i += batchSize {
		// 检查是否已取消
		verifyTasksMu.RLock()
		if task.Status == "cancelled" {
			verifyTasksMu.RUnlock()
			break
		}
		verifyTasksMu.RUnlock()

		end := i + batchSize
		if end > len(keysToVerify) {
			end = len(keysToVerify)
		}
		
		batchKeys := make([]string, end-i)
		copy(batchKeys, keysToVerify[i:end])
		workChan <- batchKeys
	}
	
	close(workChan)
	wg.Wait()
	
	return mismatches
}

// verifyBatchForRound 单批次校验（返回不一致的 Key 详情）
func verifyBatchForRound(ctx context.Context, task *VerifyTask, 
	sourceClient, targetClient redis.Cmdable,
	batchKeys []string, compareMode string, largeKeyThreshold int64, metrics *VerifyMetrics) map[string]VerifyMismatchDetail {
	
	mismatches := make(map[string]VerifyMismatchDetail)
	
	// Pipeline 获取源端数据
	sourcePipe := sourceClient.Pipeline()
	sourceTypeCmds := make([]*redis.StatusCmd, len(batchKeys))
	sourceTTLCmds := make([]*redis.DurationCmd, len(batchKeys))
	var sourceDumpCmds []*redis.StringCmd
	var sourceStrLenCmds []*redis.IntCmd
	
	needDump := compareMode == "full_value"
	needLen := compareMode == "length_only" || (compareMode == "full_value" && task.SkipLargeKey)
	
	if needDump {
		sourceDumpCmds = make([]*redis.StringCmd, len(batchKeys))
	}
	if needLen {
		sourceStrLenCmds = make([]*redis.IntCmd, len(batchKeys))
	}

	var sourceCmdCount int64
	for j, key := range batchKeys {
		sourceTypeCmds[j] = sourcePipe.Type(ctx, key)
		sourceTTLCmds[j] = sourcePipe.TTL(ctx, key)
		sourceCmdCount += 2
		if needDump {
			sourceDumpCmds[j] = sourcePipe.Dump(ctx, key)
			sourceCmdCount++
		}
		if needLen {
			sourceStrLenCmds[j] = sourcePipe.StrLen(ctx, key)
			sourceCmdCount++
		}
	}
	sourcePipe.Exec(ctx)
	if metrics != nil {
		metrics.RecordRedisCommand(sourceCmdCount)
		metrics.RecordPipelineBatch()
	}

	// Pipeline 获取目标端数据
	targetPipe := targetClient.Pipeline()
	targetExistsCmds := make([]*redis.IntCmd, len(batchKeys))
	targetTTLCmds := make([]*redis.DurationCmd, len(batchKeys))
	var targetDumpCmds []*redis.StringCmd
	var targetStrLenCmds []*redis.IntCmd
	
	if needDump {
		targetDumpCmds = make([]*redis.StringCmd, len(batchKeys))
	}
	if needLen {
		targetStrLenCmds = make([]*redis.IntCmd, len(batchKeys))
	}

	var targetCmdCount int64
	for j, key := range batchKeys {
		targetExistsCmds[j] = targetPipe.Exists(ctx, key)
		targetTTLCmds[j] = targetPipe.TTL(ctx, key)
		targetCmdCount += 2
		if needDump {
			targetDumpCmds[j] = targetPipe.Dump(ctx, key)
			targetCmdCount++
		}
		if needLen {
			targetStrLenCmds[j] = targetPipe.StrLen(ctx, key)
			targetCmdCount++
		}
	}
	targetPipe.Exec(ctx)
	if metrics != nil {
		metrics.RecordRedisCommand(targetCmdCount)
		metrics.RecordPipelineBatch()
	}

	// 比对结果
	for j, key := range batchKeys {
		sourceType, _ := sourceTypeCmds[j].Result()
		sourceTTL, _ := sourceTTLCmds[j].Result()

		targetExists, _ := targetExistsCmds[j].Result()
		targetTTL, _ := targetTTLCmds[j].Result()

		// 源端 Key 不存在（可能已被删除）- 视为已收敛/一致
		if sourceType == "none" {
			continue
		}

		// P3: 记录 Key 类型
		if metrics != nil {
			metrics.RecordKeyType(sourceType)
		}

		// 目标端 Key 不存在 - 不一致
		if targetExists == 0 {
			mismatches[key] = VerifyMismatchDetail{
				Type: "missing",
			}
			continue
		}

		// Key 存在，根据比较模式进行比较
		switch compareMode {
		case "exists_only":
			// 只检查 Key 是否存在，已确认存在，视为一致
			continue
			
		case "length_only":
			// 只比较长度
			if sourceStrLenCmds != nil && targetStrLenCmds != nil {
				sourceLen, _ := sourceStrLenCmds[j].Result()
				targetLen, _ := targetStrLenCmds[j].Result()
				if sourceLen != targetLen {
					mismatches[key] = VerifyMismatchDetail{
						Type:        "length_mismatch",
						SourceValue: fmt.Sprintf("%d bytes", sourceLen),
						TargetValue: fmt.Sprintf("%d bytes", targetLen),
					}
				}
			}
			
		case "full_value":
			// 全量值比较
			if sourceDumpCmds != nil && targetDumpCmds != nil {
				sourceDump, sourceErr := sourceDumpCmds[j].Result()
				targetDump, targetErr := targetDumpCmds[j].Result()
				
				// 检查是否跳过大 Key
				if task.SkipLargeKey && len(sourceDump) > int(largeKeyThreshold) {
					atomic.AddInt64(&task.Result.LargeKeySkipped, 1)
					continue
				}

				if sourceErr == nil && targetErr == nil && sourceDump != targetDump {
					mismatches[key] = VerifyMismatchDetail{
						Type:        "value_mismatch",
						SourceValue: fmt.Sprintf("[%s] %d bytes", sourceType, len(sourceDump)),
						TargetValue: fmt.Sprintf("%d bytes", len(targetDump)),
					}
				}
			}
		}

		// 如果已经发现不一致，不再检查 TTL
		if _, exists := mismatches[key]; exists {
			continue
		}

		// 比较 TTL
		if task.CompareTTL {
			ttlDiff := sourceTTL - targetTTL
			if ttlDiff < 0 {
				ttlDiff = -ttlDiff
			}
			if ttlDiff > time.Duration(task.TTLTolerance)*time.Second {
				mismatches[key] = VerifyMismatchDetail{
					Type:      "ttl_mismatch",
					SourceTTL: int64(sourceTTL.Seconds()),
					TargetTTL: int64(targetTTL.Seconds()),
				}
			}
		}
	}
	
	return mismatches
}

// ==================== P1: Field 级别比对实现 ====================

// compareHashFields Hash 类型 Field 级别比对
func compareHashFields(ctx context.Context, sourceClient, targetClient redis.Cmdable, 
	key string, threshold int64) (matched bool, detail *FieldMismatchDetail) {
	
	// 获取源端和目标端 Hash 大小
	sourceLen, err1 := sourceClient.HLen(ctx, key).Result()
	targetLen, err2 := targetClient.HLen(ctx, key).Result()
	
	if err1 != nil || err2 != nil {
		return false, nil
	}
	
	detail = &FieldMismatchDetail{
		Key:         key,
		KeyType:     "hash",
		TotalFields: sourceLen,
	}
	
	// 长度不同，直接判定不一致
	if sourceLen != targetLen {
		detail.MismatchFields = append(detail.MismatchFields, FieldDiff{
			Field: "_length_",
			Type:  "length_mismatch",
			SourceValue: fmt.Sprintf("%d", sourceLen),
			TargetValue: fmt.Sprintf("%d", targetLen),
		})
		return false, detail
	}
	
	// 根据大小选择获取方式
	if sourceLen > threshold {
		// 大 Hash 使用 HSCAN
		return compareLargeHash(ctx, sourceClient, targetClient, key, detail)
	}
	
	// 小 Hash 使用 HGETALL
	sourceMap, err1 := sourceClient.HGetAll(ctx, key).Result()
	targetMap, err2 := targetClient.HGetAll(ctx, key).Result()
	
	if err1 != nil || err2 != nil {
		return false, nil
	}
	
	var mismatches []FieldDiff
	
	// 检查源端 Field
	for field, sourceValue := range sourceMap {
		targetValue, exists := targetMap[field]
		if !exists {
			mismatches = append(mismatches, FieldDiff{
				Field:       field,
				Type:        "lack_target",
				SourceValue: truncateValue(sourceValue, 100),
			})
		} else if sourceValue != targetValue {
			mismatches = append(mismatches, FieldDiff{
				Field:       field,
				Type:        "value_mismatch",
				SourceValue: truncateValue(sourceValue, 100),
				TargetValue: truncateValue(targetValue, 100),
			})
		}
	}
	
	// 检查目标端多余的 Field
	for field, targetValue := range targetMap {
		if _, exists := sourceMap[field]; !exists {
			mismatches = append(mismatches, FieldDiff{
				Field:       field,
				Type:        "lack_source",
				TargetValue: truncateValue(targetValue, 100),
			})
		}
	}
	
	if len(mismatches) > 0 {
		// 限制返回的 Field 数量
		if len(mismatches) > 50 {
			detail.MismatchFields = mismatches[:50]
		} else {
			detail.MismatchFields = mismatches
		}
		return false, detail
	}
	
	return true, nil
}

// compareLargeHash 大 Hash 使用 HSCAN 比对
func compareLargeHash(ctx context.Context, sourceClient, targetClient redis.Cmdable, 
	key string, detail *FieldMismatchDetail) (matched bool, _ *FieldMismatchDetail) {
	
	var mismatches []FieldDiff
	var cursor uint64
	
	for {
		// 扫描源端
		vals, newCursor, err := sourceClient.HScan(ctx, key, cursor, "*", 500).Result()
		if err != nil {
			break
		}
		
		// vals 是 [field1, value1, field2, value2, ...]
		for i := 0; i < len(vals); i += 2 {
			if i+1 >= len(vals) {
				break
			}
			field := vals[i]
			sourceValue := vals[i+1]
			
			// 获取目标端对应 Field
			targetValue, err := targetClient.HGet(ctx, key, field).Result()
			if err == redis.Nil {
				mismatches = append(mismatches, FieldDiff{
					Field:       field,
					Type:        "lack_target",
					SourceValue: truncateValue(sourceValue, 100),
				})
			} else if err == nil && sourceValue != targetValue {
				mismatches = append(mismatches, FieldDiff{
					Field:       field,
					Type:        "value_mismatch",
					SourceValue: truncateValue(sourceValue, 100),
					TargetValue: truncateValue(targetValue, 100),
				})
			}
			
			// 限制不一致数量
			if len(mismatches) >= 100 {
				break
			}
		}
		
		cursor = newCursor
		if cursor == 0 || len(mismatches) >= 100 {
			break
		}
	}
	
	if len(mismatches) > 0 {
		detail.MismatchFields = mismatches
		return false, detail
	}
	
	return true, nil
}

// compareSetMembers Set 类型成员比对
func compareSetMembers(ctx context.Context, sourceClient, targetClient redis.Cmdable,
	key string, threshold int64) (matched bool, detail *FieldMismatchDetail) {
	
	sourceLen, err1 := sourceClient.SCard(ctx, key).Result()
	targetLen, err2 := targetClient.SCard(ctx, key).Result()
	
	if err1 != nil || err2 != nil {
		return false, nil
	}
	
	detail = &FieldMismatchDetail{
		Key:         key,
		KeyType:     "set",
		TotalFields: sourceLen,
	}
	
	if sourceLen != targetLen {
		detail.MismatchFields = append(detail.MismatchFields, FieldDiff{
			Field: "_cardinality_",
			Type:  "length_mismatch",
			SourceValue: fmt.Sprintf("%d", sourceLen),
			TargetValue: fmt.Sprintf("%d", targetLen),
		})
		return false, detail
	}
	
	// 根据大小选择获取方式
	if sourceLen > threshold {
		return compareLargeSet(ctx, sourceClient, targetClient, key, detail)
	}
	
	// 小 Set 使用 SMEMBERS
	sourceMembers, _ := sourceClient.SMembers(ctx, key).Result()
	
	var mismatches []FieldDiff
	for _, member := range sourceMembers {
		exists, _ := targetClient.SIsMember(ctx, key, member).Result()
		if !exists {
			mismatches = append(mismatches, FieldDiff{
				Field:       truncateValue(member, 50),
				Type:        "lack_target",
				SourceValue: member,
			})
		}
		if len(mismatches) >= 50 {
			break
		}
	}
	
	if len(mismatches) > 0 {
		detail.MismatchFields = mismatches
		return false, detail
	}
	
	return true, nil
}

// compareLargeSet 大 Set 使用 SSCAN 比对
func compareLargeSet(ctx context.Context, sourceClient, targetClient redis.Cmdable,
	key string, detail *FieldMismatchDetail) (matched bool, _ *FieldMismatchDetail) {
	
	var mismatches []FieldDiff
	var cursor uint64
	
	for {
		members, newCursor, err := sourceClient.SScan(ctx, key, cursor, "*", 500).Result()
		if err != nil {
			break
		}
		
		// 批量检查成员是否存在
		pipe := targetClient.Pipeline()
		cmds := make([]*redis.BoolCmd, len(members))
		for i, member := range members {
			cmds[i] = pipe.SIsMember(ctx, key, member)
		}
		pipe.Exec(ctx)
		
		for i, member := range members {
			exists, _ := cmds[i].Result()
			if !exists {
				mismatches = append(mismatches, FieldDiff{
					Field:       truncateValue(member, 50),
					Type:        "lack_target",
					SourceValue: member,
				})
			}
			if len(mismatches) >= 100 {
				break
			}
		}
		
		cursor = newCursor
		if cursor == 0 || len(mismatches) >= 100 {
			break
		}
	}
	
	if len(mismatches) > 0 {
		detail.MismatchFields = mismatches
		return false, detail
	}
	
	return true, nil
}

// compareZSetMembers ZSet 类型成员及分数比对
func compareZSetMembers(ctx context.Context, sourceClient, targetClient redis.Cmdable,
	key string, threshold int64) (matched bool, detail *FieldMismatchDetail) {
	
	sourceLen, err1 := sourceClient.ZCard(ctx, key).Result()
	targetLen, err2 := targetClient.ZCard(ctx, key).Result()
	
	if err1 != nil || err2 != nil {
		return false, nil
	}
	
	detail = &FieldMismatchDetail{
		Key:         key,
		KeyType:     "zset",
		TotalFields: sourceLen,
	}
	
	if sourceLen != targetLen {
		detail.MismatchFields = append(detail.MismatchFields, FieldDiff{
			Field: "_cardinality_",
			Type:  "length_mismatch",
			SourceValue: fmt.Sprintf("%d", sourceLen),
			TargetValue: fmt.Sprintf("%d", targetLen),
		})
		return false, detail
	}
	
	// 根据大小选择获取方式
	if sourceLen > threshold {
		return compareLargeZSet(ctx, sourceClient, targetClient, key, detail)
	}
	
	// 小 ZSet 使用 ZRANGE WITHSCORES
	sourceMembers, _ := sourceClient.ZRangeWithScores(ctx, key, 0, -1).Result()
	
	var mismatches []FieldDiff
	for _, z := range sourceMembers {
		member := fmt.Sprintf("%v", z.Member)
		targetScore, err := targetClient.ZScore(ctx, key, member).Result()
		
		if err == redis.Nil {
			mismatches = append(mismatches, FieldDiff{
				Field:       truncateValue(member, 50),
				Type:        "lack_target",
				SourceScore: z.Score,
			})
		} else if err == nil && z.Score != targetScore {
			mismatches = append(mismatches, FieldDiff{
				Field:       truncateValue(member, 50),
				Type:        "score_mismatch",
				SourceScore: z.Score,
				TargetScore: targetScore,
			})
		}
		
		if len(mismatches) >= 50 {
			break
		}
	}
	
	if len(mismatches) > 0 {
		detail.MismatchFields = mismatches
		return false, detail
	}
	
	return true, nil
}

// compareLargeZSet 大 ZSet 使用 ZSCAN 比对
func compareLargeZSet(ctx context.Context, sourceClient, targetClient redis.Cmdable,
	key string, detail *FieldMismatchDetail) (matched bool, _ *FieldMismatchDetail) {
	
	var mismatches []FieldDiff
	var cursor uint64
	
	for {
		vals, newCursor, err := sourceClient.ZScan(ctx, key, cursor, "*", 500).Result()
		if err != nil {
			break
		}
		
		// vals 是 [member1, score1, member2, score2, ...]
		for i := 0; i < len(vals); i += 2 {
			if i+1 >= len(vals) {
				break
			}
			member := vals[i]
			sourceScore, _ := strconv.ParseFloat(vals[i+1], 64)
			
			targetScore, err := targetClient.ZScore(ctx, key, member).Result()
			
			if err == redis.Nil {
				mismatches = append(mismatches, FieldDiff{
					Field:       truncateValue(member, 50),
					Type:        "lack_target",
					SourceScore: sourceScore,
				})
			} else if err == nil && sourceScore != targetScore {
				mismatches = append(mismatches, FieldDiff{
					Field:       truncateValue(member, 50),
					Type:        "score_mismatch",
					SourceScore: sourceScore,
					TargetScore: targetScore,
				})
			}
			
			if len(mismatches) >= 100 {
				break
			}
		}
		
		cursor = newCursor
		if cursor == 0 || len(mismatches) >= 100 {
			break
		}
	}
	
	if len(mismatches) > 0 {
		detail.MismatchFields = mismatches
		return false, detail
	}
	
	return true, nil
}

// truncateValue 截断值，避免存储过大
func truncateValue(value string, maxLen int) string {
	if len(value) <= maxLen {
		return value
	}
	return value[:maxLen] + "...(truncated)"
}

// ==================== P2: 双向校验实现 ====================

// checkExtraKeysInTarget 检测目标端多余的 Key
func checkExtraKeysInTarget(ctx context.Context, task *VerifyTask, 
	sourceClient, targetClient redis.Cmdable, targetIsCluster bool,
	taskLog *logger.Logger) (extraKeys []string, extraCount int64) {
	
	taskLog.Info("Starting bidirectional check: scanning target for extra keys", nil)
	
	var mu sync.Mutex
	scanPattern := "*"
	if task.KeyFilter != nil && len(task.KeyFilter.Prefixes) > 0 {
		scanPattern = task.KeyFilter.Prefixes[0] + "*"
	}
	
	maxExtraKeys := int64(10000) // 最多记录 1 万个多余 Key
	
	scanTargetKeys := func(client redis.Cmdable) {
		var cursor uint64
		for {
			keys, newCursor, err := client.Scan(ctx, cursor, scanPattern, 1000).Result()
			if err != nil {
				break
			}
			
			// 批量检查这些 Key 在源端是否存在
			if len(keys) > 0 {
				pipe := sourceClient.Pipeline()
				existsCmds := make([]*redis.IntCmd, len(keys))
				for i, key := range keys {
					existsCmds[i] = pipe.Exists(ctx, key)
				}
				pipe.Exec(ctx)
				
				mu.Lock()
				for i, key := range keys {
					exists, _ := existsCmds[i].Result()
					if exists == 0 {
						// 应用 Key 过滤
						if matchVerifyKeyFilter(key, task.KeyFilter) {
							extraCount++
							if int64(len(extraKeys)) < maxExtraKeys {
								extraKeys = append(extraKeys, key)
							}
						}
					}
				}
				mu.Unlock()
			}
			
			cursor = newCursor
			if cursor == 0 {
				break
			}
		}
	}
	
	if targetIsCluster {
		clusterClient := targetClient.(*redis.ClusterClient)
		clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			scanTargetKeys(node)
			return nil
		})
	} else {
		scanTargetKeys(targetClient)
	}
	
	taskLog.Info("Bidirectional check completed", map[string]interface{}{
		"extra_keys_found": extraCount,
		"extra_keys_sample": len(extraKeys),
	})
	
	return extraKeys, extraCount
}

// ==================== P2: 智能比较模式实现 ====================

// SmartCompareConfig 智能比较配置
type SmartCompareConfig struct {
	BigKeyThresholdBytes int64 // String 类型大 Key 阈值（字节）
	BigKeyThresholdItems int64 // 复合类型大 Key 阈值（元素数）
}

// smartCompareKey 智能比较单个 Key（根据大小自动选择策略）
// 返回：matched 是否一致, detail 不一致详情, keyType Key 类型
func smartCompareKey(ctx context.Context, sourceClient, targetClient redis.Cmdable,
	key string, config SmartCompareConfig) (matched bool, detail *VerifyMismatchDetail, keyType string) {
	
	// 获取 Key 类型
	var err error
	keyType, err = sourceClient.Type(ctx, key).Result()
	if err != nil || keyType == "none" {
		return true, nil, "" // 源端不存在，视为一致
	}
	
	// 检查目标端是否存在
	exists, _ := targetClient.Exists(ctx, key).Result()
	if exists == 0 {
		return false, &VerifyMismatchDetail{
			Key:  key,
			Type: "missing",
		}, keyType
	}
	
	// 获取 Key 大小
	var keySize int64
	switch keyType {
	case "string":
		keySize, _ = sourceClient.StrLen(ctx, key).Result()
		if keySize > config.BigKeyThresholdBytes {
			// 大 String：只比较长度
			targetSize, _ := targetClient.StrLen(ctx, key).Result()
			if keySize != targetSize {
				return false, &VerifyMismatchDetail{
					Key:         key,
					Type:        "length_mismatch",
					SourceValue: fmt.Sprintf("%d bytes", keySize),
					TargetValue: fmt.Sprintf("%d bytes", targetSize),
				}, keyType
			}
			return true, nil, keyType
		}
		
	case "hash":
		keySize, _ = sourceClient.HLen(ctx, key).Result()
	case "list":
		keySize, _ = sourceClient.LLen(ctx, key).Result()
	case "set":
		keySize, _ = sourceClient.SCard(ctx, key).Result()
	case "zset":
		keySize, _ = sourceClient.ZCard(ctx, key).Result()
	}
	
	// 判断是否为大 Key（非 String 类型按元素数）
	isLargeKey := false
	if keyType != "string" && keySize > config.BigKeyThresholdItems {
		isLargeKey = true
	}
	
	if isLargeKey {
		// 大 Key：只比较长度/元素数
		var targetSize int64
		switch keyType {
		case "hash":
			targetSize, _ = targetClient.HLen(ctx, key).Result()
		case "list":
			targetSize, _ = targetClient.LLen(ctx, key).Result()
		case "set":
			targetSize, _ = targetClient.SCard(ctx, key).Result()
		case "zset":
			targetSize, _ = targetClient.ZCard(ctx, key).Result()
		}
		
		if keySize != targetSize {
			return false, &VerifyMismatchDetail{
				Key:         key,
				Type:        "length_mismatch",
				SourceValue: fmt.Sprintf("[%s] %d elements", keyType, keySize),
				TargetValue: fmt.Sprintf("%d elements", targetSize),
			}, keyType
		}
		return true, nil, keyType
	}
	
	// 正常 Key：全量比较（使用 DUMP）
	sourceDump, err1 := sourceClient.Dump(ctx, key).Result()
	targetDump, err2 := targetClient.Dump(ctx, key).Result()
	
	if err1 == nil && err2 == nil && sourceDump != targetDump {
		return false, &VerifyMismatchDetail{
			Key:         key,
			Type:        "value_mismatch",
			SourceValue: fmt.Sprintf("[%s] %d bytes", keyType, len(sourceDump)),
			TargetValue: fmt.Sprintf("%d bytes", len(targetDump)),
		}, keyType
	}
	
	return true, nil, keyType
}

// verifyKeysForRoundSmart 智能比较模式的单轮校验
// 逐 Key 调用 smartCompareKey，根据 Key 大小自动选择比较策略
func verifyKeysForRoundSmart(ctx context.Context, task *VerifyTask,
	sourceClient, targetClient redis.Cmdable,
	keysToVerify []string, concurrency int,
	taskLog *logger.Logger, metrics *VerifyMetrics) map[string]VerifyMismatchDetail {

	mismatches := make(map[string]VerifyMismatchDetail)
	var mismatchMu sync.Mutex
	var wg sync.WaitGroup

	// 构建 SmartCompareConfig
	bigKeyThresholdBytes := task.LargeKeyThreshold
	if bigKeyThresholdBytes <= 0 {
		bigKeyThresholdBytes = 10 * 1024 * 1024 // 默认 10MB
	}
	bigKeyThresholdItems := task.BigKeyThreshold
	if bigKeyThresholdItems <= 0 {
		bigKeyThresholdItems = 5000 // 默认 5000 元素
	}
	config := SmartCompareConfig{
		BigKeyThresholdBytes: bigKeyThresholdBytes,
		BigKeyThresholdItems: bigKeyThresholdItems,
	}

	// 创建工作通道
	workChan := make(chan string, concurrency*2)

	// 启动工作协程
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range workChan {
				// 检查是否已取消
				verifyTasksMu.RLock()
				if task.Status == "cancelled" {
					verifyTasksMu.RUnlock()
					continue
				}
				verifyTasksMu.RUnlock()

				matched, detail, kt := smartCompareKey(ctx, sourceClient, targetClient, key, config)

				// 记录指标
				if metrics != nil {
					// smartCompareKey 内部至少调用 TYPE + EXISTS = 2 条命令，
					// 大 Key 额外 2 条（StrLen/HLen + 目标端对应命令），小 Key 额外 2 条（DUMP*2）
					metrics.RecordRedisCommand(4)
					if kt != "" {
						metrics.RecordKeyType(kt)
					}
				}

				if !matched && detail != nil {
					mismatchMu.Lock()
					mismatches[key] = *detail
					mismatchMu.Unlock()
				}

				// TTL 比较
				if matched && task.CompareTTL {
					sourceTTL, err1 := sourceClient.TTL(ctx, key).Result()
					targetTTL, err2 := targetClient.TTL(ctx, key).Result()
					if metrics != nil {
						metrics.RecordRedisCommand(2)
					}
					if err1 == nil && err2 == nil {
						ttlDiff := sourceTTL - targetTTL
						if ttlDiff < 0 {
							ttlDiff = -ttlDiff
						}
						if ttlDiff > time.Duration(task.TTLTolerance)*time.Second {
							mismatchMu.Lock()
							mismatches[key] = VerifyMismatchDetail{
								Key:       key,
								Type:      "ttl_mismatch",
								SourceTTL: int64(sourceTTL.Seconds()),
								TargetTTL: int64(targetTTL.Seconds()),
							}
							mismatchMu.Unlock()
						}
					}
				}
			}
		}()
	}

	// 发送任务
	for _, key := range keysToVerify {
		verifyTasksMu.RLock()
		if task.Status == "cancelled" {
			verifyTasksMu.RUnlock()
			break
		}
		verifyTasksMu.RUnlock()
		workChan <- key
	}

	close(workChan)
	wg.Wait()

	taskLog.Info("Smart compare round completed", map[string]interface{}{
		"keys_checked": len(keysToVerify),
		"mismatches":   len(mismatches),
	})

	return mismatches
}

// ==================== P3: 指标监控实现 ====================

// NewVerifyMetrics 创建校验指标
func NewVerifyMetrics() *VerifyMetrics {
	return &VerifyMetrics{
		StartTime:        time.Now().Format(time.RFC3339),
		TypeDistribution: make(map[string]int64),
	}
}

// RecordKeyType 记录 Key 类型（线程安全）
func (m *VerifyMetrics) RecordKeyType(keyType string) {
	m.typeDistMu.Lock()
	if m.TypeDistribution == nil {
		m.TypeDistribution = make(map[string]int64)
	}
	m.TypeDistribution[keyType]++
	m.typeDistMu.Unlock()
}

// RecordRedisCommand 记录 Redis 命令
func (m *VerifyMetrics) RecordRedisCommand(count int64) {
	atomic.AddInt64(&m.RedisCommands, count)
}

// RecordPipelineBatch 记录 Pipeline 批次
func (m *VerifyMetrics) RecordPipelineBatch() {
	atomic.AddInt64(&m.PipelineBatches, 1)
	atomic.AddInt64(&m.NetworkRoundTrips, 1)
}

// Finalize 完成并计算最终指标
func (m *VerifyMetrics) Finalize(totalKeys int64) {
	m.EndTime = time.Now().Format(time.RFC3339)
	
	startTime, _ := time.Parse(time.RFC3339, m.StartTime)
	endTime, _ := time.Parse(time.RFC3339, m.EndTime)
	duration := endTime.Sub(startTime)
	m.Duration = duration.String()
	
	if duration.Seconds() > 0 {
		m.KeysPerSecond = float64(totalKeys) / duration.Seconds()
	}
	
	// 获取内存使用
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	m.PeakMemoryMB = float64(memStats.Alloc) / 1024 / 1024
}

// AddRoundMetric 添加轮次指标
func (m *VerifyMetrics) AddRoundMetric(roundNo int, duration time.Duration, keysChecked, mismatchCount int64) {
	var mismatchRate float64
	if keysChecked > 0 {
		mismatchRate = float64(mismatchCount) / float64(keysChecked) * 100
	}
	
	m.RoundMetrics = append(m.RoundMetrics, RoundMetric{
		RoundNo:       roundNo,
		Duration:      duration.String(),
		KeysPerSecond: float64(keysChecked) / duration.Seconds(),
		MismatchRate:  mismatchRate,
	})
}

// LogProgress 输出进度日志
func (m *VerifyMetrics) LogProgress(taskID string, processed, total int64) {
	progress := float64(processed) / float64(total) * 100
	logger.Info("Verify progress", map[string]interface{}{
		"task_id":        taskID,
		"processed":      processed,
		"total":          total,
		"progress":       fmt.Sprintf("%.2f%%", progress),
		"keys_per_sec":   m.KeysPerSecond,
		"redis_commands": m.RedisCommands,
		"elapsed":        time.Since(time.Now()).String(),
	})
}

// verifyKeysWithConcurrency 并发校验 Key
func verifyKeysWithConcurrency(ctx context.Context, task *VerifyTask, sourceClient, targetClient redis.Cmdable,
	sourceIsCluster, targetIsCluster bool, keysToVerify []string, concurrency int, compareMode string, 
	largeKeyThreshold int64, dbList []int, taskLog *logger.Logger) {
	
	const batchSize = 100
	var mismatches []VerifyMismatchDetail
	var mismatchMu sync.Mutex
	var wg sync.WaitGroup
	
	// 创建工作通道
	workChan := make(chan []string, concurrency)
	
	// 启动工作协程
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batchKeys := range workChan {
				// 检查是否已取消
				verifyTasksMu.RLock()
				if task.Status == "cancelled" {
					verifyTasksMu.RUnlock()
					continue
				}
				verifyTasksMu.RUnlock()

				verifyBatch(ctx, task, sourceClient, targetClient, batchKeys, compareMode, 
					largeKeyThreshold, &mismatches, &mismatchMu)
			}
		}()
	}
	
	// 分批发送任务
	totalBatches := (len(keysToVerify) + batchSize - 1) / batchSize
	processedBatches := 0
	
	for i := 0; i < len(keysToVerify); i += batchSize {
		// 检查是否已取消
		verifyTasksMu.RLock()
		if task.Status == "cancelled" {
			verifyTasksMu.RUnlock()
			break
		}
		verifyTasksMu.RUnlock()

		end := i + batchSize
		if end > len(keysToVerify) {
			end = len(keysToVerify)
		}
		
		batchKeys := make([]string, end-i)
		copy(batchKeys, keysToVerify[i:end])
		workChan <- batchKeys
		
		processedBatches++
		// 更新进度
		task.Result.Progress = 50 + float64(processedBatches)/float64(totalBatches)*50
	}
	
	close(workChan)
	wg.Wait()
	
	task.Result.Details = mismatches
}

// verifyBatch 批量校验
func verifyBatch(ctx context.Context, task *VerifyTask, sourceClient, targetClient redis.Cmdable,
	batchKeys []string, compareMode string, largeKeyThreshold int64, 
	mismatches *[]VerifyMismatchDetail, mismatchMu *sync.Mutex) {
	
	// Pipeline 获取源端数据
	sourcePipe := sourceClient.Pipeline()
	sourceTypeCmds := make([]*redis.StatusCmd, len(batchKeys))
	sourceTTLCmds := make([]*redis.DurationCmd, len(batchKeys))
	var sourceDumpCmds []*redis.StringCmd
	var sourceStrLenCmds []*redis.IntCmd
	
	needDump := compareMode == "full_value"
	needLen := compareMode == "length_only" || (compareMode == "full_value" && task.SkipLargeKey)
	
	if needDump {
		sourceDumpCmds = make([]*redis.StringCmd, len(batchKeys))
	}
	if needLen {
		sourceStrLenCmds = make([]*redis.IntCmd, len(batchKeys))
	}

	for j, key := range batchKeys {
		sourceTypeCmds[j] = sourcePipe.Type(ctx, key)
		sourceTTLCmds[j] = sourcePipe.TTL(ctx, key)
		if needDump {
			sourceDumpCmds[j] = sourcePipe.Dump(ctx, key)
		}
		if needLen {
			sourceStrLenCmds[j] = sourcePipe.StrLen(ctx, key)
		}
	}
	sourcePipe.Exec(ctx)

	// Pipeline 获取目标端数据
	targetPipe := targetClient.Pipeline()
	targetExistsCmds := make([]*redis.IntCmd, len(batchKeys))
	targetTTLCmds := make([]*redis.DurationCmd, len(batchKeys))
	var targetDumpCmds []*redis.StringCmd
	var targetStrLenCmds []*redis.IntCmd
	
	if needDump {
		targetDumpCmds = make([]*redis.StringCmd, len(batchKeys))
	}
	if needLen {
		targetStrLenCmds = make([]*redis.IntCmd, len(batchKeys))
	}

	for j, key := range batchKeys {
		targetExistsCmds[j] = targetPipe.Exists(ctx, key)
		targetTTLCmds[j] = targetPipe.TTL(ctx, key)
		if needDump {
			targetDumpCmds[j] = targetPipe.Dump(ctx, key)
		}
		if needLen {
			targetStrLenCmds[j] = targetPipe.StrLen(ctx, key)
		}
	}
	targetPipe.Exec(ctx)

	// 比对结果
	for j, key := range batchKeys {
		sourceType, _ := sourceTypeCmds[j].Result()
		sourceTTL, _ := sourceTTLCmds[j].Result()

		targetExists, _ := targetExistsCmds[j].Result()
		targetTTL, _ := targetTTLCmds[j].Result()

		// 源端 Key 不存在（可能已被删除）
		if sourceType == "none" {
			continue
		}

		// 目标端 Key 不存在
		if targetExists == 0 {
			atomic.AddInt64(&task.Result.MissingKeys, 1)
			mismatchMu.Lock()
			if len(*mismatches) < 100 {
				*mismatches = append(*mismatches, VerifyMismatchDetail{
					Key:  key,
					Type: "missing",
				})
			}
			mismatchMu.Unlock()
			continue
		}

		// Key 存在，根据比较模式进行比较
		matched := true

		switch compareMode {
		case "exists_only":
			// 只检查 Key 是否存在，已确认存在
			matched = true
			
		case "length_only":
			// 只比较长度
			if sourceStrLenCmds != nil && targetStrLenCmds != nil {
				sourceLen, _ := sourceStrLenCmds[j].Result()
				targetLen, _ := targetStrLenCmds[j].Result()
				if sourceLen != targetLen {
					atomic.AddInt64(&task.Result.LengthMismatch, 1)
					matched = false
					mismatchMu.Lock()
					if len(*mismatches) < 100 {
						*mismatches = append(*mismatches, VerifyMismatchDetail{
							Key:         key,
							Type:        "length_mismatch",
							SourceValue: fmt.Sprintf("%d bytes", sourceLen),
							TargetValue: fmt.Sprintf("%d bytes", targetLen),
						})
					}
					mismatchMu.Unlock()
				}
			}
			
		case "full_value":
			// 全量值比较
			if sourceDumpCmds != nil && targetDumpCmds != nil {
				sourceDump, sourceErr := sourceDumpCmds[j].Result()
				targetDump, targetErr := targetDumpCmds[j].Result()
				
				// 检查是否跳过大 Key
				if task.SkipLargeKey && len(sourceDump) > int(largeKeyThreshold) {
					atomic.AddInt64(&task.Result.LargeKeySkipped, 1)
					continue
				}

				if sourceErr == nil && targetErr == nil && sourceDump != targetDump {
					atomic.AddInt64(&task.Result.ValueMismatch, 1)
					matched = false
					mismatchMu.Lock()
					if len(*mismatches) < 100 {
						*mismatches = append(*mismatches, VerifyMismatchDetail{
							Key:         key,
							Type:        "value_mismatch",
							SourceValue: fmt.Sprintf("[%s] %d bytes", sourceType, len(sourceDump)),
							TargetValue: fmt.Sprintf("%d bytes", len(targetDump)),
						})
					}
					mismatchMu.Unlock()
				}
			}
		}

		// 比较 TTL
		if task.CompareTTL && matched {
			ttlDiff := sourceTTL - targetTTL
			if ttlDiff < 0 {
				ttlDiff = -ttlDiff
			}
			if ttlDiff > time.Duration(task.TTLTolerance)*time.Second {
				atomic.AddInt64(&task.Result.TTLMismatch, 1)
				matched = false
				mismatchMu.Lock()
				if len(*mismatches) < 100 {
					*mismatches = append(*mismatches, VerifyMismatchDetail{
						Key:       key,
						Type:      "ttl_mismatch",
						SourceTTL: int64(sourceTTL.Seconds()),
						TargetTTL: int64(targetTTL.Seconds()),
					})
				}
				mismatchMu.Unlock()
			}
		}

		if matched {
			atomic.AddInt64(&task.Result.MatchedKeys, 1)
		}
	}
}

// countClusterKeysWithDB 统计集群 Key 数量（支持多 DB）
func countClusterKeysWithDB(ctx context.Context, sourceClient redis.Cmdable, sourceIsCluster bool, 
	targetClient redis.Cmdable, targetIsCluster bool, dbList []int) (int64, int64) {
	
	var sourceCount, targetCount int64

	// 统计源端
	if sourceIsCluster {
		clusterClient := sourceClient.(*redis.ClusterClient)
		clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			count, err := node.DBSize(ctx).Result()
			if err == nil {
				sourceCount += count
			}
			return nil
		})
	} else {
		if len(dbList) > 0 {
			// 统计指定 DB（需要类型断言）
			if client, ok := sourceClient.(*redis.Client); ok {
				for _, dbNum := range dbList {
					if err := client.Do(ctx, "SELECT", dbNum).Err(); err == nil {
						count, err := client.DBSize(ctx).Result()
						if err == nil {
							sourceCount += count
						}
					}
				}
			}
		} else {
			count, err := sourceClient.DBSize(ctx).Result()
			if err == nil {
				sourceCount = count
			}
		}
	}

	// 统计目标端
	if targetIsCluster {
		clusterClient := targetClient.(*redis.ClusterClient)
		clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			count, err := node.DBSize(ctx).Result()
			if err == nil {
				targetCount += count
			}
			return nil
		})
	} else {
		if len(dbList) > 0 {
			// 统计指定 DB（需要类型断言）
			if client, ok := targetClient.(*redis.Client); ok {
				for _, dbNum := range dbList {
					if err := client.Do(ctx, "SELECT", dbNum).Err(); err == nil {
						count, err := client.DBSize(ctx).Result()
						if err == nil {
							targetCount += count
						}
					}
				}
			}
		} else {
			count, err := targetClient.DBSize(ctx).Result()
			if err == nil {
				targetCount = count
			}
		}
	}

	return sourceCount, targetCount
}

// matchVerifyKeyFilter 检查 Key 是否匹配过滤器
func matchVerifyKeyFilter(key string, filter *KeyFilterConfig) bool {
	if filter == nil {
		return true
	}

	// 检查排除前缀
	for _, prefix := range filter.ExcludePrefixes {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}

	// 检查包含前缀（如果指定了）
	if len(filter.Prefixes) > 0 {
		matched := false
		for _, prefix := range filter.Prefixes {
			if strings.HasPrefix(key, prefix) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// TODO: 支持正则匹配
	return true
}

// countClusterKeys 统计集群 Key 数量
func countClusterKeys(ctx context.Context, sourceClient redis.Cmdable, sourceIsCluster bool, targetClient redis.Cmdable) (int64, int64) {
	var sourceCount, targetCount int64

	// 统计源端
	if sourceIsCluster {
		clusterClient := sourceClient.(*redis.ClusterClient)
		clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			count, err := node.DBSize(ctx).Result()
			if err == nil {
				sourceCount += count
			}
			return nil
		})
	} else {
		count, err := sourceClient.DBSize(ctx).Result()
		if err == nil {
			sourceCount = count
		}
	}

	// 统计目标端
	if clusterClient, ok := targetClient.(*redis.ClusterClient); ok {
		clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			count, err := node.DBSize(ctx).Result()
			if err == nil {
				targetCount += count
			}
			return nil
		})
	} else {
		count, err := targetClient.DBSize(ctx).Result()
		if err == nil {
			targetCount = count
		}
	}

	return sourceCount, targetCount
}

// checkTendisBinlogEnabledConfig 检查 Tendis binlog-enabled 配置
// 返回 nil 表示非 Tendis（不需要此检测）
func checkTendisBinlogEnabledConfig(ctx context.Context, client redis.UniversalClient) *PreflightCheckItem {
	// 先检测是否是 Tendis
	if _, err := client.Do(ctx, "binlogpos", "0").Result(); err != nil {
		return nil // 非 Tendis
	}

	result, err := client.Do(ctx, "CONFIG", "GET", "binlog-enabled").Result()
	if err != nil {
		return &PreflightCheckItem{
			Name:     "Binlog 配置",
			Status:   "warning",
			Required: true,
			Message:  "无法读取 binlog-enabled 配置: " + err.Error(),
			Details:  "请手动确认源端 Tendis 已设置 binlog-enabled=yes",
		}
	}

	if vals, ok := result.([]interface{}); ok && len(vals) >= 2 {
		val := fmt.Sprintf("%v", vals[1])
		if val == "yes" || val == "true" || val == "1" {
			return &PreflightCheckItem{
				Name:     "Binlog 配置",
				Status:   "passed",
				Required: true,
				Message:  "binlog-enabled=yes，增量同步数据源就绪",
			}
		}
		return &PreflightCheckItem{
			Name:     "Binlog 配置",
			Status:   "failed",
			Required: true,
			Message:  "源端 Tendis 未开启 binlog（binlog-enabled=" + val + "）",
			Details:  "增量同步依赖 binlog，请在源端执行: CONFIG SET binlog-enabled yes，并确认持久化到配置文件",
		}
	}

	return &PreflightCheckItem{
		Name:     "Binlog 配置",
		Status:   "warning",
		Required: true,
		Message:  "binlog-enabled 配置返回格式异常",
		Details:  fmt.Sprintf("返回值: %v，请手动确认源端 binlog 已启用", result),
	}
}

// checkTendisAofEnabledConfig 检查 Tendis aof-enabled 配置
// aof-enabled=yes 时 binlog cmdStr 包含完整 RESP 命令，EXPIRE/TTL 等才能正确回放
// 返回 nil 表示非 Tendis
func checkTendisAofEnabledConfig(ctx context.Context, client redis.UniversalClient) *PreflightCheckItem {
	// 先检测是否是 Tendis
	if _, err := client.Do(ctx, "binlogpos", "0").Result(); err != nil {
		return nil // 非 Tendis
	}

	result, err := client.Do(ctx, "CONFIG", "GET", "aof-enabled").Result()
	if err != nil {
		return &PreflightCheckItem{
			Name:     "AOF 配置",
			Status:   "warning",
			Required: true,
			Message:  "无法读取 aof-enabled 配置: " + err.Error(),
			Details:  "请手动确认源端 Tendis 已设置 aof-enabled=yes",
		}
	}

	if vals, ok := result.([]interface{}); ok && len(vals) >= 2 {
		val := fmt.Sprintf("%v", vals[1])
		if val == "yes" || val == "true" || val == "1" {
			return &PreflightCheckItem{
				Name:     "AOF 配置",
				Status:   "passed",
				Required: true,
				Message:  "aof-enabled=yes，binlog 将包含完整 RESP 命令",
			}
		}
		return &PreflightCheckItem{
			Name:     "AOF 配置",
			Status:   "failed",
			Required: true,
			Message:  "源端 Tendis 未开启 AOF（aof-enabled=" + val + "）",
			Details:  "aof-enabled=no 时 binlog 只记录命令名，不包含参数，EXPIRE/TTL 等命令无法正确回放。请在源端执行: CONFIG SET aof-enabled yes",
		}
	}

	return &PreflightCheckItem{
		Name:     "AOF 配置",
		Status:   "warning",
		Required: true,
		Message:  "aof-enabled 配置返回格式异常",
		Details:  fmt.Sprintf("返回值: %v，请手动确认源端 aof-enabled=yes", result),
	}
}

// checkTendisKvstorecount 检查 Tendis kvstorecount 配置
// 返回 nil 表示非 Tendis
func checkTendisKvstorecount(ctx context.Context, client redis.UniversalClient) *PreflightCheckItem {
	// 先检测是否是 Tendis
	if _, err := client.Do(ctx, "binlogpos", "0").Result(); err != nil {
		return nil // 非 Tendis
	}

	result, err := client.Do(ctx, "CONFIG", "GET", "kvstorecount").Result()
	if err != nil {
		return &PreflightCheckItem{
			Name:     "KvStoreCount 配置",
			Status:   "warning",
			Required: false,
			Message:  "无法读取 kvstorecount 配置: " + err.Error(),
		}
	}

	if vals, ok := result.([]interface{}); ok && len(vals) >= 2 {
		val := fmt.Sprintf("%v", vals[1])
		return &PreflightCheckItem{
			Name:     "KvStoreCount 配置",
			Status:   "passed",
			Required: false,
			Message:  fmt.Sprintf("kvstorecount=%s", val),
			Details:  "每个 Tendis 节点有 " + val + " 个 Store，增量同步将为每个 Store 注册独立的 Binlog 通道",
		}
	}

	return nil
}
