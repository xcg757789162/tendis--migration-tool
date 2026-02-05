package binlog

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// BinlogSyncer Binlog 同步器
// 用于从源端 Tendis 读取 Binlog 并同步到目标端
type BinlogSyncer struct {
	source *redis.ClusterClient
	target *redis.ClusterClient

	// 配置
	config *SyncerConfig

	// 状态
	running   atomic.Bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	// 进度
	lastBinlogId atomic.Uint64
	lastSyncTime atomic.Int64

	// 统计
	stats *SyncerStats

	// 命令转换器
	converter *CommandConverter

	// 错误处理
	onError func(error)

	// 日志
	logger *log.Logger
}

// SyncerConfig 同步器配置
type SyncerConfig struct {
	// 源端节点地址列表 (Tendis 集群)
	SourceAddrs []string
	SourcePassword string

	// 目标端节点地址列表
	TargetAddrs []string
	TargetPassword string

	// Key 过滤
	IncludePrefixes []string
	ExcludePrefixes []string

	// 性能配置
	BatchSize       int           // 批处理大小，默认 100
	BatchTimeout    time.Duration // 批处理超时，默认 100ms
	Workers         int           // 工作协程数，默认 4
	RetryTimes      int           // 重试次数，默认 3
	RetryInterval   time.Duration // 重试间隔，默认 1s

	// 断点续传
	CheckpointFile  string        // 断点文件路径
	CheckpointInterval time.Duration // 保存间隔，默认 30s

	// 冲突处理
	ConflictPolicy  string // skip/replace/error
}

// DefaultSyncerConfig 默认配置
func DefaultSyncerConfig() *SyncerConfig {
	return &SyncerConfig{
		BatchSize:          100,
		BatchTimeout:       100 * time.Millisecond,
		Workers:            4,
		RetryTimes:         3,
		RetryInterval:      time.Second,
		CheckpointInterval: 30 * time.Second,
		ConflictPolicy:     "skip",
	}
}

// SyncerStats 同步统计
type SyncerStats struct {
	mu sync.Mutex

	TotalRecords   int64 // 总记录数
	ProcessedRecords int64 // 已处理记录数
	SkippedRecords int64 // 跳过记录数 (前缀过滤)
	ErrorRecords   int64 // 错误记录数
	ConflictRecords int64 // 冲突记录数

	TotalCommands  int64 // 总命令数
	ExecutedCommands int64 // 已执行命令数

	BytesRead    int64 // 读取字节数
	BytesWritten int64 // 写入字节数

	StartTime    time.Time
	LastSyncTime time.Time
}

// NewBinlogSyncer 创建 Binlog 同步器
func NewBinlogSyncer(config *SyncerConfig) (*BinlogSyncer, error) {
	if config == nil {
		config = DefaultSyncerConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	syncer := &BinlogSyncer{
		config:    config,
		ctx:       ctx,
		cancel:    cancel,
		converter: NewCommandConverter(config.IncludePrefixes, config.ExcludePrefixes),
		stats:     &SyncerStats{StartTime: time.Now()},
		logger:    log.Default(),
	}

	// 创建源端连接
	source := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    config.SourceAddrs,
		Password: config.SourcePassword,
	})
	if err := source.Ping(ctx).Err(); err != nil {
		cancel()
		return nil, fmt.Errorf("connect source failed: %w", err)
	}
	syncer.source = source

	// 创建目标端连接
	target := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    config.TargetAddrs,
		Password: config.TargetPassword,
	})
	if err := target.Ping(ctx).Err(); err != nil {
		cancel()
		source.Close()
		return nil, fmt.Errorf("connect target failed: %w", err)
	}
	syncer.target = target

	return syncer, nil
}

// Start 启动同步
func (s *BinlogSyncer) Start() error {
	if s.running.Load() {
		return fmt.Errorf("syncer already running")
	}

	s.running.Store(true)

	// 加载断点
	if err := s.loadCheckpoint(); err != nil {
		s.logger.Printf("load checkpoint failed (will start from beginning): %v", err)
	}

	// 启动断点保存协程
	s.wg.Add(1)
	go s.checkpointLoop()

	// 启动同步协程
	s.wg.Add(1)
	go s.syncLoop()

	return nil
}

// Stop 停止同步
func (s *BinlogSyncer) Stop() error {
	if !s.running.Load() {
		return nil
	}

	s.running.Store(false)
	s.cancel()

	// 等待所有协程退出
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		return fmt.Errorf("stop timeout")
	}

	// 保存最终断点
	s.saveCheckpoint()

	// 关闭连接
	s.source.Close()
	s.target.Close()

	return nil
}

// syncLoop 同步主循环
func (s *BinlogSyncer) syncLoop() {
	defer s.wg.Done()

	for s.running.Load() {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// 同步每个源节点
		err := s.source.ForEachMaster(s.ctx, func(ctx context.Context, node *redis.Client) error {
			return s.syncNode(ctx, node)
		})

		if err != nil {
			if s.onError != nil {
				s.onError(err)
			}
			s.logger.Printf("sync error: %v", err)
		}

		// 短暂休眠避免空转
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// syncNode 同步单个节点
// 注意：这里使用模拟的 Binlog 读取，实际需要通过 PSYNC 或 Binlog 文件
func (s *BinlogSyncer) syncNode(ctx context.Context, node *redis.Client) error {
	// TODO: 实际实现需要：
	// 1. 使用 PSYNC 命令建立复制连接
	// 2. 或者读取 Binlog 文件
	// 3. 解析 Binlog 记录并转换为命令
	// 4. 应用到目标端

	// 当前使用 SCAN + DUMP/RESTORE 作为降级方案
	// 这不是真正的 Binlog 同步，仅作为示例框架

	return nil
}

// ApplyRecord 应用单条 Binlog 记录
func (s *BinlogSyncer) ApplyRecord(ctx context.Context, record *BinlogRecord) error {
	if record == nil {
		return nil
	}

	// 更新统计
	s.stats.mu.Lock()
	s.stats.TotalRecords++
	s.stats.mu.Unlock()

	// 转换为命令
	commands, err := s.converter.ConvertRecord(record)
	if err != nil {
		s.stats.mu.Lock()
		s.stats.ErrorRecords++
		s.stats.mu.Unlock()
		return fmt.Errorf("convert record failed: %w", err)
	}

	if len(commands) == 0 {
		s.stats.mu.Lock()
		s.stats.SkippedRecords++
		s.stats.mu.Unlock()
		return nil
	}

	// 执行命令
	for _, cmd := range commands {
		if err := s.executeCommand(ctx, cmd); err != nil {
			s.stats.mu.Lock()
			s.stats.ErrorRecords++
			s.stats.mu.Unlock()
			
			// 根据配置决定是否继续
			if s.config.ConflictPolicy == "error" {
				return err
			}
			continue
		}

		s.stats.mu.Lock()
		s.stats.ExecutedCommands++
		s.stats.mu.Unlock()
	}

	s.stats.mu.Lock()
	s.stats.ProcessedRecords++
	s.stats.LastSyncTime = time.Now()
	s.stats.mu.Unlock()

	// 更新 Binlog ID
	if record.Key != nil {
		s.lastBinlogId.Store(record.Key.BinlogId)
	}
	s.lastSyncTime.Store(time.Now().UnixMilli())

	return nil
}

// ApplyCommands 批量应用命令
func (s *BinlogSyncer) ApplyCommands(ctx context.Context, commands []*Command) error {
	if len(commands) == 0 {
		return nil
	}

	// 使用 Pipeline 批量执行
	pipe := s.target.Pipeline()
	
	for _, cmd := range commands {
		if err := s.addToPipeline(ctx, pipe, cmd); err != nil {
			s.logger.Printf("add to pipeline failed: %v", err)
			continue
		}
	}

	// 执行 Pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pipeline exec failed: %w", err)
	}

	return nil
}

// executeCommand 执行单条命令
func (s *BinlogSyncer) executeCommand(ctx context.Context, cmd *Command) error {
	// 重试逻辑
	var lastErr error
	for i := 0; i <= s.config.RetryTimes; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.config.RetryInterval):
			}
		}

		err := s.doExecuteCommand(ctx, cmd)
		if err == nil {
			return nil
		}
		lastErr = err

		// 检查是否是冲突错误
		if isConflictError(err) {
			s.stats.mu.Lock()
			s.stats.ConflictRecords++
			s.stats.mu.Unlock()

			if s.config.ConflictPolicy == "skip" {
				return nil
			} else if s.config.ConflictPolicy == "replace" {
				// 先删除再写入
				s.target.Del(ctx, cmd.Key)
				continue
			}
		}
	}

	return lastErr
}

// doExecuteCommand 实际执行命令
func (s *BinlogSyncer) doExecuteCommand(ctx context.Context, cmd *Command) error {
	args := make([]interface{}, len(cmd.Args)+1)
	args[0] = cmd.Name
	for i, arg := range cmd.Args {
		args[i+1] = arg
	}

	return s.target.Do(ctx, args...).Err()
}

// addToPipeline 添加命令到 Pipeline
func (s *BinlogSyncer) addToPipeline(ctx context.Context, pipe redis.Pipeliner, cmd *Command) error {
	switch cmd.Name {
	case "SET":
		if len(cmd.Args) >= 2 {
			pipe.Set(ctx, cmd.Args[0], cmd.Args[1], 0)
			if cmd.TTL > 0 {
				pipe.PExpireAt(ctx, cmd.Args[0], time.UnixMilli(cmd.TTL))
			}
		}
	case "DEL":
		if len(cmd.Args) >= 1 {
			pipe.Del(ctx, cmd.Args[0])
		}
	case "HSET":
		if len(cmd.Args) >= 3 {
			pipe.HSet(ctx, cmd.Args[0], cmd.Args[1], cmd.Args[2])
		}
	case "HDEL":
		if len(cmd.Args) >= 2 {
			pipe.HDel(ctx, cmd.Args[0], cmd.Args[1])
		}
	case "SADD":
		if len(cmd.Args) >= 2 {
			pipe.SAdd(ctx, cmd.Args[0], cmd.Args[1])
		}
	case "SREM":
		if len(cmd.Args) >= 2 {
			pipe.SRem(ctx, cmd.Args[0], cmd.Args[1])
		}
	case "ZADD":
		if len(cmd.Args) >= 3 {
			// score, member
			pipe.Do(ctx, "ZADD", cmd.Args[0], cmd.Args[1], cmd.Args[2])
		}
	case "ZREM":
		if len(cmd.Args) >= 2 {
			pipe.ZRem(ctx, cmd.Args[0], cmd.Args[1])
		}
	case "RPUSH":
		if len(cmd.Args) >= 2 {
			pipe.RPush(ctx, cmd.Args[0], cmd.Args[1])
		}
	case "LREM":
		if len(cmd.Args) >= 3 {
			pipe.LRem(ctx, cmd.Args[0], 0, cmd.Args[2])
		}
	case "PEXPIREAT":
		if len(cmd.Args) >= 2 {
			pipe.Do(ctx, "PEXPIREAT", cmd.Args[0], cmd.Args[1])
		}
	case "PERSIST":
		if len(cmd.Args) >= 1 {
			pipe.Persist(ctx, cmd.Args[0])
		}
	default:
		// 通用命令
		args := make([]interface{}, len(cmd.Args)+1)
		args[0] = cmd.Name
		for i, arg := range cmd.Args {
			args[i+1] = arg
		}
		pipe.Do(ctx, args...)
	}

	return nil
}

// checkpointLoop 断点保存循环
func (s *BinlogSyncer) checkpointLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.config.CheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.saveCheckpoint()
		}
	}
}

// loadCheckpoint 加载断点
func (s *BinlogSyncer) loadCheckpoint() error {
	// TODO: 从文件加载断点
	// 格式: binlogId, timestamp
	return nil
}

// saveCheckpoint 保存断点
func (s *BinlogSyncer) saveCheckpoint() error {
	// TODO: 保存断点到文件
	// 格式: binlogId, timestamp
	return nil
}

// GetStats 获取统计信息
func (s *BinlogSyncer) GetStats() *SyncerStats {
	s.stats.mu.Lock()
	defer s.stats.mu.Unlock()
	
	// 返回副本
	return &SyncerStats{
		TotalRecords:     s.stats.TotalRecords,
		ProcessedRecords: s.stats.ProcessedRecords,
		SkippedRecords:   s.stats.SkippedRecords,
		ErrorRecords:     s.stats.ErrorRecords,
		ConflictRecords:  s.stats.ConflictRecords,
		TotalCommands:    s.stats.TotalCommands,
		ExecutedCommands: s.stats.ExecutedCommands,
		BytesRead:        s.stats.BytesRead,
		BytesWritten:     s.stats.BytesWritten,
		StartTime:        s.stats.StartTime,
		LastSyncTime:     s.stats.LastSyncTime,
	}
}

// GetLastBinlogId 获取最后处理的 Binlog ID
func (s *BinlogSyncer) GetLastBinlogId() uint64 {
	return s.lastBinlogId.Load()
}

// SetOnError 设置错误回调
func (s *BinlogSyncer) SetOnError(handler func(error)) {
	s.onError = handler
}

// isConflictError 检查是否是冲突错误
func isConflictError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// Redis 常见冲突错误
	return containsAny(errStr, "BUSYKEY", "already exists", "WRONGTYPE")
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}

// BinlogFileSyncer 基于文件的 Binlog 同步器
type BinlogFileSyncer struct {
	*BinlogSyncer
	parser    *BinlogParser
	filePath  string
}

// NewBinlogFileSyncer 创建基于文件的同步器
func NewBinlogFileSyncer(filePath string, config *SyncerConfig) (*BinlogFileSyncer, error) {
	syncer, err := NewBinlogSyncer(config)
	if err != nil {
		return nil, err
	}

	parser, err := NewBinlogParser(filePath)
	if err != nil {
		syncer.Stop()
		return nil, fmt.Errorf("create parser failed: %w", err)
	}

	return &BinlogFileSyncer{
		BinlogSyncer: syncer,
		parser:       parser,
		filePath:     filePath,
	}, nil
}

// SyncFromFile 从文件同步
func (s *BinlogFileSyncer) SyncFromFile(ctx context.Context) error {
	iterator := NewBinlogIterator(s.parser)
	batcher := NewCommandBatcher(s.config.BatchSize, func(commands []*Command) error {
		return s.ApplyCommands(ctx, commands)
	})

	for iterator.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		record := iterator.Record()
		commands, err := s.converter.ConvertRecord(record)
		if err != nil {
			s.logger.Printf("convert record failed: %v", err)
			continue
		}

		for _, cmd := range commands {
			if err := batcher.Add(cmd); err != nil {
				s.logger.Printf("batch add failed: %v", err)
			}
		}
	}

	// 刷新剩余批次
	if err := batcher.Flush(); err != nil {
		return fmt.Errorf("flush final batch failed: %w", err)
	}

	if err := iterator.Err(); err != nil && err != io.EOF {
		return fmt.Errorf("iterator error: %w", err)
	}

	return nil
}

// Close 关闭同步器
func (s *BinlogFileSyncer) Close() error {
	s.parser.Close()
	return s.Stop()
}
