// Package engine 提供并发 Pipeline 写入器
// 灵感来源：Redis-Shake 的并发写入优化
//
// 核心优化：
// 1. 多 Pipeline 并发写入，突破单连接瓶颈
// 2. 批量积累命令，减少网络往返
// 3. 异步刷新，不阻塞主流程
package engine

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

// WriteCommand 写入命令
type WriteCommand struct {
	// 命令类型
	Type string
	// Key
	Key string
	// 参数
	Args []interface{}
	// TTL（0 表示不设置）
	TTL time.Duration
	// 回调（可选）
	Callback func(error)
}

// ConcurrentWriter 并发 Pipeline 写入器
// 使用多个 Pipeline 并发写入，提升吞吐量
type ConcurrentWriter struct {
	client       *redis.ClusterClient
	pipelines    []redis.Pipeliner
	pipelineMu   []sync.Mutex
	
	// 配置
	pipelineCount int           // Pipeline 数量
	batchSize     int           // 每个 Pipeline 积累的命令数
	flushInterval time.Duration // 强制刷新间隔
	
	// 状态
	pendingCount  []int64       // 每个 Pipeline 待处理命令数
	counter       int64         // 轮询计数器
	
	// 统计
	stats struct {
		totalCommands   atomic.Int64 // 总命令数
		flushedBatches  atomic.Int64 // 刷新批次数
		totalBytes      atomic.Int64 // 总字节数
		errors          atomic.Int64 // 错误数
	}
	
	// 控制
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	running   atomic.Bool
}

// ConcurrentWriterConfig 配置
type ConcurrentWriterConfig struct {
	// Pipeline 数量（默认 4）
	PipelineCount int
	// 每个 Pipeline 批量大小（默认 100）
	BatchSize int
	// 强制刷新间隔（默认 100ms）
	FlushInterval time.Duration
}

// DefaultConcurrentWriterConfig 默认配置
func DefaultConcurrentWriterConfig() *ConcurrentWriterConfig {
	return &ConcurrentWriterConfig{
		PipelineCount: 4,
		BatchSize:     100,
		FlushInterval: 100 * time.Millisecond,
	}
}

// NewConcurrentWriter 创建并发写入器
func NewConcurrentWriter(client *redis.ClusterClient, config *ConcurrentWriterConfig) *ConcurrentWriter {
	if config == nil {
		config = DefaultConcurrentWriterConfig()
	}
	
	if config.PipelineCount <= 0 {
		config.PipelineCount = 4
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 100 * time.Millisecond
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	w := &ConcurrentWriter{
		client:        client,
		pipelineCount: config.PipelineCount,
		batchSize:     config.BatchSize,
		flushInterval: config.FlushInterval,
		pipelines:     make([]redis.Pipeliner, config.PipelineCount),
		pipelineMu:    make([]sync.Mutex, config.PipelineCount),
		pendingCount:  make([]int64, config.PipelineCount),
		ctx:           ctx,
		cancel:        cancel,
	}
	
	// 初始化 Pipelines
	for i := 0; i < config.PipelineCount; i++ {
		w.pipelines[i] = client.Pipeline()
	}
	
	return w
}

// Start 启动后台刷新
func (w *ConcurrentWriter) Start() {
	if !w.running.CompareAndSwap(false, true) {
		return // 已经在运行
	}
	
	w.wg.Add(1)
	go w.flushLoop()
	
	log.Printf("[ConcurrentWriter] Started with %d pipelines, batchSize=%d, flushInterval=%v",
		w.pipelineCount, w.batchSize, w.flushInterval)
}

// Stop 停止并刷新所有待处理命令
func (w *ConcurrentWriter) Stop() {
	if !w.running.CompareAndSwap(true, false) {
		return
	}
	
	w.cancel()
	w.wg.Wait()
	
	// 最终刷新
	w.FlushAll()
	
	log.Printf("[ConcurrentWriter] Stopped. Stats: commands=%d, batches=%d, errors=%d",
		w.stats.totalCommands.Load(),
		w.stats.flushedBatches.Load(),
		w.stats.errors.Load())
}

// flushLoop 后台定时刷新
func (w *ConcurrentWriter) flushLoop() {
	defer w.wg.Done()
	
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.FlushAll()
		}
	}
}

// Write 写入命令（自动选择 Pipeline）
func (w *ConcurrentWriter) Write(ctx context.Context, cmd *WriteCommand) error {
	// 轮询选择 Pipeline
	idx := int(atomic.AddInt64(&w.counter, 1) % int64(w.pipelineCount))
	
	w.pipelineMu[idx].Lock()
	defer w.pipelineMu[idx].Unlock()
	
	// 添加命令到 Pipeline
	if err := w.addCommand(ctx, w.pipelines[idx], cmd); err != nil {
		w.stats.errors.Add(1)
		return err
	}
	
	w.pendingCount[idx]++
	w.stats.totalCommands.Add(1)
	
	// 达到批量阈值时刷新
	if w.pendingCount[idx] >= int64(w.batchSize) {
		w.flushPipeline(ctx, idx)
	}
	
	return nil
}

// WriteBatch 批量写入命令
func (w *ConcurrentWriter) WriteBatch(ctx context.Context, cmds []*WriteCommand) error {
	if len(cmds) == 0 {
		return nil
	}
	
	// 按 Slot 分组，避免 CROSSSLOT 错误
	slotGroups := w.groupBySlot(cmds)
	
	var wg sync.WaitGroup
	errChan := make(chan error, len(slotGroups))
	
	for _, group := range slotGroups {
		wg.Add(1)
		go func(commands []*WriteCommand) {
			defer wg.Done()
			
			// 选择一个 Pipeline
			idx := int(atomic.AddInt64(&w.counter, 1) % int64(w.pipelineCount))
			
			w.pipelineMu[idx].Lock()
			
			for _, cmd := range commands {
				if err := w.addCommand(ctx, w.pipelines[idx], cmd); err != nil {
					w.stats.errors.Add(1)
					continue
				}
				w.pendingCount[idx]++
				w.stats.totalCommands.Add(1)
			}
			
			// 立即刷新这一批
			if err := w.flushPipelineUnlocked(ctx, idx); err != nil {
				errChan <- err
			}
			
			w.pipelineMu[idx].Unlock()
		}(group)
	}
	
	wg.Wait()
	close(errChan)
	
	// 收集错误
	var lastErr error
	for err := range errChan {
		if err != nil {
			lastErr = err
		}
	}
	
	return lastErr
}

// addCommand 添加命令到 Pipeline
func (w *ConcurrentWriter) addCommand(ctx context.Context, pipe redis.Pipeliner, cmd *WriteCommand) error {
	switch cmd.Type {
	case "SET":
		if cmd.TTL > 0 {
			pipe.Set(ctx, cmd.Key, cmd.Args[0], cmd.TTL)
		} else {
			pipe.Set(ctx, cmd.Key, cmd.Args[0], 0)
		}
		
	case "HSET":
		if len(cmd.Args) >= 2 {
			pipe.HSet(ctx, cmd.Key, cmd.Args...)
			// 【BUG-FIX TTL 一致性】非 string 类型也必须设置 TTL
			if cmd.TTL > 0 {
				pipe.PExpire(ctx, cmd.Key, cmd.TTL)
			}
		}
		
	case "LPUSH":
		pipe.LPush(ctx, cmd.Key, cmd.Args...)
		// 【BUG-FIX TTL 一致性】
		if cmd.TTL > 0 {
			pipe.PExpire(ctx, cmd.Key, cmd.TTL)
		}
		
	case "RPUSH":
		pipe.RPush(ctx, cmd.Key, cmd.Args...)
		// 【BUG-FIX TTL 一致性】
		if cmd.TTL > 0 {
			pipe.PExpire(ctx, cmd.Key, cmd.TTL)
		}
		
	case "SADD":
		pipe.SAdd(ctx, cmd.Key, cmd.Args...)
		// 【BUG-FIX TTL 一致性】
		if cmd.TTL > 0 {
			pipe.PExpire(ctx, cmd.Key, cmd.TTL)
		}
		
	case "ZADD":
		// 需要转换为 Z 结构
		members := make([]*redis.Z, 0, len(cmd.Args)/2)
		for i := 0; i < len(cmd.Args); i += 2 {
			if i+1 < len(cmd.Args) {
				score, _ := cmd.Args[i].(float64)
				members = append(members, &redis.Z{
					Score:  score,
					Member: cmd.Args[i+1],
				})
			}
		}
		if len(members) > 0 {
			pipe.ZAdd(ctx, cmd.Key, members...)
			// 【BUG-FIX TTL 一致性】
			if cmd.TTL > 0 {
				pipe.PExpire(ctx, cmd.Key, cmd.TTL)
			}
		}
		
	case "DEL":
		pipe.Del(ctx, cmd.Key)
		
	case "EXPIRE":
		if len(cmd.Args) >= 1 {
			ttl, _ := cmd.Args[0].(time.Duration)
			pipe.Expire(ctx, cmd.Key, ttl)
		}
		
	case "RESTORE":
		// RESTORE key ttl serialized-value [REPLACE]
		if len(cmd.Args) >= 2 {
			ttl, _ := cmd.Args[0].(time.Duration)
			data, _ := cmd.Args[1].(string)
			pipe.Restore(ctx, cmd.Key, ttl, data)
		}
		
	case "RAW":
		// 原始命令
		if len(cmd.Args) > 0 {
			pipe.Do(ctx, cmd.Args...)
		}
		
	default:
		return fmt.Errorf("unsupported command type: %s", cmd.Type)
	}
	
	return nil
}

// flushPipeline 刷新指定 Pipeline（需要持有锁）
func (w *ConcurrentWriter) flushPipeline(ctx context.Context, idx int) error {
	return w.flushPipelineUnlocked(ctx, idx)
}

// flushPipelineUnlocked 刷新 Pipeline（调用者需持有锁）
func (w *ConcurrentWriter) flushPipelineUnlocked(ctx context.Context, idx int) error {
	if w.pendingCount[idx] == 0 {
		return nil
	}
	
	_, err := w.pipelines[idx].Exec(ctx)
	if err != nil && err != redis.Nil {
		w.stats.errors.Add(1)
		log.Printf("[ConcurrentWriter] Pipeline %d exec error: %v", idx, err)
		// 重新创建 Pipeline
		w.pipelines[idx] = w.client.Pipeline()
		return err
	}
	
	w.stats.flushedBatches.Add(1)
	w.pendingCount[idx] = 0
	
	// 重新创建 Pipeline
	w.pipelines[idx] = w.client.Pipeline()
	
	return nil
}

// FlushAll 刷新所有 Pipeline
func (w *ConcurrentWriter) FlushAll() {
	ctx := context.Background()
	
	for i := 0; i < w.pipelineCount; i++ {
		w.pipelineMu[i].Lock()
		w.flushPipelineUnlocked(ctx, i)
		w.pipelineMu[i].Unlock()
	}
}

// groupBySlot 按 Slot 分组
func (w *ConcurrentWriter) groupBySlot(cmds []*WriteCommand) map[int][]*WriteCommand {
	groups := make(map[int][]*WriteCommand)
	
	for _, cmd := range cmds {
		slot := calculateSlot(cmd.Key)
		groups[slot] = append(groups[slot], cmd)
	}
	
	return groups
}

// GetStats 获取统计信息
func (w *ConcurrentWriter) GetStats() map[string]int64 {
	return map[string]int64{
		"total_commands":  w.stats.totalCommands.Load(),
		"flushed_batches": w.stats.flushedBatches.Load(),
		"total_bytes":     w.stats.totalBytes.Load(),
		"errors":          w.stats.errors.Load(),
	}
}

// GetPendingCount 获取待处理命令数
func (w *ConcurrentWriter) GetPendingCount() int64 {
	var total int64
	for i := 0; i < w.pipelineCount; i++ {
		total += atomic.LoadInt64(&w.pendingCount[i])
	}
	return total
}
