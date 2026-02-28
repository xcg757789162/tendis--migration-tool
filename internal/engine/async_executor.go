// Package engine 提供异步命令执行器
// 灵感来源：Redis-Shake 的增量同步异步执行优化
//
// 核心优化：
// 1. 命令缓冲，不阻塞接收
// 2. 批量积累，减少网络往返
// 3. 定时刷新，保证低延迟
// 4. 失败重试，保证可靠性
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

// AsyncCommand 异步命令
type AsyncCommand struct {
	// 命令名称
	Name string
	// 参数
	Args []interface{}
	// Key（用于 Slot 路由）
	Key string
	// TTL（可选）
	TTL time.Duration
	// 时间戳
	Timestamp time.Time
	// 重试次数
	RetryCount int
}

// AsyncCommandExecutor 异步命令执行器
// 接收命令放入缓冲区，批量执行
type AsyncCommandExecutor struct {
	target *redis.ClusterClient
	
	// 配置
	bufferSize    int           // 缓冲区大小
	batchSize     int           // 批量大小
	flushInterval time.Duration // 刷新间隔
	maxRetries    int           // 最大重试次数
	
	// 缓冲区
	buffer     chan *AsyncCommand
	
	// 统计
	stats struct {
		receivedCommands atomic.Int64  // 收到的命令数
		executedCommands atomic.Int64  // 执行的命令数
		failedCommands   atomic.Int64  // 失败的命令数
		retriedCommands  atomic.Int64  // 重试的命令数
		flushedBatches   atomic.Int64  // 刷新的批次数
		currentLag       atomic.Int64  // 当前延迟（ms）
	}
	
	// 控制
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	running  atomic.Bool
	workers  int
}

// AsyncCommandExecutorConfig 配置
type AsyncCommandExecutorConfig struct {
	// 缓冲区大小（默认 10000）
	BufferSize int
	// 批量大小（默认 100）
	BatchSize int
	// 刷新间隔（默认 50ms）
	FlushInterval time.Duration
	// 最大重试次数（默认 3）
	MaxRetries int
	// Worker 数量（默认 4）
	Workers int
}

// DefaultAsyncCommandExecutorConfig 默认配置
func DefaultAsyncCommandExecutorConfig() *AsyncCommandExecutorConfig {
	return &AsyncCommandExecutorConfig{
		BufferSize:    10000,
		BatchSize:     100,
		FlushInterval: 50 * time.Millisecond,
		MaxRetries:    3,
		Workers:       4,
	}
}

// NewAsyncCommandExecutor 创建异步命令执行器
func NewAsyncCommandExecutor(target *redis.ClusterClient, config *AsyncCommandExecutorConfig) *AsyncCommandExecutor {
	if config == nil {
		config = DefaultAsyncCommandExecutorConfig()
	}
	
	if config.BufferSize <= 0 {
		config.BufferSize = 10000
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 50 * time.Millisecond
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.Workers <= 0 {
		config.Workers = 4
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	return &AsyncCommandExecutor{
		target:        target,
		bufferSize:    config.BufferSize,
		batchSize:     config.BatchSize,
		flushInterval: config.FlushInterval,
		maxRetries:    config.MaxRetries,
		workers:       config.Workers,
		buffer:        make(chan *AsyncCommand, config.BufferSize),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start 启动执行器
func (e *AsyncCommandExecutor) Start() {
	if !e.running.CompareAndSwap(false, true) {
		return
	}
	
	// 启动多个 Worker
	for i := 0; i < e.workers; i++ {
		e.wg.Add(1)
		go e.worker(i)
	}
	
	log.Printf("[AsyncCommandExecutor] Started with %d workers, batchSize=%d, flushInterval=%v",
		e.workers, e.batchSize, e.flushInterval)
}

// Stop 停止执行器
func (e *AsyncCommandExecutor) Stop() {
	if !e.running.CompareAndSwap(true, false) {
		return
	}
	
	// 关闭缓冲区（这会使 worker 退出）
	close(e.buffer)
	
	// 等待所有 worker 完成
	e.wg.Wait()
	
	e.cancel()
	
	log.Printf("[AsyncCommandExecutor] Stopped. Stats: received=%d, executed=%d, failed=%d, retried=%d",
		e.stats.receivedCommands.Load(),
		e.stats.executedCommands.Load(),
		e.stats.failedCommands.Load(),
		e.stats.retriedCommands.Load())
}

// Submit 提交命令
func (e *AsyncCommandExecutor) Submit(cmd *AsyncCommand) error {
	if !e.running.Load() {
		return fmt.Errorf("executor not running")
	}
	
	if cmd.Timestamp.IsZero() {
		cmd.Timestamp = time.Now()
	}
	
	select {
	case e.buffer <- cmd:
		e.stats.receivedCommands.Add(1)
		return nil
	default:
		// 缓冲区满，同步执行
		return e.executeSync(cmd)
	}
}

// SubmitBatch 批量提交命令
func (e *AsyncCommandExecutor) SubmitBatch(cmds []*AsyncCommand) error {
	for _, cmd := range cmds {
		if err := e.Submit(cmd); err != nil {
			return err
		}
	}
	return nil
}

// worker 工作协程
func (e *AsyncCommandExecutor) worker(id int) {
	defer e.wg.Done()
	
	batch := make([]*AsyncCommand, 0, e.batchSize)
	ticker := time.NewTicker(e.flushInterval)
	defer ticker.Stop()
	
	for {
		select {
		case cmd, ok := <-e.buffer:
			if !ok {
				// 缓冲区关闭，执行剩余批次
				if len(batch) > 0 {
					e.executeBatch(batch)
				}
				return
			}
			
			batch = append(batch, cmd)
			if len(batch) >= e.batchSize {
				e.executeBatch(batch)
				batch = batch[:0]
			}
			
		case <-ticker.C:
			if len(batch) > 0 {
				e.executeBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// executeBatch 执行一批命令
func (e *AsyncCommandExecutor) executeBatch(batch []*AsyncCommand) {
	if len(batch) == 0 {
		return
	}
	
	ctx := context.Background()
	
	// 按 Slot 分组
	slotGroups := e.groupBySlot(batch)
	
	var wg sync.WaitGroup
	failedCmds := make(chan *AsyncCommand, len(batch))
	
	for _, group := range slotGroups {
		wg.Add(1)
		go func(cmds []*AsyncCommand) {
			defer wg.Done()
			
			pipe := e.target.Pipeline()
			
			for _, cmd := range cmds {
				e.addToPipeline(ctx, pipe, cmd)
			}
			
			results, err := pipe.Exec(ctx)
			
			// 处理结果
			for i, cmd := range cmds {
				if err != nil || (i < len(results) && results[i].Err() != nil) {
					// 检查是否需要重试
					if cmd.RetryCount < e.maxRetries {
						cmd.RetryCount++
						failedCmds <- cmd
						e.stats.retriedCommands.Add(1)
					} else {
						e.stats.failedCommands.Add(1)
						log.Printf("[AsyncCommandExecutor] Command failed after %d retries: %s %v",
							cmd.RetryCount, cmd.Name, cmd.Args)
					}
				} else {
					e.stats.executedCommands.Add(1)
				}
			}
		}(group)
	}
	
	wg.Wait()
	close(failedCmds)
	
	// 重新提交失败的命令
	for cmd := range failedCmds {
		e.Submit(cmd)
	}
	
	e.stats.flushedBatches.Add(1)
	
	// 更新延迟统计
	if len(batch) > 0 {
		lag := time.Since(batch[0].Timestamp).Milliseconds()
		e.stats.currentLag.Store(lag)
	}
}

// executeSync 同步执行（缓冲区满时的降级方案）
func (e *AsyncCommandExecutor) executeSync(cmd *AsyncCommand) error {
	ctx := context.Background()
	
	args := make([]interface{}, 0, len(cmd.Args)+1)
	args = append(args, cmd.Name)
	args = append(args, cmd.Args...)
	
	err := e.target.Do(ctx, args...).Err()
	if err != nil {
		e.stats.failedCommands.Add(1)
		return err
	}
	
	e.stats.executedCommands.Add(1)
	return nil
}

// addToPipeline 添加命令到 Pipeline
func (e *AsyncCommandExecutor) addToPipeline(ctx context.Context, pipe redis.Pipeliner, cmd *AsyncCommand) {
	switch cmd.Name {
	case "SET":
		if len(cmd.Args) >= 2 {
			if cmd.TTL > 0 {
				pipe.Set(ctx, cmd.Args[0].(string), cmd.Args[1], cmd.TTL)
			} else {
				pipe.Set(ctx, cmd.Args[0].(string), cmd.Args[1], 0)
			}
		}
		
	case "DEL":
		if len(cmd.Args) >= 1 {
			keys := make([]string, len(cmd.Args))
			for i, arg := range cmd.Args {
				keys[i] = arg.(string)
			}
			pipe.Del(ctx, keys...)
		}
		
	case "HSET":
		if len(cmd.Args) >= 3 {
			pipe.HSet(ctx, cmd.Args[0].(string), cmd.Args[1:]...)
			// 【BUG-FIX TTL 一致性】非 string 类型也必须设置 TTL
			if cmd.TTL > 0 {
				pipe.PExpire(ctx, cmd.Args[0].(string), cmd.TTL)
			}
		}
		
	case "HDEL":
		if len(cmd.Args) >= 2 {
			key := cmd.Args[0].(string)
			fields := make([]string, len(cmd.Args)-1)
			for i, arg := range cmd.Args[1:] {
				fields[i] = arg.(string)
			}
			pipe.HDel(ctx, key, fields...)
		}
		
	case "SADD":
		if len(cmd.Args) >= 2 {
			pipe.SAdd(ctx, cmd.Args[0].(string), cmd.Args[1:]...)
			// 【BUG-FIX TTL 一致性】
			if cmd.TTL > 0 {
				pipe.PExpire(ctx, cmd.Args[0].(string), cmd.TTL)
			}
		}
		
	case "SREM":
		if len(cmd.Args) >= 2 {
			pipe.SRem(ctx, cmd.Args[0].(string), cmd.Args[1:]...)
		}
		
	case "ZADD":
		if len(cmd.Args) >= 3 {
			key := cmd.Args[0].(string)
			members := make([]*redis.Z, 0)
			for i := 1; i < len(cmd.Args); i += 2 {
				if i+1 < len(cmd.Args) {
					score, _ := cmd.Args[i].(float64)
					members = append(members, &redis.Z{
						Score:  score,
						Member: cmd.Args[i+1],
					})
				}
			}
			if len(members) > 0 {
				pipe.ZAdd(ctx, key, members...)
				// 【BUG-FIX TTL 一致性】
				if cmd.TTL > 0 {
					pipe.PExpire(ctx, key, cmd.TTL)
				}
			}
		}
		
	case "ZREM":
		if len(cmd.Args) >= 2 {
			pipe.ZRem(ctx, cmd.Args[0].(string), cmd.Args[1:]...)
		}
		
	case "LPUSH":
		if len(cmd.Args) >= 2 {
			pipe.LPush(ctx, cmd.Args[0].(string), cmd.Args[1:]...)
			// 【BUG-FIX TTL 一致性】
			if cmd.TTL > 0 {
				pipe.PExpire(ctx, cmd.Args[0].(string), cmd.TTL)
			}
		}
		
	case "RPUSH":
		if len(cmd.Args) >= 2 {
			pipe.RPush(ctx, cmd.Args[0].(string), cmd.Args[1:]...)
			// 【BUG-FIX TTL 一致性】
			if cmd.TTL > 0 {
				pipe.PExpire(ctx, cmd.Args[0].(string), cmd.TTL)
			}
		}
		
	case "LPOP":
		if len(cmd.Args) >= 1 {
			pipe.LPop(ctx, cmd.Args[0].(string))
		}
		
	case "RPOP":
		if len(cmd.Args) >= 1 {
			pipe.RPop(ctx, cmd.Args[0].(string))
		}
		
	case "EXPIRE":
		if len(cmd.Args) >= 2 {
			key := cmd.Args[0].(string)
			seconds, _ := cmd.Args[1].(int64)
			pipe.Expire(ctx, key, time.Duration(seconds)*time.Second)
		}
		
	case "PEXPIRE":
		if len(cmd.Args) >= 2 {
			key := cmd.Args[0].(string)
			ms, _ := cmd.Args[1].(int64)
			pipe.PExpire(ctx, key, time.Duration(ms)*time.Millisecond)
		}
		
	case "EXPIREAT":
		if len(cmd.Args) >= 2 {
			key := cmd.Args[0].(string)
			ts, _ := cmd.Args[1].(int64)
			pipe.ExpireAt(ctx, key, time.Unix(ts, 0))
		}
		
	case "PERSIST":
		if len(cmd.Args) >= 1 {
			pipe.Persist(ctx, cmd.Args[0].(string))
		}
		
	case "RENAME":
		if len(cmd.Args) >= 2 {
			pipe.Rename(ctx, cmd.Args[0].(string), cmd.Args[1].(string))
		}
		
	default:
		// 通用处理
		args := make([]interface{}, 0, len(cmd.Args)+1)
		args = append(args, cmd.Name)
		args = append(args, cmd.Args...)
		pipe.Do(ctx, args...)
	}
}

// groupBySlot 按 Slot 分组
func (e *AsyncCommandExecutor) groupBySlot(cmds []*AsyncCommand) map[int][]*AsyncCommand {
	groups := make(map[int][]*AsyncCommand)
	
	for _, cmd := range cmds {
		slot := 0
		if cmd.Key != "" {
			slot = calculateSlot(cmd.Key)
		} else if len(cmd.Args) > 0 {
			if key, ok := cmd.Args[0].(string); ok {
				slot = calculateSlot(key)
			}
		}
		groups[slot] = append(groups[slot], cmd)
	}
	
	return groups
}

// GetStats 获取统计信息
func (e *AsyncCommandExecutor) GetStats() map[string]int64 {
	return map[string]int64{
		"received_commands": e.stats.receivedCommands.Load(),
		"executed_commands": e.stats.executedCommands.Load(),
		"failed_commands":   e.stats.failedCommands.Load(),
		"retried_commands":  e.stats.retriedCommands.Load(),
		"flushed_batches":   e.stats.flushedBatches.Load(),
		"current_lag_ms":    e.stats.currentLag.Load(),
		"buffer_length":     int64(len(e.buffer)),
		"buffer_capacity":   int64(cap(e.buffer)),
	}
}

// GetLag 获取当前延迟（毫秒）
func (e *AsyncCommandExecutor) GetLag() int64 {
	return e.stats.currentLag.Load()
}

// GetBufferUsage 获取缓冲区使用率
func (e *AsyncCommandExecutor) GetBufferUsage() float64 {
	return float64(len(e.buffer)) / float64(cap(e.buffer))
}
