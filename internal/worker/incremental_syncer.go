package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"tendis-migrate/internal/storage"
	"tendis-migrate/pkg/logger"
)

// IncrementalSyncer 增量同步器
type IncrementalSyncer struct {
	taskID        string
	workerID      string
	queues        map[string]*storage.LevelDBQueue // nodeAddr -> queue
	sourceCluster *redis.ClusterClient
	targetCluster *redis.ClusterClient
	logger        *logger.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	running       bool
	mutex         sync.RWMutex

	// 统计（使用 atomic 保证并发安全）
	keysProcessed atomic.Int64
	keysSkipped   atomic.Int64
	keysFailed    atomic.Int64
	batchSize     int
}

// NewIncrementalSyncer 创建增量同步器
func NewIncrementalSyncer(
	taskID string,
	workerID string,
	queues map[string]*storage.LevelDBQueue,
	sourceCluster *redis.ClusterClient,
	targetCluster *redis.ClusterClient,
	taskLogger *logger.Logger,
	batchSize int,
) *IncrementalSyncer {
	ctx, cancel := context.WithCancel(context.Background())
	return &IncrementalSyncer{
		taskID:        taskID,
		workerID:      workerID,
		queues:        queues,
		sourceCluster: sourceCluster,
		targetCluster: targetCluster,
		logger:        taskLogger,
		ctx:           ctx,
		cancel:        cancel,
		batchSize:     batchSize,
	}
}

// Start 启动增量同步
func (is *IncrementalSyncer) Start() error {
	is.mutex.Lock()
	if is.running {
		is.mutex.Unlock()
		return fmt.Errorf("incremental syncer already running")
	}
	is.running = true
	is.mutex.Unlock()

	is.logger.Info("Starting incremental syncer", map[string]interface{}{
		"worker_id":  is.workerID,
		"batch_size": is.batchSize,
	})

	// 为每个队列启动消费 goroutine
	for nodeAddr, queue := range is.queues {
		is.wg.Add(1)
		go is.consumeQueue(nodeAddr, queue)
	}

	return nil
}

// Stop 停止增量同步
func (is *IncrementalSyncer) Stop() {
	is.mutex.Lock()
	if !is.running {
		is.mutex.Unlock()
		return
	}
	is.running = false
	is.mutex.Unlock()

	is.logger.Info("Stopping incremental syncer", nil)
	is.cancel()
	is.wg.Wait()
	is.logger.Info("Incremental syncer stopped", nil)
}

// GetStats 获取统计信息
func (is *IncrementalSyncer) GetStats() map[string]interface{} {
	is.mutex.RLock()
	defer is.mutex.RUnlock()

	return map[string]interface{}{
		"keys_processed": is.keysProcessed.Load(),
		"keys_skipped":   is.keysSkipped.Load(),
		"keys_failed":    is.keysFailed.Load(),
		"running":        is.running,
	}
}

// consumeQueue 消费单个队列
func (is *IncrementalSyncer) consumeQueue(nodeAddr string, queue *storage.LevelDBQueue) {
	defer is.wg.Done()

	is.logger.Info("Starting queue consumer", map[string]interface{}{
		"node": nodeAddr,
	})

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	batch := make([]*storage.ChangeRecord, 0, is.batchSize)
	consecutiveErrors := 0
	const maxConsecutiveErrors = 10

	for {
		select {
		case <-is.ctx.Done():
			// 处理剩余批次
			if len(batch) > 0 {
				is.processBatch(batch)
			}
			is.logger.Info("Queue consumer stopped", map[string]interface{}{
				"node": nodeAddr,
			})
			return

		case <-ticker.C:
			// 从队列中取出一批变更
			for len(batch) < is.batchSize {
				change, err := queue.Dequeue()
				if err != nil {
					consecutiveErrors++
					is.logger.Warn("Failed to dequeue change", map[string]interface{}{
						"node":              nodeAddr,
						"error":             err.Error(),
						"consecutive_errors": consecutiveErrors,
					})
					if consecutiveErrors >= maxConsecutiveErrors {
						is.logger.Error("Too many consecutive dequeue errors, pausing consumer", map[string]interface{}{
							"node":   nodeAddr,
							"errors": consecutiveErrors,
						})
						time.Sleep(5 * time.Second) // 退避等待
						consecutiveErrors = 0
					}
					break
				}
				consecutiveErrors = 0 // 成功后重置
				if change == nil {
					break // 队列为空
				}
				batch = append(batch, change)
			}

			// 处理批次
			if len(batch) > 0 {
				is.processBatch(batch)
				batch = batch[:0] // 清空批次
			}
		}
	}
}

// processBatch 处理一批变更
func (is *IncrementalSyncer) processBatch(batch []*storage.ChangeRecord) {
	for _, change := range batch {
		// 根据事件类型处理
		switch change.Operation {
		case "del", "expired", "evicted":
			// 删除操作：在目标端也删除
			if err := is.targetCluster.Del(is.ctx, change.Key).Err(); err != nil {
				is.logger.Warn("Failed to delete key in target", map[string]interface{}{
					"key":   change.Key,
					"error": err.Error(),
				})
				is.keysFailed.Add(1)
			} else {
				is.keysProcessed.Add(1)
			}

		case "set", "hset", "lpush", "rpush", "sadd", "zadd", "setex", "psetex":
			// 写入操作：迁移 key
			migrated, err := is.migrateKey(change.Key)
			if err != nil {
				is.logger.Warn("Failed to migrate key", map[string]interface{}{
					"key":   change.Key,
					"error": err.Error(),
				})
				is.keysFailed.Add(1)
			} else if migrated {
				is.keysProcessed.Add(1)
			} else {
				is.keysSkipped.Add(1)
			}

		default:
			// 其他事件类型暂时忽略
			is.keysSkipped.Add(1)
		}
	}

	is.logger.Debug("Processed incremental batch", map[string]interface{}{
		"batch_size": len(batch),
		"processed":  is.keysProcessed.Load(),
		"skipped":    is.keysSkipped.Load(),
		"failed":     is.keysFailed.Load(),
	})
}

// migrateKey 迁移单个 key（使用 DUMP/RESTORE）
func (is *IncrementalSyncer) migrateKey(key string) (bool, error) {
	// 1. 检查源端是否存在
	exists, err := is.sourceCluster.Exists(is.ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("exists check failed: %w", err)
	}
	if exists == 0 {
		return false, nil // key 已经不存在，跳过
	}

	// 2. DUMP 源端 key
	data, err := is.sourceCluster.Dump(is.ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("dump failed: %w", err)
	}

	// 3. 获取 TTL
	ttl, err := is.sourceCluster.PTTL(is.ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("pttl failed: %w", err)
	}

	// 4. RESTORE 到目标端（使用 REPLACE）
	restoreTTL := time.Duration(0)
	if ttl > 0 {
		restoreTTL = ttl
	}

	err = is.targetCluster.RestoreReplace(is.ctx, key, restoreTTL, data).Err()
	if err != nil {
		return false, fmt.Errorf("restore failed: %w", err)
	}

	return true, nil
}
