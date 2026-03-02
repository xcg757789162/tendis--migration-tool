package worker

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"tendis-migrate/pkg/logger"
)

// PipelineMigrator Pipeline 批量迁移优化器
type PipelineMigrator struct {
	sourceCluster *redis.ClusterClient
	targetCluster *redis.ClusterClient
	logger        *logger.Logger
	ctx           context.Context
	pipelineSize  int // Pipeline 批次大小（默认 100）
}

// NewPipelineMigrator 创建 Pipeline 迁移器
func NewPipelineMigrator(
	sourceCluster *redis.ClusterClient,
	targetCluster *redis.ClusterClient,
	taskLogger *logger.Logger,
	pipelineSize int,
) *PipelineMigrator {
	if pipelineSize <= 0 {
		pipelineSize = 100
	}

	return &PipelineMigrator{
		sourceCluster: sourceCluster,
		targetCluster: targetCluster,
		logger:        taskLogger,
		ctx:           context.Background(),
		pipelineSize:  pipelineSize,
	}
}

// MigrateBatch 批量迁移 keys（使用 Pipeline 优化）
func (pm *PipelineMigrator) MigrateBatch(keys []string) (migrated int, bytes int64, failed int, err error) {
	if len(keys) == 0 {
		return 0, 0, 0, nil
	}

	// 分批处理
	for i := 0; i < len(keys); i += pm.pipelineSize {
		end := i + pm.pipelineSize
		if end > len(keys) {
			end = len(keys)
		}

		batch := keys[i:end]
		m, b, f := pm.migratePipelineBatch(batch)
		migrated += m
		bytes += b
		failed += f
	}

	return migrated, bytes, failed, nil
}

// migratePipelineBatch 使用 Pipeline 迁移一批 keys
func (pm *PipelineMigrator) migratePipelineBatch(keys []string) (migrated int, bytes int64, failed int) {
	// Phase 1: Pipeline DUMP 源端所有 key
	sourcePipe := pm.sourceCluster.Pipeline()
	dumpCmds := make(map[string]*redis.StringCmd)
	ttlCmds := make(map[string]*redis.DurationCmd)

	for _, key := range keys {
		dumpCmds[key] = sourcePipe.Dump(pm.ctx, key)
		ttlCmds[key] = sourcePipe.PTTL(pm.ctx, key)
	}

	_, err := sourcePipe.Exec(pm.ctx)
	if err != nil && err != redis.Nil {
		pm.logger.Warn("Source pipeline exec failed", map[string]interface{}{
			"error": err.Error(),
		})
		failed = len(keys)
		return
	}

	// Phase 2: Pipeline RESTORE 目标端所有 key
	targetPipe := pm.targetCluster.Pipeline()
	var pipelineKeys []string // 记录实际加入 pipeline 的 key

	for _, key := range keys {
		dumpCmd := dumpCmds[key]
		ttlCmd := ttlCmds[key]

		data, err := dumpCmd.Result()
		if err != nil {
			failed++
			continue
		}

		ttl, err := ttlCmd.Result()
		if err != nil {
			ttl = 0
		}

		// 【BUG-FIX】PTTL 返回 -2 表示 key 在 DUMP 和 PTTL 之间被删除，跳过此 key 避免"幽灵数据"
		// 注意：PTTL 返回值是 time.Duration 类型，-2 代表 -2 毫秒（go-redis 将毫秒转为 Duration）
		if ttl == -2*time.Millisecond {
			pm.logger.Warn("Key expired between DUMP and PTTL, skipping", map[string]interface{}{
				"key": key,
			})
			failed++
			continue
		}

		restoreTTL := time.Duration(0)
		if ttl > 0 {
			restoreTTL = ttl
		}

		// RESTORE with REPLACE
		targetPipe.RestoreReplace(pm.ctx, key, restoreTTL, data)
		pipelineKeys = append(pipelineKeys, key)
		bytes += int64(len(data))
	}

	if len(pipelineKeys) == 0 {
		return
	}

	cmds, err := targetPipe.Exec(pm.ctx)
	if err != nil && err != redis.Nil {
		// 逐个检查结果
		var successCount int
		for i, cmd := range cmds {
			if cmd.Err() == nil {
				successCount++
			} else {
				if i < len(pipelineKeys) {
					pm.logger.Warn("Target RESTORE failed", map[string]interface{}{
						"key":   pipelineKeys[i],
						"error": cmd.Err().Error(),
					})
				}
			}
		}

		if successCount > 0 {
			// 部分成功
			migrated = successCount
			failed += len(pipelineKeys) - successCount
			pm.logger.Warn("Target pipeline partial success", map[string]interface{}{
				"succeeded": successCount,
				"failed":    len(pipelineKeys) - successCount,
			})
		} else {
			// 全部失败
			failed += len(pipelineKeys)
			migrated = 0
			bytes = 0
		}
		return
	}

	migrated = len(pipelineKeys)
	return
}

// MigrateKeysBatch 批量迁移 keys 的简化接口
func MigrateKeysBatch(
	keys []string,
	sourceCluster *redis.ClusterClient,
	targetCluster *redis.ClusterClient,
	logger *logger.Logger,
) (migrated int, bytes int64, failed int) {
	pm := NewPipelineMigrator(sourceCluster, targetCluster, logger, 100)
	m, b, f, err := pm.MigrateBatch(keys)
	if err != nil {
		logger.Error("Batch migration failed", map[string]interface{}{
			"error": err.Error(),
		})
		return 0, 0, len(keys)
	}
	return m, b, f
}
