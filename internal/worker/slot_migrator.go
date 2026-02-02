package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"tendis-migrate/internal/ipc"
	"tendis-migrate/internal/storage"
	"tendis-migrate/pkg/logger"
)

// SlotMigrator Slot 迁移器
type SlotMigrator struct {
	taskID         string
	workerID       int
	assignedSlots  []int
	sourceClient   redis.UniversalClient
	targetClient   redis.UniversalClient
	db             *storage.SQLiteDB
	ipcClient      *ipc.Client
	logger         *logger.TaskLogger
	
	// 配置
	scanBatchSize  int
	conflictPolicy string
	
	// 统计
	keysMigrated   int64
	bytesMigrated  int64
	keysFailed     int64
	keysSkipped    int64
	
	ctx context.Context
}

// NewSlotMigrator 创建 Slot 迁移器
func NewSlotMigrator(
	taskID string,
	workerID int,
	assignedSlots []int,
	sourceClient redis.UniversalClient,
	targetClient redis.UniversalClient,
	db *storage.SQLiteDB,
	ipcClient *ipc.Client,
	logger *logger.TaskLogger,
	scanBatchSize int,
	conflictPolicy string,
) *SlotMigrator {
	return &SlotMigrator{
		taskID:         taskID,
		workerID:       workerID,
		assignedSlots:  assignedSlots,
		sourceClient:   sourceClient,
		targetClient:   targetClient,
		db:             db,
		ipcClient:      ipcClient,
		logger:         logger,
		scanBatchSize:  scanBatchSize,
		conflictPolicy: conflictPolicy,
		ctx:            context.Background(),
	}
}

// MigrateAllSlots 迁移所有分配的 Slot
func (sm *SlotMigrator) MigrateAllSlots() error {
	sm.logger.Info("Starting slot migration", map[string]interface{}{
		"worker_id":      sm.workerID,
		"assigned_slots": len(sm.assignedSlots),
	})

	startTime := time.Now()

	for _, slot := range sm.assignedSlots {
		if err := sm.MigrateSlot(slot); err != nil {
			sm.logger.Error("Slot migration failed", map[string]interface{}{
				"slot":  slot,
				"error": err.Error(),
			})
			
			// 标记 Slot 失败
			sm.db.UpdateSlotStatus(sm.taskID, slot, "failed")
			
			// 通知 Master
			msg, _ := ipc.NewIPCMessage(ipc.MsgTypeSlotFailed, &ipc.MsgSlotFailed{
				WorkerID: sm.workerID,
				TaskID:   sm.taskID,
				Slot:     slot,
				Error:    err.Error(),
			})
			sm.ipcClient.Send(msg)
			
			return err
		}
	}

	duration := time.Since(startTime)
	sm.logger.Info("All slots completed", map[string]interface{}{
		"worker_id": sm.workerID,
		"duration":  duration.String(),
		"keys":      sm.keysMigrated,
		"bytes":     sm.bytesMigrated,
	})

	return nil
}

// MigrateSlot 迁移单个 Slot
func (sm *SlotMigrator) MigrateSlot(slot int) error {
	startTime := time.Now()
	
	sm.logger.Info("Starting slot migration", map[string]interface{}{
		"slot": slot,
	})

	// 检查断点恢复
	slotStatus, err := sm.db.GetSlotStatus(sm.taskID, slot)
	if err != nil {
		return fmt.Errorf("get slot status failed: %w", err)
	}

	if slotStatus.Status == "completed" {
		sm.logger.Info("Slot already completed, skipping", map[string]interface{}{
			"slot": slot,
		})
		return nil
	}

	// 从断点恢复
	cursor := slotStatus.LastCursor
	if cursor == "" {
		cursor = "0"
	}

	slotKeysMigrated := slotStatus.KeysMigrated
	slotBytesMigrated := slotStatus.BytesMigrated

	// 标记 Slot 为迁移中
	sm.db.UpdateSlotStatus(sm.taskID, slot, "migrating")

	// 使用 SCAN 扫描该 Slot 的所有 key
	// Redis Cluster 使用 Hash Tag 确保 key 属于特定 Slot: {slot}
	// 但我们无法直接用 Hash Tag 过滤，需要扫描所有 key 并检查 Slot
	
	// 获取该 Slot 对应的节点
	node, err := sm.getNodeForSlot(slot)
	if err != nil {
		return fmt.Errorf("get node for slot failed: %w", err)
	}

	// 连接到特定节点（如果是 Cluster 模式）
	nodeClient := sm.getNodeClient(node)

	keysInThisBatch := 0
	lastCheckpointTime := time.Now()

	for {
		// SCAN 扫描
		var scanCmd *redis.ScanCmd
		if cursor == "0" || cursor == "" {
			scanCmd = nodeClient.Scan(sm.ctx, 0, "*", int64(sm.scanBatchSize))
		} else {
			// 将字符串游标转换为 uint64
			cursorUint, _ := parseUint64(cursor)
			scanCmd = nodeClient.Scan(sm.ctx, cursorUint, "*", int64(sm.scanBatchSize))
		}
		
		keys, newCursor, err := scanCmd.Result()
		if err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		// 过滤属于当前 Slot 的 key
		keysForThisSlot := sm.filterKeysBySlot(keys, slot)

		// 迁移 keys
		for _, key := range keysForThisSlot {
			migrated, bytes, err := sm.migrateKey(key)
			if err != nil {
				sm.logger.Warn("Migrate key failed", map[string]interface{}{
					"key":   key,
					"error": err.Error(),
				})
				sm.keysFailed++
				continue
			}

			if migrated {
				sm.keysMigrated++
				slotKeysMigrated++
				sm.bytesMigrated += bytes
				slotBytesMigrated += bytes
			} else {
				sm.keysSkipped++
			}

			keysInThisBatch++
		}

		cursor = fmt.Sprintf("%d", newCursor)

		// 每 1000 个 key 或 5 秒保存一次断点
		if keysInThisBatch >= 1000 || time.Since(lastCheckpointTime) > 5*time.Second {
			if err := sm.saveCheckpoint(slot, cursor, slotKeysMigrated); err != nil {
				sm.logger.Warn("Save checkpoint failed", map[string]interface{}{
					"slot":  slot,
					"error": err.Error(),
				})
			}
			
			keysInThisBatch = 0
			lastCheckpointTime = time.Now()
		}

		// SCAN 游标回到起点，说明扫描完成
		if cursor == "0" {
			break
		}
	}

	// 标记 Slot 完成
	sm.db.UpdateSlotStatus(sm.taskID, slot, "completed")

	duration := time.Since(startTime)

	// 通知 Master Slot 完成
	msg, _ := ipc.NewIPCMessage(ipc.MsgTypeSlotCompleted, &ipc.MsgSlotCompleted{
		WorkerID:      sm.workerID,
		TaskID:        sm.taskID,
		Slot:          slot,
		KeysMigrated:  slotKeysMigrated,
		BytesMigrated: slotBytesMigrated,
		Duration:      duration.Milliseconds(),
	})
	sm.ipcClient.Send(msg)

	sm.logger.Info("Slot migration completed", map[string]interface{}{
		"slot":     slot,
		"keys":     slotKeysMigrated,
		"bytes":    slotBytesMigrated,
		"duration": duration.String(),
	})

	return nil
}

// migrateKey 迁移单个 key
func (sm *SlotMigrator) migrateKey(key string) (bool, int64, error) {
	ctx := sm.ctx

	// 检查目标端是否存在
	exists, err := sm.targetClient.Exists(ctx, key).Result()
	if err != nil {
		return false, 0, fmt.Errorf("check target exists failed: %w", err)
	}

	// 应用冲突策略
	if exists > 0 {
		switch sm.conflictPolicy {
		case "skip_full_only":
			// 全量阶段跳过，增量阶段强制替换（这里是全量阶段）
			return false, 0, nil
		case "error":
			return false, 0, fmt.Errorf("key already exists: %s", key)
		case "replace":
			// 继续迁移，覆盖
		}
	}

	// 使用 DUMP + RESTORE 迁移
	dumpData, err := sm.sourceClient.Dump(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			// Key 已被删除，跳过
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("dump failed: %w", err)
	}

	// 获取 TTL
	ttl, err := sm.sourceClient.PTTL(ctx, key).Result()
	if err != nil {
		ttl = 0
	}
	if ttl < 0 {
		ttl = 0
	}

	// RESTORE 到目标端
	if err := sm.targetClient.RestoreReplace(ctx, key, ttl, dumpData).Err(); err != nil {
		return false, 0, fmt.Errorf("restore failed: %w", err)
	}

	return true, int64(len(dumpData)), nil
}

// saveCheckpoint 保存断点
func (sm *SlotMigrator) saveCheckpoint(slot int, cursor string, keysMigrated int64) error {
	return sm.db.UpdateSlotCheckpoint(sm.taskID, slot, cursor, keysMigrated)
}

// getNodeForSlot 获取 Slot 对应的节点地址
func (sm *SlotMigrator) getNodeForSlot(slot int) (string, error) {
	// 查询 Cluster Slots 信息
	clusterClient, ok := sm.sourceClient.(*redis.ClusterClient)
	if !ok {
		// 非 Cluster 模式，返回默认节点
		return "", nil
	}

	slots, err := clusterClient.ClusterSlots(sm.ctx).Result()
	if err != nil {
		return "", err
	}

	for _, slotRange := range slots {
		if slot >= int(slotRange.Start) && slot <= int(slotRange.End) {
			if len(slotRange.Nodes) > 0 {
				node := slotRange.Nodes[0]
				return node.Addr, nil
			}
		}
	}

	return "", fmt.Errorf("node not found for slot %d", slot)
}

// getNodeClient 获取特定节点的客户端
func (sm *SlotMigrator) getNodeClient(nodeAddr string) redis.UniversalClient {
	if nodeAddr == "" {
		return sm.sourceClient
	}

	// 如果是 Cluster 模式，直接使用 ClusterClient（它会自动路由）
	return sm.sourceClient
}

// filterKeysBySlot 过滤属于指定 Slot 的 key
func (sm *SlotMigrator) filterKeysBySlot(keys []string, targetSlot int) []string {
	result := []string{}

	for _, key := range keys {
		keySlot := sm.calculateKeySlot(key)
		if keySlot == targetSlot {
			result = append(result, key)
		}
	}

	return result
}

// calculateKeySlot 计算 key 的 Slot（CRC16 算法）
func (sm *SlotMigrator) calculateKeySlot(key string) int {
	// 提取 Hash Tag（如果存在）
	// 例如: "user:{123}:info" -> "123"
	hashTag := key
	if start := strings.Index(key, "{"); start >= 0 {
		if end := strings.Index(key[start+1:], "}"); end >= 0 {
			hashTag = key[start+1 : start+1+end]
		}
	}

	// CRC16 计算
	return int(crc16([]byte(hashTag)) % 16384)
}

// crc16 CRC16 校验和计算（Redis 使用的算法）
func crc16(data []byte) uint16 {
	var crc uint16 = 0
	crcTable := [...]uint16{
		0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
		0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	}

	for _, b := range data {
		crc = (crc<<4 ^ crcTable[(crc>>12^uint16(b>>4))&0x0f]) & 0xffff
		crc = (crc<<4 ^ crcTable[(crc>>12^uint16(b&0x0f))&0x0f]) & 0xffff
	}

	return crc
}

// parseUint64 解析 uint64
func parseUint64(s string) (uint64, error) {
	if s == "" || s == "0" {
		return 0, nil
	}
	
	var result uint64
	fmt.Sscanf(s, "%d", &result)
	return result, nil
}
