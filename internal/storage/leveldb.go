package storage

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// LevelDBQueue LevelDB 变更队列（每个源节点一个实例）
type LevelDBQueue struct {
	db       *leveldb.DB
	nodeID   string
	basePath string
	seqID    uint64
}

// KeyChange 键变更记录
type KeyChange struct {
	Key       string `json:"key"`
	Operation string `json:"operation"` // set, del, expire, etc.
	Timestamp int64  `json:"timestamp"`
	NodeID    string `json:"node_id"`
}

// ChangeRecord 变更记录（别名，用于兼容）
type ChangeRecord = KeyChange

// NewLevelDBQueue 创建新的 LevelDB 队列
func NewLevelDBQueue(basePath string) (*LevelDBQueue, error) {
	// 打开 LevelDB（优化配置）
	opts := &opt.Options{
		WriteBuffer:            32 * 1024 * 1024, // 32MB 写缓冲
		BlockCacheCapacity:     64 * 1024 * 1024, // 64MB 块缓存
		CompactionTableSize:    8 * 1024 * 1024,  // 8MB SST 文件
		OpenFilesCacheCapacity: 1000,
		Compression:            opt.SnappyCompression,
	}

	db, err := leveldb.OpenFile(basePath, opts)
	if err != nil {
		return nil, fmt.Errorf("open leveldb failed: %w", err)
	}

	// 恢复序列号（从最后一个 key 提取）
	seqID := uint64(0)
	iter := db.NewIterator(nil, nil)
	if iter.Last() {
		// 从 key 提取序列号: "timestamp_seqID"
		// 例如: "1735891200123_0000001"
		// 这里简化处理，重新从 0 开始（重启后重新编号）
	}
	iter.Release()

	return &LevelDBQueue{
		db:       db,
		nodeID:   "",
		basePath: basePath,
		seqID:    seqID,
	}, nil
}

// Enqueue 入队（Master 进程调用）
func (q *LevelDBQueue) Enqueue(change *KeyChange) error {
	// 生成唯一 key: timestamp_seqID
	q.seqID++
	key := fmt.Sprintf("%d_%07d", time.Now().UnixMilli(), q.seqID)

	// 序列化变更记录
	value, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("marshal change failed: %w", err)
	}

	// 写入 LevelDB
	return q.db.Put([]byte(key), value, nil)
}

// EnqueueBatch 批量入队（优化性能）
func (q *LevelDBQueue) EnqueueBatch(changes []*KeyChange) error {
	batch := new(leveldb.Batch)

	for _, change := range changes {
		q.seqID++
		key := fmt.Sprintf("%d_%07d", time.Now().UnixMilli(), q.seqID)

		value, err := json.Marshal(change)
		if err != nil {
			continue
		}

		batch.Put([]byte(key), value)
	}

	return q.db.Write(batch, nil)
}

// Dequeue 出队单条记录
func (q *LevelDBQueue) Dequeue() (*ChangeRecord, error) {
	changes, err := q.DequeueBatch(1)
	if err != nil {
		return nil, err
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes[0], nil
}

// DequeueBatch 出队（Worker 进程调用）
func (q *LevelDBQueue) DequeueBatch(batchSize int) ([]*KeyChange, error) {
	iter := q.db.NewIterator(nil, nil)
	defer iter.Release()

	changes := make([]*KeyChange, 0, batchSize)
	keysToDelete := make([][]byte, 0, batchSize)

	for iter.Next() && len(changes) < batchSize {
		var change KeyChange
		if err := json.Unmarshal(iter.Value(), &change); err != nil {
			continue
		}

		changes = append(changes, &change)
		keysToDelete = append(keysToDelete, append([]byte(nil), iter.Key()...))
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	// 批量删除已消费的记录
	if len(keysToDelete) > 0 {
		batch := new(leveldb.Batch)
		for _, key := range keysToDelete {
			batch.Delete(key)
		}
		if err := q.db.Write(batch, nil); err != nil {
			return changes, fmt.Errorf("delete consumed keys failed: %w", err)
		}
	}

	return changes, nil
}

// Peek 查看队首（不删除）
func (q *LevelDBQueue) Peek(count int) ([]*KeyChange, error) {
	iter := q.db.NewIterator(nil, nil)
	defer iter.Release()

	changes := make([]*KeyChange, 0, count)

	for iter.Next() && len(changes) < count {
		var change KeyChange
		if err := json.Unmarshal(iter.Value(), &change); err != nil {
			continue
		}
		changes = append(changes, &change)
	}

	return changes, iter.Error()
}

// Count 获取队列长度
func (q *LevelDBQueue) Count() int {
	count := 0
	iter := q.db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		count++
	}

	return count
}

// Clear 清空队列
func (q *LevelDBQueue) Clear() error {
	iter := q.db.NewIterator(nil, nil)
	defer iter.Release()

	batch := new(leveldb.Batch)
	for iter.Next() {
		batch.Delete(iter.Key())
	}

	return q.db.Write(batch, nil)
}

// Close 关闭队列
func (q *LevelDBQueue) Close() error {
	return q.db.Close()
}

// GetStats 获取统计信息
func (q *LevelDBQueue) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	// 队列长度
	count := q.Count()
	stats["size"] = int64(count)
	stats["count"] = count

	// LevelDB 内部统计
	if dbStats, err := q.db.GetProperty("leveldb.stats"); err == nil {
		stats["leveldb_stats"] = dbStats
	}

	// 磁盘占用
	if size, err := q.db.SizeOf(nil); err == nil {
		stats["disk_usage_bytes"] = size.Sum()
	}

	return stats
}
