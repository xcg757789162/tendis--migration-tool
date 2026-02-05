package limiter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// BigKeyLevel 大Key等级
type BigKeyLevel string

const (
	BigKeyLevelSmall  BigKeyLevel = "small"  // 小型大Key: 1-10MB 或 10000-50000 elements
	BigKeyLevelMedium BigKeyLevel = "medium" // 中型大Key: 10-50MB 或 50000-200000 elements
	BigKeyLevelLarge  BigKeyLevel = "large"  // 大型大Key: 50-200MB 或 200000-1000000 elements
	BigKeyLevelHuge   BigKeyLevel = "huge"   // 超大Key: >200MB 或 >1000000 elements
)

// BigKeyThreshold 大Key阈值配置
type BigKeyThreshold struct {
	StringMaxBytes   int64 `json:"string_max_bytes"`   // String类型阈值(字节)，默认1MB
	HashMaxFields    int   `json:"hash_max_fields"`    // Hash字段数阈值，默认10000
	SetMaxMembers    int   `json:"set_max_members"`    // Set成员数阈值，默认10000
	ZSetMaxMembers   int   `json:"zset_max_members"`   // ZSet成员数阈值，默认10000
	ListMaxElements  int   `json:"list_max_elements"`  // List元素数阈值，默认10000
	StreamMaxEntries int   `json:"stream_max_entries"` // Stream条目数阈值，默认10000
}

// DefaultBigKeyThreshold 默认大Key阈值
func DefaultBigKeyThreshold() *BigKeyThreshold {
	return &BigKeyThreshold{
		StringMaxBytes:   1024 * 1024,  // 1MB
		HashMaxFields:    10000,
		SetMaxMembers:    10000,
		ZSetMaxMembers:   10000,
		ListMaxElements:  10000,
		StreamMaxEntries: 10000,
	}
}

// BigKeyStrategy 大Key迁移策略
type BigKeyStrategy struct {
	Level          BigKeyLevel   `json:"level"`           // 等级
	ScanChunkSize  int           `json:"scan_chunk_size"` // 扫描分片大小
	WriteBatchSize int           `json:"write_batch_size"`// 写入批次大小
	RateLimit      int           `json:"rate_limit"`      // 限速(Key/秒)
	RetryTimes     int           `json:"retry_times"`     // 重试次数
	RetryInterval  time.Duration `json:"retry_interval"`  // 重试间隔
	PipelineSize   int           `json:"pipeline_size"`   // Pipeline批次大小
}

// DefaultBigKeyStrategies 默认大Key策略
func DefaultBigKeyStrategies() []*BigKeyStrategy {
	return []*BigKeyStrategy{
		{
			Level:          BigKeyLevelSmall,
			ScanChunkSize:  1000,
			WriteBatchSize: 500,
			RateLimit:      10000,
			RetryTimes:     3,
			RetryInterval:  1 * time.Second,
			PipelineSize:   100,
		},
		{
			Level:          BigKeyLevelMedium,
			ScanChunkSize:  500,
			WriteBatchSize: 200,
			RateLimit:      5000,
			RetryTimes:     5,
			RetryInterval:  2 * time.Second,
			PipelineSize:   50,
		},
		{
			Level:          BigKeyLevelLarge,
			ScanChunkSize:  100,
			WriteBatchSize: 50,
			RateLimit:      1000,
			RetryTimes:     10,
			RetryInterval:  5 * time.Second,
			PipelineSize:   20,
		},
		{
			Level:          BigKeyLevelHuge,
			ScanChunkSize:  10,
			WriteBatchSize: 5,
			RateLimit:      100,
			RetryTimes:     20,
			RetryInterval:  10 * time.Second,
			PipelineSize:   5,
		},
	}
}

// BigKeyInfo 大Key信息
type BigKeyInfo struct {
	Key          string      `json:"key"`
	Type         string      `json:"type"`
	Size         int64       `json:"size"`         // 字节大小
	ElementCount int64       `json:"element_count"`// 元素数量
	Level        BigKeyLevel `json:"level"`        // 等级
	TTL          int64       `json:"ttl"`          // TTL(秒)
}

// BigKeyScanner 大Key扫描器
type BigKeyScanner struct {
	client      *redis.ClusterClient
	threshold   *BigKeyThreshold
	strategies  map[BigKeyLevel]*BigKeyStrategy
	concurrency int // 每节点扫描并发数

	ctx    context.Context
	cancel context.CancelFunc

	// 扫描结果
	bigKeys  []*BigKeyInfo
	bigKeyMu sync.Mutex

	// 扫描统计
	scannedKeys  int64
	detectedKeys int64
}

// NewBigKeyScanner 创建大Key扫描器
func NewBigKeyScanner(client *redis.ClusterClient, threshold *BigKeyThreshold, strategies []*BigKeyStrategy, concurrency int) *BigKeyScanner {
	if threshold == nil {
		threshold = DefaultBigKeyThreshold()
	}
	if len(strategies) == 0 {
		strategies = DefaultBigKeyStrategies()
	}
	if concurrency <= 0 {
		concurrency = 3
	}

	strategyMap := make(map[BigKeyLevel]*BigKeyStrategy)
	for _, s := range strategies {
		strategyMap[s.Level] = s
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &BigKeyScanner{
		client:      client,
		threshold:   threshold,
		strategies:  strategyMap,
		concurrency: concurrency,
		ctx:         ctx,
		cancel:      cancel,
		bigKeys:     make([]*BigKeyInfo, 0),
	}
}

// Scan 扫描大Key
func (s *BigKeyScanner) Scan(ctx context.Context, pattern string, count int64) ([]*BigKeyInfo, error) {
	s.bigKeys = make([]*BigKeyInfo, 0)

	// 对每个主节点进行扫描
	err := s.client.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		return s.scanNode(ctx, node, pattern, count)
	})

	if err != nil {
		return nil, err
	}

	return s.bigKeys, nil
}

// scanNode 扫描单个节点
func (s *BigKeyScanner) scanNode(ctx context.Context, node *redis.Client, pattern string, count int64) error {
	var cursor uint64 = 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}

		// SCAN
		keys, nextCursor, err := node.Scan(ctx, cursor, pattern, count).Result()
		if err != nil {
			return err
		}

		// 批量检查Key大小
		for _, key := range keys {
			s.scannedKeys++

			info, err := s.checkKeySize(ctx, node, key)
			if err != nil {
				continue
			}

			if info != nil {
				s.bigKeyMu.Lock()
				s.bigKeys = append(s.bigKeys, info)
				s.bigKeyMu.Unlock()
				s.detectedKeys++
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return nil
}

// checkKeySize 检查Key是否是大Key
func (s *BigKeyScanner) checkKeySize(ctx context.Context, node *redis.Client, key string) (*BigKeyInfo, error) {
	// 获取类型
	keyType, err := node.Type(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	var size int64
	var elementCount int64
	var isBigKey bool

	switch keyType {
	case "string":
		// 获取字符串长度
		size, err = node.StrLen(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		isBigKey = size >= s.threshold.StringMaxBytes
		elementCount = 1

	case "hash":
		// 获取Hash字段数
		elementCount, err = node.HLen(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		isBigKey = elementCount >= int64(s.threshold.HashMaxFields)
		// 估算大小
		size = elementCount * 100 // 假设每个字段平均100字节

	case "set":
		// 获取Set成员数
		elementCount, err = node.SCard(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		isBigKey = elementCount >= int64(s.threshold.SetMaxMembers)
		size = elementCount * 50 // 假设每个成员平均50字节

	case "zset":
		// 获取ZSet成员数
		elementCount, err = node.ZCard(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		isBigKey = elementCount >= int64(s.threshold.ZSetMaxMembers)
		size = elementCount * 60 // 假设每个成员平均60字节

	case "list":
		// 获取List长度
		elementCount, err = node.LLen(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		isBigKey = elementCount >= int64(s.threshold.ListMaxElements)
		size = elementCount * 50 // 假设每个元素平均50字节

	case "stream":
		// 获取Stream长度
		elementCount, err = node.XLen(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		isBigKey = elementCount >= int64(s.threshold.StreamMaxEntries)
		size = elementCount * 200 // 假设每条消息平均200字节

	default:
		// 其他类型使用MEMORY USAGE
		size, err = node.MemoryUsage(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		isBigKey = size >= s.threshold.StringMaxBytes
		elementCount = 1
	}

	if !isBigKey {
		return nil, nil
	}

	// 获取TTL
	ttl, _ := node.TTL(ctx, key).Result()
	ttlSeconds := int64(ttl.Seconds())
	if ttlSeconds < 0 {
		ttlSeconds = -1
	}

	// 确定等级
	level := s.determineBigKeyLevel(size, elementCount)

	return &BigKeyInfo{
		Key:          key,
		Type:         keyType,
		Size:         size,
		ElementCount: elementCount,
		Level:        level,
		TTL:          ttlSeconds,
	}, nil
}

// determineBigKeyLevel 确定大Key等级
func (s *BigKeyScanner) determineBigKeyLevel(size int64, elementCount int64) BigKeyLevel {
	// 根据大小和元素数量综合判断
	sizeMB := size / (1024 * 1024)

	if sizeMB >= 200 || elementCount >= 1000000 {
		return BigKeyLevelHuge
	} else if sizeMB >= 50 || elementCount >= 200000 {
		return BigKeyLevelLarge
	} else if sizeMB >= 10 || elementCount >= 50000 {
		return BigKeyLevelMedium
	}
	return BigKeyLevelSmall
}

// GetStrategy 获取指定等级的迁移策略
func (s *BigKeyScanner) GetStrategy(level BigKeyLevel) *BigKeyStrategy {
	if strategy, ok := s.strategies[level]; ok {
		return strategy
	}
	// 默认返回small策略
	return s.strategies[BigKeyLevelSmall]
}

// GetStats 获取扫描统计
func (s *BigKeyScanner) GetStats() (scanned, detected int64) {
	return s.scannedKeys, s.detectedKeys
}

// Stop 停止扫描
func (s *BigKeyScanner) Stop() {
	s.cancel()
}

// BigKeyMigrator 大Key迁移器
type BigKeyMigrator struct {
	source    *redis.ClusterClient
	target    *redis.ClusterClient
	scanner   *BigKeyScanner
	limiter   *RateLimiter

	ctx       context.Context
}

// NewBigKeyMigrator 创建大Key迁移器
func NewBigKeyMigrator(source, target *redis.ClusterClient, scanner *BigKeyScanner, limiter *RateLimiter) *BigKeyMigrator {
	return &BigKeyMigrator{
		source:  source,
		target:  target,
		scanner: scanner,
		limiter: limiter,
		ctx:     context.Background(),
	}
}

// MigrateBigKey 迁移大Key (流式处理，不全量读取)
func (m *BigKeyMigrator) MigrateBigKey(ctx context.Context, info *BigKeyInfo) error {
	strategy := m.scanner.GetStrategy(info.Level)

	switch info.Type {
	case "hash":
		return m.migrateHash(ctx, info, strategy)
	case "set":
		return m.migrateSet(ctx, info, strategy)
	case "zset":
		return m.migrateZSet(ctx, info, strategy)
	case "list":
		return m.migrateList(ctx, info, strategy)
	default:
		// 其他类型使用DUMP/RESTORE
		return m.migrateDumpRestore(ctx, info.Key)
	}
}

// migrateHash 流式迁移Hash
func (m *BigKeyMigrator) migrateHash(ctx context.Context, info *BigKeyInfo, strategy *BigKeyStrategy) error {
	var cursor uint64 = 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 限流
		m.limiter.AcquireSource()

		// HSCAN 分片读取
		result, nextCursor, err := m.source.HScan(ctx, info.Key, cursor, "*", int64(strategy.ScanChunkSize)).Result()
		if err != nil {
			return fmt.Errorf("HSCAN failed: %w", err)
		}

		// 构建field-value pairs
		if len(result) > 0 {
			m.limiter.AcquireTarget()

			// Pipeline写入
			pipe := m.target.Pipeline()
			for i := 0; i < len(result); i += 2 {
				if i+1 < len(result) {
					pipe.HSet(ctx, info.Key, result[i], result[i+1])
				}
			}
			_, err = pipe.Exec(ctx)
			if err != nil {
				return fmt.Errorf("HSET failed: %w", err)
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	// 设置TTL
	if info.TTL > 0 {
		m.target.Expire(ctx, info.Key, time.Duration(info.TTL)*time.Second)
	}

	return nil
}

// migrateSet 流式迁移Set
func (m *BigKeyMigrator) migrateSet(ctx context.Context, info *BigKeyInfo, strategy *BigKeyStrategy) error {
	var cursor uint64 = 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		m.limiter.AcquireSource()

		// SSCAN 分片读取
		members, nextCursor, err := m.source.SScan(ctx, info.Key, cursor, "*", int64(strategy.ScanChunkSize)).Result()
		if err != nil {
			return fmt.Errorf("SSCAN failed: %w", err)
		}

		if len(members) > 0 {
			m.limiter.AcquireTarget()

			// 转换为interface{}切片
			args := make([]interface{}, len(members))
			for i, member := range members {
				args[i] = member
			}

			err = m.target.SAdd(ctx, info.Key, args...).Err()
			if err != nil {
				return fmt.Errorf("SADD failed: %w", err)
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	if info.TTL > 0 {
		m.target.Expire(ctx, info.Key, time.Duration(info.TTL)*time.Second)
	}

	return nil
}

// migrateZSet 流式迁移ZSet
func (m *BigKeyMigrator) migrateZSet(ctx context.Context, info *BigKeyInfo, strategy *BigKeyStrategy) error {
	var cursor uint64 = 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		m.limiter.AcquireSource()

		// ZSCAN 分片读取
		result, nextCursor, err := m.source.ZScan(ctx, info.Key, cursor, "*", int64(strategy.ScanChunkSize)).Result()
		if err != nil {
			return fmt.Errorf("ZSCAN failed: %w", err)
		}

		if len(result) > 0 {
			m.limiter.AcquireTarget()

			// 构建ZAdd参数
			members := make([]redis.Z, 0, len(result)/2)
			for i := 0; i < len(result); i += 2 {
				if i+1 < len(result) {
					var score float64
					fmt.Sscanf(result[i+1], "%f", &score)
					members = append(members, redis.Z{
						Score:  score,
						Member: result[i],
					})
				}
			}

			err = m.target.ZAdd(ctx, info.Key, members...).Err()
			if err != nil {
				return fmt.Errorf("ZADD failed: %w", err)
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	if info.TTL > 0 {
		m.target.Expire(ctx, info.Key, time.Duration(info.TTL)*time.Second)
	}

	return nil
}

// migrateList 流式迁移List
func (m *BigKeyMigrator) migrateList(ctx context.Context, info *BigKeyInfo, strategy *BigKeyStrategy) error {
	listLen := info.ElementCount
	batchSize := int64(strategy.WriteBatchSize)

	for i := int64(0); i < listLen; i += batchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		m.limiter.AcquireSource()

		// LRANGE 分片读取
		end := i + batchSize - 1
		if end >= listLen {
			end = listLen - 1
		}

		elements, err := m.source.LRange(ctx, info.Key, i, end).Result()
		if err != nil {
			return fmt.Errorf("LRANGE failed: %w", err)
		}

		if len(elements) > 0 {
			m.limiter.AcquireTarget()

			// 转换为interface{}切片
			args := make([]interface{}, len(elements))
			for j, elem := range elements {
				args[j] = elem
			}

			err = m.target.RPush(ctx, info.Key, args...).Err()
			if err != nil {
				return fmt.Errorf("RPUSH failed: %w", err)
			}
		}
	}

	if info.TTL > 0 {
		m.target.Expire(ctx, info.Key, time.Duration(info.TTL)*time.Second)
	}

	return nil
}

// migrateDumpRestore 使用DUMP/RESTORE迁移
func (m *BigKeyMigrator) migrateDumpRestore(ctx context.Context, key string) error {
	m.limiter.AcquireSource()

	dump, err := m.source.Dump(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("DUMP failed: %w", err)
	}

	ttl, _ := m.source.PTTL(ctx, key).Result()
	if ttl < 0 {
		ttl = 0
	}

	m.limiter.AcquireTarget()

	err = m.target.RestoreReplace(ctx, key, ttl, dump).Err()
	if err != nil {
		return fmt.Errorf("RESTORE failed: %w", err)
	}

	return nil
}
