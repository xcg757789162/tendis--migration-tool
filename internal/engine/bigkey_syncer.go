// Package engine 提供大 Key 分批同步器
// 灵感来源：Redis-Shake 的大 Key 分批拉取优化
//
// 核心优化：
// 1. 自动检测大 Key（元素数量超过阈值）
// 2. 使用 SCAN 类命令分批读取（HSCAN/SSCAN/ZSCAN/LRANGE）
// 3. 分批 Pipeline 写入目标端
// 4. 避免内存爆炸和超时
package engine

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

// BigKeySyncer 大 Key 分批同步器
type BigKeySyncer struct {
	source *redis.ClusterClient
	target *redis.ClusterClient
	writer *ConcurrentWriter
	
	// 配置
	config BigKeySyncerConfig
	
	// 统计
	stats struct {
		bigKeysProcessed atomic.Int64
		elementsWritten  atomic.Int64
		bytesWritten     atomic.Int64
		errors           atomic.Int64
	}
}

// BigKeySyncerConfig 大 Key 同步配置
type BigKeySyncerConfig struct {
	// 判定大 Key 的阈值
	HashMaxFields    int64 // Hash 字段数阈值，默认 10000
	SetMaxMembers    int64 // Set 成员数阈值，默认 10000
	ZSetMaxMembers   int64 // ZSet 成员数阈值，默认 10000
	ListMaxElements  int64 // List 元素数阈值，默认 10000
	StringMaxBytes   int64 // String 字节数阈值，默认 10MB
	
	// 分批大小
	ScanBatchSize int64 // SCAN 每批数量，默认 1000
	ListBatchSize int64 // LRANGE 每批数量，默认 1000
	
	// 并发配置
	Workers int // 并发 Worker 数，默认 4
}

// DefaultBigKeySyncerConfig 默认配置
func DefaultBigKeySyncerConfig() BigKeySyncerConfig {
	return BigKeySyncerConfig{
		HashMaxFields:   10000,
		SetMaxMembers:   10000,
		ZSetMaxMembers:  10000,
		ListMaxElements: 10000,
		StringMaxBytes:  10 * 1024 * 1024, // 10MB
		ScanBatchSize:   1000,
		ListBatchSize:   1000,
		Workers:         4,
	}
}

// NewBigKeySyncer 创建大 Key 同步器
func NewBigKeySyncer(source, target *redis.ClusterClient, config *BigKeySyncerConfig) *BigKeySyncer {
	cfg := DefaultBigKeySyncerConfig()
	if config != nil {
		if config.HashMaxFields > 0 {
			cfg.HashMaxFields = config.HashMaxFields
		}
		if config.SetMaxMembers > 0 {
			cfg.SetMaxMembers = config.SetMaxMembers
		}
		if config.ZSetMaxMembers > 0 {
			cfg.ZSetMaxMembers = config.ZSetMaxMembers
		}
		if config.ListMaxElements > 0 {
			cfg.ListMaxElements = config.ListMaxElements
		}
		if config.StringMaxBytes > 0 {
			cfg.StringMaxBytes = config.StringMaxBytes
		}
		if config.ScanBatchSize > 0 {
			cfg.ScanBatchSize = config.ScanBatchSize
		}
		if config.ListBatchSize > 0 {
			cfg.ListBatchSize = config.ListBatchSize
		}
		if config.Workers > 0 {
			cfg.Workers = config.Workers
		}
	}
	
	// 创建并发写入器
	writerConfig := &ConcurrentWriterConfig{
		PipelineCount: cfg.Workers,
		BatchSize:     100,
		FlushInterval: 50 * time.Millisecond,
	}
	writer := NewConcurrentWriter(target, writerConfig)
	writer.Start()
	
	return &BigKeySyncer{
		source: source,
		target: target,
		writer: writer,
		config: cfg,
	}
}

// Close 关闭同步器
func (s *BigKeySyncer) Close() {
	if s.writer != nil {
		s.writer.Stop()
	}
}

// IsBigKey 判断是否为大 Key
func (s *BigKeySyncer) IsBigKey(ctx context.Context, key string) (bool, string, int64) {
	// 获取 Key 类型
	keyType, err := s.source.Type(ctx, key).Result()
	if err != nil {
		return false, "", 0
	}
	
	var size int64
	
	switch keyType {
	case "hash":
		size, _ = s.source.HLen(ctx, key).Result()
		return size > s.config.HashMaxFields, keyType, size
		
	case "set":
		size, _ = s.source.SCard(ctx, key).Result()
		return size > s.config.SetMaxMembers, keyType, size
		
	case "zset":
		size, _ = s.source.ZCard(ctx, key).Result()
		return size > s.config.ZSetMaxMembers, keyType, size
		
	case "list":
		size, _ = s.source.LLen(ctx, key).Result()
		return size > s.config.ListMaxElements, keyType, size
		
	case "string":
		// 使用 STRLEN 获取字节数
		size, _ = s.source.StrLen(ctx, key).Result()
		return size > s.config.StringMaxBytes, keyType, size
		
	default:
		return false, keyType, 0
	}
}

// SyncBigKey 同步大 Key（自动选择策略）
func (s *BigKeySyncer) SyncBigKey(ctx context.Context, key string) error {
	isBig, keyType, size := s.IsBigKey(ctx, key)
	
	if !isBig {
		// 不是大 Key，使用普通方式
		return s.syncNormalKey(ctx, key)
	}
	
	log.Printf("[BigKeySyncer] Detected big key: %s, type=%s, size=%d", key, keyType, size)
	s.stats.bigKeysProcessed.Add(1)
	
	switch keyType {
	case "hash":
		return s.syncBigHash(ctx, key)
	case "set":
		return s.syncBigSet(ctx, key)
	case "zset":
		return s.syncBigZSet(ctx, key)
	case "list":
		return s.syncBigList(ctx, key)
	case "string":
		return s.syncBigString(ctx, key)
	default:
		return fmt.Errorf("unsupported big key type: %s", keyType)
	}
}

// syncNormalKey 同步普通 Key（使用 DUMP/RESTORE）
func (s *BigKeySyncer) syncNormalKey(ctx context.Context, key string) error {
	// 获取 TTL
	ttl, _ := s.source.PTTL(ctx, key).Result()
	if ttl < 0 {
		ttl = 0
	}
	
	// DUMP
	data, err := s.source.Dump(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("dump key %s: %w", key, err)
	}
	
	// RESTORE
	return s.target.Restore(ctx, key, ttl, data).Err()
}

// syncBigHash 分批同步大 Hash
func (s *BigKeySyncer) syncBigHash(ctx context.Context, key string) error {
	var cursor uint64 = 0
	var totalElements int64 = 0
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		
		// HSCAN 分批获取
		entries, newCursor, err := s.source.HScan(ctx, key, cursor, "*", s.config.ScanBatchSize).Result()
		if err != nil {
			s.stats.errors.Add(1)
			return fmt.Errorf("hscan key %s: %w", key, err)
		}
		
		if len(entries) > 0 {
			// 构造批量写入命令
			cmds := make([]*WriteCommand, 0, len(entries)/2)
			for i := 0; i < len(entries); i += 2 {
				if i+1 < len(entries) {
					cmds = append(cmds, &WriteCommand{
						Type: "HSET",
						Key:  key,
						Args: []interface{}{entries[i], entries[i+1]},
					})
				}
			}
			
			// 批量写入
			if err := s.writer.WriteBatch(ctx, cmds); err != nil {
				s.stats.errors.Add(1)
				return err
			}
			
			totalElements += int64(len(entries) / 2)
			s.stats.elementsWritten.Add(int64(len(entries) / 2))
		}
		
		if newCursor == 0 {
			break
		}
		cursor = newCursor
	}
	
	// 同步 TTL
	if err := s.syncTTL(ctx, key); err != nil {
		log.Printf("[BigKeySyncer] Sync TTL for %s failed: %v", key, err)
	}
	
	log.Printf("[BigKeySyncer] Hash %s synced: %d fields", key, totalElements)
	return nil
}

// syncBigSet 分批同步大 Set
func (s *BigKeySyncer) syncBigSet(ctx context.Context, key string) error {
	var cursor uint64 = 0
	var totalElements int64 = 0
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		
		// SSCAN 分批获取
		members, newCursor, err := s.source.SScan(ctx, key, cursor, "*", s.config.ScanBatchSize).Result()
		if err != nil {
			s.stats.errors.Add(1)
			return fmt.Errorf("sscan key %s: %w", key, err)
		}
		
		if len(members) > 0 {
			// 构造参数
			args := make([]interface{}, len(members))
			for i, m := range members {
				args[i] = m
			}
			
			// 写入
			cmd := &WriteCommand{
				Type: "SADD",
				Key:  key,
				Args: args,
			}
			
			if err := s.writer.Write(ctx, cmd); err != nil {
				s.stats.errors.Add(1)
				return err
			}
			
			totalElements += int64(len(members))
			s.stats.elementsWritten.Add(int64(len(members)))
		}
		
		if newCursor == 0 {
			break
		}
		cursor = newCursor
	}
	
	// 同步 TTL
	s.syncTTL(ctx, key)
	
	log.Printf("[BigKeySyncer] Set %s synced: %d members", key, totalElements)
	return nil
}

// syncBigZSet 分批同步大 ZSet
func (s *BigKeySyncer) syncBigZSet(ctx context.Context, key string) error {
	var cursor uint64 = 0
	var totalElements int64 = 0
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		
		// ZSCAN 分批获取
		members, newCursor, err := s.source.ZScan(ctx, key, cursor, "*", s.config.ScanBatchSize).Result()
		if err != nil {
			s.stats.errors.Add(1)
			return fmt.Errorf("zscan key %s: %w", key, err)
		}
		
		if len(members) > 0 {
			// 构造 ZADD 参数
			args := make([]interface{}, len(members))
			copy(args, toInterfaceSlice(members))
			
			cmd := &WriteCommand{
				Type: "RAW",
				Key:  key,
				Args: append([]interface{}{"ZADD", key}, args...),
			}
			
			if err := s.writer.Write(ctx, cmd); err != nil {
				s.stats.errors.Add(1)
				return err
			}
			
			totalElements += int64(len(members) / 2)
			s.stats.elementsWritten.Add(int64(len(members) / 2))
		}
		
		if newCursor == 0 {
			break
		}
		cursor = newCursor
	}
	
	// 同步 TTL
	s.syncTTL(ctx, key)
	
	log.Printf("[BigKeySyncer] ZSet %s synced: %d members", key, totalElements)
	return nil
}

// syncBigList 分批同步大 List
func (s *BigKeySyncer) syncBigList(ctx context.Context, key string) error {
	// 获取 List 长度
	length, err := s.source.LLen(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("llen key %s: %w", key, err)
	}
	
	var totalElements int64 = 0
	batchSize := s.config.ListBatchSize
	
	// LRANGE 分批获取（从头到尾）
	for start := int64(0); start < length; start += batchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		
		end := start + batchSize - 1
		if end >= length {
			end = length - 1
		}
		
		values, err := s.source.LRange(ctx, key, start, end).Result()
		if err != nil {
			s.stats.errors.Add(1)
			return fmt.Errorf("lrange key %s [%d:%d]: %w", key, start, end, err)
		}
		
		if len(values) > 0 {
			// 构造参数
			args := make([]interface{}, len(values))
			for i, v := range values {
				args[i] = v
			}
			
			// RPUSH 保持顺序
			cmd := &WriteCommand{
				Type: "RPUSH",
				Key:  key,
				Args: args,
			}
			
			if err := s.writer.Write(ctx, cmd); err != nil {
				s.stats.errors.Add(1)
				return err
			}
			
			totalElements += int64(len(values))
			s.stats.elementsWritten.Add(int64(len(values)))
		}
	}
	
	// 同步 TTL
	s.syncTTL(ctx, key)
	
	log.Printf("[BigKeySyncer] List %s synced: %d elements", key, totalElements)
	return nil
}

// syncBigString 同步大 String（使用 GETRANGE 分批）
func (s *BigKeySyncer) syncBigString(ctx context.Context, key string) error {
	// 大 String 仍然使用 DUMP/RESTORE，因为分批 APPEND 效率不高
	// 但可以考虑使用压缩或分片传输
	
	log.Printf("[BigKeySyncer] Big string %s using DUMP/RESTORE", key)
	return s.syncNormalKey(ctx, key)
}

// syncTTL 同步 TTL
func (s *BigKeySyncer) syncTTL(ctx context.Context, key string) error {
	ttl, err := s.source.PTTL(ctx, key).Result()
	if err != nil {
		return err
	}
	
	if ttl > 0 {
		return s.target.PExpire(ctx, key, ttl).Err()
	}
	
	return nil
}

// GetStats 获取统计信息
func (s *BigKeySyncer) GetStats() map[string]int64 {
	return map[string]int64{
		"big_keys_processed": s.stats.bigKeysProcessed.Load(),
		"elements_written":   s.stats.elementsWritten.Load(),
		"bytes_written":      s.stats.bytesWritten.Load(),
		"errors":             s.stats.errors.Load(),
	}
}

// toInterfaceSlice 将 string slice 转换为 interface slice
func toInterfaceSlice(s []string) []interface{} {
	result := make([]interface{}, len(s))
	for i, v := range s {
		result[i] = v
	}
	return result
}
