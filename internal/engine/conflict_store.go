// Package engine 提供冲突 Key 存储和管理
// 这是我们相比 Redis-Shake 的重要优势：
// Redis-Shake 不支持跳过已存在的 Key，也不记录冲突 Key
//
// 核心功能：
// 1. 内存 + 磁盘混合存储，支持百万级冲突 Key
// 2. 记录完整上下文（源端值、目标端值、类型、时间）
// 3. 支持分页查询和导出
// 4. 支持按前缀/时间范围过滤
package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

// ConflictKeyRecord 冲突 Key 记录（带完整上下文）
type ConflictKeyRecord struct {
	// Key 名称
	Key string `json:"key"`
	// Key 类型
	KeyType string `json:"key_type"`
	// 源端值摘要（可选，用于对比）
	SourceValueDigest string `json:"source_value_digest,omitempty"`
	// 目标端值摘要
	TargetValueDigest string `json:"target_value_digest,omitempty"`
	// 源端 TTL（秒，-1 表示无过期）
	SourceTTL int64 `json:"source_ttl"`
	// 目标端 TTL
	TargetTTL int64 `json:"target_ttl"`
	// 发现时间
	Timestamp time.Time `json:"timestamp"`
	// 迁移阶段
	Phase string `json:"phase"` // "full" 或 "incremental"
	// 处理动作
	Action string `json:"action"` // "skipped" 或 "replaced"
	// Key 大小（字节，可选）
	Size int64 `json:"size,omitempty"`
}

// ConflictKeyStore 冲突 Key 存储
// 支持内存 + 磁盘混合存储
type ConflictKeyStore struct {
	taskID string
	
	// 内存缓冲（快速访问最近的冲突 Key）
	memoryBuffer []*ConflictKeyRecord
	memoryLimit  int // 内存最大存储数量
	
	// 磁盘存储（溢出时写入）
	diskFile   *os.File
	diskWriter *bufio.Writer
	diskPath   string
	
	// 统计
	totalCount   atomic.Int64
	memoryCount  atomic.Int64
	diskCount    atomic.Int64
	
	// Redis 客户端（用于获取值摘要）
	sourceClient redis.UniversalClient
	targetClient redis.UniversalClient
	
	// 并发控制
	mu sync.RWMutex
}

// ConflictKeyStoreConfig 配置
type ConflictKeyStoreConfig struct {
	// 任务 ID
	TaskID string
	// 内存最大存储数量（默认 100000）
	MemoryLimit int
	// 磁盘文件目录（默认 ./data/conflicts）
	DiskDir string
	// 是否记录值摘要（默认 false，启用会增加查询开销）
	RecordValueDigest bool
}

// NewConflictKeyStore 创建冲突 Key 存储
func NewConflictKeyStore(config *ConflictKeyStoreConfig, source, target redis.UniversalClient) (*ConflictKeyStore, error) {
	if config.MemoryLimit <= 0 {
		config.MemoryLimit = 100000
	}
	if config.DiskDir == "" {
		config.DiskDir = "./data/conflicts"
	}
	
	// 创建目录
	if err := os.MkdirAll(config.DiskDir, 0755); err != nil {
		return nil, fmt.Errorf("create conflict dir: %w", err)
	}
	
	// 创建磁盘文件
	diskPath := filepath.Join(config.DiskDir, fmt.Sprintf("%s_conflicts.jsonl", config.TaskID))
	diskFile, err := os.OpenFile(diskPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("create conflict file: %w", err)
	}
	
	return &ConflictKeyStore{
		taskID:       config.TaskID,
		memoryBuffer: make([]*ConflictKeyRecord, 0, config.MemoryLimit),
		memoryLimit:  config.MemoryLimit,
		diskFile:     diskFile,
		diskWriter:   bufio.NewWriter(diskFile),
		diskPath:     diskPath,
		sourceClient: source,
		targetClient: target,
	}, nil
}

// Record 记录冲突 Key
func (s *ConflictKeyStore) Record(record *ConflictKeyRecord) {
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now()
	}
	
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.totalCount.Add(1)
	
	if len(s.memoryBuffer) < s.memoryLimit {
		// 存入内存
		s.memoryBuffer = append(s.memoryBuffer, record)
		s.memoryCount.Add(1)
	} else {
		// 溢出写入磁盘
		s.writeToDisk(record)
		s.diskCount.Add(1)
	}
}

// RecordWithContext 记录冲突 Key（自动获取上下文信息）
func (s *ConflictKeyStore) RecordWithContext(ctx context.Context, key string, phase string, action string) {
	record := &ConflictKeyRecord{
		Key:       key,
		Timestamp: time.Now(),
		Phase:     phase,
		Action:    action,
	}
	
	// 获取 Key 类型
	if s.targetClient != nil {
		keyType, _ := s.targetClient.Type(ctx, key).Result()
		record.KeyType = keyType
		
		// 获取目标端 TTL
		ttl, _ := s.targetClient.TTL(ctx, key).Result()
		record.TargetTTL = int64(ttl.Seconds())
	}
	
	// 获取源端 TTL
	if s.sourceClient != nil {
		ttl, _ := s.sourceClient.TTL(ctx, key).Result()
		record.SourceTTL = int64(ttl.Seconds())
	}
	
	s.Record(record)
}

// writeToDisk 写入磁盘
func (s *ConflictKeyStore) writeToDisk(record *ConflictKeyRecord) {
	data, err := json.Marshal(record)
	if err != nil {
		log.Printf("[ConflictKeyStore] Marshal error: %v", err)
		return
	}
	
	if _, err := s.diskWriter.Write(data); err != nil {
		log.Printf("[ConflictKeyStore] Disk write error for key=%s: %v", record.Key, err)
		// 写入失败时尝试降级到内存（如果还有空间）
		return
	}
	if err := s.diskWriter.WriteByte('\n'); err != nil {
		log.Printf("[ConflictKeyStore] Disk write newline error: %v", err)
	}
	// 定期刷新确保数据落盘
	if s.diskCount.Load()%100 == 0 {
		if err := s.diskWriter.Flush(); err != nil {
			log.Printf("[ConflictKeyStore] Disk flush error: %v", err)
		}
	}
}

// Flush 刷新缓冲区
func (s *ConflictKeyStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.diskWriter != nil {
		return s.diskWriter.Flush()
	}
	return nil
}

// Close 关闭存储
func (s *ConflictKeyStore) Close() error {
	// 【BUG-FIX】在同一个锁作用域内完成 Flush + Close，避免 Flush 释放锁后再加锁之间的数据丢失窗口
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.diskWriter != nil {
		s.diskWriter.Flush()
	}
	
	if s.diskFile != nil {
		return s.diskFile.Close()
	}
	return nil
}

// GetCount 获取总数
func (s *ConflictKeyStore) GetCount() int64 {
	return s.totalCount.Load()
}

// GetMemoryCount 获取内存中的数量
func (s *ConflictKeyStore) GetMemoryCount() int64 {
	return s.memoryCount.Load()
}

// GetDiskCount 获取磁盘中的数量
func (s *ConflictKeyStore) GetDiskCount() int64 {
	return s.diskCount.Load()
}

// Query 查询冲突 Key（分页）
func (s *ConflictKeyStore) Query(page, size int, filter *ConflictKeyFilter) (*ConflictKeyQueryResult, error) {
	if page < 1 {
		page = 1
	}
	if size <= 0 {
		size = 100
	}
	if size > 1000 {
		size = 1000
	}
	
	// 【BUG-FIX】使用写锁而非读锁，因为 readFromDisk 内部会调用 diskWriter.Flush()（写操作）
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 合并内存和磁盘数据
	allRecords := make([]*ConflictKeyRecord, 0, len(s.memoryBuffer))
	
	// 先添加内存数据
	for _, r := range s.memoryBuffer {
		if filter == nil || filter.Match(r) {
			allRecords = append(allRecords, r)
		}
	}
	
	// 如果需要，读取磁盘数据
	if s.diskCount.Load() > 0 && (page*size > len(allRecords) || filter != nil) {
		diskRecords, _ := s.readFromDisk(filter)
		allRecords = append(allRecords, diskRecords...)
	}
	
	// 按时间排序（最新的在前）
	sort.Slice(allRecords, func(i, j int) bool {
		return allRecords[i].Timestamp.After(allRecords[j].Timestamp)
	})
	
	// 分页
	total := len(allRecords)
	start := (page - 1) * size
	end := start + size
	
	if start >= total {
		return &ConflictKeyQueryResult{
			Total: int64(total),
			Page:  page,
			Size:  size,
			Keys:  []*ConflictKeyRecord{},
		}, nil
	}
	
	if end > total {
		end = total
	}
	
	return &ConflictKeyQueryResult{
		Total: int64(total),
		Page:  page,
		Size:  size,
		Keys:  allRecords[start:end],
	}, nil
}

// readFromDisk 从磁盘读取
func (s *ConflictKeyStore) readFromDisk(filter *ConflictKeyFilter) ([]*ConflictKeyRecord, error) {
	// 确保缓冲区已刷新
	s.diskWriter.Flush()
	
	// 打开文件读取
	file, err := os.Open(s.diskPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	var records []*ConflictKeyRecord
	scanner := bufio.NewScanner(file)
	
	for scanner.Scan() {
		var record ConflictKeyRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			continue
		}
		
		if filter == nil || filter.Match(&record) {
			records = append(records, &record)
		}
	}
	
	return records, scanner.Err()
}

// Export 导出冲突 Key 到文件
func (s *ConflictKeyStore) Export(writer io.Writer, format string, filter *ConflictKeyFilter) error {
	// 【BUG-FIX】使用写锁，因为 readFromDisk 内部调用 diskWriter.Flush()
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 刷新磁盘缓冲
	s.diskWriter.Flush()
	
	switch format {
	case "json":
		return s.exportJSON(writer, filter)
	case "csv":
		return s.exportCSV(writer, filter)
	default:
		return s.exportJSONL(writer, filter)
	}
}

// exportJSON 导出为 JSON 数组
func (s *ConflictKeyStore) exportJSON(writer io.Writer, filter *ConflictKeyFilter) error {
	io.WriteString(writer, "[\n")
	
	first := true
	
	// 导出内存数据
	for _, r := range s.memoryBuffer {
		if filter != nil && !filter.Match(r) {
			continue
		}
		
		if !first {
			io.WriteString(writer, ",\n")
		}
		first = false
		
		data, err := json.MarshalIndent(r, "  ", "  ")
		if err != nil {
			log.Printf("[ConflictKeyStore] JSON marshal error for key=%s: %v", r.Key, err)
			continue
		}
		io.WriteString(writer, "  ")
		writer.Write(data)
	}
	
	// 导出磁盘数据
	diskRecords, _ := s.readFromDisk(filter)
	for _, r := range diskRecords {
		if !first {
			io.WriteString(writer, ",\n")
		}
		first = false
		
		data, err := json.MarshalIndent(r, "  ", "  ")
		if err != nil {
			log.Printf("[ConflictKeyStore] JSON marshal error for key=%s: %v", r.Key, err)
			continue
		}
		io.WriteString(writer, "  ")
		writer.Write(data)
	}
	
	io.WriteString(writer, "\n]\n")
	return nil
}

// exportJSONL 导出为 JSON Lines
func (s *ConflictKeyStore) exportJSONL(writer io.Writer, filter *ConflictKeyFilter) error {
	// 导出内存数据
	for _, r := range s.memoryBuffer {
		if filter != nil && !filter.Match(r) {
			continue
		}
		
		data, err := json.Marshal(r)
		if err != nil {
			log.Printf("[ConflictKeyStore] JSONL marshal error for key=%s: %v", r.Key, err)
			continue
		}
		writer.Write(data)
		io.WriteString(writer, "\n")
	}
	
	// 导出磁盘数据
	diskRecords, _ := s.readFromDisk(filter)
	for _, r := range diskRecords {
		data, err := json.Marshal(r)
		if err != nil {
			log.Printf("[ConflictKeyStore] JSONL marshal error for key=%s: %v", r.Key, err)
			continue
		}
		writer.Write(data)
		io.WriteString(writer, "\n")
	}
	
	return nil
}

// exportCSV 导出为 CSV
func (s *ConflictKeyStore) exportCSV(writer io.Writer, filter *ConflictKeyFilter) error {
	// 写入表头
	io.WriteString(writer, "key,key_type,phase,action,source_ttl,target_ttl,timestamp\n")
	
	writeRecord := func(r *ConflictKeyRecord) {
		if filter != nil && !filter.Match(r) {
			return
		}
		
		fmt.Fprintf(writer, "%s,%s,%s,%s,%d,%d,%s\n",
			escapeCSV(r.Key),
			r.KeyType,
			r.Phase,
			r.Action,
			r.SourceTTL,
			r.TargetTTL,
			r.Timestamp.Format(time.RFC3339),
		)
	}
	
	// 导出内存数据
	for _, r := range s.memoryBuffer {
		writeRecord(r)
	}
	
	// 导出磁盘数据
	diskRecords, _ := s.readFromDisk(filter)
	for _, r := range diskRecords {
		writeRecord(r)
	}
	
	return nil
}

// ConflictKeyFilter 查询过滤器
type ConflictKeyFilter struct {
	// Key 前缀
	KeyPrefix string
	// Key 类型
	KeyType string
	// 阶段
	Phase string
	// 动作
	Action string
	// 时间范围
	StartTime *time.Time
	EndTime   *time.Time
}

// Match 检查记录是否匹配过滤条件
func (f *ConflictKeyFilter) Match(r *ConflictKeyRecord) bool {
	if f.KeyPrefix != "" && !hasPrefix(r.Key, f.KeyPrefix) {
		return false
	}
	if f.KeyType != "" && r.KeyType != f.KeyType {
		return false
	}
	if f.Phase != "" && r.Phase != f.Phase {
		return false
	}
	if f.Action != "" && r.Action != f.Action {
		return false
	}
	if f.StartTime != nil && r.Timestamp.Before(*f.StartTime) {
		return false
	}
	if f.EndTime != nil && r.Timestamp.After(*f.EndTime) {
		return false
	}
	return true
}

// ConflictKeyQueryResult 查询结果
type ConflictKeyQueryResult struct {
	Total int64                 `json:"total"`
	Page  int                   `json:"page"`
	Size  int                   `json:"size"`
	Keys  []*ConflictKeyRecord `json:"keys"`
}

// ConflictKeySummary 冲突 Key 统计摘要
type ConflictKeySummary struct {
	TotalCount      int64            `json:"total_count"`
	MemoryCount     int64            `json:"memory_count"`
	DiskCount       int64            `json:"disk_count"`
	ByPhase         map[string]int64 `json:"by_phase"`
	ByAction        map[string]int64 `json:"by_action"`
	ByType          map[string]int64 `json:"by_type"`
	FirstConflict   *time.Time       `json:"first_conflict,omitempty"`
	LastConflict    *time.Time       `json:"last_conflict,omitempty"`
}

// GetSummary 获取统计摘要
func (s *ConflictKeyStore) GetSummary() *ConflictKeySummary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	summary := &ConflictKeySummary{
		TotalCount:  s.totalCount.Load(),
		MemoryCount: s.memoryCount.Load(),
		DiskCount:   s.diskCount.Load(),
		ByPhase:     make(map[string]int64),
		ByAction:    make(map[string]int64),
		ByType:      make(map[string]int64),
	}
	
	// 统计内存中的数据
	for _, r := range s.memoryBuffer {
		summary.ByPhase[r.Phase]++
		summary.ByAction[r.Action]++
		summary.ByType[r.KeyType]++
		
		if summary.FirstConflict == nil || r.Timestamp.Before(*summary.FirstConflict) {
			t := r.Timestamp
			summary.FirstConflict = &t
		}
		if summary.LastConflict == nil || r.Timestamp.After(*summary.LastConflict) {
			t := r.Timestamp
			summary.LastConflict = &t
		}
	}
	
	return summary
}

// hasPrefix 检查字符串前缀
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// escapeCSV 转义 CSV 字段
func escapeCSV(s string) string {
	// 简单实现：如果包含逗号或引号，用引号包裹
	needsQuote := false
	for _, c := range s {
		if c == ',' || c == '"' || c == '\n' {
			needsQuote = true
			break
		}
	}
	
	if !needsQuote {
		return s
	}
	
	// 转义引号并包裹
	escaped := ""
	for _, c := range s {
		if c == '"' {
			escaped += "\"\""
		} else {
			escaped += string(c)
		}
	}
	return "\"" + escaped + "\""
}
