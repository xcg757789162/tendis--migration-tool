// Package replication binlog 缓存管理
//
// 设计原则：
// 1. 原样存储 Master 推送的 binlog 数据（不解析，不转换）
// 2. 支持文件自动切分（不丢失数据）
// 3. 支持顺序回放
//
// 文件格式（每条记录）：
// +----------------+----------------+------------------+----------------+
// | Magic (4B)     | Length (4B)    | StoreID (4B)     | Data (Length B)|
// +----------------+----------------+------------------+----------------+
// | 0x42494E4C     | uint32 LE      | uint32 LE        | raw bytes      |
// | "BINL"         |                |                  |                |
// +----------------+----------------+------------------+----------------+
//
// 文件命名：binlog_cache_{storeID}_{fileIndex}.bin
// 切分策略：单文件超过 1GB 时自动切分
package replication

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// BinlogCacheMagic 缓存文件魔数 "BINL"
	BinlogCacheMagic uint32 = 0x42494E4C

	// BinlogCacheHeaderSize 记录头大小 = Magic(4) + Length(4) + StoreID(4)
	BinlogCacheHeaderSize = 12

	// DefaultMaxCacheFileSize 默认单文件最大大小 1GB
	DefaultMaxCacheFileSize = 1 << 30

	// MinCacheFileSize 最小文件大小 100MB（防止频繁切分）
	MinCacheFileSize = 100 << 20
)

// BinlogCacheConfig 缓存配置
type BinlogCacheConfig struct {
	// CacheDir 缓存目录
	CacheDir string
	// TaskID 任务ID（用于区分不同任务的缓存）
	TaskID string
	// MaxFileSize 单文件最大大小（超过后自动切分）
	MaxFileSize int64
	// FlushInterval 刷盘间隔（0 表示每次写入都刷盘）
	FlushInterval int
}

// BinlogCacheWriter binlog 缓存写入器
type BinlogCacheWriter struct {
	config    BinlogCacheConfig
	storeID   uint32
	file      *os.File
	writer    *bufio.Writer
	fileIndex int
	fileSize  int64
	mu        sync.Mutex

	// 统计信息
	totalRecords  atomic.Int64
	totalBytes    atomic.Int64
	filesCreated  atomic.Int32
	writeErrors   atomic.Int64

	// 状态
	closed atomic.Bool
}

// NewBinlogCacheWriter 创建缓存写入器
func NewBinlogCacheWriter(config BinlogCacheConfig, storeID uint32) (*BinlogCacheWriter, error) {
	if config.MaxFileSize <= 0 {
		config.MaxFileSize = DefaultMaxCacheFileSize
	}
	if config.MaxFileSize < MinCacheFileSize {
		config.MaxFileSize = MinCacheFileSize
	}

	// 确保缓存目录存在
	cacheDir := filepath.Join(config.CacheDir, config.TaskID)
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("create cache dir failed: %w", err)
	}

	w := &BinlogCacheWriter{
		config:    config,
		storeID:   storeID,
		fileIndex: 0,
	}

	// 创建第一个缓存文件
	if err := w.rotateFile(); err != nil {
		return nil, err
	}

	log.Printf("[BinlogCacheWriter] Created for storeID=%d, cacheDir=%s, maxFileSize=%d",
		storeID, cacheDir, config.MaxFileSize)

	return w, nil
}

// Write 写入一条 binlog 记录（原样存储）
func (w *BinlogCacheWriter) Write(data []byte) error {
	if w.closed.Load() {
		return fmt.Errorf("cache writer is closed")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	recordSize := int64(BinlogCacheHeaderSize + len(data))

	// 检查是否需要切分文件
	if w.fileSize+recordSize > w.config.MaxFileSize {
		if err := w.rotateFileLocked(); err != nil {
			w.writeErrors.Add(1)
			return fmt.Errorf("rotate file failed: %w", err)
		}
	}

	// 写入记录头
	header := make([]byte, BinlogCacheHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], BinlogCacheMagic)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(data)))
	binary.LittleEndian.PutUint32(header[8:12], w.storeID)

	if _, err := w.writer.Write(header); err != nil {
		w.writeErrors.Add(1)
		return fmt.Errorf("write header failed: %w", err)
	}

	// 写入数据（原样存储）
	if _, err := w.writer.Write(data); err != nil {
		w.writeErrors.Add(1)
		return fmt.Errorf("write data failed: %w", err)
	}

	w.fileSize += recordSize
	w.totalRecords.Add(1)
	w.totalBytes.Add(recordSize)

	return nil
}

// Flush 强制刷盘
func (w *BinlogCacheWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}
	if w.file != nil {
		return w.file.Sync()
	}
	return nil
}

// Close 关闭写入器
func (w *BinlogCacheWriter) Close() error {
	if w.closed.Swap(true) {
		return nil // 已经关闭
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var errs []error

	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			errs = append(errs, err)
		}
	}

	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			errs = append(errs, err)
		}
		if err := w.file.Close(); err != nil {
			errs = append(errs, err)
		}
		w.file = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}

	log.Printf("[BinlogCacheWriter] Closed storeID=%d, totalRecords=%d, totalBytes=%d, files=%d",
		w.storeID, w.totalRecords.Load(), w.totalBytes.Load(), w.filesCreated.Load())

	return nil
}

// GetStats 获取统计信息
func (w *BinlogCacheWriter) GetStats() map[string]int64 {
	return map[string]int64{
		"total_records": w.totalRecords.Load(),
		"total_bytes":   w.totalBytes.Load(),
		"files_created": int64(w.filesCreated.Load()),
		"write_errors":  w.writeErrors.Load(),
	}
}

// rotateFile 切分文件（需持有锁）
func (w *BinlogCacheWriter) rotateFile() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateFileLocked()
}

// rotateFileLocked 切分文件（已持有锁）
func (w *BinlogCacheWriter) rotateFileLocked() error {
	// 关闭旧文件
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return err
		}
		if err := w.file.Close(); err != nil {
			return err
		}
	}

	// 创建新文件
	filename := w.getFilename(w.fileIndex)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create cache file %s failed: %w", filename, err)
	}

	w.file = file
	w.writer = bufio.NewWriterSize(file, 64*1024) // 64KB 缓冲区
	w.fileSize = 0
	w.fileIndex++
	w.filesCreated.Add(1)

	log.Printf("[BinlogCacheWriter] Rotated to new file: %s (index=%d)", filename, w.fileIndex)

	return nil
}

// getFilename 获取缓存文件名
func (w *BinlogCacheWriter) getFilename(index int) string {
	cacheDir := filepath.Join(w.config.CacheDir, w.config.TaskID)
	return filepath.Join(cacheDir, fmt.Sprintf("binlog_cache_%d_%05d.bin", w.storeID, index))
}

// BinlogCacheReader binlog 缓存读取器（用于回放）
type BinlogCacheReader struct {
	config  BinlogCacheConfig
	storeID uint32
	files   []string
	current int
	file    *os.File
	reader  *bufio.Reader

	// 统计信息
	totalRecords atomic.Int64
	totalBytes   atomic.Int64
	readErrors   atomic.Int64
}

// NewBinlogCacheReader 创建缓存读取器
func NewBinlogCacheReader(config BinlogCacheConfig, storeID uint32) (*BinlogCacheReader, error) {
	cacheDir := filepath.Join(config.CacheDir, config.TaskID)

	// 查找所有缓存文件
	pattern := filepath.Join(cacheDir, fmt.Sprintf("binlog_cache_%d_*.bin", storeID))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob cache files failed: %w", err)
	}

	// 按文件名排序（确保顺序正确）
	sort.Strings(files)

	if len(files) == 0 {
		log.Printf("[BinlogCacheReader] No cache files found for storeID=%d", storeID)
	} else {
		log.Printf("[BinlogCacheReader] Found %d cache files for storeID=%d", len(files), storeID)
	}

	return &BinlogCacheReader{
		config:  config,
		storeID: storeID,
		files:   files,
		current: -1,
	}, nil
}

// Read 读取下一条 binlog 记录
// 返回 io.EOF 表示所有文件读取完毕
func (r *BinlogCacheReader) Read() ([]byte, error) {
	for {
		// 如果当前没有打开的文件，打开下一个
		if r.file == nil {
			if err := r.openNextFile(); err != nil {
				return nil, err // 可能是 io.EOF
			}
		}

		// 读取记录头
		header := make([]byte, BinlogCacheHeaderSize)
		_, err := io.ReadFull(r.reader, header)
		if err == io.EOF {
			// 当前文件读完，关闭并尝试下一个
			r.closeCurrentFile()
			continue
		}
		if err != nil {
			r.readErrors.Add(1)
			return nil, fmt.Errorf("read header failed: %w", err)
		}

		// 验证魔数
		magic := binary.LittleEndian.Uint32(header[0:4])
		if magic != BinlogCacheMagic {
			r.readErrors.Add(1)
			return nil, fmt.Errorf("invalid magic number: %x (expected %x)", magic, BinlogCacheMagic)
		}

		// 读取数据长度和 storeID
		length := binary.LittleEndian.Uint32(header[4:8])
		storeID := binary.LittleEndian.Uint32(header[8:12])

		// 验证 storeID
		if storeID != r.storeID {
			r.readErrors.Add(1)
			return nil, fmt.Errorf("storeID mismatch: %d (expected %d)", storeID, r.storeID)
		}

		// 读取数据
		data := make([]byte, length)
		_, err = io.ReadFull(r.reader, data)
		if err != nil {
			r.readErrors.Add(1)
			return nil, fmt.Errorf("read data failed: %w", err)
		}

		r.totalRecords.Add(1)
		r.totalBytes.Add(int64(BinlogCacheHeaderSize + length))

		return data, nil
	}
}

// openNextFile 打开下一个缓存文件
func (r *BinlogCacheReader) openNextFile() error {
	r.current++
	if r.current >= len(r.files) {
		return io.EOF // 所有文件读取完毕
	}

	filename := r.files[r.current]
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("open cache file %s failed: %w", filename, err)
	}

	r.file = file
	r.reader = bufio.NewReaderSize(file, 64*1024)

	log.Printf("[BinlogCacheReader] Opened file: %s (%d/%d)", filename, r.current+1, len(r.files))

	return nil
}

// closeCurrentFile 关闭当前文件
func (r *BinlogCacheReader) closeCurrentFile() {
	if r.file != nil {
		r.file.Close()
		r.file = nil
		r.reader = nil
	}
}

// Close 关闭读取器
func (r *BinlogCacheReader) Close() error {
	r.closeCurrentFile()

	log.Printf("[BinlogCacheReader] Closed storeID=%d, totalRecords=%d, totalBytes=%d, errors=%d",
		r.storeID, r.totalRecords.Load(), r.totalBytes.Load(), r.readErrors.Load())

	return nil
}

// GetStats 获取统计信息
func (r *BinlogCacheReader) GetStats() map[string]int64 {
	return map[string]int64{
		"total_records": r.totalRecords.Load(),
		"total_bytes":   r.totalBytes.Load(),
		"read_errors":   r.readErrors.Load(),
		"files_count":   int64(len(r.files)),
	}
}

// HasMoreFiles 是否还有未读取的文件
func (r *BinlogCacheReader) HasMoreFiles() bool {
	return r.current < len(r.files)-1
}

// BinlogCacheManager 缓存管理器（管理所有 storeID 的缓存）
type BinlogCacheManager struct {
	config  BinlogCacheConfig
	writers map[uint32]*BinlogCacheWriter
	mu      sync.RWMutex

	// 状态
	caching atomic.Bool // 是否正在缓存（全量阶段）
}

// NewBinlogCacheManager 创建缓存管理器
func NewBinlogCacheManager(config BinlogCacheConfig) *BinlogCacheManager {
	return &BinlogCacheManager{
		config:  config,
		writers: make(map[uint32]*BinlogCacheWriter),
	}
}

// StartCaching 开始缓存模式
func (m *BinlogCacheManager) StartCaching() {
	m.caching.Store(true)
	log.Printf("[BinlogCacheManager] Started caching mode")
}

// StopCaching 停止缓存模式（切换到实时回放）
func (m *BinlogCacheManager) StopCaching() {
	m.caching.Store(false)
	log.Printf("[BinlogCacheManager] Stopped caching mode")
}

// IsCaching 是否处于缓存模式
func (m *BinlogCacheManager) IsCaching() bool {
	return m.caching.Load()
}

// GetWriter 获取指定 storeID 的写入器
func (m *BinlogCacheManager) GetWriter(storeID uint32) (*BinlogCacheWriter, error) {
	m.mu.RLock()
	writer, exists := m.writers[storeID]
	m.mu.RUnlock()

	if exists {
		return writer, nil
	}

	// 创建新的写入器
	m.mu.Lock()
	defer m.mu.Unlock()

	// 双重检查
	if writer, exists = m.writers[storeID]; exists {
		return writer, nil
	}

	writer, err := NewBinlogCacheWriter(m.config, storeID)
	if err != nil {
		return nil, err
	}

	m.writers[storeID] = writer
	return writer, nil
}

// WriteBinlog 写入 binlog（根据当前模式决定是否缓存）
func (m *BinlogCacheManager) WriteBinlog(storeID uint32, data []byte) error {
	if !m.caching.Load() {
		return nil // 非缓存模式，不写入
	}

	writer, err := m.GetWriter(storeID)
	if err != nil {
		return err
	}

	return writer.Write(data)
}

// Flush 刷盘所有写入器
func (m *BinlogCacheManager) Flush() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var errs []error
	for _, writer := range m.writers {
		if err := writer.Flush(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("flush errors: %v", errs)
	}
	return nil
}

// Close 关闭所有写入器
func (m *BinlogCacheManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for storeID, writer := range m.writers {
		if err := writer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close writer %d: %w", storeID, err))
		}
	}
	m.writers = make(map[uint32]*BinlogCacheWriter)

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}

// GetAllStats 获取所有写入器的统计信息
func (m *BinlogCacheManager) GetAllStats() map[uint32]map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[uint32]map[string]int64)
	for storeID, writer := range m.writers {
		stats[storeID] = writer.GetStats()
	}
	return stats
}

// CleanupCache 清理缓存文件
func (m *BinlogCacheManager) CleanupCache() error {
	cacheDir := filepath.Join(m.config.CacheDir, m.config.TaskID)
	
	// 列出所有缓存文件
	files, err := filepath.Glob(filepath.Join(cacheDir, "binlog_cache_*.bin"))
	if err != nil {
		return err
	}

	for _, file := range files {
		if err := os.Remove(file); err != nil {
			log.Printf("[BinlogCacheManager] Failed to remove cache file %s: %v", file, err)
		}
	}

	log.Printf("[BinlogCacheManager] Cleaned up %d cache files in %s", len(files), cacheDir)
	return nil
}

// GetCacheDir 获取缓存目录
func (m *BinlogCacheManager) GetCacheDir() string {
	return filepath.Join(m.config.CacheDir, m.config.TaskID)
}

// GetCacheFiles 获取所有缓存文件列表
func (m *BinlogCacheManager) GetCacheFiles() ([]string, error) {
	cacheDir := filepath.Join(m.config.CacheDir, m.config.TaskID)
	return filepath.Glob(filepath.Join(cacheDir, "binlog_cache_*.bin"))
}

// GetTotalCacheSize 获取缓存文件总大小
func (m *BinlogCacheManager) GetTotalCacheSize() (int64, error) {
	files, err := m.GetCacheFiles()
	if err != nil {
		return 0, err
	}

	var total int64
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total, nil
}

// ParseStoreIDFromFilename 从文件名解析 storeID
func ParseStoreIDFromFilename(filename string) (uint32, error) {
	// 格式：binlog_cache_{storeID}_{fileIndex}.bin
	base := filepath.Base(filename)
	if !strings.HasPrefix(base, "binlog_cache_") || !strings.HasSuffix(base, ".bin") {
		return 0, fmt.Errorf("invalid cache filename: %s", base)
	}

	// 提取 storeID 部分
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(base, "binlog_cache_"), ".bin"), "_")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid cache filename format: %s", base)
	}

	var storeID uint32
	_, err := fmt.Sscanf(parts[0], "%d", &storeID)
	if err != nil {
		return 0, fmt.Errorf("parse storeID failed: %w", err)
	}

	return storeID, nil
}
