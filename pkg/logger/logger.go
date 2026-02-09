package logger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// RotationConfig 日志轮转配置
type RotationConfig struct {
	MaxFileSize    int64 // 单个文件最大大小（字节），默认 100MB
	MaxFiles       int   // 最多保留的日志文件数量，默认 7
	MaxAge         int   // 日志文件最大保留天数，默认 30
	CleanupEnabled bool  // 是否启用自动清理，默认 true
}

// DefaultRotationConfig 默认轮转配置
func DefaultRotationConfig() *RotationConfig {
	return &RotationConfig{
		MaxFileSize:    100 * 1024 * 1024, // 100MB
		MaxFiles:       7,
		MaxAge:         30,
		CleanupEnabled: true,
	}
}

// Level 日志级别
type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
	FATAL
)

func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel 解析日志级别
func ParseLevel(s string) Level {
	switch strings.ToUpper(s) {
	case "DEBUG":
		return DEBUG
	case "INFO":
		return INFO
	case "WARN", "WARNING":
		return WARN
	case "ERROR":
		return ERROR
	case "FATAL":
		return FATAL
	default:
		return INFO
	}
}

// LogEntry 日志条目
type LogEntry struct {
	ID        int64             `json:"id"`
	Timestamp string            `json:"timestamp"`
	Level     string            `json:"level"`
	Message   string            `json:"message"`
	Source    string            `json:"source,omitempty"`
	RequestID string            `json:"request_id,omitempty"`
	TaskID    string            `json:"task_id,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
	Stack     string            `json:"stack,omitempty"`
}

// Logger 日志记录器
type Logger struct {
	mu             sync.RWMutex
	level          Level
	entries        []LogEntry
	maxEntries     int
	maxTaskEntries int            // 每个任务的最大日志条目数
	taskEntries    map[string]int // 每个任务的当前日志条目数
	logFile        *os.File
	logDir         string
	entryID        int64
	writers        []io.Writer
	rotationConfig *RotationConfig
	currentLogPath string
	currentLogSize int64
	cleanupTicker  *time.Ticker
	stopCleanup    chan struct{}
}

var (
	defaultLogger *Logger
	once          sync.Once
)

// Init 初始化默认日志器
func Init(logDir string, level Level) error {
	var err error
	once.Do(func() {
		defaultLogger, err = NewLogger(logDir, level)
	})
	return err
}

// Default 获取默认日志器
func Default() *Logger {
	if defaultLogger == nil {
		defaultLogger, _ = NewLogger("./logs", INFO)
	}
	return defaultLogger
}

// NewLogger 创建新的日志器
func NewLogger(logDir string, level Level) (*Logger, error) {
	return NewLoggerWithRotation(logDir, level, DefaultRotationConfig())
}

// NewLoggerWithRotation 创建带轮转配置的日志器
func NewLoggerWithRotation(logDir string, level Level, rotation *RotationConfig) (*Logger, error) {
	if rotation == nil {
		rotation = DefaultRotationConfig()
	}

	l := &Logger{
		level:          level,
		entries:        make([]LogEntry, 0, 100000),
		maxEntries:     100000, // 增加总日志保留量
		maxTaskEntries: 500,    // 每个任务至少保留 500 条日志
		taskEntries:    make(map[string]int),
		logDir:         logDir,
		writers:        []io.Writer{os.Stdout},
		rotationConfig: rotation,
		stopCleanup:    make(chan struct{}),
	}

	// 创建日志目录
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}

	// 打开日志文件
	if err := l.openLogFile(); err != nil {
		return nil, err
	}

	// 启动自动清理（如果启用）
	if rotation.CleanupEnabled {
		l.startCleanupRoutine()
	}

	// 初始化时执行一次清理
	go l.cleanupOldLogs()

	return l, nil
}

// openLogFile 打开当天的日志文件
func (l *Logger) openLogFile() error {
	logPath := filepath.Join(l.logDir, fmt.Sprintf("tendis-migrate-%s.log", time.Now().Format("2006-01-02")))

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}

	// 获取当前文件大小
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("stat log file: %w", err)
	}

	// 如果之前有打开的文件，先关闭
	if l.logFile != nil {
		l.logFile.Close()
	}

	l.logFile = f
	l.currentLogPath = logPath
	l.currentLogSize = stat.Size()

	// 更新 writers（替换旧的文件 writer）
	l.writers = []io.Writer{os.Stdout, f}

	return nil
}

// rotateIfNeeded 检查是否需要轮转
func (l *Logger) rotateIfNeeded() {
	// 检查是否需要按日期轮转
	expectedPath := filepath.Join(l.logDir, fmt.Sprintf("tendis-migrate-%s.log", time.Now().Format("2006-01-02")))
	if l.currentLogPath != expectedPath {
		l.openLogFile()
		return
	}

	// 检查是否需要按大小轮转
	if l.rotationConfig.MaxFileSize > 0 && l.currentLogSize >= l.rotationConfig.MaxFileSize {
		l.rotateBySize()
	}
}

// rotateBySize 按大小轮转日志文件
func (l *Logger) rotateBySize() {
	if l.logFile == nil {
		return
	}

	// 关闭当前文件
	l.logFile.Close()

	// 重命名当前文件，添加时间戳后缀
	timestamp := time.Now().Format("150405")
	newPath := strings.TrimSuffix(l.currentLogPath, ".log") + "-" + timestamp + ".log"
	os.Rename(l.currentLogPath, newPath)

	// 打开新文件
	l.openLogFile()
}

// startCleanupRoutine 启动定时清理任务
func (l *Logger) startCleanupRoutine() {
	l.cleanupTicker = time.NewTicker(1 * time.Hour) // 每小时检查一次
	go func() {
		for {
			select {
			case <-l.cleanupTicker.C:
				l.cleanupOldLogs()
			case <-l.stopCleanup:
				l.cleanupTicker.Stop()
				return
			}
		}
	}()
}

// cleanupOldLogs 清理过期的日志文件
func (l *Logger) cleanupOldLogs() {
	if l.rotationConfig == nil {
		return
	}

	// 获取所有日志文件
	files, err := l.getLogFiles()
	if err != nil {
		return
	}

	if len(files) == 0 {
		return
	}

	now := time.Now()
	var removedCount, removedSize int64

	// 按修改时间排序（最新的在前）
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime().After(files[j].ModTime())
	})

	for i, f := range files {
		fullPath := filepath.Join(l.logDir, f.Name())

		// 不删除当前正在使用的文件
		if fullPath == l.currentLogPath {
			continue
		}

		shouldDelete := false

		// 按数量清理（保留最新的 MaxFiles 个文件）
		if l.rotationConfig.MaxFiles > 0 && i >= l.rotationConfig.MaxFiles {
			shouldDelete = true
		}

		// 按时间清理（删除超过 MaxAge 天的文件）
		if l.rotationConfig.MaxAge > 0 {
			age := now.Sub(f.ModTime()).Hours() / 24
			if int(age) > l.rotationConfig.MaxAge {
				shouldDelete = true
			}
		}

		if shouldDelete {
			if err := os.Remove(fullPath); err == nil {
				removedCount++
				removedSize += f.Size()
			}
		}
	}

	if removedCount > 0 {
		// 输出到 stdout（不使用 l.Info 避免递归）
		fmt.Printf("[%s] [INFO] Log cleanup completed: removed %d files, freed %s\n",
			time.Now().Format("2006-01-02 15:04:05"),
			removedCount,
			formatBytes(removedSize))
	}
}

// getLogFiles 获取日志目录中的所有日志文件
func (l *Logger) getLogFiles() ([]os.FileInfo, error) {
	entries, err := os.ReadDir(l.logDir)
	if err != nil {
		return nil, err
	}

	var files []os.FileInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "tendis-migrate-") && strings.HasSuffix(name, ".log") {
			info, err := entry.Info()
			if err == nil {
				files = append(files, info)
			}
		}
	}
	return files, nil
}

// formatBytes 格式化字节数
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// GetRotationConfig 获取轮转配置
func (l *Logger) GetRotationConfig() *RotationConfig {
	return l.rotationConfig
}

// SetRotationConfig 设置轮转配置
func (l *Logger) SetRotationConfig(config *RotationConfig) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rotationConfig = config
}

// GetLogStats 获取日志统计信息
func (l *Logger) GetLogStats() map[string]interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()

	files, _ := l.getLogFiles()
	var totalSize int64
	for _, f := range files {
		totalSize += f.Size()
	}

	return map[string]interface{}{
		"log_dir":           l.logDir,
		"current_file":      l.currentLogPath,
		"current_file_size": formatBytes(l.currentLogSize),
		"total_files":       len(files),
		"total_size":        formatBytes(totalSize),
		"memory_entries":    len(l.entries),
		"max_entries":       l.maxEntries,
		"rotation_config": map[string]interface{}{
			"max_file_size":    formatBytes(l.rotationConfig.MaxFileSize),
			"max_files":        l.rotationConfig.MaxFiles,
			"max_age_days":     l.rotationConfig.MaxAge,
			"cleanup_enabled":  l.rotationConfig.CleanupEnabled,
		},
	}
}

// CleanupNow 立即执行日志清理
func (l *Logger) CleanupNow() {
	l.cleanupOldLogs()
}

// SetLevel 设置日志级别
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// log 内部日志方法
func (l *Logger) log(level Level, requestID, taskID, msg string, fields map[string]interface{}) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// 检查是否需要轮转
	l.rotateIfNeeded()

	l.entryID++
	entry := LogEntry{
		ID:        l.entryID,
		Timestamp: time.Now().Format("2006-01-02 15:04:05.000"),
		Level:     level.String(),
		Message:   msg,
		RequestID: requestID,
		TaskID:    taskID,
		Fields:    fields,
	}

	// 获取调用位置
	if _, file, line, ok := runtime.Caller(2); ok {
		entry.Source = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}

	// 错误级别添加堆栈
	if level >= ERROR {
		buf := make([]byte, 4096)
		n := runtime.Stack(buf, false)
		entry.Stack = string(buf[:n])
	}

	// 存储到内存
	l.entries = append(l.entries, entry)
	
	// 更新任务日志计数
	if entry.TaskID != "" {
		l.taskEntries[entry.TaskID]++
	}
	
	// 智能日志淘汰：优先删除没有任务ID的日志或任务日志过多的日志
	if len(l.entries) > l.maxEntries {
		l.smartEvictEntries()
	}

	// 写入到所有 writer
	logLine := l.formatEntry(entry)
	for _, w := range l.writers {
		n, _ := w.Write([]byte(logLine))
		// 更新当前文件大小
		if w == l.logFile {
			l.currentLogSize += int64(n)
		}
	}
}

// smartEvictEntries 智能淘汰日志条目
// 优先删除：1) 没有任务ID的日志 2) 任务日志数量超过阈值的旧日志
func (l *Logger) smartEvictEntries() {
	toRemove := len(l.entries) - l.maxEntries
	if toRemove <= 0 {
		return
	}

	// 创建新的 entries 切片
	newEntries := make([]LogEntry, 0, l.maxEntries)
	removed := 0
	
	// 重新统计任务日志数量
	newTaskEntries := make(map[string]int)
	
	// 从旧到新遍历，优先删除没有任务ID的日志或任务日志过多的旧日志
	for _, e := range l.entries {
		shouldRemove := false
		
		if removed < toRemove {
			if e.TaskID == "" {
				// 没有任务ID的日志优先删除
				shouldRemove = true
			} else if newTaskEntries[e.TaskID] >= l.maxTaskEntries {
				// 该任务的日志已经足够多，删除更旧的
				shouldRemove = true
			}
		}
		
		if shouldRemove {
			removed++
		} else {
			newEntries = append(newEntries, e)
			if e.TaskID != "" {
				newTaskEntries[e.TaskID]++
			}
		}
	}
	
	// 如果还没删够，从头部继续删除
	if removed < toRemove {
		stillNeed := toRemove - removed
		if stillNeed < len(newEntries) {
			// 重新统计被保留的日志的任务计数
			finalTaskEntries := make(map[string]int)
			for _, e := range newEntries[stillNeed:] {
				if e.TaskID != "" {
					finalTaskEntries[e.TaskID]++
				}
			}
			newEntries = newEntries[stillNeed:]
			newTaskEntries = finalTaskEntries
		}
	}
	
	l.entries = newEntries
	l.taskEntries = newTaskEntries
}

// formatEntry 格式化日志条目
func (l *Logger) formatEntry(e LogEntry) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("[%s] [%s]", e.Timestamp, e.Level))
	
	if e.RequestID != "" {
		sb.WriteString(fmt.Sprintf(" [req:%s]", e.RequestID[:8]))
	}
	if e.TaskID != "" {
		sb.WriteString(fmt.Sprintf(" [task:%s]", e.TaskID[:8]))
	}
	if e.Source != "" {
		sb.WriteString(fmt.Sprintf(" [%s]", e.Source))
	}
	
	sb.WriteString(fmt.Sprintf(" %s", e.Message))
	
	if len(e.Fields) > 0 {
		fieldsJSON, _ := json.Marshal(e.Fields)
		sb.WriteString(fmt.Sprintf(" %s", string(fieldsJSON)))
	}
	
	sb.WriteString("\n")
	return sb.String()
}

// Debug 调试日志
func (l *Logger) Debug(msg string, fields ...map[string]interface{}) {
	f := mergeFields(fields)
	l.log(DEBUG, "", "", msg, f)
}

// Info 信息日志
func (l *Logger) Info(msg string, fields ...map[string]interface{}) {
	f := mergeFields(fields)
	l.log(INFO, "", "", msg, f)
}

// Warn 警告日志
func (l *Logger) Warn(msg string, fields ...map[string]interface{}) {
	f := mergeFields(fields)
	l.log(WARN, "", "", msg, f)
}

// Error 错误日志
func (l *Logger) Error(msg string, fields ...map[string]interface{}) {
	f := mergeFields(fields)
	l.log(ERROR, "", "", msg, f)
}

// Fatal 致命错误日志
func (l *Logger) Fatal(msg string, fields ...map[string]interface{}) {
	f := mergeFields(fields)
	l.log(FATAL, "", "", msg, f)
}

// WithRequest 带请求ID的日志
func (l *Logger) WithRequest(requestID string) *RequestLogger {
	return &RequestLogger{logger: l, requestID: requestID}
}

// WithTask 带任务ID的日志
func (l *Logger) WithTask(taskID string) *TaskLogger {
	return &TaskLogger{logger: l, taskID: taskID}
}

// GetEntries 获取日志条目
func (l *Logger) GetEntries(filter LogFilter) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var result []LogEntry
	for _, e := range l.entries {
		if l.matchFilter(e, filter) {
			result = append(result, e)
		}
	}

	// 按时间倒序
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID > result[j].ID
	})

	// 分页
	start := filter.Offset
	if start >= len(result) {
		return []LogEntry{}
	}
	end := start + filter.Limit
	if end > len(result) || filter.Limit == 0 {
		end = len(result)
	}

	return result[start:end]
}

// GetTotalCount 获取符合条件的日志总数
func (l *Logger) GetTotalCount(filter LogFilter) int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	count := 0
	for _, e := range l.entries {
		if l.matchFilter(e, filter) {
			count++
		}
	}
	return count
}

// matchFilter 匹配过滤条件
func (l *Logger) matchFilter(e LogEntry, f LogFilter) bool {
	if f.Level != "" && e.Level != strings.ToUpper(f.Level) {
		return false
	}
	if f.RequestID != "" && e.RequestID != f.RequestID {
		return false
	}
	if f.TaskID != "" && e.TaskID != f.TaskID {
		return false
	}
	if f.Keyword != "" && !strings.Contains(strings.ToLower(e.Message), strings.ToLower(f.Keyword)) {
		return false
	}
	if f.StartTime != "" {
		if e.Timestamp < f.StartTime {
			return false
		}
	}
	if f.EndTime != "" {
		if e.Timestamp > f.EndTime {
			return false
		}
	}
	return true
}

// Export 导出日志（仅内存中的日志）
func (l *Logger) Export(filter LogFilter, format string) ([]byte, error) {
	entries := l.GetEntries(LogFilter{
		Level:     filter.Level,
		RequestID: filter.RequestID,
		TaskID:    filter.TaskID,
		Keyword:   filter.Keyword,
		StartTime: filter.StartTime,
		EndTime:   filter.EndTime,
		Offset:    0,
		Limit:     0, // 导出全部
	})

	switch format {
	case "json":
		return json.MarshalIndent(entries, "", "  ")
	case "text":
		var sb strings.Builder
		sb.WriteString("=== Tendis Migrate Log Export ===\n")
		fmt.Fprintf(&sb, "Export Time: %s\n", time.Now().Format("2006-01-02 15:04:05"))
		fmt.Fprintf(&sb, "Total Entries: %d\n", len(entries))
		fmt.Fprintf(&sb, "Filter: level=%s, keyword=%s, task=%s\n", filter.Level, filter.Keyword, filter.TaskID)
		sb.WriteString(strings.Repeat("=", 50) + "\n\n")
		
		for _, e := range entries {
			sb.WriteString(l.formatEntry(e))
			if e.Stack != "" {
				sb.WriteString("Stack Trace:\n")
				sb.WriteString(e.Stack)
				sb.WriteString("\n")
			}
		}
		return []byte(sb.String()), nil
	default:
		return json.MarshalIndent(entries, "", "  ")
	}
}

// ExportFromDisk 从磁盘文件导出完整日志（包括所有历史日志）
// 支持按任务ID、级别、关键词过滤
func (l *Logger) ExportFromDisk(filter LogFilter, format string) ([]byte, error) {
	l.mu.RLock()
	logDir := l.logDir
	l.mu.RUnlock()

	// 获取所有日志文件
	files, err := l.getLogFiles()
	if err != nil {
		return nil, fmt.Errorf("failed to list log files: %w", err)
	}

	// 按时间排序（最旧的在前）
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime().Before(files[j].ModTime())
	})

	var sb strings.Builder
	var matchedLines int64
	var totalLines int64

	// 写入头部
	sb.WriteString("=== Tendis Migrate Full Log Export (Disk + Memory) ===\n")
	fmt.Fprintf(&sb, "Export Time: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(&sb, "Log Directory: %s\n", logDir)
	fmt.Fprintf(&sb, "Log Files: %d\n", len(files))
	fmt.Fprintf(&sb, "Filter: level=%s, keyword=%s, task=%s\n", filter.Level, filter.Keyword, filter.TaskID)
	if filter.StartTime != "" {
		fmt.Fprintf(&sb, "Start Time: %s\n", filter.StartTime)
	}
	if filter.EndTime != "" {
		fmt.Fprintf(&sb, "End Time: %s\n", filter.EndTime)
	}
	sb.WriteString(strings.Repeat("=", 60) + "\n\n")

	// 遍历所有日志文件
	for _, fileInfo := range files {
		filePath := filepath.Join(logDir, fileInfo.Name())
		
		file, err := os.Open(filePath)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(file)
		// 增大缓冲区以处理长行
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)

		for scanner.Scan() {
			line := scanner.Text()
			totalLines++

			// 应用过滤条件
			if l.matchLineFilter(line, filter) {
				sb.WriteString(line)
				sb.WriteString("\n")
				matchedLines++
			}
		}
		file.Close()
	}

	// 添加统计信息
	sb.WriteString("\n" + strings.Repeat("=", 60) + "\n")
	fmt.Fprintf(&sb, "Total Lines Scanned: %d\n", totalLines)
	fmt.Fprintf(&sb, "Matched Lines: %d\n", matchedLines)
	sb.WriteString(strings.Repeat("=", 60) + "\n")

	return []byte(sb.String()), nil
}

// matchLineFilter 检查日志行是否匹配过滤条件
func (l *Logger) matchLineFilter(line string, filter LogFilter) bool {
	// 空行跳过
	if strings.TrimSpace(line) == "" {
		return false
	}

	// 按任务ID过滤
	if filter.TaskID != "" {
		// 日志格式: [task:abc12345]
		taskPattern := fmt.Sprintf("[task:%s", filter.TaskID[:min(8, len(filter.TaskID))])
		if !strings.Contains(line, taskPattern) {
			return false
		}
	}

	// 按级别过滤
	if filter.Level != "" {
		levelPattern := fmt.Sprintf("[%s]", strings.ToUpper(filter.Level))
		if !strings.Contains(line, levelPattern) {
			return false
		}
	}

	// 按关键词过滤
	if filter.Keyword != "" {
		if !strings.Contains(strings.ToLower(line), strings.ToLower(filter.Keyword)) {
			return false
		}
	}

	// 按时间范围过滤（日志格式: [2026-02-08 12:30:45.123]）
	if filter.StartTime != "" || filter.EndTime != "" {
		// 提取时间戳
		if len(line) > 25 && line[0] == '[' {
			timeStr := line[1:24] // "2026-02-08 12:30:45.123"
			if filter.StartTime != "" && timeStr < filter.StartTime {
				return false
			}
			if filter.EndTime != "" && timeStr > filter.EndTime {
				return false
			}
		}
	}

	return true
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Clear 清除日志
func (l *Logger) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = make([]LogEntry, 0, l.maxEntries)
	l.taskEntries = make(map[string]int)
}

// ClearTaskLogs 清除指定任务的日志
func (l *Logger) ClearTaskLogs(taskID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	newEntries := make([]LogEntry, 0, len(l.entries))
	for _, e := range l.entries {
		if e.TaskID != taskID {
			newEntries = append(newEntries, e)
		}
	}
	l.entries = newEntries
	delete(l.taskEntries, taskID)
}

// Close 关闭日志器
func (l *Logger) Close() error {
	// 停止清理任务
	if l.stopCleanup != nil {
		close(l.stopCleanup)
	}
	if l.cleanupTicker != nil {
		l.cleanupTicker.Stop()
	}

	// 关闭日志文件
	if l.logFile != nil {
		return l.logFile.Close()
	}
	return nil
}

// LogFilter 日志过滤器
type LogFilter struct {
	Level     string `json:"level"`
	RequestID string `json:"request_id"`
	TaskID    string `json:"task_id"`
	Keyword   string `json:"keyword"`
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
	Offset    int    `json:"offset"`
	Limit     int    `json:"limit"`
}

// RequestLogger 请求日志器
type RequestLogger struct {
	logger    *Logger
	requestID string
}

func (r *RequestLogger) Debug(msg string, fields ...map[string]interface{}) {
	r.logger.log(DEBUG, r.requestID, "", msg, mergeFields(fields))
}

func (r *RequestLogger) Info(msg string, fields ...map[string]interface{}) {
	r.logger.log(INFO, r.requestID, "", msg, mergeFields(fields))
}

func (r *RequestLogger) Warn(msg string, fields ...map[string]interface{}) {
	r.logger.log(WARN, r.requestID, "", msg, mergeFields(fields))
}

func (r *RequestLogger) Error(msg string, fields ...map[string]interface{}) {
	r.logger.log(ERROR, r.requestID, "", msg, mergeFields(fields))
}

// TaskLogger 任务日志器
type TaskLogger struct {
	logger *Logger
	taskID string
}

func (t *TaskLogger) Debug(msg string, fields ...map[string]interface{}) {
	t.logger.log(DEBUG, "", t.taskID, msg, mergeFields(fields))
}

func (t *TaskLogger) Info(msg string, fields ...map[string]interface{}) {
	t.logger.log(INFO, "", t.taskID, msg, mergeFields(fields))
}

func (t *TaskLogger) Warn(msg string, fields ...map[string]interface{}) {
	t.logger.log(WARN, "", t.taskID, msg, mergeFields(fields))
}

func (t *TaskLogger) Error(msg string, fields ...map[string]interface{}) {
	t.logger.log(ERROR, "", t.taskID, msg, mergeFields(fields))
}

func mergeFields(fields []map[string]interface{}) map[string]interface{} {
	if len(fields) == 0 {
		return nil
	}
	result := make(map[string]interface{})
	for _, f := range fields {
		for k, v := range f {
			result[k] = v
		}
	}
	return result
}

// 包级别函数
func Debug(msg string, fields ...map[string]interface{}) {
	Default().Debug(msg, fields...)
}

func Info(msg string, fields ...map[string]interface{}) {
	Default().Info(msg, fields...)
}

func Warn(msg string, fields ...map[string]interface{}) {
	Default().Warn(msg, fields...)
}

func Error(msg string, fields ...map[string]interface{}) {
	Default().Error(msg, fields...)
}

func Fatal(msg string, fields ...map[string]interface{}) {
	Default().Fatal(msg, fields...)
}

func WithRequest(requestID string) *RequestLogger {
	return Default().WithRequest(requestID)
}

func WithTask(taskID string) *TaskLogger {
	return Default().WithTask(taskID)
}
