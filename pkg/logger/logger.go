package logger

import (
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
	mu          sync.RWMutex
	level       Level
	entries     []LogEntry
	maxEntries  int
	logFile     *os.File
	logDir      string
	entryID     int64
	writers     []io.Writer
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
	l := &Logger{
		level:      level,
		entries:    make([]LogEntry, 0, 10000),
		maxEntries: 10000,
		logDir:     logDir,
		writers:    []io.Writer{os.Stdout},
	}

	// 创建日志目录
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}

	// 打开日志文件
	logFile := filepath.Join(logDir, fmt.Sprintf("tendis-migrate-%s.log", time.Now().Format("2006-01-02")))
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	l.logFile = f
	l.writers = append(l.writers, f)

	return l, nil
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
	if len(l.entries) > l.maxEntries {
		l.entries = l.entries[len(l.entries)-l.maxEntries:]
	}

	// 写入到所有 writer
	logLine := l.formatEntry(entry)
	for _, w := range l.writers {
		w.Write([]byte(logLine))
	}
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

// Export 导出日志
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
		sb.WriteString(fmt.Sprintf("=== Tendis Migrate Log Export ===\n"))
		sb.WriteString(fmt.Sprintf("Export Time: %s\n", time.Now().Format("2006-01-02 15:04:05")))
		sb.WriteString(fmt.Sprintf("Total Entries: %d\n", len(entries)))
		sb.WriteString(fmt.Sprintf("Filter: level=%s, keyword=%s, task=%s\n", filter.Level, filter.Keyword, filter.TaskID))
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

// Clear 清除日志
func (l *Logger) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = make([]LogEntry, 0, l.maxEntries)
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
}

// Close 关闭日志器
func (l *Logger) Close() error {
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
