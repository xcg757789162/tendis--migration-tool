package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"tendis-migrate/pkg/logger"
)

type Task struct {
	ID             string  `json:"id"`
	Name           string  `json:"name"`
	Status         string  `json:"status"`
	Progress       float64 `json:"progress"`
	SourceCluster  string  `json:"source_cluster"`
	TargetCluster  string  `json:"target_cluster"`
	SourcePassword string  `json:"-"`
	TargetPassword string  `json:"-"`
	MigrationMode  string  `json:"migration_mode"` // full_only, full_and_incremental
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
	FullStartAt    string  `json:"full_start_at,omitempty"`    // 全量迁移开始时间
	IncrStartAt    string  `json:"incr_start_at,omitempty"`    // 增量迁移开始时间
	StartedAt      string  `json:"started_at,omitempty"`       // 任务开始时间（用于计算已耗时间）
	KeysTotal      int64   `json:"keys_total"`
	KeysMigrated   int64   `json:"keys_migrated"`
	KeysFailed     int64   `json:"keys_failed"`
	KeysSkipped    int64   `json:"keys_skipped"`
	KeysFiltered   int64   `json:"keys_filtered"`
	BytesMigrated  int64   `json:"bytes_migrated"`
	BytesTotal     int64   `json:"bytes_total"`
	Speed          int64   `json:"speed"`
	Phase          string  `json:"phase"` // full, incremental, completed
	// 配置选项
	Options *TaskOptions `json:"options,omitempty"`
}

// TaskTemplate 任务模板
type TaskTemplate struct {
	ID            string       `json:"id"`
	Name          string       `json:"name"`
	Description   string       `json:"description"`
	SourceCluster string       `json:"source_cluster"`
	TargetCluster string       `json:"target_cluster"`
	SourcePassword string      `json:"source_password,omitempty"`
	TargetPassword string      `json:"target_password,omitempty"`
	MigrationMode string       `json:"migration_mode"`
	Options       *TaskOptions `json:"options,omitempty"`
	CreatedAt     string       `json:"created_at"`
	UpdatedAt     string       `json:"updated_at"`
}

// TaskOptions 任务配置选项
type TaskOptions struct {
	WorkerCount       int        `json:"worker_count"`
	ScanBatchSize     int        `json:"scan_batch_size"`
	ConflictPolicy    string     `json:"conflict_policy"`     // skip, replace, error, skip_full_only
	LargeKeyThreshold int64      `json:"large_key_threshold"`
	EnableCompression bool       `json:"enable_compression"`
	SkipFullSync      bool       `json:"skip_full_sync"`
	SkipIncremental   bool       `json:"skip_incremental"`
	KeyFilter         *KeyFilter `json:"key_filter,omitempty"`
	RateLimit         *RateLimit `json:"rate_limit,omitempty"`
}

// RateLimit 限速配置
type RateLimit struct {
	SourceQPS         int `json:"source_qps"`          // 源端QPS限制，0表示不限制
	TargetQPS         int `json:"target_qps"`          // 目标端QPS限制，0表示不限制
	SourceConnections int `json:"source_connections"`  // 源端连接数
	TargetConnections int `json:"target_connections"`  // 目标端连接数
}

// ClusterInfo 集群信息（用于推荐配置）
type ClusterInfo struct {
	Addrs            []string `json:"addrs"`
	IsCluster        bool     `json:"is_cluster"`
	MasterCount      int      `json:"master_count"`
	TotalKeys        int64    `json:"total_keys"`
	UsedMemory       int64    `json:"used_memory"`
	UsedMemoryHuman  string   `json:"used_memory_human"`
	MaxMemory        int64    `json:"max_memory"`
	MaxClients       int      `json:"max_clients"`
	ConnectedClients int      `json:"connected_clients"`
	InstantaneousOPS int64    `json:"instantaneous_ops"`
	Version          string   `json:"version"`
	AvgKeySize       int64    `json:"avg_key_size"`       // 估算的平均key大小
	LargeKeyCount    int64    `json:"large_key_count"`    // 大key数量估算
}

// RecommendedConfig 推荐配置
type RecommendedConfig struct {
	WorkerCount       int    `json:"worker_count"`
	ScanBatchSize     int    `json:"scan_batch_size"`
	SourceQPS         int    `json:"source_qps"`
	TargetQPS         int    `json:"target_qps"`
	SourceConnections int    `json:"source_connections"`
	TargetConnections int    `json:"target_connections"`
	LargeKeyThreshold int64  `json:"large_key_threshold"`
	EstimatedTime     string `json:"estimated_time"`      // 预计耗时
	EstimatedSpeed    int64  `json:"estimated_speed"`     // 预计速度 keys/s
	Reason            string `json:"reason"`              // 推荐理由
}

// KeyFilter Key过滤配置
type KeyFilter struct {
	Mode            string   `json:"mode"` // all, prefix, pattern
	Prefixes        []string `json:"prefixes"`
	ExcludePrefixes []string `json:"exclude_prefixes"`
	Patterns        []string `json:"patterns"`
}

// ErrorKey 记录迁移失败或跳过的Key
type ErrorKey struct {
	Key       string `json:"key"`
	Type      string `json:"type"`
	Reason    string `json:"reason"`
	Detail    string `json:"detail"`
	Timestamp string `json:"timestamp"`
}

var (
	tasks      = make(map[string]*Task)
	tasksMu    sync.RWMutex
	templates  = make(map[string]*TaskTemplate)
	templateMu sync.RWMutex
	errorKeys  = make(map[string][]ErrorKey) // taskID -> error keys
	errorKeyMu sync.RWMutex
	startTime  time.Time
)

func main() {
	startTime = time.Now()
	
	// 初始化日志系统
	if err := logger.Init("./logs", logger.DEBUG); err != nil {
		fmt.Printf("Failed to init logger: %v\n", err)
	}
	
	logger.Info("🚀 Tendis Migration Tool starting", map[string]interface{}{
		"port":    8088,
		"version": "1.0.0",
		"pid":     fmt.Sprintf("%d", getPID()),
	})

	initDemoData()

	// 使用自定义 handler 统一处理
	server := &http.Server{
		Addr:         ":8088",
		Handler:      http.HandlerFunc(mainHandler),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	logger.Info("Server listening on http://localhost:8088")
	if err := server.ListenAndServe(); err != nil {
		logger.Fatal("Server failed to start", map[string]interface{}{"error": err.Error()})
	}
}

func mainHandler(w http.ResponseWriter, r *http.Request) {
	// 生成请求ID
	requestID := uuid.New().String()
	startTime := time.Now()
	
	// CORS
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("X-Request-ID", requestID)
	
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	path := r.URL.Path
	log := logger.WithRequest(requestID)
	
	// 记录请求日志
	log.Info("Request started", map[string]interface{}{
		"method": r.Method,
		"path":   path,
		"query":  r.URL.RawQuery,
		"remote": r.RemoteAddr,
		"ua":     r.UserAgent(),
	})

	// 包装 ResponseWriter 以捕获状态码
	rw := &responseWriter{ResponseWriter: w, statusCode: 200}

	// 路由处理
	switch {
	// 日志相关 API
	case path == "/api/v1/logs":
		logsHandler(rw, r, log)
	case path == "/api/v1/logs/export":
		logsExportHandler(rw, r, log)
	case path == "/api/v1/logs/clear":
		logsClearHandler(rw, r, log)
	case path == "/api/v1/logs/stats":
		logsStatsHandler(rw, r, log)
		
	// 业务 API
	case path == "/api/v1/health":
		healthHandler(rw, r, log)
	case path == "/api/v1/tasks":
		tasksHandler(rw, r, log)
	case strings.HasPrefix(path, "/api/v1/tasks/"):
		taskHandler(rw, r, log)
	case path == "/api/v1/system/status":
		systemHandler(rw, r, log)
	case path == "/api/v1/test-connection":
		testConnectionHandler(rw, r, log)
	case path == "/api/v1/analyze-cluster":
		analyzeClusterHandler(rw, r, log)
	case path == "/api/v1/recommend-config":
		recommendConfigHandler(rw, r, log)
	case path == "/api/v1/templates":
		templatesHandler(rw, r, log)
	case strings.HasPrefix(path, "/api/v1/templates/"):
		templateHandler(rw, r, log)
		
	// 静态资源
	case strings.HasPrefix(path, "/assets/"):
		http.FileServer(http.Dir("./web/dist")).ServeHTTP(rw, r)
		
	// SPA 入口
	default:
		http.ServeFile(rw, r, "./web/dist/index.html")
	}

	// 记录响应日志
	duration := time.Since(startTime)
	log.Info("Request completed", map[string]interface{}{
		"status":      rw.statusCode,
		"duration_ms": duration.Milliseconds(),
	})
}

// responseWriter 包装器
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func getPID() int {
	return 1 // 简化处理
}

func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func initDemoData() {
	// 不再初始化demo数据，让用户创建真实任务
	logger.Info("System initialized", map[string]interface{}{"mode": "production"})
}

// ==================== 日志 API ====================

func logsHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if limit == 0 {
		limit = 100
	}

	filter := logger.LogFilter{
		Level:     q.Get("level"),
		RequestID: q.Get("request_id"),
		TaskID:    q.Get("task_id"),
		Keyword:   q.Get("keyword"),
		StartTime: q.Get("start_time"),
		EndTime:   q.Get("end_time"),
		Offset:    offset,
		Limit:     limit,
	}

	entries := logger.Default().GetEntries(filter)
	total := logger.Default().GetTotalCount(filter)

	log.Debug("Logs queried", map[string]interface{}{
		"filter": filter,
		"count":  len(entries),
		"total":  total,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"items":  entries,
			"total":  total,
			"offset": offset,
			"limit":  limit,
		},
	})
}

func logsExportHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	format := q.Get("format")
	if format == "" {
		format = "text"
	}

	taskID := q.Get("task_id")
	filter := logger.LogFilter{
		Level:     q.Get("level"),
		RequestID: q.Get("request_id"),
		TaskID:    taskID,
		Keyword:   q.Get("keyword"),
		StartTime: q.Get("start_time"),
		EndTime:   q.Get("end_time"),
	}

	// 如果指定了任务ID，获取任务名称用于文件名
	taskName := ""
	if taskID != "" {
		tasksMu.RLock()
		for _, t := range tasks {
			if t.ID == taskID || strings.HasPrefix(t.ID, taskID) {
				taskName = t.Name
				filter.TaskID = t.ID // 使用完整ID
				break
			}
		}
		tasksMu.RUnlock()
	}

	data, err := logger.Default().Export(filter, format)
	if err != nil {
		log.Error("Failed to export logs", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 500, "message": err.Error()})
		return
	}

	// 生成文件名
	var filename string
	timestamp := time.Now().Format("20060102-150405")
	ext := "txt"
	if format == "json" {
		ext = "json"
	}
	if taskID != "" {
		shortID := taskID
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}
		if taskName != "" {
			// 清理任务名中的特殊字符
			safeName := strings.Map(func(r rune) rune {
				if r == '/' || r == '\\' || r == ':' || r == '*' || r == '?' || r == '"' || r == '<' || r == '>' || r == '|' {
					return '-'
				}
				return r
			}, taskName)
			filename = fmt.Sprintf("task-%s-%s-%s.%s", shortID, safeName, timestamp, ext)
		} else {
			filename = fmt.Sprintf("task-%s-logs-%s.%s", shortID, timestamp, ext)
		}
	} else {
		filename = fmt.Sprintf("tendis-migrate-logs-%s.%s", timestamp, ext)
	}
	
	if format == "json" {
		w.Header().Set("Content-Type", "application/json")
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Write(data)

	log.Info("Logs exported", map[string]interface{}{
		"format":   format,
		"size":     len(data),
		"filename": filename,
		"task_id":  taskID,
	})
}

func logsClearHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	logger.Default().Clear()
	log.Info("Logs cleared")

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
	})
}

func logsStatsHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	l := logger.Default()
	
	// 统计各级别日志数量
	stats := map[string]int{
		"DEBUG": 0,
		"INFO":  0,
		"WARN":  0,
		"ERROR": 0,
		"FATAL": 0,
	}
	
	allEntries := l.GetEntries(logger.LogFilter{Limit: 0})
	for _, e := range allEntries {
		stats[e.Level]++
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"total":     len(allEntries),
			"by_level":  stats,
			"uptime":    time.Since(startTime).String(),
			"memory_mb": getMemoryUsage(),
		},
	})
}

func getMemoryUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / 1024 / 1024
}

// ==================== 业务 API ====================

func healthHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	log.Debug("Health check")
	jsonResponse(w, map[string]interface{}{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	})
}

func tasksHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	switch r.Method {
	case "GET":
		listTasksHandler(w, r, log)
	case "POST":
		createTaskHandler(w, r, log)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func listTasksHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	tasksMu.RLock()
	defer tasksMu.RUnlock()

	var items []map[string]interface{}
	for _, t := range tasks {
		items = append(items, map[string]interface{}{
			"id":         t.ID,
			"name":       t.Name,
			"status":     t.Status,
			"created_at": t.CreatedAt,
			"updated_at": t.UpdatedAt,
			"progress": map[string]interface{}{
				"percentage":    t.Progress,
				"keys_total":    t.KeysTotal,
				"keys_migrated": t.KeysMigrated,
				"speed":         t.Speed,
			},
		})
	}

	log.Debug("Tasks listed", map[string]interface{}{"count": len(items)})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"items": items,
			"total": len(items),
		},
	})
}

func createTaskHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	var req struct {
		Name          string `json:"name"`
		MigrationMode string `json:"migration_mode"`
		SourceCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"source_cluster"`
		TargetCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"target_cluster"`
		Options *TaskOptions `json:"options"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Error("Failed to decode request", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 400, "message": err.Error()})
		return
	}

	mode := req.MigrationMode
	if mode == "" {
		mode = "full_and_incremental"
	}

	// 设置默认选项
	options := req.Options
	if options == nil {
		options = &TaskOptions{
			WorkerCount:       4,
			ScanBatchSize:     1000,
			ConflictPolicy:    "skip_full_only",
			LargeKeyThreshold: 10485760,
			EnableCompression: true,
			KeyFilter: &KeyFilter{
				Mode: "all",
			},
		}
	} else {
		// 确保 KeyFilter 有默认值
		if options.KeyFilter == nil {
			options.KeyFilter = &KeyFilter{Mode: "all"}
		} else if options.KeyFilter.Mode == "" {
			options.KeyFilter.Mode = "all"
		}
		// 确保其他选项有默认值
		if options.WorkerCount == 0 {
			options.WorkerCount = 4
		}
		if options.ScanBatchSize == 0 {
			options.ScanBatchSize = 1000
		}
		if options.ConflictPolicy == "" {
			options.ConflictPolicy = "skip_full_only"
		}
		if options.LargeKeyThreshold == 0 {
			options.LargeKeyThreshold = 10485760
		}
	}

	task := &Task{
		ID:             uuid.New().String(),
		Name:           req.Name,
		Status:         "pending",
		Progress:       0,
		SourceCluster:  strings.Join(req.SourceCluster.Addrs, ","),
		TargetCluster:  strings.Join(req.TargetCluster.Addrs, ","),
		SourcePassword: req.SourceCluster.Password,
		TargetPassword: req.TargetCluster.Password,
		MigrationMode:  mode,
		CreatedAt:      time.Now().Format(time.RFC3339),
		UpdatedAt:      time.Now().Format(time.RFC3339),
		Phase:          "full",
		Options:        options,
	}

	tasksMu.Lock()
	tasks[task.ID] = task
	
	// 保留最近3个任务，清理旧任务
	cleanupOldTasks()
	tasksMu.Unlock()

	log.Info("Task created", map[string]interface{}{
		"task_id":        task.ID,
		"task_name":      task.Name,
		"migration_mode": mode,
	})

	// 同时记录任务日志
	logger.WithTask(task.ID).Info("Task created", map[string]interface{}{
		"name":           task.Name,
		"source":         task.SourceCluster,
		"target":         task.TargetCluster,
		"migration_mode": mode,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    map[string]string{"task_id": task.ID},
	})
}

// cleanupOldTasks 清理旧任务，保留最近3个（必须在tasksMu锁内调用）
func cleanupOldTasks() {
	const maxTasks = 3
	if len(tasks) <= maxTasks {
		return
	}
	
	// 获取所有任务并按创建时间排序
	taskList := make([]*Task, 0, len(tasks))
	for _, t := range tasks {
		taskList = append(taskList, t)
	}
	sort.Slice(taskList, func(i, j int) bool {
		return taskList[i].CreatedAt > taskList[j].CreatedAt // 降序，最新在前
	})
	
	// 删除超出限制的旧任务
	for i := maxTasks; i < len(taskList); i++ {
		oldTask := taskList[i]
		// 如果任务正在运行，跳过
		if oldTask.Status == "running" {
			continue
		}
		// 清理该任务的日志
		logger.Default().ClearTaskLogs(oldTask.ID)
		// 删除任务
		delete(tasks, oldTask.ID)
		logger.Default().Info("Old task cleaned up", map[string]interface{}{
			"task_id":   oldTask.ID,
			"task_name": oldTask.Name,
		})
	}
}

func taskHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/tasks/")
	parts := strings.Split(path, "/")

	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	id := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	taskLog := logger.WithTask(id)

	switch {
	case action == "" && r.Method == "GET":
		getTaskHandler(w, r, id, log)
	case action == "" && r.Method == "DELETE":
		deleteTaskHandler(w, r, id, log, taskLog)
	case action == "start" && r.Method == "POST":
		startTaskHandler(w, r, id, log, taskLog)
	case action == "pause" && r.Method == "POST":
		pauseTaskHandler(w, r, id, log, taskLog)
	case action == "resume" && r.Method == "POST":
		resumeTaskHandler(w, r, id, log, taskLog)
	case action == "progress" && r.Method == "GET":
		progressHandler(w, r, id, log)
	case action == "logs" && r.Method == "GET":
		taskLogsHandler(w, r, id, log)
	case action == "verify" && r.Method == "POST":
		triggerVerifyHandler(w, r, id, log)
	case strings.HasPrefix(action, "verify") && r.Method == "GET":
		verifyResultsHandler(w, r, id, log)
	case action == "error-keys" && r.Method == "GET":
		errorKeysHandler(w, r, id, log)
	case strings.HasPrefix(action, "error-keys/download") && r.Method == "GET":
		downloadErrorKeysHandler(w, r, id, log)
	default:
		http.NotFound(w, r)
	}
}

func getTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		log.Warn("Task not found", map[string]interface{}{"task_id": id})
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	log.Debug("Task retrieved", map[string]interface{}{"task_id": id})

	phase := task.Phase
	if phase == "" {
		phase = "full"
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"id":             task.ID,
			"name":           task.Name,
			"status":         task.Status,
			"migration_mode": task.MigrationMode,
			"source_cluster": map[string]interface{}{"addrs": strings.Split(task.SourceCluster, ",")},
			"target_cluster": map[string]interface{}{"addrs": strings.Split(task.TargetCluster, ",")},
			"created_at":     task.CreatedAt,
			"updated_at":     task.UpdatedAt,
			"started_at":     task.StartedAt,
			"full_start_at":  task.FullStartAt,
			"incr_start_at":  task.IncrStartAt,
			"progress": map[string]interface{}{
				"percentage":     task.Progress,
				"total_keys":     task.KeysTotal,
				"migrated_keys":  task.KeysMigrated,
				"total_bytes":    task.BytesTotal,
				"migrated_bytes": task.BytesMigrated,
				"current_speed":  task.Speed,
				"phase":          phase,
				"estimated_eta":  calculateETA(task),
				"elapsed_time":   calculateElapsedTime(task),
			},
			"stats": map[string]interface{}{
				"total_keys":     task.KeysTotal,
				"migrated_keys":  task.KeysMigrated,
				"failed_keys":    task.KeysFailed,
				"skipped_keys":   task.KeysSkipped,
				"filtered_keys":  task.KeysFiltered,
				"bytes_sent":     task.BytesMigrated,
			},
		},
	})
}

func calculateETA(task *Task) string {
	if task.Speed <= 0 || task.KeysTotal <= task.KeysMigrated {
		return "-"
	}
	remaining := task.KeysTotal - task.KeysMigrated
	seconds := remaining / task.Speed
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	} else if seconds < 3600 {
		return fmt.Sprintf("%dm %ds", seconds/60, seconds%60)
	}
	return fmt.Sprintf("%dh %dm", seconds/3600, (seconds%3600)/60)
}

// calculateElapsedTime 计算已耗时间
func calculateElapsedTime(task *Task) string {
	if task.StartedAt == "" {
		return "-"
	}
	// 使用本地时区解析时间
	loc := time.Local
	startTime, err := time.ParseInLocation("2006-01-02 15:04:05", task.StartedAt, loc)
	if err != nil {
		return "-"
	}
	elapsed := time.Since(startTime)
	seconds := int64(elapsed.Seconds())
	if seconds < 0 {
		return "-"
	}
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	} else if seconds < 3600 {
		return fmt.Sprintf("%dm %ds", seconds/60, seconds%60)
	} else if seconds < 86400 {
		return fmt.Sprintf("%dh %dm", seconds/3600, (seconds%3600)/60)
	}
	return fmt.Sprintf("%dd %dh", seconds/86400, (seconds%86400)/3600)
}

func deleteTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	delete(tasks, id)
	tasksMu.Unlock()
	
	log.Info("Task deleted", map[string]interface{}{"task_id": id})
	taskLog.Info("Task deleted")
	
	jsonResponse(w, map[string]interface{}{"code": 0, "message": "success"})
}

func startTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	if ok {
		task.Status = "running"
		task.UpdatedAt = time.Now().Format(time.RFC3339)
		task.StartedAt = time.Now().Format("2006-01-02 15:04:05")
		go simulateProgress(task)
	}
	tasksMu.Unlock()
	
	if ok {
		log.Info("Task started", map[string]interface{}{"task_id": id})
		taskLog.Info("Task started", map[string]interface{}{
			"keys_total": task.KeysTotal,
		})
	} else {
		log.Warn("Task not found for start", map[string]interface{}{"task_id": id})
	}
	
	jsonResponse(w, map[string]interface{}{"code": 0, "message": "success"})
}

func pauseTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	if ok {
		task.Status = "paused"
		task.Speed = 0
		task.UpdatedAt = time.Now().Format(time.RFC3339)
	}
	tasksMu.Unlock()
	
	if ok {
		log.Info("Task paused", map[string]interface{}{"task_id": id})
		taskLog.Info("Task paused", map[string]interface{}{
			"progress": task.Progress,
		})
	}
	
	jsonResponse(w, map[string]interface{}{"code": 0, "message": "success"})
}

func resumeTaskHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger, taskLog *logger.TaskLogger) {
	tasksMu.Lock()
	task, ok := tasks[id]
	if ok {
		task.Status = "running"
		task.UpdatedAt = time.Now().Format(time.RFC3339)
		go simulateProgress(task)
	}
	tasksMu.Unlock()
	
	if ok {
		log.Info("Task resumed", map[string]interface{}{"task_id": id})
		taskLog.Info("Task resumed")
	}
	
	jsonResponse(w, map[string]interface{}{"code": 0, "message": "success"})
}

func progressHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		log.Warn("Task not found for progress", map[string]interface{}{"task_id": id})
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"percentage":    task.Progress,
			"keys_total":    task.KeysTotal,
			"keys_migrated": task.KeysMigrated,
			"speed":         task.Speed,
			"phase":         "full",
			"eta":           int64((100 - task.Progress) / 0.5 * 2),
		},
	})
}

func taskLogsHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if limit == 0 {
		limit = 100
	}

	filter := logger.LogFilter{
		TaskID:  id,
		Level:   q.Get("level"),
		Keyword: q.Get("keyword"),
		Offset:  offset,
		Limit:   limit,
	}

	entries := logger.Default().GetEntries(filter)
	total := logger.Default().GetTotalCount(filter)

	log.Debug("Task logs queried", map[string]interface{}{
		"task_id": id,
		"count":   len(entries),
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"items":  entries,
			"total":  total,
			"offset": offset,
			"limit":  limit,
		},
	})
}

func triggerVerifyHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	log.Info("Verify triggered", map[string]interface{}{"task_id": id})
	
	// 模拟触发校验
	batchID := uuid.New().String()
	
	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    map[string]string{"batch_id": batchID},
	})
}

func verifyResultsHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	log.Debug("Verify results queried", map[string]interface{}{"task_id": id})
	
	// 返回空数组，暂无校验结果
	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    []interface{}{},
	})
}

func simulateProgress(task *Task) {
	taskLog := logger.WithTask(task.ID)
	taskLog.Info("Migration started - connecting to clusters")

	ctx := context.Background()

	// 解析源端和目标端地址
	sourceAddrs := strings.Split(task.SourceCluster, ",")
	targetAddrs := strings.Split(task.TargetCluster, ",")

	for i := range sourceAddrs {
		sourceAddrs[i] = strings.TrimSpace(sourceAddrs[i])
	}
	for i := range targetAddrs {
		targetAddrs[i] = strings.TrimSpace(targetAddrs[i])
	}

	// 尝试连接源端
	sourceClient, sourceIsCluster, err := connectRedis(ctx, sourceAddrs, task.SourcePassword)
	if err != nil {
		taskLog.Error("Failed to connect source cluster", map[string]interface{}{"error": err.Error()})
		tasksMu.Lock()
		task.Status = "failed"
		task.UpdatedAt = time.Now().Format(time.RFC3339)
		tasksMu.Unlock()
		return
	}
	defer sourceClient.Close()

	// 尝试连接目标端
	targetClient, targetIsCluster, err := connectRedis(ctx, targetAddrs, task.TargetPassword)
	if err != nil {
		taskLog.Error("Failed to connect target cluster", map[string]interface{}{"error": err.Error()})
		tasksMu.Lock()
		task.Status = "failed"
		task.UpdatedAt = time.Now().Format(time.RFC3339)
		tasksMu.Unlock()
		return
	}
	defer targetClient.Close()

	taskLog.Info("Connected to clusters", map[string]interface{}{
		"source_mode": map[bool]string{true: "cluster", false: "standalone"}[sourceIsCluster],
		"target_mode": map[bool]string{true: "cluster", false: "standalone"}[targetIsCluster],
	})

	// 记录Key过滤配置
	if task.Options != nil && task.Options.KeyFilter != nil {
		taskLog.Info("Key filter config", map[string]interface{}{
			"mode":             task.Options.KeyFilter.Mode,
			"prefixes":         task.Options.KeyFilter.Prefixes,
			"exclude_prefixes": task.Options.KeyFilter.ExcludePrefixes,
			"patterns":         task.Options.KeyFilter.Patterns,
		})
	}

	// 获取源端Key总数
	totalKeys, err := getDBSize(ctx, sourceClient, sourceIsCluster)
	if err != nil {
		taskLog.Warn("Failed to get source DB size, using estimate", map[string]interface{}{"error": err.Error()})
		totalKeys = 10000 // 默认估算值
	}

	tasksMu.Lock()
	task.KeysTotal = totalKeys
	task.BytesTotal = totalKeys * 256 // 估算平均每个key 256 bytes
	task.FullStartAt = time.Now().Format("2006-01-02 15:04:05")
	tasksMu.Unlock()

	taskLog.Info("Starting full migration", map[string]interface{}{
		"total_keys": totalKeys,
	})

	// 执行全量迁移
	doFullMigration(ctx, task, sourceClient, targetClient, sourceIsCluster, targetIsCluster, taskLog)

	// 检查是否需要增量迁移
	tasksMu.RLock()
	status := task.Status
	mode := task.MigrationMode
	tasksMu.RUnlock()

	if status == "running" && mode == "full_and_incremental" {
		taskLog.Info("Starting incremental sync")
		tasksMu.Lock()
		task.Phase = "incremental"
		task.IncrStartAt = time.Now().Format("2006-01-02 15:04:05")
		tasksMu.Unlock()
		// 增量同步逻辑（简化版本：持续监听）
		doIncrementalSync(ctx, task, sourceClient, targetClient, sourceIsCluster, targetIsCluster, taskLog)
	}
}

// connectRedis 连接Redis，返回通用客户端接口
func connectRedis(ctx context.Context, addrs []string, password string) (redis.UniversalClient, bool, error) {
	// 先尝试集群模式
	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		Password: password,
	})
	if err := clusterClient.Ping(ctx).Err(); err == nil {
		return clusterClient, true, nil
	}
	clusterClient.Close()

	// 尝试单机模式
	standaloneClient := redis.NewClient(&redis.Options{
		Addr:     addrs[0],
		Password: password,
	})
	if err := standaloneClient.Ping(ctx).Err(); err != nil {
		standaloneClient.Close()
		return nil, false, err
	}
	return standaloneClient, false, nil
}

// getDBSize 获取数据库Key数量
func getDBSize(ctx context.Context, client redis.UniversalClient, isCluster bool) (int64, error) {
	if !isCluster {
		return client.DBSize(ctx).Result()
	}

	// 集群模式需要遍历所有节点
	clusterClient := client.(*redis.ClusterClient)
	var total int64
	var mu sync.Mutex

	err := clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		size, err := node.DBSize(ctx).Result()
		if err != nil {
			return err
		}
		mu.Lock()
		total += size
		mu.Unlock()
		return nil
	})
	return total, err
}

// doFullMigration 执行全量迁移（并行Worker模式）
func doFullMigration(ctx context.Context, task *Task, sourceClient, targetClient redis.UniversalClient, sourceIsCluster, targetIsCluster bool, taskLog *logger.TaskLogger) {
	// 获取配置参数
	batchSize := int64(1000)
	workerCount := 4
	var rateLimiter *RateLimiter

	if task.Options != nil {
		if task.Options.ScanBatchSize > 0 {
			batchSize = int64(task.Options.ScanBatchSize)
		}
		if task.Options.WorkerCount > 0 {
			workerCount = task.Options.WorkerCount
		}
		// 初始化限速器
		if task.Options.RateLimit != nil && task.Options.RateLimit.SourceQPS > 0 {
			rateLimiter = NewRateLimiter(task.Options.RateLimit.SourceQPS)
		}
	}

	// 获取冲突策略
	conflictPolicy := "skip_full_only"
	if task.Options != nil && task.Options.ConflictPolicy != "" {
		conflictPolicy = task.Options.ConflictPolicy
	}

	taskLog.Info("Starting parallel migration", map[string]interface{}{
		"worker_count": workerCount,
		"batch_size":   batchSize,
		"policy":       conflictPolicy,
	})

	// 统计计数器（使用原子操作）
	var migratedCount int64
	var migratedBytes int64
	var failedCount int64
	var skippedCount int64
	var filteredCount int64
	startTime := time.Now()
	lastLogTime := time.Now()
	var lastLogMu sync.Mutex

	// 用于追踪已处理的key（避免重复处理）
	processedKeys := sync.Map{}

	// 创建Key通道
	keyChan := make(chan string, workerCount*100)
	var wg sync.WaitGroup

	// 启动Worker协程池
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for key := range keyChan {
				// 检查任务状态
				tasksMu.RLock()
				status := task.Status
				tasksMu.RUnlock()
				if status != "running" {
					continue
				}

				// 检查是否已处理
				if _, loaded := processedKeys.LoadOrStore(key, true); loaded {
					continue
				}

				// 限速
				if rateLimiter != nil {
					rateLimiter.Wait()
				}

				// 检查Key是否匹配过滤规则
				if !matchKeyFilter(key, task.Options) {
					atomic.AddInt64(&filteredCount, 1)
					continue
				}

				// 迁移Key
				migrated, bytes, reason := migrateKeyWithPolicy(ctx, sourceClient, targetClient, key, conflictPolicy)

				if migrated {
					atomic.AddInt64(&migratedCount, 1)
					atomic.AddInt64(&migratedBytes, bytes)
				} else if reason == "skipped" {
					atomic.AddInt64(&skippedCount, 1)
				} else if reason == "filtered" {
					atomic.AddInt64(&filteredCount, 1)
				} else {
					atomic.AddInt64(&failedCount, 1)
					addErrorKey(task.ID, key, "string", "failed", reason)
				}
			}
		}(i)
	}

	// 进度更新协程
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopProgress:
				return
			case <-ticker.C:
				mc := atomic.LoadInt64(&migratedCount)
				mb := atomic.LoadInt64(&migratedBytes)
				fc := atomic.LoadInt64(&failedCount)
				sc := atomic.LoadInt64(&skippedCount)
				ftc := atomic.LoadInt64(&filteredCount)

				tasksMu.Lock()
				task.KeysMigrated = mc
				task.BytesMigrated = mb
				task.KeysFailed = fc
				task.KeysSkipped = sc
				task.KeysFiltered = ftc
				if task.KeysTotal > 0 {
					task.Progress = float64(mc+sc+ftc) / float64(task.KeysTotal) * 100
					if task.Progress > 100 {
						task.Progress = 100
					}
				}
				elapsed := time.Since(startTime).Seconds()
				if elapsed > 0 {
					task.Speed = int64(float64(mc) / elapsed)
				}
				task.UpdatedAt = time.Now().Format(time.RFC3339)
				tasksMu.Unlock()

				// 每10秒记录一次日志
				lastLogMu.Lock()
				if time.Since(lastLogTime) > 10*time.Second {
					taskLog.Info("Migration progress", map[string]interface{}{
						"progress":      fmt.Sprintf("%.1f%%", task.Progress),
						"migrated_keys": mc,
						"failed_keys":   fc,
						"skipped_keys":  sc,
						"filtered_keys": ftc,
						"speed":         task.Speed,
						"workers":       workerCount,
					})
					lastLogTime = time.Now()
				}
				lastLogMu.Unlock()
			}
		}
	}()

	// SCAN并分发Key到Worker
	if sourceIsCluster {
		// 集群模式：并行遍历所有master节点
		clusterClient := sourceClient.(*redis.ClusterClient)
		var scanWg sync.WaitGroup

		clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			scanWg.Add(1)
			go func(nodeClient *redis.Client) {
				defer scanWg.Done()
				var cursor uint64
				for {
					tasksMu.RLock()
					status := task.Status
					tasksMu.RUnlock()
					if status != "running" {
						return
					}

					keys, newCursor, err := nodeClient.Scan(ctx, cursor, "*", batchSize).Result()
					if err != nil {
						taskLog.Warn("SCAN failed on node", map[string]interface{}{"error": err.Error()})
						time.Sleep(time.Second)
						continue
					}

					for _, key := range keys {
						tasksMu.RLock()
						status := task.Status
						tasksMu.RUnlock()
						if status != "running" {
							return
						}
						keyChan <- key
					}

					cursor = newCursor
					if cursor == 0 {
						break
					}
				}
			}(node)
			return nil
		})

		scanWg.Wait()
	} else {
		// 单机模式
		var cursor uint64
		for {
			tasksMu.RLock()
			status := task.Status
			tasksMu.RUnlock()
			if status != "running" {
				break
			}

			keys, newCursor, err := sourceClient.Scan(ctx, cursor, "*", batchSize).Result()
			if err != nil {
				taskLog.Error("SCAN failed", map[string]interface{}{"error": err.Error()})
				time.Sleep(time.Second)
				continue
			}

			for _, key := range keys {
				tasksMu.RLock()
				status := task.Status
				tasksMu.RUnlock()
				if status != "running" {
					break
				}
				keyChan <- key
			}

			cursor = newCursor
			if cursor == 0 {
				break
			}
		}
	}

	// 关闭通道，等待所有Worker完成
	close(keyChan)
	wg.Wait()
	close(stopProgress)

	// 最终更新统计
	mc := atomic.LoadInt64(&migratedCount)
	fc := atomic.LoadInt64(&failedCount)
	sc := atomic.LoadInt64(&skippedCount)
	ftc := atomic.LoadInt64(&filteredCount)

	tasksMu.Lock()
	task.KeysMigrated = mc
	task.KeysFailed = fc
	task.KeysSkipped = sc
	task.KeysFiltered = ftc
	if task.Status == "running" {
		if task.MigrationMode == "full_only" {
			task.Status = "completed"
			task.Progress = 100
			task.Phase = "completed"
		} else {
			// 全量迁移完成，准备进入增量同步
			task.Progress = 100
			task.Phase = "incremental"
		}
	}
	task.UpdatedAt = time.Now().Format(time.RFC3339)
	tasksMu.Unlock()

	taskLog.Info("Full migration completed", map[string]interface{}{
		"migrated_keys": mc,
		"failed_keys":   fc,
		"skipped_keys":  sc,
		"filtered_keys": ftc,
		"duration":      time.Since(startTime).String(),
		"avg_speed":     int64(float64(mc) / time.Since(startTime).Seconds()),
	})
}

// RateLimiter 简单的限速器
type RateLimiter struct {
	ticker   *time.Ticker
	tokens   chan struct{}
	stopChan chan struct{}
}

// NewRateLimiter 创建限速器
func NewRateLimiter(qps int) *RateLimiter {
	if qps <= 0 {
		return nil
	}
	interval := time.Second / time.Duration(qps)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}

	rl := &RateLimiter{
		ticker:   time.NewTicker(interval),
		tokens:   make(chan struct{}, qps),
		stopChan: make(chan struct{}),
	}

	// 预填充tokens
	for i := 0; i < qps/10+1; i++ {
		select {
		case rl.tokens <- struct{}{}:
		default:
		}
	}

	// 持续填充tokens
	go func() {
		for {
			select {
			case <-rl.stopChan:
				return
			case <-rl.ticker.C:
				select {
				case rl.tokens <- struct{}{}:
				default:
				}
			}
		}
	}()

	return rl
}

// Wait 等待获取令牌
func (rl *RateLimiter) Wait() {
	if rl == nil {
		return
	}
	<-rl.tokens
}

// Stop 停止限速器
func (rl *RateLimiter) Stop() {
	if rl == nil {
		return
	}
	close(rl.stopChan)
	rl.ticker.Stop()
}

// matchKeyFilter 检查Key是否匹配过滤规则
func matchKeyFilter(key string, options *TaskOptions) bool {
	if options == nil || options.KeyFilter == nil {
		return true
	}

	filter := options.KeyFilter
	switch filter.Mode {
	case "prefix":
		// 检查排除前缀
		for _, prefix := range filter.ExcludePrefixes {
			if strings.HasPrefix(key, prefix) {
				return false
			}
		}
		// 如果设置了包含前缀，只迁移匹配的
		if len(filter.Prefixes) > 0 {
			for _, prefix := range filter.Prefixes {
				if strings.HasPrefix(key, prefix) {
					return true
				}
			}
			return false
		}
		return true
	case "pattern":
		// 正则匹配（简化实现，使用 strings.Contains）
		if len(filter.Patterns) > 0 {
			for _, pattern := range filter.Patterns {
				if strings.Contains(key, pattern) {
					return true
				}
			}
			return false
		}
		return true
	default:
		return true
	}
}

// migrateKeyWithPolicy 根据冲突策略迁移Key
func migrateKeyWithPolicy(ctx context.Context, sourceClient, targetClient redis.UniversalClient, key string, policy string) (bool, int64, string) {
	// 获取Key的TTL
	ttl, err := sourceClient.TTL(ctx, key).Result()
	if err != nil {
		return false, 0, "get TTL failed: " + err.Error()
	}

	// 使用DUMP+RESTORE迁移
	dump, err := sourceClient.Dump(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, 0, "skipped" // Key不存在，跳过
		}
		return false, 0, "dump failed: " + err.Error()
	}

	bytes := int64(len(dump))

	// 检查目标是否存在
	exists, err := targetClient.Exists(ctx, key).Result()
	if err != nil {
		return false, 0, "check exists failed: " + err.Error()
	}

	if exists > 0 {
		switch policy {
		case "skip", "skip_full_only":
			return false, 0, "skipped"
		case "replace":
			// 先删除目标Key
			if err := targetClient.Del(ctx, key).Err(); err != nil {
				return false, 0, "delete failed: " + err.Error()
			}
		case "error":
			return false, 0, "key conflict"
		default:
			return false, 0, "skipped"
		}
	}

	// RESTORE到目标
	if ttl < 0 {
		ttl = 0 // 无过期时间
	}
	err = targetClient.Restore(ctx, key, ttl, dump).Err()
	if err != nil {
		return false, 0, "restore failed: " + err.Error()
	}

	return true, bytes, ""
}

// migrateKey 迁移单个Key（保留旧函数供兼容）
func migrateKey(ctx context.Context, sourceClient, targetClient redis.UniversalClient, key string) (bool, int64, string) {
	return migrateKeyWithPolicy(ctx, sourceClient, targetClient, key, "skip_full_only")
}

// scanAllKeys 扫描所有key（支持集群模式）
func scanAllKeys(ctx context.Context, client redis.UniversalClient, isCluster bool) (map[string]bool, error) {
	knownKeys := make(map[string]bool)

	if !isCluster {
		// 单机模式：直接SCAN
		var cursor uint64
		for {
			keys, newCursor, err := client.Scan(ctx, cursor, "*", 1000).Result()
			if err != nil {
				return knownKeys, err
			}
			for _, key := range keys {
				knownKeys[key] = true
			}
			cursor = newCursor
			if cursor == 0 {
				break
			}
		}
		return knownKeys, nil
	}

	// 集群模式：遍历所有master节点扫描
	clusterClient := client.(*redis.ClusterClient)
	var mu sync.Mutex

	err := clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		var cursor uint64
		for {
			keys, newCursor, err := node.Scan(ctx, cursor, "*", 1000).Result()
			if err != nil {
				return err
			}
			mu.Lock()
			for _, key := range keys {
				knownKeys[key] = true
			}
			mu.Unlock()
			cursor = newCursor
			if cursor == 0 {
				break
			}
		}
		return nil
	})

	return knownKeys, err
}

// doIncrementalSync 增量同步
func doIncrementalSync(ctx context.Context, task *Task, sourceClient, targetClient redis.UniversalClient, sourceIsCluster, targetIsCluster bool, taskLog *logger.TaskLogger) {
	taskLog.Info("Incremental sync mode - monitoring for changes")

	// 记录已知的key集合（用于检测新key）
	// 使用新的 scanAllKeys 确保集群模式下扫描所有节点
	knownKeys, err := scanAllKeys(ctx, sourceClient, sourceIsCluster)
	if err != nil {
		taskLog.Warn("Failed to take initial key snapshot", map[string]interface{}{"error": err.Error()})
	}
	taskLog.Info("Initial key snapshot taken", map[string]interface{}{"known_keys": len(knownKeys)})

	// 定期扫描检测新key
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	syncedInIncr := int64(0)
	skippedInIncr := int64(0)

	for {
		select {
		case <-ticker.C:
			tasksMu.RLock()
			status := task.Status
			tasksMu.RUnlock()
			if status != "running" {
				taskLog.Info("Incremental sync stopped", map[string]interface{}{
					"synced_in_incremental":  syncedInIncr,
					"skipped_in_incremental": skippedInIncr,
				})
				return
			}

			// 扫描所有key，查找新key（集群模式下扫描所有节点）
			currentKeys, scanErr := scanAllKeys(ctx, sourceClient, sourceIsCluster)
			if scanErr != nil {
				taskLog.Warn("Incremental scan failed", map[string]interface{}{"error": scanErr.Error()})
				continue
			}

			newKeysFound := 0
			newKeysSkipped := 0
			for key := range currentKeys {
				if !knownKeys[key] {
					// 检查是否匹配过滤规则
					if !matchKeyFilter(key, task.Options) {
						knownKeys[key] = true
						tasksMu.Lock()
						task.KeysFiltered++
						tasksMu.Unlock()
						continue
					}

					// 发现新key，同步到目标
					migrated, bytes, reason := migrateKeyWithPolicy(ctx, sourceClient, targetClient, key, "replace")
					knownKeys[key] = true

					if migrated {
						syncedInIncr++
						newKeysFound++
						tasksMu.Lock()
						task.KeysMigrated++
						task.BytesMigrated += bytes
						task.UpdatedAt = time.Now().Format(time.RFC3339)
						tasksMu.Unlock()

						taskLog.Debug("Incremental key synced", map[string]interface{}{
							"key":   key,
							"bytes": bytes,
						})
					} else if reason == "skipped" {
						// 增量阶段的冲突跳过也需要统计
						skippedInIncr++
						newKeysSkipped++
						tasksMu.Lock()
						task.KeysSkipped++
						task.UpdatedAt = time.Now().Format(time.RFC3339)
						tasksMu.Unlock()
						addErrorKey(task.ID, key, "string", "skipped", "Key already exists in target (incremental)")
					} else if reason != "" {
						tasksMu.Lock()
						task.KeysFailed++
						task.UpdatedAt = time.Now().Format(time.RFC3339)
						tasksMu.Unlock()
						taskLog.Warn("Failed to sync incremental key", map[string]interface{}{
							"key":    key,
							"reason": reason,
						})
						addErrorKey(task.ID, key, "string", "failed", reason+" (incremental)")
					}
				}
			}

			// 更新总key数（只在源端key数增加时更新）
			newTotal, _ := getDBSize(ctx, sourceClient, sourceIsCluster)
			tasksMu.Lock()
			if newTotal > task.KeysTotal {
				task.KeysTotal = newTotal
			}
			tasksMu.Unlock()

			if newKeysFound > 0 || newKeysSkipped > 0 {
				taskLog.Info("Incremental sync progress", map[string]interface{}{
					"new_keys_synced":        newKeysFound,
					"new_keys_skipped":       newKeysSkipped,
					"total_synced_in_incr":   syncedInIncr,
					"total_skipped_in_incr":  skippedInIncr,
				})
			}
		}
	}
}

// addErrorKey 添加错误Key记录
func addErrorKey(taskID, key, keyType, reason, detail string) {
	errorKeyMu.Lock()
	defer errorKeyMu.Unlock()

	if errorKeys[taskID] == nil {
		errorKeys[taskID] = []ErrorKey{}
	}

	// 限制最大记录数
	if len(errorKeys[taskID]) < 10000 {
		errorKeys[taskID] = append(errorKeys[taskID], ErrorKey{
			Key:       key,
			Type:      keyType,
			Reason:    reason,
			Detail:    detail,
			Timestamp: time.Now().Format(time.RFC3339),
		})
	}
}

// errorKeysHandler 获取错误Key列表
func errorKeysHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	errorKeyMu.RLock()
	keys := errorKeys[id]
	errorKeyMu.RUnlock()

	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Task not found"})
		return
	}

	// 统计
	stats := map[string]int64{
		"total":      int64(len(keys)),
		"failed":     task.KeysFailed,
		"skipped":    task.KeysSkipped,
		"large_keys": 0,
	}

	// 只返回前100条
	items := keys
	if len(items) > 100 {
		items = items[:100]
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"stats": stats,
			"items": items,
		},
	})
}

// downloadErrorKeysHandler 下载错误Key CSV
func downloadErrorKeysHandler(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	errorKeyMu.RLock()
	keys := errorKeys[id]
	errorKeyMu.RUnlock()

	// 生成CSV
	var sb strings.Builder
	sb.WriteString("Key,Type,Reason,Detail,Timestamp\n")
	for _, k := range keys {
		sb.WriteString(fmt.Sprintf("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n",
			strings.ReplaceAll(k.Key, "\"", "\"\""),
			k.Type,
			k.Reason,
			strings.ReplaceAll(k.Detail, "\"", "\"\""),
			k.Timestamp,
		))
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"error-keys-%s.csv\"", id[:8]))
	w.Write([]byte(sb.String()))

	log.Info("Error keys downloaded", map[string]interface{}{"task_id": id, "count": len(keys)})
}

func systemHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	running := 0
	tasksMu.RLock()
	for _, t := range tasks {
		if t.Status == "running" {
			running++
		}
	}
	tasksMu.RUnlock()

	log.Debug("System status queried")

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"status":        "running",
			"worker_count":  4,
			"running_tasks": running,
			"total_tasks":   len(tasks),
			"uptime":        time.Since(startTime).String(),
			"memory_mb":     getMemoryUsage(),
		},
	})
}

// ==================== 测试连接 API ====================

func testConnectionHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Addrs    []string `json:"addrs"`
		Password string   `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Error("Failed to decode request", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Invalid request: " + err.Error()})
		return
	}

	// 过滤空地址
	var addrs []string
	for _, addr := range req.Addrs {
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "至少需要一个集群地址"})
		return
	}

	log.Info("Testing connection", map[string]interface{}{
		"addrs": addrs,
	})

	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 先尝试集群模式连接
	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		Password: req.Password,
	})
	defer clusterClient.Close()

	if err := clusterClient.Ping(ctx).Err(); err == nil {
		// 集群模式连接成功
		info := getClusterInfo(ctx, clusterClient)
		info["mode"] = "cluster"

		log.Info("Cluster connection successful", map[string]interface{}{
			"mode":       "cluster",
			"node_count": info["node_count"],
			"latency_ms": time.Since(startTime).Milliseconds(),
		})

		jsonResponse(w, map[string]interface{}{
			"code":    0,
			"message": "success",
			"data": map[string]interface{}{
				"success":      true,
				"message":      "集群连接成功",
				"cluster_info": info,
				"latency_ms":   time.Since(startTime).Milliseconds(),
			},
		})
		return
	}

	// 尝试单机模式连接
	standaloneClient := redis.NewClient(&redis.Options{
		Addr:     addrs[0],
		Password: req.Password,
	})
	defer standaloneClient.Close()

	if err := standaloneClient.Ping(ctx).Err(); err != nil {
		log.Error("Connection failed", map[string]interface{}{
			"error":      err.Error(),
			"latency_ms": time.Since(startTime).Milliseconds(),
		})

		jsonResponse(w, map[string]interface{}{
			"code":    0,
			"message": "success",
			"data": map[string]interface{}{
				"success":    false,
				"message":    "连接失败: " + err.Error(),
				"latency_ms": time.Since(startTime).Milliseconds(),
			},
		})
		return
	}

	// 单机模式连接成功
	info := getStandaloneInfo(ctx, standaloneClient, addrs[0])
	info["mode"] = "standalone"

	log.Info("Standalone connection successful", map[string]interface{}{
		"mode":       "standalone",
		"latency_ms": time.Since(startTime).Milliseconds(),
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"success":      true,
			"message":      "单机模式连接成功",
			"cluster_info": info,
			"latency_ms":   time.Since(startTime).Milliseconds(),
		},
	})
}

func getClusterInfo(ctx context.Context, client *redis.ClusterClient) map[string]interface{} {
	info := map[string]interface{}{
		"nodes":        []map[string]interface{}{},
		"node_count":   0,
		"total_keys":   int64(0),
		"total_memory": int64(0),
		"version":      "unknown",
	}

	var nodes []map[string]interface{}
	var totalKeys int64
	var totalMemory int64
	var version string
	var mu sync.Mutex

	// 获取集群节点信息
	client.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		nodeInfo := map[string]interface{}{
			"role": "master",
		}

		// 获取节点地址
		opts := node.Options()
		nodeInfo["addr"] = opts.Addr

		// 获取DBSize
		if dbsize, err := node.DBSize(ctx).Result(); err == nil {
			nodeInfo["keys"] = dbsize
			mu.Lock()
			totalKeys += dbsize
			mu.Unlock()
		}

		// 获取内存使用
		if memInfo, err := node.Info(ctx, "memory").Result(); err == nil {
			mem := parseMemoryFromInfo(memInfo)
			nodeInfo["memory"] = mem
			mu.Lock()
			totalMemory += mem
			mu.Unlock()
		}

		// 获取Redis版本（只需获取一次）
		mu.Lock()
		if version == "" {
			if serverInfo, err := node.Info(ctx, "server").Result(); err == nil {
				version = parseVersionFromInfo(serverInfo)
			}
		}
		mu.Unlock()

		mu.Lock()
		nodes = append(nodes, nodeInfo)
		mu.Unlock()
		return nil
	})

	info["nodes"] = nodes
	info["node_count"] = len(nodes)
	info["total_keys"] = totalKeys
	info["total_memory"] = totalMemory
	info["version"] = version

	return info
}

func getStandaloneInfo(ctx context.Context, client *redis.Client, addr string) map[string]interface{} {
	info := map[string]interface{}{
		"node_count":   1,
		"total_keys":   int64(0),
		"total_memory": int64(0),
		"version":      "unknown",
	}

	nodeInfo := map[string]interface{}{
		"addr": addr,
		"role": "master",
	}

	// 获取DBSize
	if dbsize, err := client.DBSize(ctx).Result(); err == nil {
		nodeInfo["keys"] = dbsize
		info["total_keys"] = dbsize
	}

	// 获取内存和版本信息
	if serverInfo, err := client.Info(ctx, "server").Result(); err == nil {
		info["version"] = parseVersionFromInfo(serverInfo)
	}
	if memInfo, err := client.Info(ctx, "memory").Result(); err == nil {
		mem := parseMemoryFromInfo(memInfo)
		nodeInfo["memory"] = mem
		info["total_memory"] = mem
	}

	info["nodes"] = []map[string]interface{}{nodeInfo}
	return info
}

func parseVersionFromInfo(info string) string {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "redis_version:") {
			return strings.TrimPrefix(line, "redis_version:")
		}
	}
	return "unknown"
}

func parseMemoryFromInfo(info string) int64 {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "used_memory:") {
			memStr := strings.TrimPrefix(line, "used_memory:")
			mem, _ := strconv.ParseInt(memStr, 10, 64)
			return mem
		}
	}
	return 0
}

// analyzeClusterHandler 分析集群详细信息
func analyzeClusterHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Addrs    []string `json:"addrs"`
		Password string   `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Invalid request"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	info, err := analyzeCluster(ctx, req.Addrs, req.Password)
	if err != nil {
		log.Error("Failed to analyze cluster", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 500, "message": err.Error()})
		return
	}

	log.Info("Cluster analyzed", map[string]interface{}{
		"total_keys": info.TotalKeys,
		"is_cluster": info.IsCluster,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    info,
	})
}

// analyzeCluster 分析集群详细信息
func analyzeCluster(ctx context.Context, addrs []string, password string) (*ClusterInfo, error) {
	info := &ClusterInfo{
		Addrs: addrs,
	}

	// 尝试集群模式
	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		Password: password,
	})

	if err := clusterClient.Ping(ctx).Err(); err == nil {
		info.IsCluster = true
		defer clusterClient.Close()

		var mu sync.Mutex
		var totalKeys int64
		var totalMemory int64
		var maxMemory int64
		var masterCount int
		var connectedClients int
		var instantaneousOPS int64
		var maxClients int

		clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			masterCount++

			// DBSize
			if dbsize, err := node.DBSize(ctx).Result(); err == nil {
				mu.Lock()
				totalKeys += dbsize
				mu.Unlock()
			}

			// Memory info
			if memInfo, err := node.Info(ctx, "memory").Result(); err == nil {
				mem := parseMemoryFromInfo(memInfo)
				maxMem := parseMaxMemoryFromInfo(memInfo)
				mu.Lock()
				totalMemory += mem
				if maxMem > maxMemory {
					maxMemory = maxMem
				}
				mu.Unlock()
			}

			// Stats info
			if statsInfo, err := node.Info(ctx, "stats").Result(); err == nil {
				ops := parseOPSFromInfo(statsInfo)
				mu.Lock()
				instantaneousOPS += ops
				mu.Unlock()
			}

			// Clients info
			if clientInfo, err := node.Info(ctx, "clients").Result(); err == nil {
				clients := parseConnectedClientsFromInfo(clientInfo)
				mu.Lock()
				connectedClients += clients
				mu.Unlock()
			}

			// Server info (version, maxclients)
			if serverInfo, err := node.Info(ctx, "server").Result(); err == nil {
				mu.Lock()
				if info.Version == "" {
					info.Version = parseVersionFromInfo(serverInfo)
				}
				mu.Unlock()
			}

			// Config maxclients
			if result, err := node.ConfigGet(ctx, "maxclients").Result(); err == nil && len(result) >= 2 {
				if mcStr, ok := result[1].(string); ok {
					mc, _ := strconv.Atoi(mcStr)
					mu.Lock()
					if mc > maxClients {
						maxClients = mc
					}
					mu.Unlock()
				}
			}

			return nil
		})

		info.MasterCount = masterCount
		info.TotalKeys = totalKeys
		info.UsedMemory = totalMemory
		info.UsedMemoryHuman = formatBytes(totalMemory)
		info.MaxMemory = maxMemory
		info.MaxClients = maxClients
		info.ConnectedClients = connectedClients
		info.InstantaneousOPS = instantaneousOPS

		// 估算平均key大小
		if totalKeys > 0 {
			info.AvgKeySize = totalMemory / totalKeys
		}

		return info, nil
	}
	clusterClient.Close()

	// 尝试单机模式
	standaloneClient := redis.NewClient(&redis.Options{
		Addr:     addrs[0],
		Password: password,
	})
	defer standaloneClient.Close()

	if err := standaloneClient.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	info.IsCluster = false
	info.MasterCount = 1

	if dbsize, err := standaloneClient.DBSize(ctx).Result(); err == nil {
		info.TotalKeys = dbsize
	}

	if memInfo, err := standaloneClient.Info(ctx, "memory").Result(); err == nil {
		info.UsedMemory = parseMemoryFromInfo(memInfo)
		info.UsedMemoryHuman = formatBytes(info.UsedMemory)
		info.MaxMemory = parseMaxMemoryFromInfo(memInfo)
	}

	if statsInfo, err := standaloneClient.Info(ctx, "stats").Result(); err == nil {
		info.InstantaneousOPS = parseOPSFromInfo(statsInfo)
	}

	if clientInfo, err := standaloneClient.Info(ctx, "clients").Result(); err == nil {
		info.ConnectedClients = parseConnectedClientsFromInfo(clientInfo)
	}

	if serverInfo, err := standaloneClient.Info(ctx, "server").Result(); err == nil {
		info.Version = parseVersionFromInfo(serverInfo)
	}

	if result, err := standaloneClient.ConfigGet(ctx, "maxclients").Result(); err == nil && len(result) >= 2 {
		if mcStr, ok := result[1].(string); ok {
			info.MaxClients, _ = strconv.Atoi(mcStr)
		}
	}

	if info.TotalKeys > 0 {
		info.AvgKeySize = info.UsedMemory / info.TotalKeys
	}

	return info, nil
}

// recommendConfigHandler 推荐配置
func recommendConfigHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SourceCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"source_cluster"`
		TargetCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"target_cluster"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "Invalid request"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 分析源端集群
	sourceInfo, err := analyzeCluster(ctx, req.SourceCluster.Addrs, req.SourceCluster.Password)
	if err != nil {
		jsonResponse(w, map[string]interface{}{"code": 500, "message": "分析源端集群失败: " + err.Error()})
		return
	}

	// 分析目标端集群
	targetInfo, err := analyzeCluster(ctx, req.TargetCluster.Addrs, req.TargetCluster.Password)
	if err != nil {
		jsonResponse(w, map[string]interface{}{"code": 500, "message": "分析目标端集群失败: " + err.Error()})
		return
	}

	// 生成推荐配置
	config := generateRecommendedConfig(sourceInfo, targetInfo)

	log.Info("Config recommended", map[string]interface{}{
		"worker_count":    config.WorkerCount,
		"estimated_speed": config.EstimatedSpeed,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"source_info": sourceInfo,
			"target_info": targetInfo,
			"recommended": config,
		},
	})
}

// generateRecommendedConfig 生成推荐配置
func generateRecommendedConfig(source, target *ClusterInfo) *RecommendedConfig {
	config := &RecommendedConfig{
		ScanBatchSize:     1000,
		LargeKeyThreshold: 10 * 1024 * 1024, // 10MB
	}

	var reasons []string

	// 1. 计算Worker数量
	// 基于多个因素：源端负载、连接数限制、CPU核心数
	cpuCores := runtime.NumCPU()
	maxWorkersByCPU := cpuCores * 4 // 每核4个worker

	// 基于连接数限制
	sourceMaxConns := source.MaxClients - source.ConnectedClients
	targetMaxConns := target.MaxClients - target.ConnectedClients
	if sourceMaxConns < 100 {
		sourceMaxConns = 100
	}
	if targetMaxConns < 100 {
		targetMaxConns = 100
	}
	maxWorkersByConn := min(sourceMaxConns/3, targetMaxConns/3) // 每worker需要约3个连接

	// 基于源端当前负载
	var maxWorkersByLoad int
	if source.InstantaneousOPS < 1000 {
		// 低负载，可以激进配置
		maxWorkersByLoad = 100
		reasons = append(reasons, "源端负载较低(OPS<1000)，可使用较多Worker")
	} else if source.InstantaneousOPS < 10000 {
		// 中等负载
		maxWorkersByLoad = 50
		reasons = append(reasons, "源端负载中等，Worker数量适中")
	} else {
		// 高负载，保守配置
		maxWorkersByLoad = 20
		reasons = append(reasons, "源端负载较高，Worker数量保守设置")
	}

	// 取最小值
	config.WorkerCount = min(maxWorkersByCPU, min(maxWorkersByConn, maxWorkersByLoad))
	if config.WorkerCount < 4 {
		config.WorkerCount = 4
	}
	if config.WorkerCount > 100 {
		config.WorkerCount = 100
	}

	// 2. 计算QPS限制
	// 源端：预留70%给业务，迁移使用30%
	if source.InstantaneousOPS < 100 {
		// 几乎无业务，不限制
		config.SourceQPS = 0
		reasons = append(reasons, "源端几乎无业务负载，不限制QPS")
	} else {
		// 估算最大容量（假设当前是业务负载的50%）
		estimatedMaxOPS := source.InstantaneousOPS * 2
		if estimatedMaxOPS < 50000 {
			estimatedMaxOPS = 50000 // 最低假设5万
		}
		config.SourceQPS = int(estimatedMaxOPS * 30 / 100) // 使用30%
		reasons = append(reasons, fmt.Sprintf("源端QPS限制为预估容量的30%%(%d)", config.SourceQPS))
	}

	// 目标端：通常可以更激进
	if target.InstantaneousOPS < 100 {
		config.TargetQPS = 0
		reasons = append(reasons, "目标端几乎无负载，不限制QPS")
	} else {
		estimatedMaxOPS := target.InstantaneousOPS * 2
		if estimatedMaxOPS < 50000 {
			estimatedMaxOPS = 50000
		}
		config.TargetQPS = int(estimatedMaxOPS * 50 / 100) // 使用50%
	}

	// 3. 计算连接数
	// 每个Worker需要约2-3个连接
	config.SourceConnections = config.WorkerCount * 3
	config.TargetConnections = config.WorkerCount * 3

	// 确保不超过可用连接数的50%
	if config.SourceConnections > sourceMaxConns/2 {
		config.SourceConnections = sourceMaxConns / 2
	}
	if config.TargetConnections > targetMaxConns/2 {
		config.TargetConnections = targetMaxConns / 2
	}

	// 最小连接数
	if config.SourceConnections < 10 {
		config.SourceConnections = 10
	}
	if config.TargetConnections < 10 {
		config.TargetConnections = 10
	}

	// 4. 估算迁移速度和时间
	// 基于key大小估算单worker吞吐
	var singleWorkerSpeed int64
	if source.AvgKeySize < 1024 { // < 1KB
		singleWorkerSpeed = 500
	} else if source.AvgKeySize < 10*1024 { // < 10KB
		singleWorkerSpeed = 200
	} else if source.AvgKeySize < 100*1024 { // < 100KB
		singleWorkerSpeed = 50
	} else { // >= 100KB
		singleWorkerSpeed = 10
	}

	config.EstimatedSpeed = singleWorkerSpeed * int64(config.WorkerCount)

	// QPS限制可能成为瓶颈
	if config.SourceQPS > 0 && int64(config.SourceQPS) < config.EstimatedSpeed*4 {
		// 每个key需要约4次操作，QPS限制可能影响速度
		config.EstimatedSpeed = int64(config.SourceQPS) / 4
	}

	// 估算时间
	if config.EstimatedSpeed > 0 && source.TotalKeys > 0 {
		seconds := source.TotalKeys / config.EstimatedSpeed
		config.EstimatedTime = formatDuration(seconds)
	} else {
		config.EstimatedTime = "无法估算"
	}

	// 5. 大Key阈值
	if source.AvgKeySize > 1024*1024 { // 平均大于1MB
		config.LargeKeyThreshold = 5 * 1024 * 1024 // 5MB
		reasons = append(reasons, "检测到较大的平均Key大小，调低大Key阈值")
	}

	config.Reason = strings.Join(reasons, "；")

	return config
}

// 辅助函数
func parseMaxMemoryFromInfo(info string) int64 {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "maxmemory:") {
			memStr := strings.TrimPrefix(line, "maxmemory:")
			mem, _ := strconv.ParseInt(memStr, 10, 64)
			return mem
		}
	}
	return 0
}

func parseOPSFromInfo(info string) int64 {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "instantaneous_ops_per_sec:") {
			opsStr := strings.TrimPrefix(line, "instantaneous_ops_per_sec:")
			ops, _ := strconv.ParseInt(opsStr, 10, 64)
			return ops
		}
	}
	return 0
}

func parseConnectedClientsFromInfo(info string) int {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "connected_clients:") {
			clientsStr := strings.TrimPrefix(line, "connected_clients:")
			clients, _ := strconv.Atoi(clientsStr)
			return clients
		}
	}
	return 0
}

func formatBytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	} else if bytes < 1024*1024 {
		return fmt.Sprintf("%.2f KB", float64(bytes)/1024)
	} else if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
	}
	return fmt.Sprintf("%.2f GB", float64(bytes)/(1024*1024*1024))
}

func formatDuration(seconds int64) string {
	if seconds < 60 {
		return fmt.Sprintf("%d秒", seconds)
	} else if seconds < 3600 {
		return fmt.Sprintf("%d分%d秒", seconds/60, seconds%60)
	} else if seconds < 86400 {
		return fmt.Sprintf("%d小时%d分", seconds/3600, (seconds%3600)/60)
	}
	return fmt.Sprintf("%d天%d小时", seconds/86400, (seconds%86400)/3600)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func templatesHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	switch r.Method {
	case "GET":
		listTemplates(w, r, log)
	case "POST":
		createTemplate(w, r, log)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// listTemplates 获取模板列表
func listTemplates(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	templateMu.RLock()
	defer templateMu.RUnlock()

	items := make([]*TaskTemplate, 0, len(templates))
	for _, t := range templates {
		items = append(items, t)
	}

	// 按创建时间排序
	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt > items[j].CreatedAt
	})

	log.Debug("Templates listed", map[string]interface{}{"count": len(items)})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"items": items,
			"total": len(items),
		},
	})
}

// createTemplate 创建模板
func createTemplate(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	var req struct {
		Name          string `json:"name"`
		Description   string `json:"description"`
		SourceCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"source_cluster"`
		TargetCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"target_cluster"`
		MigrationMode string       `json:"migration_mode"`
		Options       *TaskOptions `json:"options"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Error("Failed to decode request", map[string]interface{}{"error": err.Error()})
		jsonResponse(w, map[string]interface{}{"code": 400, "message": err.Error()})
		return
	}

	if req.Name == "" {
		jsonResponse(w, map[string]interface{}{"code": 400, "message": "模板名称不能为空"})
		return
	}

	template := &TaskTemplate{
		ID:             uuid.New().String(),
		Name:           req.Name,
		Description:    req.Description,
		SourceCluster:  strings.Join(req.SourceCluster.Addrs, ","),
		TargetCluster:  strings.Join(req.TargetCluster.Addrs, ","),
		SourcePassword: req.SourceCluster.Password,
		TargetPassword: req.TargetCluster.Password,
		MigrationMode:  req.MigrationMode,
		Options:        req.Options,
		CreatedAt:      time.Now().Format(time.RFC3339),
		UpdatedAt:      time.Now().Format(time.RFC3339),
	}

	templateMu.Lock()
	templates[template.ID] = template
	templateMu.Unlock()

	log.Info("Template created", map[string]interface{}{
		"template_id":   template.ID,
		"template_name": template.Name,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    map[string]string{"template_id": template.ID},
	})
}

// templateHandler 处理单个模板请求
func templateHandler(w http.ResponseWriter, r *http.Request, log *logger.RequestLogger) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/templates/")
	parts := strings.Split(path, "/")

	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	id := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	switch r.Method {
	case "GET":
		getTemplate(w, r, id, log)
	case "PUT":
		updateTemplate(w, r, id, log)
	case "DELETE":
		deleteTemplate(w, r, id, log)
	case "POST":
		if action == "create-task" {
			createTaskFromTemplate(w, r, id, log)
		} else {
			http.NotFound(w, r)
		}
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// getTemplate 获取模板详情
func getTemplate(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	templateMu.RLock()
	template, ok := templates[id]
	templateMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Template not found"})
		return
	}

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    template,
	})
}

// updateTemplate 更新模板
func updateTemplate(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	templateMu.Lock()
	template, ok := templates[id]
	if !ok {
		templateMu.Unlock()
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Template not found"})
		return
	}

	var req struct {
		Name          string `json:"name"`
		Description   string `json:"description"`
		SourceCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"source_cluster"`
		TargetCluster struct {
			Addrs    []string `json:"addrs"`
			Password string   `json:"password"`
		} `json:"target_cluster"`
		MigrationMode string       `json:"migration_mode"`
		Options       *TaskOptions `json:"options"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		templateMu.Unlock()
		jsonResponse(w, map[string]interface{}{"code": 400, "message": err.Error()})
		return
	}

	if req.Name != "" {
		template.Name = req.Name
	}
	if req.Description != "" {
		template.Description = req.Description
	}
	if len(req.SourceCluster.Addrs) > 0 {
		template.SourceCluster = strings.Join(req.SourceCluster.Addrs, ",")
		template.SourcePassword = req.SourceCluster.Password
	}
	if len(req.TargetCluster.Addrs) > 0 {
		template.TargetCluster = strings.Join(req.TargetCluster.Addrs, ",")
		template.TargetPassword = req.TargetCluster.Password
	}
	if req.MigrationMode != "" {
		template.MigrationMode = req.MigrationMode
	}
	if req.Options != nil {
		template.Options = req.Options
	}
	template.UpdatedAt = time.Now().Format(time.RFC3339)
	templateMu.Unlock()

	log.Info("Template updated", map[string]interface{}{"template_id": id})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
	})
}

// deleteTemplate 删除模板
func deleteTemplate(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	templateMu.Lock()
	_, ok := templates[id]
	if !ok {
		templateMu.Unlock()
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Template not found"})
		return
	}
	delete(templates, id)
	templateMu.Unlock()

	log.Info("Template deleted", map[string]interface{}{"template_id": id})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
	})
}

// createTaskFromTemplate 从模板创建任务
func createTaskFromTemplate(w http.ResponseWriter, r *http.Request, id string, log *logger.RequestLogger) {
	templateMu.RLock()
	template, ok := templates[id]
	templateMu.RUnlock()

	if !ok {
		jsonResponse(w, map[string]interface{}{"code": 404, "message": "Template not found"})
		return
	}

	// 可选：允许覆盖部分参数
	var req struct {
		Name string `json:"name"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	taskName := req.Name
	if taskName == "" {
		taskName = template.Name + "-" + time.Now().Format("0102-1504")
	}

	mode := template.MigrationMode
	if mode == "" {
		mode = "full_and_incremental"
	}

	options := template.Options
	if options == nil {
		options = &TaskOptions{
			WorkerCount:       4,
			ScanBatchSize:     1000,
			ConflictPolicy:    "skip_full_only",
			LargeKeyThreshold: 10485760,
			EnableCompression: true,
			KeyFilter:         &KeyFilter{Mode: "all"},
		}
	} else if options.KeyFilter == nil || options.KeyFilter.Mode == "" {
		if options.KeyFilter == nil {
			options.KeyFilter = &KeyFilter{Mode: "all"}
		} else {
			options.KeyFilter.Mode = "all"
		}
	}

	task := &Task{
		ID:             uuid.New().String(),
		Name:           taskName,
		Status:         "pending",
		Progress:       0,
		SourceCluster:  template.SourceCluster,
		TargetCluster:  template.TargetCluster,
		SourcePassword: template.SourcePassword,
		TargetPassword: template.TargetPassword,
		MigrationMode:  mode,
		CreatedAt:      time.Now().Format(time.RFC3339),
		UpdatedAt:      time.Now().Format(time.RFC3339),
		Phase:          "full",
		Options:        options,
	}

	tasksMu.Lock()
	tasks[task.ID] = task
	tasksMu.Unlock()

	log.Info("Task created from template", map[string]interface{}{
		"task_id":     task.ID,
		"template_id": id,
		"task_name":   taskName,
	})

	logger.WithTask(task.ID).Info("Task created from template", map[string]interface{}{
		"name":        task.Name,
		"template_id": id,
		"source":      task.SourceCluster,
		"target":      task.TargetCluster,
	})

	jsonResponse(w, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    map[string]string{"task_id": task.ID},
	})
}
