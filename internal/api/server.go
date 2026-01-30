package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"tendis-migrate/internal/engine"
	"tendis-migrate/internal/model"
)

// Server API服务器
type Server struct {
	master *engine.Master
	router *gin.Engine
	port   int
}

// NewServer 创建API服务器
func NewServer(master *engine.Master, port int) *Server {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	// CORS配置
	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	s := &Server{
		master: master,
		router: router,
		port:   port,
	}

	s.setupRoutes()
	return s
}

// setupRoutes 设置路由
func (s *Server) setupRoutes() {
	// API v1
	v1 := s.router.Group("/api/v1")
	{
		// 任务管理
		tasks := v1.Group("/tasks")
		{
			tasks.GET("", s.listTasks)
			tasks.POST("", s.createTask)
			tasks.GET("/:id", s.getTask)
			tasks.DELETE("/:id", s.deleteTask)
			tasks.POST("/:id/start", s.startTask)
			tasks.POST("/:id/pause", s.pauseTask)
			tasks.POST("/:id/resume", s.resumeTask)
			tasks.GET("/:id/progress", s.getProgress)
			tasks.GET("/:id/metrics", s.getMetrics)
			tasks.POST("/:id/verify", s.triggerVerify)
			tasks.GET("/:id/verify/results", s.getVerifyResults)
			tasks.GET("/:id/report", s.getReport)
		}

		// 系统信息
		v1.GET("/system/status", s.getSystemStatus)
		v1.GET("/system/workers", s.getWorkers)
		v1.GET("/health", s.healthCheck)

		// 测试连接
		v1.POST("/test-connection", s.testConnection)
	}

	// 静态文件（前端）
	s.router.Static("/assets", "./web/dist/assets")
	s.router.StaticFile("/", "./web/dist/index.html")
	s.router.NoRoute(func(c *gin.Context) {
		c.File("./web/dist/index.html")
	})
}

// Run 运行服务器
func (s *Server) Run() error {
	addr := ":" + strconv.Itoa(s.port)
	return s.router.Run(addr)
}

// Response 通用响应
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func success(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "success",
		Data:    data,
	})
}

func fail(c *gin.Context, code int, message string) {
	c.JSON(http.StatusOK, Response{
		Code:    code,
		Message: message,
	})
}

// ============ Task Handlers ============

// CreateTaskRequest 创建任务请求
type CreateTaskRequest struct {
	Name          string                  `json:"name" binding:"required"`
	SourceCluster *model.ClusterConfig    `json:"source_cluster" binding:"required"`
	TargetCluster *model.ClusterConfig    `json:"target_cluster" binding:"required"`
	Options       *model.MigrationOptions `json:"options"`
}

func (s *Server) createTask(c *gin.Context) {
	var req CreateTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, 400, "Invalid request: "+err.Error())
		return
	}

	// 设置默认选项
	if req.Options == nil {
		req.Options = model.DefaultMigrationOptions()
	}

	task, err := s.master.CreateTask(&engine.CreateTaskRequest{
		Name:          req.Name,
		SourceCluster: req.SourceCluster,
		TargetCluster: req.TargetCluster,
		Options:       req.Options,
	})

	if err != nil {
		fail(c, 500, "Create task failed: "+err.Error())
		return
	}

	success(c, map[string]string{"task_id": task.ID})
}

func (s *Server) listTasks(c *gin.Context) {
	status := c.Query("status")
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	size, _ := strconv.Atoi(c.DefaultQuery("size", "20"))

	if page < 1 {
		page = 1
	}
	if size < 1 || size > 100 {
		size = 20
	}

	tasks, total, err := s.master.ListTasks(status, page, size)
	if err != nil {
		fail(c, 500, "List tasks failed: "+err.Error())
		return
	}

	// 转换响应
	var items []map[string]interface{}
	for _, task := range tasks {
		progress, _ := s.master.GetTaskProgress(task.ID)
		
		item := map[string]interface{}{
			"id":         task.ID,
			"name":       task.Name,
			"status":     task.Status,
			"created_at": time.Unix(task.CreatedAt, 0),
			"updated_at": time.Unix(task.UpdatedAt, 0),
		}
		
		if task.StartedAt != nil {
			item["started_at"] = time.Unix(*task.StartedAt, 0)
		}
		if task.CompletedAt != nil {
			item["completed_at"] = time.Unix(*task.CompletedAt, 0)
		}
		if progress != nil {
			item["progress"] = progress
		}
		
		items = append(items, item)
	}

	success(c, map[string]interface{}{
		"items": items,
		"total": total,
		"page":  page,
		"size":  size,
	})
}

func (s *Server) getTask(c *gin.Context) {
	taskID := c.Param("id")

	task, err := s.master.GetTask(taskID)
	if err != nil {
		fail(c, 500, "Get task failed: "+err.Error())
		return
	}
	if task == nil {
		fail(c, 404, "Task not found")
		return
	}

	progress, _ := s.master.GetTaskProgress(taskID)
	stats, _ := s.master.GetTaskStats(taskID)

	result := map[string]interface{}{
		"id":             task.ID,
		"name":           task.Name,
		"source_cluster": task.SourceCluster,
		"target_cluster": task.TargetCluster,
		"status":         task.Status,
		"config":         task.Config,
		"created_at":     time.Unix(task.CreatedAt, 0),
		"updated_at":     time.Unix(task.UpdatedAt, 0),
	}

	if task.StartedAt != nil {
		result["started_at"] = time.Unix(*task.StartedAt, 0)
	}
	if task.CompletedAt != nil {
		result["completed_at"] = time.Unix(*task.CompletedAt, 0)
	}
	if progress != nil {
		result["progress"] = progress
	}
	if stats != nil {
		result["stats"] = stats
	}

	success(c, result)
}

func (s *Server) deleteTask(c *gin.Context) {
	taskID := c.Param("id")

	// 验证密码（危险操作）
	password := c.GetHeader("X-Confirm-Password")
	if password != "confirm-delete" {
		fail(c, 403, "Password required for delete operation")
		return
	}

	if err := s.master.DeleteTask(taskID); err != nil {
		fail(c, 500, "Delete task failed: "+err.Error())
		return
	}

	success(c, nil)
}

func (s *Server) startTask(c *gin.Context) {
	taskID := c.Param("id")

	if err := s.master.StartTask(taskID); err != nil {
		fail(c, 500, "Start task failed: "+err.Error())
		return
	}

	success(c, nil)
}

func (s *Server) pauseTask(c *gin.Context) {
	taskID := c.Param("id")

	if err := s.master.PauseTask(taskID); err != nil {
		fail(c, 500, "Pause task failed: "+err.Error())
		return
	}

	success(c, nil)
}

func (s *Server) resumeTask(c *gin.Context) {
	taskID := c.Param("id")

	if err := s.master.ResumeTask(taskID); err != nil {
		fail(c, 500, "Resume task failed: "+err.Error())
		return
	}

	success(c, nil)
}

func (s *Server) getProgress(c *gin.Context) {
	taskID := c.Param("id")

	progress, err := s.master.GetTaskProgress(taskID)
	if err != nil {
		fail(c, 500, "Get progress failed: "+err.Error())
		return
	}

	success(c, progress)
}

func (s *Server) getMetrics(c *gin.Context) {
	taskID := c.Param("id")

	// 获取最近1小时的指标
	endTime := time.Now().Unix()
	startTime := endTime - 3600

	metrics, err := s.master.Store().GetMetrics(taskID, startTime, endTime)
	if err != nil {
		fail(c, 500, "Get metrics failed: "+err.Error())
		return
	}

	success(c, metrics)
}

func (s *Server) triggerVerify(c *gin.Context) {
	taskID := c.Param("id")

	batchID, err := s.master.TriggerVerify(taskID)
	if err != nil {
		fail(c, 500, "Trigger verify failed: "+err.Error())
		return
	}

	success(c, map[string]string{"batch_id": batchID})
}

func (s *Server) getVerifyResults(c *gin.Context) {
	taskID := c.Param("id")

	results, err := s.master.GetVerifyResults(taskID)
	if err != nil {
		fail(c, 500, "Get verify results failed: "+err.Error())
		return
	}

	success(c, results)
}

func (s *Server) getReport(c *gin.Context) {
	taskID := c.Param("id")
	format := c.DefaultQuery("format", "json")

	task, err := s.master.GetTask(taskID)
	if err != nil || task == nil {
		fail(c, 404, "Task not found")
		return
	}

	stats, _ := s.master.GetTaskStats(taskID)
	verifyResults, _ := s.master.GetVerifyResults(taskID)

	report := map[string]interface{}{
		"task":           task,
		"stats":          stats,
		"verify_results": verifyResults,
		"generated_at":   time.Now(),
	}

	switch format {
	case "json":
		success(c, report)
	case "csv":
		// TODO: CSV格式导出
		success(c, report)
	case "html":
		// TODO: HTML报告
		success(c, report)
	default:
		fail(c, 400, "Invalid format")
	}
}

// ============ System Handlers ============

func (s *Server) getSystemStatus(c *gin.Context) {
	workers := s.master.IPCServer().GetConnectedWorkers()

	success(c, map[string]interface{}{
		"status":        "running",
		"worker_count":  len(workers),
		"workers":       workers,
		"uptime":        time.Now().Unix(),
	})
}

func (s *Server) getWorkers(c *gin.Context) {
	workers := s.master.IPCServer().GetConnectedWorkers()

	var items []map[string]interface{}
	for _, id := range workers {
		items = append(items, map[string]interface{}{
			"id":     id,
			"status": "connected",
		})
	}

	success(c, items)
}

func (s *Server) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	})
}

// ============ Test Connection Handler ============

// TestConnectionRequest 测试连接请求
type TestConnectionRequest struct {
	Addrs    []string `json:"addrs" binding:"required"`
	Password string   `json:"password"`
}

// TestConnectionResponse 测试连接响应
type TestConnectionResponse struct {
	Success     bool              `json:"success"`
	Message     string            `json:"message"`
	ClusterInfo *ClusterInfoData  `json:"cluster_info,omitempty"`
	Latency     int64             `json:"latency_ms"`
}

// ClusterInfoData 集群信息
type ClusterInfoData struct {
	Mode         string     `json:"mode"`          // cluster 或 standalone
	Version      string     `json:"version"`       // Redis版本
	NodeCount    int        `json:"node_count"`    // 节点数
	TotalKeys    int64      `json:"total_keys"`    // 总Key数
	TotalMemory  int64      `json:"total_memory"`  // 总内存(bytes)
	Nodes        []NodeInfo `json:"nodes"`         // 节点详情
}

// NodeInfo 节点信息
type NodeInfo struct {
	Addr   string `json:"addr"`
	Role   string `json:"role"`
	Keys   int64  `json:"keys"`
	Memory int64  `json:"memory"`
}

func (s *Server) testConnection(c *gin.Context) {
	var req TestConnectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, 400, "Invalid request: "+err.Error())
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
		fail(c, 400, "至少需要一个集群地址")
		return
	}

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
		info := s.getClusterInfo(ctx, clusterClient)
		info.Mode = "cluster"
		
		success(c, &TestConnectionResponse{
			Success:     true,
			Message:     "集群连接成功",
			ClusterInfo: info,
			Latency:     time.Since(startTime).Milliseconds(),
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
		success(c, &TestConnectionResponse{
			Success: false,
			Message: "连接失败: " + err.Error(),
			Latency: time.Since(startTime).Milliseconds(),
		})
		return
	}

	// 单机模式连接成功
	info := s.getStandaloneInfo(ctx, standaloneClient, addrs[0])
	info.Mode = "standalone"

	success(c, &TestConnectionResponse{
		Success:     true,
		Message:     "单机模式连接成功",
		ClusterInfo: info,
		Latency:     time.Since(startTime).Milliseconds(),
	})
}

func (s *Server) getClusterInfo(ctx context.Context, client *redis.ClusterClient) *ClusterInfoData {
	info := &ClusterInfoData{
		Nodes: make([]NodeInfo, 0),
	}

	// 获取集群节点信息
	err := client.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		nodeInfo := NodeInfo{}
		
		// 获取节点地址
		opts := node.Options()
		nodeInfo.Addr = opts.Addr
		nodeInfo.Role = "master"

		// 获取DBSize
		if dbsize, err := node.DBSize(ctx).Result(); err == nil {
			nodeInfo.Keys = dbsize
			info.TotalKeys += dbsize
		}

		// 获取内存使用
		if memInfo, err := node.Info(ctx, "memory").Result(); err == nil {
			nodeInfo.Memory = parseMemoryFromInfo(memInfo)
			info.TotalMemory += nodeInfo.Memory
		}

		// 获取Redis版本（只需获取一次）
		if info.Version == "" {
			if serverInfo, err := node.Info(ctx, "server").Result(); err == nil {
				info.Version = parseVersionFromInfo(serverInfo)
			}
		}

		info.Nodes = append(info.Nodes, nodeInfo)
		return nil
	})

	if err == nil {
		info.NodeCount = len(info.Nodes)
	}

	return info
}

func (s *Server) getStandaloneInfo(ctx context.Context, client *redis.Client, addr string) *ClusterInfoData {
	info := &ClusterInfoData{
		NodeCount: 1,
		Nodes:     make([]NodeInfo, 0),
	}

	nodeInfo := NodeInfo{
		Addr: addr,
		Role: "master",
	}

	// 获取DBSize
	if dbsize, err := client.DBSize(ctx).Result(); err == nil {
		nodeInfo.Keys = dbsize
		info.TotalKeys = dbsize
	}

	// 获取内存和版本信息
	if serverInfo, err := client.Info(ctx, "server").Result(); err == nil {
		info.Version = parseVersionFromInfo(serverInfo)
	}
	if memInfo, err := client.Info(ctx, "memory").Result(); err == nil {
		nodeInfo.Memory = parseMemoryFromInfo(memInfo)
		info.TotalMemory = nodeInfo.Memory
	}

	info.Nodes = append(info.Nodes, nodeInfo)
	return info
}

func parseVersionFromInfo(info string) string {
	for _, line := range splitLines(info) {
		if len(line) > 14 && line[:14] == "redis_version:" {
			return line[14:]
		}
	}
	return "unknown"
}

func parseMemoryFromInfo(info string) int64 {
	for _, line := range splitLines(info) {
		if len(line) > 10 && line[:10] == "used_memory:" {
			var mem int64
			for i := 10; i < len(line); i++ {
				if line[i] >= '0' && line[i] <= '9' {
					mem = mem*10 + int64(line[i]-'0')
				}
			}
			return mem
		}
	}
	return 0
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			lines = append(lines, line)
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
