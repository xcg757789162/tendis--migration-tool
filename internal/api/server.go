package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	wsHub  *WebSocketHub // WebSocket Hub
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

	// 创建 WebSocket Hub
	wsHub := NewWebSocketHub()

	s := &Server{
		master: master,
		router: router,
		port:   port,
		wsHub:  wsHub,
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
			tasks.POST("/:id/stop", s.stopTask)      // 停止任务（也是停止增量同步）
			tasks.POST("/:id/complete", s.completeTask) // 完成任务（停止+可选校验+标记完成）
			tasks.POST("/:id/preflight-check", s.preflightCheck) // 迁移前校验
			tasks.GET("/:id/progress", s.getProgress)
			tasks.GET("/:id/metrics", s.getMetrics)
			tasks.POST("/:id/verify", s.triggerVerify)
			tasks.GET("/:id/verify/results", s.getVerifyResults)
			tasks.GET("/:id/report", s.getReport)
			
			// 冲突 Key 管理（这是我们相比 Redis-Shake 的重要优势）
			tasks.GET("/:id/conflicts", s.getConflictKeys)          // 查询冲突 Key
			tasks.GET("/:id/conflicts/summary", s.getConflictSummary) // 获取统计摘要
			tasks.GET("/:id/conflicts/export", s.exportConflictKeys) // 导出冲突 Key
		}

		// 系统信息
		v1.GET("/system/status", s.getSystemStatus)
		v1.GET("/system/workers", s.getWorkers)
		v1.GET("/health", s.healthCheck)

		// 测试连接
		v1.POST("/test-connection", s.testConnection)
	}

	// WebSocket 路由 (实时监控)
	s.router.GET("/ws", s.wsHub.HandleWebSocket)

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

// GetWebSocketHub 获取 WebSocket Hub
func (s *Server) GetWebSocketHub() *WebSocketHub {
	return s.wsHub
}

// BroadcastTaskMetrics 广播任务指标 (供外部调用)
func (s *Server) BroadcastTaskMetrics(taskId string, metrics *TaskMetrics) {
	if s.wsHub != nil {
		s.wsHub.BroadcastMetrics(taskId, metrics)
	}
}

// BroadcastTaskStatus 广播任务状态变更
func (s *Server) BroadcastTaskStatus(taskId string, status string) {
	if s.wsHub != nil {
		s.wsHub.BroadcastStatus(taskId, status)
	}
}

// BroadcastTaskLog 广播任务日志
func (s *Server) BroadcastTaskLog(taskId string, level string, message string) {
	if s.wsHub != nil {
		s.wsHub.BroadcastLog(taskId, level, message)
	}
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

// stopTask 停止任务（也是停止增量同步的方式）
// 简化设计：停止增量同步 = 停止任务
func (s *Server) stopTask(c *gin.Context) {
	taskID := c.Param("id")

	if err := s.master.StopTask(taskID); err != nil {
		fail(c, 500, "Stop task failed: "+err.Error())
		return
	}

	success(c, map[string]interface{}{
		"message": "Task stopped (incremental sync also stopped)",
	})
}

// completeTask 完成任务（停止任务，可选触发校验，标记完成）
func (s *Server) completeTask(c *gin.Context) {
	taskID := c.Param("id")
	
	// 可选参数：是否跳过校验
	skipVerify := c.Query("skip_verify") == "true"

	if err := s.master.CompleteTask(taskID, skipVerify); err != nil {
		fail(c, 500, "Complete task failed: "+err.Error())
		return
	}

	success(c, map[string]interface{}{
		"message": "Task completed successfully",
	})
}

// PreflightCheckResult 迁移前校验结果
type PreflightCheckResult struct {
	CanStart bool                  `json:"can_start"`
	Checks   []PreflightCheckItem  `json:"checks"`
	Summary  string                `json:"summary"`
}

// PreflightCheckItem 单个校验项
type PreflightCheckItem struct {
	Name     string `json:"name"`
	Status   string `json:"status"` // passed, failed, warning
	Message  string `json:"message"`
	Details  string `json:"details,omitempty"`
	Required bool   `json:"required"` // 是否为必须通过的校验项
}

// preflightCheck 迁移前依赖校验
func (s *Server) preflightCheck(c *gin.Context) {
	taskID := c.Param("id")

	// 获取任务配置
	task, err := s.master.GetTask(taskID)
	if err != nil {
		fail(c, 500, "Get task failed: "+err.Error())
		return
	}
	if task == nil {
		fail(c, 404, "Task not found")
		return
	}

	// 解析集群配置
	var sourceCluster, targetCluster model.ClusterConfig
	if err := json.Unmarshal([]byte(task.SourceCluster), &sourceCluster); err != nil {
		fail(c, 500, "Parse source cluster config failed: "+err.Error())
		return
	}
	if err := json.Unmarshal([]byte(task.TargetCluster), &targetCluster); err != nil {
		fail(c, 500, "Parse target cluster config failed: "+err.Error())
		return
	}

	// 解析任务配置
	var migrationOpts model.MigrationOptions
	if task.Config != "" {
		if err := json.Unmarshal([]byte(task.Config), &migrationOpts); err != nil {
			fail(c, 500, "Parse migration options failed: "+err.Error())
			return
		}
	}

	// 执行校验
	result := &PreflightCheckResult{
		CanStart: true,
		Checks:   make([]PreflightCheckItem, 0),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second) // 留5秒buffer
	defer cancel()

	// 1. 校验源集群连接（必须）
	sourceCheck := s.checkClusterConnection(ctx, "源集群", &sourceCluster)
	sourceCheck.Required = true
	result.Checks = append(result.Checks, sourceCheck)
	if sourceCheck.Status == "failed" {
		result.CanStart = false
	}

	// 2. 校验目标集群连接（必须）
	targetCheck := s.checkClusterConnection(ctx, "目标集群", &targetCluster)
	targetCheck.Required = true
	result.Checks = append(result.Checks, targetCheck)
	if targetCheck.Status == "failed" {
		result.CanStart = false
	}

	// 3. 校验源集群增量同步相关配置（如果需要增量同步）
	if !migrationOpts.SkipIncremental {
		// 3a. 校验增量同步支持（必须）
		incrCheck := s.checkIncrementalSupport(ctx, &sourceCluster)
		incrCheck.Required = true
		result.Checks = append(result.Checks, incrCheck)
		if incrCheck.Status == "failed" {
			result.CanStart = false
		}

		// 3b. 校验 binlog-enabled 配置（必须 - Tendis 特有）
		binlogCheck := s.checkBinlogEnabled(ctx, &sourceCluster)
		if binlogCheck != nil {
			binlogCheck.Required = true
			result.Checks = append(result.Checks, *binlogCheck)
			if binlogCheck.Status == "failed" {
				result.CanStart = false
			}
		}

		// 3c. 校验 aof-enabled 配置（必须 - Tendis 特有）
		aofCheck := s.checkAofEnabled(ctx, &sourceCluster)
		if aofCheck != nil {
			aofCheck.Required = true
			result.Checks = append(result.Checks, *aofCheck)
			if aofCheck.Status == "failed" {
				result.CanStart = false
			}
		}
	}

	// 4. 校验网络延迟（非必须，仅警告）
	latencyCheck := s.checkNetworkLatency(ctx, &sourceCluster, &targetCluster)
	latencyCheck.Required = false
	result.Checks = append(result.Checks, latencyCheck)

	// 5. 校验目标端数据覆盖风险（非必须，仅警告）
	overwriteCheck := s.checkTargetDataOverwrite(ctx, &targetCluster)
	overwriteCheck.Required = false
	result.Checks = append(result.Checks, overwriteCheck)

	// 6. 校验集群 Slot 完整性（必须）
	if sourceCheck.Status == "passed" {
		slotCheck := s.checkSlotCoverage(ctx, "源集群", &sourceCluster)
		if slotCheck != nil {
			slotCheck.Required = true
			result.Checks = append(result.Checks, *slotCheck)
			if slotCheck.Status == "failed" {
				result.CanStart = false
			}
		}
	}
	if targetCheck.Status == "passed" {
		slotCheck := s.checkSlotCoverage(ctx, "目标集群", &targetCluster)
		if slotCheck != nil {
			slotCheck.Required = true
			result.Checks = append(result.Checks, *slotCheck)
			if slotCheck.Status == "failed" {
				result.CanStart = false
			}
		}
	}

	// 7. 校验 Tendis kvstorecount 配置（如果需要增量同步且为 Tendis）
	if !migrationOpts.SkipIncremental {
		kvCheck := s.checkKvstorecount(ctx, &sourceCluster)
		if kvCheck != nil {
			kvCheck.Required = false // 仅作信息展示和警告
			result.Checks = append(result.Checks, *kvCheck)
		}
	}

	// 生成摘要
	if result.CanStart {
		result.Summary = "所有校验通过，可以开始迁移"
	} else {
		result.Summary = "部分校验未通过，请解决问题后再启动任务"
	}

	success(c, result)
}

// checkClusterConnection 检查集群连接
func (s *Server) checkClusterConnection(ctx context.Context, name string, config *model.ClusterConfig) PreflightCheckItem {
	item := PreflightCheckItem{
		Name: name + "连接",
	}

	if config == nil || len(config.Addrs) == 0 {
		item.Status = "failed"
		item.Message = "集群配置为空"
		return item
	}

	// 尝试连接
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    config.Addrs,
		Password: config.Password,
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		// 尝试单机模式
		standaloneClient := redis.NewClient(&redis.Options{
			Addr:     config.Addrs[0],
			Password: config.Password,
		})
		defer standaloneClient.Close()

		if err := standaloneClient.Ping(ctx).Err(); err != nil {
			item.Status = "failed"
			item.Message = "连接失败: " + err.Error()
			return item
		}

		item.Status = "passed"
		item.Message = "单机模式连接成功"
		return item
	}

	// 获取集群信息
	var nodeCount int
	var totalKeys int64
	err := client.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		nodeCount++
		if dbsize, err := node.DBSize(ctx).Result(); err == nil {
			totalKeys += dbsize
		}
		return nil
	})

	if err != nil {
		item.Status = "warning"
		item.Message = "连接成功，但获取集群信息失败"
		item.Details = err.Error()
	} else {
		item.Status = "passed"
		item.Message = "连接成功"
		item.Details = fmt.Sprintf("节点数: %d, 总Key数: %d", nodeCount, totalKeys)
	}

	return item
}

// checkIncrementalSupport 检查增量同步支持
func (s *Server) checkIncrementalSupport(ctx context.Context, config *model.ClusterConfig) PreflightCheckItem {
	item := PreflightCheckItem{
		Name: "增量同步支持",
	}

	if config == nil || len(config.Addrs) == 0 {
		item.Status = "failed"
		item.Message = "集群配置为空"
		return item
	}

	// 连接到第一个节点检查
	client := redis.NewClient(&redis.Options{
		Addr:     config.Addrs[0],
		Password: config.Password,
	})
	defer client.Close()

	// 检查是否支持 binlogpos 命令（Tendis）
	if _, err := client.Do(ctx, "binlogpos", 0).Result(); err == nil {
		item.Status = "passed"
		item.Message = "支持Tendis Binlog增量同步"
		return item
	}

	// 检查是否支持 PSYNC（标准Redis）
	if info, err := client.Info(ctx, "replication").Result(); err == nil {
		if len(info) > 0 {
			item.Status = "passed"
			item.Message = "支持Redis PSYNC增量同步"
			return item
		}
	}

	item.Status = "failed"
	item.Message = "源端不支持 Binlog 或 PSYNC 增量同步"
	item.Details = "增量同步需要源端支持 Tendis Binlog（binlogpos 命令）或 Redis PSYNC。请确认源端版本是否支持增量同步"
	return item
}

// checkNetworkLatency 检查网络延迟
func (s *Server) checkNetworkLatency(ctx context.Context, sourceConfig, targetConfig *model.ClusterConfig) PreflightCheckItem {
	item := PreflightCheckItem{
		Name: "网络延迟",
	}

	if sourceConfig == nil || targetConfig == nil || len(sourceConfig.Addrs) == 0 || len(targetConfig.Addrs) == 0 {
		item.Status = "warning"
		item.Message = "无法检测延迟"
		return item
	}

	// 测试源集群延迟
	sourceClient := redis.NewClient(&redis.Options{
		Addr:     sourceConfig.Addrs[0],
		Password: sourceConfig.Password,
	})
	defer sourceClient.Close()

	sourceStart := time.Now()
	if err := sourceClient.Ping(ctx).Err(); err != nil {
		item.Status = "warning"
		item.Message = "无法测试源集群延迟: " + err.Error()
		return item
	}
	sourceLatency := time.Since(sourceStart).Milliseconds()

	// 测试目标集群延迟
	targetClient := redis.NewClient(&redis.Options{
		Addr:     targetConfig.Addrs[0],
		Password: targetConfig.Password,
	})
	defer targetClient.Close()

	targetStart := time.Now()
	if err := targetClient.Ping(ctx).Err(); err != nil {
		item.Status = "warning"
		item.Message = "无法测试目标集群延迟: " + err.Error()
		return item
	}
	targetLatency := time.Since(targetStart).Milliseconds()

	maxLatency := sourceLatency
	if targetLatency > maxLatency {
		maxLatency = targetLatency
	}

	if maxLatency < 10 {
		item.Status = "passed"
		item.Message = "网络延迟低"
	} else if maxLatency < 50 {
		item.Status = "passed"
		item.Message = "网络延迟正常"
	} else if maxLatency < 100 {
		item.Status = "warning"
		item.Message = "网络延迟较高，可能影响迁移性能"
	} else {
		item.Status = "warning"
		item.Message = "网络延迟很高，建议优化网络环境"
	}

	item.Details = fmt.Sprintf("源集群: %dms, 目标集群: %dms", sourceLatency, targetLatency)
	return item
}

// checkBinlogEnabled 检查源端 Tendis 是否启用了 binlog
// 返回 nil 表示非 Tendis（不需要此检测）
func (s *Server) checkBinlogEnabled(ctx context.Context, config *model.ClusterConfig) *PreflightCheckItem {
	if config == nil || len(config.Addrs) == 0 {
		return nil
	}

	client := redis.NewClient(&redis.Options{
		Addr:     config.Addrs[0],
		Password: config.Password,
	})
	defer client.Close()

	// 先检测是否是 Tendis（通过 binlogpos 命令判断）
	if _, err := client.Do(ctx, "binlogpos", 0).Result(); err != nil {
		return nil // 非 Tendis，不需要检测 binlog-enabled
	}

	// 是 Tendis，检查 binlog-enabled 配置
	result, err := client.Do(ctx, "CONFIG", "GET", "binlog-enabled").Result()
	if err != nil {
		item := &PreflightCheckItem{
			Name:    "Binlog 配置",
			Status:  "warning",
			Message: "无法读取 binlog-enabled 配置: " + err.Error(),
			Details: "请手动确认源端 Tendis 已设置 binlog-enabled=yes",
		}
		return item
	}

	// CONFIG GET 返回 []interface{}{"binlog-enabled", "yes/no"}
	if vals, ok := result.([]interface{}); ok && len(vals) >= 2 {
		val := fmt.Sprintf("%v", vals[1])
		if val == "yes" || val == "true" || val == "1" {
			item := &PreflightCheckItem{
				Name:    "Binlog 配置",
				Status:  "passed",
				Message: "binlog-enabled=yes，增量同步数据源就绪",
			}
			return item
		}
		item := &PreflightCheckItem{
			Name:    "Binlog 配置",
			Status:  "failed",
			Message: "源端 Tendis 未开启 binlog（binlog-enabled=" + val + "）",
			Details: "增量同步依赖 binlog 数据，请在源端执行: CONFIG SET binlog-enabled yes，并确认持久化到配置文件",
		}
		return item
	}

	item := &PreflightCheckItem{
		Name:    "Binlog 配置",
		Status:  "warning",
		Message: "binlog-enabled 配置返回格式异常",
		Details: fmt.Sprintf("返回值: %v，请手动确认源端 binlog 已启用", result),
	}
	return item
}

// checkAofEnabled 检查源端 Tendis 是否启用了 aof-enabled
// aof-enabled=yes 时 binlog cmdStr 包含完整 RESP 命令，增量同步才能正确回放 EXPIRE/TTL 等命令
// 返回 nil 表示非 Tendis（不需要此检测）
func (s *Server) checkAofEnabled(ctx context.Context, config *model.ClusterConfig) *PreflightCheckItem {
	if config == nil || len(config.Addrs) == 0 {
		return nil
	}

	client := redis.NewClient(&redis.Options{
		Addr:     config.Addrs[0],
		Password: config.Password,
	})
	defer client.Close()

	// 先检测是否是 Tendis（通过 binlogpos 命令判断）
	if _, err := client.Do(ctx, "binlogpos", 0).Result(); err != nil {
		return nil // 非 Tendis，不需要检测 aof-enabled
	}

	// 是 Tendis，检查 aof-enabled 配置
	result, err := client.Do(ctx, "CONFIG", "GET", "aof-enabled").Result()
	if err != nil {
		item := &PreflightCheckItem{
			Name:    "AOF 配置",
			Status:  "warning",
			Message: "无法读取 aof-enabled 配置: " + err.Error(),
			Details: "请手动确认源端 Tendis 已设置 aof-enabled=yes",
		}
		return item
	}

	// CONFIG GET 返回 []interface{}{"aof-enabled", "yes/no"}
	if vals, ok := result.([]interface{}); ok && len(vals) >= 2 {
		val := fmt.Sprintf("%v", vals[1])
		if val == "yes" || val == "true" || val == "1" {
			item := &PreflightCheckItem{
				Name:    "AOF 配置",
				Status:  "passed",
				Message: "aof-enabled=yes，binlog 将包含完整 RESP 命令",
			}
			return item
		}
		item := &PreflightCheckItem{
			Name:    "AOF 配置",
			Status:  "failed",
			Message: "源端 Tendis 未开启 AOF（aof-enabled=" + val + "）",
			Details: "aof-enabled=no 时 binlog 只记录命令名，不包含参数，EXPIRE/TTL 等命令无法在增量同步中正确回放。请在源端执行: CONFIG SET aof-enabled yes，并确认持久化到配置文件",
		}
		return item
	}

	item := &PreflightCheckItem{
		Name:    "AOF 配置",
		Status:  "warning",
		Message: "aof-enabled 配置返回格式异常",
		Details: fmt.Sprintf("返回值: %v，请手动确认源端 aof-enabled=yes", result),
	}
	return item
}

// checkTargetDataOverwrite 检查目标端是否已有数据（覆盖风险提醒）
func (s *Server) checkTargetDataOverwrite(ctx context.Context, config *model.ClusterConfig) PreflightCheckItem {
	item := PreflightCheckItem{
		Name: "目标端数据检查",
	}

	if config == nil || len(config.Addrs) == 0 {
		item.Status = "warning"
		item.Message = "无法检查目标端数据"
		return item
	}

	// 尝试集群模式
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    config.Addrs,
		Password: config.Password,
	})
	defer client.Close()

	var totalKeys int64
	err := client.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		if dbsize, err := node.DBSize(ctx).Result(); err == nil {
			totalKeys += dbsize
		}
		return nil
	})

	if err != nil {
		// 降级为单机模式
		standaloneClient := redis.NewClient(&redis.Options{
			Addr:     config.Addrs[0],
			Password: config.Password,
		})
		defer standaloneClient.Close()
		if dbsize, err := standaloneClient.DBSize(ctx).Result(); err == nil {
			totalKeys = dbsize
		}
	}

	if totalKeys == 0 {
		item.Status = "passed"
		item.Message = "目标端为空，可安全写入"
	} else {
		item.Status = "warning"
		item.Message = fmt.Sprintf("目标端已有 %d 个 Key，迁移可能覆盖已有数据", totalKeys)
		item.Details = "如果使用 RESTORE REPLACE 模式，同名 Key 将被覆盖。请确认目标端数据是否可以被覆盖"
	}

	return item
}

// checkSlotCoverage 检查集群 Slot 是否完整覆盖 0-16383
// 返回 nil 表示非集群模式（不需要此检测）
func (s *Server) checkSlotCoverage(ctx context.Context, name string, config *model.ClusterConfig) *PreflightCheckItem {
	if config == nil || len(config.Addrs) == 0 {
		return nil
	}

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    config.Addrs,
		Password: config.Password,
	})
	defer client.Close()

	slots, err := client.ClusterSlots(ctx).Result()
	if err != nil {
		// 非集群模式，不需要检查 slot
		return nil
	}

	// 统计已覆盖的 slot 范围
	covered := make([]bool, 16384)
	for _, slot := range slots {
		for i := int(slot.Start); i <= int(slot.End) && i < 16384; i++ {
			covered[i] = true
		}
	}

	// 统计未覆盖的 slot
	var uncoveredCount int
	var uncoveredRanges []string
	rangeStart := -1
	for i := 0; i <= 16384; i++ {
		if i < 16384 && !covered[i] {
			if rangeStart == -1 {
				rangeStart = i
			}
			uncoveredCount++
		} else {
			if rangeStart != -1 {
				if rangeStart == i-1 {
					uncoveredRanges = append(uncoveredRanges, fmt.Sprintf("%d", rangeStart))
				} else {
					uncoveredRanges = append(uncoveredRanges, fmt.Sprintf("%d-%d", rangeStart, i-1))
				}
				rangeStart = -1
			}
		}
	}

	item := &PreflightCheckItem{
		Name: name + " Slot 覆盖",
	}

	if uncoveredCount == 0 {
		item.Status = "passed"
		item.Message = fmt.Sprintf("Slot 完整覆盖 (0-16383), %d 个 Master 节点", len(slots))
	} else {
		item.Status = "failed"
		item.Message = fmt.Sprintf("Slot 不完整，缺少 %d 个 Slot", uncoveredCount)
		details := "未覆盖的 Slot 范围: "
		if len(uncoveredRanges) > 10 {
			details += fmt.Sprintf("%v ... (共 %d 个范围)", uncoveredRanges[:10], len(uncoveredRanges))
		} else {
			details += fmt.Sprintf("%v", uncoveredRanges)
		}
		item.Details = details
	}

	return item
}

// checkKvstorecount 检查 Tendis kvstorecount 配置
// 返回 nil 表示非 Tendis
func (s *Server) checkKvstorecount(ctx context.Context, config *model.ClusterConfig) *PreflightCheckItem {
	if config == nil || len(config.Addrs) == 0 {
		return nil
	}

	client := redis.NewClient(&redis.Options{
		Addr:     config.Addrs[0],
		Password: config.Password,
	})
	defer client.Close()

	// 先检测是否是 Tendis
	if _, err := client.Do(ctx, "binlogpos", 0).Result(); err != nil {
		return nil // 非 Tendis
	}

	// 读取 kvstorecount
	result, err := client.Do(ctx, "CONFIG", "GET", "kvstorecount").Result()
	if err != nil {
		item := &PreflightCheckItem{
			Name:    "KvStoreCount 配置",
			Status:  "warning",
			Message: "无法读取 kvstorecount 配置: " + err.Error(),
		}
		return item
	}

	if vals, ok := result.([]interface{}); ok && len(vals) >= 2 {
		val := fmt.Sprintf("%v", vals[1])
		item := &PreflightCheckItem{
			Name:    "KvStoreCount 配置",
			Status:  "passed",
			Message: fmt.Sprintf("kvstorecount=%s", val),
			Details: "每个 Tendis 节点有 " + val + " 个 Store，增量同步将为每个 Store 注册独立的 Binlog 通道",
		}
		return item
	}

	return nil
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

	// 解析校验配置
	mode := c.DefaultQuery("mode", "sample")
	sampleSizeStr := c.DefaultQuery("sample_size", "10000")
	sampleSize := 10000
	if n, err := strconv.Atoi(sampleSizeStr); err == nil && n > 0 {
		sampleSize = n
	}

	var config *engine.VerifyConfig
	switch mode {
	case "full":
		config = &engine.VerifyConfig{
			Mode:        engine.VerifyModeFull,
			BatchSize:   1000,
			Concurrency: 50,
		}
	default:
		config = &engine.VerifyConfig{
			Mode:        engine.VerifyModeSample,
			SampleSize:  sampleSize,
			BatchSize:   1000,
			Concurrency: 50,
		}
	}

	batchID, err := s.master.TriggerVerify(taskID, config)
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

	clusterErr := clusterClient.Ping(ctx).Err()
	if clusterErr == nil {
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
	log.Printf("[TestConnection] Cluster ping failed: %v (elapsed: %v)", clusterErr, time.Since(startTime))

	// 尝试单机模式连接（使用独立的 context 避免被集群连接耗尽）
	standaloneCtx, standaloneCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer standaloneCancel()

	standaloneClient := redis.NewClient(&redis.Options{
		Addr:     addrs[0],
		Password: req.Password,
	})
	defer standaloneClient.Close()

	if err := standaloneClient.Ping(standaloneCtx).Err(); err != nil {
		log.Printf("[TestConnection] Standalone ping also failed: %v", err)
		fail(c, 400, "连接失败: "+err.Error())
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

// =====================================================
// 冲突 Key 管理 API（这是我们相比 Redis-Shake 的重要优势）
// Redis-Shake 不支持跳过已存在的 Key，也不记录冲突 Key
// =====================================================

// getConflictKeys 查询冲突 Key 列表
// GET /api/v1/tasks/:id/conflict-keys?page=1&size=100&prefix=&type=&phase=&action=
func (s *Server) getConflictKeys(c *gin.Context) {
	taskID := c.Param("id")
	
	// 解析分页参数
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	size, _ := strconv.Atoi(c.DefaultQuery("size", "100"))
	
	if page < 1 {
		page = 1
	}
	if size <= 0 {
		size = 100
	}
	if size > 1000 {
		size = 1000 // 最大1000条
	}
	
	// 解析过滤参数
	filter := &engine.ConflictKeyFilter{
		KeyPrefix: c.Query("prefix"),
		KeyType:   c.Query("type"),
		Phase:     c.Query("phase"),
		Action:    c.Query("action"),
	}
	
	// 解析时间范围
	if startTime := c.Query("start_time"); startTime != "" {
		if t, err := time.Parse(time.RFC3339, startTime); err == nil {
			filter.StartTime = &t
		}
	}
	if endTime := c.Query("end_time"); endTime != "" {
		if t, err := time.Parse(time.RFC3339, endTime); err == nil {
			filter.EndTime = &t
		}
	}
	
	// 获取冲突 Key 存储
	conflictStore := s.master.GetConflictKeyStore(taskID)
	if conflictStore == nil {
		// 如果没有冲突 Key 存储，返回空结果
		success(c, map[string]interface{}{
			"total": 0,
			"page":  page,
			"size":  size,
			"keys":  []interface{}{},
		})
		return
	}
	
	// 查询
	result, err := conflictStore.Query(page, size, filter)
	if err != nil {
		fail(c, 500, "Query conflict keys failed: "+err.Error())
		return
	}
	
	success(c, result)
}

// getConflictSummary 获取冲突 Key 统计摘要
// GET /api/v1/tasks/:id/conflict-keys/summary
func (s *Server) getConflictSummary(c *gin.Context) {
	taskID := c.Param("id")
	
	conflictStore := s.master.GetConflictKeyStore(taskID)
	if conflictStore == nil {
		success(c, map[string]interface{}{
			"total_count":   0,
			"memory_count":  0,
			"disk_count":    0,
			"by_phase":      map[string]int64{},
			"by_action":     map[string]int64{},
			"by_type":       map[string]int64{},
		})
		return
	}
	
	summary := conflictStore.GetSummary()
	success(c, summary)
}

// exportConflictKeys 导出冲突 Key
// GET /api/v1/tasks/:id/conflict-keys/export?format=jsonl|json|csv
func (s *Server) exportConflictKeys(c *gin.Context) {
	taskID := c.Param("id")
	format := c.DefaultQuery("format", "jsonl")
	
	conflictStore := s.master.GetConflictKeyStore(taskID)
	if conflictStore == nil {
		fail(c, 404, "No conflict keys found for this task")
		return
	}
	
	// 解析过滤参数
	filter := &engine.ConflictKeyFilter{
		KeyPrefix: c.Query("prefix"),
		KeyType:   c.Query("type"),
		Phase:     c.Query("phase"),
		Action:    c.Query("action"),
	}
	
	// 设置响应头
	var contentType string
	var filename string
	switch format {
	case "json":
		contentType = "application/json"
		filename = taskID + "_conflict_keys.json"
	case "csv":
		contentType = "text/csv"
		filename = taskID + "_conflict_keys.csv"
	default:
		contentType = "application/x-ndjson"
		filename = taskID + "_conflict_keys.jsonl"
		format = "jsonl"
	}
	
	c.Header("Content-Type", contentType)
	c.Header("Content-Disposition", "attachment; filename=\""+filename+"\"")
	
	// 导出
	if err := conflictStore.Export(c.Writer, format, filter); err != nil {
		// 已经开始写入响应，无法返回错误JSON
		c.Writer.WriteString("\n\nExport error: " + err.Error())
	}
}

