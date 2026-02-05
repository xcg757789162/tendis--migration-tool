package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// WebSocketHub WebSocket 连接管理器
type WebSocketHub struct {
	// 客户端连接
	clients map[*WebSocketClient]bool
	mu      sync.RWMutex

	// 订阅管理：taskId -> 客户端列表
	subscriptions map[string]map[*WebSocketClient]bool
	subMu         sync.RWMutex

	// WebSocket 升级器
	upgrader websocket.Upgrader

	// 广播间隔
	broadcastInterval time.Duration

	// 上下文
	ctx    context.Context
	cancel context.CancelFunc

	// 日志
	logger *log.Logger
}

// WebSocketClient 单个 WebSocket 连接
type WebSocketClient struct {
	hub     *WebSocketHub
	conn    *websocket.Conn
	send    chan []byte
	taskIds []string // 订阅的任务ID列表
}

// WebSocketMessage WebSocket 消息格式
type WebSocketMessage struct {
	Type    string      `json:"type"`    // 消息类型: subscribe, unsubscribe, metrics, log, status
	TaskId  string      `json:"task_id,omitempty"`
	Payload interface{} `json:"payload,omitempty"`
}

// TaskMetrics 任务指标数据
type TaskMetrics struct {
	TaskId          string    `json:"task_id"`
	Status          string    `json:"status"`
	Progress        float64   `json:"progress"`
	ProcessedKeys   int64     `json:"processed_keys"`
	TotalKeys       int64     `json:"total_keys"`
	SuccessKeys     int64     `json:"success_keys"`
	FailedKeys      int64     `json:"failed_keys"`
	SkippedKeys     int64     `json:"skipped_keys"`
	ConflictKeys    int64     `json:"conflict_keys"`
	CurrentQPS      int64     `json:"current_qps"`
	AvgSpeed        float64   `json:"avg_speed"`
	BytesRead       int64     `json:"bytes_read"`
	BytesWritten    int64     `json:"bytes_written"`
	BigKeyFound     int64     `json:"bigkey_found"`
	BigKeySuccess   int64     `json:"bigkey_success"`
	BigKeyFailed    int64     `json:"bigkey_failed"`
	ValidatedKeys   int64     `json:"validated_keys"`
	ConsistentKeys  int64     `json:"consistent_keys"`
	InconsistentKeys int64   `json:"inconsistent_keys"`
	LastUpdateTime  time.Time `json:"last_update_time"`
}

// NewWebSocketHub 创建 WebSocket Hub
func NewWebSocketHub() *WebSocketHub {
	ctx, cancel := context.WithCancel(context.Background())

	hub := &WebSocketHub{
		clients:           make(map[*WebSocketClient]bool),
		subscriptions:     make(map[string]map[*WebSocketClient]bool),
		broadcastInterval: time.Second,
		ctx:               ctx,
		cancel:            cancel,
		logger:            log.Default(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // 允许所有来源
			},
		},
	}

	return hub
}

// Run 运行 Hub
func (h *WebSocketHub) Run() {
	// Hub 现在是无状态的，不需要后台协程
	// 指标广播由外部调用 BroadcastMetrics
}

// Stop 停止 Hub
func (h *WebSocketHub) Stop() {
	h.cancel()

	// 关闭所有客户端
	h.mu.Lock()
	for client := range h.clients {
		close(client.send)
	}
	h.clients = make(map[*WebSocketClient]bool)
	h.mu.Unlock()
}

// HandleWebSocket 处理 WebSocket 连接
func (h *WebSocketHub) HandleWebSocket(c *gin.Context) {
	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.logger.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	client := &WebSocketClient{
		hub:     h,
		conn:    conn,
		send:    make(chan []byte, 256),
		taskIds: make([]string, 0),
	}

	// 注册客户端
	h.registerClient(client)

	// 启动读写协程
	go client.writePump()
	go client.readPump()
}

// registerClient 注册客户端
func (h *WebSocketHub) registerClient(client *WebSocketClient) {
	h.mu.Lock()
	h.clients[client] = true
	h.mu.Unlock()
}

// unregisterClient 注销客户端
func (h *WebSocketHub) unregisterClient(client *WebSocketClient) {
	h.mu.Lock()
	if _, ok := h.clients[client]; ok {
		delete(h.clients, client)
		close(client.send)
	}
	h.mu.Unlock()

	// 清理订阅
	h.subMu.Lock()
	for _, taskId := range client.taskIds {
		if clients, ok := h.subscriptions[taskId]; ok {
			delete(clients, client)
			if len(clients) == 0 {
				delete(h.subscriptions, taskId)
			}
		}
	}
	h.subMu.Unlock()
}

// Subscribe 订阅任务
func (h *WebSocketHub) Subscribe(client *WebSocketClient, taskId string) {
	h.subMu.Lock()
	defer h.subMu.Unlock()

	if _, ok := h.subscriptions[taskId]; !ok {
		h.subscriptions[taskId] = make(map[*WebSocketClient]bool)
	}
	h.subscriptions[taskId][client] = true
	client.taskIds = append(client.taskIds, taskId)
}

// Unsubscribe 取消订阅
func (h *WebSocketHub) Unsubscribe(client *WebSocketClient, taskId string) {
	h.subMu.Lock()
	defer h.subMu.Unlock()

	if clients, ok := h.subscriptions[taskId]; ok {
		delete(clients, client)
		if len(clients) == 0 {
			delete(h.subscriptions, taskId)
		}
	}

	// 更新客户端订阅列表
	newTaskIds := make([]string, 0, len(client.taskIds)-1)
	for _, id := range client.taskIds {
		if id != taskId {
			newTaskIds = append(newTaskIds, id)
		}
	}
	client.taskIds = newTaskIds
}

// BroadcastMetrics 广播任务指标
func (h *WebSocketHub) BroadcastMetrics(taskId string, metrics *TaskMetrics) {
	msg := WebSocketMessage{
		Type:    "metrics",
		TaskId:  taskId,
		Payload: metrics,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	h.subMu.RLock()
	clients := h.subscriptions[taskId]
	h.subMu.RUnlock()

	for client := range clients {
		select {
		case client.send <- data:
		default:
			// 发送缓冲区满，丢弃消息
		}
	}
}

// BroadcastLog 广播任务日志
func (h *WebSocketHub) BroadcastLog(taskId string, level string, message string) {
	msg := WebSocketMessage{
		Type:   "log",
		TaskId: taskId,
		Payload: map[string]interface{}{
			"level":     level,
			"message":   message,
			"timestamp": time.Now().UnixMilli(),
		},
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	h.subMu.RLock()
	clients := h.subscriptions[taskId]
	h.subMu.RUnlock()

	for client := range clients {
		select {
		case client.send <- data:
		default:
		}
	}
}

// BroadcastStatus 广播任务状态变更
func (h *WebSocketHub) BroadcastStatus(taskId string, status string) {
	msg := WebSocketMessage{
		Type:   "status",
		TaskId: taskId,
		Payload: map[string]interface{}{
			"status":    status,
			"timestamp": time.Now().UnixMilli(),
		},
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	h.subMu.RLock()
	clients := h.subscriptions[taskId]
	h.subMu.RUnlock()

	for client := range clients {
		select {
		case client.send <- data:
		default:
		}
	}
}

// BroadcastToAll 向所有客户端广播
func (h *WebSocketHub) BroadcastToAll(msgType string, payload interface{}) {
	msg := WebSocketMessage{
		Type:    msgType,
		Payload: payload,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	h.mu.RLock()
	clients := make([]*WebSocketClient, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.RUnlock()

	for _, client := range clients {
		select {
		case client.send <- data:
		default:
		}
	}
}

// GetClientCount 获取连接客户端数量
func (h *WebSocketHub) GetClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// ============ WebSocketClient Methods ============

// readPump 读取消息
func (c *WebSocketClient) readPump() {
	defer func() {
		c.hub.unregisterClient(c)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(4096)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.hub.logger.Printf("WebSocket error: %v", err)
			}
			break
		}

		// 解析消息
		var msg WebSocketMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		// 处理消息
		c.handleMessage(&msg)
	}
}

// writePump 发送消息
func (c *WebSocketClient) writePump() {
	ticker := time.NewTicker(30 * time.Second) // Ping 间隔
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// 批量发送缓冲区中的消息
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// handleMessage 处理客户端消息
func (c *WebSocketClient) handleMessage(msg *WebSocketMessage) {
	switch msg.Type {
	case "subscribe":
		if msg.TaskId != "" {
			c.hub.Subscribe(c, msg.TaskId)
			// 发送确认
			response := WebSocketMessage{
				Type:   "subscribed",
				TaskId: msg.TaskId,
			}
			data, _ := json.Marshal(response)
			select {
			case c.send <- data:
			default:
			}
		}

	case "unsubscribe":
		if msg.TaskId != "" {
			c.hub.Unsubscribe(c, msg.TaskId)
			// 发送确认
			response := WebSocketMessage{
				Type:   "unsubscribed",
				TaskId: msg.TaskId,
			}
			data, _ := json.Marshal(response)
			select {
			case c.send <- data:
			default:
			}
		}

	case "ping":
		// 响应 pong
		response := WebSocketMessage{Type: "pong"}
		data, _ := json.Marshal(response)
		select {
		case c.send <- data:
		default:
		}
	}
}
