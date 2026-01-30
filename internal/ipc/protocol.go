package ipc

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// MaxMessageSize 最大消息大小
	MaxMessageSize = 64 * 1024 * 1024 // 64MB

	// HeartbeatInterval 心跳间隔
	HeartbeatInterval = 5 * time.Second

	// HeartbeatTimeout 心跳超时
	HeartbeatTimeout = 30 * time.Second
)

// MessageType 消息类型
type MessageType int

const (
	// Master -> Worker
	MsgAssignSlots MessageType = iota + 1
	MsgPause
	MsgResume
	MsgStop
	MsgPing

	// Worker -> Master
	MsgCheckpointReport
	MsgProgressReport
	MsgError
	MsgCompleted
	MsgPong

	// Data Request (Worker -> Master)
	MsgRequestChanges
	MsgChangesResponse
)

// Message IPC消息
type Message struct {
	Type      MessageType `json:"type"`
	RequestID string      `json:"request_id,omitempty"`
	Data      interface{} `json:"data"`
	Timestamp int64       `json:"ts"`
}

// SlotAssignment Slot分配消息
type SlotAssignment struct {
	WorkerID  string `json:"worker_id"`
	SlotStart int    `json:"slot_start"`
	SlotEnd   int    `json:"slot_end"`
	TaskID    string `json:"task_id"`
}

// CheckpointReport 断点报告
type CheckpointReport struct {
	WorkerID   string      `json:"worker_id"`
	SlotID     int         `json:"slot_id"`
	Cursor     string      `json:"cursor"`
	Keys       int64       `json:"keys"`
	Bytes      int64       `json:"bytes"`
	LastKey    string      `json:"last_key"`
	Timestamp  time.Time   `json:"timestamp"`
}

// ProgressReport 进度报告
type ProgressReport struct {
	WorkerID      string  `json:"worker_id"`
	KeysProcessed int64   `json:"keys_processed"`
	BytesSent     int64   `json:"bytes_sent"`
	CurrentSpeed  float64 `json:"current_speed"`
	ErrorCount    int64   `json:"error_count"`
}

// ErrorReport 错误报告
type ErrorReport struct {
	WorkerID string `json:"worker_id"`
	SlotID   int    `json:"slot_id"`
	Error    string `json:"error"`
	Key      string `json:"key,omitempty"`
}

// EncodeMessage 编码消息
func EncodeMessage(msg *Message) ([]byte, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)
	return buf, nil
}

// DecodeMessage 解码消息
func DecodeMessage(r io.Reader) (*Message, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lengthBuf)
	if length > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes", length)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// SendMessage 发送消息
func SendMessage(conn net.Conn, msg *Message) error {
	msg.Timestamp = time.Now().Unix()
	data, err := EncodeMessage(msg)
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}

// Server Unix Socket服务端
type Server struct {
	socketPath string
	listener   net.Listener
	handlers   map[MessageType]MessageHandler
	clients    map[string]net.Conn
	mu         sync.RWMutex
	stopCh     chan struct{}
}

// MessageHandler 消息处理器
type MessageHandler func(conn net.Conn, msg *Message) error

// NewServer 创建服务端
func NewServer(socketPath string) *Server {
	return &Server{
		socketPath: socketPath,
		handlers:   make(map[MessageType]MessageHandler),
		clients:    make(map[string]net.Conn),
		stopCh:     make(chan struct{}),
	}
}

// RegisterHandler 注册处理器
func (s *Server) RegisterHandler(msgType MessageType, handler MessageHandler) {
	s.handlers[msgType] = handler
}

// Start 启动服务
func (s *Server) Start() error {
	// 确保目录存在
	dir := filepath.Dir(s.socketPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// 删除旧的socket文件
	os.Remove(s.socketPath)

	listener, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return err
	}
	s.listener = listener

	go s.acceptLoop()
	return nil
}

// acceptLoop 接受连接
func (s *Server) acceptLoop() {
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			continue
		}

		go s.handleConnection(conn)
	}
}

// handleConnection 处理连接
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	// 读取首条消息获取WorkerID
	msg, err := DecodeMessage(conn)
	if err != nil {
		return
	}

	// 注册客户端
	if data, ok := msg.Data.(map[string]interface{}); ok {
		if workerID, ok := data["worker_id"].(string); ok {
			s.mu.Lock()
			s.clients[workerID] = conn
			s.mu.Unlock()

			defer func() {
				s.mu.Lock()
				delete(s.clients, workerID)
				s.mu.Unlock()
			}()
		}
	}

	// 处理首条消息
	if handler, ok := s.handlers[msg.Type]; ok {
		handler(conn, msg)
	}

	// 持续读取消息
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(HeartbeatTimeout))
		msg, err := DecodeMessage(conn)
		if err != nil {
			return
		}

		if handler, ok := s.handlers[msg.Type]; ok {
			if err := handler(conn, msg); err != nil {
				// Log error
			}
		}
	}
}

// SendToWorker 发送消息到Worker
func (s *Server) SendToWorker(workerID string, msg *Message) error {
	s.mu.RLock()
	conn, ok := s.clients[workerID]
	s.mu.RUnlock()

	if !ok {
		return fmt.Errorf("worker %s not found", workerID)
	}

	return SendMessage(conn, msg)
}

// BroadcastToWorkers 广播消息
func (s *Server) BroadcastToWorkers(msg *Message) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, conn := range s.clients {
		SendMessage(conn, msg)
	}
}

// GetConnectedWorkers 获取已连接的Worker
func (s *Server) GetConnectedWorkers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	workers := make([]string, 0, len(s.clients))
	for id := range s.clients {
		workers = append(workers, id)
	}
	return workers
}

// Stop 停止服务
func (s *Server) Stop() {
	close(s.stopCh)
	if s.listener != nil {
		s.listener.Close()
	}
	os.Remove(s.socketPath)
}

// Client Unix Socket客户端
type Client struct {
	socketPath string
	workerID   string
	conn       net.Conn
	handlers   map[MessageType]MessageHandler
	mu         sync.Mutex
	stopCh     chan struct{}
}

// NewClient 创建客户端
func NewClient(socketPath, workerID string) *Client {
	return &Client{
		socketPath: socketPath,
		workerID:   workerID,
		handlers:   make(map[MessageType]MessageHandler),
		stopCh:     make(chan struct{}),
	}
}

// RegisterHandler 注册处理器
func (c *Client) RegisterHandler(msgType MessageType, handler MessageHandler) {
	c.handlers[msgType] = handler
}

// Connect 连接服务端
func (c *Client) Connect() error {
	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		return err
	}
	c.conn = conn

	// 发送注册消息
	msg := &Message{
		Type: MsgPong,
		Data: map[string]interface{}{
			"worker_id": c.workerID,
		},
	}
	if err := SendMessage(conn, msg); err != nil {
		conn.Close()
		return err
	}

	go c.readLoop()
	go c.heartbeatLoop()

	return nil
}

// readLoop 读取消息循环
func (c *Client) readLoop() {
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		c.conn.SetReadDeadline(time.Now().Add(HeartbeatTimeout))
		msg, err := DecodeMessage(c.conn)
		if err != nil {
			continue
		}

		if handler, ok := c.handlers[msg.Type]; ok {
			handler(c.conn, msg)
		}

		// 处理Ping
		if msg.Type == MsgPing {
			c.Send(&Message{Type: MsgPong})
		}
	}
}

// heartbeatLoop 心跳循环
func (c *Client) heartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.Send(&Message{Type: MsgPong, Data: map[string]interface{}{"worker_id": c.workerID}})
		case <-c.stopCh:
			return
		}
	}
}

// Send 发送消息
func (c *Client) Send(msg *Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}
	return SendMessage(c.conn, msg)
}

// SendCheckpointReport 发送断点报告
func (c *Client) SendCheckpointReport(report *CheckpointReport) error {
	return c.Send(&Message{
		Type: MsgCheckpointReport,
		Data: report,
	})
}

// SendProgressReport 发送进度报告
func (c *Client) SendProgressReport(report *ProgressReport) error {
	return c.Send(&Message{
		Type: MsgProgressReport,
		Data: report,
	})
}

// SendError 发送错误
func (c *Client) SendError(report *ErrorReport) error {
	return c.Send(&Message{
		Type: MsgError,
		Data: report,
	})
}

// SendCompleted 发送完成
func (c *Client) SendCompleted() error {
	return c.Send(&Message{
		Type: MsgCompleted,
		Data: map[string]interface{}{"worker_id": c.workerID},
	})
}

// Close 关闭连接
func (c *Client) Close() {
	close(c.stopCh)
	if c.conn != nil {
		c.conn.Close()
	}
}
