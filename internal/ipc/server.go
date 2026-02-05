package ipc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// IPC 消息类型常量（用于 Master-Worker 通信，仅定义 protocol.go 中没有的）
const (
	// Worker -> Master (补充)
	MsgCheckpointReport = "checkpoint_report"
	MsgProgressReport   = "progress_report"
	MsgError            = "error"
	MsgCompleted        = "completed"
	MsgPong             = "pong"

	// Master -> Worker (补充)
	MsgPing       = "ping"
	MsgAssignSlot = "assign_slot"

	// 心跳间隔
	HeartbeatInterval = 5 * time.Second
)

// Message IPC 消息结构
type Message struct {
	Type string      `json:"type"`
	Data interface{} `json:"data,omitempty"`
}

// CheckpointReport 断点报告
type CheckpointReport struct {
	WorkerID string `json:"worker_id"`
	SlotID   int    `json:"slot_id"`
	Cursor   uint64 `json:"cursor"`
	Keys     int64  `json:"keys"`
	Bytes    int64  `json:"bytes"`
	LastKey  string `json:"last_key"`
}

// ProgressReport 进度报告
type ProgressReport struct {
	WorkerID      string `json:"worker_id"`
	KeysProcessed int64  `json:"keys_processed"`
	BytesSent     int64  `json:"bytes_sent"`
}

// ErrorReport 错误报告
type ErrorReport struct {
	WorkerID string `json:"worker_id"`
	SlotID   int    `json:"slot_id"`
	Error    string `json:"error"`
}

// MessageHandler IPC 消息处理函数（新版）
type MessageHandlerFunc func(conn net.Conn, msg *Message) error

// MessageHandler IPC 消息处理函数（旧版兼容）
type MessageHandler func(msg *IPCMessage, conn interface{}) error

// Server IPC 服务器（Master 端）
type Server struct {
	socketPath string
	listener   net.Listener
	handler    MessageHandler

	// 新版消息处理器
	handlers map[string]MessageHandlerFunc
	handlerMu sync.RWMutex

	// 连接管理
	connections   map[int]*Codec    // workerID -> codec
	connByStrID   map[string]net.Conn // workerID(string) -> conn
	connMu        sync.RWMutex

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewServer 创建新的 IPC 服务器
func NewServer(socketPath string, handlerOrNil ...MessageHandler) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	var handler MessageHandler
	if len(handlerOrNil) > 0 {
		handler = handlerOrNil[0]
	}

	// 确保 socket 目录存在
	socketDir := filepath.Dir(socketPath)
	os.MkdirAll(socketDir, 0755)

	return &Server{
		socketPath:  socketPath,
		handler:     handler,
		handlers:    make(map[string]MessageHandlerFunc),
		connections: make(map[int]*Codec),
		connByStrID: make(map[string]net.Conn),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// RegisterHandler 注册消息处理器
func (s *Server) RegisterHandler(msgType string, handler MessageHandlerFunc) {
	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()
	s.handlers[msgType] = handler
}

// Start 启动 IPC 服务器
func (s *Server) Start() error {
	// 清理旧 socket 文件
	os.Remove(s.socketPath)

	// 创建 Unix Socket 监听器
	ln, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("listen unix socket failed: %w", err)
	}
	s.listener = ln

	// 设置 socket 权限（允许 Worker 进程连接）
	if err := os.Chmod(s.socketPath, 0666); err != nil {
		return fmt.Errorf("chmod socket failed: %w", err)
	}

	// 启动接受连接的 goroutine
	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// acceptLoop 接受连接循环
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				continue
			}
		}

		// 为每个连接启动处理 goroutine
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection 处理单个连接
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	codec := NewCodec(conn)
	var workerID int

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// 读取消息
		msg, err := codec.ReadMessage()
		if err != nil {
			// 连接断开时清理
			if workerID > 0 {
				s.removeConnection(workerID)
			}
			return
		}

		// 处理消息
		if err := s.handler(msg, conn); err != nil {
			continue
		}

		// 注册连接（Worker Ready 消息）
		if msg.Type == MsgTypeWorkerReady {
			var ready MsgWorkerReady
			if err := msg.DecodePayload(&ready); err == nil {
				workerID = ready.WorkerID
				s.addConnection(workerID, codec)
			}
		}
	}
}

// addConnection 添加连接
func (s *Server) addConnection(workerID int, codec *Codec) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	s.connections[workerID] = codec
}

// addConnectionByStrID 添加连接（字符串ID）
func (s *Server) addConnectionByStrID(workerID string, conn net.Conn) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	s.connByStrID[workerID] = conn
}

// removeConnection 移除连接
func (s *Server) removeConnection(workerID int) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	delete(s.connections, workerID)
}

// removeConnectionByStrID 移除连接（字符串ID）
func (s *Server) removeConnectionByStrID(workerID string) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	delete(s.connByStrID, workerID)
}

// GetConnection 获取 Worker 连接
func (s *Server) GetConnection(workerID int) *Codec {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return s.connections[workerID]
}

// GetConnectedWorkers 获取所有已连接的 Worker ID 列表
func (s *Server) GetConnectedWorkers() []string {
	s.connMu.RLock()
	defer s.connMu.RUnlock()

	workers := make([]string, 0, len(s.connections)+len(s.connByStrID))
	
	for id := range s.connections {
		workers = append(workers, fmt.Sprintf("worker-%d", id))
	}
	
	for id := range s.connByStrID {
		workers = append(workers, id)
	}
	
	return workers
}

// GetConnectedWorkersCount 获取已连接的 Worker 数量
func (s *Server) GetConnectedWorkersCount() int {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return len(s.connections) + len(s.connByStrID)
}

// SendMessage 向指定 Worker 发送消息
func (s *Server) SendMessage(workerID int, msg *IPCMessage) error {
	codec := s.GetConnection(workerID)
	if codec == nil {
		return fmt.Errorf("worker %d not connected", workerID)
	}
	return codec.WriteMessage(msg)
}

// SendMessageToWorker 向指定 Worker 发送新版消息（字符串ID）
func (s *Server) SendMessageToWorker(workerID string, msg *Message) error {
	s.connMu.RLock()
	conn, ok := s.connByStrID[workerID]
	s.connMu.RUnlock()
	
	if !ok {
		return fmt.Errorf("worker %s not connected", workerID)
	}
	
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	
	_, err = conn.Write(append(data, '\n'))
	return err
}

// Broadcast 向所有 Worker 广播消息
func (s *Server) Broadcast(msg *IPCMessage) error {
	s.connMu.RLock()
	defer s.connMu.RUnlock()

	var lastErr error
	for workerID, codec := range s.connections {
		if err := codec.WriteMessage(msg); err != nil {
			lastErr = fmt.Errorf("send to worker %d failed: %w", workerID, err)
		}
	}

	return lastErr
}

// BroadcastToWorkers 向所有 Worker 广播新版消息
func (s *Server) BroadcastToWorkers(msg *Message) error {
	s.connMu.RLock()
	defer s.connMu.RUnlock()

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	dataWithNewline := append(data, '\n')

	var lastErr error
	
	// 向所有 Codec 连接广播
	for workerID, codec := range s.connections {
		ipcMsg := &IPCMessage{
			Type:    msg.Type,
			Payload: data,
		}
		if err := codec.WriteMessage(ipcMsg); err != nil {
			lastErr = fmt.Errorf("send to worker %d failed: %w", workerID, err)
		}
	}
	
	// 向所有字符串 ID 连接广播
	for workerID, conn := range s.connByStrID {
		if _, err := conn.Write(dataWithNewline); err != nil {
			lastErr = fmt.Errorf("send to worker %s failed: %w", workerID, err)
		}
	}

	return lastErr
}

// Stop 停止服务器
func (s *Server) Stop() error {
	s.cancel()

	// 关闭监听器
	if s.listener != nil {
		s.listener.Close()
	}

	// 关闭所有连接
	s.connMu.Lock()
	for _, codec := range s.connections {
		codec.Close()
	}
	s.connections = make(map[int]*Codec)
	s.connMu.Unlock()

	// 等待所有 goroutine 退出
	s.wg.Wait()

	// 清理 socket 文件
	os.Remove(s.socketPath)

	return nil
}
