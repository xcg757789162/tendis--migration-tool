package ipc

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
)

// MessageHandler IPC 消息处理函数
type MessageHandler func(msg *IPCMessage, conn interface{}) error

// Server IPC 服务器（Master 端）
type Server struct {
	socketPath string
	listener   net.Listener
	handler    MessageHandler
	
	// 连接管理
	connections map[int]*Codec // workerID -> codec
	connMu      sync.RWMutex
	
	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewServer 创建新的 IPC 服务器
func NewServer(socketPath string, handler MessageHandler) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		socketPath:  socketPath,
		handler:     handler,
		connections: make(map[int]*Codec),
		ctx:         ctx,
		cancel:      cancel,
	}
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

// removeConnection 移除连接
func (s *Server) removeConnection(workerID int) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	delete(s.connections, workerID)
}

// GetConnection 获取 Worker 连接
func (s *Server) GetConnection(workerID int) *Codec {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return s.connections[workerID]
}

// SendMessage 向指定 Worker 发送消息
func (s *Server) SendMessage(workerID int, msg *IPCMessage) error {
	codec := s.GetConnection(workerID)
	if codec == nil {
		return fmt.Errorf("worker %d not connected", workerID)
	}
	return codec.WriteMessage(msg)
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
