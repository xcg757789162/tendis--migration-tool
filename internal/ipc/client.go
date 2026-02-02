package ipc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// Client IPC 客户端（Worker 端）
type Client struct {
	serverAddr string
	codec      *Codec
	handler    MessageHandler
	
	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	
	// 重连配置
	reconnectInterval time.Duration
	maxReconnectDelay time.Duration
}

// NewClient 创建新的 IPC 客户端
func NewClient(serverAddr string, handler MessageHandler) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		serverAddr:        serverAddr,
		handler:           handler,
		ctx:               ctx,
		cancel:            cancel,
		reconnectInterval: 1 * time.Second,
		maxReconnectDelay: 30 * time.Second,
	}
}

// Connect 连接到服务器（带重连机制）
func (c *Client) Connect() error {
	delay := c.reconnectInterval
	
	for {
		conn, err := net.Dial("unix", c.serverAddr)
		if err == nil {
			c.mu.Lock()
			c.codec = NewCodec(conn)
			c.mu.Unlock()
			
			// 启动消息接收循环
			c.wg.Add(1)
			go c.receiveLoop()
			
			return nil
		}
		
		// 重连延迟
		select {
		case <-c.ctx.Done():
			return fmt.Errorf("connect cancelled")
		case <-time.After(delay):
			delay *= 2
			if delay > c.maxReconnectDelay {
				delay = c.maxReconnectDelay
			}
		}
	}
}

// receiveLoop 消息接收循环
func (c *Client) receiveLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		c.mu.Lock()
		codec := c.codec
		c.mu.Unlock()

		if codec == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 读取消息
		msg, err := codec.ReadMessage()
		if err != nil {
			// 连接断开，尝试重连
			c.mu.Lock()
			c.codec = nil
			c.mu.Unlock()
			
			go c.Connect() // 异步重连
			time.Sleep(c.reconnectInterval)
			continue
		}

		// 处理消息
		if c.handler != nil {
			if err := c.handler(msg, nil); err != nil {
				// 处理错误（可以记录日志）
			}
		}
	}
}

// Send 发送消息（线程安全）
func (c *Client) Send(msg *IPCMessage) error {
	c.mu.Lock()
	codec := c.codec
	c.mu.Unlock()

	if codec == nil {
		return fmt.Errorf("not connected to server")
	}

	return codec.WriteMessage(msg)
}

// SendWithRetry 发送消息（带重试）
func (c *Client) SendWithRetry(msg *IPCMessage, maxRetries int) error {
	var lastErr error
	
	for i := 0; i < maxRetries; i++ {
		if err := c.Send(msg); err == nil {
			return nil
		} else {
			lastErr = err
		}
		
		// 等待重连
		time.Sleep(c.reconnectInterval)
	}
	
	return fmt.Errorf("send failed after %d retries: %w", maxRetries, lastErr)
}

// Close 关闭客户端
func (c *Client) Close() error {
	c.cancel()
	
	c.mu.Lock()
	if c.codec != nil {
		c.codec.Close()
		c.codec = nil
	}
	c.mu.Unlock()
	
	c.wg.Wait()
	
	return nil
}

// IsConnected 检查连接状态
func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.codec != nil
}
