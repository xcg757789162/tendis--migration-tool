package ipc

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
)

// Codec IPC 消息编解码器（长度前缀协议）
type Codec struct {
	conn net.Conn
	mu   sync.Mutex
	reader *bufio.Reader
}

// NewCodec 创建新的编解码器
func NewCodec(conn net.Conn) *Codec {
	return &Codec{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}
}

// WriteMessage 写入消息（线程安全）
// 格式: [4字节长度(BigEndian)][JSON消息体]
func (c *Codec) WriteMessage(msg *IPCMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 序列化消息
	msgData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message failed: %w", err)
	}

	// 写入长度前缀（4字节）
	msgLen := uint32(len(msgData))
	if err := binary.Write(c.conn, binary.BigEndian, msgLen); err != nil {
		return fmt.Errorf("write length prefix failed: %w", err)
	}

	// 写入消息体
	if _, err := c.conn.Write(msgData); err != nil {
		return fmt.Errorf("write message body failed: %w", err)
	}

	return nil
}

// ReadMessage 读取消息
func (c *Codec) ReadMessage() (*IPCMessage, error) {
	// 读取长度前缀（4字节）
	var msgLen uint32
	if err := binary.Read(c.reader, binary.BigEndian, &msgLen); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read length prefix failed: %w", err)
	}

	// 防止恶意大消息
	if msgLen > 10*1024*1024 { // 10MB 上限
		return nil, fmt.Errorf("message too large: %d bytes", msgLen)
	}

	// 读取消息体
	msgData := make([]byte, msgLen)
	if _, err := io.ReadFull(c.reader, msgData); err != nil {
		return nil, fmt.Errorf("read message body failed: %w", err)
	}

	// 反序列化消息
	var msg IPCMessage
	if err := json.Unmarshal(msgData, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal message failed: %w", err)
	}

	return &msg, nil
}

// Close 关闭连接
func (c *Codec) Close() error {
	return c.conn.Close()
}
