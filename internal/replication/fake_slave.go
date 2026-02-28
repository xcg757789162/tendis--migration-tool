// Package replication 实现伪装成 Tendis Slave 接收 Binlog 的增量同步
//
// 基于官方 Tendis 2.7.0 源码分析实现：
// - 参考 spov.cpp 中的 slaveChkSyncStatus() 方法
// - 参考 mpov.cpp 中的 registerIncrSync() 方法
// - 参考 repl.cpp 中的 applybinlogsv2 命令处理
//
// 协议流程：
// 1. Slave -> Master: INCRSYNC storeId dstStoreId binlogPos ip port
// 2. Master -> Slave: +OK
// 3. Slave -> Master: +PONG
// 4. Master -> Slave: applybinlogsv2 storeId binlogs cnt flag (持续推送)
// 5. Master -> Slave: binlog_heartbeat storeId timestamp (心跳保活)
package replication

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

// FakeSlaveConfig 伪 Slave 配置
type FakeSlaveConfig struct {
	// 源端 Tendis 地址
	SourceAddr string
	// 源端密码（如果有）
	SourcePassword string
	// 存储 ID（Tendis 集群中的 store ID，通常 0-9）
	StoreID uint32
	// 起始 binlog 位置（0 表示从头开始）
	StartBinlogPos uint64
	// 伪装的监听 IP（可以是任意值，Master 不会真正连接）
	FakeListenIP string
	// 伪装的监听端口
	FakeListenPort uint16
	// 读取超时
	ReadTimeout time.Duration
	// 心跳超时（超过此时间没收到数据则重连）
	HeartbeatTimeout time.Duration
	// Key 过滤函数（返回 true 表示需要迁移）
	KeyFilter func(key string) bool

	// ===== 新增：缓存模式配置 =====
	// CacheMode 是否启用缓存模式（全量阶段缓存 binlog 到本地文件）
	CacheMode bool
	// CacheManager 缓存管理器（缓存模式必须设置）
	CacheManager *BinlogCacheManager
}

// errorWrapper 包装 error 用于 atomic.Value 存储
// atomic.Value 要求每次 Store 的值类型完全一致，
// 但 error 是接口，不同实现类型（*net.OpError, *errors.errorString 等）会导致 panic
type errorWrapper struct {
	err error
}

// BinlogEntry 表示一个 binlog 条目
type BinlogEntry struct {
	// Binlog ID
	BinlogID uint64
	// 时间戳（毫秒）
	Timestamp uint64
	// 操作类型
	OpType string
	// Key
	Key string
	// Value（序列化后的数据）
	Value []byte
	// TTL（毫秒，0 表示无过期）
	TTL int64
}

// BinlogHandler binlog 处理回调
type BinlogHandler func(entries []BinlogEntry) error

// FakeSlave 伪装成 Tendis Slave 接收 binlog
type FakeSlave struct {
	config       FakeSlaveConfig
	conn         net.Conn
	reader       *bufio.Reader
	writer       *bufio.Writer
	handler      BinlogHandler
	targetClient redis.UniversalClient
	parser       *BinlogParser

	// 当前 binlog 位置
	currentBinlogPos atomic.Uint64
	// 最后收到数据的时间
	lastRecvTime atomic.Int64
	// 运行状态
	running atomic.Bool
	// 停止信号
	stopCh   chan struct{}
	stopOnce sync.Once // 【BUG-FIX】防止多次 close(stopCh) panic
	// 错误信息
	lastError atomic.Value

	// ===== 新增：连接就绪通知 =====
	// connectedCh 连接成功后通知（用于等待连接就绪）
	connectedCh chan struct{}
	// connected 是否已连接成功
	connected atomic.Bool
	// connectionError 连接错误（如果连接失败）
	connectionError atomic.Value

	// 统计信息
	stats struct {
		totalBinlogs    atomic.Int64
		appliedBinlogs  atomic.Int64
		filteredBinlogs atomic.Int64
		cachedBinlogs   atomic.Int64 // 新增：缓存的 binlog 数量
		errors          atomic.Int64
		heartbeats      atomic.Int64
		reconnects      atomic.Int64
	}

	mu sync.Mutex
}

// NewFakeSlave 创建伪 Slave 实例
func NewFakeSlave(config FakeSlaveConfig, targetClient redis.UniversalClient) *FakeSlave {
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 30 * time.Second
	}
	if config.HeartbeatTimeout == 0 {
		config.HeartbeatTimeout = 30 * time.Second
	}
	if config.FakeListenIP == "" {
		config.FakeListenIP = "127.0.0.1"
	}
	if config.FakeListenPort == 0 {
		config.FakeListenPort = 6379
	}

	fs := &FakeSlave{
		config:       config,
		targetClient: targetClient,
		parser:       NewBinlogParser(),
		stopCh:       make(chan struct{}),
		connectedCh:  make(chan struct{}),
	}
	fs.currentBinlogPos.Store(config.StartBinlogPos)
	return fs
}

// SetBinlogHandler 设置 binlog 处理回调
func (fs *FakeSlave) SetBinlogHandler(handler BinlogHandler) {
	fs.handler = handler
}

// Start 启动伪 Slave，开始接收 binlog
func (fs *FakeSlave) Start(ctx context.Context) error {
	if fs.running.Load() {
		return fmt.Errorf("fake slave already running")
	}

	fs.running.Store(true)
	defer fs.running.Store(false)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[FakeSlave] Context cancelled, stopping...")
			return ctx.Err()
		case <-fs.stopCh:
			log.Printf("[FakeSlave] Stop signal received, stopping...")
			return nil
		default:
		}

		// 建立连接并运行
		if err := fs.connectAndRun(ctx); err != nil {
			fs.lastError.Store(&errorWrapper{err: err})
			fs.stats.errors.Add(1)
			log.Printf("[FakeSlave] Connection error: %v, will retry in 3 seconds...", err)

			// 等待重试
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-fs.stopCh:
				return nil
			case <-time.After(3 * time.Second):
				fs.stats.reconnects.Add(1)
			}
		}
	}
}

// Stop 停止伪 Slave（安全：支持多次调用，不会 panic）
func (fs *FakeSlave) Stop() {
	// 【BUG-FIX】使用 sync.Once 防止多次 close(stopCh) 导致 panic
	// 场景：autoStopTask 关闭 task.stopCh → simulateProgress 退出时又调用 fs.Stop()
	// 如果 panic，defer sourceClient.Close() 被跳过，导致连接泄漏
	fs.stopOnce.Do(func() {
		close(fs.stopCh)
	})
	fs.mu.Lock()
	if fs.conn != nil {
		fs.conn.Close()
		fs.conn = nil
	}
	fs.mu.Unlock()
}

// GetCurrentBinlogPos 获取当前 binlog 位置
func (fs *FakeSlave) GetCurrentBinlogPos() uint64 {
	return fs.currentBinlogPos.Load()
}

// GetStats 获取统计信息
func (fs *FakeSlave) GetStats() map[string]int64 {
	return map[string]int64{
		"total_binlogs":    fs.stats.totalBinlogs.Load(),
		"applied_binlogs":  fs.stats.appliedBinlogs.Load(),
		"filtered_binlogs": fs.stats.filteredBinlogs.Load(),
		"cached_binlogs":   fs.stats.cachedBinlogs.Load(),
		"errors":           fs.stats.errors.Load(),
		"heartbeats":       fs.stats.heartbeats.Load(),
		"reconnects":       fs.stats.reconnects.Load(),
	}
}

// WaitConnected 等待连接成功（用于确保连接就绪后再开始全量迁移）
// 返回 nil 表示连接成功，返回 error 表示连接失败
func (fs *FakeSlave) WaitConnected(timeout time.Duration) error {
	select {
	case <-fs.connectedCh:
		// 检查是否有连接错误
		if v := fs.connectionError.Load(); v != nil {
			return v.(*errorWrapper).err
		}
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("wait connected timeout after %v", timeout)
	case <-fs.stopCh:
		return fmt.Errorf("fake slave stopped")
	}
}

// IsConnected 是否已连接成功
func (fs *FakeSlave) IsConnected() bool {
	return fs.connected.Load()
}

// connectAndRun 建立连接并运行增量同步
func (fs *FakeSlave) connectAndRun(ctx context.Context) error {
	// 1. 建立 TCP 连接
	conn, err := net.DialTimeout("tcp", fs.config.SourceAddr, 10*time.Second)
	if err != nil {
		// 连接失败，记录错误并通知等待者
		connErr := fmt.Errorf("connect to %s failed: %w", fs.config.SourceAddr, err)
		if !fs.connected.Load() {
			fs.connectionError.Store(&errorWrapper{err: connErr})
			close(fs.connectedCh) // 通知等待者连接失败
		}
		return connErr
	}

	fs.mu.Lock()
	fs.conn = conn
	fs.reader = bufio.NewReader(conn)
	fs.writer = bufio.NewWriter(conn)
	fs.mu.Unlock()

	defer func() {
		fs.mu.Lock()
		if fs.conn != nil {
			fs.conn.Close()
			fs.conn = nil
		}
		fs.mu.Unlock()
	}()

	log.Printf("[FakeSlave] Connected to %s", fs.config.SourceAddr)

	// 2. 如果有密码，先认证
	if fs.config.SourcePassword != "" {
		if err := fs.authenticate(); err != nil {
			authErr := fmt.Errorf("auth failed: %w", err)
			if !fs.connected.Load() {
				fs.connectionError.Store(&errorWrapper{err: authErr})
				close(fs.connectedCh)
			}
			return authErr
		}
		log.Printf("[FakeSlave] Authentication successful")
	}

	// 3. 发送 INCRSYNC 命令进行注册
	if err := fs.sendIncrSync(); err != nil {
		syncErr := fmt.Errorf("INCRSYNC failed: %w", err)
		if !fs.connected.Load() {
			fs.connectionError.Store(&errorWrapper{err: syncErr})
			close(fs.connectedCh)
		}
		return syncErr
	}
	log.Printf("[FakeSlave] INCRSYNC handshake successful, binlogPos=%d", fs.currentBinlogPos.Load())

	// 4. 标记连接成功，通知等待者
	if !fs.connected.Swap(true) {
		close(fs.connectedCh) // 首次连接成功，通知等待者
		log.Printf("[FakeSlave] Connection ready, notifying waiters")
	}

	// 5. 进入主循环，接收并处理 binlog
	return fs.receiveLoop(ctx)
}

// authenticate 发送 AUTH 命令
func (fs *FakeSlave) authenticate() error {
	cmd := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n",
		len(fs.config.SourcePassword), fs.config.SourcePassword)

	if _, err := fs.writer.WriteString(cmd); err != nil {
		return err
	}
	if err := fs.writer.Flush(); err != nil {
		return err
	}

	// 读取响应
	response, err := fs.readLine()
	if err != nil {
		return err
	}
	if !strings.HasPrefix(response, "+") {
		return fmt.Errorf("AUTH failed: %s", response)
	}
	return nil
}

// sendIncrSync 发送 INCRSYNC 命令注册为 Slave
// 协议：INCRSYNC storeId dstStoreId binlogPos ip port
func (fs *FakeSlave) sendIncrSync() error {
	binlogPos := fs.currentBinlogPos.Load()

	// 构造 INCRSYNC 命令
	// 格式：INCRSYNC <storeId> <dstStoreId> <binlogPos> <ip> <port>
	cmd := fmt.Sprintf("INCRSYNC %d %d %d %s %d",
		fs.config.StoreID,
		fs.config.StoreID, // dstStoreId 通常与 storeId 相同
		binlogPos,
		fs.config.FakeListenIP,
		fs.config.FakeListenPort,
	)

	// 以 RESP 协议格式发送
	parts := strings.Fields(cmd)
	respCmd := fmt.Sprintf("*%d\r\n", len(parts))
	for _, part := range parts {
		respCmd += fmt.Sprintf("$%d\r\n%s\r\n", len(part), part)
	}

	if _, err := fs.writer.WriteString(respCmd); err != nil {
		return fmt.Errorf("write INCRSYNC failed: %w", err)
	}
	if err := fs.writer.Flush(); err != nil {
		return fmt.Errorf("flush INCRSYNC failed: %w", err)
	}

	// 等待 +OK 响应
	response, err := fs.readLine()
	if err != nil {
		return fmt.Errorf("read INCRSYNC response failed: %w", err)
	}
	if !strings.HasPrefix(response, "+") {
		return fmt.Errorf("INCRSYNC failed: %s", response)
	}

	// 发送 +PONG 确认
	if _, err := fs.writer.WriteString("+PONG\r\n"); err != nil {
		return fmt.Errorf("write PONG failed: %w", err)
	}
	if err := fs.writer.Flush(); err != nil {
		return fmt.Errorf("flush PONG failed: %w", err)
	}

	return nil
}

// receiveLoop 主循环：接收并处理 binlog
func (fs *FakeSlave) receiveLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-fs.stopCh:
			return nil
		default:
		}

		// 设置读取超时
		fs.conn.SetReadDeadline(time.Now().Add(fs.config.ReadTimeout))

		// 读取并解析 RESP 命令
		cmd, args, err := fs.readCommand()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// 检查心跳超时
				lastRecv := time.Unix(0, fs.lastRecvTime.Load())
				if time.Since(lastRecv) > fs.config.HeartbeatTimeout {
					return fmt.Errorf("heartbeat timeout")
				}
				continue
			}
			return fmt.Errorf("read command failed: %w", err)
		}

		fs.lastRecvTime.Store(time.Now().UnixNano())

		// 处理命令
		if err := fs.handleCommand(ctx, cmd, args); err != nil {
			log.Printf("[FakeSlave] Handle command %s failed: %v", cmd, err)
			fs.stats.errors.Add(1)
		}
	}
}

// readLine 读取一行
func (fs *FakeSlave) readLine() (string, error) {
	line, err := fs.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// readCommand 读取 RESP 格式的命令
func (fs *FakeSlave) readCommand() (string, []string, error) {
	// 读取第一个字符，判断类型
	firstByte, err := fs.reader.ReadByte()
	if err != nil {
		return "", nil, err
	}

	switch firstByte {
	case '*': // 数组（多批量回复）
		return fs.readArrayCommand()
	case '+': // 简单字符串
		line, err := fs.readLine()
		if err != nil {
			return "", nil, err
		}
		return line, nil, nil
	case '-': // 错误
		line, err := fs.readLine()
		if err != nil {
			return "", nil, err
		}
		return "", nil, fmt.Errorf("server error: %s", line)
	case '$': // 批量字符串
		fs.reader.UnreadByte()
		data, err := fs.readBulkString()
		if err != nil {
			return "", nil, err
		}
		return string(data), nil, nil
	default:
		// 可能是内联命令
		fs.reader.UnreadByte()
		line, err := fs.readLine()
		if err != nil {
			return "", nil, err
		}
		parts := strings.Fields(line)
		if len(parts) == 0 {
			return "", nil, fmt.Errorf("empty command")
		}
		return strings.ToUpper(parts[0]), parts[1:], nil
	}
}

// readArrayCommand 读取数组格式的命令
func (fs *FakeSlave) readArrayCommand() (string, []string, error) {
	// 读取数组长度
	countStr, err := fs.readLine()
	if err != nil {
		return "", nil, err
	}
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return "", nil, fmt.Errorf("invalid array count: %s", countStr)
	}
	if count <= 0 {
		return "", nil, fmt.Errorf("empty array")
	}

	// 读取每个元素
	args := make([]string, count)
	for i := 0; i < count; i++ {
		data, err := fs.readBulkString()
		if err != nil {
			return "", nil, fmt.Errorf("read array element %d failed: %w", i, err)
		}
		args[i] = string(data)
	}

	if len(args) == 0 {
		return "", nil, fmt.Errorf("empty command")
	}
	return strings.ToUpper(args[0]), args[1:], nil
}

// readBulkString 读取批量字符串
func (fs *FakeSlave) readBulkString() ([]byte, error) {
	// 读取 $
	firstByte, err := fs.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if firstByte != '$' {
		return nil, fmt.Errorf("expected '$', got '%c'", firstByte)
	}

	// 读取长度
	lenStr, err := fs.readLine()
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(lenStr)
	if err != nil {
		return nil, fmt.Errorf("invalid bulk string length: %s", lenStr)
	}
	if length < 0 {
		return nil, nil // nil bulk string
	}

	// 读取数据
	data := make([]byte, length)
	if _, err := io.ReadFull(fs.reader, data); err != nil {
		return nil, fmt.Errorf("read bulk string data failed: %w", err)
	}

	// 读取 \r\n
	if _, err := fs.reader.ReadString('\n'); err != nil {
		return nil, fmt.Errorf("read bulk string trailer failed: %w", err)
	}

	return data, nil
}

// handleCommand 处理接收到的命令
func (fs *FakeSlave) handleCommand(ctx context.Context, cmd string, args []string) error {
	switch cmd {
	case "APPLYBINLOGSV2":
		err := fs.handleApplyBinlogsV2(ctx, args)
		// 【关键】无论成功失败，都要发送响应给 Master
		// Master 在发送 applybinlogsv2 后会等待 +OK 响应（超时时间 timeoutSecBinlogWaitRsp，默认3秒）
		if err != nil {
			// 发送错误响应
			fs.writer.WriteString(fmt.Sprintf("-ERR %s\r\n", err.Error()))
			fs.writer.Flush()
			return err
		}
		// 发送成功响应
		fs.writer.WriteString("+OK\r\n")
		fs.writer.Flush()
		return nil
	case "BINLOG_HEARTBEAT":
		err := fs.handleBinlogHeartbeat(args)
		// 【关键】心跳也需要响应 +OK
		if err != nil {
			fs.writer.WriteString(fmt.Sprintf("-ERR %s\r\n", err.Error()))
			fs.writer.Flush()
			return err
		}
		fs.writer.WriteString("+OK\r\n")
		fs.writer.Flush()
		return nil
	case "PING":
		// 响应 PING
		fs.writer.WriteString("+PONG\r\n")
		fs.writer.Flush()
		return nil
	default:
		log.Printf("[FakeSlave] Unknown command: %s %v", cmd, args)
		// 对于未知命令也发送 +OK 以保持连接
		fs.writer.WriteString("+OK\r\n")
		fs.writer.Flush()
		return nil
	}
}

// handleApplyBinlogsV2 处理 applybinlogsv2 命令
// 格式：applybinlogsv2 storeId binlogs cnt flag
func (fs *FakeSlave) handleApplyBinlogsV2(ctx context.Context, args []string) error {
	if len(args) < 4 {
		return fmt.Errorf("applybinlogsv2 requires at least 4 arguments, got %d", len(args))
	}

	storeID, _ := strconv.ParseUint(args[0], 10, 32)
	binlogsData := args[1]
	count, _ := strconv.Atoi(args[2])
	// flag := args[3] // 暂时不用

	log.Printf("[FakeSlave] Received applybinlogsv2: storeId=%d, count=%d, dataLen=%d",
		storeID, count, len(binlogsData))

	fs.stats.totalBinlogs.Add(int64(count))

	// ===== 缓存模式：原样存储到本地文件 =====
	if fs.config.CacheMode && fs.config.CacheManager != nil && fs.config.CacheManager.IsCaching() {
		// 原样存储 binlog 数据到缓存文件
		if err := fs.config.CacheManager.WriteBinlog(uint32(storeID), []byte(binlogsData)); err != nil {
			log.Printf("[FakeSlave] Cache binlog failed: %v", err)
			return fmt.Errorf("cache binlog failed: %w", err)
		}
		fs.stats.cachedBinlogs.Add(int64(count))
		log.Printf("[FakeSlave] Cached %d binlogs to local file (storeID=%d)", count, storeID)
		return nil
	}

	// ===== 非缓存模式：直接解析并应用 =====
	// 解析 binlog 数据
	entries, err := fs.parseBinlogs([]byte(binlogsData), count)
	if err != nil {
		return fmt.Errorf("parse binlogs failed: %w", err)
	}

	// 过滤并应用 binlog
	var filteredEntries []BinlogEntry
	for _, entry := range entries {
		// 更新 binlog 位置
		if entry.BinlogID > fs.currentBinlogPos.Load() {
			fs.currentBinlogPos.Store(entry.BinlogID)
		}

		// Key 过滤
		// 对于 CMD 类型，需要从 Value（命令字符串）中提取 Key
		keyToFilter := entry.Key
		if entry.OpType == "CMD" && entry.Key == "" {
			// 从命令中提取 Key
			args := parseRedisCommand(string(entry.Value))
			if len(args) >= 2 {
				keyToFilter = args[1] // 大多数命令的第二个参数是 Key
			}
		}

		if fs.config.KeyFilter != nil && keyToFilter != "" && !fs.config.KeyFilter(keyToFilter) {
			fs.stats.filteredBinlogs.Add(1)
			log.Printf("[FakeSlave] Filtered entry: opType=%s, key=%q", entry.OpType, keyToFilter)
			continue
		}

		filteredEntries = append(filteredEntries, entry)
	}

	// 应用到目标端
	if len(filteredEntries) > 0 {
		if err := fs.applyBinlogsToTarget(ctx, filteredEntries); err != nil {
			return fmt.Errorf("apply binlogs to target failed: %w", err)
		}
		fs.stats.appliedBinlogs.Add(int64(len(filteredEntries)))
	}

	return nil
}

// handleBinlogHeartbeat 处理心跳
// 格式：binlog_heartbeat storeId timestamp
func (fs *FakeSlave) handleBinlogHeartbeat(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("binlog_heartbeat requires 2 arguments")
	}

	// storeID := args[0]
	// timestamp := args[1]

	fs.stats.heartbeats.Add(1)
	return nil
}

// parseBinlogs 解析 binlog 数据
// 使用 Tendis 官方 binlog 格式解析
func (fs *FakeSlave) parseBinlogs(data []byte, count int) ([]BinlogEntry, error) {
	// 使用 binlog 解析器解析数据
	parsed, err := fs.parser.ParseBinlogs(data, count)
	if err != nil {
		return nil, fmt.Errorf("parse binlog data failed: %w", err)
	}

	var entries []BinlogEntry

	for _, binlog := range parsed {
		// 调试日志：显示解析结果
		log.Printf("[FakeSlave] Parsed binlog: binlogID=%d, cmdStr=%q, entriesCount=%d",
			binlog.BinlogID, binlog.CmdStr, len(binlog.Entries))

		// 【重要】Tendis binlog 包含两部分：
		// 1. cmdStr：原始 Redis 命令字符串（RESP 格式或命令名）
		// 2. entries：RocksDB 格式的 KV 操作
		//
		// 同步策略：
		// - 如果 cmdStr 是 RESP 格式（以 '*' 开头），优先使用它直接执行命令
		// - 否则使用 entries（但 entries 是 RocksDB 格式，需要特殊处理）

		// 检查 cmdStr 是否是 RESP 格式
		if binlog.CmdStr != "" && len(binlog.CmdStr) > 0 && binlog.CmdStr[0] == '*' {
			// RESP 格式的完整命令，可以直接执行
			log.Printf("[FakeSlave] Using cmdStr mode (RESP format): %q", binlog.CmdStr)
			entries = append(entries, BinlogEntry{
				BinlogID:  binlog.BinlogID,
				Timestamp: binlog.Timestamp,
				OpType:    "CMD",
				Key:       "", // 从命令中解析
				Value:     []byte(binlog.CmdStr),
				TTL:       0,
			})
		} else if len(binlog.Entries) > 0 {
			// 使用 entries（RocksDB 格式）
			// 注意：entry.Key 和 entry.Value 是 RocksDB RecordKey/RecordValue 格式
			for _, entry := range binlog.Entries {
				log.Printf("[FakeSlave] Using entry mode: op=%s, keyLen=%d, valueLen=%d",
					entry.Op.String(), len(entry.Key), len(entry.Value))
				entries = append(entries, BinlogEntry{
					BinlogID:  binlog.BinlogID,
					Timestamp: entry.Timestamp,
					OpType:    entry.Op.String(),
					Key:       entry.Key,
					Value:     entry.Value,
					TTL:       0,
				})
			}
		} else if binlog.CmdStr != "" {
			// cmdStr 不是 RESP 格式（只是命令名），记录警告
			log.Printf("[FakeSlave] Warning: cmdStr is not RESP format, cannot execute: %q", binlog.CmdStr)
		}
	}

	log.Printf("[FakeSlave] parseBinlogs: parsed %d binlogs, got %d entries", len(parsed), len(entries))
	return entries, nil
}

// applyBinlogsToTarget 将 binlog 应用到目标端
func (fs *FakeSlave) applyBinlogsToTarget(ctx context.Context, entries []BinlogEntry) error {
	if fs.targetClient == nil {
		log.Printf("[FakeSlave] Target client not set, skipping apply")
		return nil
	}

	// 如果设置了自定义 handler，使用它
	if fs.handler != nil {
		return fs.handler(entries)
	}

	// 使用 Pipeline 批量执行
	pipe := fs.targetClient.Pipeline()
	var cmdCount int

	for _, entry := range entries {
		switch entry.OpType {
		case "CMD":
			// CMD 类型：Value 包含原始 Redis 命令字符串
			// 格式类似：SET key value 或 HSET key field value
			cmdStr := string(entry.Value)
			args := parseRedisCommand(cmdStr)
			if len(args) > 0 {
				log.Printf("[FakeSlave] Executing command: %v", args)
				// 将字符串数组转为 interface{} 数组
				iargs := make([]interface{}, len(args))
				for i, v := range args {
					iargs[i] = v
				}
				pipe.Do(ctx, iargs...)
				cmdCount++
			}
		case "DEL", "UNLINK", "EXPIRED", "EVICTED":
			if entry.Key != "" {
				pipe.Del(ctx, entry.Key)
				cmdCount++
			}
		case "SET":
			// SET 操作：entry 格式为 Key + Value (RocksDB 格式)
			// 注意：RocksDB Value 不是 DUMP 格式，需要特殊处理
			// 暂时跳过，优先使用 CMD 模式
			log.Printf("[FakeSlave] SET entry (RocksDB format) - key=%q, valueLen=%d, skipping (use CMD mode)",
				entry.Key, len(entry.Value))
		case "TTL":
			// 【BUG-FIX TTL 一致性】TTL 设置操作（EXPIRE/PEXPIRE 等）
			// Tendis binlog 中 EXPIRE 等命令产生 ReplOpTTL(4) 类型的 entry
			// 需要由 handler（processBinlogEntries）从源端获取 PTTL 并设置
			log.Printf("[FakeSlave] TTL entry - key=%q, skipping (handled by binlog handler)", entry.Key)
		case "TTLDEL":
			// 【BUG-FIX TTL 一致性】TTL 删除操作（PERSIST 命令）
			// Tendis binlog 中 PERSIST 命令产生 ReplOpTTLDel(5) 类型的 entry
			if entry.Key != "" {
				pipe.Persist(ctx, entry.Key)
				cmdCount++
			}
		default:
			// 未知操作类型
			log.Printf("[FakeSlave] Unknown OpType: %q, key=%q", entry.OpType, entry.Key)
		}
	}

	if cmdCount > 0 {
		results, err := pipe.Exec(ctx)
		if err != nil {
			// 检查是否是部分失败
			log.Printf("[FakeSlave] Pipeline exec error: %v", err)
			for i, result := range results {
				if result.Err() != nil {
					log.Printf("[FakeSlave] Command %d failed: %v", i, result.Err())
				}
			}
			return err
		}
		log.Printf("[FakeSlave] Successfully executed %d commands", cmdCount)
	}

	return nil
}

// parseRedisCommand 解析 Redis 命令字符串
// Tendis cmdStr 格式类似 RESP 但更简单
// 格式：cmd\r\nkey\r\nvalue\r\n... 或者简单的空格分隔
func parseRedisCommand(cmdStr string) []string {
	if cmdStr == "" {
		log.Printf("[FakeSlave] parseRedisCommand: empty cmdStr")
		return nil
	}

	log.Printf("[FakeSlave] parseRedisCommand: input len=%d, first char=%d", len(cmdStr), cmdStr[0])

	// Tendis cmdStr 可能是 RESP 格式或简单格式
	// 首先尝试 RESP 格式解析
	if len(cmdStr) > 0 && cmdStr[0] == '*' {
		args := parseRESPCommand(cmdStr)
		log.Printf("[FakeSlave] parseRESPCommand result: %v", args)
		return args
	}

	// 简单格式：按空格分隔（需要处理引号内的空格）
	args := parseSimpleCommand(cmdStr)
	log.Printf("[FakeSlave] parseSimpleCommand result: %v", args)
	return args
}

// parseRESPCommand 解析 RESP 格式的命令
// 格式：*N\r\n$len1\r\narg1\r\n$len2\r\narg2\r\n...
// 【重要】正确处理二进制数据，不能使用 strings.Split，必须根据长度读取
func parseRESPCommand(cmdStr string) []string {
	if len(cmdStr) == 0 || cmdStr[0] != '*' {
		return nil
	}

	data := []byte(cmdStr)
	offset := 1 // 跳过 '*'

	// 读取数组长度
	argCountEnd := offset
	for argCountEnd < len(data) && data[argCountEnd] != '\r' {
		argCountEnd++
	}
	if argCountEnd >= len(data)-1 {
		return nil
	}
	argCount, err := strconv.Atoi(string(data[offset:argCountEnd]))
	if err != nil || argCount <= 0 {
		return nil
	}
	offset = argCountEnd + 2 // 跳过 "\r\n"

	var args []string
	for j := 0; j < argCount && offset < len(data); j++ {
		// 期望 '$'
		if data[offset] != '$' {
			break
		}
		offset++ // 跳过 '$'

		// 读取长度
		lenEnd := offset
		for lenEnd < len(data) && data[lenEnd] != '\r' {
			lenEnd++
		}
		if lenEnd >= len(data)-1 {
			break
		}
		argLen, err := strconv.Atoi(string(data[offset:lenEnd]))
		if err != nil || argLen < 0 {
			break
		}
		offset = lenEnd + 2 // 跳过 "\r\n"

		// 读取实际参数（根据长度，正确处理二进制数据）
		if offset+argLen > len(data) {
			break
		}
		args = append(args, string(data[offset:offset+argLen]))
		offset += argLen

		// 跳过参数后的 "\r\n"
		if offset+2 <= len(data) && data[offset] == '\r' && data[offset+1] == '\n' {
			offset += 2
		}
	}

	return args
}

// parseSimpleCommand 解析简单格式的命令（空格分隔）
func parseSimpleCommand(cmdStr string) []string {
	// 简单的按空格分隔，暂不处理引号
	fields := strings.Fields(cmdStr)
	return fields
}

// ReplayCachedBinlogs 回放缓存的 binlog 数据
// 在全量迁移完成后调用，将缓存文件中的 binlog 应用到目标端
func (fs *FakeSlave) ReplayCachedBinlogs(ctx context.Context, cacheConfig BinlogCacheConfig) error {
	reader, err := NewBinlogCacheReader(cacheConfig, fs.config.StoreID)
	if err != nil {
		return fmt.Errorf("create cache reader failed: %w", err)
	}
	defer reader.Close()

	log.Printf("[FakeSlave] Starting to replay cached binlogs for storeID=%d", fs.config.StoreID)

	var replayedCount, errorCount int64
	batchSize := 100
	var batch []BinlogEntry

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 读取缓存的 binlog 数据
		data, err := reader.Read()
		if err == io.EOF {
			break // 读取完毕
		}
		if err != nil {
			log.Printf("[FakeSlave] Read cached binlog failed: %v", err)
			errorCount++
			continue
		}

		// 解析 binlog 数据（与实时接收相同的格式）
		// 注意：缓存的是原始的 binlogsData，需要解析
		entries, err := fs.parser.ParseBinlogs(data, 0) // 0 表示解析所有
		if err != nil {
			log.Printf("[FakeSlave] Parse cached binlog failed: %v", err)
			errorCount++
			continue
		}

		// 转换为 BinlogEntry 并过滤
		for _, binlog := range entries {
			if len(binlog.Entries) > 0 {
				for _, entry := range binlog.Entries {
					// Key 过滤
					if fs.config.KeyFilter != nil && !fs.config.KeyFilter(entry.Key) {
						continue
					}

					batch = append(batch, BinlogEntry{
						BinlogID:  binlog.BinlogID,
						Timestamp: entry.Timestamp,
						OpType:    entry.Op.String(),
						Key:       entry.Key,
						Value:     entry.Value,
						TTL:       0,
					})
				}
			} else if binlog.CmdStr != "" {
				batch = append(batch, BinlogEntry{
					BinlogID:  binlog.BinlogID,
					Timestamp: binlog.Timestamp,
					OpType:    "CMD",
					Key:       "",
					Value:     []byte(binlog.CmdStr),
					TTL:       0,
				})
			}
		}

		// 批量应用
		if len(batch) >= batchSize {
			if err := fs.applyBinlogsToTarget(ctx, batch); err != nil {
				log.Printf("[FakeSlave] Apply cached binlogs failed: %v", err)
				errorCount++
			} else {
				replayedCount += int64(len(batch))
			}
			batch = batch[:0]
		}
	}

	// 处理剩余的 batch
	if len(batch) > 0 {
		if err := fs.applyBinlogsToTarget(ctx, batch); err != nil {
			log.Printf("[FakeSlave] Apply remaining cached binlogs failed: %v", err)
			errorCount++
		} else {
			replayedCount += int64(len(batch))
		}
	}

	stats := reader.GetStats()
	log.Printf("[FakeSlave] Replay completed for storeID=%d: replayed=%d, errors=%d, cache_records=%d",
		fs.config.StoreID, replayedCount, errorCount, stats["total_records"])

	return nil
}
