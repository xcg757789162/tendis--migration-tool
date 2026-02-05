package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// Command Redis 命令
type Command struct {
	Name string   // 命令名称 (SET, DEL, HSET等)
	Args []string // 命令参数
	Key  string   // 主Key
	TTL  int64    // TTL (毫秒), -1表示永不过期, 0表示未设置
}

// CommandConverter Binlog 记录到 Redis 命令的转换器
type CommandConverter struct {
	// 前缀过滤
	includePrefixes []string
	excludePrefixes []string
}

// NewCommandConverter 创建命令转换器
func NewCommandConverter(includePrefixes, excludePrefixes []string) *CommandConverter {
	return &CommandConverter{
		includePrefixes: includePrefixes,
		excludePrefixes: excludePrefixes,
	}
}

// ShouldProcess 检查 Key 是否应该被处理
func (c *CommandConverter) ShouldProcess(key string) bool {
	// 检查排除前缀
	for _, prefix := range c.excludePrefixes {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}

	// 如果没有指定包含前缀，则处理所有未排除的 Key
	if len(c.includePrefixes) == 0 {
		return true
	}

	// 检查包含前缀
	for _, prefix := range c.includePrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}

	return false
}

// ConvertRecord 将 Binlog 记录转换为 Redis 命令列表
func (c *CommandConverter) ConvertRecord(record *BinlogRecord) ([]*Command, error) {
	if record == nil || record.Value == nil {
		return nil, nil
	}

	commands := make([]*Command, 0, len(record.Value.Entries))

	for _, entry := range record.Value.Entries {
		cmd, err := c.convertEntry(entry)
		if err != nil {
			// 记录错误但继续处理
			continue
		}
		if cmd == nil {
			continue
		}

		// 检查是否应该处理这个 Key
		if !c.ShouldProcess(cmd.Key) {
			continue
		}

		commands = append(commands, cmd)
	}

	return commands, nil
}

// convertEntry 将单个 Entry 转换为 Redis 命令
func (c *CommandConverter) convertEntry(entry *ReplLogEntryV2) (*Command, error) {
	if entry == nil {
		return nil, nil
	}

	switch entry.Op {
	case ReplOpSet:
		return c.convertSetOp(entry)
	case ReplOpDel:
		return c.convertDelOp(entry)
	case ReplOpTTL:
		return c.convertTTLOp(entry)
	case ReplOpTTLDel:
		// TTL 删除 = 移除 TTL
		return c.convertPersistOp(entry)
	case ReplOpCmd:
		return c.convertCmdOp(entry)
	default:
		return nil, nil
	}
}

// convertSetOp 转换 SET 操作
// Tendis 的 Key 格式: type(1) + dbId(4) + pk (primary key)
// 参考 record.h RecordKey 格式
func (c *CommandConverter) convertSetOp(entry *ReplLogEntryV2) (*Command, error) {
	if len(entry.Key) < 5 {
		return nil, fmt.Errorf("key too short for SET op")
	}

	// 解析 RecordKey
	keyType := entry.Key[0]
	// dbId := binary.BigEndian.Uint32(entry.Key[1:5])
	pk := entry.Key[5:] // primary key

	// 解析 RecordValue
	value, ttl, err := parseRecordValue(entry.Value)
	if err != nil {
		return nil, err
	}

	switch keyType {
	case 'K': // KV (String)
		return &Command{
			Name: "SET",
			Args: []string{string(pk), string(value)},
			Key:  string(pk),
			TTL:  ttl,
		}, nil

	case 'H': // Hash
		// Hash 的 pk 格式: mainKey + '\x00' + field
		parts := bytes.SplitN(pk, []byte{0}, 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid hash key format")
		}
		mainKey := string(parts[0])
		field := string(parts[1])
		return &Command{
			Name: "HSET",
			Args: []string{mainKey, field, string(value)},
			Key:  mainKey,
			TTL:  ttl,
		}, nil

	case 'S': // Set
		// Set 的 pk 格式: mainKey + '\x00' + member
		parts := bytes.SplitN(pk, []byte{0}, 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid set key format")
		}
		mainKey := string(parts[0])
		member := string(parts[1])
		return &Command{
			Name: "SADD",
			Args: []string{mainKey, member},
			Key:  mainKey,
			TTL:  ttl,
		}, nil

	case 'Z': // ZSet
		// ZSet 的 pk 格式: mainKey + '\x00' + member
		// value 是 score
		parts := bytes.SplitN(pk, []byte{0}, 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid zset key format")
		}
		mainKey := string(parts[0])
		member := string(parts[1])
		score := parseZSetScore(value)
		return &Command{
			Name: "ZADD",
			Args: []string{mainKey, fmt.Sprintf("%f", score), member},
			Key:  mainKey,
			TTL:  ttl,
		}, nil

	case 'L': // List
		// List 比较复杂，需要根据 secondary key 确定位置
		// 简化处理：使用 RPUSH
		parts := bytes.SplitN(pk, []byte{0}, 2)
		if len(parts) < 1 {
			return nil, fmt.Errorf("invalid list key format")
		}
		mainKey := string(parts[0])
		return &Command{
			Name: "RPUSH",
			Args: []string{mainKey, string(value)},
			Key:  mainKey,
			TTL:  ttl,
		}, nil

	default:
		// 未知类型，尝试作为普通 KV 处理
		return &Command{
			Name: "SET",
			Args: []string{string(pk), string(value)},
			Key:  string(pk),
			TTL:  ttl,
		}, nil
	}
}

// convertDelOp 转换 DEL 操作
func (c *CommandConverter) convertDelOp(entry *ReplLogEntryV2) (*Command, error) {
	if len(entry.Key) < 5 {
		return nil, fmt.Errorf("key too short for DEL op")
	}

	keyType := entry.Key[0]
	pk := entry.Key[5:]

	switch keyType {
	case 'K': // KV (String)
		return &Command{
			Name: "DEL",
			Args: []string{string(pk)},
			Key:  string(pk),
		}, nil

	case 'H': // Hash
		parts := bytes.SplitN(pk, []byte{0}, 2)
		if len(parts) != 2 {
			// 可能是删除整个 Hash
			return &Command{
				Name: "DEL",
				Args: []string{string(pk)},
				Key:  string(pk),
			}, nil
		}
		mainKey := string(parts[0])
		field := string(parts[1])
		return &Command{
			Name: "HDEL",
			Args: []string{mainKey, field},
			Key:  mainKey,
		}, nil

	case 'S': // Set
		parts := bytes.SplitN(pk, []byte{0}, 2)
		if len(parts) != 2 {
			return &Command{
				Name: "DEL",
				Args: []string{string(pk)},
				Key:  string(pk),
			}, nil
		}
		mainKey := string(parts[0])
		member := string(parts[1])
		return &Command{
			Name: "SREM",
			Args: []string{mainKey, member},
			Key:  mainKey,
		}, nil

	case 'Z': // ZSet
		parts := bytes.SplitN(pk, []byte{0}, 2)
		if len(parts) != 2 {
			return &Command{
				Name: "DEL",
				Args: []string{string(pk)},
				Key:  string(pk),
			}, nil
		}
		mainKey := string(parts[0])
		member := string(parts[1])
		return &Command{
			Name: "ZREM",
			Args: []string{mainKey, member},
			Key:  mainKey,
		}, nil

	case 'L': // List
		parts := bytes.SplitN(pk, []byte{0}, 2)
		if len(parts) < 1 {
			return nil, fmt.Errorf("invalid list key format")
		}
		mainKey := string(parts[0])
		// List 元素删除比较复杂，暂时用 LREM
		return &Command{
			Name: "LREM",
			Args: []string{mainKey, "0", string(entry.Value)},
			Key:  mainKey,
		}, nil

	default:
		return &Command{
			Name: "DEL",
			Args: []string{string(pk)},
			Key:  string(pk),
		}, nil
	}
}

// convertTTLOp 转换 TTL 设置操作
func (c *CommandConverter) convertTTLOp(entry *ReplLogEntryV2) (*Command, error) {
	if len(entry.Key) < 5 {
		return nil, fmt.Errorf("key too short for TTL op")
	}

	pk := entry.Key[5:]
	
	// 从 value 中解析 TTL (毫秒时间戳)
	ttlMs := parseTTLValue(entry.Value)
	if ttlMs <= 0 {
		return nil, nil
	}

	return &Command{
		Name: "PEXPIREAT",
		Args: []string{string(pk), strconv.FormatInt(ttlMs, 10)},
		Key:  string(pk),
		TTL:  ttlMs,
	}, nil
}

// convertPersistOp 转换 PERSIST 操作 (移除 TTL)
func (c *CommandConverter) convertPersistOp(entry *ReplLogEntryV2) (*Command, error) {
	if len(entry.Key) < 5 {
		return nil, fmt.Errorf("key too short for PERSIST op")
	}

	pk := entry.Key[5:]

	return &Command{
		Name: "PERSIST",
		Args: []string{string(pk)},
		Key:  string(pk),
		TTL:  -1,
	}, nil
}

// convertCmdOp 转换原始命令操作
// Tendis 有时会直接存储原始 Redis 命令
func (c *CommandConverter) convertCmdOp(entry *ReplLogEntryV2) (*Command, error) {
	// 命令格式: RESP 协议或空格分隔
	cmdStr := string(entry.Value)
	if cmdStr == "" {
		return nil, nil
	}

	// 尝试解析 RESP 格式
	if cmdStr[0] == '*' {
		return parseRESPCommand(cmdStr)
	}

	// 简单空格分隔格式
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		return nil, nil
	}

	cmd := &Command{
		Name: strings.ToUpper(parts[0]),
		Args: parts[1:],
	}

	// 提取 Key
	if len(cmd.Args) > 0 {
		cmd.Key = cmd.Args[0]
	}

	return cmd, nil
}

// parseRecordValue 解析 RecordValue
// 格式参考 record.h: type(1) + ttl(varint) + versionEp(varint) + value
func parseRecordValue(data []byte) (value []byte, ttl int64, err error) {
	if len(data) == 0 {
		return nil, -1, nil
	}

	offset := 0

	// RecordType (1 byte) - 有时可能没有
	// 跳过类型字节（如果存在）
	if len(data) > 0 && (data[0] == 'K' || data[0] == 'H' || data[0] == 'S' || data[0] == 'Z' || data[0] == 'L') {
		offset++
	}

	// TTL (varint) - 毫秒时间戳，0表示无TTL
	if offset < len(data) {
		ttlVal, n, err := ReadVarint(data, offset)
		if err == nil {
			offset += n
			if ttlVal > 0 {
				ttl = int64(ttlVal)
			} else {
				ttl = -1 // 无过期
			}
		}
	}

	// VersionEp (varint)
	if offset < len(data) {
		_, n, err := ReadVarint(data, offset)
		if err == nil {
			offset += n
		}
	}

	// 剩余的就是 value
	if offset < len(data) {
		value = data[offset:]
	}

	return value, ttl, nil
}

// parseZSetScore 解析 ZSet 分数
func parseZSetScore(data []byte) float64 {
	if len(data) == 8 {
		bits := binary.BigEndian.Uint64(data)
		// Tendis 使用特殊编码存储 double
		// 这里简化处理
		return float64(int64(bits))
	}
	
	// 尝试解析字符串形式
	score, err := strconv.ParseFloat(string(data), 64)
	if err != nil {
		return 0
	}
	return score
}

// parseTTLValue 解析 TTL 值
func parseTTLValue(data []byte) int64 {
	if len(data) >= 8 {
		return int64(binary.BigEndian.Uint64(data))
	}
	if len(data) >= 4 {
		return int64(binary.BigEndian.Uint32(data))
	}
	
	// 尝试 varint
	val, _, err := ReadVarint(data, 0)
	if err == nil {
		return int64(val)
	}
	
	return 0
}

// parseRESPCommand 解析 RESP 格式命令
func parseRESPCommand(data string) (*Command, error) {
	lines := strings.Split(data, "\r\n")
	if len(lines) < 1 || lines[0][0] != '*' {
		return nil, fmt.Errorf("invalid RESP format")
	}

	count, err := strconv.Atoi(lines[0][1:])
	if err != nil {
		return nil, err
	}

	if count <= 0 {
		return nil, fmt.Errorf("invalid argument count")
	}

	args := make([]string, 0, count)
	lineIdx := 1

	for i := 0; i < count && lineIdx < len(lines); i++ {
		if lines[lineIdx][0] != '$' {
			return nil, fmt.Errorf("expected bulk string")
		}
		length, err := strconv.Atoi(lines[lineIdx][1:])
		if err != nil {
			return nil, err
		}
		lineIdx++

		if lineIdx >= len(lines) {
			break
		}

		if len(lines[lineIdx]) >= length {
			args = append(args, lines[lineIdx][:length])
		} else {
			args = append(args, lines[lineIdx])
		}
		lineIdx++
	}

	if len(args) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	cmd := &Command{
		Name: strings.ToUpper(args[0]),
		Args: args[1:],
	}

	if len(cmd.Args) > 0 {
		cmd.Key = cmd.Args[0]
	}

	return cmd, nil
}

// CommandBatcher 命令批处理器
type CommandBatcher struct {
	commands   []*Command
	batchSize  int
	onBatch    func([]*Command) error
}

// NewCommandBatcher 创建命令批处理器
func NewCommandBatcher(batchSize int, onBatch func([]*Command) error) *CommandBatcher {
	return &CommandBatcher{
		commands:  make([]*Command, 0, batchSize),
		batchSize: batchSize,
		onBatch:   onBatch,
	}
}

// Add 添加命令
func (b *CommandBatcher) Add(cmd *Command) error {
	b.commands = append(b.commands, cmd)

	if len(b.commands) >= b.batchSize {
		return b.Flush()
	}

	return nil
}

// Flush 刷新批次
func (b *CommandBatcher) Flush() error {
	if len(b.commands) == 0 {
		return nil
	}

	batch := b.commands
	b.commands = make([]*Command, 0, b.batchSize)

	if b.onBatch != nil {
		return b.onBatch(batch)
	}

	return nil
}

// Count 获取当前缓冲的命令数
func (b *CommandBatcher) Count() int {
	return len(b.commands)
}
