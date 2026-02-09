// Package replication 提供 Tendis binlog 解析功能
//
// 基于官方 Tendis 2.7.0 源码分析实现：
// - 参考 src/tendisplus/storage/record.h 中的 ReplLogKeyV2, ReplLogValueV2, ReplLogValueEntryV2
// - 参考 src/tendisplus/storage/repllog.cpp 中的 decode 实现
// - 参考 src/tendisplus/storage/varint.cpp 中的 varint 编解码
// - 参考 src/tendisplus/utils/string.cpp 中的 lenStrDecode
//
// 【重要】Tendis 编码规则：
// 1. 整数使用 Big-Endian 编码 (int32Decode, int64Decode 使用 be32toh/be64toh)
// 2. 字符串长度使用 varint 编码，不是固定 4 字节
// 3. timestamp 在 ReplLogValueEntryV2 中使用 varint 编码
//
// Binlog 格式：
// 1. BinlogWriter 在 applybinlogsv2 中发送的数据格式：
//    - 1 字节 header (version=2)
//    - N 个 ReplLogRawV2 (每个是 lenStr(key) + lenStr(value))
//
// 2. ReplLogValueV2 格式（binlog value header，所有整数 Big-Endian）：
//    - 4 字节: chunkId (BE)
//    - 2 字节: flag (BE)
//    - 8 字节: txnId (BE)
//    - 8 字节: timestamp (BE)
//    - 8 字节: versionEp (BE)
//    - N 字节: cmdStr (varint-length-prefixed)
//    - 剩余: ReplLogValueEntryV2 列表
//
// 3. ReplLogValueEntryV2 格式：
//    - 1 字节: op (操作类型)
//    - N 字节: timestamp (varint)
//    - N 字节: key (varint-length-prefixed)
//    - N 字节: value (varint-length-prefixed)
package replication

import (
	"encoding/binary"
	"fmt"
)

// ReplOp 复制操作类型
// 来自 Tendis record.h: enum class ReplOp
type ReplOp uint8

const (
	ReplOpNone               ReplOp = 0 // REPL_OP_NONE
	ReplOpSet                ReplOp = 1 // REPL_OP_SET
	ReplOpDel                ReplOp = 2 // REPL_OP_DEL
	ReplOpStmt               ReplOp = 3 // REPL_OP_STMT (statement)
	ReplOpSpec               ReplOp = 4 // REPL_OP_SPEC (special)
	ReplOpDelRange           ReplOp = 5 // REPL_OP_DEL_RANGE
	ReplOpDelFilesIncludeEnd ReplOp = 6 // REPL_OP_DEL_FILES_INCLUDE_END
	ReplOpDelFilesExcludeEnd ReplOp = 7 // REPL_OP_DEL_FILES_EXCLUDE_END
)

// ReplFlag 复制标志
type ReplFlag uint16

const (
	ReplFlagNone ReplFlag = iota
)

// BinlogHeaderSize binlog 头部大小
const BinlogHeaderSize = 1

// BinlogVersion 当前 binlog 版本
const BinlogVersion uint8 = 2

// ReplLogKeyV2 binlog key
type ReplLogKeyV2 struct {
	BinlogID uint64
}

// ReplLogValueV2 binlog value header
type ReplLogValueV2 struct {
	ChunkID   uint32
	Flag      ReplFlag
	TxnID     uint64
	Timestamp uint64 // 毫秒
	VersionEp uint64
	CmdStr    string
	Data      []byte // 原始数据，包含 entry 列表
}

// ReplLogValueEntryV2 binlog entry
type ReplLogValueEntryV2 struct {
	Op        ReplOp
	Timestamp uint64 // 毫秒
	Key       string
	Value     []byte
}

// ReplLogRawV2 原始 binlog 记录
type ReplLogRawV2 struct {
	Key   []byte
	Value []byte
}

// ParsedBinlog 解析后的完整 binlog 记录
type ParsedBinlog struct {
	BinlogID  uint64
	Timestamp uint64
	ChunkID   uint32
	CmdStr    string // 如果是通用命令，这里是 Redis 命令字符串
	Entries   []ReplLogValueEntryV2
}

// BinlogParser Tendis binlog 解析器
type BinlogParser struct{}

// NewBinlogParser 创建 binlog 解析器
func NewBinlogParser() *BinlogParser {
	return &BinlogParser{}
}

// ParseBinlogs 解析 applybinlogsv2 命令中的 binlog 数据
// 输入：binlog 数据字节流，期望的条目数量
// 输出：解析后的 binlog 列表
func (p *BinlogParser) ParseBinlogs(data []byte, expectedCount int) ([]ParsedBinlog, error) {
	if len(data) < BinlogHeaderSize {
		return nil, fmt.Errorf("binlog data too short: %d bytes", len(data))
	}

	// 检查版本
	version := data[0]
	if version != BinlogVersion {
		return nil, fmt.Errorf("unsupported binlog version: %d, expected: %d", version, BinlogVersion)
	}

	offset := BinlogHeaderSize
	results := make([]ParsedBinlog, 0, expectedCount)

	for i := 0; i < expectedCount && offset < len(data); i++ {
		// 读取 ReplLogRawV2
		raw, bytesRead, err := p.readReplLogRaw(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("read binlog entry %d failed: %w", i, err)
		}
		offset += bytesRead

		// 解析 key
		key, err := p.decodeReplLogKey(raw.Key)
		if err != nil {
			return nil, fmt.Errorf("decode binlog key %d failed: %w", i, err)
		}

		// 解析 value
		value, err := p.decodeReplLogValue(raw.Value)
		if err != nil {
			return nil, fmt.Errorf("decode binlog value %d failed: %w", i, err)
		}

		// 解析 entries
		entries, err := p.decodeEntries(value.Data)
		if err != nil {
			// 如果解析 entries 失败，可能是命令格式，忽略错误
			entries = nil
		}

		results = append(results, ParsedBinlog{
			BinlogID:  key.BinlogID,
			Timestamp: value.Timestamp,
			ChunkID:   value.ChunkID,
			CmdStr:    value.CmdStr,
			Entries:   entries,
		})
	}

	return results, nil
}

// varintDecode 解码 varint（与 Tendis varintDecodeFwd 一致）
// varint 编码：每字节低 7 位是数据，最高位表示是否有更多字节
// 返回值：(解码后的值, 消耗的字节数, 错误)
func varintDecode(data []byte) (uint64, int, error) {
	var result uint64
	var i int
	for i = 0; i < len(data) && (data[i]&0x80) != 0; i++ {
		result |= uint64(data[i]&0x7f) << (7 * i)
	}
	if i >= len(data) {
		return 0, 0, fmt.Errorf("varint decode: incomplete data")
	}
	result |= uint64(data[i]&0x7f) << (7 * i)
	return result, i + 1, nil
}

// lenStrDecode 解码长度前缀字符串（varint + data）
// 返回值：(字符串内容, 消耗的总字节数, 错误)
func lenStrDecode(data []byte) ([]byte, int, error) {
	// 先解码 varint 长度
	strLen, lenBytes, err := varintDecode(data)
	if err != nil {
		return nil, 0, fmt.Errorf("decode string length: %w", err)
	}

	totalBytes := lenBytes + int(strLen)
	if len(data) < totalBytes {
		return nil, 0, fmt.Errorf("data too short for string: need %d, have %d", strLen, len(data)-lenBytes)
	}

	content := make([]byte, strLen)
	copy(content, data[lenBytes:totalBytes])
	return content, totalBytes, nil
}

// readReplLogRaw 从数据流中读取一个 ReplLogRawV2
// 格式：lenStr(key) + lenStr(value)（varint 长度前缀）
func (p *BinlogParser) readReplLogRaw(data []byte) (ReplLogRawV2, int, error) {
	offset := 0

	// 读取 key（varint 长度前缀）
	key, keyBytes, err := lenStrDecode(data[offset:])
	if err != nil {
		return ReplLogRawV2{}, 0, fmt.Errorf("decode key: %w", err)
	}
	offset += keyBytes

	// 读取 value（varint 长度前缀）
	value, valueBytes, err := lenStrDecode(data[offset:])
	if err != nil {
		return ReplLogRawV2{}, 0, fmt.Errorf("decode value: %w", err)
	}
	offset += valueBytes

	return ReplLogRawV2{Key: key, Value: value}, offset, nil
}

// RecordKey 结构：
// CHUNKID_OFFSET = 0 (4 bytes)
// TYPE_OFFSET = 4 (1 byte)  
// DBID_OFFSET = 5 (4 bytes)
// PK_OFFSET = 9 (binlogId 从这里开始，8 bytes)
const (
	RecordKeyChunkIDOffset = 0
	RecordKeyTypeOffset    = 4
	RecordKeyDBIDOffset    = 5
	RecordKeyPKOffset      = 9 // binlogId 的偏移量
)

// decodeReplLogKey 解析 binlog key
// 格式：chunkId(4,BE) + type(1) + dbId(4,BE) + binlogId(8,BE) + ...
func (p *BinlogParser) decodeReplLogKey(data []byte) (ReplLogKeyV2, error) {
	if len(data) < RecordKeyPKOffset+8 {
		return ReplLogKeyV2{}, fmt.Errorf("key too short: %d bytes", len(data))
	}

	// binlogId 在 PK_OFFSET (9) 处，使用 Big-Endian
	binlogID := binary.BigEndian.Uint64(data[RecordKeyPKOffset:])

	return ReplLogKeyV2{BinlogID: binlogID}, nil
}

// RecordValue header 最小大小（用于 binlog）
// 包含 7 个 varint 字段，每个最少 1 字节
const RecordValueMinSize = 7

// decodeReplLogValue 解析 binlog value
// 【重要】value 结构：RecordValue header (7 bytes) + ReplLogValueV2 header + entries
// 所有整数字段使用 Big-Endian 编码
func (p *BinlogParser) decodeReplLogValue(data []byte) (ReplLogValueV2, error) {
	const fixedHeaderSize = 4 + 2 + 8 + 8 + 8 // 30 bytes for ReplLogValueV2 header

	// 首先跳过 RecordValue header
	if len(data) < RecordValueMinSize+fixedHeaderSize {
		return ReplLogValueV2{}, fmt.Errorf("value too short for headers: %d bytes", len(data))
	}

	// 跳过 RecordValue header（对于 binlog 固定为 7 字节）
	offset := RecordValueMinSize

	// 现在解析 ReplLogValueV2 header

	// chunkId (4 bytes, Big-Endian)
	chunkID := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// flag (2 bytes, Big-Endian)
	flag := ReplFlag(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	// txnId (8 bytes, Big-Endian)
	txnID := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// timestamp (8 bytes, Big-Endian)
	timestamp := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// versionEp (8 bytes, Big-Endian)
	versionEp := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// cmdStr (varint-length-prefixed)
	cmdStr := ""
	if offset < len(data) {
		cmdBytes, bytesRead, err := lenStrDecode(data[offset:])
		if err == nil {
			cmdStr = string(cmdBytes)
			offset += bytesRead
		}
	}

	// 剩余数据是 entry 列表
	var entryData []byte
	if offset < len(data) {
		entryData = data[offset:]
	}

	return ReplLogValueV2{
		ChunkID:   chunkID,
		Flag:      flag,
		TxnID:     txnID,
		Timestamp: timestamp,
		VersionEp: versionEp,
		CmdStr:    cmdStr,
		Data:      entryData,
	}, nil
}

// decodeEntries 解析 entry 列表
// 【重要】timestamp 和字符串长度都使用 varint 编码
// 格式：op(1) + timestamp(varint) + key(varint-prefixed) + value(varint-prefixed)
func (p *BinlogParser) decodeEntries(data []byte) ([]ReplLogValueEntryV2, error) {
	var entries []ReplLogValueEntryV2
	offset := 0

	for offset < len(data) {
		// op (1 byte)
		if offset >= len(data) {
			break
		}
		op := ReplOp(data[offset])
		offset++

		// timestamp (varint)
		timestamp, bytesRead, err := varintDecode(data[offset:])
		if err != nil {
			return entries, fmt.Errorf("decode timestamp: %w", err)
		}
		offset += bytesRead

		// key (varint-length-prefixed)
		keyBytes, keyBytesRead, err := lenStrDecode(data[offset:])
		if err != nil {
			return entries, fmt.Errorf("decode entry key: %w", err)
		}
		offset += keyBytesRead

		// value (varint-length-prefixed)
		valueBytes, valueBytesRead, err := lenStrDecode(data[offset:])
		if err != nil {
			return entries, fmt.Errorf("decode entry value: %w", err)
		}
		offset += valueBytesRead

		entries = append(entries, ReplLogValueEntryV2{
			Op:        op,
			Timestamp: timestamp,
			Key:       extractRedisKey(string(keyBytes)), // 从 RocksDB 格式中提取真正的 Redis Key
			Value:     valueBytes,
		})
	}

	return entries, nil
}

// extractRedisKey 从 RocksDB RecordKey 格式中提取真正的 Redis Key
// 
// Tendis RocksDB RecordKey 格式（参考 src/tendisplus/storage/record.h）：
//   chunkId(4字节,BE) + type(1字节) + dbId(4字节,BE) + primaryKey(varint长度前缀) + [secondaryKey]
//
// 策略：
//   1. 如果整个字符串都是可打印字符，认为是纯文本 Key，直接返回
//   2. 尝试按 RocksDB RecordKey 格式解析（跳过 9 字节头部，读取 varint 长度前缀的 primaryKey）
//   3. 如果解析失败，降级为查找最长可打印字符序列
func extractRedisKey(recordKey string) string {
	if len(recordKey) == 0 {
		return ""
	}
	
	// 策略1：如果整个字符串都是可打印字符，直接返回
	if isValidRedisKey(recordKey) {
		return recordKey
	}
	
	data := []byte(recordKey)
	
	// 策略2：尝试按 RocksDB RecordKey 格式解析
	// 头部固定 9 字节：chunkId(4) + type(1) + dbId(4)
	const recordKeyHeaderSize = 9
	if len(data) > recordKeyHeaderSize {
		// 跳过头部，尝试读取 varint 长度前缀的 primaryKey
		offset := recordKeyHeaderSize
		
		// 读取 primaryKey 长度（varint 编码）
		pkLen, lenBytes, err := varintDecode(data[offset:])
		if err == nil && lenBytes > 0 && pkLen > 0 && pkLen < 10000 {
			offset += lenBytes
			
			// 检查是否有足够的数据
			if offset+int(pkLen) <= len(data) {
				primaryKey := string(data[offset : offset+int(pkLen)])
				
				// 验证提取的 primaryKey 是否是有效的 Redis Key
				if isValidRedisKey(primaryKey) {
					return primaryKey
				}
			}
		}
	}
	
	// 策略3：降级方案 - 查找最长的连续可打印字符序列
	var bestStart, bestEnd int
	var currentStart int
	inPrintable := false
	
	for i := 0; i <= len(data); i++ {
		var isPrintable bool
		if i < len(data) {
			// 允许 ASCII 32-126 的字符（可打印字符包括空格和标点符号）
			isPrintable = data[i] >= 32 && data[i] <= 126
		}
		
		if isPrintable && !inPrintable {
			// 开始新的可打印序列
			currentStart = i
			inPrintable = true
		} else if !isPrintable && inPrintable {
			// 结束当前可打印序列
			if i-currentStart > bestEnd-bestStart {
				bestStart = currentStart
				bestEnd = i
			}
			inPrintable = false
		}
	}
	
	// 如果找到了足够长的可打印序列（至少 2 个字符）
	if bestEnd-bestStart >= 2 {
		extracted := string(data[bestStart:bestEnd])
		// 验证提取的字符串看起来像一个 Redis Key
		if looksLikeRedisKey(extracted) {
			return extracted
		}
	}
	
	// 策略4：最后降级方案 - 返回原始字符串（让上层过滤逻辑处理）
	return recordKey
}

// isValidRedisKey 检查整个字符串是否是有效的 Redis Key
// 有效的 Key 应该完全由可打印字符组成
func isValidRedisKey(key string) bool {
	if len(key) == 0 || len(key) > 1024 {
		return false
	}
	
	for _, c := range key {
		if c < 32 || c > 126 {
			return false
		}
	}
	
	return true
}

// looksLikeRedisKey 检查字符串是否看起来像一个 Redis Key
// Redis Key 通常包含字母、数字、冒号、下划线等
func looksLikeRedisKey(s string) bool {
	if len(s) < 1 {
		return false
	}
	
	// 统计"合理"字符的数量
	goodChars := 0
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || 
		   (c >= '0' && c <= '9') || c == ':' || c == '_' || c == '-' || c == '.' || c == '/' {
			goodChars++
		}
	}
	
	// 至少 40% 是"合理"字符（降低门槛以支持更多 Key 格式）
	return float64(goodChars)/float64(len(s)) >= 0.4
}

// GetOpStr 获取操作类型的字符串表示
func (op ReplOp) String() string {
	switch op {
	case ReplOpNone:
		return "NONE"
	case ReplOpSet:
		return "SET"
	case ReplOpDel:
		return "DEL"
	case ReplOpStmt:
		return "STMT" // Statement
	case ReplOpSpec:
		return "SPEC" // Special
	case ReplOpDelRange:
		return "DEL_RANGE"
	case ReplOpDelFilesIncludeEnd:
		return "DEL_FILES_INCLUDE"
	case ReplOpDelFilesExcludeEnd:
		return "DEL_FILES_EXCLUDE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", op)
	}
}
