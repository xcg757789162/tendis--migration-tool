package binlog

import (
	"encoding/binary"
	"errors"
	"io"
	"time"
)

// 常量定义 - 基于 Tendis 2.7.0 官方源码 (kvstore.h, record.h)
const (
	// BINLOG_HEADER_V2 = "BINLOG_V2\r\n" + storeId(4 bytes big-endian)
	BinlogHeaderV2     = "BINLOG_V2\r\n"
	BinlogHeaderV2Size = 11 + 4 // "BINLOG_V2\r\n" + storeId

	// ReplLogKeyV2 结构:
	// - ChunkId: uint32 (4 bytes)
	// - Type: uint8 (1 byte) = 'L' for ReplLog
	// - DbId: uint32 (4 bytes)
	// - BinlogId: uint64 (8 bytes)
	ReplLogKeyV2Size = 4 + 1 + 4 + 8

	// RecordType
	RecordTypeReplLog = 'L'

	// ReplOp 操作类型 (repllog.cpp)
	ReplOpNone   uint8 = 0
	ReplOpCmd    uint8 = 1
	ReplOpSet    uint8 = 2 // Key-Value set
	ReplOpDel    uint8 = 3 // Key delete
	ReplOpTTL    uint8 = 4 // TTL set
	ReplOpTTLDel uint8 = 5 // TTL delete

	// ReplFlag
	ReplFlagMulti      uint8 = 1 << 0 // 事务
	ReplFlagFlushed    uint8 = 1 << 1 // 已刷盘
	ReplFlagSessionEnd uint8 = 1 << 2 // 会话结束
)

// 错误定义
var (
	ErrInvalidHeader    = errors.New("invalid binlog header")
	ErrInvalidRecord    = errors.New("invalid binlog record")
	ErrUnexpectedEOF    = errors.New("unexpected end of file")
	ErrInvalidKeyType   = errors.New("invalid key type")
	ErrCorruptedData    = errors.New("corrupted data")
	ErrVersionMismatch  = errors.New("binlog version mismatch")
	ErrInvalidStoreId   = errors.New("invalid store id")
	ErrInvalidOperation = errors.New("invalid operation")
)

// BinlogHeader Binlog 文件头
type BinlogHeader struct {
	Version string // "BINLOG_V2\r\n"
	StoreId uint32 // Store ID (分片ID)
}

// ReplLogKeyV2 复制日志Key (基于 record.h)
// 格式: ChunkId(4) + Type(1) + DbId(4) + BinlogId(8)
type ReplLogKeyV2 struct {
	ChunkId  uint32 // Chunk ID
	Type     uint8  // 记录类型, 应该是 'L'
	DbId     uint32 // 数据库ID (store ID)
	BinlogId uint64 // Binlog 序列号
}

// ReplLogValueV2 复制日志Value (基于 record.h, repllog.cpp)
// 格式: RecordValue header + ChunkId + Flag + TxnId + Timestamp + VersionEp + CmdStr + Entries
type ReplLogValueV2 struct {
	ChunkId   uint32              // Chunk ID
	Flag      uint8               // 标志位 (ReplFlagMulti, ReplFlagFlushed等)
	TxnId     uint64              // 事务ID
	Timestamp uint64              // 时间戳 (毫秒)
	VersionEp uint64              // 版本epoch
	CmdStr    string              // 原始命令字符串 (可选)
	Entries   []*ReplLogEntryV2   // 操作条目列表
}

// ReplLogEntryV2 复制日志条目 (基于 record.h)
// 格式: Op(1) + Timestamp(varint) + Key + Value
type ReplLogEntryV2 struct {
	Op        uint8  // 操作类型 (ReplOpSet, ReplOpDel等)
	Timestamp uint64 // 条目时间戳 (varint编码)
	Key       []byte // Key
	Value     []byte // Value
}

// BinlogRecord 完整的Binlog记录
type BinlogRecord struct {
	Key      *ReplLogKeyV2   // Key
	Value    *ReplLogValueV2 // Value
	RawKey   []byte          // 原始Key字节
	RawValue []byte          // 原始Value字节
}

// GetOperationType 获取操作类型名称
func GetOperationType(op uint8) string {
	switch op {
	case ReplOpNone:
		return "NONE"
	case ReplOpCmd:
		return "CMD"
	case ReplOpSet:
		return "SET"
	case ReplOpDel:
		return "DEL"
	case ReplOpTTL:
		return "TTL"
	case ReplOpTTLDel:
		return "TTLDEL"
	default:
		return "UNKNOWN"
	}
}

// IsMulti 是否是事务
func (v *ReplLogValueV2) IsMulti() bool {
	return v.Flag&ReplFlagMulti != 0
}

// IsFlushed 是否已刷盘
func (v *ReplLogValueV2) IsFlushed() bool {
	return v.Flag&ReplFlagFlushed != 0
}

// IsSessionEnd 是否会话结束
func (v *ReplLogValueV2) IsSessionEnd() bool {
	return v.Flag&ReplFlagSessionEnd != 0
}

// GetTime 获取时间
func (v *ReplLogValueV2) GetTime() time.Time {
	return time.UnixMilli(int64(v.Timestamp))
}

// ReadUint32BE 读取大端序uint32
func ReadUint32BE(r io.Reader) (uint32, error) {
	var buf [4]byte
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}

// ReadUint64BE 读取大端序uint64
func ReadUint64BE(r io.Reader) (uint64, error) {
	var buf [8]byte
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

// ReadUint32LE 读取小端序uint32
func ReadUint32LE(r io.Reader) (uint32, error) {
	var buf [4]byte
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

// ReadUint64LE 读取小端序uint64
func ReadUint64LE(r io.Reader) (uint64, error) {
	var buf [8]byte
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf[:]), nil
}

// ReadVarint 读取变长整数 (Tendis 使用 RocksDB 的 varint 编码)
func ReadVarint(data []byte, offset int) (uint64, int, error) {
	if offset >= len(data) {
		return 0, 0, ErrUnexpectedEOF
	}

	var result uint64
	var shift uint
	for i := offset; i < len(data); i++ {
		b := data[i]
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return result, i + 1 - offset, nil
		}
		shift += 7
		if shift >= 64 {
			return 0, 0, ErrCorruptedData
		}
	}
	return 0, 0, ErrUnexpectedEOF
}

// ReadLengthPrefixedBytes 读取长度前缀的字节数组
// 格式: length(4 bytes LE) + data
func ReadLengthPrefixedBytes(data []byte, offset int) ([]byte, int, error) {
	if offset+4 > len(data) {
		return nil, 0, ErrUnexpectedEOF
	}

	length := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if offset+int(length) > len(data) {
		return nil, 0, ErrUnexpectedEOF
	}

	result := make([]byte, length)
	copy(result, data[offset:offset+int(length)])

	return result, 4 + int(length), nil
}

// WriteVarint 写入变长整数
func WriteVarint(value uint64) []byte {
	var buf [10]byte
	n := 0
	for value >= 0x80 {
		buf[n] = byte(value) | 0x80
		value >>= 7
		n++
	}
	buf[n] = byte(value)
	n++
	return buf[:n]
}

// WriteLengthPrefixedBytes 写入长度前缀的字节数组
func WriteLengthPrefixedBytes(data []byte) []byte {
	result := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint32(result[:4], uint32(len(data)))
	copy(result[4:], data)
	return result
}
