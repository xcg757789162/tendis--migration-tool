package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// BinlogParser Binlog 解析器
// 基于 Tendis 2.7.0 官方源码 (binlog_tool.cpp, repl_util.cpp, repllog.cpp)
type BinlogParser struct {
	file     *os.File
	reader   io.Reader
	header   *BinlogHeader
	position int64 // 当前文件位置
	
	// 统计
	totalRecords int64
	errorRecords int64
}

// NewBinlogParser 创建 Binlog 解析器
func NewBinlogParser(filePath string) (*BinlogParser, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open file failed: %w", err)
	}

	parser := &BinlogParser{
		file:   file,
		reader: file,
	}

	// 读取并验证文件头
	header, err := parser.readHeader()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("read header failed: %w", err)
	}
	parser.header = header

	return parser, nil
}

// NewBinlogParserFromReader 从 Reader 创建解析器
func NewBinlogParserFromReader(r io.Reader) (*BinlogParser, error) {
	parser := &BinlogParser{
		reader: r,
	}

	header, err := parser.readHeader()
	if err != nil {
		return nil, fmt.Errorf("read header failed: %w", err)
	}
	parser.header = header

	return parser, nil
}

// readHeader 读取 Binlog 文件头
// 格式: "BINLOG_V2\r\n" (11 bytes) + storeId (4 bytes, big-endian)
func (p *BinlogParser) readHeader() (*BinlogHeader, error) {
	// 读取版本标识 "BINLOG_V2\r\n"
	versionBuf := make([]byte, len(BinlogHeaderV2))
	n, err := io.ReadFull(p.reader, versionBuf)
	if err != nil {
		return nil, fmt.Errorf("read version failed: %w", err)
	}
	if n != len(BinlogHeaderV2) {
		return nil, ErrUnexpectedEOF
	}

	version := string(versionBuf)
	if version != BinlogHeaderV2 {
		return nil, fmt.Errorf("%w: expected %q, got %q", ErrVersionMismatch, BinlogHeaderV2, version)
	}

	// 读取 storeId (4 bytes, big-endian)
	storeId, err := ReadUint32BE(p.reader)
	if err != nil {
		return nil, fmt.Errorf("read storeId failed: %w", err)
	}

	p.position = int64(BinlogHeaderV2Size)

	return &BinlogHeader{
		Version: version,
		StoreId: storeId,
	}, nil
}

// GetHeader 获取文件头
func (p *BinlogParser) GetHeader() *BinlogHeader {
	return p.header
}

// ReadRecord 读取下一条记录
// 记录格式: keyLen(4 bytes LE) + key + valueLen(4 bytes LE) + value
func (p *BinlogParser) ReadRecord() (*BinlogRecord, error) {
	// 读取 key 长度
	keyLen, err := ReadUint32LE(p.reader)
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read keyLen failed: %w", err)
	}

	// 读取 key
	keyBuf := make([]byte, keyLen)
	_, err = io.ReadFull(p.reader, keyBuf)
	if err != nil {
		return nil, fmt.Errorf("read key failed: %w", err)
	}

	// 读取 value 长度
	valueLen, err := ReadUint32LE(p.reader)
	if err != nil {
		return nil, fmt.Errorf("read valueLen failed: %w", err)
	}

	// 读取 value
	valueBuf := make([]byte, valueLen)
	_, err = io.ReadFull(p.reader, valueBuf)
	if err != nil {
		return nil, fmt.Errorf("read value failed: %w", err)
	}

	p.position += 4 + int64(keyLen) + 4 + int64(valueLen)
	p.totalRecords++

	// 解析 key
	key, err := p.parseKey(keyBuf)
	if err != nil {
		p.errorRecords++
		return &BinlogRecord{
			RawKey:   keyBuf,
			RawValue: valueBuf,
		}, fmt.Errorf("parse key failed: %w", err)
	}

	// 解析 value
	value, err := p.parseValue(valueBuf)
	if err != nil {
		p.errorRecords++
		return &BinlogRecord{
			Key:      key,
			RawKey:   keyBuf,
			RawValue: valueBuf,
		}, fmt.Errorf("parse value failed: %w", err)
	}

	return &BinlogRecord{
		Key:      key,
		Value:    value,
		RawKey:   keyBuf,
		RawValue: valueBuf,
	}, nil
}

// parseKey 解析 ReplLogKeyV2
// 格式: ChunkId(4) + Type(1) + DbId(4) + BinlogId(8) = 17 bytes
func (p *BinlogParser) parseKey(data []byte) (*ReplLogKeyV2, error) {
	if len(data) < ReplLogKeyV2Size {
		return nil, fmt.Errorf("%w: key too short (%d < %d)", ErrInvalidRecord, len(data), ReplLogKeyV2Size)
	}

	key := &ReplLogKeyV2{
		ChunkId:  binary.BigEndian.Uint32(data[0:4]),
		Type:     data[4],
		DbId:     binary.BigEndian.Uint32(data[5:9]),
		BinlogId: binary.BigEndian.Uint64(data[9:17]),
	}

	// 验证类型
	if key.Type != RecordTypeReplLog {
		return nil, fmt.Errorf("%w: expected type 'L' (0x%x), got 0x%x", ErrInvalidKeyType, RecordTypeReplLog, key.Type)
	}

	return key, nil
}

// parseValue 解析 ReplLogValueV2
// 格式参考 repllog.cpp 中的 ReplLogValueV2::decode()
// RecordValue header + ChunkId(4) + Flag(1) + TxnId(varint) + Timestamp(varint) + 
// VersionEp(varint) + CmdStr(lengthPrefixed) + EntriesCount(varint) + Entries
func (p *BinlogParser) parseValue(data []byte) (*ReplLogValueV2, error) {
	if len(data) < 10 {
		return nil, fmt.Errorf("%w: value too short", ErrInvalidRecord)
	}

	offset := 0
	value := &ReplLogValueV2{}

	// 跳过 RecordValue header (如果存在)
	// RecordValue header 格式: type(1) + ttl(varint) + versionEp(varint) + value
	// 但在 binlog 中，ReplLogValueV2 有自己的编码格式

	// ChunkId (4 bytes, big-endian)
	if offset+4 > len(data) {
		return nil, ErrUnexpectedEOF
	}
	value.ChunkId = binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// Flag (1 byte)
	if offset+1 > len(data) {
		return nil, ErrUnexpectedEOF
	}
	value.Flag = data[offset]
	offset++

	// TxnId (varint)
	txnId, n, err := ReadVarint(data, offset)
	if err != nil {
		return nil, fmt.Errorf("read txnId failed: %w", err)
	}
	value.TxnId = txnId
	offset += n

	// Timestamp (varint)
	timestamp, n, err := ReadVarint(data, offset)
	if err != nil {
		return nil, fmt.Errorf("read timestamp failed: %w", err)
	}
	value.Timestamp = timestamp
	offset += n

	// VersionEp (varint)
	versionEp, n, err := ReadVarint(data, offset)
	if err != nil {
		return nil, fmt.Errorf("read versionEp failed: %w", err)
	}
	value.VersionEp = versionEp
	offset += n

	// CmdStr (length-prefixed string)
	if offset+4 > len(data) {
		// 没有 cmdStr，可能是旧格式
		value.CmdStr = ""
	} else {
		cmdStr, n, err := ReadLengthPrefixedBytes(data, offset)
		if err != nil {
			// 可能没有 cmdStr
			value.CmdStr = ""
		} else {
			value.CmdStr = string(cmdStr)
			offset += n
		}
	}

	// Entries
	value.Entries = make([]*ReplLogEntryV2, 0)

	// 读取条目数量 (varint)
	if offset < len(data) {
		entriesCount, n, err := ReadVarint(data, offset)
		if err == nil {
			offset += n

			// 读取每个条目
			for i := uint64(0); i < entriesCount && offset < len(data); i++ {
				entry, bytesRead, err := p.parseEntry(data, offset)
				if err != nil {
					// 跳过错误的条目
					break
				}
				value.Entries = append(value.Entries, entry)
				offset += bytesRead
			}
		}
	}

	return value, nil
}

// parseEntry 解析单个 ReplLogEntryV2
// 格式: Op(1) + Timestamp(varint) + KeyLen(varint) + Key + ValueLen(varint) + Value
func (p *BinlogParser) parseEntry(data []byte, offset int) (*ReplLogEntryV2, int, error) {
	startOffset := offset
	entry := &ReplLogEntryV2{}

	// Op (1 byte)
	if offset+1 > len(data) {
		return nil, 0, ErrUnexpectedEOF
	}
	entry.Op = data[offset]
	offset++

	// 验证操作类型
	if entry.Op > ReplOpTTLDel {
		return nil, 0, fmt.Errorf("%w: op=%d", ErrInvalidOperation, entry.Op)
	}

	// Timestamp (varint)
	timestamp, n, err := ReadVarint(data, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("read entry timestamp failed: %w", err)
	}
	entry.Timestamp = timestamp
	offset += n

	// KeyLen (varint)
	keyLen, n, err := ReadVarint(data, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("read keyLen failed: %w", err)
	}
	offset += n

	// Key
	if offset+int(keyLen) > len(data) {
		return nil, 0, ErrUnexpectedEOF
	}
	entry.Key = make([]byte, keyLen)
	copy(entry.Key, data[offset:offset+int(keyLen)])
	offset += int(keyLen)

	// ValueLen (varint)
	valueLen, n, err := ReadVarint(data, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("read valueLen failed: %w", err)
	}
	offset += n

	// Value
	if offset+int(valueLen) > len(data) {
		return nil, 0, ErrUnexpectedEOF
	}
	entry.Value = make([]byte, valueLen)
	copy(entry.Value, data[offset:offset+int(valueLen)])
	offset += int(valueLen)

	return entry, offset - startOffset, nil
}

// SeekTo 跳转到指定位置 (仅文件模式支持)
// 注意：不使用 Seek 作为方法名，避免与 io.Seeker 接口签名冲突
func (p *BinlogParser) SeekTo(position int64) error {
	if p.file == nil {
		return fmt.Errorf("seek not supported for reader mode")
	}

	_, err := p.file.Seek(position, io.SeekStart)
	if err != nil {
		return err
	}
	p.position = position
	return nil
}

// Position 获取当前位置
func (p *BinlogParser) Position() int64 {
	return p.position
}

// Close 关闭解析器
func (p *BinlogParser) Close() error {
	if p.file != nil {
		return p.file.Close()
	}
	return nil
}

// Stats 获取统计信息
func (p *BinlogParser) Stats() (total, errors int64) {
	return p.totalRecords, p.errorRecords
}

// BinlogIterator Binlog 迭代器 (用于流式处理)
type BinlogIterator struct {
	parser  *BinlogParser
	current *BinlogRecord
	err     error
}

// NewBinlogIterator 创建迭代器
func NewBinlogIterator(parser *BinlogParser) *BinlogIterator {
	return &BinlogIterator{parser: parser}
}

// Next 迭代下一条记录
func (it *BinlogIterator) Next() bool {
	if it.err != nil {
		return false
	}

	record, err := it.parser.ReadRecord()
	if err != nil {
		if err != io.EOF {
			it.err = err
		}
		return false
	}

	it.current = record
	return true
}

// Record 获取当前记录
func (it *BinlogIterator) Record() *BinlogRecord {
	return it.current
}

// Err 获取错误
func (it *BinlogIterator) Err() error {
	return it.err
}

// ParseRawRecord 解析原始记录数据 (不从文件读取)
// 用于处理从 PSYNC 或其他来源获取的原始数据
func ParseRawRecord(rawKey, rawValue []byte) (*BinlogRecord, error) {
	parser := &BinlogParser{}

	key, err := parser.parseKey(rawKey)
	if err != nil {
		return nil, err
	}

	value, err := parser.parseValue(rawValue)
	if err != nil {
		return nil, err
	}

	return &BinlogRecord{
		Key:      key,
		Value:    value,
		RawKey:   rawKey,
		RawValue: rawValue,
	}, nil
}

// BinlogWriter Binlog 写入器 (用于测试或备份)
type BinlogWriter struct {
	writer io.Writer
	buffer *bytes.Buffer
}

// NewBinlogWriter 创建 Binlog 写入器
func NewBinlogWriter(w io.Writer) *BinlogWriter {
	return &BinlogWriter{
		writer: w,
		buffer: bytes.NewBuffer(nil),
	}
}

// WriteHeader 写入文件头
func (w *BinlogWriter) WriteHeader(storeId uint32) error {
	// 写入版本标识
	_, err := w.writer.Write([]byte(BinlogHeaderV2))
	if err != nil {
		return err
	}

	// 写入 storeId (big-endian)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], storeId)
	_, err = w.writer.Write(buf[:])
	return err
}

// WriteRecord 写入记录
func (w *BinlogWriter) WriteRecord(key, value []byte) error {
	// 写入 key 长度 (little-endian)
	var keyLenBuf [4]byte
	binary.LittleEndian.PutUint32(keyLenBuf[:], uint32(len(key)))
	_, err := w.writer.Write(keyLenBuf[:])
	if err != nil {
		return err
	}

	// 写入 key
	_, err = w.writer.Write(key)
	if err != nil {
		return err
	}

	// 写入 value 长度 (little-endian)
	var valueLenBuf [4]byte
	binary.LittleEndian.PutUint32(valueLenBuf[:], uint32(len(value)))
	_, err = w.writer.Write(valueLenBuf[:])
	if err != nil {
		return err
	}

	// 写入 value
	_, err = w.writer.Write(value)
	return err
}
