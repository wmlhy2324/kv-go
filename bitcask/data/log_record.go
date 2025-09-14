package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
	LogRecordTxnFinished
)

//crc type keySize valueSize
// 4     1     5       5

const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// 写入到数据文件的数据
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type logRecordHeader struct {
	crc        uint32        //crc校验值
	recordType LogRecordType //标识的LogRecord类型
	keySize    uint32        //key长度
	valueSize  uint32        //value长度
}

// 内存索引的数据结构,主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	//文件id哪个文件当中
	Fid uint32
	//偏移量,文件的哪个位置
	Offset int64
	//在磁盘中的大小
	Size uint32
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
// crc需要后面的都知道才可以
// +--------+----------+---------+------------+-------------+----+------+
// | crc 校验值 | type 类型 | key size | value size | key | value |
// +--------+----------+---------+------------+-------------+----+------+
// | 4字节 | 1字节 | 变长（最大5） | 变长（最大5） |   |    |
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	//初始化一个header部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)
	//从第五个字节开始写
	header[4] = logRecord.Type
	var index = 5
	//5字节之后，存储的是key和value的一个长度信息
	//使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)

	encBytes := make([]byte, size)
	//将header内容拷贝过来
	copy(encBytes[:index], header[:index])

	//将key和value的数据拷贝到字节数组中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)
	//进行crc的校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	return encBytes, int64(size)
}

//对字节数组中的header信息解码

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	//crc四个字节都没有占到
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	var index = 5
	//取出实际的key size
	keySize, n := binary.Varint(buf[index:])
	index += n
	header.keySize = uint32(keySize)

	//取出实际的value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n
	return header, int64(index)
}

// 对位置信息进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]
}

// 解码
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	return &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: offset,
		Size:   uint32(size),
	}
}
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}

// 暂存事务相关的数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}
