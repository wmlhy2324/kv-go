package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

//crc type keySize valueSize
//4     1     5         5

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
}

// 利用LogRecord进行编码,返回字节数组以及长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

//对字节数组中的header信息解码

func decodeLogRecord(data []byte) (*logRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
