package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

// 写入到数据文件的数据
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
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
