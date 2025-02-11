package data

import "kv-go/bitcask/fio"

const DataFileNameSuffix = ".data"

// 数据文件
type DataFile struct {
	FileId    uint32
	WriteOff  int64         //文件写到了哪个位置
	IoManager fio.IOManager //io读写
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

// 非文件怎么sync呢
func (df *DataFile) Sync() error {
	return nil
}
func (df *DataFile) Write(buf []byte) error {
	return nil
}
