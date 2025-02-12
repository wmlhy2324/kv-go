package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"kv-go/bitcask/fio"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid CRC")
)

const DataFileNameSuffix = ".data"

// 数据文件
type DataFile struct {
	FileId    uint32
	WriteOff  int64         //文件写到了哪个位置
	IoManager fio.IOManager //io读写
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {

	fileName := filepath.Join(fmt.Sprintf("%09d", fileId) + DataFileNameSuffix)
	//初始化iomanager管理器接口
	ioManager, err := fio.NewFileIOManager(fileName)
	if err != nil {
		return nil, err
	}
	//为什么偏移量为0
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil

}

// 根据offset从数据文件中读取logrecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	//读取header信息
	HeaderBuf, err := df.readNBytes(maxLogRecordHeaderSize, offset)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := decodeLogRecord(HeaderBuf)
	//读取到了文件末尾
	if header == nil {
		return nil, 0, io.EOF
	}
	//读取到了文件末尾
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	//取出对应key和value的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize
	logRecord := &LogRecord{Type: header.recordType}
	//根据key和value的长度去读取用户实际存储的key，value数据
	if keySize > 0 || valueSize > 0 {
		//kvbuf就是用户实际存储的一个数据
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		//解除key和value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	//最后校验数据的crc是否正确
	//? 把不需要的长度截取掉
	crc := getLogRecordCRC(logRecord, HeaderBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

// 非文件怎么sync呢
func (df *DataFile) Sync() error {
	return nil
}
func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return b, err

}
