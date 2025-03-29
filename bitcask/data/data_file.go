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

const (
	DataFileNameSuffix = ".data"
	HintFileName       = "hint-index"
	MergeFinishName    = "merge-finished"
	SeqNoFileName      = "seq-no"
)

// 数据文件
type DataFile struct {
	FileId    uint32
	WriteOff  int64         //文件写到了哪个位置
	IoManager fio.IOManager //io读写
}

func OpenDataFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {

	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId, ioType)

}

// 为什么这里hint文件的id是0
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}
func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	//初始化iomanager管理器接口
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	//偏移量为0
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}
func OpenMergeFinishFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// 存储事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)

}
func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

// 根据offset从数据文件中读取logrecord,这里io重复操作，待优化
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {

	//拿到当前文件的一个大小
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var headerBytes int64 = maxLogRecordHeaderSize
	//如果读取的最大header长度已经超过了文件长度，只需要读取到文件末尾

	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}
	//读取header信息
	HeaderBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	//这里解码应该会把不需要的部分给删掉才对(已解决)
	header, headerSize := decodeLogRecordHeader(HeaderBuf)
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
	crc := getLogRecordCRC(logRecord, HeaderBuf[crc32.Size:headerSize]) //这里有疑问
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

// 非文件怎么sync呢
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

// 写入索引信息到hint文件里面
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	EncodeLogRecord, _ := EncodeLogRecord(record)
	return df.Write(EncodeLogRecord)
}
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	//从数据源的哪个位置开始读取
	_, err = df.IoManager.Read(b, offset)
	return b, err

}
func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}
	df.IoManager = ioManager
	return nil
}
