package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	//正常情况
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))
	t.Log(res1)
	t.Log(n1)
	//value为空的情况
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))
	t.Log(res2)
	//对delete情况的测试
}

func TestDecodeLogRecord(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	t.Log(n1)
	res2, n2 := decodeLogRecordHeader(res1[:7])
	t.Log(res2)
	t.Log(n2)

	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res3, _ := EncodeLogRecord(rec2)
	res4, n4 := decodeLogRecordHeader(res3[:7])
	t.Log(res4)
	t.Log(n4)
}

func TestGetLogRecord(t *testing.T) {
	//正常情况
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}

	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))
	t.Log(res1[4:7])
	crc := getLogRecordCRC(rec1, res1[4:7])
	t.Log(crc)
	assert.Equal(t, uint32(2532332136), crc)

}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 339)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv go"),
	}
	res1, size1 := EncodeLogRecord(rec1)
	t.Log(res1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)
	t.Log(size1)
	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)

	//多条LogRecord,从不同的位置读取

	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
		Type:  0,
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)
	t.Log(size2)

	readRec2, readSize2, err := dataFile.ReadLogRecord(24)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)

	//被删除的文件在数据文件的末尾
	rec3 := &LogRecord{
		Key:   []byte("1"),
		Value: []byte(""),
		Type:  LogRecordDelete,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)
	t.Log(size3)
	readRec3, size3, err := dataFile.ReadLogRecord(size2 + size1)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, size3)
}
