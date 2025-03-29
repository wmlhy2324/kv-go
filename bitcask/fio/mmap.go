package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// 用于加速bitcask启动
type MMap struct {
	//官方的mmap只能用于读取数据
	readerAt *mmap.ReaderAt
}

// 初始化mmapio
func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	//映射到虚拟内存
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{
		readerAt: readerAt,
	}, nil
}

// 从给定位置读文件
func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

// 写入字节数组到文件
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

//持久化数据

func (mmap *MMap) Sync() error {
	panic("not implemented")
}

// 关闭文件
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

// Size 获取到文件大小
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
