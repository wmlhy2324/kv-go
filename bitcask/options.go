package bitcask

import "os"

type Options struct {
	DirPath      string //数据库数据目录
	DataFileSize int64
	SyncWrites   bool //是否持久化

	IndexType IndexerType
}

type IndexerType = int8

const (
	//BTree索引
	BTree IndexerType = iota + 1
	//ART索引
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    BTree,
}
