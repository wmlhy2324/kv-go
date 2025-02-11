package bitcask

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
