package bitcask

import "os"

type Options struct {
	DirPath      string //数据库数据目录
	DataFileSize int64  //数据文件大小
	SyncWrites   bool   //是否持久化
	//累计写到的预期值进行持久化
	BytesPerSync uint
	IndexType    IndexerType

	//是否需要启动mmap的加载
	MMapAtStartup bool
	//数据文件merge合并的阈值
	DataFileMergeRatio float32
}

type IndexerType = int8

const (
	//BTree索引
	BTree IndexerType = iota + 1
	//ART索引
	ART
	//b+树
	BPlusTree
)

type WriteBatchOptions struct {
	//一个批次的最大数据量
	MaxBatchNum uint

	//提交时是否sync持久化
	SyncWrites bool
}

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024,
	SyncWrites:         false,
	BytesPerSync:       8,
	IndexType:          BTree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

// 索引迭代器配置项
type IteratorOptions struct {
	Prefix  []byte //遍历前缀为指定的key值
	Reverse bool
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
