package index

import (
	"bytes"
	"github.com/google/btree"
	"kv-go/bitcask/data"
)

// Indexer indexer 后续如果想接入其他数据结构，则直接实现这个接口就可以了
type Indexer interface {
	// Put Put向索引中储存key对应数据的位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	// Get Get根据key取出对应索引的信息
	Get(key []byte) *data.LogRecordPos //拿到索引的位置信息
	// Delete 根据key删除索引对应的位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)

	//索引迭代器
	Iterator(reverse bool) Iterator

	//索引中存在的数据量
	Size() int
	//关闭索引迭代器(b树和基数树是不需要的)
	Close() error
}

type IndexType = int8

const (
	//Btree索引
	BTree IndexType = iota + 1

	//自适应基数树
	ART

	//b+树
	BPTree
)

func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case BTree:
		return NewBtree()
	case ART:
		//todo
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)

	default:
		panic("unknown index type")
	}

}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	//使用了接口断言,使用指针避免拷贝大对象
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// 通用的索引迭代器，这里定义一个接口的原因是如果有其他数据类型，这里可以直接调用
type Iterator interface {
	//重新回到的迭代器的起点,就是第一个数据
	Rewind()

	//根据传入的key查到到第一个大于或者小于等于目标的eky，从这个key开始遍历
	Seek(key []byte)

	//跳转到下一个key
	Next()

	//表示遍历完了所有的key，用于退出遍历
	Valid() bool

	//当前遍历位置的key数据
	Key() []byte

	//当前遍历位置的value数据
	Value() *data.LogRecordPos

	//关闭迭代器，释放相应资源
	Close()
}
