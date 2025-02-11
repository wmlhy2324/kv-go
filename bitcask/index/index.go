package index

import (
	"bytes"
	"github.com/google/btree"
	"kv-go/bitcask/data"
)

// Indexer indexer 后续如果想接入其他数据结构，则直接实现这个接口就可以了
type Indexer interface {
	// Put Put向索引中储存key对应数据的位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get Get根据key取出对应索引的信息
	Get(key []byte) *data.LogRecordPos //拿到索引的位置信息
	// Delete 根据key删除索引对应的位置信息
	Delete(key []byte) bool
}

type IndexType = int8

const (
	//Btree索引
	BTree IndexType = iota + 1

	//自适应基数树
	ART
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case BTree:
		return NewBtree()
	case ART:
		//todo
		return nil
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
