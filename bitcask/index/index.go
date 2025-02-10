package index

import "kv-go/bitcask/data"

// indexer 后续如果想接入其他数据结构，则直接实现这个接口就可以了
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos)
	Get(key []byte) *data.LogRecordPos //拿到索引的位置信息
	Delete(key []byte) bool
}
