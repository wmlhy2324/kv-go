package index

import (
	"github.com/google/btree"
	"kv-go/bitcask/data"

	"sync"
)

//主要封装谷歌的btree库

type Btree struct {
	tree *btree.BTree
	lock sync.RWMutex //由于这个Write operations are not safe for concurrent mutation by multiple,所以要进行加锁保护
}

func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) bool {

}
func (bt *Btree) Get(key []byte) *data.LogRecordPos {

} //拿到索引的位置信息
func (bt *Btree) Delete(key []byte) bool {

}
