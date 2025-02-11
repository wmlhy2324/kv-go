package index

import (
	"github.com/google/btree"
	"kv-go/bitcask/data"

	"sync"
)

//主要封装谷歌的btree库

type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex //由于这个Write operations are not safe for concurrent mutation by multiple,所以要进行加锁保护
}

func NewBtree() *Btree {
	return &Btree{
		tree: btree.New(32), //控制叶子节点的数量，可以后期让用户进行选择
		lock: new(sync.RWMutex),
	}
}
func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(&it) //ReplaceOrInsert 方法：这是 btree 包提供的标准方法，用于插入或更新键值对。
	//如果二叉树中已经存在相同的键，则替换其对应的值。
	//如果键不存在，则插入新的键值对
	bt.lock.Unlock()
	return true

}
func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{
		key: key,
	}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
} //拿到索引的位置信息
func (bt *Btree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true

}
