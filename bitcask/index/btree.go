package index

import (
	"bytes"
	"github.com/google/btree"
	"kv-go/bitcask/data"
	"sort"

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
func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(&it) //ReplaceOrInsert 方法：这是 btree 包提供的标准方法，用于插入或更新键值对。
	//如果二叉树中已经存在相同的键，则替换其对应的值。
	//如果键不存在，则插入新的键值对
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos

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
func (bt *Btree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true

}
func (bt *Btree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()

	return newBtreeIterator(bt.tree, reverse)
}
func (bt *Btree) Size() int {
	return bt.tree.Len()
}
func (bt *Btree) Close() error {
	return nil
}

// btree索引迭代器，学习每个数据的用处
type btreeIterator struct {
	//当前遍历数组的哪个下标
	currIndex int

	//是否反向
	reverse bool

	value []*Item //key+位置索引信息
}

// 这里的迭代器没有面向用户
func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	value := make([]*Item, tree.Len())

	//将所有的数据存放到数组中
	saveValues := func(it btree.Item) bool {

		value[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}
	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		value:     value,
	}
}
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0

}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	//二分查找
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.value), func(i int) bool {
			return bytes.Compare(bti.value[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.value), func(i int) bool {
			return bytes.Compare(bti.value[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (bti *btreeIterator) Next() {
	bti.currIndex += 1

}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (bti *btreeIterator) Valid() bool {

	return bti.currIndex < len(bti.value)
}

// Key 当前遍历位置的 Key 数据
func (bti *btreeIterator) Key() []byte {
	return bti.value[bti.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.value[bti.currIndex].pos
}

// Close 关闭迭代器，释放相应资源
func (bti *btreeIterator) Close() {
	bti.value = nil
}
