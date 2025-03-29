package index

// 自适应基数树
// go get github.com/plar/go-adaptive-radix-tree
import (
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"kv-go/bitcask/data"
	"sort"
	"sync"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// 初始化
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	if oldValue == nil {
		return nil
	}
	art.lock.Unlock()
	return oldValue.(*data.LogRecordPos)
}

// Get Get根据key取出对应索引的信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
} //拿到索引的位置信息
// Delete 根据key删除索引对应的位置信息
func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	defer art.lock.Unlock()
	oldValue, ok := art.tree.Delete(key)
	if oldValue == nil {
		return nil, ok
	}
	return oldValue.(*data.LogRecordPos), ok
}

// 索引迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newArtIterator(art.tree, reverse)
}
func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// 索引中存在的数据量
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

// art索引迭代器，学习每个数据的用处
type artIterator struct {
	//当前遍历数组的哪个下标
	currIndex int

	//是否反向
	reverse bool

	value []*Item //key+位置索引信息
}

// 这里的迭代器没有面向用户
func newArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int

	if reverse {
		idx = tree.Size() - 1
	}
	value := make([]*Item, tree.Size())
	//nodes是一个节点，包含了key和value的数据
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		value[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	//完整遍历数据
	tree.ForEach(saveValues)
	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		value:     value,
	}
}
func (ai *artIterator) Rewind() {
	ai.currIndex = 0

}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (ai *artIterator) Seek(key []byte) {
	//二分查找
	if ai.reverse {
		ai.currIndex = sort.Search(len(ai.value), func(i int) bool {
			return bytes.Compare(ai.value[i].key, key) <= 0
		})
	} else {
		ai.currIndex = sort.Search(len(ai.value), func(i int) bool {
			return bytes.Compare(ai.value[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (ai *artIterator) Next() {
	ai.currIndex += 1

}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (ai *artIterator) Valid() bool {

	return ai.currIndex < len(ai.value)
}

// Key 当前遍历位置的 Key 数据
func (ai *artIterator) Key() []byte {
	return ai.value[ai.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.value[ai.currIndex].pos
}

// Close 关闭迭代器，释放相应资源
func (ai *artIterator) Close() {
	ai.value = nil
}
