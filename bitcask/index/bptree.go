package index

import (
	"go.etcd.io/bbolt"
	"kv-go/bitcask/data"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// b+树索引
type BPlusTree struct {
	tree *bbolt.DB
}

// 初始化b+树
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0600, opts)
	if err != nil {
		panic(err)
	}
	//创建对应的bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic(err)
	}
	return &BPlusTree{bptree}
}

// Put Put向索引中储存key对应数据的位置信息
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	//取出旧值
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldVal = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic(err)
	}
	if len(oldVal) == 0 {
		return nil
	}

	return data.DecodeLogRecordPos(oldVal)
}

// Get Get根据key取出对应索引的信息
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {

	}
	return pos
} //拿到索引的位置信息
// Delete 根据key删除索引对应的位置信息
func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if oldVal = bucket.Get(key); len(oldVal) != 0 {

			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	if len(oldVal) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(oldVal), true
}

// 索引中存在的数据量
func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic(err)
	}
	return size
}
func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

// b+索引迭代器

type bptreeIterator struct {
	tx *bbolt.Tx
	//类似迭代器
	cursor  *bbolt.Cursor
	reverse bool
	//暂存key和value
	currKey   []byte
	currValue []byte
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}
func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	//手动开启一个事务
	tx, err := tree.Begin(false)
	if err != nil {
		panic(err)
	}
	bpi := &bptreeIterator{tx, tx.Bucket(indexBucketName).Cursor(), reverse, nil, nil}
	//这里由于刚刚初始化，key和value都是空的
	bpi.Rewind()
	return bpi
}

// 重新回到的迭代器的起点,就是第一个数据
func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Last()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.First()
	}
}

// 根据传入的key查到到第一个大于或者小于等于目标的eky，从这个key开始遍历
func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.currKey, bpi.currValue = bpi.cursor.Seek(key)

}

// 跳转到下一个key
func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Prev()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.Next()
	}
}

// 表示遍历完了所有的key，用于退出遍历
func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.currKey) != 0
}

// 当前遍历位置的key数据
func (bpi *bptreeIterator) Key() []byte {
	return bpi.currKey
}

// 当前遍历位置的value数据
func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.currValue)
}

// 关闭迭代器，释放相应资源
func (bpi *bptreeIterator) Close() {
	//暂存事务提交
	_ = bpi.tx.Rollback()
}
