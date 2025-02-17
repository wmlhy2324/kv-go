package index

import (
	"github.com/stretchr/testify/assert"
	"kv-go/bitcask/data"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBtree()
	res := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res)
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    2,
		Offset: 100,
	})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBtree()
	res := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})

	assert.True(t, res)
	post1 := bt.Get(nil)
	assert.Equal(t, uint32(1), post1.Fid)
	assert.Equal(t, int64(100), post1.Offset)
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    2,
		Offset: 2,
	})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    2,
		Offset: 3,
	})
	assert.True(t, res3)
	post2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(2), post2.Fid)
	assert.Equal(t, int64(3), post2.Offset)

	t.Log(post2)
}
func TestBTree_Delete(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    22,
		Offset: 33,
	})
	assert.True(t, res3)
	res4 := bt.Delete([]byte("a"))
	assert.True(t, res4)
}
func TestBTree_Range(t *testing.T) {
	bt1 := NewBtree()
	//btree为空的情况
	iter1 := bt1.Iterator(false)

	assert.Equal(t, false, iter1.Valid())

	//btree有数据的情况
	bt1.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	t.Log(iter2.Key())
	t.Log(iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())
	//有多条数据
	bt1.Put([]byte("b"), &data.LogRecordPos{
		Fid:    1,
		Offset: 20,
	})
	bt1.Put([]byte("c"), &data.LogRecordPos{
		Fid:    1,
		Offset: 30,
	})
	bt1.Put([]byte("d"), &data.LogRecordPos{
		Fid:    1,
		Offset: 40,
	})
	iter3 := bt1.Iterator(false)
	//可以加上断言
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log("key = ", string(iter3.Key()))
		t.Log(iter3.Value())
	}

	//测试seek，close

	iter5 := bt1.Iterator(false)
	iter5.Seek([]byte("b"))
	for iter5.Seek([]byte("b")); iter5.Valid(); iter5.Next() {
		t.Log("key = ", string(iter5.Key()))
	}
}
