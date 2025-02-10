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
	t.Log(post2)
}
