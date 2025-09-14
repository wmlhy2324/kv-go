package benchmark

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
	"kv-go/bitcask"
	"kv-go/bitcask/utils"
	"os"
	"testing"
	"time"
)

var db *bitcask.DB

func init() {

	//初始化用于基准测试的存储引擎
	options := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-bench")
	options.DirPath = dir
	var err error
	db, err = bitcask.Open(options)
	if err != nil {
		panic(err)
	}
}
func Benchmark_Put(b *testing.B) {
	//正式测试之前重置计时器
	b.ResetTimer()
	//打印基准测试内存分配情况
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {

	for i := 0; i < 100; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(uint64(time.Now().UnixNano()))
	//前面的put不是我们想要测试的代码
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {

		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
			b.Fatal(err)
		}
	}

}
func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	rand.Seed(uint64(time.Now().UnixNano()))
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}

}
