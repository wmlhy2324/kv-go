package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("QWERTYIOPASDFGHLZXCBNM123456789")
)

func GetTestKey(i int) []byte {

	return []byte(fmt.Sprintf("bitcask-go-key-%09d", i))
}

// 随机生成value
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte(fmt.Sprintf("bitcask-go-value-" + string(b)))
}
