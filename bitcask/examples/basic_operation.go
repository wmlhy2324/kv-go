package main

import (
	"fmt"
	"kv-go/bitcask"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = "/tmp/bitcash-go"
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))
}
