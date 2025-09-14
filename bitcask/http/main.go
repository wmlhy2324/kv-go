package main

import (
	"encoding/json"
	"errors"
	"kv-go/bitcask"
	"log"
	"net/http"
	"os"
)

var db *bitcask.DB

func init() {
	//初始化 DB 实例
	var err error
	option := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-http")

	option.DirPath = dir
	db, err = bitcask.Open(option)
	if err != nil {
		panic(err)
	}
}

func handePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var data map[string]string
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("fail to put value in db:%v\n", err)
		}

	}
}
func handeGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
	key := r.URL.Query().Get("key")
	value, err := db.Get([]byte(key))
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("fail to get value in db:%v\n", err)
		return
	}
	w.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(string(value))
}
func handeDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
	key := r.URL.Query().Get("key")
	err := db.Delete([]byte(key))
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("fail to get value in db:%v\n", err)
		return
	}
	_ = json.NewEncoder(w).Encode("OK")
}

func handeList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := db.ListKeys()
	w.Header().Set("content-type", "application/json")
	var result []string
	for _, key := range key {
		result = append(result, string(key))
	}
	_ = json.NewEncoder(w).Encode(result)
}
func handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	stat := db.Stat()
	w.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(stat)
}
func main() {

	//注册处理方法
	http.HandleFunc("/bitcask/put", handePut)
	http.HandleFunc("/bitcask/get", handeGet)
	http.HandleFunc("/bitcask/delete", handeDelete)
	http.HandleFunc("/bitcask/list", handeList)
	http.HandleFunc("/bitcask/status", handleStatus)
	//启动http服务
	http.ListenAndServe("localhost:8080", nil)
}
