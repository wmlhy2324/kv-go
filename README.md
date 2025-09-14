# BitCask KV - 高性能键值存储引擎

[![Go Version](https://img.shields.io/badge/Go-1.23+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

一个基于 BitCask 模型实现的高性能键值存储引擎，用 Go 语言开发。支持多种索引结构，提供 Redis 兼容接口和 HTTP API。

## ✨ 特性

- 🚀 **高性能**：基于 BitCask 存储模型，写入性能极佳
- 💾 **持久化**：WAL 日志保证数据持久性
- 🔍 **多种索引**：支持 B-Tree、ART、B+Tree 等索引结构
- 🔄 **自动合并**：支持数据文件自动合并，节省存储空间
- 🌐 **多接口**：提供原生 Go API、Redis 兼容接口、HTTP RESTful API
- 📊 **数据结构**：支持 String、Hash、Set、List、ZSet 等 Redis 数据类型
- 🔒 **并发安全**：读写锁保护，支持多线程并发访问
- 📈 **可监控**：提供数据统计和性能指标
- 💾 **内存映射**：可选的 mmap 支持，提升读取性能

## 🏗️ 架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   HTTP API      │    │  Redis Protocol │    │   Native Go API │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│   RESTful API   │    │  Redis Commands │    │  Direct Calls   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                        ┌───────▼───────┐
                        │  BitCask Core │
                        ├───────────────┤
                        │  Index Layer  │
                        │ BTree/ART/B+  │
                        ├───────────────┤
                        │ Storage Layer │
                        │  Data Files   │
                        └───────────────┘
```

## 🚀 快速开始

### 安装依赖

```bash
git clone https://github.com/wmlhy2324/kv-go.git
cd kv-go
go mod tidy
```

### 基本使用

```go
package main

import (
    "fmt"
    "kv-go/bitcask"
)

func main() {
    // 配置选项
    opts := bitcask.DefaultOptions
    opts.DirPath = "/tmp/bitcask-go"
    
    // 打开数据库
    db, err := bitcask.Open(opts)
    if err != nil {
        panic(err)
    }
    defer db.Close()
    
    // 写入数据
    err = db.Put([]byte("name"), []byte("BitCask"))
    if err != nil {
        panic(err)
    }
    
    // 读取数据
    val, err := db.Get([]byte("name"))
    if err != nil {
        panic(err)
    }
    fmt.Println("Value:", string(val)) // Output: Value: BitCask
    
    // 删除数据
    err = db.Delete([]byte("name"))
    if err != nil {
        panic(err)
    }
}
```

## 🎮 运行示例

### 1. 基本操作示例

```bash
go run bitcask/examples/basic_operation.go
```

### 2. HTTP API 服务器

启动 HTTP API 服务器（监听端口 8080）：

```bash
go run bitcask/http/main.go
```

测试 API：

```bash
# 存储数据
curl -X PUT http://localhost:8080/bitcask/put \
  -H "Content-Type: application/json" \
  -d '{"key1": "value1", "key2": "value2"}'

# 获取数据
curl -X GET "http://localhost:8080/bitcask/get?key=key1"

# 删除数据
curl -X DELETE "http://localhost:8080/bitcask/delete?key=key1"

# 列出所有键
curl -X GET http://localhost:8080/bitcask/list

# 查看统计信息
curl -X GET http://localhost:8080/bitcask/status
```

### 3. Redis 兼容服务器

启动 Redis 兼容服务器（监听端口 6380）：

```bash
cd bitcask/redis/cmd
go run *.go
```

使用 Redis 客户端连接：

```bash
# 使用 redis-cli
redis-cli -p 6380

# 或使用 telnet
telnet 127.0.0.1 6380

# 或使用 nc
nc 127.0.0.1 6380
```

支持的 Redis 命令：

```redis
# 字符串操作
SET mykey "Hello World"
GET mykey

# 哈希操作
HSET user:1 name "张三"
HSET user:1 age "25"

# 集合操作
SADD myset "member1"

# 列表操作
LPUSH mylist "item1"

# 有序集合操作
ZADD myzset 1.0 "member1"

# 基本命令
PING
```

## ⚙️ 配置选项

```go
type Options struct {
    DirPath            string      // 数据目录路径
    DataFileSize       int64       // 数据文件大小限制
    SyncWrites         bool        // 是否同步写入磁盘
    BytesPerSync       uint        // 累计写入字节数触发同步
    IndexType          IndexerType // 索引类型 (BTree/ART/BPlusTree)
    MMapAtStartup      bool        // 启动时是否使用内存映射
    DataFileMergeRatio float32     // 数据文件合并阈值
}

// 默认配置
var DefaultOptions = Options{
    DirPath:            os.TempDir(),
    DataFileSize:       256 * 1024 * 1024, // 256MB
    SyncWrites:         false,
    BytesPerSync:       8,
    IndexType:          BTree,
    MMapAtStartup:      true,
    DataFileMergeRatio: 0.5,
}
```

## 📊 性能特点

- **写入性能**：顺序写入，性能优异
- **读取性能**：内存索引 + 可选 mmap，读取快速
- **存储效率**：自动数据合并，减少空间浪费
- **并发性能**：读写锁机制，支持高并发访问

## 🗂️ 项目结构

```
kv-go/
├── main.go                    # 主程序入口
├── bitcask/                   # 核心存储引擎
│   ├── db.go                  # 数据库主要实现
│   ├── options.go             # 配置选项
│   ├── batch.go               # 批量写入
│   ├── iterator.go            # 迭代器
│   ├── merge.go               # 数据合并
│   ├── data/                  # 数据文件操作
│   ├── index/                 # 索引实现
│   │   ├── btree.go           # B-Tree 索引
│   │   ├── art.go             # ART 索引
│   │   └── bptree.go          # B+Tree 索引
│   ├── fio/                   # 文件 I/O
│   ├── redis/                 # Redis 兼容层
│   │   ├── types.go           # Redis 数据类型
│   │   └── cmd/               # Redis 服务器
│   ├── http/                  # HTTP API 服务器
│   ├── examples/              # 使用示例
│   └── benchmark/             # 性能测试
└── utils/                     # 工具函数
```

## 🧪 运行测试

```bash
# 运行所有测试
go test ./...

# 运行特定模块测试
go test ./bitcask
go test ./bitcask/data
go test ./bitcask/index

# 运行性能测试
go test -bench=. ./bitcask/benchmark
```

## 📈 性能基准测试

```bash
cd bitcask/benchmark
go test -bench=. -benchmem
```

## 🛠️ API 文档

### Go API

```go
// 数据库操作
func Open(options Options) (*DB, error)
func (db *DB) Close() error
func (db *DB) Put(key, value []byte) error
func (db *DB) Get(key []byte) ([]byte, error)
func (db *DB) Delete(key []byte) error

// 批量操作
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch
func (wb *WriteBatch) Put(key, value []byte) error
func (wb *WriteBatch) Delete(key []byte) error
func (wb *WriteBatch) Commit() error

// 迭代器
func (db *DB) NewIterator(opts IteratorOptions) Iterator
func (iter *Iterator) Rewind()
func (iter *Iterator) Seek(key []byte)
func (iter *Iterator) Next()

// 统计信息
func (db *DB) Stat() *Stat
func (db *DB) ListKeys() [][]byte
```

### HTTP API

| 方法 | 路径 | 描述 |
|------|------|------|
| PUT | `/bitcask/put` | 存储键值对 |
| GET | `/bitcask/get?key=<key>` | 获取值 |
| DELETE | `/bitcask/delete?key=<key>` | 删除键 |
| GET | `/bitcask/list` | 列出所有键 |
| GET | `/bitcask/status` | 获取统计信息 |

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 🔗 相关资源

- [BitCask 论文](https://riak.com/assets/bitcask-intro.pdf)
- [Go 官方文档](https://golang.org/doc/)
- [Redis 协议规范](https://redis.io/topics/protocol)

---

⭐ 如果这个项目对你有帮助，请给个 Star！