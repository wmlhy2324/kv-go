package bitcask

import (
	"encoding/binary"
	"kv-go/bitcask/data"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

//原子批量写数据，保证原子性

type Writebatch struct {
	Options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord //暂存用户写入的数据
}

// 初始化
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *Writebatch {
	if db.Options.IndexType == BPlusTree && !db.seqNoFileExists && !db.isInitial {

		panic("cannot use write batch ,seq no file exist")

	}

	return &Writebatch{
		Options:       opts,
		mu:            &sync.Mutex{},
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}

}

// 把记录存在原子写的一个map里面
// 通过原子操作获取序列号
// 批量写数据
func (wb *Writebatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	//暂存LogRecord
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除数据
func (wb *Writebatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//数据不存在则直接返回
	logRecordPos := wb.db.index.Get(key)

	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}
	//暂存LogRecord
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDelete}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// 提交事务,将暂存的数据写到数据文件，并更新内存索引
func (wb *Writebatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if uint(len(wb.pendingWrites)) > wb.Options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}
	//加锁保证事务提交的串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()
	//获取当前最新事务的序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	//开始写数据到数据文件里面
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	//写一条事务完成提交的数据

	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}
	//根据配置决定是否持久化
	if wb.Options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}
	//更新内存索引
	for _, record := range wb.pendingWrites {

		pos := positions[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDelete {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}
	//将暂存的数据清空
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// key+seq编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// 解析LogRecord的key，获取实际的key和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo

}
