package bitcask

import (
	"errors"
	"github.com/gofrs/flock"
	"io"
	"kv-go/bitcask/data"
	"kv-go/bitcask/fio"
	"kv-go/bitcask/index"
	"kv-go/bitcask/utils"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const seqNoKey = "seq.no"
const fileLockName = "flock"

// DB 存储引擎的实例
type DB struct {
	Options         Options
	fileIds         []int //只用于加载索引的时候使用文件id
	mu              *sync.RWMutex
	activeFile      *data.DataFile            //当前活跃文件
	olderFiles      map[uint32]*data.DataFile //旧文件，只用于读
	index           index.Indexer             //内存索引,btree是其中一个
	seqNo           uint64                    //事务序列号,全局递增
	isMerging       bool                      //是否有merge正在进行
	seqNoFileExists bool                      //存储事务序列号的文件是否存在
	isInitial       bool                      //是否是第一次初始化这个目录
	fileLock        *flock.Flock              // 文件锁保证多进程之间互斥
	bytesWrite      uint                      // 累计写了字节的数量
	reclaimSize     int64                     //表示有多少数据是无效的
}
type Stat struct {
	KeyNum      uint  // key总量
	DataFileNum uint  //磁盘数据文件数量
	ReclaimSize int64 // 可以进行回收的数据量,字节为单位
	DisSize     int64 //所占磁盘空间大小

}

// 打开存储引擎实例
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	var isInitial bool
	//对用户传递过来的一个目录做校验，如果不存在则需要校验
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	//判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	//没有拿到，有其他数据正在使用这个目录
	if !hold {
		return nil, ErrDatabaseIsUsing
	}
	//文件为空
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}
	//初始化db实例的结构体
	db := &DB{
		Options:    options,
		mu:         new(sync.RWMutex),
		activeFile: nil,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}
	//加载merge数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}
	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	//?b+树索引不需要从数据文件中加载索引
	if options.IndexType != BPlusTree {
		//从hint索引文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}
		//从数据文件当中加载索引
		if err := db.loadIndexFromDateFiles(); err != nil {
			return nil, err

		}

		//重置io类型为标准文件
		if db.Options.MMapAtStartup {
			if err := db.resetIoType(); err != nil {

			}
		}
	}
	//取出当前事务序列号
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}
	return db, nil
}

// 关闭数据库
func (db *DB) Close() error {
	//释放文件锁
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic("failed to unlock file")
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	//关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}
	//保存事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.Options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
		Type:  0,
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}
	//关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	//关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	//持久化当前的一个活跃文件
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// 返回数据相关统计数据
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}
	dirSize, err := utils.DirSize(db.Options.DirPath)
	if err != nil {
		panic(err)

	}

	return &Stat{
		KeyNum:      uint(db.index.Size()),
		DataFileNum: dataFiles,
		ReclaimSize: db.reclaimSize,
		DisSize:     dirSize, //todo
	}
}
func (db *DB) Backup(dir string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	//文件锁的路径排除掉
	return utils.CopyDir(db.Options.DirPath, dir, []string{fileLockName})
}

// 写入key/value
func (db *DB) Put(key []byte, value []byte) error {
	//判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//构造结构体
	logRecord := data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}
	//追加当前数据到活跃文件当中
	pos, err := db.appendLogRecordWithLock(&logRecord)
	if err != nil {
		return err
	}
	//更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	//先判断用户传递过来的key
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//先检查key是否存在，如果不存在的话就直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	//构造LogRecord,标识其是被删除的
	logRecord := data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDelete,
	}
	//写入到数据文件里面
	pos, err := db.appendLogRecordWithLock(&logRecord)
	if err != nil {
		db.reclaimSize += int64(pos.Size)
		return err
	}
	//从内存索引中删除掉
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// 从数据库中获取所有的key
func (db *DB) ListKeys() [][]byte {

	iterator := db.index.Iterator(false)

	keys := make([][]byte, db.index.Size())

	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys

}

// 获取所有的数据，并执行用户指定的操作，这里的key和value是不需要指定的
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {

	db.mu.RLock()
	defer db.mu.RUnlock()
	iterator := db.index.Iterator(false)

	//?
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		//执行用户给的方法,函数返回false时停止
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// 根据key读取文件
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	//判断key的有效性
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	//从内存数据结构中取出key对应的索引信息
	logRecordPos := db.index.Get(key)
	//如果keu不在内存索引里面，就是不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	////根据文件的id找到对应的数据文件,如果是活跃文件就用活跃文件，否则去旧文件里面寻找
	//var dataFile *data.DataFile
	//if db.activeFile.FileId == logRecordPos.Fid {
	//	dataFile = db.activeFile
	//} else {
	//	dataFile = db.olderFiles[logRecordPos.Fid]
	//}
	//if dataFile == nil {
	//	return nil, ErrKeyNotFound
	//}
	//
	////找到了对应的数据文件，根据偏移量读取我们的数据
	//logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	//if err != nil {
	//	return nil, err
	//}
	////判断logRecord的类型
	//if logRecord.Type == data.LogRecordDelete {
	//	return nil, ErrKeyNotFound
	//}
	////实际返回数据

	//从数据文件中获取value
	return db.getValueByPosition(logRecordPos)
}

func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	//根据文件的id找到对应的数据文件,如果是活跃文件就用活跃文件，否则去旧文件里面寻找
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	if dataFile == nil {
		return nil, ErrKeyNotFound
	}

	//找到了对应的数据文件，根据偏移量读取我们的数据
	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	//判断logRecord的类型
	if logRecord.Type == data.LogRecordDelete {
		return nil, ErrKeyNotFound
	}
	//实际返回数据
	return logRecord.Value, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 追加写入到活跃文件
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	//判断当前活跃文件是否存在,数据在没有写入的时候没有文件
	//如果不存在就要初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//写入编码数据
	encRecord, size := data.EncodeLogRecord(logRecord)
	//如果写入数据加上活跃文件大小超过了阈值，就需要转换新数据文件为旧数据文件
	if db.activeFile.WriteOff+size > db.Options.DataFileSize {
		//先将当前文件持久化，确保文件写入到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		//打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	writeOff := db.activeFile.WriteOff

	//记录写入字节总数
	db.bytesWrite += uint(size)
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	//写到了规定字节数也要持久化
	var needSync = db.Options.SyncWrites

	if !needSync && db.Options.BytesPerSync > 0 && db.bytesWrite >= db.Options.BytesPerSync {
		needSync = true
	}

	//是否需要对数据进行一次安全的持久化,根据用户配置决定
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//清空累计值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}
	//构造内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
		Size:   uint32(size),
	}
	return pos, nil
}

// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	//数据文件的创建id是递增的
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	//打开文件
	dataFile, err := data.OpenDataFile(db.Options.DirPath, initialFileId, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件到一个map
func (db *DB) loadDataFiles() error {
	//根据配置项把其中的目录都读取出来
	dirEntries, err := os.ReadDir(db.Options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	//遍历目录中的所有文件，找到所有以.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			//0001.data  解析文件id
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])

			//目录文件可能损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			//将文件id存放到列表中
			fileIds = append(fileIds, fileId)

		}
	}
	//对文件id进行排序，从小到大依次加载
	sort.Ints(fileIds)
	db.fileIds = fileIds
	//遍历每个文件id，打开对应的数据文件
	for i, fid := range fileIds {
		ioType := fio.StandardFIO
		if db.Options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.Options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}
		//存储文件id对应的文件信息
		if i == len(fileIds)-1 {
			//最后一个，id是最大的，说明是当前的活跃文件
			db.activeFile = dataFile
		} else {
			//说明是旧的数据文件
			db.olderFiles = make(map[uint32]*data.DataFile)
		}
	}
	return nil

}

func (db *DB) loadIndexFromDateFiles() error {
	//没有文件，说明数据库是空的
	if len(db.fileIds) == 0 {
		return nil
	}
	//查看是否发生merge
	hasMerge, nonMergeFileId := false, uint32(0)

	mergeFinishName := filepath.Join(db.Options.DirPath, data.MergeFinishName)
	if _, err := os.Stat(mergeFinishName); err != nil {
		fid, err := db.getNonMergeFileId(db.Options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDelete {
			oldPos, _ = db.index.Delete(key)
			//无效的数据
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			panic("failed to put index")
		}
	}
	//暂存我们对应事务的数据 ,事务id对应一个列表
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo uint64 = nonTransactionSeqNo
	//需要遍历所有文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		//从小到大遍历
		var fileId = uint32(fid)
		//如果比最近未参与的merge文件id更小，则说明已经从hint文件加载了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}
		var offset int64
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			//读到了文件末尾
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			//构造内存索引并保存
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
				Size:   uint32(size),
			}

			//解析key,拿到事务序列号

			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			//非事务提交和事务提交
			if seqNo == nonTransactionSeqNo {
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				//对应事务id的数据都是有效的
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					//事务写入的数据，不确定是否可以put
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}

			}
			//更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			//递增offset，下次从新位置开始读取
			offset += size
		}
		//如果当前是活跃文件，下一次从新的位置开始读写
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	//更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("DirPath is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("DataFileSize is less than 0")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("DataFileMergeRatio must be between 0 and 1")
	}
	return nil
}

func (db *DB) loadSeqNo() error {

	fileName := filepath.Join(db.Options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); err != nil {
		return err
	}
	seqNoFile, err := data.OpenSeqNoFile(db.Options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true
	return nil
}

// 将数据文件的io类型设置为标准文件
func (db *DB) resetIoType() error {
	//数据目录是空的
	if db.activeFile == nil {
		return nil
	}
	if err := db.activeFile.SetIOManager(db.Options.DirPath, fio.StandardFIO); err != nil {
		return err
	}
	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.Options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}

// ListKeys 获取数据库中所有的 key (待修改)
