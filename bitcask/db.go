package bitcask

import (
	"errors"
	"io"
	"kv-go/bitcask/data"
	"kv-go/bitcask/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB 存储引擎的实例
type DB struct {
	Options    Options
	fileIds    []int //只用于加载索引的时候使用文件id
	mu         *sync.RWMutex
	activeFile *data.DataFile            //当前活跃文件
	olderFiles map[uint32]*data.DataFile //旧文件，只用于读
	index      index.Indexer             //内存索引
}

// 打开存储引擎实例
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	//对用户传递过来的一个目录做校验，如果不存在则需要校验
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	//初始化db实例的结构体
	db := &DB{
		Options:    options,
		mu:         new(sync.RWMutex),
		activeFile: nil,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}
	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//从数据文件当中加载索引
	if err := db.loadIndexFromDateFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

// 写入key/value
func (db *DB) Put(key []byte, value []byte) error {
	//判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//构造结构体
	logRecord := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	//追加当前数据到活跃文件当中
	pos, err := db.appendLogRecord(&logRecord)
	if err != nil {
		return err
	}
	//更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
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
		Key:  key,
		Type: data.LogRecordDelete,
	}
	//写入到数据文件里面
	_, err := db.appendLogRecord(&logRecord)
	if err != nil {
		return err
	}
	//从内存索引中删除掉
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
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

	//根据文件的id找到对应的数据文件,如果是活跃文件就用活跃文件，否则去旧文件里面寻找
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, ErrKeyNotFound
	}

	//找到了对应的数据文件，根据偏移量读取我们的数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
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

// 追加写入到活跃文件
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

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
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	//是否需要对数据进行一次安全的持久化,根据用户配置决定
	if db.Options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	//构造内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
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
	dataFile, err := data.OpenDataFile(db.Options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
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
		dataFile, err := data.OpenDataFile(db.Options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
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

	//需要遍历所有文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		//从小到大遍历
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}
		var offset int64
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
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
			}
			var ok bool
			if logRecord.Type == data.LogRecordDelete {
				ok = db.index.Delete(logRecord.Key)
			} else {
				ok = db.index.Put(logRecord.Key, logRecordPos)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}
			//递增offset，下次从新位置开始读取
			offset += size
		}
		//如果当前是活跃文件，下一次从新的位置开始读取
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("DirPath is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("DataFileSize is less than 0")
	}
	return nil
}
