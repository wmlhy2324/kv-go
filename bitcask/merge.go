package bitcask

import (
	"io"
	"kv-go/bitcask/data"
	"kv-go/bitcask/utils"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName   = "-merge"
	mergeFinishKey = "merge.finished"
)

// Merge 清理无效数据，生成hint文件
// merge过程
// 1.检查是否在merge，如果在就返回，如果没有就进入merge状态 2.持久化当前活跃文件，持久化当前活跃文件并且转换为旧文件，则当前活跃文件就算最新文件
// 3.取出所有需要merge的文件 4.待merge的文件从小到大排列，依次merge
func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()

	//如果merge正在进行，则直接返回
	if db.isMerging {
		db.mu.Unlock()
		return ErrIsMerging
	}

	//查看可以merge是否到达了阈值
	totalSize, err := utils.DirSize(db.Options.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}

	if float32(db.reclaimSize)/float32(totalSize) < db.Options.DataFileMergeRatio {
		db.mu.Unlock()
		return ErrMergeRatioUnreached
	}
	//查看剩余空间的容量是否可以查看merge之后的数据量
	av, err := utils.AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if uint64(totalSize-db.reclaimSize) >= av {
		db.mu.Unlock()
		return ErrNoEnoughSpace
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	//0 1 2 3
	//持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		return err
	}
	//转为旧文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	//打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	//记录最近没有参与merge的文件
	nonMergeFileId := db.activeFile.FileId
	//现在需要merge的文件都是旧的数据文件了
	//取出所有需要merge的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	//待merge的文件按照id从小到大排列，依次merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})
	mergePath := db.getMergePath()
	//如果目录存在，说明发生过merge，将其删除
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	//新建一个merge path的目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	//打开一个新的bitcask实例
	mergeOptions := db.Options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}
	//打开hint文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	//遍历处理每个数据
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				//读到文件末尾
				if err == io.EOF {
					break
				}
				return err
			}
			//解析拿到实际的key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			//和内存中的索引位置信息进行比较，如果有效就重写
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				//说明是有效的数据
				//??事务序列号
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				//将当前位置索引写到hint文件里面
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			//增加offset
			offset += size
		}
	}
	//sync保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	//持久化的是当前的一个活跃文件
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	//增加一个标识merge完成,创建了新文件标志着merge完成
	mergeFinishedFile, err := data.OpenMergeFinishFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key: []byte(mergeFinishKey),
		//比这个id小的说明都merge过了
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	//这里还有待理解
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

// 假设源目录在/tmp/bitcask
// merge目录 /tmp/bitcask-merge
func (db *DB) getMergePath() string {
	//拿到目录
	//   dir := path.Dir(...)：获取主目录的父目录路径。  /tmp/bitcask → 父目录为 /tmp
	dir := path.Dir(path.Clean(db.Options.DirPath))
	//base := path.Base(...)：提取主目录的最后一层目录名
	base := path.Base(db.Options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

// 数据库启动的时候对merge文件的一个处理
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	//merge目录不存在就直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	//查找merge完成的文件，判断merge是否处理完了
	var merFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishName {
			merFinished = true
		}
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		if entry.Name() == fileLockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}
	//没有merge完成
	if !merFinished {
		return nil
	}
	//最近没有merge的文件id（id比这个小的都是merge过的 ）
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}
	//删除旧文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.Options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}
	//将新的数据文件移动到数据目录中
	for _, fileName := range mergeFileNames {
		//  /tmp/bitcask-merge
		// /tmp/bitcask
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.Options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(mergePath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishFile(mergePath)
	if err != nil {
		return 0, err
	}
	//只有一条数据
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err

	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

func (db *DB) loadIndexFromHintFile() error {
	//查看索引文件是否存在
	hintFileName := filepath.Join(db.Options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	//如果存在打开hint索引文件
	hintFile, err := data.OpenHintFile(db.Options.DirPath)
	if err != nil {
		return err
	}
	//hint里面存放的数据都是有效的方式
	//读取文件中的索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		//解码拿到实际的位置信息
		pos := data.DecodeLogRecordPos(logRecord.Key)
		db.index.Put(logRecord.Key, pos)
		offset += size

	}
	return nil
}
