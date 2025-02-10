package data

// 内存索引的数据结构,主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	//文件id哪个文件当中
	Fid uint32
	//偏移量,文件的哪个位置
	Offset int64
}
