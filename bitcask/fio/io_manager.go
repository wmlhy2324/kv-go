package fio

const DataFilePerm = 0644

// 抽象io管理接口，可以接入不同的io类型
type IOManager interface {
	//从给定位置读文件
	Read([]byte, int64) (int, error)
	//写入字节数组到文件
	Write([]byte) (int, error)
	//持久化数据

	Sync() error
	//关闭文件
	Close() error
}

// 初始化IOManager 目前支持标准FileIO
func NewIOManager(fileName string) (IOManager, error) {

	return NewFileIOManager(fileName)
}
