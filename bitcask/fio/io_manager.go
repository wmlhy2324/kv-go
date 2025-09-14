package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	//标准文件io
	StandardFIO FileIOType = iota
	//内存文件映射
	MemoryMap
)

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
	//Size 获取到文件大小
	Size() (int64, error)
}

// 初始化IOManager 目前支持标准FileIO
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {

	switch ioType {
	//标准文件io
	case StandardFIO:

		return NewFileIOManager(fileName)
		//内存映射io

	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}

}

func (fio *FileIO) Size() (int64, error) {

	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
