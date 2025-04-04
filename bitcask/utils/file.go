package utils

import (
	"os"
	"path/filepath"
	"syscall"
)

// 获取一个目录的大小
func DirSize(DirPath string) (int64, error) {
	var size int64

	err := filepath.Walk(DirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil

	})
	return size, err

}

// 获取磁盘剩余大小
func AvailableDiskSize() (uint64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	var stat syscall.Statfs_t

	if err = syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
