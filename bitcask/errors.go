package bitcask

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrIndexUpdateFailed      = errors.New("index update failed")
	ErrKeyNotFound            = errors.New("key not found")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataDirectoryCorrupted = errors.New("data directory corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed max batch num")
	ErrIsMerging              = errors.New("is merging")
	ErrDatabaseIsUsing        = errors.New("database is used by other process")
	ErrMergeRatioUnreached    = errors.New("merge ratio unreached")
	ErrNoEnoughSpace          = errors.New("no enough space")
)
