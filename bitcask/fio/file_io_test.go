package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destoryFile(name string) {
	if err := os.Remove(name); err != nil {
		panic(err)
	}
}
func TestNewFileManager(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)
	defer assert.Nil(t, err)
	assert.NotNil(t, fio)
}
func TestFileIO_Write(t *testing.T) {

	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	n, err := fio.Write([]byte("hello world"))
	assert.Nil(t, err)
	t.Log(n, err)
	n, err = fio.Write([]byte(""))

	assert.Nil(t, err)
	assert.Equal(t, 0, n)
}

func TestFileIO_Read(t *testing.T) {

	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)
	b1 := make([]byte, 5)

	n, err := fio.Read(b1, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)
	b2 := make([]byte, 5)

	n, err = fio.Read(b2, 5)
	t.Log(b2, err)
}

func TestFileIO_Close(t *testing.T) {

	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
func TestFileIO_Sync(t *testing.T) {

	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}
