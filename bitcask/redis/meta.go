package redis

import (
	"encoding/binary"
	"kv-go/bitcask/utils"
	"math"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListMark   = math.MaxUint64 / 2
)

// 元数据
type Metadata struct {
	dateType byte
	expire   int64
	version  int64
	size     uint32 //数据量
	head     uint64 //list
	tail     uint64 //list
}

// 对元数据进行编码
func (md *Metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dateType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)
	buf[0] = md.dateType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))
	if md.dateType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}
	return buf[:index]
}
func decodeMetadata(buf []byte) *Metadata {
	dataType := buf[0]

	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n
	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])

	}
	return &Metadata{
		dateType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}

}

type hashInternalKey struct {
	key     []byte
	version int64
	filed   []byte
}

func (hk *hashInternalKey) encode() []byte {

	buf := make([]byte, len(hk.key)+len(hk.filed)+8)
	//key
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.filed)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	//field
	copy(buf[index:], hk.filed)
	return buf
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte {

	buf := make([]byte, len(sk.key)+len(sk.member)+8+4)
	//key
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.member)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	//field
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))
	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)

	//key
	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)

	index += len(lk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8
	//index
	binary.LittleEndian.PutUint64(buf[index:index+8], lk.index)

	return buf
}

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *zsetInternalKey) encodeWithMember() []byte {

	buf := make([]byte, len(zk.key)+len(zk.member)+8)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// member
	copy(buf[index:], zk.member)

	return buf

}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+len(zk.member)+len(scoreBuf)+8+4)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	// member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))

	return buf
}
