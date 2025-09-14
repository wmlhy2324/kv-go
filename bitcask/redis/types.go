package redis

import (
	"encoding/binary"
	"errors"
	"kv-go/bitcask"
	"kv-go/bitcask/utils"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("wrong type operation")
)

type RedisDataType = byte

const (
	String RedisDataType = iota
	Hash
	Set
	List
	ZSet
)

// redis数据结构服务
type RedisDataStructure struct {
	db *bitcask.DB
}

func NewRedisDataStructure(options bitcask.Options) (*RedisDataStructure, error) {

	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}

	return &RedisDataStructure{db: db}, nil
}
func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}

//=============string 数据结构 ==================

func (rds *RedisDataStructure) Set(key []byte, value []byte, ttl time.Duration) error {

	if value == nil {
		return nil
	}

	//编码value:type +expire + payload
	//字节切片储存编码之后的数据
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {

		expire = time.Now().Add(ttl).UnixNano()

	}
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)
	//调用存储引擎接口进行写入
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	//解码
	dateType := encValue[0]
	if dateType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	if expire > 0 && expire < time.Now().UnixNano() {
		return nil, nil
	}
	//?
	return encValue[index:], nil
}

// ===================hash数据结构======================
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	//查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	//构造Hash数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	encKey := hk.encode()

	//先查找是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	//不存在更新元数据
	if !exist {
		//这里数据量对应的是key下面的field value数量
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	//找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	//找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	encKey := hk.encode()

	//先查看是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}
	if exist {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(encKey, meta.encode())
		_ = wb.Delete(encKey)

		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}
func (rds *RedisDataStructure) findMetadata(key []byte, dataType RedisDataType) (*Metadata, error) {

	metaBuf, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return nil, err
	}
	var meta *Metadata
	var exist = true
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)

		//判断数据类型
		if meta.dateType != dataType {
			return nil, ErrWrongTypeOperation
		}
		//判断过期时间,?
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	//不存在
	if !exist {
		meta = &Metadata{
			dateType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark

		}
	}
	return meta, nil
}

// ===============================Set 数据结构 ============================
func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	//构造一个数据部分的key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	var ok bool
	if _, err = rds.db.Get(sk.encode()); errors.Is(err, bitcask.ErrKeyNotFound) {
		//不存在的话则更新
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {

	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}
	return true, nil
}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {

	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	if _, err = rds.db.Get(sk.encode()); errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}

	//更新元数据和数据部分
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(sk.encode(), meta.encode())
	_ = wb.Delete(sk.encode())

	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ===================list数据结构======================
func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}
func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)

}
func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)

}

func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {

	//查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	//构造数据部分的key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {

		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}
	//更新元数据和数据部分
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--

	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	//key下面有多少数据
	return meta.size, nil
}
func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {

	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err

	}

	if meta.size == 0 {
		return nil, nil
	}
	//构造数据部分的key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {

		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	//更新元数据
	meta.size--

	if isLeft {
		meta.head++

	} else {
		meta.tail--
	}
	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}
	return element, nil
}

//========================ZSet======================

func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	// 查看是否已经存在
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	// 更新元数据和数据
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}
func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}
