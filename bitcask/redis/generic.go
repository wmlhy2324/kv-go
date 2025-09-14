package redis

import "errors"

func (rds *RedisDataStructure) Del(key []byte) error {

	return rds.db.Delete(key)

}

func (rds *RedisDataStructure) Type(key []byte) (RedisDataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	//防止数组越界
	if len(encValue) == 0 {
		return 0, errors.New("key is empty")
	}
	//第一个字节就是类型+
	return encValue[0], nil
}
