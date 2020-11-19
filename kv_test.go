package sidb

import (
	assertion "github.com/stretchr/testify/assert"
	"testing"
)

func TestGetCommonPrefix(t *testing.T) {
	assert := assertion.New(t)
	assert.Equal(getCommonPrefix(nil, nil), uint8(0))
	assert.Equal(getCommonPrefix([]byte("abcde"), nil), uint8(0))
	assert.Equal(getCommonPrefix(nil, []byte("abcde")), uint8(0))
	assert.Equal(getCommonPrefix([]byte("abcde"), []byte("abcdefg")), uint8(5))
	assert.Equal(getCommonPrefix([]byte("abcdefg"), []byte("abcde")), uint8(5))
}

func TestKVSerdeSnappy(t *testing.T) {
	assert := assertion.New(t)
	prev := []byte("key")
	key := []byte("keykeykeykey")
	val := []byte("valuevaluevaluevaluevaluevalue")
	kv := KVPair{key, val}
	ser := kv.Marshal(prev, SnappyCompress)
	t.Log(len(ser), ser)
	kv2 := KVPair{}
	err := kv2.Unmarshal(ser, prev, SnappyDeCompress)
	assert.NoError(err)
	assert.Equal(kv2.Key, kv.Key)
	assert.Equal(kv2.Value, kv.Value)
}

func TestKVSerdeLz4(t *testing.T) {
	assert := assertion.New(t)
	prev := []byte("key")
	key := []byte("keykeykeykey")
	val := []byte("valuevaluevaluevaluevaluevalue")
	kv := KVPair{key, val}
	ser := kv.Marshal(prev, Lz4Compress)
	t.Log(len(ser), ser)
	kv2 := KVPair{}
	err := kv2.Unmarshal(ser, prev, Lz4DeCompress)
	assert.NoError(err)
	assert.Equal(kv.Key, kv2.Key)
	assert.Equal(kv.Value, kv2.Value)
}
