package sidb

import (
	"bytes"
	"encoding/binary"
	"github.com/pkg/errors"
)

type KVFlag uint8

// minKVSize = flag + kLen + k + vLen + v = 1 + 1 + 1 + 1 + 1 = 5
var minKVSize = 5

// maxKVPerPage = 8(head) + n * 5 + n <= 4096  -> n = 681

const (
	KVKeyPrefixed KVFlag = 1 << iota
	KVKeyCompressed
	KVValueCompressed
	// store hex string as uint, not implemented
	//KVStringToUint
)

type KVPair struct {
	Key   []byte
	Value []byte
}

func (kv KVPair) Marshal(prevKey []byte, compressor Compressor) []byte {
	var flag KVFlag
	length := 1
	var prefixed bool
	var keyLen, valLen []byte
	prefixLen := getCommonPrefix(prevKey, kv.Key)
	if prefixLen > 0 {
		prefixed = true
		length += 1
		flag |= KVKeyPrefixed
	}
	key := kv.Key[prefixLen:]
	value := kv.Value
	if compressor != nil {
		keyC := compressor(key)
		if len(keyC) < len(key) {
			key = keyC
			flag |= KVKeyCompressed
		}
		valueC := compressor(value)
		if len(valueC) < len(value) {
			value = valueC
			flag |= KVValueCompressed
		}
	}
	kLenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(kLenBuf, uint64(len(key)))
	keyLen = kLenBuf[:n]

	vLenBuf := make([]byte, binary.MaxVarintLen64)
	n = binary.PutUvarint(vLenBuf, uint64(len(value)))
	valLen = vLenBuf[:n]

	length += len(keyLen) + len(key) + len(valLen) + len(value)
	//buf := bytes.NewBuffer(make([]byte, length))
	buf := bytes.NewBuffer(nil)
	buf.Write([]byte{byte(flag)})
	if prefixed {
		buf.Write([]byte{prefixLen})
	}
	buf.Write(keyLen)
	buf.Write(key)
	buf.Write(valLen)
	buf.Write(value)
	return buf.Bytes()
}

func (kv *KVPair) clear() {
	kv.Key = nil
	kv.Value = nil
}

func (kv *KVPair) Unmarshal(data, prevKey []byte, decompressor DeCompressor) (err error) {
	reader := bytes.NewReader(data)
	if data == nil {
		return errors.New("empty KV data")
	}
	if len(data) < minKVSize {
		return errors.New("KV data les than min data size 5, flag + keyLen + key + valueLen + value")
	}
	var prefix, key, val []byte
	_flag, _ := reader.ReadByte()
	flag := KVFlag(_flag)
	if flag&KVKeyPrefixed != 0 {
		_prefixedLen, _ := reader.ReadByte()
		prefixedLen := int(_prefixedLen)
		if len(prevKey) < prefixedLen {
			return errors.New("wrong prefixed key len")
		}
		prefix = prevKey[:prefixedLen]
	}
	if decompressor == nil && (flag&KVKeyCompressed != 0 || flag&KVValueCompressed != 0) {
		return errors.New("key is compressed but decompressor is nil")
	}
	kLen, err := binary.ReadUvarint(reader)
	if err != nil {
		return errors.Wrap(err, "failed to read key length")
	}
	key = make([]byte, kLen)
	_, err = reader.Read(key)
	if err != nil {
		return errors.Wrap(err, "failed to read key")
	}

	vLen, err := binary.ReadUvarint(reader)
	if err != nil {
		return errors.Wrap(err, "failed to read value length")
	}
	val = make([]byte, vLen)
	_, err = reader.Read(val)
	if err != nil {
		return errors.Wrap(err, "failed to read value")
	}

	if flag&KVKeyCompressed != 0 {
		key, err = decompressor(key)
		if err != nil {
			return errors.Wrap(err, "failed to decompress key")
		}
	}

	if flag&KVValueCompressed != 0 {
		val, err = decompressor(val)
		if err != nil {
			return errors.Wrap(err, "failed to decompress value")
		}
	}
	kv.Key = append(prefix, key...)
	kv.Value = val
	return nil
}

func getCommonPrefix(a, b []byte) (length uint8) {
	if a == nil || b == nil {
		return
	}
	for i, v := range b {
		if i >= len(a) || v != a[i] {
			return
		}
		length++
		if length >= 255 {
			return
		}
	}
	return
}
