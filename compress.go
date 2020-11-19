package sidb

import (
	"bytes"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

type CompressAlgorithm uint16

const (
	CompNone CompressAlgorithm = iota
	CompSnappy
	CompLz4
)

type Compressor func([]byte) []byte
type DeCompressor func([]byte) ([]byte, error)

func SnappyCompress(in []byte) []byte {
	return snappy.Encode(nil, in)
}

func SnappyDeCompress(in []byte) ([]byte, error) {
	return snappy.Decode(nil, in)
}

func Lz4Compress(in []byte) []byte {
	buf := &bytes.Buffer{}
	writer := lz4.NewWriter(buf)
	defer writer.Close()
	writer.NoChecksum = true
	_, err := writer.Write(in)
	if err != nil {
		panic(err)
	}
	_ = writer.Flush()
	return buf.Bytes()
}

func Lz4DeCompress(in []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	reader := lz4.NewReader(bytes.NewReader(in))
	_, err := buf.ReadFrom(reader)
	return buf.Bytes(), err
}
