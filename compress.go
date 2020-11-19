package sidb

import (
	"bytes"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

type CompressAlgorithm uint16

const (
	CompSnappy CompressAlgorithm = iota // default
	CompNone
	CompLz4
)

type Compressor func([]byte) []byte
type DeCompressor func([]byte) ([]byte, error)

var (
	SnappyCompress Compressor = func(in []byte) []byte {
		return snappy.Encode(nil, in)
	}
	SnappyDeCompress DeCompressor = func(in []byte) ([]byte, error) {
		return snappy.Decode(nil, in)

	}
)

var (
	Lz4Compress Compressor = func(in []byte) []byte {
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

	Lz4DeCompress DeCompressor = func(in []byte) ([]byte, error) {
		buf := &bytes.Buffer{}
		reader := lz4.NewReader(bytes.NewReader(in))
		_, err := buf.ReadFrom(reader)
		return buf.Bytes(), err
	}
)
