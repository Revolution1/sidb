package main

import (
	"fmt"
	"sidb"
	"unsafe"
)

func main() {
	//h := sidb.HeadPage{
	//	Version:     0,
	//	Compression: sidb.CompSnappy,
	//	PageSize:    sidb.PageSz(sidb.DefaultPageSize),
	//	PageCount:   200,
	//	Checksum:    crc32.ChecksumIEEE([]byte("dasdasdsd")),
	//}
	//se := (*[unsafe.Sizeof(h)]byte)(unsafe.Pointer(&h))
	//fmt.Println(unsafe.Sizeof(h), se)
	//b, _ := bolt.Open("", 0600, &bolt.Options{})
	//b.Update(func(tx *bolt.Tx) error {
	//	tx.Bucket([]byte("a")).Put([]byte("k"), []byte("v"))
	//	return nil
	//})
	//s := []byte("abcdefg")
	//fmt.Printf("%p %p %p\n", &s[1], s, s[:0])
	//copy(s[1:], s[:])
	//fmt.Printf("%p %p %p\n", &s[1], s, s[:0])
	//fmt.Printf("%x\n", s)
	//fmt.Println(string(s))
	type T1 struct {
		a [2]int8
		b int64
		c int16
	}
	type T2 struct {
		a [2]int8
		c int16
		b int64
	}
	fmt.Printf("arrange fields to reduce size:\n"+
		"T1 align: %d, size: %d\n"+
		"T2 align: %d, size: %d\n",
		unsafe.Alignof(T1{}), unsafe.Sizeof(T1{}),
		unsafe.Alignof(T2{}), unsafe.Sizeof(T2{}))

	fmt.Println("HeadPage", unsafe.Alignof(sidb.HeadPage{}), unsafe.Sizeof(sidb.HeadPage{}))
	fmt.Println("Page", unsafe.Alignof(sidb.Page{}), unsafe.Sizeof(sidb.Page{}))
	fmt.Println("Index", unsafe.Alignof(sidb.Index{}), unsafe.Sizeof(sidb.Index{}))
	fmt.Println("RecordPtr", unsafe.Alignof(sidb.RecordPtr{}), unsafe.Sizeof(sidb.RecordPtr{}))
}
