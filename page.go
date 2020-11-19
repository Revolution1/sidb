package sidb

var (
	//DefaultPageSize = os.Getpagesize()
	// default system pagesize for most OS
	DefaultPageSize = 4096
)

type PageFlag uint8

const (
	// page of page index
	PageIndex PageFlag = 1 << iota
	// data of key-value pair
	PageData

	// full page: all data contained in page
	PageFull

	// data is stored across pages
	PageFirst
	PageMiddle
	PageLast
)

// size: 11, aligned: 20
type Page struct {
	Flag PageFlag // 1+3
	// how many kv/index in page
	Count uint16 // 2
	// size of data
	Len PageSz // 2
	// next same typed page num
	Next     PagePtr // 4
	ptr      PageSz  // 2+2
	CheckSum uint32  // 4
}

type PageObj struct {
	Id         PagePtr
	Header     *Page
	data       []byte
	start, end [6]byte
	offsetList []PageSz
}

type Chunk struct {
	pages []*Page
}

type IndexPage struct {
	Id      uint32
	Header  *Page
	Indexes []*Index
}
