package sidb

import (
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"hash/crc32"
	"os"
	"runtime"
	"sync"
	"unsafe"
)

const (
	// sidbMagic = "SIDB" in bigEndian
	Magic        uint32 = 0x42444953
	Version      uint16 = 1
	IgnoreNoSync        = runtime.GOOS == "openbsd"
	// maxMapSize represents the largest mmap size supported by Bolt.
	maxMapSize = 0xFFFFFFFFFFFF // 256TB
	// The largest step that can be taken when remapping the mmap.
	maxMmapStep = 1 << 30 // 1GB

	// maxAllocSize is the size used when creating array pointers.
	maxAllocSize = 0x7FFFFFFF
	// alloc 8 * pagesize on every grow
	AllocPages = 8
)

// Options represents the options that can be set when opening a database.
type Options struct {
	// Sets the DB.NoGrowSync flag before memory mapping the file.
	NoGrowSync bool

	// Open database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to
	// grab a shared lock (UNIX).
	ReadOnly bool

	OrderedWrite bool

	// Sets the DB.MmapFlags flag before memory mapping the file.
	MmapFlags int

	// InitialMmapSize is the initial mmap size of the database
	// in bytes. Read transactions won't block write transaction
	// if the InitialMmapSize is large enough to hold database mmap
	// size. (See DB.Begin for more information)
	//
	// If <=0, the initial map size is 0.
	// If initialMmapSize is smaller than the previous database size,
	// it takes no effect.
	InitialMmapSize int

	Compression CompressAlgorithm

	//PageSize uint32
}

var DefaultOptions = &Options{
	NoGrowSync: false,
}

type PageId uint32
type PageSz uint32

const maxPageSize PageSz = 0xFFFF

// size: 8
type RecordPtr struct {
	pageNum uint32 // 4
	offset  PageSz // 4
}

// size: 16
// index for page
type Index struct {
	Start   [6]byte
	End     [6]byte
	PageNum uint32
}

// size: 48, aligned: 48
type HeadPage struct {
	magic uint32 // 4
	// checksum of the rest data of this first page
	Checksum uint32 // 4

	Version     uint16            // 1
	Compression CompressAlgorithm // 2
	PageSize    PageSz            // 4

	// count of all allocated pages including head page
	PageCount PageId // 4
	// count of all index page
	IndexPageCount uint32 // 4

	// point to the last record
	indexPtr RecordPtr // 8

	kvPtr RecordPtr // 8

	// point to the next index page
	nextIndexPage PageId // 4
	// the start pos of data in page
	ptr PageSz // 4
}

func (h *HeadPage) validate(db *DB) error {
	if h.magic != Magic {
		return errors.New("wrong magic")
	}
	if h.Version != Version {
		return errors.New("version mismatch")
	}
	if h.Checksum != 0 && h.Checksum != crc32.ChecksumIEEE(db.data[h.ptr:h.PageSize]) {
		return errors.New("checksum mismatch")
	}
	return nil
}

type DB struct {
	// When enabled, the database will perform a Check() after every commit.
	// A panic is issued if the database is in an inconsistent state. This
	// flag has a large performance impact so it should only be used for
	// debugging purposes.
	StrictMode bool

	// Setting the NoSync flag will cause the database to skip fsync()
	// calls after each commit. This can be useful when bulk loading data
	// into a database and you can restart the bulk load in the event of
	// a system failure or database corruption. Do not set this flag for
	// normal use.
	//
	// If the package global IgnoreNoSync constant is true, this value is
	// ignored.  See the comment on that constant for more details.
	//
	// THIS IS UNSAFE. PLEASE USE WITH CAUTION.
	NoSync bool

	// When true, skips the truncate call when growing the database.
	// Setting this to true is only safe on non-ext3/ext4 systems.
	// Skipping truncation avoids preallocation of hard drive space and
	// bypasses a truncate() and fsync() syscall on remapping.
	//
	// https://github.com/sidbdb/sidb/issues/284
	NoGrowSync bool

	// If you want to read the entire database fast, you can set MmapFlag to
	// syscall.MAP_POPULATE on Linux 2.6.23+ for sequential read-ahead.
	MmapFlags int

	path string
	file *os.File
	//lockfile *os.File // windows only
	dataref   []byte // mmap'ed readonly, write throws SEGV
	data      *[maxMapSize]byte
	datasz    int
	filesz    int // current on disk file size
	pageSize  int
	allocSize int
	opened    bool

	rwlock   sync.Mutex   // Allows only one writer at a time.
	headlock sync.Mutex   // Protects head page access.
	mmaplock sync.RWMutex // Protects mmap access during remapping.
	pagePool sync.Pool

	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	// Read only mode.
	// When true, Update() and Begin(true) return ErrDatabaseReadOnly immediately.
	readOnly bool

	head    *HeadPage
	indexes []*Index

	compression  CompressAlgorithm
	compressor   Compressor
	decompressor DeCompressor
}

func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	var db = &DB{opened: true}

	// Set default options if no options are provided.
	if options == nil {
		options = DefaultOptions
	}
	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags

	db.compression = options.Compression

	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}

	// Open data file and separate sync handler for metadata writes.
	db.path = path
	var err error
	if db.file, err = os.OpenFile(db.path, flag, mode); err != nil {
		if os.IsNotExist(err) && db.readOnly {
			_ = db.close()
			return nil, err
		}
		if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
			_ = db.close()
			return nil, err
		}
	}

	// Lock file so that other processes using in read-write mode cannot
	// use the database  at the same time. This would cause corruption since
	// the two processes would write meta pages and free pages separately.
	// The database file is locked exclusively (only one process can grab the lock)
	// if !options.ReadOnly.
	// The database file is locked using the shared lock (more than one process may
	// hold a lock at the same time) otherwise (options.ReadOnly is set).
	if err := flock(db); err != nil {
		_ = db.close()
		return nil, err
	}

	// Default values for test hooks
	db.ops.writeAt = db.file.WriteAt

	// Initialize the database if it doesn't exist.
	if info, err := db.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		// Initialize new files with meta pages.
		if err := db.init(); err != nil {
			return nil, err
		}
	} else {
		// Read the first meta page to determine the page size.
		var buf [4096]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			h := (*HeadPage)(unsafe.Pointer(&buf))
			db.pageSize = int(h.PageSize)
		}
	}
	db.allocSize = AllocPages * db.pageSize

	// Initialize page pool.
	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	// Memory map the data file.
	if err := db.mmap(options.InitialMmapSize); err != nil {
		_ = db.close()
		return nil, err
	}

	switch db.compression {
	case CompSnappy:
		db.compressor = SnappyCompress
		db.decompressor = SnappyDeCompress
	case CompLz4:
		db.compressor = Lz4Compress
		db.decompressor = Lz4DeCompress
	}

	// Mark the database as opened and return.
	return db, nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.headlock.Lock()
	defer db.headlock.Unlock()

	db.mmaplock.RLock()
	defer db.mmaplock.RUnlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false

	// Clear ops.
	db.ops.writeAt = nil

	// Close the mmap.
	if err := db.munmap(); err != nil {
		return err
	}

	// Close file handles.
	if db.file != nil {
		// No need to unlock read-only file.
		if !db.readOnly {
			// Unlock the file.
			if err := funlock(db); err != nil {
				log.Printf("sidb.Close(): funlock error: %s", err)
			}
		}
		// Close the file descriptor.
		if err := db.file.Close(); err != nil {
			return errors.Wrap(err, "db file closed")
		}
		db.file = nil
	}
	db.path = ""
	return nil
}

// init creates a new database file and initializes its meta pages.
func (db *DB) init() error {
	// Set the page size to the OS page size.
	db.pageSize = os.Getpagesize()
	if db.pageSize > int(maxPageSize) {
		db.pageSize = int(maxPageSize)
	}

	// 1 headPage + 1 dataPage
	buf := make([]byte, db.pageSize*2)
	{
		head := db.headPageInBuffer(buf)
		head.magic = Magic
		head.Compression = db.compression
		head.Version = Version
		offset := PageSz(unsafe.Sizeof(*head))
		head.indexPtr = RecordPtr{0, offset}
		head.kvPtr = RecordPtr{1, PageSz(unsafe.Sizeof(Page{}))}
		head.ptr = offset
		head.PageCount = 2
		head.IndexPageCount = 0
		head.PageSize = PageSz(db.pageSize)
		db.head = head
	}
	{
		page1 := db.pageInBuffer(buf, 1)
		page1.ptr = PageSz(unsafe.Sizeof(*page1))
		page1.Flag = PageData | PageFull
	}

	// Write the buffer to our data file.
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	if err := db.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) gerFreePage() PageId {
	return 0
}

// grow grows the size of the database to the given sz.
func (db *DB) grow(sz int) error {
	// Ignore if the new size is less than available file size.
	if sz <= db.filesz {
		return nil
	}

	// If the data is smaller than the alloc size then only allocate what's needed.
	// Once it goes over the allocation size then allocate in chunks.
	if db.datasz < db.allocSize {
		sz = db.datasz
	} else {
		sz += db.allocSize
	}

	// Truncate and fsync to ensure file size metadata is flushed.
	// https://github.com/sidbdb/sidb/issues/284
	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			if err := db.file.Truncate(int64(sz)); err != nil {
				return errors.Wrap(err, "file resize error")
			}
		}
		if err := db.file.Sync(); err != nil {
			return errors.Wrap(err, "file sync error")
		}
	}

	db.filesz = sz
	return nil
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return errors.Wrap(err, "mmap stat error")
	} else if int(info.Size()) < db.pageSize*2 {
		return errors.New("file size too small")
	}

	// Ensure the size is at least the minimum size.
	var size = int(info.Size())
	db.filesz = size
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	// Unmap existing data before continuing.
	if err := db.munmap(); err != nil {
		return err
	}

	// Memory-map the data file as a byte slice.
	if err := mmap(db, size); err != nil {
		return err
	}

	// Save references to the meta pages.
	db.head = db.headPage()

	// Validate the meta pages. We only return an error if both meta pages fail
	// validation, since meta0 failing validation means that it wasn't saved
	// properly -- but we can recover using meta1. And vice-versa.
	err = db.head.validate(db)
	if err != nil {
		return err
	}
	return nil
}

// munmap unmaps the data file from memory.
func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return errors.Wrap(err, "unmap error")
	}
	return nil
}

// mmapSize determines the appropriate size for the mmap given the current size
// of the database. The minimum size is 32KB and doubles until it reaches 1GB.
// Returns an error if the new mmap size is greater than the max allowed.
func (db *DB) mmapSize(size int) (int, error) {
	// Double the size from 32KB until 1GB.
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// Verify the requested size is not above the maximum allowed.
	if size > maxMapSize {
		return 0, errors.New("mmap too large")
	}

	// If larger than 1GB then grow by 1GB at a time.
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// Ensure that the mmap size is a multiple of the page size.
	// This should always be true since we're incrementing in MBs.
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// If we've exceeded the max size then only grow up to the max size.
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

// page retrieves a page reference from the mmap based on the current page size.
func (db *DB) headPage() *HeadPage {
	return (*HeadPage)(unsafe.Pointer(&db.data[0]))
}

// page retrieves a page reference from the mmap based on the current page size.
func (db *DB) page(id PageId) *Page {
	if id == 0 {
		panic("reading HeadPage page 0 as Page ")
	}
	pos := id * PageId(db.pageSize)
	return (*Page)(unsafe.Pointer(&db.data[pos]))
}

// headPageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (*DB) headPageInBuffer(b []byte) *HeadPage {
	return (*HeadPage)(unsafe.Pointer(&b[0]))
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (db *DB) pageInBuffer(b []byte, id PageId) *Page {
	return (*Page)(unsafe.Pointer(&b[id*PageId(db.pageSize)]))
}

// GoString returns the Go string representation of the database.
func (db *DB) GoString() string {
	return fmt.Sprintf("sidb.DB{path:%q}", db.path)
}

// String returns the string representation of the database.
func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}
