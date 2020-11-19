package sidb

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"os"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

const (
	// sidbMagic = "SIDB" in bigEndian
	Magic        uint32 = 0x42444953
	Version      uint16 = 1
	IgnoreNoSync        = runtime.GOOS == "openbsd"
	// maxMapSize represents the largest mmap size supported by Bolt.
	maxMapSize = 0xFFFFFFFFFFFF // 256TB

	// maxAllocSize is the size used when creating array pointers.
	maxAllocSize     = 0x7FFFFFFF
	DefaultAllocSize = 16 * 1024 * 1024
)

// Options represents the options that can be set when opening a database.
type Options struct {
	// Timeout is the amount of time to wait to obtain a file lock.
	// When set to zero it will wait indefinitely. This option is only
	// available on Darwin and Linux.
	Timeout time.Duration

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

	//PageSize uint32
}

var DefaultOptions = &Options{
	Timeout:    0,
	NoGrowSync: false,
}

type PagePtr uint32
type PageSz uint32

const maxPageSize PageSz = 0xFFFF

// size: 8
type RecordPtr struct {
	pageNum uint32 // 4
	offset  PageSz // 4
}

// size: 48, aligned: 48
type HeadPage struct {
	magic uint32 // 4
	// checksum of the rest data of this first page
	Checksum uint32 // 4

	Version     uint16            // 1
	Compression CompressAlgorithm // 2
	PageSize    PageSz            // 4

	PageCount      PagePtr // 4
	IndexPageCount uint32  // 4

	// point to the last record
	indexPtr RecordPtr // 8

	kvPtr RecordPtr // 8

	// point to the next index page
	nextIndexPage PagePtr // 4
	// the start pos of data in page
	ptr PageSz // 4
}

// size: 16
// index for page
type Index struct {
	Start   [6]byte
	End     [6]byte
	PageNum uint32
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
	// https://github.com/boltdb/bolt/issues/284
	NoGrowSync bool

	// If you want to read the entire database fast, you can set MmapFlag to
	// syscall.MAP_POPULATE on Linux 2.6.23+ for sequential read-ahead.
	MmapFlags int

	// AllocSize is the amount of space allocated when the database
	// needs to create new pages. This is done to amortize the cost
	// of truncate() and fsync() when growing the data file.
	AllocSize int

	path string
	file *os.File
	//lockfile *os.File // windows only
	dataref  []byte // mmap'ed readonly, write throws SEGV
	data     *[maxMapSize]byte
	datasz   int
	filesz   int // current on disk file size
	pageSize int
	opened   bool

	rwlock   sync.Mutex   // Allows only one writer at a time.
	metalock sync.Mutex   // Protects meta page access.
	mmaplock sync.RWMutex // Protects mmap access during remapping.
	statlock sync.RWMutex // Protects stats access.

	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	// Read only mode.
	// When true, Update() and Begin(true) return ErrDatabaseReadOnly immediately.
	readOnly bool

	header  *HeadPage
	indexes []*Index
}

func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	var db = &DB{opened: true}

	// Set default options if no options are provided.
	if options == nil {
		options = DefaultOptions
	}
	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags

	// Set default values for later DB operations.
	db.AllocSize = DefaultAllocSize

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
	if err := flock(db, options.Timeout); err != nil {
		_ = db.close()
		return nil, err
	}

	// Default values for test hooks
	db.ops.writeAt = db.file.WriteAt

	//// Initialize the database if it doesn't exist.
	//if info, err := db.file.Stat(); err != nil {
	//	return nil, err
	//} else if info.Size() == 0 {
	//	// Initialize new files with meta pages.
	//	if err := db.init(); err != nil {
	//		return nil, err
	//	}
	//} else {
	//	// Read the first meta page to determine the page size.
	//	var buf [4096]byte
	//	if _, err := db.file.ReadAt(buf[:], 0); err == nil {
	//		m := (*HeadPage)(unsafe.Pointer(&buf))
	//		if err := m.validate(); err != nil {
	//			// If we can't read the page size, we can assume it's the same
	//			// as the OS -- since that's how the page size was chosen in the
	//			// first place.
	//			//
	//			// If the first page is invalid and this OS uses a different
	//			// page size than what the database was created with then we
	//			// are out of luck and cannot access the database.
	//			db.pageSize = os.Getpagesize()
	//		} else {
	//			db.pageSize = int(m.pageSize)
	//		}
	//	}
	//}
	//
	//// Initialize page pool.
	//db.pagePool = sync.Pool{
	//	New: func() interface{} {
	//		return make([]byte, db.pageSize)
	//	},
	//}
	//
	//// Memory map the data file.
	//if err := db.mmap(options.InitialMmapSize); err != nil {
	//	_ = db.close()
	//	return nil, err
	//}

	// Mark the database as opened and return.
	return db, nil
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false

	// Clear ops.
	db.ops.writeAt = nil

	// Close the mmap.
	//if err := db.munmap(); err != nil {
	//	return err
	//}

	// Close file handles.
	if db.file != nil {
		// No need to unlock read-only file.
		if !db.readOnly {
			// Unlock the file.
			if err := funlock(db); err != nil {
				log.Printf("bolt.Close(): funlock error: %s", err)
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
		head.Compression = CompSnappy
		head.Version = Version
		offset := PageSz(unsafe.Sizeof(*head))
		head.indexPtr = RecordPtr{0, offset}
		head.kvPtr = RecordPtr{1, PageSz(unsafe.Sizeof(Page{}))}
		head.ptr = offset
		head.PageCount = 2
		head.PageSize = PageSz(db.pageSize)
		db.header = head
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

// headPageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (*DB) headPageInBuffer(b []byte) *HeadPage {
	return (*HeadPage)(unsafe.Pointer(&b[0]))
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (db *DB) pageInBuffer(b []byte, id PagePtr) *Page {
	return (*Page)(unsafe.Pointer(&b[id*PagePtr(db.pageSize)]))
}
