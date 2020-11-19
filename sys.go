package sidb

import (
	"github.com/pkg/errors"
	"syscall"
	"time"
	"unsafe"
)

var ErrWriteByOther = errors.New("db opened with write mode by another process")

// flock acquires an advisory lock on a file descriptor.
func flock(db *DB) error {
	flag := syscall.LOCK_SH
	if !db.readOnly {
		flag = syscall.LOCK_EX
	}

	// Otherwise attempt to obtain an exclusive lock.
	err := syscall.Flock(int(db.file.Fd()), flag|syscall.LOCK_NB)
	if err == nil {
		return nil
	} else if err.(syscall.Errno) == syscall.EWOULDBLOCK || err.(syscall.Errno) == syscall.EAGAIN { // linux & unix
		return ErrWriteByOther
	} else {
		return errors.Wrap(err, "flock failed: unknown error")
	}
}

// flock acquires an advisory lock on a file descriptor.
func waitflock(db *DB, timeout time.Duration) error {
	var t time.Time
	for {
		// If we're beyond our timeout then return an error.
		// This can only occur after we've attempted a flock once.
		if t.IsZero() {
			t = time.Now()
		} else if timeout > 0 && time.Since(t) > timeout {
			return errors.New("timeout")
		}
		// Otherwise attempt to obtain an exclusive lock.
		err := flock(db)
		if !errors.Is(err, ErrWriteByOther) {
			return errors.Wrap(err, "flock failed: unknown error")
		}
		// Wait for a bit and try again.
		time.Sleep(50 * time.Millisecond)
	}
}

// funlock releases an advisory lock on a file descriptor.
func funlock(db *DB) error {
	return syscall.Flock(int(db.file.Fd()), syscall.LOCK_UN)
}

// mmap memory maps a DB's data file.
func mmap(db *DB, sz int) error {
	// Map the data file to memory.
	b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|db.MmapFlags)
	if err != nil {
		return err
	}

	// Advise the kernel that the mmap is accessed randomly.
	if err := madvise(b, syscall.MADV_RANDOM); err != nil {
		return errors.Wrap(err, "madvise error")
	}

	// Save the original byte slice and convert to a byte array pointer.
	db.dataref = b
	db.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	db.datasz = sz
	return nil
}

// munmap unmaps a DB's data file from memory.
func munmap(db *DB) error {
	// Ignore the unmap if we have no mapped data.
	if db.dataref == nil {
		return nil
	}

	// Unmap using the original byte slice.
	err := syscall.Munmap(db.dataref)
	db.dataref = nil
	db.data = nil
	db.datasz = 0
	return err
}

// NOTE: This function is copied from stdlib because it is not available on darwin.
func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}
