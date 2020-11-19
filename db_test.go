package sidb

import (
	"github.com/pkg/errors"
	assertion "github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const testDB = "/tmp/test-sidb-init.sidb"

func TestInit(t *testing.T) {
	assert := assertion.New(t)
	db := &DB{opened: true}
	var err error
	db.file, err = os.OpenFile(testDB, os.O_RDWR|os.O_CREATE, 0755)
	db.ops.writeAt = db.file.WriteAt
	assert.NoError(err)
	assert.NoError(db.init())
	assert.NoError(db.close())
	defer os.Remove(testDB)
}

func TestOpen(t *testing.T) {
	assert := assertion.New(t)
	os.Remove(testDB)
	defer os.Remove(testDB)
	// open un-exist with readonly
	db, err := Open(testDB, 0755, &Options{ReadOnly: true})
	assert.Nil(db)
	assert.Error(err)
	assert.True(os.IsNotExist(err))

	// open with create
	db, err = Open(testDB, 0755, nil)
	assert.NoError(err)
	assert.Equal(CompSnappy, db.compression)
	assert.Equal(2*db.pageSize, db.filesz)
	assert.Equal(32*1024, db.datasz)
	assert.Equal(Magic, db.head.magic)

	// concurrent open with write and readonly
	dbr, err := Open(testDB, 0755, &Options{ReadOnly: true})
	assert.Nil(dbr)
	assert.Error(err)
	assert.True(errors.Is(err, ErrWriteByOther))

	assert.NoError(db.Close())

	// reopen with readonly
	db, err = Open(testDB, 0755, &Options{ReadOnly: true})
	assert.NoError(err)
	assert.Equal(CompSnappy, db.compression)
	assert.Equal(2*db.pageSize, db.filesz)
	assert.Equal(32*1024, db.datasz)
	assert.Equal(Magic, db.head.magic)

	// concurrent open with 2 readonly
	dbr, err = Open(testDB, 0755, &Options{ReadOnly: true})
	assert.NoError(err)
	assert.Equal(CompSnappy, db.compression)
	assert.Equal(2*db.pageSize, db.filesz)
	assert.Equal(32*1024, db.datasz)
	assert.Equal(Magic, db.head.magic)

	assert.NoError(db.Close())
	assert.NoError(dbr.Close())
}
