package sidb

import (
	assertion "github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestInit(t *testing.T) {
	assert := assertion.New(t)
	db := &DB{opened: true}
	var err error
	db.file, err = os.OpenFile("/tmp/testsidbinit.sidb", os.O_RDWR|os.O_CREATE, 0755)
	db.ops.writeAt = db.file.WriteAt
	assert.NoError(err)
	assert.NoError(db.init())
	assert.NoError(db.close())
}
