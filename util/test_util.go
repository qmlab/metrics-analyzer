package util

import (
	"testing"

	"../db"
	"github.com/stretchr/testify/assert"
)

const TestDB = "testdb"

func CreateDB(t *testing.T, c db.DBClient) {
	err := c.CreateDB(TestDB)
	assert.Nil(t, err)
}

func DropDB(t *testing.T, c db.DBClient) {
	err := c.DropDB(TestDB)
	assert.Nil(t, err)
}
