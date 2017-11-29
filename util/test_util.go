package util

import (
	"testing"

	"../db"
	"github.com/stretchr/testify/assert"
)

func CreateDB(t *testing.T, c db.DBClient) {
	err := c.CreateDB("testdb")
	assert.Nil(t, err)
}

func DropDB(t *testing.T, c db.DBClient) {
	err := c.DropDB("testdb")
	assert.Nil(t, err)
}
