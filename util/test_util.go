package util

import (
	"testing"

	"../db"
	"github.com/stretchr/testify/assert"
)

func CreateDB(t *testing.T, c db.DBClient) {
	_, err := c.ExecuteQuery("CREATE DATABASE testdb", "", "")
	assert.Nil(t, err)
}

func DropDB(t *testing.T, c db.DBClient) {
	_, err := c.ExecuteQuery("DROP DATABASE testdb", "", "")
	assert.Nil(t, err)
}
