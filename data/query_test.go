package data

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const testFile = "./testdata/query_output.txt"

func TestDeserializeQuery(t *testing.T) {
	data, err := ioutil.ReadFile(testFile)
	assert.Nil(t, err)

	q, err := NewQuery(data)
	assert.Nil(t, err)
	assert.Equal(t, "dqs-query", q.Event)
	assert.Equal(t, int64(1511222489), q.Properties.Time)
	et := "2017-11-20T00:00:00"
	assert.Equal(t, et, q.Properties.RequestParams.FromDate)
}

func TestCreateDatapoint(t *testing.T) {
	data, err := ioutil.ReadFile(testFile)
	assert.Nil(t, err)

	q, _ := NewQuery(data)
	mp, _ := NewMPQuery(q)

	assert.Equal(t, "dqs-query", mp.Event)
	assert.Equal(t, int64(1511222489), mp.Time)
	et, _ := time.Parse("2006-01-02T15:04:05", "2017-11-20T00:00:00")
	assert.Equal(t, et.Unix(), mp.FromDate)
	assert.Equal(t, int64(19), mp.SSQMs)
}
