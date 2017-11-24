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
	et, _ := time.Parse(time.RFC3339, "2017-11-21T00:00:00")
	assert.Equal(t, et, q.Properties.RequestParams.FromDate)
}

func TestCreateDatapoint(t *testing.T) {
	data, err := ioutil.ReadFile(testFile)
	assert.Nil(t, err)

	q, _ := NewQuery(data)
	mp := NewMPQuery(q)

	assert.Equal(t, "dqs-query", mp.Event)
	assert.Equal(t, int64(1511222489), mp.Time)
	et, _ := time.Parse(time.RFC3339, "2017-11-21T00:00:00")
	assert.Equal(t, et, mp.FromDate)
	assert.Equal(t, int64(19), mp.SSQMs)
}
