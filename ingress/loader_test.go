package ingress

import (
	"encoding/json"
	"log"
	"os"
	"testing"

	"../config"
	"github.com/stretchr/testify/assert"
)

const testFile = "../data/testdata/query_output.txt"
const configDir = "../config"

func TestBasicDB(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "DataLoader", log.LstdFlags)
	l, err := NewLoader(conf, logger)
	assert.Nil(t, err)
	CreateDB(t, l)
	DropDB(t, l)
}

func TestBasicLoadDB(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "DataLoader", log.LstdFlags)
	l, _ := NewLoader(conf, logger)
	CreateDB(t, l)
	err := l.InsertData(testFile, "testdb", 0)
	assert.Nil(t, err)
	QueryDB(t, l)
	DropDB(t, l)
}

func TestGroupBySignature(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "DataLoader", log.LstdFlags)
	l, _ := NewLoader(conf, logger)
	CreateDB(t, l)
	err := l.InsertData(testFile, "testdb", 0)
	assert.Nil(t, err)
	QueryGroupBy(t, l)
	DropDB(t, l)
}

func Test100kInserts(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "DataLoader", log.LstdFlags)
	l, _ := NewLoader(conf, logger)
	CreateDB(t, l)
	err := l.InsertData(testFile, "testdb", 1e6)
	assert.Nil(t, err)
	DropDB(t, l)
}

func CreateDB(t *testing.T, l *Loader) {
	_, err := l.ExecuteQuery("CREATE DATABASE testdb", "", "")
	assert.Nil(t, err)
}

func DropDB(t *testing.T, l *Loader) {
	_, err := l.ExecuteQuery("DROP DATABASE testdb", "", "")
	assert.Nil(t, err)
}

func QueryDB(t *testing.T, l *Loader) {
	r, err := l.ExecuteQuery("Select * FROM mp_query", "testdb", "")
	assert.Nil(t, err)
	m := r.Results[0].Series[0]
	for i, c := range m.Columns {
		v := m.Values[0][i]
		switch c {
		case "Event":
			assert.Equal(t, "dqs-query", v)
		case "ProjectID":
			assert.Equal(t, "1255222", v)
		case "FromDate":
			assert.Equal(t, json.Number("1511136000"), v)
		case "TotalWorkerCPUMs":
			assert.Equal(t, json.Number("4400"), v)
		}
	}
}

func QueryGroupBy(t *testing.T, l *Loader) {
	r, err := l.ExecuteQuery("select SUM(QueryMs) from mp_query where time >= now()-1000d group by Signature", "testdb", "")
	assert.Nil(t, err)
	assert.Equal(t, json.Number("3792"), r.Results[0].Series[0].Values[0][1])
	assert.Equal(t, json.Number("338"), r.Results[0].Series[1].Values[0][1])
}
