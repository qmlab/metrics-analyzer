package ingress

import (
	"encoding/json"
	"log"
	"os"
	"testing"

	"../config"
	"../util"
	"github.com/stretchr/testify/assert"
)

const testFile = "../data/testdata/query_output.txt"
const configDir = "../config"

func TestBasicDB(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "DataLoader", log.LstdFlags)
	l, err := NewLoader(conf, logger)
	assert.Nil(t, err)
	util.CreateDB(t, l.DBClient)
	util.DropDB(t, l.DBClient)
}

func TestBasicLoadDB(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "DataLoader", log.LstdFlags)
	l, _ := NewLoader(conf, logger)
	util.CreateDB(t, l.DBClient)
	err := l.InsertData(testFile, "testdb", 0)
	assert.Nil(t, err)
	QueryDB(t, l)
	util.DropDB(t, l.DBClient)
}

func TestGroupBySignature(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "DataLoader", log.LstdFlags)
	l, _ := NewLoader(conf, logger)
	util.CreateDB(t, l.DBClient)
	err := l.InsertData(testFile, "testdb", 0)
	assert.Nil(t, err)
	QueryGroupBy(t, l)
	util.DropDB(t, l.DBClient)
}

func TestInsertsN(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "DataLoader", log.LstdFlags)
	l, _ := NewLoader(conf, logger)
	util.CreateDB(t, l.DBClient)
	err := l.InsertData(testFile, "testdb", 1e4)
	assert.Nil(t, err)
	util.DropDB(t, l.DBClient)
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
	assert.Equal(t, "e8c421b8560012ea9531bb3c20526628", r.Results[0].Series[1].Tags["Signature"])
}
