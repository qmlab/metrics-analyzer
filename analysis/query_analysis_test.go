package analysis

import (
	"log"
	"os"
	"testing"

	"../config"
	"../ingress"
	"../util"
	"github.com/stretchr/testify/assert"
)

func TestGetElapsedRate(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "QueryAnalyzer", log.LstdFlags)
	a, _ := NewQueryAnalyzer(conf, logger)
	l, _ := ingress.NewLoader(conf, logger)
	util.CreateDB(t, a.DBClient)
	err := l.InsertData(testFile, "testdb", 0)
	assert.Nil(t, err)

	m, err := a.GetElapsedRate(10)
	assert.Nil(t, err)
	for _, v := range m {
		assert.True(t, v > 0)
	}

	util.DropDB(t, a.DBClient)
}

func TestGetTotalWorkerCPURate(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "QueryAnalyzer", log.LstdFlags)
	a, _ := NewQueryAnalyzer(conf, logger)
	l, _ := ingress.NewLoader(conf, logger)
	util.CreateDB(t, a.DBClient)
	err := l.InsertData(testFile, "testdb", 0)
	assert.Nil(t, err)

	m, err := a.GetTotalWorkerCPURate(10)
	assert.Nil(t, err)
	for _, v := range m {
		assert.True(t, v > 0)
	}

	util.DropDB(t, a.DBClient)
}

func TestGetSuccessRate(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "QueryAnalyzer", log.LstdFlags)
	a, _ := NewQueryAnalyzer(conf, logger)
	l, _ := ingress.NewLoader(conf, logger)
	util.CreateDB(t, a.DBClient)
	err := l.InsertData(testFile, "testdb", 0)
	assert.Nil(t, err)

	m, err := a.GetSuccessRate(10)
	assert.Nil(t, err)
	for _, v := range m {
		assert.True(t, v > 0)
	}

	util.DropDB(t, a.DBClient)
}

func TestGetElapsedRateN(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "QueryAnalyzer", log.LstdFlags)
	a, _ := NewQueryAnalyzer(conf, logger)
	l, _ := ingress.NewLoader(conf, logger)
	util.CreateDB(t, a.DBClient)
	err := l.InsertData(testFile, "testdb", 1e4)
	assert.Nil(t, err)

	m, err := a.GetElapsedRate(10)
	assert.Nil(t, err)
	for _, v := range m {
		//debugging
		// fmt.Printf("%s: %f\n", k, v)
		assert.True(t, v > 0)
	}

	util.DropDB(t, a.DBClient)
}

func TestGetTotalWorkerCPURateN(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "QueryAnalyzer", log.LstdFlags)
	a, _ := NewQueryAnalyzer(conf, logger)
	l, _ := ingress.NewLoader(conf, logger)
	util.CreateDB(t, a.DBClient)
	err := l.InsertData(testFile, "testdb", 1e4)
	assert.Nil(t, err)

	m, err := a.GetTotalWorkerCPURate(10)
	assert.Nil(t, err)
	for _, v := range m {
		//debugging
		// fmt.Printf("%s: %f\n", k, v)
		assert.True(t, v > 0)
	}

	util.DropDB(t, a.DBClient)
}

func TestGetSuccessRateN(t *testing.T) {
	conf := config.NewConfig(configDir, config.OneBox)
	logger := log.New(os.Stdout, "QueryAnalyzer", log.LstdFlags)
	a, _ := NewQueryAnalyzer(conf, logger)
	l, _ := ingress.NewLoader(conf, logger)
	util.CreateDB(t, a.DBClient)
	err := l.InsertData(testFile, "testdb", 1e4)
	assert.Nil(t, err)

	m, err := a.GetSuccessRate(10)
	assert.Nil(t, err)
	for _, v := range m {
		//debugging
		// fmt.Printf("%s: %f\n", k, v)
		assert.True(t, v > 0)
	}

	util.DropDB(t, a.DBClient)
}
