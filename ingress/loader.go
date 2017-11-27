package ingress

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"../config"
	"../data"
	client "github.com/influxdata/influxdb/client/v2"
)

// Loader is for loading cached files, generate fake points and insert to store

type Loader struct {
	config *config.Config
	client client.Client
	logger *log.Logger
}

func NewLoader(config *config.Config, logger *log.Logger) (*Loader, error) {
	if config == nil {
		return nil, fmt.Errorf("[Loader]No config")
	}

	httpClient, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     config.DB.Address,
		Username: config.DB.Username,
		Password: config.DB.Password,
	})

	if err != nil {
		return nil, err
	}

	l := &Loader{
		config: config,
		client: httpClient,
		logger: logger,
	}

	return l, nil
}

func (l *Loader) InsertData(file, db string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}

	defer f.Close()

	reader := bufio.NewReader(f)
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  db,
		Precision: "s",
	})

	for {
		line, erl := reader.ReadBytes('\n')
		if erl == io.EOF {
			break
		}
		if len(line) == 0 {
			continue
		}

		pt, erp := parse(line)
		bp.AddPoint(pt)

		if erp != nil {
			//debugging
			println(erp.Error())
			continue
		}
	}

	// Write the batch
	err = l.client.Write(bp)

	return err
}

func (l *Loader) ExecuteQuery(cmd, db, precision string) (*client.Response, error) {
	q := client.NewQuery(cmd, db, precision)
	return l.client.Query(q)
}

func parse(line []byte) (*client.Point, error) {
	if len(line) == 0 {
		return nil, fmt.Errorf("[Loader]Empty line to parse")
	}

	q, err := data.NewQuery(line)
	if err != nil {
		return nil, err
	}

	mp, _ := data.NewMPQuery(q)
	tags := map[string]string{
		"Event":            mp.Event,
		"ProjectID":        mp.ProjectID,
		"QueryID":          mp.QueryID,
		"Source":           mp.Source,
		"Unit":             mp.Unit,
		"SSQHostname":      mp.SSQHostname,
		"QueryPool":        mp.QueryPool,
		"Selector":         mp.Selector,
		"Queries":          mp.Queries,
		"Script":           mp.Script,
		"PropertiesMethod": mp.PropertiesMethod,
		"RetentionType":    mp.RetentionType,
	}

	fields := map[string]interface{}{
		"QueryMs":          mp.QueryMs,
		"TotalWorkerCPUMs": mp.TotalWorkerCPUMs,
		"Result":           mp.Result,
		"SSQMs":            mp.SSQMs,
		"FromDate":         mp.FromDate,
		"ToDate":           mp.ToDate,
	}

	pt, err := client.NewPoint("mp_query", tags, fields, time.Unix(mp.Time, 0))
	if err != nil {
		return nil, err
	}

	return pt, nil
}
