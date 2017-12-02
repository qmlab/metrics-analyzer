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
	"../db"
	client "github.com/influxdata/influxdb/client/v2"
)

// Loader is for loading cached files, generate fake points and insert to store

const MaxDataBatch = 1000

type Loader struct {
	db.DBClient
}

func NewLoader(config *config.Config, logger *log.Logger) (*Loader, error) {
	l := &Loader{}
	err := l.DBClient.Init(config, logger)
	return l, err
}

func (l *Loader) InsertData(file, db string, mutations int) error {
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

	count := 0
	for {
		line, erl := reader.ReadBytes('\n')
		if erl == io.EOF {
			break
		}
		if len(line) == 0 {
			continue
		}

		mp, erp := parse(line)
		if erp != nil {
			//debugging
			println("[debug]", erp.Error())
			continue
		}

		pt, erc := generatePoint(mp)
		if erp != nil {
			//debugging
			println("[debug]", erc.Error())
			continue
		}

		bp.AddPoint(pt)
		count++

		for _, mmp := range mp.MutateN(mutations) {
			if count >= MaxDataBatch {
				err := l.Client.Write(bp)

				// reset
				count = 0
				bp, _ = client.NewBatchPoints(client.BatchPointsConfig{
					Database:  db,
					Precision: "s",
				})

				if err != nil {
					return err
				}
			}

			mpt, erc := generatePoint(mmp)
			if erp != nil {
				//debugging
				println("[debug]", erc.Error())
				continue
			}

			bp.AddPoint(mpt)
			count++
		}

		if count >= MaxDataBatch {
			err := l.Client.Write(bp)

			// reset
			count = 0
			bp, _ = client.NewBatchPoints(client.BatchPointsConfig{
				Database:  db,
				Precision: "s",
			})

			if err != nil {
				return err
			}
		}
	}

	// Flush the batch
	r := l.Flush(bp, count)
	return r
}

func (l *Loader) Flush(bp client.BatchPoints, count int) error {
	if count > 0 {
		// Write the batch
		return l.Client.Write(bp)
	}

	return nil
}

func parse(line []byte) (*data.MPQuery, error) {
	if len(line) == 0 {
		return nil, fmt.Errorf("[Loader]Empty line to parse")
	}

	q, err := data.NewQuery(line)
	if err != nil {
		return nil, err
	}

	mp, err := data.NewMPQuery(q)
	return mp, err
}

func generatePoint(mp *data.MPQuery) (*client.Point, error) {
	tags := map[string]string{
		"Event":       mp.Event,
		"ProjectID":   mp.ProjectID,
		"QueryID":     mp.QueryID,
		"Source":      mp.Source,
		"Unit":        mp.Unit,
		"SSQHostname": mp.SSQHostname,
		"QueryPool":   mp.QueryPool,
		"Selector":    mp.Selector,
		// "Queries":          mp.Queries,	// problematic > 3xx bytes
		// "Script":           mp.Script,
		"PropertiesMethod": mp.PropertiesMethod,
		"RetentionType":    mp.RetentionType,
		"Signature":        mp.Signature,
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
