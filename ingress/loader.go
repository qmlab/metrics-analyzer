package ingress

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"

	"../config"
	client "github.com/influxdata/influxdb/client/v2"
)

// Loader is for loading cached files, generate fake points and insert to store

type Loader struct {
	file   string
	config *config.Config
	client client.Client
	logger *log.Logger
}

func NewLoader(file string, config *config.Config, logger *log.Logger) (*Loader, error) {
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
		file:   file,
		config: config,
		client: httpClient,
		logger: logger,
	}

	return l, nil
}

func (l *Loader) LoadData() error {
	f, err := os.Open(l.file)
	if err != nil {
		return err
	}

	defer f.Close()

	reader := bufio.NewReader(f)
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  l.config.DB.Address,
		Precision: "s",
	})
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if len(line) == 0 {
			continue
		}

		point, err := parse(line)

		if err != nil {
			continue
		}
	}

	return nil
}

func parse(line []byte) (*client.Point, error) {
	if len(line) == 0 {
		return nil, fmt.Errorf("[Loader]Empty line to parse")
	}

}
