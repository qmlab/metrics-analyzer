package ingress

import (
	"fmt"
	"log"

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
