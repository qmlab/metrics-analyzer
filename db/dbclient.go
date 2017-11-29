package db

import (
	"encoding/json"
	"fmt"
	"log"

	"../config"

	client "github.com/influxdata/influxdb/client/v2"
)

type DBClient struct {
	Config *config.Config
	Client client.Client
	Logger *log.Logger
}

func (c *DBClient) Init(config *config.Config, logger *log.Logger) error {
	if config == nil {
		return fmt.Errorf("[DBClient]No config")
	}

	httpClient, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     config.DB.Address,
		Username: config.DB.Username,
		Password: config.DB.Password,
	})

	if err != nil {
		return err
	}

	c.Config, c.Client, c.Logger = config, httpClient, logger
	return nil
}

func (c *DBClient) ExecuteQuery(cmd, db, precision string) (*client.Response, error) {
	q := client.NewQuery(cmd, db, precision)
	return c.Client.Query(q)
}

// GetMinutelyRate computes the per minute rate of a measurement per query signature
func (c *DBClient) GetMinutelyRate(groupkey, query string, minutes int) (map[string]float64, error) {
	r, err := c.ExecuteQuery(query, "testdb", "")
	if err != nil {
		return nil, err
	}

	m := make(map[string]float64)
	if len(r.Results) == 0 {
		return nil, fmt.Errorf("no result")
	}

	for _, s := range r.Results[0].Series {
		key := s.Tags[groupkey]
		number, err := s.Values[0][1].(json.Number).Float64()
		if err != nil {
			return nil, err
		}

		m[key] = number / float64(minutes)
	}

	return m, nil
}

// GetTotal computes the total of a measurement per query signature
func (c *DBClient) GetTotal(groupkey, query string) (map[string]float64, error) {
	r, err := c.ExecuteQuery(query, "testdb", "")
	if err != nil {
		return nil, err
	}

	m := make(map[string]float64)
	if len(r.Results) == 0 {
		return nil, fmt.Errorf("no result")
	}

	for _, s := range r.Results[0].Series {
		key := s.Tags[groupkey]
		number, err := s.Values[0][1].(json.Number).Float64()
		if err != nil {
			return nil, err
		}

		m[key] = float64(number)
	}

	return m, nil
}
