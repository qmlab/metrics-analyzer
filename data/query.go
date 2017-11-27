package data

import (
	"encoding/json"
	"fmt"
)

// query is the data model for query metrics

type Properties struct {
	ElapsedMs     int64         `json:"dqs_elapsed_ms"`
	ProjectID     string        `json:"distinct_id"`
	QueryID       string        `json:"query_id"`
	Result        bool          `json:"success"`
	RequestParams RequestParams `json:"request_params"`
	Source        string        `json:"source"`
	Subquery      struct {
		RespSentAt int64  `json:"lqs_resp_sent_at"`
		ReqRecvAt  int64  `json:"lqs_req_received_at"`
		ElapsedMs  int64  `json:"lqs_elapsed_ms"`
		Hostname   string `json:"lqs_hostname"`
	} `json:"slowest_subquery"`
	Time             int64  `json:"time"`
	TotalWorkerCPUMs int64  `json:"lqs_total_cpu_ms"`
	Unit             string `json:"unit"`
}

type RequestParams struct {
	FromDate  string `json:"from_date"`
	ToDate    string `json:"to_date"`
	QueryPool string `json:"query_pool"`

	Selector         string `json:"selector"`                // list
	Queries          string `json:"queries"`                 // normal, funnel, history, retention, addiction
	Script           string `json:"script"`                  // jql
	PropertiesMethod string `json:"properties_query_method"` // properties
	RetentionType    string `json:"retention_type"`          // retention
}

type Query struct {
	Event      string `json:"event"`
	Properties Properties
}

func NewQuery(data []byte) (*Query, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("[Query]No data")
	}

	q := &Query{}
	err := json.Unmarshal(data, q)
	return q, err
}

// GetQuerySignature - returns the hash value of the query signature based on key properties and query params
func (p *Properties) GetQuerySignature() (int64, error) {
	//TODO
	return 0, nil
}

// GenerateFakeClone - clone a query with randomly generated measurements but same signature
func (p *Query) GenerateFakeClone() (*Query, error) {
	//TODO
	return nil, nil
}
