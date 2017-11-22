package data

import "time"

// query is the data model for query metrics

type Properties struct {
	ProjectId        string `json:"distinct_id"`
	QueryId          string `json:"query_id"`
	ElapsedMs        int64  `json:"dqs_elapsed_ms"`
	TotalWorkerCPUMs int64  `json:"lqs_total_cpu_ms"`
	Result           bool   `json:"success"`
	Time             uint64 `json:"time"`
	Source           string `json:"source"`
	Unit             string `json:"unit"`
	Subquery         Subquery
	RequestParams    RequestParams
}

type Subquery struct {
	ElapsedMs int64  `json:"lqs_elapsed_ms"`
	Hostname  string `json:"lqs_hostname"`
}

type RequestParams struct {
	FromDate  time.Time `json:"from_date"`
	ToDate    time.Time `json:"to_date"`
	QueryPool string    `json:"query_pool"`

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

// GetQuerySignature - returns the hash value of the query signature based on key properties and query params
func (p *Properties) GetQuerySignature() int64 {
	//TODO
	return 0
}
