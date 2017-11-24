package data

import "time"

type MPQuery struct {
	Event            string    `json:"event"`
	ProjectID        string    `json:"distinct_id"`
	QueryID          string    `json:"query_id"`
	QueryMs          int64     `json:"dqs_elapsed_ms"`
	TotalWorkerCPUMs int64     `json:"lqs_total_cpu_ms"`
	Result           bool      `json:"success"`
	Source           string    `json:"source"`
	Unit             string    `json:"unit"`
	SSQMs            int64     `json:"lqs_elapsed_ms"`
	SSQHostname      string    `json:"lqs_hostname"`
	FromDate         time.Time `json:"from_date"`
	ToDate           time.Time `json:"to_date"`
	QueryPool        string    `json:"query_pool"`
	Selector         string    `json:"selector"`                // list
	Queries          string    `json:"queries"`                 // normal, funnel, history, retention, addiction
	Script           string    `json:"script"`                  // jql
	PropertiesMethod string    `json:"properties_query_method"` // properties
	RetentionType    string    `json:"retention_type"`          // retention

	Time int64 `json:"time"`
}

func NewMPQuery(q *Query) *MPQuery {
	if q == nil {
		return nil
	}

	mq := &MPQuery{
		Event:            q.Event,
		ProjectID:        q.Properties.ProjectID,
		QueryID:          q.Properties.QueryID,
		QueryMs:          q.Properties.ElapsedMs,
		TotalWorkerCPUMs: q.Properties.TotalWorkerCPUMs,
		Result:           q.Properties.Result,
		Time:             q.Properties.Time,
		Source:           q.Properties.Source,
		Unit:             q.Properties.Unit,
		SSQMs:            q.Properties.Subquery.RespSentAt - q.Properties.Subquery.ReqRecvAt,
		SSQHostname:      q.Properties.Subquery.Hostname,
		FromDate:         q.Properties.RequestParams.FromDate,
		ToDate:           q.Properties.RequestParams.ToDate,
		QueryPool:        q.Properties.RequestParams.QueryPool,
		Selector:         q.Properties.RequestParams.Selector,
		Queries:          q.Properties.RequestParams.Queries,
		Script:           q.Properties.RequestParams.Script,
		PropertiesMethod: q.Properties.RequestParams.PropertiesMethod,
		RetentionType:    q.Properties.RequestParams.RetentionType,
	}

	return mq
}
