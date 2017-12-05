package data

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

type MPQuery struct {
	Event     string `json:"event"`
	ProjectID string `json:"distinct_id"`
	// QueryID          string `json:"query_id"`
	QueryMs          int64  `json:"dqs_elapsed_ms"`
	TotalWorkerCPUMs int64  `json:"lqs_total_cpu_ms"`
	Result           bool   `json:"success"`
	Source           string `json:"source"`
	Unit             string `json:"unit"`
	SSQMs            int64  `json:"lqs_elapsed_ms"`
	SSQHostname      string `json:"lqs_hostname"`
	FromDate         int64  `json:"from_date"`
	ToDate           int64  `json:"to_date"`
	QueryPool        string `json:"query_pool"`
	Selector         string `json:"selector"` // list
	// Queries          string `json:"queries"`                 // normal, funnel, history, retention, addiction
	// Script           string `json:"script"`                  // jql
	PropertiesMethod string `json:"properties_query_method"` // properties
	RetentionType    string `json:"retention_type"`          // retention
	Signature        string `json:"signature"`               // signature of MP query tags

	Time int64 `json:"time"`
}

func NewMPQuery(q *Query) (*MPQuery, error) {
	if q == nil {
		return nil, fmt.Errorf("Empty query")
	}

	fromDate, _ := time.Parse("2006-01-02T15:04:05", q.Properties.RequestParams.FromDate)
	toDate, _ := time.Parse("2006-01-02T15:04:05", q.Properties.RequestParams.ToDate)

	mq := &MPQuery{
		Event:     q.Event,
		ProjectID: q.Properties.ProjectID,
		// QueryID:          q.Properties.QueryID,
		QueryMs:          q.Properties.ElapsedMs,
		TotalWorkerCPUMs: q.Properties.TotalWorkerCPUMs,
		Result:           q.Properties.Result,
		Time:             q.Properties.Time,
		Source:           q.Properties.Source,
		Unit:             q.Properties.Unit,
		SSQMs:            q.Properties.Subquery.RespSentAt - q.Properties.Subquery.ReqRecvAt,
		SSQHostname:      q.Properties.Subquery.Hostname,
		FromDate:         fromDate.Unix(),
		ToDate:           toDate.Unix(),
		QueryPool:        q.Properties.RequestParams.QueryPool,
		Selector:         q.Properties.RequestParams.Selector,
		// Queries:          q.Properties.RequestParams.Queries,
		// Script:           q.Properties.RequestParams.Script,
		PropertiesMethod: q.Properties.RequestParams.PropertiesMethod,
		RetentionType:    q.Properties.RequestParams.RetentionType,
		Signature:        GetSignature(q),
	}

	return mq, nil
}

func GetSignature(q *Query) string {
	text := strings.Join([]string{
		q.Event,
		q.Properties.ProjectID,
		q.Properties.Unit,
		q.Properties.RequestParams.FromDate,
		q.Properties.RequestParams.ToDate,
		q.Properties.RequestParams.QueryPool,
		q.Properties.RequestParams.Selector,
		// q.Properties.RequestParams.Queries,
		// q.Properties.RequestParams.Script,
		q.Properties.RequestParams.PropertiesMethod,
		q.Properties.RequestParams.RetentionType,
	}, "|")

	hasher := md5.New()
	hasher.Write([]byte(text))

	return hex.EncodeToString(hasher.Sum(nil))
}

// Generate N mutates
func (mp *MPQuery) MutateN(n int) []*MPQuery {
	arr := make([]*MPQuery, n)
	for i := 0; i < n; i++ {
		arr[i] = mp.MutateOne(i, n)
	}

	return arr
}

// Mutate generates a new query with same signature but different times and measurements
func (mp *MPQuery) MutateOne(i, n int) *MPQuery {
	np := &MPQuery{
		Event:     mp.Event,
		ProjectID: mp.ProjectID,
		// QueryID:          mp.QueryID,
		QueryMs:          mp.QueryMs,
		TotalWorkerCPUMs: mp.TotalWorkerCPUMs,
		Result:           mp.Result,
		Time:             mp.Time,
		Source:           mp.Source,
		Unit:             mp.Unit,
		SSQMs:            mp.SSQMs,
		SSQHostname:      mp.SSQHostname,
		FromDate:         mp.FromDate,
		ToDate:           mp.ToDate,
		QueryPool:        mp.QueryPool,
		Selector:         mp.Selector,
		// Queries:          mp.Queries,
		// Script:           mp.Script,
		PropertiesMethod: mp.PropertiesMethod,
		RetentionType:    mp.RetentionType,
		Signature:        mp.Signature,
	}

	choice := rand.Intn(3)
	switch choice {
	case 0:
		np.QueryMs = rand.Int63n(240 * 1e3)
	case 1:
		np.TotalWorkerCPUMs = rand.Int63n(240 * 1e3)
	case 2:
		np.SSQMs = rand.Int63n(240 * 1e3)
	}

	np.Time = time.Now().Unix() - int64(n-i)
	return np
}
