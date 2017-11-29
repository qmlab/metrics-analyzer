package analysis

import (
	"fmt"
	"log"

	"../config"
	"../db"
)

// query_analysis is for query-level query_analysis, which includes:
// 1.impact score of recent query elapsed
// 2.impact score of recent query workers' total cpu time
// 3.recent success rate
type QueryAnalyzer struct {
	db.DBClient
}

func NewQueryAnalyzer(config *config.Config, logger *log.Logger) (*QueryAnalyzer, error) {
	a := &QueryAnalyzer{}
	err := a.DBClient.Init(config, logger)

	return a, err
}

func (a *QueryAnalyzer) GetElapsedRate(days int) (map[string]float64, error) {
	q := fmt.Sprintf("select SUM(QueryMs) from mp_query where time >= now()-%dd group by Signature", days)
	return a.GetMinutelyRate("Signature", q, days*24*60)
}

func (a *QueryAnalyzer) GetTotalWorkerCPURate(days int) (map[string]float64, error) {
	q := fmt.Sprintf("select SUM(TotalWorkerCPUMs) from mp_query where time >= now()-%dd group by Signature", days)
	return a.GetMinutelyRate("Signature", q, days*24*60)
}

func (a *QueryAnalyzer) GetSuccessRate(days int) (map[string]float64, error) {
	qs := fmt.Sprintf("select COUNT(QueryMs) from mp_query where time >= now()-%dd and Result = true group by Signature", days)
	qa := fmt.Sprintf("select COUNT(QueryMs) from mp_query where time >= now()-%dd group by Signature", days)
	rs, err := a.GetTotal("Signature", qs)
	if err != nil {
		return nil, err
	}
	ra, err := a.GetTotal("Signature", qa)
	if err != nil {
		return nil, err
	}

	rst := make(map[string]float64)
	for k, v := range ra {
		if vs, ok := rs[k]; ok && v != 0 {
			rst[k] = vs / v
		} else {
			rst[k] = 0
		}
	}

	return rst, nil
}
