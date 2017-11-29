package analysis

// subquery_analysis is for subquery-level query_analysis, which includes:
// 1.slowest subquery elapsed time percentile by host
// 2.slowest subquery elapsed time percentile by pool

import (
	"fmt"
	"log"

	"../config"
	"../db"
)

type SubQueryAnalyzer struct {
	db.DBClient
}

func NewSubQueryAnalyzer(config *config.Config, logger *log.Logger) (*SubQueryAnalyzer, error) {
	a := &SubQueryAnalyzer{}
	err := a.DBClient.Init(config, logger)

	return a, err
}

func (a *SubQueryAnalyzer) GetSSQElapsedByHost(days int, percentile int) (map[string]float64, error) {
	q := fmt.Sprintf("select PERCENTILE(SSQMs, %d) from mp_query where time >= now()-%dd group by SSQHostname", percentile, days)
	return a.GetTotal("SSQHostname", q)
}

func (a *SubQueryAnalyzer) GetSSQElapsedByPool(days int, percentile int) (map[string]float64, error) {
	q := fmt.Sprintf("select PERCENTILE(SSQMs, %d) from mp_query where time >= now()-%dd group by QueryPool", percentile, days)
	return a.GetTotal("QueryPool", q)
}
