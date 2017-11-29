package analysis

// subquery_analysis is for subquery-level query_analysis, which includes:
// 1.slowest subquery elapsed time percentile by host
// 2.slowest subquery elapsed time percentile by pool

import (
	"fmt"
)

func (a *QueryAnalyzer) GetSSQElapsedByHost(days int, percentile int) (map[string]float64, error) {
	q := fmt.Sprintf("select PERCENTILE(SSQMs, %d) from mp_query where time >= now()-%dd group by SSQHostname", percentile, days)
	return a.GetTotal("SSQHostname", q)
}

func (a *QueryAnalyzer) GetSSQElapsedByPool(days int, percentile int) (map[string]float64, error) {
	q := fmt.Sprintf("select PERCENTILE(SSQMs, %d) from mp_query where time >= now()-%dd group by QueryPool", percentile, days)
	return a.GetTotal("QueryPool", q)
}
