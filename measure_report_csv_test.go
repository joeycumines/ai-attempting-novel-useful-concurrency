package bigbuff

// CSV exports: deterministic metrics_wide and scores files written beside the report.

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// --- CSV exports ---

func writeReportCSVs(metricsPath, scoresPath string, data *reportData, derived derivedData) error {
	// metrics_wide.csv
	if metricsPath == "" {
		metricsPath = scoresPath + ".metrics.csv"
	}
	mf, err := os.Create(metricsPath)
	if err != nil {
		return err
	}
	mw := csv.NewWriter(mf)
	if err := mw.Write([]string{"impl", "family", "metric", "dims", "unit", "ops", "n", "mean", "stddev", "cv", "min", "p50", "p90", "p99", "max", "ci95_halfwidth", "ci95_low", "ci95_high", "lower_is_better", "expected_good_fit", "exposure_limited"}); err != nil {
		return err
	}
	for _, m := range data.metrics {
		dims, _ := json.Marshal(m["dims"])
		row := []string{
			fmt.Sprint(m["impl"]), fmt.Sprint(m["family"]), fmt.Sprint(m["metric"]),
			string(dims), fmt.Sprint(m["unit"]), fmt.Sprint(m["ops"]), fmt.Sprint(m["n"]),
			ffloat(m["mean"]), ffloat(m["stddev"]), ffloat(m["cv"]), ffloat(m["min"]),
			ffloat(m["p50"]), ffloat(m["p90"]), ffloat(m["p99"]), ffloat(m["max"]),
			ffloat(m["ci95_halfwidth"]), ffloat(m["ci95_low"]), ffloat(m["ci95_high"]),
			fmt.Sprint(m["lower_is_better"]), fmt.Sprint(m["expected_good_fit"]), fmt.Sprint(m["exposure_limited"]),
		}
		if err := mw.Write(row); err != nil {
			return err
		}
	}
	mw.Flush()
	mf.Close()

	// scores.csv — deterministic order: family asc, score desc, impl asc.
	if scoresPath == "" {
		scoresPath = metricsPath + ".scores.csv"
	}
	sf, err := os.Create(scoresPath)
	if err != nil {
		return err
	}
	sw := csv.NewWriter(sf)
	if err := sw.Write([]string{"family", "impl", "score", "verdict", "baseline", "integrity", "throughput", "latency", "allocations", "churn", "scaling", "stall_resilience", "expected_good_fit", "exposure_limited"}); err != nil {
		return err
	}
	families := make([]string, 0, len(derived.Scores))
	for family := range derived.Scores {
		families = append(families, family)
	}
	sort.Strings(families)
	for _, family := range families {
		scores := make([]implScore, 0, len(derived.Scores[family]))
		for _, s := range derived.Scores[family] {
			scores = append(scores, s)
		}
		sort.Slice(scores, func(i, j int) bool {
			if scores[i].Score != scores[j].Score {
				return scores[i].Score > scores[j].Score
			}
			return scores[i].Impl < scores[j].Impl
		})
		for _, s := range scores {
			// An unranked implementation (family below minRankedImpls) has
			// empty score and dimension cells, never a zero.
			num := func(v int) string {
				if !s.Ranked {
					return ""
				}
				return fmt.Sprint(v)
			}
			// A dimension the implementation has no data for is empty too.
			dim := func(key string) string {
				for _, missing := range s.DimsMissing {
					if missing == key {
						return ""
					}
				}
				return num(s.Dims[key])
			}
			row := []string{
				family, s.Impl, num(s.Score), s.Verdict, fmt.Sprint(s.Baseline), s.Integrity,
				dim("throughput"), dim("latency"), dim("allocations"),
				dim("churn"), dim("scaling"), dim("stall_resilience"),
				fmt.Sprint(s.ExpectedGoodFit), fmt.Sprint(s.ExposureLimited),
			}
			if err := sw.Write(row); err != nil {
				return err
			}
		}
	}
	sw.Flush()
	sf.Close()
	return nil
}

func ffloat(v any) string {
	if f, ok := v.(float64); ok {
		return fmt.Sprintf("%.6g", f)
	}
	return ""
}
