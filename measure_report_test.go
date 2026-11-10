package bigbuff

// Interactive HTML report generator. Reads a bigbuff-measure JSONL results
// file (and the registry), then emits a fully self-contained HTML file: no
// external assets, no network, every chart rendered client-side from the
// embedded dataset. The same embedded dataset powers JSON/CSV downloads.
//
// The report is designed to answer, per implementation: what does it cost,
// how does it behave under load, what happens when a consumer stalls, what
// does it retain, and would a staff engineer adopt it? It is generated from
// data; no numbers are hard-coded.

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func cmdMeasureReport(args []string) int {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	in := fs.String("in", "", "bigbuff-measure results file (default: newest results-*-01-results.jsonl)")
	out := fs.String("out", "", "output HTML report path (default: same run group, -03-report.html)")
	registryPath := fs.String("registry", "", "registry JSON (default: dumped live from the test binary)")
	csvMetrics := fs.String("csv-metrics", "", "optional output path for the metrics_wide CSV")
	csvScores := fs.String("csv-scores", "", "optional output path for the scores CSV")
	csvDir := fs.String("csv", "", "deprecated alias: write <dir>/metrics_wide.csv and <dir>/scores.csv")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *in == "" {
		*in = latestRunResultsJSONL(".")
		if *in == "" {
			fmt.Fprintf(os.Stderr, "report: no results-*-01-results.jsonl found; pass --in explicitly\n")
			return 2
		}
	}
	if *out == "" {
		*out = reportDefaultOut(*in)
	}
	data, err := loadReportData(*in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "report: load %s: %v\n", *in, err)
		return 1
	}
	registry := map[string]any{}
	if *registryPath != "" {
		raw, err := os.ReadFile(*registryPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "report: read registry: %v\n", err)
			return 1
		}
		var parsed map[string]any
		if err := json.Unmarshal(raw, &parsed); err != nil {
			fmt.Fprintf(os.Stderr, "report: parse registry: %v\n", err)
			return 1
		}
		registry = parsed
	} else {
		dump, err := registryDump()
		if err != nil {
			fmt.Fprintf(os.Stderr, "report: dump registry: %v\n", err)
			return 1
		}
		registry = dump
	}
	derived := deriveReport(data, registry)
	page, err := renderReportPage(data, registry, derived)
	if err != nil {
		fmt.Fprintf(os.Stderr, "report: render: %v\n", err)
		return 1
	}
	if dir := filepath.Dir(*out); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "report: mkdir: %v\n", err)
			return 1
		}
	}
	if err := os.WriteFile(*out, []byte(page), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "report: write: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "report: wrote %s (%d bytes)\n", *out, len(page))
	if *csvDir != "" {
		if *csvMetrics == "" {
			*csvMetrics = filepath.Join(*csvDir, "metrics_wide.csv")
		}
		if *csvScores == "" {
			*csvScores = filepath.Join(*csvDir, "scores.csv")
		}
	}
	if *csvMetrics != "" || *csvScores != "" {
		if err := writeReportCSVs(*csvMetrics, *csvScores, data, derived); err != nil {
			fmt.Fprintf(os.Stderr, "report: csv: %v\n", err)
			return 1
		}
		fmt.Fprintf(os.Stderr, "report: wrote CSVs (%s, %s)\n", *csvMetrics, *csvScores)
	}
	return 0
}

// reportDefaultOut derives the run-group-consistent report path for a results
// JSONL: results-NN-01-results.jsonl -> results-NN-03-report.html.
func reportDefaultOut(in string) string {
	base := filepath.Base(in)
	dir := filepath.Dir(in)
	m := runFileRe.FindStringSubmatch(base)
	if m == nil {
		return filepath.Join(dir, "report.html")
	}
	return filepath.Join(dir, "results-"+m[1]+"-03-report.html")
}

// --- Loading ---

type reportData struct {
	meta    map[string]any
	metrics []map[string]any
	traces  []map[string]any
	skipped []map[string]any
	summary map[string]any
}

func loadReportData(path string) (*reportData, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	d := &reportData{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 128<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("malformed record: %w", err)
		}
		switch rec["type"] {
		case "meta":
			d.meta = rec
		case "metric":
			d.metrics = append(d.metrics, rec)
		case "trace":
			d.traces = append(d.traces, rec)
		case "skipped":
			d.skipped = append(d.skipped, rec)
		case "summary":
			d.summary = rec
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if d.meta == nil {
		// Tolerate a results file without metadata (e.g. filtered), but say so.
		d.meta = map[string]any{"type": "meta", "generator": "bigbuff-measure", "profile": "unknown", "timestamp": ""}
	}
	return d, nil
}

// --- Derived structures & scoring ---

type metricKey struct {
	metric string
	impl   string
	subs   int // -1 when not dimensioned
}

func dimsSubs(rec map[string]any) int {
	dims, ok := rec["dims"].(map[string]any)
	if !ok {
		return -1
	}
	for _, k := range []string{"subs", "receivers", "senders"} {
		if v, ok := dims[k]; ok {
			switch x := v.(type) {
			case float64:
				return int(x)
			case int:
				return x
			}
		}
	}
	return -1
}

func recMean(rec map[string]any) (float64, bool) {
	v, ok := rec["mean"].(float64)
	if !ok {
		// stalled_consumer and torn_reads records carry no mean; derive one.
		if v2, ok := rec["ops_completed"].(float64); ok {
			return v2, true
		}
		return 0, false
	}
	return v, true
}

type implScore struct {
	Impl            string         `json:"impl"`
	Family          string         `json:"family"`
	Score           int            `json:"score"`
	Verdict         string         `json:"verdict"`
	Baseline        bool           `json:"baseline"`
	Integrity       string         `json:"integrity"` // "verified" | "failed" | "insufficient-data"
	Dims            map[string]int `json:"dims"`
	Metrics         map[string]any `json:"metrics"`
	Scenarios       []string       `json:"scenarios"`
	ExpectedGoodFit bool           `json:"expected_good_fit"`
	ExposureLimited bool           `json:"exposure_limited"`
}

type derivedData struct {
	Scores       map[string]map[string]implScore `json:"scores"` // family -> impl -> score
	Coverage     map[string]map[string]any       `json:"coverage"`
	Observations []map[string]any                `json:"observations"`
	FamilyOf     map[string]string               `json:"family_of"`
	Baselines    []string                        `json:"baselines"`
}

func isBaselineName(name string) bool {
	return strings.Contains(name, "Baseline") || strings.Contains(name, "(baseline)")
}

// percentileRank returns (fraction of values strictly worse than v) * 100.
// lowerIsBetter controls which direction is "worse" (we want 100 = best).
func percentileRank(values []float64, v float64, lowerIsBetter bool) int {
	n := len(values)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return 100
	}
	worse := 0
	for _, o := range values {
		if lowerIsBetter {
			if o > v {
				worse++
			}
		} else {
			if o < v {
				worse++
			}
		}
	}
	return int(math.Round(100 * float64(worse) / float64(n-1)))
}

func metricMeans(metrics []map[string]any, metric, family string, dimsSubsFilter int) map[string]float64 {
	out := map[string]float64{}
	for _, m := range metrics {
		if m["metric"] != metric || m["family"] != family {
			continue
		}
		if dimsSubsFilter >= 0 && dimsSubs(m) != dimsSubsFilter {
			continue
		}
		impl, _ := m["impl"].(string)
		if mean, ok := recMean(m); ok {
			out[impl] = mean
		}
	}
	return out
}

// higherIsBetterFlag reports whether the metric is better when larger.
func stalledOpsCompleted(metrics []map[string]any, family string) map[string]float64 {
	out := map[string]float64{}
	for _, m := range metrics {
		if m["metric"] != "stalled_consumer_ops_completed" || m["family"] != family {
			continue
		}
		impl, _ := m["impl"].(string)
		if v, ok := m["ops_completed"].(float64); ok {
			out[impl] = v
		}
	}
	return out
}

func integrityOf(metrics []map[string]any, impl, family string) (string, float64) {
	if family == "payload" {
		for _, m := range metrics {
			if m["metric"] == "torn_reads" && m["impl"] == impl {
				v, _ := m["violations"].(float64)
				checked, _ := m["checked"].(float64)
				if v > 0 {
					return "failed", v
				}
				if checked < 100 {
					return "insufficient-data", 0
				}
				return "verified", 0
			}
		}
		return "insufficient-data", 0
	}
	// Exactly-once accounting is an integrity gate, as torn reads are for payload
	// lanes: a violation summed over every sweep point fails the implementation,
	// and deriveReport caps its score.
	var violations float64
	for _, m := range metrics {
		if m["impl"] != impl || m["family"] != family {
			continue
		}
		if v, ok := m["exactly_once_violations"].(float64); ok {
			violations += v
		}
	}
	if violations > 0 {
		return "failed", violations
	}
	return "verified", 0
}

func deriveReport(data *reportData, registry map[string]any) derivedData {
	dd := derivedData{
		Scores:   map[string]map[string]implScore{},
		Coverage: map[string]map[string]any{},
		FamilyOf: map[string]string{},
	}
	families := map[string][]string{}
	for _, m := range data.metrics {
		fam, _ := m["family"].(string)
		impl, _ := m["impl"].(string)
		if fam == "" || impl == "" {
			continue
		}
		families[fam] = append(families[fam], impl)
		dd.FamilyOf[impl] = fam
	}
	for _, t := range data.traces {
		fam, _ := t["family"].(string)
		impl, _ := t["impl"].(string)
		if fam != "" && impl != "" {
			dd.FamilyOf[impl] = fam
		}
	}
	unique := func(xs []string) []string {
		m := map[string]bool{}
		out := []string{}
		for _, x := range xs {
			if !m[x] {
				m[x] = true
				out = append(out, x)
			}
		}
		sort.Strings(out)
		return out
	}

	for family, implsRaw := range families {
		impls := unique(implsRaw)
		thrDims := 8 // dims filter for the headlining throughput metric
		if family == "fanin" {
			thrDims = faninSenders
		}
		thrAll := metricMeans(data.metrics, map[string]string{ // metric name differs per family
			"broadcast": "publish_ns_op", "payload": "publish_ns_op", "caster": "send_ns_op", "fanin": "merge_ns_op",
		}[family], family, thrDims)
		latAll := metricMeans(data.metrics, map[string]string{
			"broadcast": "publish_latency_ns", "payload": "publish_latency_ns", "caster": "send_latency_ns",
		}[family], family, -1)
		var allocAll map[string]float64
		if family == "broadcast" {
			allocAll = metricMeans(data.metrics, "publish_allocs_op", "broadcast", 8)
		} else {
			allocAll = nil
		}
		churnAll := metricMeans(data.metrics, "churn_ns_op", family, -1)
		ns1 := metricMeans(data.metrics, map[string]string{
			"broadcast": "publish_ns_op", "payload": "publish_ns_op", "caster": "send_ns_op",
		}[family], family, 1)
		ns64 := metricMeans(data.metrics, map[string]string{
			"broadcast": "publish_ns_op", "payload": "publish_ns_op", "caster": "send_ns_op",
		}[family], family, 64)
		stallAll := stalledOpsCompleted(data.metrics, family)
		thrVals, latVals, allocVals, churnVals, stallVals, scaleVals := []float64{}, []float64{}, []float64{}, []float64{}, []float64{}, []float64{}
		scaleOf := map[string]float64{}
		for _, impl := range impls {
			if v, ok := thrAll[impl]; ok {
				thrVals = append(thrVals, v)
			}
			if v, ok := latAll[impl]; ok {
				latVals = append(latVals, v)
			}
			if allocAll != nil {
				if v, ok := allocAll[impl]; ok {
					allocVals = append(allocVals, v)
				}
			}
			if v, ok := churnAll[impl]; ok {
				churnVals = append(churnVals, v)
			}
			if v, ok := stallAll[impl]; ok {
				stallVals = append(stallVals, math.Log10(v+1))
			}
			lo, okLo := ns1[impl]
			hi, okHi := ns64[impl]
			if okLo && okHi && lo > 0 {
				scaleOf[impl] = hi / lo
				scaleVals = append(scaleVals, hi/lo)
			} else {
				scaleOf[impl] = 1
			}
		}

		dd.Scores[family] = map[string]implScore{}
		for _, impl := range impls {
			thr, hasThr := thrAll[impl]
			lat, hasLat := latAll[impl]
			alloc, hasAlloc := float64(0), false
			if allocAll != nil {
				alloc, hasAlloc = allocAll[impl]
			}
			churn, hasChurn := churnAll[impl]
			stall, hasStall := stallAll[impl]
			scale, hasScale := scaleOf[impl], true

			thrScore := 100
			if hasThr {
				thrScore = percentileRank(thrVals, thr, true)
			}
			latScore := 100
			if hasLat {
				latScore = percentileRank(latVals, lat, true)
			}
			allocScore := 100
			if allocAll != nil && hasAlloc {
				allocScore = percentileRank(allocVals, alloc, true)
			}
			churnScore := 100
			if hasChurn {
				churnScore = percentileRank(churnVals, churn, true)
			}
			stallScore := 100
			if hasStall {
				stallScore = percentileRank(stallVals, math.Log10(stall+1), false)
			}
			scaleScore := 100
			if hasScale && len(scaleVals) > 0 {
				scaleScore = percentileRank(scaleVals, scale, true)
			}

			// Weights documented in measurement.md.
			raw := 0.28*float64(thrScore) + 0.14*float64(latScore) + 0.14*float64(allocScore) +
				0.08*float64(churnScore) + 0.16*float64(scaleScore) + 0.20*float64(stallScore)

			integrity, _ := integrityOf(data.metrics, impl, family)
			if integrity == "failed" && raw > 50 {
				raw = 50
			}
			score := int(math.Round(raw))

			verdict := "Avoid for most uses"
			switch {
			case integrity == "failed":
				verdict = "Do not adopt (integrity failure)"
			case score >= 75:
				verdict = "Strong candidate"
			case score >= 60:
				verdict = "Situational (strengths offset by trade-offs)"
			case score >= 45:
				verdict = "Niche use only"
			}

			scen, goodFit, expLimit := scenariosOf(data.metrics, impl)

			metrics := map[string]any{}
			if hasThr {
				thrKey := "publish_ns_op@8"
				if family == "fanin" {
					thrKey = "merge_ns_op@4"
				}
				metrics[thrKey] = thr
			}
			if hasLat {
				metrics["publish_latency_ns@8"] = lat
			}
			if allocAll != nil && hasAlloc {
				metrics["publish_allocs_op@8"] = alloc
			}
			if hasChurn {
				metrics["churn_ns_op"] = churn
			}
			if hasStall {
				metrics["stalled_ops_completed"] = stall
			}
			if hasScale {
				metrics["scaling_ratio_64_over_1"] = scale
			}

			dd.Scores[family][impl] = implScore{
				Impl:            impl,
				Family:          family,
				Score:           score,
				Verdict:         verdict,
				Baseline:        isBaselineName(impl),
				Integrity:       integrity,
				Dims:            map[string]int{"throughput": thrScore, "latency": latScore, "allocations": allocScore, "churn": churnScore, "scaling": scaleScore, "stall_resilience": stallScore},
				Metrics:         metrics,
				Scenarios:       scen,
				ExpectedGoodFit: goodFit,
				ExposureLimited: expLimit,
			}
		}
	}

	for _, b := range []string{} {
		_ = b
	}

	// Baselines list.
	for impl := range dd.FamilyOf {
		if isBaselineName(impl) {
			dd.Baselines = append(dd.Baselines, impl)
		}
	}
	sort.Strings(dd.Baselines)

	// Coverage accounting (measured vs expected per family, against verify semantics).
	measured := map[string]map[string]bool{}
	for fam := range dd.Scores {
		measured[fam] = map[string]bool{}
		for impl := range dd.Scores[fam] {
			measured[fam][impl] = true
		}
	}
	for fam, set := range map[string][]string{
		"broadcast": broadcastFactoryNames(), "caster": casterNames(), "scatter": scatterNames(), "gather": gatherNames(), "payload": payloadNames(), "fanin": faninNames(),
	} {
		m := measured[fam]
		seen := 0
		missing := []string{}
		for _, impl := range set {
			if m != nil && m[impl] {
				seen++
			} else {
				missing = append(missing, impl)
			}
		}
		dd.Coverage[fam] = map[string]any{"expected": len(set), "measured": seen, "missing": missing}
	}

	// Observations: data-driven key findings.
	dd.Observations = computeObservations(data, dd)
	return dd
}

func broadcastFactoryNames() []string {
	out := []string{}
	for _, f := range AllBroadcasters() {
		p := f()
		out = append(out, p.Name())
		p.Close()
	}
	return out
}
func casterNames() []string {
	out := []string{}
	for name := range casterLikes() {
		out = append(out, name)
	}
	return append(out, "BaselineRendezvousCaster")
}
func scatterNames() []string {
	out := []string{}
	for name := range scatterLikes() {
		out = append(out, name)
	}
	return out
}
func gatherNames() []string {
	out := []string{}
	for name := range gatherLikes() {
		out = append(out, name)
	}
	return out
}
func payloadNames() []string {
	out := []string{}
	for name := range asyncImpls() {
		out = append(out, name)
	}
	return out
}
func faninNames() []string {
	out := []string{}
	for name := range faninLikes() {
		out = append(out, name)
	}
	return out
}

func scenariosOf(metrics []map[string]any, impl string) ([]string, bool, bool) {
	seen := map[string]bool{}
	goodFit, expLimit := false, false
	for _, m := range metrics {
		if m["impl"] != impl {
			continue
		}
		switch s := m["scenarios"].(type) {
		case []any:
			for _, x := range s {
				if str, ok := x.(string); ok {
					seen[str] = true
				}
			}
		case []string:
			for _, str := range s {
				seen[str] = true
			}
		}
		if v, ok := m["expected_good_fit"].(bool); ok && v {
			goodFit = true
		}
		if v, ok := m["exposure_limited"].(bool); ok && v {
			expLimit = true
		}
	}
	out := []string{}
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, goodFit, expLimit
}
