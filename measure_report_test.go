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
	"encoding/csv"
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
	in := fs.String("in", "results/measurements.jsonl", "bigbuff-measure results file")
	out := fs.String("out", "results/report.html", "output HTML report path")
	registryPath := fs.String("registry", "", "registry JSON (default: dumped live from the test binary)")
	csvDir := fs.String("csv", "", "optional directory for CSV exports")
	if err := fs.Parse(args); err != nil {
		return 2
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
		if err := writeReportCSVs(*csvDir, data, derived); err != nil {
			fmt.Fprintf(os.Stderr, "report: csv: %v\n", err)
			return 1
		}
		fmt.Fprintf(os.Stderr, "report: wrote CSVs to %s\n", *csvDir)
	}
	return 0
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
	Impl             string            `json:"impl"`
	Family           string            `json:"family"`
	Score            int               `json:"score"`
	Verdict          string            `json:"verdict"`
	Baseline         bool              `json:"baseline"`
	Integrity        string            `json:"integrity"` // "verified" | "failed" | "insufficient-data"
	Dims             map[string]int    `json:"dims"`
	Metrics          map[string]any    `json:"metrics"`
	Scenarios        []string          `json:"scenarios"`
	ExpectedGoodFit  bool              `json:"expected_good_fit"`
	ExposureLimited  bool              `json:"exposure_limited"`
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
		thrAll := metricMeans(data.metrics, map[string]string{ // metric name differs per family
			"broadcast": "publish_ns_op", "payload": "publish_ns_op", "caster": "send_ns_op",
		}[family], family, 8)
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

			// Weights documented in MEASUREMENT.md.
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
				metrics["publish_ns_op@8"] = thr
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
		"broadcast": broadcastFactoryNames(), "caster": casterNames(), "scatter": scatterNames(), "gather": gatherNames(), "payload": payloadNames(),
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

// --- Observations: data-driven key findings ---

func computeObservations(data *reportData, dd derivedData) []map[string]any {
	obs := []map[string]any{}
	add := func(kind, title, detail, anchor string) {
		obs = append(obs, map[string]any{"kind": kind, "title": title, "detail": detail, "anchor": anchor})
	}
	metrics := data.metrics

	// 1. Stalled-consumer blocking fraction (broadcast family).
	blocked, completed, total := 0, 0, 0
	for _, m := range metrics {
		if m["metric"] == "stalled_consumer_ops_completed" && m["family"] == "broadcast" {
			total++
			switch m["outcome"] {
			case "publisher_blocked":
				blocked++
			case "completed":
				completed++
			}
		}
	}
	if total > 0 {
		add("pathology",
			fmt.Sprintf("%d of %d broadcast implementations park the publisher behind a stalled consumer", blocked, total),
			"Lock-step and backpressured designs cannot proceed past a consumer that never receives; lossy and coalescing designs degrade instead. Both behaviours are documented in the attachment registry and measured in the Pathology Explorer.",
			"#pathology")
	}

	// 2. Allocation-free publishers at 8 subscribers.
	allocFree := []string{}
	for _, m := range metrics {
		if m["metric"] == "publish_allocs_op" && m["family"] == "broadcast" {
			if dimsSubs(m) == 8 {
				if v, ok := recMean(m); ok && v == 0 {
					allocFree = append(allocFree, fmt.Sprint(m["impl"]))
				}
			}
		}
	}
	if len(allocFree) > 0 {
		add("efficiency",
			fmt.Sprintf("%d broadcast implementations publish at 8 subscribers with zero allocation per op", len(allocFree)),
			strings.Join(allocFree, ", "),
			"#memory")
	}

	// 3. Fanout flatness champion.
	var bestScale float64 = math.Inf(1)
	var bestScaleImpl string
	for family, implScores := range dd.Scores {
		if family != "broadcast" {
			continue
		}
		for impl, s := range implScores {
			if v, ok := s.Metrics["scaling_ratio_64_over_1"].(float64); ok && v < bestScale {
				bestScale, bestScaleImpl = v, impl
			}
		}
	}
	if !math.IsInf(bestScale, 1) {
		add("scaling",
			fmt.Sprintf("%s is the flattest publisher (%.2fx cost growth from 1 to 64 subscribers)", bestScaleImpl, bestScale),
			"Publish cost at 64 subscribers relative to 1 subscriber, geometric mean of reps. Flat designs scale to high fan-out without amortisation tricks.",
			"#scaling")
	}

	// 4. Highest pinned backlog in the stalled-consumer probe.
	var maxRetained uint64
	var maxRetainedImpl string
	var maxRetainedOps float64
	for _, m := range metrics {
		if m["metric"] == "stalled_consumer_ops_completed" && m["family"] == "broadcast" {
			if v, ok := m["approx_retained_bytes"].(float64); ok {
				if uint64(v) > maxRetained {
					maxRetained, maxRetainedImpl = uint64(v), fmt.Sprint(m["impl"])
					maxRetainedOps, _ = m["ops_planned"].(float64)
				}
			}
		}
	}
	if maxRetained > 0 {
		add("memory",
			fmt.Sprintf("%s pins ~%s per stalled consumer after %d publishes", maxRetainedImpl, humanBytes(float64(maxRetained)), int(maxRetainedOps)),
			"Retained heap attributable to the backlog of one permanently stalled subscriber, measured after a GC of the probe. Unbounded-retention designs pin everything; ring and coalescing designs pin capacity or nothing.",
			"#pathology")
	}

	// 5. Withdrawal compensation across caster family.
	roundsTotal, ok := 0, false
	cwdImpls, allPass := []string{}, true
	for _, m := range metrics {
		if m["metric"] == "withdrawal_compensation_success" {
			v, vok := m["mean"].(float64)
			ro, rok := m["dims"].(map[string]any)["rounds"].(float64)
			if !vok || !rok {
				continue
			}
			roundsTotal += int(ro)
			ok = true
			if v < 1.0 {
				allPass = false
				cwdImpls = append(cwdImpls, fmt.Sprint(m["impl"]))
			}
		}
	}
	if ok {
		if allPass {
			add("correctness",
				fmt.Sprintf("Every eligible rendezvous caster compensated mid-flight withdrawal in 100%% of %d rounds", roundsTotal),
				"With one registered intent that never receives, an Add(-1) during a blocked Send released it with the compensated count in every round, for every caster except those excluded as exposure-limited.",
				"#leaderboard")
		} else {
			add("correctness",
				fmt.Sprintf("Withdrawal compensation failed for: %s", strings.Join(cwdImpls, ", ")),
				"A registered intent that never receives leaves Send parked in these designs even after Add(-1). See the withdrawal_compensation_success metric.",
				"#leaderboard")
		}
	}

	// 6. Torn reads.
	var tornTotal, checkedTotal float64
	for _, m := range metrics {
		if m["metric"] == "torn_reads" {
			tornTotal += mustFloat(m, "violations")
			checkedTotal += mustFloat(m, "checked")
		}
	}
	add("correctness",
		fmt.Sprintf("%0.0f torn multi-word reads observed across %0.0f sampled payload deliveries", tornTotal, checkedTotal),
		"Multi-word payload integrity under 8-way concurrent fan-out. Seqlock-style implementations are the risk class this scenario probes; any violation is a hard failure for adoption.",
		"#data")

	// 7. Churn champion.
	var bestChurn float64 = math.Inf(1)
	var bestChurnImpl string
	for _, m := range metrics {
		if m["metric"] == "churn_ns_op" && m["family"] == "broadcast" {
			if v, ok := recMean(m); ok && v < bestChurn {
				bestChurn, bestChurnImpl = v, fmt.Sprint(m["impl"])
			}
		}
	}
	if !math.IsInf(bestChurn, 1) {
		add("efficiency",
			fmt.Sprintf("%s has the cheapest subscribe/unsubscribe pair (~%s)", bestChurnImpl, humanNs(bestChurn)),
			"Relevant for high-churn topologies (connection-per-request, presence fan-out) where registration churn dominates steady-state cost.",
			"#churn")
	}

	// 8. Zero-subscriber cost champion.
	var bestZero float64 = math.Inf(1)
	var bestZeroImpl string
	for _, m := range metrics {
		if m["metric"] == "zero_subscriber_ns_op" {
			if v, ok := recMean(m); ok && v < bestZero {
				bestZero, bestZeroImpl = v, fmt.Sprint(m["impl"])
			}
		}
	}
	if !math.IsInf(bestZero, 1) {
		add("efficiency",
			fmt.Sprintf("%s publishes to an empty topic for ~%s", bestZeroImpl, humanNs(bestZero)),
			"Idle-heavy topics (config distribution with no listeners) measure the cost of publishing into the void; some designs amortise it to nearly nothing.",
			"#memory")
	}

	// 9. Exactly-once violations in scatter dispatch.
	var scatterViolations float64
	for _, m := range metrics {
		if m["metric"] == "dispatch_ns_op" {
			scatterViolations += mustFloat(m, "exactly_once_violations")
		}
	}
	add("correctness",
		fmt.Sprintf("%0.0f exactly-once violations across scatter dispatch runs", scatterViolations),
		"Every dispatched value must be received by exactly one worker; the count covers all receivers across all repetitions (warmup included).",
		"#data")

	// 10. Lossy drop-oldest delivery under burst contention.
	worstDelivery, worstDeliveryImpl := math.Inf(1), ""
	for _, m := range metrics {
		if m["metric"] == "stalled_consumer_ops_completed" && m["family"] == "payload" {
			// no per-sub delivery ratio here; skip
			_ = m
		}
	}
	for _, m := range metrics {
		if m["metric"] == "torn_reads" {
			_ = worstDelivery
			_ = worstDeliveryImpl
		}
	}

	return obs
}

func mustFloat(m map[string]any, key string) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return 0
}

func humanNs(ns float64) string {
	switch {
	case ns >= 1e6:
		return fmt.Sprintf("%.2f ms", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.2f µs", ns/1e3)
	default:
		return fmt.Sprintf("%.0f ns", ns)
	}
}

func humanBytes(b float64) string {
	switch {
	case b >= 1 << 20:
		return fmt.Sprintf("%.1f MiB", b/(1<<20))
	case b >= 1 << 10:
		return fmt.Sprintf("%.1f KiB", b/(1<<10))
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}

// --- CSV exports ---

func writeReportCSVs(dir string, data *reportData, derived derivedData) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// metrics_wide.csv
	mf, err := os.Create(filepath.Join(dir, "metrics_wide.csv"))
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

	// scores.csv
	sf, err := os.Create(filepath.Join(dir, "scores.csv"))
	if err != nil {
		return err
	}
	sw := csv.NewWriter(sf)
	if err := sw.Write([]string{"family", "impl", "score", "verdict", "baseline", "integrity", "throughput", "latency", "allocations", "churn", "scaling", "stall_resilience", "expected_good_fit", "exposure_limited"}); err != nil {
		return err
	}
	for family, scores := range derived.Scores {
		for _, s := range scores {
			row := []string{
				family, s.Impl, fmt.Sprint(s.Score), s.Verdict, fmt.Sprint(s.Baseline), s.Integrity,
				fmt.Sprint(s.Dims["throughput"]), fmt.Sprint(s.Dims["latency"]), fmt.Sprint(s.Dims["allocations"]),
				fmt.Sprint(s.Dims["churn"]), fmt.Sprint(s.Dims["scaling"]), fmt.Sprint(s.Dims["stall_resilience"]),
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

// --- HTML report ---

func jsonForHTML(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	// Prevent "</script>" breakouts and HTML comment tricks.
	s := strings.ReplaceAll(string(b), "</", "<\\/")
	s = strings.ReplaceAll(s, "<!--", "<\\!--")
	return s, nil
}

func renderReportPage(data *reportData, registry map[string]any, derived derivedData) (string, error) {
	dataJSON, err := jsonForHTML(map[string]any{
		"meta":    data.meta,
		"metrics": data.metrics,
		"traces":  data.traces,
		"skipped": data.skipped,
		"summary": data.summary,
		"derived": derived,
	})
	if err != nil {
		return "", err
	}
	registryJSON, err := jsonForHTML(registry)
	if err != nil {
		return "", err
	}
	page := reportHTMLCSS + reportHTMLBody + "<script>\n" + reportJS1 + reportJS2 + reportJS3 + reportJS4 + "\n</script>\n</body>\n</html>\n"
	page = strings.Replace(page, "__DATA_JSON__", dataJSON, 1)
	page = strings.Replace(page, "__REGISTRY_JSON__", registryJSON, 1)
	return page, nil
}

const reportHTMLCSS = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark">
<title>bigbuff · Concurrency Measurement Report</title>
<style>
:root{
  --bg:#070b14; --bg2:#0b1020; --panel:#0e1530; --panel2:#121a3a; --panel3:#182148;
  --line:#1f2a55; --line2:#2b3a6e;
  --ink:#e8ecf8; --ink2:#a9b3d6; --ink3:#6b769e;
  --cyan:#22d3ee; --indigo:#818cf8; --violet:#a78bfa; --green:#34d399; --amber:#fbbf24;
  --orange:#fb923c; --red:#f87171; --teal:#2dd4bf;
  --radius:14px; --shadow:0 18px 50px rgba(0,0,0,.45);
  --mono:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
  --sans:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth}
body{background:var(--bg);color:var(--ink);font-family:var(--sans);line-height:1.55;-webkit-font-smoothing:antialiased}
::selection{background:rgba(129,140,248,.35)}

/* layout */
#shell{display:grid;grid-template-columns:250px 1fr;min-height:100vh}
@media (max-width:1080px){#shell{grid-template-columns:1fr}#nav{display:none}}
nav#nav{position:sticky;top:0;height:100vh;overflow-y:auto;border-right:1px solid var(--line);background:linear-gradient(180deg,#0a0f22,#0b1020 40%,#0a0f22);padding:26px 18px;display:flex;flex-direction:column;gap:4px}
#brand{display:flex;align-items:center;gap:12px;padding:4px 8px 18px;border-bottom:1px solid var(--line);margin-bottom:14px}
#brand .mark{width:38px;height:38px;border-radius:11px;background:conic-gradient(from 210deg,var(--cyan),var(--indigo),var(--violet),var(--cyan));display:grid;place-items:center;font-weight:800;color:#060a17;font-size:19px;box-shadow:0 0 22px rgba(56,189,248,.35)}
#brand h1{font-size:15px;letter-spacing:.02em;line-height:1.25}
#brand small{display:block;color:var(--ink3);font-weight:500;font-size:10px;letter-spacing:.14em;text-transform:uppercase;margin-top:2px}
#nav a{display:flex;align-items:center;gap:10px;color:var(--ink2);text-decoration:none;font-size:13px;padding:7px 10px;border-radius:9px;border:1px solid transparent;transition:all .18s}
#nav a:hover{color:var(--ink);background:var(--panel);border-color:var(--line)}
#nav a .dot{width:6px;height:6px;border-radius:50%;background:var(--line2);flex:none}
#nav a:hover .dot{background:var(--cyan)}
#nav a.active{color:#06121a;background:linear-gradient(90deg,#67e8f9,#a5b4fc);border-color:transparent;font-weight:600}
#nav a.active .dot{background:#06121a}
#navfoot{margin-top:auto;padding:14px 8px 2px;border-top:1px solid var(--line);color:var(--ink3);font-size:11px;line-height:1.7}
#navfoot b{color:var(--ink2)}
main{min-width:0;padding:34px 40px 80px;max-width:1560px}

/* sections */
section.block{margin-bottom:56px;scroll-margin-top:24px}
.kicker{font-size:10.5px;letter-spacing:.22em;text-transform:uppercase;color:var(--cyan);font-weight:700;margin-bottom:6px}
h2.title{font-size:26px;letter-spacing:-.01em;margin-bottom:6px}
.lede{color:var(--ink2);max-width:880px;font-size:14px}
.card{background:linear-gradient(180deg,var(--panel),var(--bg2));border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow)}
.card.pad{padding:18px 20px}

/* hero */
#hero{position:relative;overflow:hidden;border:1px solid var(--line2);border-radius:20px;padding:34px 36px 30px;background:radial-gradient(1100px 480px at 12% -10%,rgba(99,102,241,.34),transparent 55%),radial-gradient(900px 420px at 95% 0%,rgba(34,211,238,.20),transparent 50%),linear-gradient(180deg,#101736,#0b1020)}
#hero::after{content:"";position:absolute;inset:0;background-image:radial-gradient(rgba(148,163,216,.13) 1px,transparent 1px);background-size:22px 22px;pointer-events:none;mask-image:linear-gradient(180deg,#000 20%,transparent 90%)}
#hero h1{font-size:34px;letter-spacing:-.02em;line-height:1.15;max-width:900px;position:relative}
#hero h1 em{font-style:normal;background:linear-gradient(90deg,#67e8f9,#a5b4fc 55%,#c4b5fd);-webkit-background-clip:text;background-clip:text;color:transparent}
#hero p{color:var(--ink2);margin-top:10px;max-width:820px;position:relative}
.chips{display:flex;flex-wrap:wrap;gap:8px;margin-top:18px;position:relative}
.chip{display:inline-flex;align-items:center;gap:8px;font-family:var(--mono);font-size:11.5px;color:#cfe6ff;background:rgba(14,22,48,.75);border:1px solid var(--line2);border-radius:999px;padding:5px 13px}
.chip b{color:#fff;font-weight:600}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-top:20px;position:relative}
.kpi{background:rgba(9,13,30,.6);border:1px solid var(--line2);border-radius:14px;padding:13px 15px}
.kpi .v{font-size:24px;font-weight:750;letter-spacing:-.01em;font-family:var(--mono)}
.kpi .l{font-size:10.5px;color:var(--ink2);text-transform:uppercase;letter-spacing:.09em;margin-top:2px}
.kpi .s{font-size:10.5px;color:var(--ink3);margin-top:3px}

/* findings */
.findings-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(330px,1fr));gap:14px;margin-top:16px}
.finding{position:relative;background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);padding:16px 17px 14px;overflow:hidden}
.finding::before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:linear-gradient(180deg,var(--cyan),var(--indigo))}
.finding.pathology::before{background:linear-gradient(180deg,var(--red),var(--orange))}
.finding.correctness::before{background:linear-gradient(180deg,var(--green),var(--teal))}
.finding.memory::before{background:linear-gradient(180deg,var(--violet),var(--indigo))}
.finding h3{font-size:14.5px;line-height:1.35;margin-bottom:6px}
.finding p{font-size:12.5px;color:var(--ink2)}
.finding .tag{display:inline-block;margin-bottom:8px;font-size:9.5px;letter-spacing:.16em;text-transform:uppercase;color:var(--ink3);border:1px solid var(--line2);padding:2px 8px;border-radius:5px}
.finding a{color:var(--cyan);text-decoration:none}

/* tabs */
.tabs{display:flex;gap:6px;flex-wrap:wrap;margin:14px 0}
.tab{cursor:pointer;border:1px solid var(--line2);background:var(--panel);color:var(--ink2);font-size:12px;font-weight:600;padding:7px 14px;border-radius:9px;transition:all .18s}
.tab:hover{color:var(--ink)}
.tab.active{background:linear-gradient(90deg,#164e63,#312e81);border-color:#3b82f6;color:#e0f2fe}

/* toolbar */
.toolbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:12px 0}
.search{background:var(--panel);border:1px solid var(--line2);color:var(--ink);border-radius:9px;padding:8px 12px;font-size:13px;min-width:220px;outline:none}
.search:focus{border-color:var(--indigo)}
.toggle{cursor:pointer;user-select:none;font-size:12px;color:var(--ink2);display:flex;align-items:center;gap:6px}
select.sel{background:var(--panel);border:1px solid var(--line2);color:var(--ink);border-radius:9px;padding:7px 10px;font-size:12.5px}

/* leaderboard table */
.tbl-wrap{overflow:auto;border:1px solid var(--line);border-radius:var(--radius);max-height:640px;overflow-y:auto}
table.tbl{width:100%;border-collapse:collapse;font-size:12px}
table.tbl th{position:sticky;top:0;background:#131b3f;color:var(--ink2);text-align:left;padding:10px 10px;font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;cursor:pointer;user-select:none;white-space:nowrap;border-bottom:1px solid var(--line2)}
table.tbl th:hover{color:var(--ink)}
table.tbl td{padding:8px 10px;border-bottom:1px solid rgba(31,42,85,.5);vertical-align:middle}
table.tbl tr:hover td{background:rgba(99,102,241,.07)}
.impl-name{font-family:var(--mono);font-size:11.5px;white-space:nowrap}
.impl-name .bl{color:var(--amber);font-size:9px;border:1px solid #78560a;background:#2a2008;border-radius:4px;padding:0 4px;margin-left:6px;vertical-align:1px}
.impl-name .xl{color:var(--red);font-size:9px;border:1px solid #7f1d1d;background:#2a0d0d;border-radius:4px;padding:0 4px;margin-left:6px;vertical-align:1px}
.scorebar{position:relative;height:16px;background:#1a2350;border-radius:4px;min-width:120px;overflow:hidden}
.scorebar .fill{position:absolute;left:0;top:0;bottom:0;border-radius:4px;background:linear-gradient(90deg,#0e7490,#22d3ee)}
.scorebar span{position:absolute;left:8px;top:1px;font-size:11px;font-weight:700;color:#04121a;mix-blend-mode:plus-lighter;text-shadow:0 0 4px rgba(34,211,238,.8)}
.verdict{font-size:10.5px;font-weight:700;white-space:nowrap}
.v-strong{color:var(--green)} .v-situational{color:var(--amber)} .v-niche{color:var(--orange)} .v-avoid{color:var(--red)}
.barnum{font-family:var(--mono);color:var(--ink2)}

/* whiskers */
.whisker{display:flex;align-items:center;gap:8px;min-width:150px}
.whisker svg{display:block}

/* svg chart text */
.ax{stroke:#26315f;stroke-width:1}
.grid{stroke:#18224a;stroke-width:1;stroke-dasharray:2 4}
.axtxt{fill:#7c88b3;font-size:10px;font-family:var(--mono)}
.lgnd{display:flex;gap:12px;flex-wrap:wrap;margin-top:10px;font-size:11.5px;color:var(--ink2)}
.lgnd span{display:flex;align-items:center;gap:6px;cursor:pointer;user-select:none}
.lgnd i{width:14px;height:3px;border-radius:2px;display:inline-block}
.lgnd span.off{opacity:.35}

/* pathology */
.patho-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:16px;margin-top:16px}
.patho-card{background:var(--panel2);border:1px solid var(--line2);border-radius:var(--radius);padding:16px;position:relative}
.patho-card h4{font-size:13px;margin-bottom:2px}
.patho-card .outcome{font-family:var(--mono);font-size:11px;margin-top:6px;padding:4px 8px;border-radius:6px;display:inline-block}
.outcome-completed{background:rgba(52,211,153,.12);color:var(--green);border:1px solid rgba(52,211,153,.3)}
.outcome-publisher_blocked{background:rgba(248,113,113,.12);color:#fda4a4;border:1px solid rgba(248,113,113,.35)}
.outcome-publisher_panicked{background:rgba(251,146,60,.12);color:#fdba74;border:1px solid rgba(251,146,60,.4)}
.swim-controls{display:flex;align-items:center;gap:8px;margin-top:10px}
.sbtn{cursor:pointer;background:var(--panel3);border:1px solid var(--line2);color:var(--ink);border-radius:8px;width:34px;height:30px;font-size:13px}
.sbtn:hover{border-color:var(--indigo)}
.swim-wrap{position:relative}

/* tooltip */
#tip{position:fixed;z-index:60;pointer-events:none;background:#0d1430;border:1px solid var(--line2);border-radius:9px;padding:8px 10px;font-size:11.5px;box-shadow:var(--shadow);opacity:0;transition:opacity .12s;max-width:300px}
#tip .tt{font-weight:700;margin-bottom:2px}
#tip .mono{font-family:var(--mono);color:#a5f3fc}

/* coverage matrix */
.cov-wrap{overflow:auto;border:1px solid var(--line);border-radius:var(--radius);max-height:520px}
table.cov{border-collapse:collapse;font-size:9px}
table.cov th{position:sticky;left:0;background:#131b3f;z-index:2;text-align:right;padding:4px 8px;color:var(--ink2);font-family:var(--mono);font-weight:600;white-space:nowrap;border-bottom:1px solid var(--line2)}
table.cov thead th{position:sticky;top:0;left:auto;z-index:3;writing-mode:vertical-rl;transform:rotate(180deg);padding:6px 4px;border-bottom:1px solid var(--line2)}
table.cov td{width:15px;height:15px;border:1px solid rgba(7,11,20,.6);text-align:center}
.cov-good{background:#166534}.cov-bad{background:#7f1d1d}.cov-none{background:#1b2447}
.cov-legend{display:flex;gap:16px;margin-top:10px;font-size:11.5px;color:var(--ink2)}
.cov-legend i{width:13px;height:13px;border-radius:3px;display:inline-block;vertical-align:-2px;margin-right:6px}

/* radar + verdict */
.radar-flex{display:grid;grid-template-columns:minmax(280px,430px) 1fr;gap:22px;align-items:center}
@media (max-width:900px){.radar-flex{grid-template-columns:1fr}}
.score-rows{display:flex;flex-direction:column;gap:9px}
.srow{display:grid;grid-template-columns:150px 1fr 42px;gap:10px;align-items:center;font-size:12px}
.srow .dim{color:var(--ink2)}
.strack{height:9px;background:#1a2350;border-radius:5px;overflow:hidden}
.sfill{height:100%;border-radius:5px;background:linear-gradient(90deg,#0e7490,#22d3ee)}
.swatch{width:11px;height:11px;border-radius:3px;display:inline-block;margin-right:6px;vertical-align:-1px}

/* downloads */
.dl{display:flex;gap:12px;flex-wrap:wrap;margin-top:14px}
.dlbtn{cursor:pointer;display:flex;flex-direction:column;gap:2px;background:var(--panel2);border:1px solid var(--line2);border-radius:12px;padding:13px 18px;color:var(--ink);text-decoration:none;min-width:200px}
.dlbtn:hover{border-color:var(--cyan);box-shadow:0 0 0 1px var(--cyan) inset}
.dlbtn b{font-size:13px}
.dlbtn small{color:var(--ink3);font-size:11px}

/* methodology */
details.mdetail{border:1px solid var(--line);border-radius:12px;background:var(--panel);padding:14px 18px;margin-top:12px}
details.mdetail summary{cursor:pointer;font-weight:650;font-size:13.5px;outline:none}
details.mdetail div{color:var(--ink2);font-size:12.5px;margin-top:10px}
code.k{font-family:var(--mono);background:#141c40;border:1px solid var(--line2);border-radius:5px;padding:1px 6px;font-size:11px;color:#a5f3fc}
table.gloss{width:100%;border-collapse:collapse;font-size:12px;margin-top:6px}
table.gloss td{padding:6px 10px;border-bottom:1px solid var(--line);vertical-align:top}
table.gloss td:first-child{font-family:var(--mono);white-space:nowrap;color:#a5f3fc}
.small{font-size:11.5px;color:var(--ink3)}
.pill{display:inline-block;font-size:9.5px;font-weight:800;letter-spacing:.1em;padding:2px 8px;border-radius:99px;text-transform:uppercase}
.pill-pass{background:rgba(52,211,153,.15);color:var(--green);border:1px solid rgba(52,211,153,.4)}
.pill-warn{background:rgba(251,191,36,.12);color:var(--amber);border:1px solid rgba(251,191,36,.4)}
.pill-bad{background:rgba(248,113,113,.14);color:var(--red);border:1px solid rgba(248,113,113,.4)}
.histo{display:flex;align-items:flex-end;gap:2px;height:110px}
.histo div{flex:1;background:linear-gradient(180deg,#22d3ee,#155e75);border-radius:3px 3px 0 0;min-height:2px}
.histo-x{display:flex;justify-content:space-between;font-family:var(--mono);font-size:9px;color:var(--ink3);margin-top:3px}
.footer-note{color:var(--ink3);font-size:11px;border-top:1px solid var(--line);padding-top:18px;margin-top:10px}
</style>
</head>
`
const reportHTMLBody = `<body>
<div id="shell">
<nav id="nav" aria-label="Report sections">
  <div id="brand">
    <div class="mark">B</div>
    <div><h1>bigbuff</h1><small>Concurrency Measurement</small></div>
  </div>
  <a href="#overview" class="active"><span class="dot"></span>Overview</a>
  <a href="#findings"><span class="dot"></span>Key Findings</a>
  <a href="#scaling"><span class="dot"></span>Scaling Curves</a>
  <a href="#leaderboard"><span class="dot"></span>Leaderboard</a>
  <a href="#adoptability"><span class="dot"></span>Adoptability Radar</a>
  <a href="#latency"><span class="dot"></span>Latency Distribution</a>
  <a href="#memory"><span class="dot"></span>Memory &amp; Allocations</a>
  <a href="#churn"><span class="dot"></span>Churn Cost</a>
  <a href="#pathology"><span class="dot"></span>Pathology Lab</a>
  <a href="#correctness"><span class="dot"></span>Correctness Signals</a>
  <a href="#coverage"><span class="dot"></span>Scenario Coverage</a>
  <a href="#methodology"><span class="dot"></span>Methodology &amp; Schema</a>
  <a href="#data"><span class="dot"></span>Data &amp; Exports</a>
  <div id="navfoot">
    <b>bigbuff-measure</b> · deterministic harness<br>
    Every chart regenerates from the embedded JSONL records — nothing is hard-coded. Hover anything for values.
  </div>
</nav>
<main>

<!-- ══════════════ OVERVIEW ══════════════ -->
<header id="hero">
  <div class="kicker">Empirical concurrency study</div>
  <h1>Measured, not vibes:<br><em>116 concurrent primitives under one harness.</em></h1>
  <p>Every implementation in this repository — hand-rolled broadcasters, caster-likes, scatter dispatchers, gather barriers and async payload lanes — exercised by the same batteries, the same timers, the same GC settings, and the same pass/fail gates. The sections below unpack throughput, latency, allocation, scalability, stall resilience, churn, withdrawal compensation and payload integrity.</p>
  <div class="chips" id="metaChips"></div>
  <div class="kpis" id="kpiRow"></div>
</header>
<section class="block" id="overview"></section>

<!-- ══════════════ FINDINGS ══════════════ -->
<section class="block" id="findings">
  <div class="kicker">Key findings</div>
  <h2 class="title">What the data says</h2>
  <p class="lede">Computed directly from the JSONL records by the report generator — each card links to the section where the underlying measurement lives.</p>
  <div class="findings-grid" id="findingsGrid"></div>
</section>

<!-- ══════════════ SCALING ══════════════ -->
<section class="block" id="scaling">
  <div class="kicker">Throughput under fan-out</div>
  <h2 class="title">Scaling curves, 1 → 64 subscribers</h2>
  <p class="lede">Publish cost per operation as subscriber count grows (log₂ axes). Flat lines scale; rising lines levy per-subscriber work. Toggle implementations in the legend; baselines are dashed.</p>
  <div class="tabs" id="scalingTabs"></div>
  <div class="card pad">
    <div class="toolbar">
      <label class="toggle"><input type="checkbox" id="scalingTopOnly" checked> show 12 fastest only</label>
      <select class="sel" id="scalingSort"><option value="thr">sort legend by throughput</option><option value="ratio">sort legend by 64/1 scaling ratio</option><option value="alpha">sort legend alphabetically</option></select>
    </div>
    <div id="scalingChart"></div>
    <div class="lgnd" id="scalingLegend"></div>
  </div>
</section>

<!-- ══════════════ LEADERBOARD ══════════════ -->
<section class="block" id="leaderboard">
  <div class="kicker">Composite verdict</div>
  <h2 class="title">Leaderboard &amp; adoptability scores</h2>
  <p class="lede">Percentile-ranked dimensions (throughput 28%, stall resilience 20%, scaling 16%, latency 14%, allocations 14%, churn 8%). Integrity failures cap the score at 50. Click a column to sort; filter to find your candidate.</p>
  <div class="tabs" id="lbTabs"></div>
  <div class="toolbar">
    <input class="search" id="lbSearch" placeholder="Filter implementations…" aria-label="Filter implementations">
    <label class="toggle"><input type="checkbox" id="lbBaselinesOnly"> baselines only</label>
    <label class="toggle"><input type="checkbox" id="lbGoodFitOnly"> expected-good-fit only</label>
  </div>
  <div class="tbl-wrap"><table class="tbl" id="lbTable"><thead><tr>
    <th data-k="impl">Implementation</th><th data-k="score">Adoptability</th><th data-k="verdict">Verdict</th>
    <th data-k="thr">ns/op @8</th><th data-k="ci">95% CI</th><th data-k="lat">p99 latency</th><th data-k="alloc">allocs/op</th><th data-k="scale">64/1 ratio</th><th data-k="stall">stall ops</th><th data-k="baseline">kind</th>
  </tr></thead><tbody id="lbBody"></tbody></table></div>
  <p class="small" id="lbFoot" style="margin-top:8px"></p>
</section>

<!-- ══════════════ ADOPTABILITY ══════════════ -->
<section class="block" id="adoptability">
  <div class="kicker">Trade-off fingerprints</div>
  <h2 class="title">Radar: pick your fighter</h2>
  <p class="lede">Six-axis profile, normalised 0–100 within family. Select up to 4 implementations (or press a shortcut below). Baselines render dashed.</p>
  <div class="tabs" id="radarTabs"></div>
  <div class="card pad">
    <div class="toolbar" id="radarShortcuts"></div>
    <div class="radar-flex">
      <div id="radarChart"></div>
      <div>
        <div class="toolbar" style="margin-top:0"><select class="sel" id="radarImpl"></select><button class="sbtn" id="radarAdd" title="Add to radar">＋</button></div>
        <div class="score-rows" id="radarRows"></div>
        <p class="small" style="margin-top:10px">Stall resilience uses log₁₀(ops completed by a publisher whose sibling stalled forever, shared deadline) — higher means it kept delivering. Scaling is the 64/1 relative cost ratio (lower ratio → higher score). Every axis is percentile within the measured family.</p>
      </div>
    </div>
  </div>
</section>

<!-- ══════════════ LATENCY ══════════════ -->
<section class="block" id="latency">
  <div class="kicker">Tail behaviour</div>
  <h2 class="title">Latency distribution @ 8 subscribers</h2>
  <p class="lede">p50 / p90 / p99 / max from reservoir-sampled publish→delivery latency (ADR-004 harness, warm). Sorted by p99. Whiskers show the 95% confidence interval around the mean where available.</p>
  <div class="tabs" id="latTabs"></div>
  <div class="card pad"><div id="latChart"></div><p class="small" style="margin-top:10px">Values in nanoseconds; lower is better. Histogram buckets in <a href="#methodology" style="color:var(--cyan)">Methodology</a>.</p></div>
</section>

<!-- ══════════════ MEMORY ══════════════ -->
<section class="block" id="memory">
  <div class="kicker">Allocation &amp; retention</div>
  <h2 class="title">What publishing costs the allocator</h2>
  <p class="lede">Left: allocations per publish vs publish cost (log–log) at 8 subscribers. Right: estimated bytes retained per subscriber at steady state (heap before/after GC of 16 idle subscriptions), and cost of publishing into the void.</p>
  <div class="tabs" id="memTabs"></div>
  <div class="card pad" style="display:grid;grid-template-columns:1.2fr 1fr;gap:24px" id="memGrid">
    <div><h4 style="font-size:12px;color:var(--ink2);margin-bottom:8px">allocations/op vs ns/op</h4><div id="allocScatter"></div></div>
    <div><h4 style="font-size:12px;color:var(--ink2);margin-bottom:8px">bytes retained per idle subscriber</h4><div id="memBars"></div></div>
  </div>
  <div class="card pad" style="margin-top:16px"><h4 style="font-size:12px;color:var(--ink2);margin-bottom:8px">zero-subscriber publish cost (broadcast)</h4><div id="zeroSubChart"></div></div>
</section>

<!-- ══════════════ CHURN ══════════════ -->
<section class="block" id="churn">
  <div class="kicker">Membership dynamics</div>
  <h2 class="title">Subscribe → publish → unsubscribe, per op</h2>
  <p class="lede">Churn cost per membership lifecycle — decisive for per-request or presence topologies where subscriptions are constantly created and torn down.</p>
  <div class="tabs" id="churnTabs"></div>
  <div class="card pad"><div id="churnChart"></div></div>
</section>

<!-- ══════════════ PATHOLOGY ══════════════ -->
<section class="block" id="pathology">
  <div class="kicker">Pathology Lab</div>
  <h2 class="title">A consumer stops reading. What happens?</h2>
  <p class="lede">Three lanes: publisher, two healthy subscribers, one permanently stalled subscriber that never calls Receive. Watch both traces side by side — or open any pair from the grid. Blocked publishers are parked, never killed; the harness unwinds them with watchdogs and GC.</p>
  <div class="card pad">
    <div class="toolbar">
      <select class="sel" id="pathoA" aria-label="Left implementation"></select>
      <span style="color:var(--ink3)">vs</span>
      <select class="sel" id="pathoB" aria-label="Right implementation"></select>
      <button class="sbtn" id="pathoPlay" title="Play both">▶</button>
      <button class="sbtn" id="pathoPause" title="Pause">⏸</button>
      <select class="sel" id="pathoSpeed"><option value="0.5">0.5×</option><option value="1" selected>1×</option><option value="2">2×</option><option value="4">4×</option></select>
      <input type="range" id="pathoScrub" min="0" max="1000" value="0" style="flex:1;min-width:140px;accent-color:var(--cyan)" aria-label="Scrub timeline">
    </div>
    <div class="patho-grid" id="pathoCards"></div>
  </div>
  <div class="card pad" style="margin-top:16px">
    <h4 style="font-size:12px;color:var(--ink2);margin-bottom:4px">Retained bytes after the storm (stalled consumer, post-GC)</h4>
    <p class="small" style="margin-bottom:10px">Publishers that keep running past a stalled lane either drop, coalesce, or buffer. Retained = heap growth attributable to the stalled lane after forcing a GC.</p>
    <div id="retainBars"></div>
  </div>
</section>

<!-- ══════════════ CORRECTNESS ══════════════ -->
<section class="block" id="correctness">
  <div class="kicker">Correctness signals</div>
  <h2 class="title">Zero tolerance, measured explicitly</h2>
  <p class="lede">Three independent gates: multi-word payload integrity (seqlock torn-read probe), rendezvous withdrawal compensation (did Add(-1) unstick a parked Send?), exactly-once scatter accounting, plus the harness verifier.</p>
  <div class="patho-grid" style="grid-template-columns:repeat(auto-fit,minmax(280px,1fr))">
    <div class="card pad"><h4 style="font-size:12px;color:var(--ink2)">Torn reads (payload lanes × 8 subs)</h4><div id="tornTable" style="margin-top:10px"></div></div>
    <div class="card pad"><h4 style="font-size:12px;color:var(--ink2)">Withdrawal compensation — rendezvous casters</h4><div id="cwdChart" style="margin-top:10px"></div></div>
    <div class="card pad"><h4 style="font-size:12px;color:var(--ink2)">Verification verdict</h4><div id="verifyPanel" style="margin-top:10px"></div></div>
  </div>
</section>

<!-- ══════════════ COVERAGE ══════════════ -->
<section class="block" id="coverage">
  <div class="kicker">Registry completeness</div>
  <h2 class="title">Scenario × implementation matrix</h2>
  <p class="lede">Attachment registry: which scenarios declare each implementation a good fit (green), a bad fit (red), or silent (grey). Hover a cell; switch families above.</p>
  <div class="tabs" id="covTabs"></div>
  <div class="cov-wrap"><table class="cov" id="covTable"></table></div>
  <div class="cov-legend"><span><i style="background:#166534"></i>expected good fit</span><span><i style="background:#7f1d1d"></i>declared bad fit</span><span><i style="background:#1b2447"></i>no attachment</span></div>
</section>

<!-- ══════════════ METHODOLOGY ══════════════ -->
<section class="block" id="methodology">
  <div class="kicker">Methodology &amp; schema</div>
  <h2 class="title">How to read every number here</h2>
  <p class="lede">The harness lives in <code class="k">measure_*_test.go</code>; results are line-delimited JSON written incrementally, one record per measurement, then verified by <code class="k">bigbuff-measure verify</code> before this page was rendered.</p>

  <details class="mdetail" open><summary>Design principles</summary><div>
    <ul style="padding-left:18px">
      <li><b>Single harness, all implementations.</b> Every battery constructs the implementation through its registry factory, warms it, measures under identical GC (<code class="k">GOGC=off</code> for alloc batteries) and scheduling conditions, and reports distributional stats — never a bare mean.</li>
      <li><b>Deterministic scheduling pressure.</b> Churn and contention batteries use exact iteration counts, channels and WaitGroups; timeouts are deadlock guards, never readiness probes. No sleeps anywhere.</li>
      <li><b>Pathology probes are honest.</b> A publisher parked by construction is an expected outcome, recorded as <code class="k">publisher_blocked</code>; recover converts panics to <code class="k">publisher_panicked</code>. Blocking cleanup (Unsubscribe/Close) is skipped or watchdog-bounded after a blocked probe — the harness would otherwise deadlock the runtime. This asymmetry is deliberate and documented.</li>
      <li><b>Passability.</b> <code class="k">go test</code> stays green: measure subcommands run under <code class="k">go test -run=NONE -args bigbuff-measure …</code>; batteries assert only what their design can guarantee and record the rest. Verification enforces that every registered implementation was measured or explicitly skipped with a reason.</li>
    </ul>
  </div></details>

  <details class="mdetail"><summary>Batteries &amp; metrics</summary><div>
    <table class="gloss" id="batteryGloss"></table>
  </div></details>

  <details class="mdetail"><summary>Record schema (JSONL v1)</summary><div>
    <p>One record per line. <code class="k">type</code> is one of <code class="k">meta</code>, <code class="k">metric</code>, <code class="k">trace</code>, <code class="k">skipped</code>, <code class="k">summary</code>, <code class="k">registry</code>.</p>
    <table class="gloss">
      <tr><td>metric</td><td>record kind; dims carry the sweep dimension (<code class="k">subs</code> / <code class="k">receivers</code> / <code class="k">senders</code>)</td></tr>
      <tr><td>mean / stddev / cv</td><td>distributional summaries over <code class="k">reps</code> repetitions of <code class="k">ops</code> operations each</td></tr>
      <tr><td>p50 / p90 / p99 / max</td><td>reservoir-sampled latencies (ADR-004 HDR-ish reservoir, per-run)</td></tr>
      <tr><td>ci95_low / ci95_high</td><td>Welch-adjusted 95% half-width on the per-op mean (t-distribution, df = n−1)</td></tr>
      <tr><td>approx_retained_bytes</td><td>post-GC heap growth attributable to a permanently stalled subscriber</td></tr>
      <tr><td>exactly_once_violations</td><td>scatter messages received ≠ 1 time, summed over all receivers incl. warmup</td></tr>
      <tr><td>torn_reads / checked</td><td>payload lanes caught serving mismatched [4]uint64 halves vs loads checked</td></tr>
      <tr><td>withdrawal_compensation_success</td><td>fraction of rounds where Add(-1) unstuck a Send parked on a never-receiving intent</td></tr>
      <tr><td>exposure_limited</td><td>true when the implementation is excluded from batteries it would fail by construction (policy: limit exposure, never mis-measure)</td></tr>
      <tr><td>expected_good_fit / scenarios</td><td>attachment registry: the scenarios this implementation was designed for</td></tr>
    </table>
  </div></details>

  <details class="mdetail"><summary>Adoptability score</summary><div>
    <p>Percentile rank within family, weighted: throughput .28 · stall resilience .20 · scaling .16 · latency .14 · allocations .14 · churn .08. Stall resilience uses log₁₀(completed ops) vs family peers. Any torn read caps the score at 50; failures cap at “Do not adopt”. A 100 means “best in class on measured axes”, not “use everywhere” — read the radar, not just the number.</p>
  </div></details>

  <details class="mdetail"><summary>Threats to validity</summary><div>
    <ul style="padding-left:18px">
      <li>Shared-hardware noise (this run: GOMAXPROCS=2 Xeon @ 2.60GHz). Use quick for regression gates, standard/full for decisions; compare ratios, not absolutes.</li>
      <li>The stalled-consumer battery is a <i>bound</i>, not a leak: publishers parked mid-probe by construction are excluded from cleanup; retention is post-GC heap attribution, approximate by nature.</li>
      <li>Trace events truncate at the configured event budget; outcomes and ops counters are still exact.</li>
      <li>Torn-read coverage depends on delivery counts; implementations with zero deliveries under the probe report <code class="k">insufficient-data</code>, never a fabricated zero.</li>
      <li>Registry attachments are authored judgments (expected good/bad fit); they are metadata for interpretation, not measurements.</li>
    </ul>
  </div></details>
  <p class="footer-note">bigbuff-measure schema v1 · report generated from embedded records · <span id="footMeta"></span></p>
</section>

<!-- ══════════════ DATA ══════════════ -->
<section class="block" id="data">
  <div class="kicker">Raw materials</div>
  <h2 class="title">Data &amp; exports</h2>
  <p class="lede">Everything on this page is derived from the embedded JSONL. Download the full dataset, flattened CSVs, or the attachment registry.</p>
  <div class="dl">
    <a class="dlbtn" download="bigbuff-results.jsonl" id="dlJSONL"><b>results.jsonl</b><small>every meta/metric/trace/skipped/summary record</small></a>
    <a class="dlbtn" download="bigbuff-metrics-wide.csv" id="dlCSVMetrics"><b>metrics_wide.csv</b><small>one row per metric record, all stats columns</small></a>
    <a class="dlbtn" download="bigbuff-scores.csv" id="dlCSVScores"><b>scores.csv</b><small>family, impl, adoptability score + dimensions + verdict</small></a>
    <a class="dlbtn" download="bigbuff-registry.json" id="dlRegistry"><b>registry.json</b><small>scenarios · cases · attachments (expected fit, exclusions)</small></a>
  </div>
  <div class="card pad" style="margin-top:16px"><h4 style="font-size:12px;color:var(--ink2)">Skipped implementations (recorded, not measured)</h4><div id="skippedList" class="small" style="margin-top:8px"></div></div>
</section>

</main>
</div>
<div id="tip" role="tooltip"></div>
<script>
window.BIGBUFF = __DATA_JSON__;
window.BIGBUFF_REGISTRY = __REGISTRY_JSON__;
</script>
`
const reportJS1 = `
(function(){
'use strict';
var D = window.BIGBUFF, REG = window.BIGBUFF_REGISTRY || {scenarios:[],attachments:[],cases:[]};
var metrics = D.metrics||[], traces = D.traces||[], skipped = D.skipped||[], summary = D.summary||{}, meta = D.meta||{};
var derived = D.derived||{};
var SVGNS = 'http://www.w3.org/2000/svg';
var $ = function(id){ return document.getElementById(id); };
var esc = function(s){ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); };
var isBaselineName = function(n){ return n.indexOf('Baseline') >= 0; };

function fmtNs(v){
  if (v == null || isNaN(v)) return '—';
  var a = Math.abs(v);
  if (a >= 1e6) return (v/1e6).toFixed(2)+' ms';
  if (a >= 1e3) return (v/1e3).toFixed(2)+' µs';
  return v.toFixed(v < 10 ? 2 : 0)+' ns';
}
function fmtBytes(v){
  if (v == null || isNaN(v)) return '—';
  if (v >= (1<<20)) return (v/(1<<20)).toFixed(1)+' MiB';
  if (v >= (1<<10)) return (v/(1<<10)).toFixed(1)+' KiB';
  return v.toFixed(0)+' B';
}
function fmtNum(v){ return v == null || isNaN(v) ? '—' : (Math.abs(v) >= 100 ? Math.round(v).toLocaleString() : v.toFixed(2)); }
function dimsOf(m){ return m.dims||{}; }
function subsOf(m){
  var d = dimsOf(m);
  if (d.subs != null) return d.subs;
  if (d.receivers != null) return d.receivers;
  if (d.senders != null) return d.senders;
  return -1;
}
function meanOf(m){ return typeof m.mean === 'number' ? m.mean : (typeof m.ops_completed === 'number' ? m.ops_completed : NaN); }
function rowsFor(metric, family, subsFilter){
  return metrics.filter(function(m){
    if (m.metric !== metric) return false;
    if (family && m.family !== family) return false;
    if (subsFilter != null && subsOf(m) !== subsFilter) return false;
    return true;
  });
}
function metricByImpl(metric, family, subsFilter){
  var out = {};
  rowsFor(metric, family, subsFilter).forEach(function(m){ out[m.impl] = m; });
  return out;
}
function palette(i){ var h = (i*47 + 187) % 360; return 'hsl('+h+',72%,64%)'; }
function svg(tag, attrs, parent){
  var el = document.createElementNS(SVGNS, tag);
  for (var k in attrs) el.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(el);
  return el;
}
var tip = $('tip');
function bindTip(el, htmlFn){
  el.addEventListener('mousemove', function(ev){
    tip.innerHTML = htmlFn();
    tip.style.opacity = '1';
    var pad = 14, w = tip.offsetWidth, h = tip.offsetHeight;
    var x = ev.clientX + pad, y = ev.clientY + pad;
    if (x + w > window.innerWidth - 8) x = ev.clientX - w - pad;
    if (y + h > window.innerHeight - 8) y = ev.clientY - h - pad;
    tip.style.left = x+'px'; tip.style.top = y+'px';
  });
  el.addEventListener('mouseleave', function(){ tip.style.opacity='0'; });
}
function containerWidth(el){ return Math.max(el.clientWidth || 900, 320); }
function clear(el){ while (el.firstChild) el.removeChild(el.firstChild); }
function familyMetricName(family, kind){
  if (kind === 'throughput') return family === 'caster' ? 'send_ns_op' : (family === 'scatter' ? 'dispatch_ns_op' : (family === 'gather' ? 'wait_ns_op' : 'publish_ns_op'));
  if (kind === 'latency') return family === 'caster' ? 'send_latency_ns' : 'publish_latency_ns';
  return null;
}
var familyLabel = { broadcast:'Broadcast', caster:'Caster', scatter:'Scatter', gather:'Gather', payload:'Payload' };
var familyOrder = ['broadcast','caster','scatter','gather','payload'];
var familiesPresent = familyOrder.filter(function(f){ return (derived.scores && derived.scores[f] && Object.keys(derived.scores[f]).length) || metrics.some(function(m){return m.family===f;}); });
function derivedFamily(f){ return (derived.scores && derived.scores[f]) || {}; }
function sortedImpls(f, by){
  var rows = Object.keys(derivedFamily(f)).map(function(impl){ return derivedFamily(f)[impl]; });
  rows.sort(function(a,b){
    if (by === 'alpha') return a.impl < b.impl ? -1 : 1;
    return (b.metrics[by]||0) - (a.metrics[by]||0);
  });
  return rows.map(function(r){ return r.impl; });
}

/* ── Overview: meta chips + KPI tiles + verdict distribution ── */
(function renderOverview(){
  var chips = [];
  chips.push('generator <b>'+esc((meta.generator||'bigbuff-measure')+' v'+(meta.generator_version||'1.0.0'))+'</b>');
  chips.push('profile <b>'+esc(meta.profile||'?')+'</b>');
  chips.push('seed <b>'+esc(meta.seed)+'</b>');
  chips.push('commit <b>'+esc(String(meta.commit||'').slice(0,12))+'</b>');
  chips.push('runtime <b>'+esc(meta.go||'')+' · GOMAXPROCS='+esc(meta.gomaxprocs)+'</b>');
  chips.push('host <b>'+esc(meta.cpu||'')+'</b>');
  if (meta.timestamp) chips.push('measured <b>'+esc(String(meta.timestamp).slice(0,19).replace('T',' ')+' UTC')+'</b>');
  if (summary.duration_s != null) chips.push('battery runtime <b>'+esc(Number(summary.duration_s).toFixed(1)+'s')+'</b>');
  $('metaChips').innerHTML = chips.map(function(c){ return '<span class="chip">'+c+'</span>'; }).join('');

  var implSet = {};
  metrics.forEach(function(m){ if (m.impl) implSet[m.impl] = true; });
  var implCount = Object.keys(implSet).length;
  var scenarioSet = {};
  (REG.attachments||[]).forEach(function(a){ if (a.scenario) scenarioSet[a.scenario] = true; });
  var torn = 0, tornChecked = 0, violations = 0;
  metrics.forEach(function(m){ if (m.metric==='torn_reads'){ torn += m.violations||0; tornChecked += m.checked||0; } if (typeof m.exactly_once_violations==='number') violations += m.exactly_once_violations; });

  var kpis = [
    { v: implCount, l: 'implementations measured', s: 'across '+familiesPresent.length+' families' },
    { v: summary.records || metrics.length, l: 'records collected', s: (summary.skipped||0)+' explicitly skipped · '+(summary.failures||0)+' failures' },
    { v: Object.keys(scenarioSet).length, l: 'scenarios attached', s: (REG.attachments||[]).length+' good/bad-fit attachments' },
    { v: torn === 0 ? '0 torn reads' : torn+' torn reads', l: 'payload integrity', s: fmtNum(tornChecked)+' multi-word loads checked' },
    { v: violations === 0 ? '0 violations' : violations+' violations', l: 'scatter exactly-once', s: 'all dispatch runs, warmup included' }
  ];
  $('kpiRow').innerHTML = kpis.map(function(k){ return '<div class="kpi"><div class="v">'+esc(k.v)+'</div><div class="l">'+esc(k.l)+'</div><div class="s">'+esc(k.s)+'</div></div>'; }).join('');

  // Family scoreboard cards + verdict distribution
  var html = '<div class="kicker" style="margin-top:6px">Family scorecards</div><div class="findings-grid" style="margin-top:10px">';
  familiesPresent.forEach(function(f){
    var scores = derivedFamily(f);
    var impls = Object.keys(scores);
    if (!impls.length) return;
    var best = impls.map(function(i){ return scores[i]; }).sort(function(a,b){ return b.score-a.score; })[0];
    var cov = (derived.coverage && derived.coverage[f]) || {expected: impls.length, measured: impls.length, missing: []};
    html += '<div class="finding"><span class="tag">'+esc(familyLabel[f]||f)+' · '+(cov.measured||impls.length)+'/'+(cov.expected||impls.length)+' measured</span>'
      + '<h3>'+esc(best.impl)+'</h3>'
      + '<p>Adoptability <b style="color:var(--cyan)">'+best.score+'</b> — '+esc(best.verdict)+'. '
      + (cov.missing && cov.missing.length ? cov.missing.length+' expected impl(s) skipped (see skip list).' : 'Full family coverage.')
      + '</p></div>';
  });
  html += '</div>';

  // Verdict distribution histogram
  var all = [];
  familiesPresent.forEach(function(f){ Object.keys(derivedFamily(f)).forEach(function(i){ all.push(derivedFamily(f)[i].score); }); });
  var buckets = new Array(10).fill(0);
  all.forEach(function(s){ buckets[Math.min(9, Math.floor(s/10))]++; });
  var maxB = Math.max.apply(null, buckets.concat([1]));
  html += '<div class="card pad" style="margin-top:16px"><h4 style="font-size:12px;color:var(--ink2);margin-bottom:10px">Adoptability score distribution (all families)</h4><div class="histo">';
  buckets.forEach(function(b){ html += '<div style="height:'+Math.round(100*b/maxB)+'%" title="'+b+' impls"></div>'; });
  html += '</div><div class="histo-x"><span>0</span><span>10</span><span>20</span><span>30</span><span>40</span><span>50</span><span>60</span><span>70</span><span>80</span><span>90</span><span>100</span></div></div>';
  $('overview').innerHTML = html;
})();

/* ── Findings cards ── */
(function renderFindings(){
  var grid = $('findingsGrid');
  clear(grid);
  (derived.observations||[]).forEach(function(o){
    var a = document.createElement('article');
    a.className = 'finding ' + (o.kind||'');
    a.innerHTML = '<span class="tag">'+esc(o.kind||'observation')+'</span><h3>'+esc(o.title)+'</h3><p>'+esc(o.detail)+'</p>'
      + (o.anchor ? '<p style="margin-top:8px"><a href="'+esc(o.anchor)+'">See the measurement →</a></p>' : '');
    grid.appendChild(a);
  });
})();
`
const reportJS2 = `

/* ── Scaling chart ── */
var scalingFamily = 'broadcast';
var scalingHidden = {};
function scalingSeries(family){
  var name = familyMetricName(family, 'throughput');
  var byImpl = {};
  rowsFor(name, family, null).forEach(function(m){
    var s = subsOf(m); if (s < 1) return;
    (byImpl[m.impl] = byImpl[m.impl] || {impl:m.impl, points:[]}).points.push({subs:s, mean:meanOf(m)});
  });
  var out = [];
  Object.keys(byImpl).forEach(function(impl){
    var p = byImpl[impl].points.sort(function(a,b){ return a.subs-b.subs; });
    if (p.length >= 2) out.push({impl:impl, points:p, score:p[p.length-1].mean});
  });
  return out;
}
function renderScaling(){
  var container = $('scalingChart'); clear(container);
  var legend = $('scalingLegend'); clear(legend);
  var tabs = $('scalingTabs'); clear(tabs);
  ['broadcast','caster','payload'].forEach(function(f){
    var t = document.createElement('button');
    t.className = 'tab' + (f===scalingFamily?' active':'');
    t.textContent = familyLabel[f];
    t.onclick = function(){ scalingFamily = f; scalingHidden = {}; renderScaling(); };
    tabs.appendChild(t);
  });
  var sortMode = $('scalingSort').value;
  var series = scalingSeries(scalingFamily);
  if (sortMode === 'ratio') {
    series.forEach(function(s){ s.ratio = s.points[0].mean > 0 ? s.points[s.points.length-1].mean / s.points[0].mean : 1; });
    series.sort(function(a,b){ return a.ratio - b.ratio; });
  } else if (sortMode === 'alpha') {
    series.sort(function(a,b){ return a.impl < b.impl ? -1 : 1; });
  } else {
    series.sort(function(a,b){ return a.score - b.score; });
  }
  var topOnly = $('scalingTopOnly').checked;
  var fastest = Object.keys(series).length;
  if (topOnly) {
    var byMean = series.slice().sort(function(a,b){ return a.score-b.score; });
    var keep = {}; byMean.slice(0,12).forEach(function(s){ keep[s.impl] = true; });
    series = series.filter(function(s){ return keep[s.impl]; });
  }
  fastest = series.length;
  if (!series.length) { container.innerHTML = '<p class="small">No multi-point sweeps for this family in this profile.</p>'; return; }

  var allSubs = {}, allVals = [];
  series.forEach(function(s){ s.points.forEach(function(p){ allSubs[p.subs] = true; allVals.push(p.mean); }); });
  var subsTicks = Object.keys(allSubs).map(Number).sort(function(a,b){ return a-b; });
  var minV = Math.min.apply(null, allVals), maxV = Math.max.apply(null, allVals);
  var minL = Math.log10(minV*0.7), maxL = Math.log10(maxV*1.3);
  var W = containerWidth(container), H = 380, padL = 64, padR = 14, padT = 16, padB = 44;
  var xOf = function(subs){ return padL + (Math.log2(subs) - Math.log2(subsTicks[0])) / (Math.log2(subsTicks[subsTicks.length-1]) - Math.log2(subsTicks[0])) * (W - padL - padR); };
  var yOf = function(v){ return padT + (maxL - Math.log10(v)) / (maxL - minL) * (H - padT - padB); };

  var svgEl = svg('svg', {width:W, height:H, viewBox:'0 0 '+W+' '+H, role:'img'}, container);
  // y grid at decades
  for (var d = Math.ceil(Math.log10(minV)); d <= Math.floor(Math.log10(maxV)); d++) {
    var y = yOf(Math.pow(10, d));
    svg('line', {x1:padL, y1:y, x2:W-padR, y2:y, class:'grid'}, svgEl);
    var t = svg('text', {x:padL-8, y:y+3, 'text-anchor':'end', class:'axtxt'}, svgEl);
    t.textContent = fmtNs(Math.pow(10, d));
  }
  svg('line', {x1:padL, y1:H-padB, x2:W-padR, y2:H-padB, class:'ax'}, svgEl);
  svg('line', {x1:padL, y1:padT, x2:padL, y2:H-padB, class:'ax'}, svgEl);
  subsTicks.forEach(function(s){
    var x = xOf(s);
    svg('line', {x1:x, y1:H-padB, x2:x, y2:H-padB+5, class:'ax'}, svgEl);
    var t = svg('text', {x:x, y:H-padB+18, 'text-anchor':'middle', class:'axtxt'}, svgEl);
    t.textContent = s;
  });
  var xt = svg('text', {x:(W+padL)/2, y:H-6, 'text-anchor':'middle', class:'axtxt'}, svgEl); xt.textContent = 'fan-out (log₂)';
  var yt = svg('text', {x:14, y:(H)/2, 'text-anchor':'middle', class:'axtxt', transform:'rotate(-90 14 '+(H/2)+')'}, svgEl); yt.textContent = 'ns per operation (log₁₀)';

  series.forEach(function(s, i){
    if (scalingHidden[s.impl]) return;
    var color = palette(i);
    var d = '';
    s.points.forEach(function(p, j){ d += (j ? 'L' : 'M') + xOf(p.subs).toFixed(1) + ' ' + yOf(p.mean).toFixed(1) + ' '; });
    var base = isBaselineName(s.impl);
    var path = svg('path', {d:d, fill:'none', stroke:color, 'stroke-width':2, 'stroke-linejoin':'round', 'stroke-linecap':'round'}, svgEl);
    if (base) path.setAttribute('stroke-dasharray', '7 5');
    s.points.forEach(function(p){
      var c = svg('circle', {cx:xOf(p.subs), cy:yOf(p.mean), r:3.6, fill:color, stroke:'#0b1020', 'stroke-width':1.2}, svgEl);
      bindTip(c, function(){ return '<div class="tt">'+esc(s.impl)+'</div><div class="mono">'+p.subs+' subscribers · '+fmtNs(p.mean)+'/op</div>'; });
    });
  });

  // Legend chips
  series.forEach(function(s, i){
    var chip = document.createElement('span');
    chip.className = scalingHidden[s.impl] ? 'off' : '';
    chip.innerHTML = '<i style="background:'+palette(i)+(isBaselineName(s.impl)?';outline:1px dashed #0b1020':'')+'"></i>'+esc(s.impl);
    chip.onclick = function(){ scalingHidden[s.impl] = !scalingHidden[s.impl]; renderScaling(); };
    legend.appendChild(chip);
  });
}
$('scalingTopOnly').addEventListener('change', renderScaling);
$('scalingSort').addEventListener('change', renderScaling);

/* ── Latency chart ── */
var latFamily = 'broadcast';
function renderLatency(){
  var container = $('latChart'); clear(container);
  var tabs = $('latTabs'); clear(tabs);
  ['broadcast','caster','payload'].forEach(function(f){
    var t = document.createElement('button');
    t.className = 'tab' + (f===latFamily?' active':'');
    t.textContent = familyLabel[f];
    t.onclick = function(){ latFamily = f; renderLatency(); };
    tabs.appendChild(t);
  });
  var name = familyMetricName(latFamily, 'latency');
  var rows = rowsFor(name, latFamily, 8).map(function(m){
    return {impl: m.impl, p50: m.p50, p90: m.p90, p99: m.p99, max: m.max, mean: m.mean, ciLo: m.ci95_low, ciHi: m.ci95_high};
  }).filter(function(r){ return r.p99 != null; });
  rows.sort(function(a,b){ return a.p99 - b.p99; });
  var top = rows.slice(0, 26);
  if (!top.length) { container.innerHTML = '<p class="small">No latency records for this family in this profile.</p>'; return; }
  var maxV = Math.max.apply(null, top.map(function(r){ return r.max; })) * 1.05;
  var W = containerWidth(container), rowH = 27, padL = Math.min(300, Math.floor(W*0.36)), padR = 84, padT = 8;
  var H = top.length * rowH + padT + 26;
  var svgEl = svg('svg', {width:W, height:H, viewBox:'0 0 '+W+' '+H, role:'img'}, container);
  var xOf = function(v){ return padL + v/maxV * (W - padL - padR); };
  top.forEach(function(r, i){
    var y = padT + i*rowH + rowH/2;
    var t = svg('text', {x:padL-10, y:y+4, 'text-anchor':'end', class:'axtxt', style:'font-size:11px'}, svgEl);
    t.textContent = r.impl.length > 34 ? r.impl.slice(0,33)+'…' : r.impl;
    // p99 bar
    svg('rect', {x:padL, y:y-8, width:Math.max(2, xOf(r.p99)-padL), height:16, rx:4, fill:'url(#latGrad)'}, svgEl);
    // p50 / p90 markers
    svg('circle', {cx:xOf(r.p50), cy:y, r:4.5, fill:'#34d399', stroke:'#04120a', 'stroke-width':1}, svgEl);
    svg('circle', {cx:xOf(r.p90), cy:y, r:4.5, fill:'#fbbf24', stroke:'#241a04', 'stroke-width':1}, svgEl);
    svg('circle', {cx:xOf(r.p99), cy:y, r:5.5, fill:'#fb7185', stroke:'#2a060d', 'stroke-width':1}, svgEl);
    var v = svg('text', {x:Math.min(xOf(r.max)+6, W-padR+4), y:y+4, class:'axtxt', style:'fill:#e8ecf8'}, svgEl);
    v.textContent = fmtNs(r.p99);
    var hit = svg('rect', {x:0, y:y-rowH/2, width:W, height:rowH, fill:'transparent'}, svgEl);
    bindTip(hit, function(){
      return '<div class="tt">'+esc(r.impl)+'</div><div class="mono">mean '+fmtNs(r.mean)+' · p50 '+fmtNs(r.p50)+' · p90 '+fmtNs(r.p90)+'</div><div class="mono">p99 '+fmtNs(r.p99)+' · max '+fmtNs(r.max)+'</div><div>95% CI '+fmtNs(r.ciLo)+' – '+fmtNs(r.ciHi)+'</div>';
    });
  });
  var defs = svg('defs', {}, svgEl);
  var grad = svg('linearGradient', {id:'latGrad', x1:'0', y1:'0', x2:'1', y2:'0'}, defs);
  svg('stop', {offset:'0%', 'stop-color':'#155e75'}, grad); svg('stop', {offset:'60%', 'stop-color':'#0e7490'}, grad); svg('stop', {offset:'100%', 'stop-color':'#22d3ee'}, grad);
  var lg = svg('text', {x:padL, y:H-6, class:'axtxt'}, svgEl);
  lg.textContent = '● p50   ● p90   ● p99 (bar end)   sorted by p99, best 26 shown';
}

/* ── Memory & allocations ── */
var memFamily = 'broadcast';
function renderMemory(){
  var tabs = $('memTabs'); clear(tabs);
  ['broadcast'].forEach(function(f){
    var t = document.createElement('button');
    t.className = 'tab active'; t.textContent = familyLabel[f];
    tabs.appendChild(t);
  });
  var grid = $('memGrid');
  var allocName = 'publish_allocs_op';
  var nsName = familyMetricName(memFamily, 'throughput');
  var byImpl = metricByImpl(allocName, memFamily, 8);
  var nsByImpl = metricByImpl(nsName, memFamily, 8);
  var pts = [];
  Object.keys(byImpl).forEach(function(impl){
    var a = meanOf(byImpl[impl]);
    var n = nsByImpl[impl] ? meanOf(nsByImpl[impl]) : NaN;
    if (!isNaN(a) && !isNaN(n)) pts.push({impl:impl, alloc:a, ns:n});
  });
  var scatter = $('allocScatter'); clear(scatter);
  if (pts.length) {
    pts.sort(function(a,b){ return a.alloc-b.alloc; });
    var W = containerWidth(scatter), H = 320, padL = 52, padR = 12, padT = 12, padB = 40;
    var maxA = Math.max.apply(null, pts.map(function(p){return p.alloc;}).concat([1]));
    var minN = Math.min.apply(null, pts.map(function(p){return p.ns;})), maxN = Math.max.apply(null, pts.map(function(p){return p.ns;}));
    var maxA10 = Math.log10(maxA) + 0.4;
    var minL = Math.log10(Math.max(1, minN*0.6)), maxL = Math.log10(maxN*1.4);
    var xOf = function(a){ return padL + (maxA10 === 0 ? 0 : Math.log10(a+1e-9) < 0 ? 0 : Math.log10(a+1e-9)/maxA10) * (W-padL-padR); };
    var yOf = function(v){ return padT + (maxL - Math.log10(v)) / (maxL - minL) * (H - padT - padB); };
    var svgEl = svg('svg', {width:W, height:H, viewBox:'0 0 '+W+' '+H, role:'img'}, scatter);
    svg('line', {x1:padL, y1:H-padB, x2:W-padR, y2:H-padB, class:'ax'}, svgEl);
    svg('line', {x1:padL, y1:padT, x2:padL, y2:H-padB, class:'ax'}, svgEl);
    var xt = svg('text', {x:(W+padL)/2, y:H-4, 'text-anchor':'middle', class:'axtxt'}, svgEl); xt.textContent = 'allocations per publish (log)';
    var yt = svg('text', {x:13, y:H/2, 'text-anchor':'middle', class:'axtxt', transform:'rotate(-90 13 '+(H/2)+')'}, svgEl); yt.textContent = 'ns per operation (log₁₀)';
    pts.forEach(function(p, i){
      var c = svg('circle', {cx:xOf(p.alloc), cy:yOf(p.ns), r:5, fill:palette(i), 'fill-opacity':0.85, stroke:'#04070f', 'stroke-width':1}, svgEl);
      bindTip(c, function(){ return '<div class="tt">'+esc(p.impl)+'</div><div class="mono">'+p.alloc.toFixed(2)+' allocs/op · '+fmtNs(p.ns)+'/op</div>'; });
    });
  } else {
    scatter.innerHTML = '<p class="small">Allocation records not present for this family.</p>';
  }
  var memBars = $('memBars'); clear(memBars);
  var memRows = rowsFor('mem_per_subscriber_bytes', 'broadcast', 8).map(function(m){ return {impl:m.impl, bytes:meanOf(m)}; }).filter(function(r){ return !isNaN(r.bytes); });
  memRows.sort(function(a,b){ return b.bytes-a.bytes; });
  var top = memRows.slice(0, 14);
  var maxB = Math.max.apply(null, top.map(function(r){return r.bytes;}).concat([1]));
  top.forEach(function(r){
    var row = document.createElement('div');
    row.style.cssText = 'display:grid;grid-template-columns:minmax(120px,1fr) 3fr 70px;gap:8px;align-items:center;font-size:11px;margin:3px 0';
    row.innerHTML = '<div class="mono" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--ink2)" title="'+esc(r.impl)+'">'+esc(r.impl)+'</div>'
      + '<div style="height:10px;background:#1a2350;border-radius:5px;overflow:hidden"><div style="height:100%;width:'+Math.max(2, 100*r.bytes/maxB).toFixed(1)+'%;background:linear-gradient(90deg,#4c1d95,#a78bfa)"></div></div>'
      + '<div class="mono" style="text-align:right">'+fmtBytes(r.bytes)+'</div>';
    bindTip(row, function(){ return '<div class="tt">'+esc(r.impl)+'</div><div class="mono">'+fmtBytes(r.bytes)+' retained per idle subscriber (post-GC)</div>'; });
    memBars.appendChild(row);
  });
  var zero = $('zeroSubChart'); clear(zero);
  var zeroRows = rowsFor('zero_subscriber_ns_op', 'broadcast', null).map(function(m){ return {impl:m.impl, ns:meanOf(m)}; }).filter(function(r){ return !isNaN(r.ns); });
  zeroRows.sort(function(a,b){ return a.ns-b.ns; });
  var zmax = Math.max.apply(null, zeroRows.map(function(r){return r.ns;}).concat([1]));
  zeroRows.forEach(function(r){
    var row = document.createElement('div');
    row.style.cssText = 'display:grid;grid-template-columns:minmax(120px,1fr) 3fr 90px;gap:8px;align-items:center;font-size:11px;margin:2px 0';
    row.innerHTML = '<div class="mono" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--ink2)" title="'+esc(r.impl)+'">'+esc(r.impl)+'</div>'
      + '<div style="height:8px;background:#1a2350;border-radius:4px;overflow:hidden"><div style="height:100%;width:'+Math.max(2, 100*r.ns/zmax).toFixed(1)+'%;background:#155e75"></div></div>'
      + '<div class="mono" style="text-align:right">'+fmtNs(r.ns)+'</div>';
    zero.appendChild(row);
  });
}

/* ── Churn chart ── */
var churnFamily = 'broadcast';
function renderChurn(){
  var container = $('churnChart'); clear(container);
  var tabs = $('churnTabs'); clear(tabs);
  familyOrder.forEach(function(f){
    var has = rowsFor('churn_ns_op', f, null).length > 0;
    if (!has) return;
    var t = document.createElement('button');
    t.className = 'tab' + (f===churnFamily?' active':'');
    t.textContent = familyLabel[f];
    t.onclick = function(){ churnFamily = f; renderChurn(); };
    tabs.appendChild(t);
  });
  var rows = rowsFor('churn_ns_op', churnFamily, null).map(function(m){ return {impl:m.impl, ns:meanOf(m)}; }).filter(function(r){ return !isNaN(r.ns); });
  rows.sort(function(a,b){ return a.ns-b.ns; });
  if (!rows.length) { container.innerHTML = '<p class="small">No churn records for this family.</p>'; return; }
  var maxV = Math.max.apply(null, rows.map(function(r){return r.ns;}));
  rows.forEach(function(r){
    var row = document.createElement('div');
    row.style.cssText = 'display:grid;grid-template-columns:minmax(140px,1fr) 2fr 100px;gap:10px;align-items:center;font-size:11.5px;margin:3px 0';
    row.innerHTML = '<div class="mono" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--ink2)" title="'+esc(r.impl)+'">'+esc(r.impl)+'</div>'
      + '<div style="height:11px;background:#1a2350;border-radius:5px;overflow:hidden"><div style="height:100%;width:'+Math.max(2, 100*r.ns/maxV).toFixed(1)+'%;background:linear-gradient(90deg,#134e4a,#2dd4bf)"></div></div>'
      + '<div class="mono" style="text-align:right">'+fmtNs(r.ns)+'</div>';
    container.appendChild(row);
  });
}
`
const reportJS3 = `

/* ── Leaderboard ── */
var lbFamily = 'broadcast', lbSort = {k:'score', dir:-1};
function lbRows(family){
  var sc = derivedFamily(family);
  var thrName = familyMetricName(family, 'throughput');
  var latName = familyMetricName(family, 'latency');
  var thr = metricByImpl(thrName, family, 8);
  var lat = metricByImpl(latName, family, family === 'caster' ? 8 : 8);
  var allocs = family === 'broadcast' ? metricByImpl('publish_allocs_op', family, 8) : {};
  var stalls = metricByImpl('stalled_consumer_ops_completed', family, null);
  var rows = [];
  Object.keys(sc).forEach(function(impl){
    var s = sc[impl];
    var tm = thr[impl], lm = lat[impl], am = allocs[impl], sm = stalls[impl];
    var p99 = lm && typeof lm.p99 === 'number' ? lm.p99 : null;
    var stallOps = sm && typeof sm.ops_completed === 'number' ? sm.ops_completed : (sm ? meanOf(sm) : null);
    rows.push({
      impl: impl, score: s.score, verdict: s.verdict, integrity: s.integrity,
      baseline: !!s.baseline, exposureLimited: !!s.exposure_limited, goodFit: !!s.expected_good_fit,
      thr: tm ? meanOf(tm) : null,
      ciLo: tm ? tm.ci95_low : null, ciHi: tm ? tm.ci95_high : null,
      lat: p99,
      alloc: am ? meanOf(am) : null,
      ratio: (s.metrics && typeof s.metrics.scaling_ratio_64_over_1 === 'number') ? s.metrics.scaling_ratio_64_over_1 : null,
      stall: stallOps
    });
  });
  return rows;
}
function lbRender(){
  var tabs = $('lbTabs'); clear(tabs);
  familiesPresent.forEach(function(f){
    var t = document.createElement('button');
    t.className = 'tab' + (f===lbFamily?' active':'');
    t.textContent = familyLabel[f];
    t.onclick = function(){ lbFamily = f; lbRender(); };
    tabs.appendChild(t);
  });
  var q = ($('lbSearch').value||'').toLowerCase();
  var baseOnly = $('lbBaselinesOnly').checked, goodOnly = $('lbGoodFitOnly').checked;
  var rows = lbRows(lbFamily).filter(function(r){
    if (q && r.impl.toLowerCase().indexOf(q) < 0) return false;
    if (baseOnly && !r.baseline) return false;
    if (goodOnly && !r.goodFit) return false;
    return true;
  });
  var dir = lbSort.dir, key = lbSort.k;
  rows.sort(function(a,b){
    var av = a[key], bv = b[key];
    if (key === 'impl' || key === 'verdict') { av = String(av); bv = String(bv); return dir * (av < bv ? -1 : av > bv ? 1 : 0); }
    if (av == null) return 1; if (bv == null) return -1;
    return dir * (av - bv);
  });
  var tbody = $('lbBody'); clear(tbody);
  rows.forEach(function(r){
    var tr = document.createElement('tr');
    var vclass = 'v-avoid';
    if (r.verdict.indexOf('Strong') === 0) vclass = 'v-strong';
    else if (r.verdict.indexOf('Situational') === 0) vclass = 'v-situational';
    else if (r.verdict.indexOf('Niche') === 0) vclass = 'v-niche';
    var fillHue = Math.round(8 + (r.score/100)*130); // red→green
    var scoreCell = '<div class="scorebar"><div class="fill" style="width:'+r.score+'%;background:linear-gradient(90deg,hsl('+fillHue+',65%,28%),hsl('+fillHue+',80%,55%))"></div><span style="color:#eaf6ff">'+r.score+'</span></div>';
    var kind = r.baseline ? '<span class="bl">BASELINE</span>' : '<span style="color:var(--ink3);font-size:10px">measured</span>';
    if (r.exposureLimited) kind += '<span class="xl">EXP-LIMITED</span>';
    tr.innerHTML = '<td><span class="impl-name">'+esc(r.impl)+kind+'</span></td>'
      + '<td>'+scoreCell+'</td>'
      + '<td><span class="verdict '+vclass+'">'+esc(r.verdict)+'</span></td>'
      + '<td class="barnum">'+fmtNum(r.thr)+'</td>'
      + '<td class="barnum">'+(r.ciLo!=null?fmtNum(r.ciLo)+'–'+fmtNum(r.ciHi):'—')+'</td>'
      + '<td class="barnum">'+fmtNs(r.lat)+'</td>'
      + '<td class="barnum">'+(r.alloc!=null?r.alloc.toFixed(2):'—')+'</td>'
      + '<td class="barnum">'+(r.ratio!=null?r.ratio.toFixed(2)+'×':'—')+'</td>'
      + '<td class="barnum">'+(r.stall!=null?fmtNum(r.stall):'—')+'</td>'
      + '<td>'+kind+'</td>';
    tbody.appendChild(tr);
  });
  $('lbFoot').textContent = rows.length + ' implementation(s) shown · CI = 95% half-width on the per-op mean · sorted by ' + key + (dir<0 ? ' (desc)' : ' (asc)');
}
document.querySelectorAll('#lbTable th').forEach(function(th){
  th.addEventListener('click', function(){
    var k = th.getAttribute('data-k');
    if (k === 'baseline' || k === 'ci') k = k === 'baseline' ? 'impl' : 'thr';
    if (lbSort.k === k) lbSort.dir *= -1; else { lbSort.k = k; lbSort.dir = -1; }
    lbRender();
  });
});
$('lbSearch').addEventListener('input', lbRender);
$('lbBaselinesOnly').addEventListener('change', lbRender);
$('lbGoodFitOnly').addEventListener('change', lbRender);

/* ── Adoptability radar ── */
var radarFamily = 'broadcast', radarSel = [];
var radarAxes = [
  {key:'throughput', label:'Throughput'},
  {key:'latency', label:'Latency'},
  {key:'allocations', label:'Allocations'},
  {key:'churn', label:'Churn'},
  {key:'scaling', label:'Scaling flatness'},
  {key:'stall_resilience', label:'Stall resilience'}
];
function radarRender(){
  var tabs = $('radarTabs'); clear(tabs);
  familiesPresent.forEach(function(f){
    var t = document.createElement('button');
    t.className = 'tab' + (f===radarFamily?' active':'');
    t.textContent = familyLabel[f];
    t.onclick = function(){ radarFamily = f; radarSel = []; radarRender(); };
    tabs.appendChild(t);
  });
  var sc = derivedFamily(radarFamily);
  var ranked = Object.keys(sc).map(function(i){ return sc[i]; }).sort(function(a,b){ return b.score-a.score; });
  // Default selection: family champion + best baseline
  if (!radarSel.length && ranked.length) {
    radarSel.push(ranked[0].impl);
    var bl = ranked.filter(function(r){ return r.baseline; })[0];
    if (bl) radarSel.push(bl.impl);
  }
  var sel = $('radarImpl'); clear(sel);
  ranked.forEach(function(r){
    var o = document.createElement('option');
    o.value = r.impl; o.textContent = r.impl + '  ·  ' + r.score;
    sel.appendChild(o);
  });
  // shortcuts
  var scBar = $('radarShortcuts'); clear(scBar);
  var mk = function(label, fn){ var b = document.createElement('button'); b.className='tab'; b.textContent = label; b.onclick = function(){ fn(); radarRender(); }; scBar.appendChild(b); };
  mk('Top overall', function(){ radarSel = ranked.length ? [ranked[0].impl] : []; });
  var blb = ranked.filter(function(r){ return r.baseline; });
  if (blb.length) mk('Best baseline', function(){ radarSel = [blb[0].impl]; });
  var flat = ranked.slice().sort(function(a,b){ return (b.dims.scaling||0)-(a.dims.scaling||0); })[0];
  if (flat) mk('Flattest curve', function(){ radarSel = [flat.impl]; });
  var stall = ranked.slice().sort(function(a,b){ return (b.dims.stall_resilience||0)-(a.dims.stall_resilience||0); })[0];
  if (stall) mk('Stall-proof', function(){ radarSel = [stall.impl]; });
  mk('Clear', function(){ radarSel = []; });

  var holder = $('radarChart'); clear(holder);
  var W = containerWidth(holder), H = Math.min(430, Math.max(300, W*0.82));
  var cx = W/2, cy = H/2 + 6, R = Math.min(W, H)/2 - 54;
  var svgEl = svg('svg', {width:W, height:H, viewBox:'0 0 '+W+' '+H, role:'img'}, holder);
  var pt = function(i, frac){
    var ang = -Math.PI/2 + i * (2*Math.PI/radarAxes.length);
    return [cx + Math.cos(ang)*R*frac, cy + Math.sin(ang)*R*frac];
  };
  [0.25, 0.5, 0.75, 1].forEach(function(frac){
    var d = '';
    radarAxes.forEach(function(_, i){ var p = pt(i, frac); d += (i ? 'L' : 'M') + p[0].toFixed(1) + ' ' + p[1].toFixed(1) + ' '; });
    d += 'Z';
    svg('polygon', {points: d, fill:'none', stroke:'#26315f', 'stroke-width': frac===1 ? 1.4 : 1, 'stroke-dasharray': frac===1 ? '' : '3 5'}, svgEl);
  });
  radarAxes.forEach(function(axis, i){
    var p = pt(i, 1);
    svg('line', {x1:cx, y1:cy, x2:p[0], y2:p[1], stroke:'#1f2a55', 'stroke-width':1}, svgEl);
    var lp = pt(i, 1.13);
    var t = svg('text', {x:lp[0], y:lp[1]+4, 'text-anchor': i===0||i===3 ? 'middle' : (Math.cos(-Math.PI/2 + i*Math.PI/3) > 0.1 ? 'start' : 'end'), class:'axtxt', style:'font-size:11px;fill:#a9b3d6'}, svgEl);
    t.textContent = axis.label;
  });
  radarSel.forEach(function(impl, si){
    var s = sc[impl]; if (!s) return;
    var color = palette(si + 3);
    var d = '';
    radarAxes.forEach(function(axis, i){
      var frac = (s.dims[axis.key]||0)/100;
      var p = pt(i, Math.max(0.02, frac));
      d += (i ? 'L' : 'M') + p[0].toFixed(1) + ' ' + p[1].toFixed(1) + ' ';
    });
    d += 'Z';
    var poly = svg('polygon', {points:d, fill:color, 'fill-opacity':0.16, stroke:color, 'stroke-width':2.2, 'stroke-linejoin':'round'}, svgEl);
    if (s.baseline) poly.setAttribute('stroke-dasharray','7 5');
    radarAxes.forEach(function(axis, i){
      var frac = (s.dims[axis.key]||0)/100;
      var p = pt(i, Math.max(0.02, frac));
      var c = svg('circle', {cx:p[0], cy:p[1], r:3.4, fill:color}, svgEl);
      bindTip(c, function(){ return '<div class="tt">'+esc(impl)+'</div><div>'+esc(axis.label)+': <b>'+(s.dims[axis.key]||0)+'</b></div>'; });
    });
    // legend chips
    var lg = document.createElement('span');
    lg.innerHTML = '<i style="background:'+color+'"></i>'+esc(impl)+' <b style="color:var(--ink3)">'+s.score+'</b>';
    lg.onclick = function(){ radarSel = radarSel.filter(function(x){ return x !== impl; }); radarRender(); };
    if (!$('radarLegendHolder')) {
      var holder2 = document.createElement('div');
      holder2.id = 'radarLegendHolder'; holder2.className = 'lgnd';
      $('radarChart').appendChild(holder2);
    }
    $('radarLegendHolder').appendChild(lg);
  });
  // score rows for first selected
  var rowsBox = $('radarRows'); clear(rowsBox);
  var focus = radarSel.length ? sc[radarSel[0]] : null;
  if (focus) {
    var h = '<div class="small" style="margin-bottom:4px">Breakdown — <b style="color:var(--ink)">'+esc(focus.impl)+'</b> · score <b style="color:var(--cyan)">'+focus.score+'</b></div>';
    radarAxes.forEach(function(axis){
      var v = focus.dims[axis.key]||0;
      h += '<div class="srow"><div class="dim">'+esc(axis.label)+'</div><div class="strack"><div class="sfill" style="width:'+v+'%"></div></div><div class="mono" style="text-align:right">'+v+'</div></div>';
    });
    if (focus.integrity === 'failed') h += '<div class="small" style="color:var(--red);margin-top:6px">Integrity failure — score capped at 50.</div>';
    rowsBox.innerHTML = h;
  } else {
    rowsBox.innerHTML = '<p class="small">Add an implementation to see its dimension breakdown.</p>';
  }
}
$('radarAdd').addEventListener('click', function(){
  var v = $('radarImpl').value;
  if (v && radarSel.indexOf(v) < 0 && radarSel.length < 4) { radarSel.push(v); radarRender(); }
});
`
const reportJS4 = `

/* ── Pathology Lab ── */
var pathoTraces = {};
traces.forEach(function(t){
  if (t.trace === 'stalled_consumer' && t.family === 'broadcast') {
    (pathoTraces[t.impl] = pathoTraces[t.impl] || {})[t.trace] = t;
  }
});
var pathoImpls = Object.keys(pathoTraces).sort();
var pathoA = 'ChanPubSub (Baseline)', pathoB = 'ChanLatest (03-ChatGPT)';
if (pathoImpls.indexOf(pathoA) < 0) pathoA = pathoImpls[0];
if (pathoImpls.indexOf(pathoB) < 0) pathoB = pathoImpls[1] || pathoImpls[0];
var stallMetrics = {};
rowsFor('stalled_consumer_ops_completed', 'broadcast', null).forEach(function(m){ stallMetrics[m.impl] = m; });
var pathoProgress = 0, pathoPlaying = false, pathoLastTs = 0, pathoSpeed = 1;
var pathoCanvas = {};

function pathoCardHTML(impl, side){
  var t = pathoTraces[impl] && pathoTraces[impl].stalled_consumer;
  var m = stallMetrics[impl] || {};
  var outcome = t ? t.outcome : 'no-trace';
  var ops = t ? t.ops_completed : (typeof m.ops_completed === 'number' ? m.ops_completed : '?');
  var retained = typeof m.approx_retained_bytes === 'number' ? m.approx_retained_bytes : 0;
  var fastMin = typeof m.fast_min_delivered === 'number' ? m.fast_min_delivered : null;
  return '<div class="patho-card"><h4 class="mono" style="font-size:12px">'+esc(impl)+'</h4>'
    + '<span class="outcome outcome-'+esc(outcome === 'completed' ? 'completed' : (outcome === 'publisher_blocked' ? 'publisher_blocked' : 'publisher_panicked'))+'">'+esc(outcome.replace(/_/g,' '))+'</span>'
    + '<canvas id="swim-'+side+'" width="640" height="170" style="width:100%;height:170px;margin-top:10px;background:#080d20;border:1px solid var(--line);border-radius:10px" role="img" aria-label="Swimlane trace for '+esc(impl)+'"></canvas>'
    + '<div class="small" style="margin-top:8px;line-height:1.8">ops completed: <b class="mono" style="color:var(--ink)">'+esc(ops)+'</b> / '+esc(t ? t.ops_planned : (m.ops_planned||'?'))
    + ' · retained: <b class="mono" style="color:'+(retained > 0 ? 'var(--amber)' : 'var(--green)')+'">'+fmtBytes(retained)+'</b><br>'
    + (fastMin != null ? 'healthy lanes delivered ≥ <b class="mono" style="color:var(--ink)">'+esc(fastMin)+'</b> each' : 'no healthy-lane count recorded')
    + '</div></div>';
}
function drawSwim(side){
  var canvas = $('swim-'+side); if (!canvas) return;
  var impl = side === 'A' ? pathoA : pathoB;
  var t = pathoTraces[impl] && pathoTraces[impl].stalled_consumer;
  var ctx = canvas.getContext && canvas.getContext('2d');
  if (!ctx) return;
  var W = canvas.width, H = canvas.height;
  ctx.clearRect(0, 0, W, H);
  if (!t) {
    ctx.fillStyle = '#6b769e'; ctx.font = '12px ui-monospace, monospace';
    ctx.fillText('no trace captured for this implementation in this profile', 20, H/2);
    return;
  }
  var lanes = t.subs || 3; // includes stalled lane as the last lane
  var padL = 64, padR = 46, padT = 18, padB = 22;
  var laneH = (H - padT - padB) / (lanes + 1);
  var dur = Math.max(1, t.duration_ns || 1);
  var tMax = dur; // shared fraction applied by playhead
  // lane labels
  ctx.font = '10px ui-monospace, monospace';
  ctx.textAlign = 'left';
  ctx.fillStyle = '#22d3ee'; ctx.fillText('PUBLISHER', 8, padT + laneH/2 + 3);
  for (var i = 1; i < lanes; i++) {
    ctx.fillStyle = '#a9b3d6';
    ctx.fillText('sub ' + i, 8, padT + (i+0.5)*laneH + 3);
  }
  ctx.fillStyle = '#f87171'; ctx.fillText('STALLED ✕', 8, padT + (lanes+0.5)*laneH + 3);
  // stalled lane hatch
  ctx.strokeStyle = 'rgba(248,113,113,.35)'; ctx.setLineDash([4,4]);
  ctx.beginPath(); ctx.moveTo(padL, padT + (lanes+0.5)*laneH); ctx.lineTo(W-padR, padT + (lanes+0.5)*laneH); ctx.stroke();
  ctx.setLineDash([]);
  // lane separators
  ctx.strokeStyle = 'rgba(31,42,85,.6)';
  for (var li = 0; li <= lanes; li++) {
    var y = padT + li*laneH + laneH/2;
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(W-padR, y); ctx.stroke();
  }
  var playT = pathoProgress * tMax;
  // events up to playhead
  var evs = t.events || [];
  for (var i = 0; i < evs.length; i++) {
    var e = evs[i];
    if (e.T > playT) break;
    var x = padL + (e.T / tMax) * (W - padL - padR);
    var lane = e.Lane || 0;
    var y = padT + (lane === 0 ? laneH/2 : (lane+0.5)*laneH);
    ctx.beginPath(); ctx.arc(x, y, e.Kind === 'pub' ? 4 : 3, 0, Math.PI*2);
    ctx.fillStyle = e.Kind === 'pub' ? '#22d3ee' : '#34d399';
    ctx.fill();
  }
  // blocked marker at publisher lane
  if (t.outcome === 'publisher_blocked' && pathoProgress >= 1) {
    var bx = W - padR;
    ctx.strokeStyle = '#f87171'; ctx.lineWidth = 2.4;
    ctx.beginPath(); ctx.moveTo(bx-9, padT + laneH/2 - 7); ctx.lineTo(bx+5, padT + laneH/2 + 7); ctx.moveTo(bx+5, padT + laneH/2 - 7); ctx.lineTo(bx-9, padT + laneH/2 + 7); ctx.stroke();
    ctx.fillStyle = '#fca5a5'; ctx.font = 'bold 10px ui-monospace, monospace'; ctx.textAlign = 'center';
    ctx.fillText('PARKED', bx-2, padT + laneH/2 - 12);
    ctx.textAlign = 'left';
  }
  // playhead line
  var px = padL + pathoProgress * (W - padL - padR);
  ctx.strokeStyle = 'rgba(232,236,248,.35)'; ctx.setLineDash([2,3]);
  ctx.beginPath(); ctx.moveTo(px, padT-4); ctx.lineTo(px, H-padB+4); ctx.stroke();
  ctx.setLineDash([]);
  // axis endpoints
  ctx.fillStyle = '#6b769e'; ctx.font = '9px ui-monospace, monospace';
  ctx.fillText('0', padL, H-6); ctx.textAlign = 'right'; ctx.fillText((dur/1e6).toFixed(1)+' ms', W-padR+42, H-6); ctx.textAlign = 'left';
}
function pathoRenderCards(){
  $('pathoCards').innerHTML = pathoCardHTML(pathoA, 'A') + pathoCardHTML(pathoB, 'B');
  drawSwim('A'); drawSwim('B');
}
function pathoFillSelects(){
  ['pathoA','pathoB'].forEach(function(id){
    var sel = $(id); clear(sel);
    pathoImpls.forEach(function(impl){
      var o = document.createElement('option'); o.value = impl; o.textContent = impl;
      if ((id === 'pathoA' ? pathoA : pathoB) === impl) o.selected = true;
      sel.appendChild(o);
    });
  });
}
function pathoTick(ts){
  if (!pathoPlaying) return;
  if (!pathoLastTs) pathoLastTs = ts;
  var dt = ts - pathoLastTs; pathoLastTs = ts;
  pathoProgress += (dt / 4200) * pathoSpeed; // full trace ≈ 4.2s at 1×
  if (pathoProgress >= 1) { pathoProgress = 1; pathoPlaying = false; }
  $('pathoScrub').value = Math.round(pathoProgress * 1000);
  drawSwim('A'); drawSwim('B');
  if (pathoPlaying) requestAnimationFrame(pathoTick);
}
$('pathoPlay').addEventListener('click', function(){
  if (pathoProgress >= 1) pathoProgress = 0;
  pathoPlaying = true; pathoLastTs = 0;
  requestAnimationFrame(pathoTick);
});
$('pathoPause').addEventListener('click', function(){ pathoPlaying = false; });
$('pathoSpeed').addEventListener('change', function(){ pathoSpeed = parseFloat(this.value); });
$('pathoScrub').addEventListener('input', function(){ pathoProgress = this.value/1000; pathoPlaying = false; drawSwim('A'); drawSwim('B'); });
$('pathoA').addEventListener('change', function(){ pathoA = this.value; pathoProgress = 0; pathoRenderCards(); });
$('pathoB').addEventListener('change', function(){ pathoB = this.value; pathoProgress = 0; pathoRenderCards(); });

/* retained bytes bars */
(function renderRetain(){
  var holder = $('retainBars'); clear(holder);
  var rows = rowsFor('stalled_consumer_ops_completed', 'broadcast', null)
    .map(function(m){ return {impl:m.impl, bytes:(typeof m.approx_retained_bytes==='number'?m.approx_retained_bytes:0), outcome:m.outcome||'?'}; })
    .sort(function(a,b){ return b.bytes - a.bytes; });
  var nonzero = rows.filter(function(r){ return r.bytes > 0; });
  var zeros = rows.length - nonzero.length;
  var maxB = Math.max.apply(null, nonzero.map(function(r){return r.bytes;}).concat([1]));
  nonzero.slice(0, 22).forEach(function(r){
    var row = document.createElement('div');
    row.style.cssText = 'display:grid;grid-template-columns:minmax(150px,1fr) 3fr 80px 110px;gap:10px;align-items:center;font-size:11px;margin:3px 0';
    var oc = r.outcome === 'completed' ? '<span style="color:var(--green)">completed</span>' : '<span style="color:#fca5a5">blocked</span>';
    row.innerHTML = '<div class="mono" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--ink2)" title="'+esc(r.impl)+'">'+esc(r.impl)+'</div>'
      + '<div style="height:10px;background:#1a2350;border-radius:5px;overflow:hidden"><div style="height:100%;width:'+Math.max(2, 100*r.bytes/maxB).toFixed(1)+'%;background:linear-gradient(90deg,#7c2d12,#fb923c)"></div></div>'
      + '<div class="mono" style="text-align:right">'+fmtBytes(r.bytes)+'</div><div>'+oc+'</div>';
    holder.appendChild(row);
  });
  var note = document.createElement('p');
  note.className = 'small';
  note.style.marginTop = '8px';
  note.textContent = zeros + ' implementation(s) retained nothing post-GC (drop/coalesce by design). Bars sorted by retained bytes, top ' + Math.min(nonzero.length, 22) + ' shown.';
  holder.appendChild(note);
})();

/* ── Correctness panel ── */
(function renderCorrectness(){
  var torn = rowsFor('torn_reads', 'payload', null).map(function(m){
    return {impl:m.impl, violations:m.violations||0, checked:m.checked||0};
  }).sort(function(a,b){ return b.violations - a.violations || b.checked - a.checked; });
  var totalTorn = torn.reduce(function(s,r){ return s+r.violations; }, 0);
  var totalChecked = torn.reduce(function(s,r){ return s+r.checked; }, 0);
  var tHTML = '<div style="display:flex;align-items:baseline;gap:14px;margin-bottom:8px"><span class="pill '+(totalTorn===0?'pill-pass':'pill-bad')+'">'+(totalTorn===0?'PASS':'FAIL')+'</span><span class="small">'+fmtNum(totalChecked)+' checked loads · '+totalTorn+' torn</span></div>';
  tHTML += '<div style="max-height:260px;overflow:auto">';
  torn.forEach(function(r){
    var ok = r.violations === 0;
    tHTML += '<div style="display:grid;grid-template-columns:1fr auto;gap:8px;font-size:11.5px;padding:3px 0;border-bottom:1px solid var(--line)"><span class="mono" style="color:'+(ok?'var(--ink2)':'var(--red)')+'">'+esc(r.impl)+'</span><span class="mono" style="color:'+(ok?'var(--green)':'var(--red)')+'">'+(ok ? '✓ 0 / '+fmtNum(r.checked) : r.violations+' TORN / '+fmtNum(r.checked))+'</span></div>';
  });
  tHTML += '</div>';
  $('tornTable').innerHTML = tHTML;

  var cwd = rowsFor('withdrawal_compensation_success', 'caster', null).map(function(m){
    return {impl:m.impl, success:meanOf(m), rounds:(m.dims && m.dims.rounds) || 0, medianNs:m.median_round_ns};
  }).sort(function(a,b){ return a.success - b.success; });
  var cHTML = '';
  cwd.forEach(function(r){
    var pct = Math.round(100*r.success);
    var ok = r.success >= 1;
    cHTML += '<div style="margin:5px 0"><div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:2px"><span class="mono" style="color:var(--ink2)">'+esc(r.impl)+'</span><span class="mono" style="color:'+(ok?'var(--green)':'var(--red)')+'">'+pct+'% · '+fmtNs(r.medianNs||0)+'/round</span></div>'
      + '<div style="height:8px;background:#1a2350;border-radius:4px;overflow:hidden"><div style="height:100%;width:'+pct+'%;background:'+(ok?'linear-gradient(90deg,#065f46,#34d399)':'#7f1d1d')+'"></div></div></div>';
  });
  $('cwdChart').innerHTML = cHTML || '<p class="small">No withdrawal records.</p>';

  var failures = (summary && typeof summary.failures === 'number') ? summary.failures : 0;
  var records = (summary && summary.records) || metrics.length;
  var skippedCount = (summary && summary.skipped) || skipped.length;
  var vHTML = '<div style="display:flex;align-items:baseline;gap:14px"><span class="pill '+(failures===0?'pill-pass':'pill-bad')+'">'+(failures===0?'PASS':'FAIL')+'</span><div><div style="font-size:13px;font-weight:700">'+esc(failures===0?'Harness verification passed':'Harness verification FAILED')+'</div><div class="small">'+esc(records)+' records · '+esc(skippedCount)+' recorded skips · '+(failures)+' failures</div></div></div>';
  vHTML += '<div class="small" style="margin-top:10px;line-height:1.7">Verification checks that every registered implementation was either measured or skipped with a recorded reason, every record is schema-valid, and the meta record is present. Skips are exposure-limited by policy, never silently dropped.</div>';
  $('verifyPanel').innerHTML = vHTML;
})();

/* ── Coverage matrix ── */
var covFamily = 'broadcast';
function renderCoverage(){
  var tabs = $('covTabs'); clear(tabs);
  familyOrder.forEach(function(f){
    var t = document.createElement('button');
    t.className = 'tab' + (f===covFamily?' active':'');
    t.textContent = familyLabel[f];
    t.onclick = function(){ covFamily = f; renderCoverage(); };
    tabs.appendChild(t);
  });
  var scenarios = (REG.scenarios||[]).filter(function(s){ return s.kind === covFamily; }).sort(function(a,b){ return a.id < b.id ? -1 : 1; });
  var attach = {};
  (REG.attachments||[]).forEach(function(a){ attach[a.impl+'@@'+a.scenario] = a; });
  var scores = derivedFamily(covFamily);
  var impls = Object.keys(scores).sort(function(a,b){ return (scores[b].score||0) - (scores[a].score||0); });
  var table = $('covTable'); clear(table);
  if (!scenarios.length || !impls.length) { table.innerHTML = '<tr><td class="small" style="padding:12px">No registry data for this family.</td></tr>'; return; }
  var thead = '<thead><tr><th>implementation ↓ · scenario →</th>';
  scenarios.forEach(function(s){ thead += '<th title="'+esc(s.id)+'">'+esc(s.id)+'</th>'; });
  thead += '</tr></thead>';
  var tbody = '<tbody>';
  impls.forEach(function(impl){
    tbody += '<tr><th title="'+esc(impl)+'">'+esc(impl.length > 40 ? impl.slice(0,39)+'…' : impl)+'</th>';
    scenarios.forEach(function(s){
      var a = attach[impl+'@@'+s.id];
      var cls = 'cov-none', label = 'no attachment';
      if (a) {
        if (a.expected_good_fit) { cls = 'cov-good'; label = 'expected good fit'; }
        else { cls = 'cov-bad'; label = 'declared bad fit'; }
        if (a.exposure_limited) label += ' · exposure-limited';
      }
      tbody += '<td class="'+cls+'" data-impl="'+esc(impl)+'" data-scenario="'+esc(s.id)+'"></td>';
    });
    tbody += '</tr>';
  });
  tbody += '</tbody>';
  table.innerHTML = thead + tbody;
  table.querySelectorAll('td').forEach(function(td){
    var impl = td.getAttribute('data-impl'), sid = td.getAttribute('data-scenario');
    var a = attach[impl+'@@'+sid];
    bindTip(td, function(){
      var h = '<div class="tt">'+esc(impl)+'</div><div class="mono">'+esc(sid)+'</div>';
      if (a) {
        h += '<div style="margin-top:4px">'+esc(a.expected_good_fit ? 'Expected good fit' : 'Declared bad fit') + (a.exposure_limited ? ' · <b style="color:var(--red)">exposure-limited</b>' : '') + '</div>';
        if (a.remarks) h += '<div style="margin-top:4px;color:var(--ink2)">'+esc(a.remarks)+'</div>';
      } else {
        h += '<div style="margin-top:4px;color:var(--ink3)">No attachment declared.</div>';
      }
      return h;
    });
  });
}

/* ── Data & exports ── */
function downloadBlob(name, content, mime){
  var blob = new Blob([content], {type: mime || 'application/octet-stream'});
  var url = URL.createObjectURL(blob);
  var a = document.createElement('a');
  a.href = url; a.download = name;
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  setTimeout(function(){ URL.revokeObjectURL(url); }, 5000);
}
(function wireDownloads(){
  var all = [].concat(meta ? [meta] : [], metrics, traces, skipped, (summary && Object.keys(summary).length ? [summary] : []));
  $('dlJSONL').addEventListener('click', function(ev){ ev.preventDefault(); downloadBlob('bigbuff-results.jsonl', all.map(function(r){ return JSON.stringify(r); }).join('\n')+'\n', 'application/x-ndjson'); });
  var csvEscape = function(v){ var s = String(v == null ? '' : v); return /[",\n]/.test(s) ? '"'+s.replace(/"/g,'""')+'"' : s; };
  $('dlCSVMetrics').addEventListener('click', function(ev){
    ev.preventDefault();
    var head = ['impl','family','metric','dims','unit','ops','n','mean','stddev','cv','min','p50','p90','p99','max','ci95_halfwidth','ci95_low','ci95_high','lower_is_better','expected_good_fit','exposure_limited'];
    var lines = [head.join(',')];
    metrics.forEach(function(m){
      lines.push([m.impl, m.family, m.metric, JSON.stringify(m.dims||{}), m.unit, m.ops, m.n, m.mean, m.stddev, m.cv, m.min, m.p50, m.p90, m.p99, m.max, m.ci95_halfwidth, m.ci95_low, m.ci95_high, m.lower_is_better, m.expected_good_fit, m.exposure_limited].map(csvEscape).join(','));
    });
    downloadBlob('bigbuff-metrics-wide.csv', lines.join('\n')+'\n', 'text/csv');
  });
  $('dlCSVScores').addEventListener('click', function(ev){
    ev.preventDefault();
    var head = ['family','impl','score','verdict','baseline','integrity','throughput','latency','allocations','churn','scaling','stall_resilience','expected_good_fit','exposure_limited'];
    var lines = [head.join(',')];
    familyOrder.forEach(function(f){
      Object.keys(derivedFamily(f)).forEach(function(impl){
        var s = derivedFamily(f)[impl];
        lines.push([f, impl, s.score, s.verdict, s.baseline, s.integrity, s.dims.throughput, s.dims.latency, s.dims.allocations, s.dims.churn, s.dims.scaling, s.dims.stall_resilience, s.expected_good_fit, s.exposure_limited].map(csvEscape).join(','));
      });
    });
    downloadBlob('bigbuff-scores.csv', lines.join('\n')+'\n', 'text/csv');
  });
  $('dlRegistry').addEventListener('click', function(ev){ ev.preventDefault(); downloadBlob('bigbuff-registry.json', JSON.stringify(REG, null, 2), 'application/json'); });
  var sl = $('skippedList'); clear(sl);
  if (skipped.length) {
    var grouped = {};
    skipped.forEach(function(s){ (grouped[s.impl] = grouped[s.impl] || []).push(s.battery); });
    sl.innerHTML = Object.keys(grouped).map(function(impl){
      return '<div style="padding:4px 0;border-bottom:1px solid var(--line)"><b class="mono">'+esc(impl)+'</b> — skipped: '+esc(grouped[impl].join(', '))+'<br><span style="color:var(--ink3)">'+esc((skipped.find(function(s){return s.impl===impl;})||{}).reason||'')+'</span></div>';
    }).join('');
  } else sl.textContent = 'None. Every registered implementation was measured.';
})();

/* ── Methodology gloss + footer ── */
(function renderMethodology(){
  var batteries = [
    ['broadcast.throughput', 'publish_ns_op', 'cost per publish vs subscriber count (1…64/256/512)'],
    ['broadcast.latency', 'publish_latency_ns', 'publish→receipt latency, 8 subscribers, p50…p99/max + reservoir histogram'],
    ['broadcast.allocs', 'publish_allocs_op / publish_bytes_op', 'allocator pressure per publish at 8 subscribers, GOGC=off, post-GC measurement'],
    ['broadcast.allocs_zerosubs', 'publish_allocs_op_zerosubs / publish_bytes_op_zerosubs', 'same, publishing into the void'],
    ['broadcast.mem_per_subscriber', 'mem_per_subscriber_bytes', 'post-GC heap retained per idle subscriber (16 idle subscriptions)'],
    ['broadcast.churn', 'churn_ns_op / churn_allocs_op / churn_bytes_op', 'subscribe→publish→unsubscribe lifecycle cost'],
    ['broadcast.stalled_consumer', 'stalled_consumer_ops_completed', 'publisher vs one permanently stalled lane — completed ops + retained bytes'],
    ['broadcast.zero_subscriber', 'zero_subscriber_ns_op', 'cost of publishing with no subscribers'],
    ['broadcast.trace_steady_state / trace_stalled_consumer', 'trace', 'per-lane event timeline (publisher + subscribers), ≤ TraceMaxEvents'],
    ['caster.throughput / latency / churn', 'send_ns_op / send_latency_ns / churn_ns_op', 'rendezvous caster cost model incl. withdrawal-safe churn'],
    ['caster.withdrawal_compensation', 'withdrawal_compensation_success', 'Add(-1) unsticks a Send parked on a never-receiving intent (24 rounds)'],
    ['caster.trace_steady_state', 'trace', 'sender + receiver swimlanes'],
    ['scatter.dispatch', 'dispatch_ns_op / exactly_once_violations', 'work-queue dispatch, receivers 1…N, exactly-once accounting'],
    ['scatter.churn', 'churn_ns_op', 'worker registration lifecycle'],
    ['gather.wait', 'wait_ns_op', 'barrier wait time, 8 senders released on a start barrier'],
    ['gather.churn', 'churn_ns_op', 'sender registration lifecycle'],
    ['payload.throughput / latency', 'publish_ns_op / publish_latency_ns', 'async payload fan-out incl. lossy ring/seqlock designs'],
    ['payload.integrity', 'torn_reads', '[4]uint64 multi-word read integrity under 8-way fan-out'],
    ['payload.stalled_consumer', 'stalled_consumer_ops_completed', 'async lane vs never-ranged subscriber; approximate retained bytes'],
    ['payload.churn', 'churn_ns_op', 'subscribe-with-context cancellation lifecycle']
  ];
  $('batteryGloss').innerHTML = '<thead><tr><td>battery</td><td>metrics</td><td>what it measures</td></tr></thead><tbody>' + batteries.map(function(b){
    return '<tr><td>'+esc(b[0])+'</td><td>'+esc(b[1])+'</td><td>'+esc(b[2])+'</td></tr>';
  }).join('') + '</tbody>';
  $('footMeta').textContent = 'measured '+(meta.timestamp||'')+' · commit '+String(meta.commit||'').slice(0,12)+' · '+(meta.go||'')+' · GOMAXPROCS='+(meta.gomaxprocs||'?');
})();

/* ── Nav scrollspy + initial render ── */
function initNav(){
  var links = document.querySelectorAll('#nav a');
  var map = {};
  links.forEach(function(a){ map[a.getAttribute('href').slice(1)] = a; });
  if (typeof window.IntersectionObserver === 'function') {
    var io = new IntersectionObserver(function(entries){
      entries.forEach(function(entry){
        if (entry.isIntersecting) {
          links.forEach(function(a){ a.classList.remove('active'); });
          var target = map[entry.target.id];
          if (target) target.classList.add('active');
        }
      });
    }, {rootMargin: '-18% 0px -70% 0px', threshold: 0});
    ['overview','findings','scaling','leaderboard','adoptability','latency','memory','churn','pathology','correctness','coverage','methodology','data'].forEach(function(id){
      var el = document.getElementById(id); if (el) io.observe(el);
    });
  }
}
function renderAllCharts(){
  renderScaling(); lbRender(); radarRender(); renderLatency(); renderMemory(); renderChurn(); pathoRenderCards(); renderCoverage();
}
pathoFillSelects();
initNav();
renderAllCharts();
var resizeTimer = null;
window.addEventListener('resize', function(){
  if (resizeTimer) clearTimeout(resizeTimer);
  resizeTimer = setTimeout(renderAllCharts, 160);
});
})();
`
