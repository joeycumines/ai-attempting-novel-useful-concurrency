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
	registryPath := fs.String("registry", "", "registry JSON (default: the dump of the run group the results belong to, else the nearest earlier run's, else the live registry)")
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
	// A run is reported against the registry it was made from, for the same
	// reason it is verified against it: the fit, exposure and baseline flags
	// shown beside a measurement must be the ones that held when it was taken.
	if *registryPath == "" {
		if p, note := registryDumpForResults(*in); p != "" {
			*registryPath = p
			fmt.Fprintf(os.Stderr, "report: registry %s%s\n", filepath.Base(p), note)
		} else {
			fmt.Fprintf(os.Stderr, "report: no registry dump at or before this run; using the live registry\n")
		}
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
	// residue holds what a unit left running in its process (record contract
	// 1.3.0 on; see measure_isolate_test.go).
	residue []map[string]any
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
		case "residue":
			d.residue = append(d.residue, rec)
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
	Impl    string `json:"impl"`
	Family  string `json:"family"`
	Score   int    `json:"score"`
	Verdict string `json:"verdict"`
	// Ranked is false when the family has fewer than minRankedImpls measured
	// implementations: Score and Dims are then zero and carry no meaning.
	Ranked bool `json:"ranked"`
	// Class is the delivery class the implementation is scored in, for the
	// families that have them (see deliveryClasses), and empty otherwise.
	// Score, Dims and Peers are relative to the class, not the family.
	Class string `json:"class,omitempty"`
	// DimsUniform lists the dimensions on which every implementation of the
	// scoring group has the same value. Such a dimension distinguishes none
	// of them, so it is left out of the composite.
	DimsUniform []string `json:"dims_uniform"`
	// UnrankedReason says why, in words a reader of the report can act on.
	UnrankedReason string `json:"unranked_reason,omitempty"`
	Peers          int    `json:"peers"` // implementations measured in the family
	// DimsMissing lists the score dimensions this implementation has no
	// data for. They are left out of the composite, and their Dims entry is
	// zero.
	DimsMissing     []string       `json:"dims_missing"`
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
	// Unranked maps each family that gets no score to the reason.
	Unranked map[string]string `json:"unranked"`
	// UnrankedSince maps each family whose measurement is not valid in this
	// run to the first record contract under which it is.
	UnrankedSince map[string]string `json:"unranked_since"`
}

// minRankedImpls is the smallest family that gets a composite score. Every
// dimension of the score is a percentile rank within the family, so with n
// subjects it can only take n values: among two, the faster is 100 and the
// other 0 whatever the difference between them, and a 3 percent difference
// inside run-to-run noise moves a subject by 100/(n-1) points. Families
// below this size are reported with their measurements and confidence
// intervals only.
const minRankedImpls = 5

// casterDeliveryContract is the first record contract whose caster send
// figures measure delivery (see measureGeneratorVersion).
const casterDeliveryContract = "1.2.0"

// deliveryClassContract is the first record contract whose broadcast and
// payload throughput records say what was delivered.
const deliveryClassContract = "1.4.0"

// allocResolution is the step, in allocations per publish, to which the
// allocation dimension ranks from record contract allocResolutionContract.
//
// The battery counts the process's mallocs over the timed publishes, so the
// count includes whatever the runtime allocated meanwhile. For an
// implementation whose publish allocates nothing that stray count is the
// whole figure, and it differs in every repetition (a coefficient of
// variation of 224 percent is one repetition in five with a stray
// allocation). In runs 09 and 10, 52 of the 94 broadcast implementations had
// a repetition with no allocation at all in each run; their means ran up to
// 0.016 allocations per publish and single repetitions up to 0.04. Ranked as
// measured, they were put in an order that the next run reshuffled:
// RingPubSub measured 0.00006 and 0.00002 in the two runs and EpochCaster
// 0.00004 and 0, and their allocation scores moved from 76 to 92 and from 80
// to 98 on that alone.
//
// A tenth of an allocation per publish is about six times the largest stray
// mean seen. Implementations that round to the same figure tie. The price is
// that an implementation which really allocates once in 35 to 100 publishes
// (three did in both runs, at 0.01 to 0.03) ties with those that never
// allocate; the figure reported is still the one measured.
//
// A finer rule was considered and not taken: tie only the implementations
// with a repetition at zero. It tells those three apart, but it also moves
// implementations in and out of the tie between runs. Two had no repetition
// at zero in run 09 and two others in run 10 (smallest repetition 0.0002 to
// 0.001), and each of the four had one in the other run.
const (
	allocResolution         = 0.1
	allocResolutionContract = "1.5.0"
)

// allocRankValue is an allocations-per-publish figure as the allocation
// dimension ranks it: rounded to allocResolution.
func allocRankValue(v float64) float64 {
	return math.Round(v/allocResolution) * allocResolution
}

// The two delivery classes. An implementation is in classDeliversAll when,
// in every cell and repetition of its family's throughput battery, its
// subscribers received every value that was published to them, and in
// classDrops otherwise. The class is measured, not declared: it says what
// the implementation did with a publisher running as fast as it can against
// subscribers that do nothing but receive.
const (
	classDeliversAll = "delivers-all"
	classDrops       = "drops"
)

// deliveryClassLabel is the class as a reader sees it.
func deliveryClassLabel(class string) string {
	switch class {
	case classDeliversAll:
		return "delivers every value"
	case classDrops:
		return "drops or coalesces"
	}
	return ""
}

// hasDeliveryClasses reports whether a family is scored in delivery classes:
// the families whose implementations differ in whether a publish that
// returns has been, or will be, received.
func hasDeliveryClasses(family string) bool {
	return family == "broadcast" || family == "payload"
}

// deliveryClasses returns the class of every implementation of a family that
// has throughput records carrying the delivery fields, and the least
// delivered fraction over its cells.
func deliveryClasses(metrics []map[string]any, family string) (class map[string]string, least map[string]float64) {
	class, least = map[string]string{}, map[string]float64{}
	for _, m := range metrics {
		if m["family"] != family || m["metric"] != "publish_ns_op" {
			continue
		}
		impl, _ := m["impl"].(string)
		frac, ok := m["delivered_fraction"].(float64)
		if impl == "" || !ok {
			continue
		}
		if old, seen := least[impl]; !seen || frac < old {
			least[impl] = frac
		}
		pubs, _ := m["publishes"].(float64)
		recv, _ := m["receives"].(float64)
		all := recv == pubs*float64(dimsSubs(m))
		if !all {
			class[impl] = classDrops
		} else if class[impl] == "" {
			class[impl] = classDeliversAll
		}
	}
	return class, least
}

// familyRanking reports whether a family of `measured` implementations is
// scored in a run taken under the record contract `generator`, and if not,
// why. It is the one place that decision is made: the report, the score CSV,
// the evaluation and the landing page all read its result.
//
// since is the first record contract under which the family's measurement is
// valid, when the reason is that this run's is not; it is empty when the
// measurement is valid and the family is only too small to rank.
func familyRanking(family string, measured int, generator string) (ranked bool, reason, since string) {
	if family == "caster" && generatorBefore(generator, casterDeliveryContract) {
		return false, "the caster batteries of record contract " + generator + " timed Sends that found no receiver registered and delivered nothing, so its send figures do not measure delivery", isolationContract
	}
	if hasDeliveryClasses(family) && !generatorBefore(generator, isolationContract) && generatorBefore(generator, deliveryClassContract) {
		return false, "the " + family + " throughput records of record contract " + generator + " give the cost of calling Publish and do not say how much of what was published was received, so its score ranked implementations that discard nearly everything a fast publisher hands them above every implementation that delivers all of it", deliveryClassContract
	}
	if generatorBefore(generator, isolationContract) {
		return false, "a run under record contract " + generator + " was one process, in which the publishers left spinning by the stalled-consumer probes of its eleventh broadcast implementation occupied the CPUs for the rest of the run: the ten implementations measured before them had the machine to themselves, and everything measured after them did not", isolationContract
	}
	if measured < minRankedImpls {
		return false, fmt.Sprintf("%d implementations measured, fewer than the %d a percentile score needs", measured, minRankedImpls), ""
	}
	return true, "", ""
}

func isBaselineName(name string) bool {
	return strings.Contains(name, "Baseline") || strings.Contains(name, "(baseline)")
}

// percentileRank returns the percentile rank of v among values, 100 being
// best: the share of the other values that are worse than v, with each value
// tied with v counted as half. (It used to count a tie as nothing, so on a
// dimension where half the field shares the best value, as half the
// broadcast family does on stall resilience, the best attainable was 51.)
// lowerIsBetter controls which direction is "worse".
func percentileRank(values []float64, v float64, lowerIsBetter bool) int {
	n := len(values)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return 100
	}
	worse, same := 0, 0
	for _, o := range values {
		switch {
		case o == v:
			same++
		case lowerIsBetter == (o > v):
			worse++
		}
	}
	if same > 0 {
		same-- // v itself
	}
	return int(math.Round(100 * (float64(worse) + 0.5*float64(same)) / float64(n-1)))
}

// indistinguishable decides whether the headline cost separates the members
// of a scoring group. It takes the 95 percent interval of each member's
// headline record and counts the pairs of members whose intervals do not
// overlap. When fewer than half of the pairs can be told apart, a percentile
// rank would mostly be ordering noise: two runs of the caster family, whose
// thirteen members send in 680 to 1,130 ns with intervals of 30 to 240 ns
// (about one pair in five apart), put different implementations first and
// moved the same implementation from 2nd to 7th.
//
// apart is the number of pairs told apart, of pairs. ok is false when the
// rule cannot be applied (fewer than two members carry an interval).
func indistinguishable(metrics []map[string]any, metric, family string, dims int, impls []string) (same bool, apart, pairs int, ok bool) {
	member := map[string]bool{}
	for _, impl := range impls {
		member[impl] = true
	}
	type interval struct{ lo, hi float64 }
	var ivs []interval
	for _, m := range metrics {
		if m["metric"] != metric || m["family"] != family || (dims >= 0 && dimsSubs(m) != dims) {
			continue
		}
		impl, _ := m["impl"].(string)
		lo, okLo := m["ci95_low"].(float64)
		hi, okHi := m["ci95_high"].(float64)
		if member[impl] && okLo && okHi {
			ivs = append(ivs, interval{lo, hi})
		}
	}
	if len(ivs) < 2 {
		return false, 0, 0, false
	}
	for i := range ivs {
		for j := i + 1; j < len(ivs); j++ {
			pairs++
			if ivs[i].hi < ivs[j].lo || ivs[j].hi < ivs[i].lo {
				apart++
			}
		}
	}
	return 2*apart < pairs, apart, pairs, true
}

// uniformValues reports whether a dimension has the same value for every
// member of a scoring group of more than one.
func uniformValues(values []float64) bool {
	if len(values) < 2 {
		return false
	}
	for _, v := range values[1:] {
		if v != values[0] {
			return false
		}
	}
	return true
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

// latencyRankValues is what the latency dimension ranks, by implementation.
//
// From record contract 1.6.0 a latency record holds the 90th percentile of
// each of its episodes, and the value is their mean: the time within which
// nine publishes in ten returned, averaged over fresh instances. A record
// without episodes is from an earlier contract, whose battery was a single
// episode; its value is the mean of that episode's samples, which is what
// those runs were scored on.
//
// Why the 90th percentile and not the 99th: with 400 samples an episode's
// 99th percentile is its fourth largest sample, and on a shared two-CPU
// machine that sample is a preemption more often than it is the
// implementation. Between repeated passes of this battery alone (31
// episodes, three passes) the order of a class by the 99th percentile
// agreed at 0.62 to 0.96, by the 90th at 0.80 to 0.96 and by the median at
// 0.87 to 1.00. The median says least that the throughput dimension does not
// already say, so the dimension takes the highest percentile that repeats.
func latencyRankValues(metrics []map[string]any, metric, family string) map[string]float64 {
	out := map[string]float64{}
	for _, m := range metrics {
		if m["metric"] != metric || m["family"] != family {
			continue
		}
		impl, _ := m["impl"].(string)
		if v, ok := episodeMean(m, "episode_p90"); ok {
			out[impl] = v
			continue
		}
		if mean, ok := recMean(m); ok {
			out[impl] = mean
		}
	}
	return out
}

// episodeMean is the mean of a per-episode list of a latency record.
func episodeMean(m map[string]any, key string) (float64, bool) {
	list, ok := m[key].([]any)
	if !ok || len(list) == 0 {
		return 0, false
	}
	sum := 0.0
	for _, v := range list {
		f, ok := v.(float64)
		if !ok {
			return 0, false
		}
		sum += f
	}
	return sum / float64(len(list)), true
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

// stalledBusyCores returns, for each implementation of a family whose
// publisher was blocked by the stalled-consumer probe, how many CPUs the
// process used while it was blocked (blocked_cpu_cores, record contract 1.5.0
// on). Earlier runs did not measure it and return nothing, so their scores
// are what their own reports say. (Runs under 1.3.0 and 1.4.0 hold a related
// figure in their residue records, taken after the probe returned; the
// evaluation lists it, and it is not used to re-score them.)
func stalledBusyCores(data *reportData, family string) map[string]float64 {
	out := map[string]float64{}
	for _, m := range data.metrics {
		if m["metric"] != "stalled_consumer_ops_completed" || m["family"] != family || m["outcome"] != "publisher_blocked" {
			continue
		}
		impl, _ := m["impl"].(string)
		if v, ok := m["blocked_cpu_cores"].(float64); ok {
			out[impl] = v
		}
	}
	return out
}

// stallRank is the value the stall-resilience dimension ranks, higher being
// better: the logarithm of the publishes completed by the deadline, except
// that a publisher which was blocked and waited by spinning ranks below every
// publisher that parked or completed. Completing few publishes behind a
// subscriber that never reads is what a lossless design is specified to do;
// holding a CPU while waiting is a cost the others do not have, and before
// this rule the dimension could not see it.
func stallRank(ops, cores float64, measured bool) float64 {
	if measured && cores >= stallBusyCores {
		return -1
	}
	return math.Log10(ops + 1)
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
		// A caster Send made with every receiver registered that reported
		// fewer deliveries than receivers lost a value.
		if v, ok := m["deliveries_missing"].(float64); ok {
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
		Scores:        map[string]map[string]implScore{},
		Coverage:      map[string]map[string]any{},
		FamilyOf:      map[string]string{},
		Unranked:      map[string]string{},
		UnrankedSince: map[string]string{},
	}
	generator, _ := data.meta["generator_version"].(string)
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
		thrMetric := map[string]string{ // metric name differs per family
			"broadcast": "publish_ns_op", "payload": "publish_ns_op", "caster": "send_ns_op", "fanin": "merge_ns_op",
			"exchange": "exchange_ns_op", "barrier": "send_ns_op",
		}[family]
		// The scoring groups of the family: one, or one per delivery class.
		type scoreGroup struct {
			class string
			impls []string
		}
		groups := []scoreGroup{{"", impls}}
		classOf, leastDelivered := map[string]string{}, map[string]float64{}
		if hasDeliveryClasses(family) && !generatorBefore(generator, deliveryClassContract) {
			classOf, leastDelivered = deliveryClasses(data.metrics, family)
			byClass := map[string][]string{}
			for _, impl := range impls {
				byClass[classOf[impl]] = append(byClass[classOf[impl]], impl)
			}
			groups = groups[:0]
			for _, c := range []string{classDeliversAll, classDrops, ""} {
				if len(byClass[c]) > 0 {
					groups = append(groups, scoreGroup{c, byClass[c]})
				}
			}
		}
		delivered8 := map[string]float64{}
		for _, m := range data.metrics {
			if m["family"] == family && m["metric"] == "publish_ns_op" && dimsSubs(m) == thrDims {
				if f, ok := m["delivered_fraction"].(float64); ok {
					delivered8[m["impl"].(string)] = f
				}
			}
		}
		thrAll := metricMeans(data.metrics, thrMetric, family, thrDims)
		latAll := latencyRankValues(data.metrics, map[string]string{
			"broadcast": "publish_latency_ns", "payload": "publish_latency_ns", "caster": "send_latency_ns",
		}[family], family)
		var allocAll map[string]float64
		allocRaw := map[string]float64{}
		if family == "broadcast" {
			allocAll = metricMeans(data.metrics, "publish_allocs_op", "broadcast", 8)
			for impl, v := range allocAll {
				allocRaw[impl] = v
			}
			if !generatorBefore(generator, allocResolutionContract) {
				for impl, v := range allocAll {
					allocAll[impl] = allocRankValue(v)
				}
			}
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
		stallCores := stalledBusyCores(data, family)
		stallRankOf := func(impl string) float64 {
			cores, measured := stallCores[impl]
			return stallRank(stallAll[impl], cores, measured)
		}
		dd.Scores[family] = map[string]implScore{}
		anyRanked, firstReason, firstSince := false, "", ""
		for gi, grp := range groups {
			impls := grp.impls
			ranked, unrankedReason, validSince := familyRanking(family, len(impls), generator)
			if grp.class == "" && len(groups) > 1 {
				ranked, unrankedReason, validSince = false, "the throughput battery recorded no delivery figures for it (every cell was skipped), so it has no delivery class", ""
			} else if !ranked && validSince == "" && grp.class != "" {
				unrankedReason = fmt.Sprintf("%d implementations measured in the delivery class \"%s\", fewer than the %d a percentile score needs", len(impls), deliveryClassLabel(grp.class), minRankedImpls)
			}
			indistinct := false
			if ranked && thrMetric != "" {
				if same, apart, pairs, ok := indistinguishable(data.metrics, thrMetric, family, thrDims, impls); ok && same {
					ranked, validSince, indistinct = false, "", true
					unrankedReason = fmt.Sprintf("the cost of one %s tells apart only %d of the %d pairs of its %d implementations (the 95 percent intervals of the other pairs overlap), which is fewer than half, so a percentile rank would mostly order noise", strings.TrimSuffix(thrMetric, "_ns_op"), apart, pairs, len(impls))
				}
			}
			anyRanked = anyRanked || ranked
			if gi == 0 {
				firstReason, firstSince = unrankedReason, validSince
			}
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
				if _, ok := stallAll[impl]; ok {
					stallVals = append(stallVals, stallRankOf(impl))
				}
				lo, okLo := ns1[impl]
				hi, okHi := ns64[impl]
				if okLo && okHi && lo > 0 {
					scaleOf[impl] = hi / lo
					scaleVals = append(scaleVals, hi/lo)
				}
			}

			for _, impl := range impls {
				thr, hasThr := thrAll[impl]
				lat, hasLat := latAll[impl]
				alloc, hasAlloc := float64(0), false
				if allocAll != nil {
					alloc, hasAlloc = allocAll[impl]
				}
				churn, hasChurn := churnAll[impl]
				stall, hasStall := stallAll[impl]
				scale, hasScale := scaleOf[impl]

				// A dimension the implementation has no data for scores 0 here
				// and is left out of the composite below. (It used to score 100,
				// which handed every implementation full marks on any dimension
				// its family has no battery for.)
				thrScore, latScore, allocScore, churnScore, stallScore, scaleScore := 0, 0, 0, 0, 0, 0
				if hasThr {
					thrScore = percentileRank(thrVals, thr, true)
				}
				if hasLat {
					latScore = percentileRank(latVals, lat, true)
				}
				hasAlloc = allocAll != nil && hasAlloc
				if hasAlloc {
					allocScore = percentileRank(allocVals, alloc, true)
				}
				if hasChurn {
					churnScore = percentileRank(churnVals, churn, true)
				}
				if hasStall {
					stallScore = percentileRank(stallVals, stallRankOf(impl), false)
				}
				if hasScale {
					scaleScore = percentileRank(scaleVals, scale, true)
				}

				// Weights documented in measurement.md. The composite is the
				// weighted mean over the dimensions that have data.
				raw, weight := 0.0, 0.0
				dimsMissing, dimsUniform := []string{}, []string{}
				for _, d := range []struct {
					key   string
					w     float64
					score int
					has   bool
					vals  []float64
				}{
					{"throughput", 0.28, thrScore, hasThr, thrVals},
					{"latency", 0.14, latScore, hasLat, latVals},
					{"allocations", 0.14, allocScore, hasAlloc, allocVals},
					{"churn", 0.08, churnScore, hasChurn, churnVals},
					{"scaling", 0.16, scaleScore, hasScale, scaleVals},
					{"stall_resilience", 0.20, stallScore, hasStall, stallVals},
				} {
					if !d.has {
						dimsMissing = append(dimsMissing, d.key)
						continue
					}
					if uniformValues(d.vals) {
						dimsUniform = append(dimsUniform, d.key)
						continue
					}
					raw += d.w * float64(d.score)
					weight += d.w
				}
				if weight > 0 {
					raw /= weight
				}

				integrity, _ := integrityOf(data.metrics, impl, family)
				if integrity == "failed" && raw > 50 {
					raw = 50
				}
				score := int(math.Round(raw))

				verdict := "Avoid for most uses"
				switch {
				case integrity == "failed":
					verdict = "Do not adopt (integrity failure)"
				case !ranked:
					verdict = fmt.Sprintf("Not ranked (family of %d)", len(impls))
					if indistinct {
						verdict = "Not ranked (not distinguishable)"
					} else if grp.class != "" {
						verdict = fmt.Sprintf("Not ranked (class of %d)", len(impls))
					} else if len(groups) > 1 {
						verdict = "Not ranked (no delivery figures)"
					}
					if validSince != "" {
						verdict = "Not ranked (measurement not valid)"
					}
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
					switch family {
					case "fanin":
						thrKey = "merge_ns_op@4"
					case "exchange", "barrier":
						thrKey = thrMetric + "@8"
					}
					metrics[thrKey] = thr
				}
				if hasLat {
					metrics["publish_latency_ns@8"] = lat
				}
				if hasAlloc {
					// The figure as measured; the rank above used it to
					// allocResolution.
					if raw, ok := allocRaw[impl]; ok {
						alloc = raw
					}
					metrics["publish_allocs_op@8"] = alloc
				}
				if hasChurn {
					metrics["churn_ns_op"] = churn
				}
				if hasStall {
					metrics["stalled_ops_completed"] = stall
					if cores, ok := stallCores[impl]; ok {
						metrics["stalled_blocked_cpu_cores"] = cores
						metrics["stalled_publisher_spins"] = cores >= stallBusyCores
					}
				}
				if hasScale {
					metrics["scaling_ratio_64_over_1"] = scale
				}
				if f, ok := delivered8[impl]; ok {
					metrics["delivered_fraction@8"] = f
				}
				if f, ok := leastDelivered[impl]; ok {
					metrics["delivered_fraction_least"] = f
				}

				if !ranked {
					score, thrScore, latScore, allocScore, churnScore, scaleScore, stallScore = 0, 0, 0, 0, 0, 0, 0
				}
				dd.Scores[family][impl] = implScore{
					Impl:            impl,
					Family:          family,
					Score:           score,
					Verdict:         verdict,
					Ranked:          ranked,
					Class:           grp.class,
					UnrankedReason:  unrankedReason,
					Peers:           len(impls),
					DimsMissing:     dimsMissing,
					DimsUniform:     dimsUniform,
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
		// A family is unranked as a whole when none of its groups is.
		if !anyRanked {
			dd.Unranked[family] = firstReason
			if firstSince != "" {
				dd.UnrankedSince[family] = firstSince
			}
		}
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
		"exchange": exchangeNames(), "barrier": barrierNames(),
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
