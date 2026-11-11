package bigbuff

// Measurement harness entry points (see measurement.md for the methodology,
// schema and runbook).
//
// This file wires a subcommand-dispatched measurement CLI into the test
// binary via TestMain. Regular `go test`, `go test -bench=.` and fuzzing are
// completely unaffected: the harness only activates when the sentinel
// argument "bigbuff-measure" is present in os.Args, e.g.:
//
//	go test -run=NONE -args bigbuff-measure list
//	go test -run=NONE -args bigbuff-measure run --profile quick
//	go test -run=NONE -args bigbuff-measure report
//
// The sentinel is positional: everything after it is the subcommand argv.

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const measureSentinel = "bigbuff-measure"
const measureSchemaVersion = 1

// TestMain preserves the standard test binary behaviour unless the
// bigbuff-measure sentinel is present in os.Args.
func TestMain(m *testing.M) {
	for i, a := range os.Args {
		if a == measureSentinel {
			os.Exit(runMeasureCommand(os.Args[i+1:]))
		}
	}
	os.Exit(m.Run())
}

func measureUsage(w io.Writer) {
	fmt.Fprint(w, `bigbuff-measure — measurement battery for the bigbuff experiments

Usage: go test -run=NONE -args bigbuff-measure <command> [flags]

Commands:
  list           List implementations, batteries and profiles.
  dump-registry  Write the scenario/case/attachment registry as JSON.
  run            Run the measurement battery; write results (JSONL).
  verify         Verify a results file covers every registered implementation.
  report         Generate the interactive HTML report from results.
  site           Build landing page (index.html) and wire backlinks across all reports.
  index          Write per-run manifests (results-NN-00-manifest.json) for the flat tree.
  gates          Record what 'make verify VERIFY_LOG=<dir>' reported (results-NN-06-gates.json).
  evaluate       Generate the run's evaluation (results-NN-07-evaluation.md) from its own files.
  extract        Extract the source files embedded in a model response (NN-*.md).
  help           Show this help.

Common flags:
  Flat layout (see measurement.md): results-NN-SS-slug.ext, zero-padded;
  run 01 = standard profile, run 02 = quick profile; slot 00 = manifest.
  run:    --out results-NN-01-results.jsonl  --profile quick|standard|full
          --impl <glob>   --battery <glob>   --no-verify
  report: --in  results-NN-01-results.jsonl  --out results-NN-03-report.html
          --registry <file>   --csv-metrics <file>   --csv-scores <file>
  verify: --in  results-NN-01-results.jsonl  (default: newest results-*-01-results.jsonl)
  index:  --dir .                            (rewrites every results-NN-00-manifest.json)
  gates:  --log <dir>  --out results-NN-06-gates.json
  evaluate: --in results-NN-01-results.jsonl  (registry, gates and output default to the run group)
  extract: --in NN-response.md  --out <scratch dir>   [--list]

Everything after the bigbuff-measure sentinel is the subcommand argv; all
other arguments are interpreted as usual by the test binary.
`)
}

func runMeasureCommand(args []string) int {
	if len(args) == 0 {
		measureUsage(os.Stderr)
		return 2
	}
	switch args[0] {
	case "list":
		return cmdMeasureList(args[1:])
	case "dump-registry":
		return cmdDumpRegistry(args[1:])
	case "run":
		return cmdMeasureRun(args[1:])
	case "verify":
		return cmdMeasureVerify(args[1:])
	case "report":
		return cmdMeasureReport(args[1:])
	case "site", "wire":
		return cmdMeasureSite(args[1:])
	case "index":
		return cmdMeasureIndex(args[1:])
	case "gates":
		return cmdMeasureGates(args[1:])
	case "evaluate":
		return cmdMeasureEvaluate(args[1:])
	case "extract":
		return cmdMeasureExtract(args[1:])
	case "help", "-h", "--help":
		measureUsage(os.Stdout)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "bigbuff-measure: unknown command %q\n\n", args[0])
		measureUsage(os.Stderr)
		return 2
	}
}

// --- JSONL writer ---

type measureWriter struct {
	f       *os.File
	enc     *json.Encoder
	records int
}

func newMeasureWriter(outPath string) (*measureWriter, error) {
	if dir := filepath.Dir(outPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.Create(outPath)
	if err != nil {
		return nil, err
	}
	return &measureWriter{f: f, enc: json.NewEncoder(f)}, nil
}

func (w *measureWriter) write(rec map[string]any) error {
	if err := w.enc.Encode(rec); err != nil {
		return err
	}
	w.records++
	return nil
}

func (w *measureWriter) close() error { return w.f.Close() }

// --- Environment metadata ---

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

func gitCommit() string {
	out, err := exec.Command("git", "rev-parse", "--short=12", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func cpuModel() string {
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "model name") {
				if parts := strings.SplitN(line, ":", 2); len(parts) == 2 {
					return strings.TrimSpace(parts[1])
				}
			}
		}
	}
	return runtime.GOARCH
}

// measureGeneratorVersion identifies the record contract of a results file.
//
//	1.0.0  runs 01 to 04. The stalled-consumer records carry no
//	       ops_completed value, although the metric is named for it, so the
//	       stall-resilience dimension of every score in those runs is a
//	       constant (see results-00-00-readme.md).
//	1.1.0  ops_completed recorded; exchange and barrier batteries; verify
//	       rejects a metric record that carries no value.
const measureGeneratorVersion = "1.1.0"

func measureMetaRecords(profile string, seed int64) map[string]any {
	return map[string]any{
		"type":              "meta",
		"schema":            measureSchemaVersion,
		"generator":         "bigbuff-measure",
		"generator_version": measureGeneratorVersion,
		"timestamp":         time.Now().UTC().Format(time.RFC3339Nano),
		"go":                runtime.Version(),
		"goos":              runtime.GOOS,
		"goarch":            runtime.GOARCH,
		"num_cpu":           runtime.NumCPU(),
		"gomaxprocs":        runtime.GOMAXPROCS(0),
		"cpu":               cpuModel(),
		"hostname":          hostname(),
		"commit":            gitCommit(),
		"profile":           profile,
		"seed":              seed,
	}
}

// --- Profiles ---

type measureProfile struct {
	Name               string
	Reps               int
	TargetPerRep       time.Duration // calibration target for throughput/latency cells
	MinOps             int
	MaxOps             int
	ScaleSubs          []int
	CasterScaleSubs    []int
	LatencySamples     int
	AllocReps          int
	ChurnOps           int
	ChurnReps          int
	MemProbeSubs       int
	StalledOps         int
	StalledDeadline    time.Duration
	TraceOps           int
	TraceMaxEvents     int
	IntegrityPublishes int
	ScatterScaleSubs   []int
}

func profileByName(name string) (measureProfile, bool) {
	switch name {
	case "quick":
		return measureProfile{Name: "quick", Reps: 3, TargetPerRep: 25 * time.Millisecond,
			MinOps: 32, MaxOps: 200000,
			ScaleSubs: []int{1, 8, 64}, CasterScaleSubs: []int{1, 8, 32},
			LatencySamples: 160, AllocReps: 3, ChurnOps: 100, ChurnReps: 2,
			MemProbeSubs: 8, StalledOps: 100, StalledDeadline: 150 * time.Millisecond,
			TraceOps: 40, TraceMaxEvents: 1200, IntegrityPublishes: 3000,
			ScatterScaleSubs: []int{1, 8}}, true
	case "standard":
		return measureProfile{Name: "standard", Reps: 5, TargetPerRep: 60 * time.Millisecond,
			MinOps: 64, MaxOps: 400000,
			ScaleSubs: []int{1, 4, 8, 16, 32, 64, 128, 256}, CasterScaleSubs: []int{1, 4, 8, 16, 32},
			LatencySamples: 400, AllocReps: 5, ChurnOps: 200, ChurnReps: 3,
			MemProbeSubs: 16, StalledOps: 1000, StalledDeadline: 250 * time.Millisecond,
			TraceOps: 80, TraceMaxEvents: 2400, IntegrityPublishes: 8000,
			ScatterScaleSubs: []int{1, 4, 8, 16}}, true
	case "full":
		return measureProfile{Name: "full", Reps: 7, TargetPerRep: 120 * time.Millisecond,
			MinOps: 64, MaxOps: 800000,
			ScaleSubs: []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512}, CasterScaleSubs: []int{1, 2, 4, 8, 16, 32, 64},
			LatencySamples: 1000, AllocReps: 7, ChurnOps: 400, ChurnReps: 5,
			MemProbeSubs: 32, StalledOps: 2000, StalledDeadline: 600 * time.Millisecond,
			TraceOps: 120, TraceMaxEvents: 4000, IntegrityPublishes: 20000,
			ScatterScaleSubs: []int{1, 2, 4, 8, 16, 32}}, true
	default:
		return measureProfile{}, false
	}
}

// --- Measurement context & battery registry ---

type measureContext struct {
	profile     measureProfile
	implGlob    string
	batteryGlob string
	out         *measureWriter
	seed        int64
	started     time.Time
	skipped     int
	failures    []string
}

func (c *measureContext) implAllowed(name string) bool {
	if c.implGlob == "" {
		return true
	}
	ok, err := path.Match(c.implGlob, name)
	if err != nil {
		return strings.Contains(name, c.implGlob)
	}
	return ok
}

func (c *measureContext) batteryAllowed(id string) bool {
	if c.batteryGlob == "" {
		return true
	}
	ok, err := path.Match(c.batteryGlob, id)
	if err != nil {
		return strings.Contains(id, c.batteryGlob)
	}
	return ok
}

func (c *measureContext) write(rec map[string]any) {
	if err := c.out.write(rec); err != nil {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: write record: %v\n", err)
		os.Exit(1)
	}
}

func (c *measureContext) skip(family, impl, battery, reason string) {
	c.skipped++
	c.write(map[string]any{
		"type":    "skipped",
		"family":  family,
		"impl":    impl,
		"battery": battery,
		"reason":  reason,
	})
}

func (c *measureContext) fail(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	c.failures = append(c.failures, msg)
	fmt.Fprintf(os.Stderr, "bigbuff-measure: FAIL: %s\n", msg)
}

func finite(v float64) float64 {
	if v != v || v > 1e300 || v < -1e300 { // NaN or absurd
		return 0
	}
	return v
}

func attachmentsOfImpl(list []Attachment, impl string) (scenarios []string, goodFit, exposureLimited bool) {
	for _, a := range list {
		if a.Impl != impl {
			continue
		}
		scenarios = append(scenarios, string(a.Scenario))
		if a.ExpectedGoodFit {
			goodFit = true
		}
		if a.ExposureLimited {
			exposureLimited = true
		}
	}
	return scenarios, goodFit, exposureLimited
}

func broadcastAttachmentMeta(impl string) ([]string, bool, bool) {
	return attachmentsOfImpl(broadcastAttachments, impl)
}

func casterAttachmentMeta(impl string) ([]string, bool, bool) {
	return attachmentsOfImpl(casterAttachments, impl)
}

func scatterAttachmentMeta(impl string) ([]string, bool, bool) {
	return attachmentsOfImpl(scatterAttachments, impl)
}

func gatherAttachmentMeta(impl string) ([]string, bool, bool) {
	return attachmentsOfImpl(gatherAttachments, impl)
}

func payloadAttachmentMeta(impl string) ([]string, bool, bool) {
	return attachmentsOfImpl(payloadAttachments, impl)
}

func faninAttachmentMeta(impl string) ([]string, bool, bool) {
	return attachmentsOfImpl(faninAttachments, impl)
}

// writeMetric emits a metric record. summary is computed from raw rep samples
// (kept in the record for full re-analysis) unless raw is nil.
func (c *measureContext) writeMetric(m map[string]any, samples []float64) {
	m["type"] = "metric"
	m["schema"] = measureSchemaVersion
	if samples != nil {
		s := summarize(samples)
		m["samples"] = samples
		m["n"] = s.N
		m["mean"] = s.Mean
		m["stddev"] = s.StdDev
		m["cv"] = s.CV
		m["min"] = s.Min
		m["p50"] = s.P50
		m["p90"] = s.P90
		m["p99"] = s.P99
		m["max"] = s.Max
		m["ci95_halfwidth"] = s.CI95HalfWidth
		m["ci95_low"] = s.CI95Low
		m["ci95_high"] = s.CI95High
	}
	for k, v := range m {
		if f, ok := v.(float64); ok {
			m[k] = finite(f)
		}
	}
	c.write(m)
}

// --- Expected implementation sets (for coverage verification) ---

type expectedImplSet struct {
	family string
	names  []string
}

func expectedImplSets() []expectedImplSet {
	sets := []expectedImplSet{}
	bc := []string{}
	for _, f := range AllBroadcasters() {
		p := f()
		bc = append(bc, p.Name())
		p.Close()
	}
	sets = append(sets, expectedImplSet{"broadcast", bc})
	casterNames := []string{}
	for name := range casterImpls() {
		casterNames = append(casterNames, name)
	}
	casterNames = append(casterNames, "BaselineRendezvousCaster")
	sets = append(sets, expectedImplSet{"caster", casterNames})
	scatterNames := []string{}
	for name := range scatterLikes() {
		scatterNames = append(scatterNames, name)
	}
	sets = append(sets, expectedImplSet{"scatter", scatterNames})
	gatherNames := []string{}
	for name := range gatherLikes() {
		gatherNames = append(gatherNames, name)
	}
	sets = append(sets, expectedImplSet{"gather", gatherNames})
	payloadNames := []string{}
	for name := range asyncImpls() {
		payloadNames = append(payloadNames, name)
	}
	sets = append(sets, expectedImplSet{"payload", payloadNames})
	faninNames := []string{}
	for name := range faninLikes() {
		faninNames = append(faninNames, name)
	}
	sets = append(sets, expectedImplSet{"fanin", faninNames})
	sets = append(sets, expectedImplSet{"exchange", exchangeNames()})
	sets = append(sets, expectedImplSet{"barrier", barrierNames()})
	return sets
}
