package bigbuff

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestGateLogParsing(t *testing.T) {
	leg := func(name, log string) gateLeg {
		t.Helper()
		l, _ := parseGateLog(name, log)
		return l
	}
	if l := leg("vet", "== go vet ./...\n== go vet -race ./...\nverify.vet: ok\n"); l.Status != "pass" || len(l.Commands) != 2 {
		t.Errorf("vet: %+v", l)
	}
	if l := leg("vet", "== go vet ./...\n./x.go:1:1: bad\n"); l.Status != "fail" {
		t.Errorf("vet without its ok line must fail: %+v", l)
	}
	if l := leg("test", "== go test -count=1 -cpu 1,2,4 ./...\nok  \tbigbuff\t139.201s\n"); l.Status != "pass" || l.Detail != "139.201 s" {
		t.Errorf("test: %+v", l)
	}
	if l := leg("test", "--- FAIL: TestX (0.00s)\nFAIL\nFAIL\tbigbuff\t1.0s\n"); l.Status != "fail" {
		t.Errorf("failing test log: %+v", l)
	}
	// Silence is not a pass.
	if l := leg("race", ""); l.Status != "fail" {
		t.Errorf("empty log: %+v", l)
	}
	if l := leg("alloc", "--- PASS: TestAllocA (0.00s)\n--- PASS: TestAllocB (0.00s)\n--- SKIP: TestAllocC (0.00s)\nok  \tbigbuff\t2.5s\n"); l.Status != "pass" || !strings.HasPrefix(l.Detail, "2 passing, 0 failing, 1 skipped") {
		t.Errorf("alloc: %+v", l)
	}
	if l := leg("arm64", "verify.arm64: FAILED (the arm64 assembly was not executed)\n"); l.Status != "fail" {
		t.Errorf("arm64: %+v", l)
	}

	const fuzzLog = `== FuzzA (10s)
fuzz: elapsed: 0s, gathering baseline coverage: 0/5 completed
fuzz: elapsed: 3s, execs: 4000 (1333/sec), new interesting: 1 (total: 6)
fuzz: elapsed: 10s, execs: 58123 (5800/sec), new interesting: 2 (total: 7)
PASS
ok  	bigbuff	10.1s
== FuzzB (10s)
fuzz: elapsed: 2s, execs: 77 (38/sec), new interesting: 0 (total: 1)
--- FAIL: FuzzB (2.00s)
FAIL
exit status 1
`
	l, targets := parseGateLog("fuzz", fuzzLog)
	if l.Status != "fail" || l.Detail != "1 of 2 targets passed" {
		t.Errorf("fuzz leg: %+v", l)
	}
	if len(targets) != 2 || targets[0] != (gateFuzzTarget{"FuzzA", "pass", "10s", 58123}) || targets[1] != (gateFuzzTarget{"FuzzB", "fail", "10s", 77}) {
		t.Errorf("fuzz targets: %+v", targets)
	}
	ok := strings.Replace(fuzzLog, "--- FAIL: FuzzB (2.00s)\nFAIL\nexit status 1\n", "PASS\nok  \tbigbuff\t10.0s\nverify.fuzz: ok (2 targets)\n", 1)
	if l, _ := parseGateLog("fuzz", ok); l.Status != "pass" || len(l.Commands) != 0 {
		t.Errorf("passing fuzz log: %+v", l)
	}
	// verify.fuzz prints its command once, before the first target.
	const fuzzCommand = "go test -run=NONE -fuzz='^TARGET$' -fuzztime=10s -timeout 20m . (once for each target)"
	if l, targets := parseGateLog("fuzz", "== "+fuzzCommand+"\n"+ok); l.Status != "pass" || len(l.Commands) != 1 || l.Commands[0] != fuzzCommand || len(targets) != 2 {
		t.Errorf("fuzz log with its command line: %+v, %d targets", l, len(targets))
	}
}

// The gate record takes the commit and tree state from the stamp verify.all
// writes before its first leg, and says so; without a stamp it says that the
// tree was read afterwards. A leg with no log is not-run, never a pass.
func TestGateRecordReadsTheStamp(t *testing.T) {
	dir := t.TempDir()
	write := func(name, text string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("vet.log", "== go vet ./...\nverify.vet: ok\n")
	write("stamp.txt", "commit=0123456789ab\ndirty=3\nstarted=2026-01-02T03:04:05Z\n")
	rec := readGateLogs(dir)
	if rec.Commit != "0123456789ab" || !rec.Dirty || rec.Recorded != "2026-01-02T03:04:05Z" || !strings.HasPrefix(rec.Stamp, "written by verify.all") {
		t.Errorf("stamped record: commit %q dirty %v recorded %q stamp %q", rec.Commit, rec.Dirty, rec.Recorded, rec.Stamp)
	}
	status := map[string]string{}
	for _, l := range rec.Legs {
		status[l.Leg] = l.Status
	}
	if len(rec.Legs) != len(gateLegOrder) || status["vet"] != "pass" || status["test"] != "not-run" || status["arm64"] != "not-run" {
		t.Errorf("legs: %v", status)
	}
	write("stamp.txt", "commit=0123456789ab\ndirty=0\nstarted=2026-01-02T03:04:05Z\n")
	if rec := readGateLogs(dir); rec.Dirty {
		t.Error("a stamp with dirty=0 was recorded as a dirty tree")
	}
	if err := os.Remove(filepath.Join(dir, "stamp.txt")); err != nil {
		t.Fatal(err)
	}
	if rec := readGateLogs(dir); !strings.HasPrefix(rec.Stamp, "none") {
		t.Errorf("record without a stamp: stamp %q", rec.Stamp)
	}
}

// What changed between the measured commit and the gated commit decides
// whether the gates speak for the measured code.
func TestGateCommitComparison(t *testing.T) {
	diff := func(files []string, err error) func(a, b string) ([]string, error) {
		return func(a, b string) ([]string, error) {
			if a != "aaa" || b != "bbb" {
				t.Errorf("diff called with %q, %q; want the measured commit, then the gated commit", a, b)
			}
			return files, err
		}
	}
	never := func(a, b string) ([]string, error) {
		t.Errorf("diff called with %q, %q; nothing to compare", a, b)
		return nil, nil
	}
	rec := gatesRecord{Commit: "bbb"}
	compareGateCommit(&rec, "", never)
	if !strings.HasPrefix(rec.Comparison, "not-compared") || rec.SourceChanged != nil {
		t.Errorf("no measured commit: %+v", rec)
	}
	rec = gatesRecord{Commit: "bbb"}
	compareGateCommit(&rec, "bbb", never)
	if rec.Comparison != "same-commit" || rec.MeasuredCommit != "bbb" {
		t.Errorf("same commit: %+v", rec)
	}
	rec = gatesRecord{Commit: "bbb"}
	compareGateCommit(&rec, "aaa", diff([]string{"results-05-01-results.jsonl", "report-03-x.md", "index.html", "results-00-01-index.json"}, nil))
	if rec.Comparison != "compared" || rec.SourceChanged == nil || len(rec.SourceChanged) != 0 || rec.ResultsChanged != 4 {
		t.Errorf("results only: %+v", rec)
	}
	rec = gatesRecord{Commit: "bbb"}
	compareGateCommit(&rec, "aaa", diff([]string{"z_test.go", "results-05-01-results.jsonl", "a.go", "measure.mk", "docs/index.html"}, nil))
	if rec.Comparison != "compared" || strings.Join(rec.SourceChanged, ",") != "a.go,docs/index.html,measure.mk,z_test.go" || rec.ResultsChanged != 1 {
		t.Errorf("source changed: %+v", rec)
	}
	rec = gatesRecord{Commit: "bbb"}
	compareGateCommit(&rec, "aaa", diff(nil, os.ErrNotExist))
	if !strings.HasPrefix(rec.Comparison, "not-compared: ") || rec.SourceChanged != nil {
		t.Errorf("diff failed: %+v", rec)
	}
}

// The evaluation states, from the gate record, whether the gates ran on the
// measured code.
func TestEvaluationStatesWhatChangedBeforeTheGates(t *testing.T) {
	data := &reportData{meta: map[string]any{"profile": "quick", "commit": "abc123", "generator_version": measureGeneratorVersion},
		summary: map[string]any{"failures": 0.0}}
	render := func(g gatesRecord) string {
		g.Legs = []gateLeg{{Leg: "vet", Status: "pass"}}
		return renderEvaluation(evaluationInput{data: data, derived: deriveReport(data, nil), gates: &g,
			resultsName: "results-09-01-results.jsonl", registryName: "results-09-02-registry.json", gatesName: "results-09-06-gates.json"})
	}
	for name, c := range map[string]struct {
		gates gatesRecord
		want  string
		deny  string
	}{
		"same commit":  {gatesRecord{Commit: "abc123", MeasuredCommit: "abc123", Comparison: "same-commit"}, "the commit the measurements were taken at", "different commit"},
		"results only": {gatesRecord{Commit: "def456", MeasuredCommit: "abc123", Comparison: "compared", SourceChanged: []string{}, ResultsChanged: 17}, "only result, report and site files changed (17 files), so the gates ran on the code that was measured", "did not run on exactly"},
		"source":       {gatesRecord{Commit: "def456", MeasuredCommit: "abc123", Comparison: "compared", SourceChanged: []string{"a.go", "b_test.go"}}, "**2 files other than results changed between the two commits**, so the gates did not run on exactly the code that was measured: `a.go`, `b_test.go`", "ran on the code that was measured"},
		"not compared": {gatesRecord{Commit: "def456"}, "what changed between the two was not recorded", "ran on the code that was measured"},
		// A comparison made for another run's commit says nothing about this one.
		"other run": {gatesRecord{Commit: "def456", MeasuredCommit: "999999", Comparison: "compared", SourceChanged: []string{}}, "what changed between the two was not recorded", "ran on the code that was measured"},
	} {
		page := render(c.gates)
		if !strings.Contains(page, c.want) {
			t.Errorf("%s: evaluation lacks %q", name, c.want)
		}
		if strings.Contains(page, c.deny) {
			t.Errorf("%s: evaluation must not say %q", name, c.deny)
		}
	}
}

// The per-scenario table compares each good-fit attachment with the baseline
// of its own scenario, and only calls a difference when the 95 percent
// intervals do not overlap.
func TestEvaluationComparesEachScenarioWithItsBaseline(t *testing.T) {
	// Every implementation delivers all 100 publishes to its 8 subscribers,
	// except the one that is made to drop below.
	rec := func(family, impl string, mean, half float64) map[string]any {
		return map[string]any{"type": "metric", "family": family, "impl": impl, "metric": "publish_ns_op", "unit": "ns",
			"dims": map[string]any{"subs": 8.0}, "mean": mean, "ci95_halfwidth": half, "ci95_low": mean - half, "ci95_high": mean + half,
			"publishes": 100.0, "receives": 800.0, "delivered_fraction": 1.0}
	}
	dropper := rec("broadcast", "Dropper", 5, 1) // cheaper than everything, and delivers a tenth
	dropper["receives"], dropper["delivered_fraction"] = 80.0, 0.1
	metrics := []map[string]any{
		rec("broadcast", "BaselineB", 100, 10),
		rec("broadcast", "Fast", 50, 5),     // 45..55 against 90..110: faster
		rec("broadcast", "Close", 95, 20),   // 75..115 overlaps: not distinguishable
		rec("broadcast", "Slow", 300, 20),   // 280..320: slower
		rec("broadcast", "Elsewhere", 1, 0), // attached to another scenario only
		rec("broadcast", "PoorFit", 2, 0),   // attached, but not as a good fit
		dropper,
		// The same metric at another subscriber count is not the headline.
		{"type": "metric", "family": "broadcast", "impl": "Fast", "metric": "publish_ns_op", "dims": map[string]any{"subs": 64.0}, "mean": 9999.0, "ci95_low": 9999.0, "ci95_high": 9999.0},
		rec("fanin", "BaselineF", 100, 1),
		rec("fanin", "Other", 10, 1),
	}
	attach := func(scenario, impl string, good bool) any {
		return map[string]any{"scenario": scenario, "impl": impl, "expected_good_fit": good}
	}
	registry := map[string]any{
		"scenarios": []any{
			map[string]any{"id": "ring", "kind": "broadcast", "baseline": "BaselineB"},
			map[string]any{"id": "other", "kind": "broadcast", "baseline": "BaselineMissing"},
			map[string]any{"id": "merge", "kind": "fanin", "baseline": "BaselineF"},
		},
		"attachments": []any{
			attach("ring", "BaselineB", true), attach("ring", "Fast", true), attach("ring", "Close", true), attach("ring", "Slow", true),
			attach("ring", "PoorFit", false), attach("ring", "NotMeasured", true), attach("ring", "Dropper", true),
			attach("other", "Elsewhere", true), attach("merge", "Other", true),
		},
	}
	data := &reportData{meta: map[string]any{"generator_version": measureGeneratorVersion}, metrics: metrics}
	var b strings.Builder
	evalScenarioBaselines(&b, evaluationInput{data: data, registry: registry, derived: deriveReport(data, registry)}, []string{"broadcast", "fanin"})
	page := b.String()
	for _, want := range []string{
		"## Each scenario against its own baseline",
		// The baseline delivers everything. The attachment that drops is the
		// cheapest of all and is counted, not compared, and not called faster.
		"| `ring` | broadcast | BaselineB | 100 ns (90 ns to 110 ns) | 100% | 3 | 1 | 1 | 1 | 1 | Fast (50 ns); Close (95 ns); Slow (300 ns) |",
		// A scenario whose baseline has no record is listed without a verdict.
		"| `other` | broadcast | BaselineMissing | no record | no record | 1 |  |  |  | 0 | Elsewhere (1 ns) |",
	} {
		if !strings.Contains(page, want) {
			t.Errorf("per-scenario section lacks %q\n%s", want, page)
		}
	}
	// A family too small to rank has no row: its measurements are printed in full elsewhere.
	if strings.Contains(page, "`merge`") {
		t.Errorf("the unranked fanin family has a per-scenario row\n%s", page)
	}
	// A family whose measurement is not valid under the run's contract has no row either.
	caster := []map[string]any{}
	for _, impl := range []string{"BaselineC", "C1", "C2", "C3", "C4"} {
		m := rec("caster", impl, 10, 1)
		m["metric"], m["dims"] = "send_ns_op", map[string]any{"receivers": 8.0}
		caster = append(caster, m)
	}
	registry = map[string]any{
		"scenarios":   []any{map[string]any{"id": "handoff", "kind": "caster", "baseline": "BaselineC"}},
		"attachments": []any{attach("handoff", "C1", true)},
	}
	for generator, wantRow := range map[string]bool{"1.1.0": false, casterDeliveryContract: false, isolationContract: true} {
		data := &reportData{meta: map[string]any{"generator_version": generator}, metrics: caster}
		b.Reset()
		evalScenarioBaselines(&b, evaluationInput{data: data, registry: registry, derived: deriveReport(data, registry)}, []string{"caster"})
		if got := strings.Contains(b.String(), "`handoff`"); got != wantRow {
			t.Errorf("generator %s: caster row present = %v, want %v", generator, got, wantRow)
		}
		page := renderEvaluation(evaluationInput{data: data, registry: registry, derived: deriveReport(data, registry),
			resultsName: "results-09-01-results.jsonl", registryName: "results-09-02-registry.json"})
		if got := strings.Contains(page, "## Family caster: not ranked in this run"); got == wantRow {
			t.Errorf("generator %s: 'not ranked in this run' section present = %v", generator, got)
		}
		if got := strings.Contains(page, "## Family caster: 5 implementations"); got != wantRow {
			t.Errorf("generator %s: ranked caster section present = %v, want %v", generator, got, wantRow)
		}
	}
}

func TestEvaluationIsGeneratedFromTheRecords(t *testing.T) {
	var metrics []map[string]any
	for i, impl := range []string{"A", "B", "C", "D", "BaselineE"} {
		metrics = append(metrics,
			map[string]any{"type": "metric", "family": "broadcast", "impl": impl, "metric": "publish_ns_op", "unit": "ns/op",
				"dims": map[string]any{"subs": float64(8)}, "mean": float64(100 * (i + 1)), "cv": 0.02 * float64(i), "ci95_halfwidth": float64(i + 1)},
			map[string]any{"type": "metric", "family": "broadcast", "impl": impl, "metric": "stalled_consumer_ops_completed",
				"outcome": map[bool]string{true: "completed", false: "publisher_blocked"}[i%2 == 0], "ops_completed": float64(10 * i)},
		)
	}
	metrics = append(metrics,
		map[string]any{"type": "metric", "family": "exchange", "impl": "X|Y", "metric": "exchange_ns_op", "unit": "ns/op",
			"dims": map[string]any{"senders": float64(2)}, "mean": 1500.0, "ci95_halfwidth": 150.0, "cv": 0.3, "exactly_once_violations": 0.0},
		map[string]any{"type": "metric", "family": "payload", "impl": "P", "metric": "torn_reads", "checked": 500.0, "violations": 0.0},
	)
	data := &reportData{
		meta:    map[string]any{"profile": "quick", "commit": "abc123", "num_cpu": 2.0, "gomaxprocs": 2.0, "seed": 1.0, "generator_version": measureGeneratorVersion},
		metrics: metrics,
		// One skip record per repetition: the evaluation reports the two as one row.
		skipped: []map[string]any{
			{"family": "broadcast", "impl": "S", "battery": "broadcast.churn", "reason": "cannot churn"},
			{"family": "broadcast", "impl": "S", "battery": "broadcast.churn", "reason": "cannot churn"},
		},
		summary: map[string]any{"failures": 0.0, "duration_s": 12.4},
	}
	registry := map[string]any{"scenarios": []any{1, 2}, "attachments": []any{1, 2, 3}}
	gates := &gatesRecord{Commit: "def456", Dirty: true, Legs: []gateLeg{{Leg: "vet", Status: "pass", Detail: "4 vet invocations"}, {Leg: "arm64", Status: "not-run", Detail: "no log"}}}
	page := renderEvaluation(evaluationInput{data: data, registry: registry, derived: deriveReport(data, registry), gates: gates,
		resultsName: "results-09-01-results.jsonl", registryName: "results-09-02-registry.json", gatesName: "results-09-06-gates.json", verified: true})
	for _, want := range []string{
		"# Evaluation of measurement run 09",
		"| Commit | `abc123` |",
		"at commit `def456`, **with uncommitted changes in the tree**",
		"The measurements were taken at commit `abc123`. That is a different commit, and what changed between the two was not recorded.",
		"| `verify.arm64` | not-run | no log |",
		"2 scenarios and 3 attachments",
		"## Family broadcast: 5 implementations",
		"| 1 | A | 58 | 100 | no data | no data | no data | no data | 0 |",
		"No battery in this family produces data for: latency, allocations, churn, scaling.",
		"### exchange",
		"| `exchange_ns_op` (senders=2) | 1.50 µs ±10% |",
		"X\\|Y", // a pipe in a name must not break the table
		"| broadcast | completed | 3 | 0 / 20 / 40 |",
		"| broadcast | publisher_blocked | 2 | 10 / 20 / 30 |",
		"2 skip records, 1 distinct.",
		"| broadcast | S | broadcast.churn | cannot churn | 2 |",
		"checked for tearing: 500; torn: 0.",
		"- No implementation failed an integrity check.",
		"2 logical CPUs at GOMAXPROCS 2",
	} {
		if !strings.Contains(page, want) {
			t.Errorf("evaluation lacks %q", want)
		}
	}
	for _, delimiter := range []string{"{{", "{%"} {
		if strings.Contains(page, delimiter) {
			t.Errorf("evaluation contains the Liquid delimiter %q", delimiter)
		}
	}
	// Without a gates record the evaluation says so; it does not imply a pass.
	page = renderEvaluation(evaluationInput{data: data, registry: registry, derived: deriveReport(data, registry),
		resultsName: "results-09-01-results.jsonl", registryName: "results-09-02-registry.json"})
	if !strings.Contains(page, "No gate record exists for this run group.") {
		t.Error("evaluation without gates does not say that no gate record exists")
	}
}

// Every file the tracker names must exist, in whatever shorthand the row
// uses, so the landing page never links a file that is not there.
func TestTrackerFilesResolve(t *testing.T) {
	cat, err := loadSiteCatalogData(".", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Responses) == 0 {
		t.Fatal("no responses found; the check would pass vacuously")
	}
	for _, r := range cat.Responses {
		if r.Files == "-" || r.Files == "" {
			continue
		}
		files, unresolved := trackerFiles(".", r.Files)
		if len(unresolved) > 0 {
			t.Errorf("response %s: the tracker names %q, which resolves to no file (cell: %s)", r.Num, unresolved, r.Files)
		}
		if len(files) == 0 {
			t.Errorf("response %s: no file resolved from %q", r.Num, r.Files)
		}
	}
	files, unresolved := trackerFiles(".", "`telemetryring_63_qwen.go`, `_fast.go`, `_race.go`, `bitmap_72_gemini_{amd64,arm64}.s`, `nosuchfile.go`")
	want := []string{"telemetryring_63_qwen.go", "telemetryring_63_qwen_fast.go", "telemetryring_63_qwen_race.go", "bitmap_72_gemini_amd64.s", "bitmap_72_gemini_arm64.s"}
	if strings.Join(files, " ") != strings.Join(want, " ") || len(unresolved) != 1 || unresolved[0] != "nosuchfile.go" {
		t.Errorf("trackerFiles: files %q, unresolved %q", files, unresolved)
	}
}

// renderCommittedLandingPage renders the landing page from the working tree
// without writing anything.
func renderCommittedLandingPage(t *testing.T) (string, siteFacts) {
	t.Helper()
	reports, err := discoverSiteReports(".")
	if err != nil {
		t.Fatal(err)
	}
	cat, err := loadSiteCatalogData(".", reports)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := loadSiteFacts(".")
	if err != nil {
		t.Fatal(err)
	}
	return renderLandingPage(cat, facts), facts
}

// The landing page links every run's report and files and every written
// analysis, and no template placeholder survives rendering.
// Every response file has a tracker row, and every tracker row has a response
// file. The landing page shows what the tracker says and nothing else, so a
// response the tracker omits would appear with no implementation or state.
func TestTrackerCoversEveryResponse(t *testing.T) {
	reports, err := discoverSiteReports(".")
	if err != nil {
		t.Fatal(err)
	}
	cat, err := loadSiteCatalogData(".", reports)
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Responses) == 0 {
		t.Fatal("no response files found")
	}
	listed := map[string]bool{}
	for _, r := range cat.Responses {
		listed[r.Num] = true
		if strings.HasPrefix(r.State, "not listed in the tracker") {
			t.Errorf("%s has no row in the tracker table of 01-readme.md", r.Filename)
		}
		if strings.TrimSpace(r.Impls) == "" || strings.TrimSpace(r.State) == "" {
			t.Errorf("%s: the tracker row has an empty implementation or state cell", r.Filename)
		}
	}
	raw, err := os.ReadFile("01-readme.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		cells := strings.Split(line, "|")
		if len(cells) < 5 {
			continue
		}
		num := strings.TrimSpace(cells[1])
		if len(num) == 2 && num[0] >= '0' && num[0] <= '9' && num[1] >= '0' && num[1] <= '9' && !listed[num] {
			t.Errorf("tracker row %s has no response file %s-*.md", num, num)
		}
	}
}

func TestLandingPageCoversEveryRunAndReport(t *testing.T) {
	page, facts := renderCommittedLandingPage(t)
	if len(facts.Runs) == 0 {
		t.Skip("no committed measurement runs")
	}
	if m := regexp.MustCompile(`__[A-Z][A-Z_]+__`).FindAllString(page, -1); len(m) > 0 {
		t.Errorf("placeholders left in the landing page: %q", m)
	}
	var want []string
	for _, pattern := range []string{"results-[0-9][0-9]-[0-9][0-9]-*", "report-[0-9][0-9]-*"} {
		files, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, files...)
	}
	for _, file := range want {
		if !strings.Contains(page, `href="`+file+`"`) {
			t.Errorf("the landing page does not link %s", file)
		}
	}
	latest := facts.Runs[len(facts.Runs)-1]
	if !strings.Contains(page, "Measurement Report (Run "+latest.Run+")") {
		t.Errorf("the navigation does not point at the newest run, %s", latest.Run)
	}
}

// TestCommittedLandingPageIsCurrent fails when index.html is not what the
// committed files render to: a run, report, tracker row or score changed and
// the page was not rebuilt.
func TestCommittedLandingPageIsCurrent(t *testing.T) {
	committed, err := os.ReadFile("index.html")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := renderCommittedLandingPage(t)
	if page != string(committed) {
		t.Error("index.html is stale: run 'make site' and commit the result")
	}
}

// The report script draws outlines as SVG paths. A polygon's points attribute
// takes bare coordinates, not path data; every report up to run 04 gave it
// path data, so browsers drew neither the radar's rings nor its shapes. A DOM
// emulator does not notice, which is why this is checked on the source.
func TestReportScriptDrawsOutlinesAsPaths(t *testing.T) {
	script := reportJS1 + reportJS2 + reportJS3 + reportJS4
	if strings.Contains(script, "svg('polygon'") || strings.Contains(script, "svg('polyline'") {
		t.Error("the report script builds a polygon or polyline; its outlines are path data and must be path elements")
	}
	for _, want := range []string{"svg('path', {d: d,", "class:'radar-shape'", "class:'radar-nodata'", "function renderRunnables()"} {
		if !strings.Contains(script, want) {
			t.Errorf("the report script no longer contains %q", want)
		}
	}
	if !strings.Contains(reportHTMLBody, `id="runTable"`) || !strings.Contains(reportHTMLBody, `id="runSummary"`) {
		t.Error("the report body has no runnables table")
	}
}
