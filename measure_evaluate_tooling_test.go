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
	if l, _ := parseGateLog("fuzz", ok); l.Status != "pass" {
		t.Errorf("passing fuzz log: %+v", l)
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
		skipped: []map[string]any{{"family": "broadcast", "impl": "S", "battery": "broadcast.churn", "reason": "cannot churn"}},
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
		"The measurements were taken at commit `abc123`, which is a different commit.",
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
		"| broadcast | S | broadcast.churn | cannot churn |",
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
