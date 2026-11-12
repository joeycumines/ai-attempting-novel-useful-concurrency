package bigbuff

// bigbuff-measure gates and bigbuff-measure evaluate.
//
// gates     reads the logs `make verify VERIFY_LOG=<dir>` writes and records,
//           as results-NN-06-gates.json, which correctness gates ran, at
//           which commit, and what each one reported.
// evaluate  writes results-NN-07-evaluation.md from the run's own files: the
//           results JSONL, the registry dump and the gates record.
//
// The evaluation is generated so that every number in it can be traced to a
// committed file and regenerated. It states what was measured and makes no
// recommendation: judgement belongs in a separate, signed analysis.

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// --- gates ---

type gateLeg struct {
	Leg      string   `json:"leg"`
	Status   string   `json:"status"` // pass | fail | not-run
	Commands []string `json:"commands,omitempty"`
	Detail   string   `json:"detail"`
}

type gateFuzzTarget struct {
	Target string `json:"target"`
	Status string `json:"status"`
	Budget string `json:"budget"`
	Execs  int64  `json:"execs"`
}

type gatesRecord struct {
	Type     string           `json:"type"`
	Schema   int              `json:"schema"`
	Commit   string           `json:"commit"`
	Dirty    bool             `json:"dirty_tree"`
	Recorded string           `json:"recorded"`
	Stamp    string           `json:"stamp"`
	Go       string           `json:"go"`
	GOOS     string           `json:"goos"`
	GOARCH   string           `json:"goarch"`
	NumCPU   int              `json:"num_cpu"`
	Legs     []gateLeg        `json:"legs"`
	Fuzz     []gateFuzzTarget `json:"fuzz"`

	// How the tree the gates ran on relates to the tree that was measured.
	// Filled in when `gates` is given the run (--run); see compareGateCommit.
	MeasuredCommit string `json:"measured_commit,omitempty"`
	Comparison     string `json:"comparison,omitempty"` // same-commit | compared | not-compared: <why>
	// SourceChanged is null unless Comparison is "compared"; an empty list
	// then means that no file other than results changed.
	SourceChanged  []string `json:"source_files_changed_since_measurement"`
	ResultsChanged int      `json:"result_files_changed_since_measurement"`
}

// isResultFile reports whether a path is something the harness writes or
// rewires (run artifacts, the landing page, report pages) rather than code,
// build files or documentation.
func isResultFile(path string) bool {
	return strings.HasPrefix(path, "results-") || strings.HasPrefix(path, "report-") || path == "index.html"
}

// compareGateCommit records what changed between the measured commit and the
// commit the gates ran on. The usual order is: measure on a clean tree, commit
// the run, run the gates on that commit. The two commits then differ, and the
// question that matters is whether any code differs. diff lists the files
// changed between two commits; it is a parameter so the rule can be tested
// without a repository.
func compareGateCommit(rec *gatesRecord, measured string, diff func(a, b string) ([]string, error)) {
	rec.MeasuredCommit = measured
	if measured == "" {
		rec.Comparison = "not-compared: the run records no commit"
		return
	}
	if measured == rec.Commit {
		rec.Comparison = "same-commit"
		return
	}
	files, err := diff(measured, rec.Commit)
	if err != nil {
		rec.Comparison = "not-compared: " + err.Error()
		return
	}
	rec.Comparison = "compared"
	rec.SourceChanged = []string{}
	for _, f := range files {
		if isResultFile(f) {
			rec.ResultsChanged++
		} else {
			rec.SourceChanged = append(rec.SourceChanged, f)
		}
	}
	sort.Strings(rec.SourceChanged)
}

func gitChangedFiles(a, b string) ([]string, error) {
	out, err := exec.Command("git", "diff", "--name-only", a, b).Output()
	if err != nil {
		return nil, fmt.Errorf("git diff %s %s failed", a, b)
	}
	files := []string{}
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			files = append(files, line)
		}
	}
	return files, nil
}

// gateLegOrder is every leg verify.mk can log, in the order it is reported.
var gateLegOrder = []string{"vet", "test", "race", "alloc", "fuzz", "arm64"}

var (
	gateOkLine    = regexp.MustCompile(`(?m)^ok\s+\S+\s+([0-9.]+)s`)
	gateFailLine  = regexp.MustCompile(`(?m)^(FAIL|panic:|--- FAIL)`)
	gateFuzzHead  = regexp.MustCompile(`^== (Fuzz[A-Za-z0-9_]*) \(([^)]*)\)`)
	gateFuzzExecs = regexp.MustCompile(`execs: ([0-9]+)`)
)

// parseGateLog turns one leg's log into its record. A log that does not
// positively report success is a failure: silence is never a pass.
func parseGateLog(leg, text string) (gateLeg, []gateFuzzTarget) {
	out := gateLeg{Leg: leg, Status: "fail"}
	var fuzz []gateFuzzTarget
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "== ") {
			out.Commands = append(out.Commands, strings.TrimPrefix(line, "== "))
		}
	}
	failed := gateFailLine.MatchString(text)
	switch leg {
	case "vet":
		if strings.Contains(text, "verify.vet: ok") && !failed {
			out.Status = "pass"
		}
		out.Detail = fmt.Sprintf("%d vet invocations", len(out.Commands))
	case "test", "race", "alloc":
		m := gateOkLine.FindStringSubmatch(text)
		if m != nil && !failed {
			out.Status = "pass"
			out.Detail = m[1] + " s"
		}
		if leg == "alloc" {
			out.Detail = strings.TrimSpace(fmt.Sprintf("%d passing, %d failing, %d skipped test results; %s",
				strings.Count(text, "--- PASS"), strings.Count(text, "--- FAIL"), strings.Count(text, "--- SKIP"), out.Detail))
		}
		if failed {
			out.Detail = strings.TrimSpace("reported a failure; " + out.Detail)
		}
	case "fuzz":
		var cur *gateFuzzTarget
		flush := func() {
			if cur != nil {
				fuzz = append(fuzz, *cur)
			}
		}
		for _, line := range strings.Split(text, "\n") {
			if m := gateFuzzHead.FindStringSubmatch(line); m != nil {
				flush()
				cur = &gateFuzzTarget{Target: m[1], Budget: m[2], Status: "fail"}
				continue
			}
			if cur == nil {
				continue
			}
			if m := gateFuzzExecs.FindStringSubmatch(line); m != nil {
				cur.Execs, _ = strconv.ParseInt(m[1], 10, 64)
			}
			if strings.HasPrefix(line, "ok ") || line == "PASS" {
				if cur.Status != "failed" {
					cur.Status = "pass"
				}
			}
			if gateFailLine.MatchString(line) {
				cur.Status = "failed"
			}
		}
		flush()
		for i := range fuzz {
			if fuzz[i].Status == "failed" {
				fuzz[i].Status = "fail"
			}
		}
		passed := 0
		for _, f := range fuzz {
			if f.Status == "pass" {
				passed++
			}
		}
		// The command is the line verify.fuzz prints before its first
		// target; the per-target headers are not commands.
		commands := out.Commands
		out.Commands = nil
		for _, c := range commands {
			if !strings.HasPrefix(c, "Fuzz") {
				out.Commands = append(out.Commands, c)
			}
		}
		out.Detail = fmt.Sprintf("%d of %d targets passed", passed, len(fuzz))
		if strings.Contains(text, "verify.fuzz: ok") && passed == len(fuzz) && len(fuzz) > 0 {
			out.Status = "pass"
		}
	case "arm64":
		if strings.Contains(text, "verify.arm64: ok") && !failed {
			out.Status = "pass"
			out.Detail = "the arm64 test binary executed"
			// Where it ran is part of what the leg shows: an emulator
			// executes the instructions and says nothing about the memory
			// ordering of real arm64 hardware.
			switch {
			case strings.Contains(text, "== native arm64"):
				out.Detail += " on an arm64 host"
			case strings.Contains(text, "== GOARCH=arm64 under "):
				emulator := "an emulator"
				for _, line := range strings.Split(text, "\n") {
					if v, ok := strings.CutPrefix(line, "== emulator: "); ok && strings.TrimSpace(v) != "" {
						emulator = "an emulator (" + strings.TrimSpace(v) + ")"
					}
				}
				out.Detail += " under " + emulator + ", not on arm64 hardware"
			}
		} else {
			out.Detail = "the arm64 assembly was not executed"
		}
	}
	return out, fuzz
}

func readGateLogs(dir string) gatesRecord {
	rec := gatesRecord{
		Type: "gates", Schema: measureSchemaVersion,
		Commit:   gitCommit(),
		Recorded: time.Now().UTC().Format(time.RFC3339),
		Go:       runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, NumCPU: runtime.NumCPU(),
		Legs: []gateLeg{}, Fuzz: []gateFuzzTarget{},
	}
	// The stamp verify.all writes before its first leg says which tree the
	// gates ran on. Without it the tree is read now, and the record says so.
	if raw, err := os.ReadFile(filepath.Join(dir, "stamp.txt")); err == nil {
		rec.Stamp = "written by verify.all before its first leg"
		for _, line := range strings.Split(string(raw), "\n") {
			key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
			if !ok {
				continue
			}
			switch key {
			case "commit":
				rec.Commit = value
			case "dirty":
				rec.Dirty = value != "0"
			case "started":
				rec.Recorded = value
			}
		}
	} else {
		rec.Stamp = "none: commit and tree state were read when this record was written, after the gates ran"
		if out, err := exec.Command("git", "status", "--porcelain").Output(); err != nil || len(strings.TrimSpace(string(out))) > 0 {
			rec.Dirty = true
		}
	}
	for _, leg := range gateLegOrder {
		raw, err := os.ReadFile(filepath.Join(dir, leg+".log"))
		if err != nil {
			rec.Legs = append(rec.Legs, gateLeg{Leg: leg, Status: "not-run", Detail: "no log"})
			continue
		}
		l, fuzz := parseGateLog(leg, string(raw))
		rec.Legs = append(rec.Legs, l)
		rec.Fuzz = append(rec.Fuzz, fuzz...)
	}
	return rec
}

func cmdMeasureGates(args []string) int {
	fs := flag.NewFlagSet("gates", flag.ContinueOnError)
	logDir := fs.String("log", "", "directory holding the <leg>.log files written by `make verify VERIFY_LOG=<dir>`")
	out := fs.String("out", "", "output path (results-NN-06-gates.json)")
	run := fs.String("run", "", "results JSONL of the run the record belongs to; records what changed between the measured commit and the gated one")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *logDir == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "gates: --log <dir> and --out <file> are required")
		return 2
	}
	rec := readGateLogs(*logDir)
	if *run != "" {
		data, err := loadReportData(*run)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gates: load %s: %v\n", *run, err)
			return 1
		}
		measured, _ := data.meta["commit"].(string)
		compareGateCommit(&rec, measured, gitChangedFiles)
		fmt.Fprintf(os.Stderr, "gates: measured at %s, gated at %s: %s, %d source and %d result files changed\n", measured, rec.Commit, rec.Comparison, len(rec.SourceChanged), rec.ResultsChanged)
	}
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "gates: %v\n", err)
		return 1
	}
	if err := os.WriteFile(*out, append(raw, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "gates: %v\n", err)
		return 1
	}
	rc := 0
	for _, l := range rec.Legs {
		fmt.Fprintf(os.Stderr, "gates: %-6s %-8s %s\n", l.Leg, l.Status, l.Detail)
		if l.Status == "fail" {
			rc = 1
		}
	}
	fmt.Fprintf(os.Stderr, "gates: wrote %s (commit %s, dirty tree: %v)\n", *out, rec.Commit, rec.Dirty)
	return rc
}

// --- evaluate ---

func cmdMeasureEvaluate(args []string) int {
	fs := flag.NewFlagSet("evaluate", flag.ContinueOnError)
	in := fs.String("in", "", "results file (default: newest results-*-01-results.jsonl)")
	registryPath := fs.String("registry", "", "registry dump (default: the run group's slot 02)")
	gatesPath := fs.String("gates", "", "gates record (default: the run group's slot 06, if present)")
	out := fs.String("out", "", "output path (default: the run group's slot 07, results-NN-07-evaluation.md)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *in == "" {
		if *in = latestRunResultsJSONL("."); *in == "" {
			fmt.Fprintln(os.Stderr, "evaluate: no results-*-01-results.jsonl found; pass --in")
			return 2
		}
	}
	dir, base := filepath.Dir(*in), filepath.Base(*in)
	stem := strings.TrimSuffix(base, "-01-results.jsonl")
	if stem == base {
		stem = strings.TrimSuffix(base, filepath.Ext(base))
	}
	registryNote := ""
	if *registryPath == "" {
		if *registryPath, registryNote = registryDumpForResults(*in); *registryPath == "" {
			fmt.Fprintf(os.Stderr, "evaluate: no registry dump at or before %s; pass --registry\n", base)
			return 1
		}
	}
	if *gatesPath == "" {
		if p := filepath.Join(dir, stem+"-06-gates.json"); fileExists(p) {
			*gatesPath = p
		}
	}
	if *out == "" {
		*out = filepath.Join(dir, stem+"-07-evaluation.md")
	}
	data, err := loadReportData(*in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "evaluate: load %s: %v\n", *in, err)
		return 1
	}
	registry, err := readRegistryDump(*registryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "evaluate: %v\n", err)
		return 1
	}
	var gates *gatesRecord
	if *gatesPath != "" {
		raw, err := os.ReadFile(*gatesPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "evaluate: %v\n", err)
			return 1
		}
		gates = &gatesRecord{}
		if err := json.Unmarshal(raw, gates); err != nil {
			fmt.Fprintf(os.Stderr, "evaluate: parse %s: %v\n", *gatesPath, err)
			return 1
		}
	}
	var verify strings.Builder
	verified := verifyResultsAgainst(*in, measuredSetsOf(registry), filepath.Base(*registryPath), &verify) == 0
	page := renderEvaluation(evaluationInput{
		data: data, registry: registry, derived: deriveReport(data, registry), gates: gates,
		resultsName: base, registryName: filepath.Base(*registryPath), registryNote: registryNote, gatesName: filepath.Base(*gatesPath),
		verified: verified,
		previous: loadPreviousRun(*in),
	})
	if err := os.WriteFile(*out, []byte(page), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "evaluate: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "evaluate: wrote %s (%d bytes)\n", *out, len(page))
	return 0
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

type evaluationInput struct {
	data         *reportData
	registry     map[string]any
	derived      derivedData
	gates        *gatesRecord
	resultsName  string
	registryName string
	registryNote string // set when the dump is not the run group's own
	gatesName    string
	verified     bool
	// previous is the run numbered one below this one, when it is on disk.
	previous *evalPrevious
}

// mdCell makes a value safe inside a Markdown table cell. Braces are spaced
// apart so that GitHub Pages never reads a Liquid delimiter.
func mdCell(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "{{", "{ {")
	s = strings.ReplaceAll(s, "{%", "{ %")
	return s
}

func mdTable(b *strings.Builder, header []string, rows [][]string) {
	head := make([]string, len(header))
	for i, h := range header {
		head[i] = mdCell(h)
	}
	b.WriteString("| " + strings.Join(head, " | ") + " |\n|")
	for range header {
		b.WriteString("---|")
	}
	b.WriteString("\n")
	for _, row := range rows {
		cells := make([]string, len(row))
		for i, c := range row {
			cells[i] = mdCell(c)
		}
		b.WriteString("| " + strings.Join(cells, " | ") + " |\n")
	}
	b.WriteString("\n")
}

func evalStr(m map[string]any, key string) string {
	switch v := m[key].(type) {
	case nil:
		return "unknown"
	case string:
		return v
	case float64:
		if v == math.Trunc(v) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

// evalDims renders a record's dimensions as "receivers=8, rounds=24".
func evalDims(m map[string]any) string {
	dims, _ := m["dims"].(map[string]any)
	keys := make([]string, 0, len(dims))
	for k := range dims {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+evalStr(dims, k))
	}
	return strings.Join(parts, ", ")
}

func evalQuantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	// Linear interpolation between the two nearest order statistics.
	pos := q * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	return sorted[lo] + (pos-float64(lo))*(sorted[hi]-sorted[lo])
}

// evalValue renders a metric mean in the unit of its record.
func evalValue(m map[string]any) string {
	mean, ok := m["mean"].(float64)
	if !ok {
		return "no value"
	}
	unit, _ := m["unit"].(string)
	var s string
	switch {
	case strings.HasPrefix(unit, "ns"):
		s = humanNs(mean)
	case mean == math.Trunc(mean) && math.Abs(mean) < 1e15:
		s = thousandsSep(mean)
	default:
		s = strconv.FormatFloat(mean, 'g', 4, 64)
	}
	if hw, ok := m["ci95_halfwidth"].(float64); ok && mean != 0 {
		s += fmt.Sprintf(" ±%.0f%%", 100*hw/math.Abs(mean))
	}
	return s
}

func renderEvaluation(in evaluationInput) string {
	var b strings.Builder
	meta, data, dd := in.data.meta, in.data, in.derived
	runLabel := strings.TrimSuffix(strings.TrimPrefix(in.resultsName, "results-"), "-01-results.jsonl")

	fmt.Fprintf(&b, "# Evaluation of measurement run %s\n\n", runLabel)
	sources := "`" + in.resultsName + "` and `" + in.registryName + "`"
	if in.gates != nil {
		sources += " and `" + in.gatesName + "`"
	}
	fmt.Fprintf(&b, "Generated by `bigbuff-measure evaluate` from %s. Do not edit it: `make measure-evaluate` regenerates it. It reports what was measured. It recommends nothing.\n\n", sources)
	if in.registryNote != "" {
		fmt.Fprintf(&b, "This run group has no registry dump of its own. `%s` is the nearest earlier one, so the registry facts below are those of that run.\n\n", in.registryName)
	}

	// --- The run ---
	b.WriteString("## The run\n\n")
	counts := map[string]int{"metric": len(data.metrics), "trace": len(data.traces), "skipped": len(data.skipped)}
	failures, duration := "unknown", "unknown"
	if data.summary != nil {
		failures = evalStr(data.summary, "failures")
		if d, ok := data.summary["duration_s"].(float64); ok {
			duration = fmt.Sprintf("%.0f s", d)
		}
	}
	verdict := "**does not cover** the implementations `" + in.registryName + "` names; see `bigbuff-measure verify`"
	if in.verified {
		verdict = "covers every implementation `" + in.registryName + "` names, and every metric record carries a value"
	}
	mdTable(&b, []string{"", ""}, [][]string{
		{"Profile", evalStr(meta, "profile")},
		{"Commit", "`" + evalStr(meta, "commit") + "`"},
		{"Started", evalStr(meta, "timestamp")},
		{"Duration", duration},
		{"Toolchain", evalStr(meta, "go") + " " + evalStr(meta, "goos") + "/" + evalStr(meta, "goarch")},
		{"Processor", evalStr(meta, "cpu")},
		{"Logical CPUs / GOMAXPROCS", evalStr(meta, "num_cpu") + " / " + evalStr(meta, "gomaxprocs")},
		{"Seed", evalStr(meta, "seed")},
		{"Record contract", "generator " + evalStr(meta, "generator_version")},
		{"Records", fmt.Sprintf("%d metric, %d trace, %d skipped", counts["metric"], counts["trace"], counts["skipped"])},
		{"Harness failures", failures},
		{"Verification", verdict},
	})

	families := []string{}
	for _, f := range measureFamilyOrder {
		if len(dd.Scores[f]) > 0 {
			families = append(families, f)
		}
	}
	rows := [][]string{}
	for _, f := range families {
		cov := dd.Coverage[f]
		missing := 0
		if list, ok := cov["missing"].([]string); ok {
			missing = len(list)
		}
		ranked := evalClassScoring(dd.Scores[f])
		if reason, no := dd.Unranked[f]; no {
			ranked = "not ranked: " + reason
		}
		rows = append(rows, []string{f, strconv.Itoa(len(dd.Scores[f])), strconv.Itoa(missing), ranked})
	}
	mdTable(&b, []string{"Family", "Implementations measured", "Registered, without a metric record (see Skipped)", "Scoring"}, rows)

	// --- Gates ---
	b.WriteString("## Correctness gates\n\n")
	if in.gates == nil {
		b.WriteString("No gate record exists for this run group. `make measure-gates` runs `make verify` and writes one.\n\n")
	} else {
		g := in.gates
		tree := "clean tree"
		if g.Dirty {
			tree = "**with uncommitted changes in the tree**"
		}
		fmt.Fprintf(&b, "`make verify` at commit `%s`, %s, recorded %s on %s %s/%s with %d logical CPUs.", g.Commit, tree, g.Recorded, g.Go, g.GOOS, g.GOARCH, g.NumCPU)
		if run := evalStr(meta, "commit"); run != g.Commit {
			fmt.Fprintf(&b, " The measurements were taken at commit `%s`.", run)
			switch {
			case g.Comparison == "compared" && g.MeasuredCommit == run && len(g.SourceChanged) == 0:
				fmt.Fprintf(&b, " Between the two commits only result, report and site files changed (%d files), so the gates ran on the code that was measured.", g.ResultsChanged)
			case g.Comparison == "compared" && g.MeasuredCommit == run:
				shown := g.SourceChanged
				more := ""
				if len(shown) > 12 {
					more = fmt.Sprintf(" and %d more", len(shown)-12)
					shown = shown[:12]
				}
				fmt.Fprintf(&b, " **%d files other than results changed between the two commits**, so the gates did not run on exactly the code that was measured: `%s`%s.", len(g.SourceChanged), strings.Join(shown, "`, `"), more)
			default:
				b.WriteString(" That is a different commit, and what changed between the two was not recorded.")
			}
		} else {
			b.WriteString(" That is the commit the measurements were taken at.")
		}
		if strings.HasPrefix(g.Stamp, "none") {
			b.WriteString(" The logs carried no stamp, so the commit and tree state above were read after the gates ran.")
		}
		b.WriteString("\n\n")
		rows = rows[:0]
		for _, l := range g.Legs {
			rows = append(rows, []string{"`verify." + l.Leg + "`", l.Status, l.Detail, strings.Join(l.Commands, "; ")})
		}
		mdTable(&b, []string{"Gate", "Result", "Detail", "Commands"}, rows)
		if len(g.Fuzz) > 0 {
			rows = rows[:0]
			for _, f := range g.Fuzz {
				rows = append(rows, []string{"`" + f.Target + "`", f.Status, f.Budget, thousandsSep(float64(f.Execs))})
			}
			mdTable(&b, []string{"Fuzz target", "Result", "Budget", "Executions"}, rows)
		}
		b.WriteString("A gate that is `not-run` was not run; it did not pass.\n\n")
	}

	// --- Registry ---
	b.WriteString("## Registry\n\n")
	scenarios, _ := in.registry["scenarios"].([]any)
	attachments, _ := in.registry["attachments"].([]any)
	fmt.Fprintf(&b, "%d scenarios and %d attachments of an implementation to a scenario.", len(scenarios), len(attachments))
	if runnables, ok := in.registry["runnables"].([]any); ok {
		class := map[string]int{}
		for _, r := range runnables {
			if m, ok := r.(map[string]any); ok {
				c, _ := m["class"].(string)
				class[c]++
			}
		}
		fmt.Fprintf(&b, " %d top-level tests, benchmarks and fuzz targets: %d registered scenario cases, which run every attached implementation; %d unit runnables, each registered against the scenarios of the one implementation it tests; %d harness and tooling runnables.",
			len(runnables), class["case"], class["unit"], class["harness"])
	}
	b.WriteString("\n\n")

	// --- Integrity ---
	b.WriteString("## Integrity\n\n")
	var tornChecked, tornViolations float64
	exactlyOnce := map[string][2]float64{} // family -> records, violations
	for _, m := range data.metrics {
		fam, _ := m["family"].(string)
		if m["metric"] == "torn_reads" {
			c, _ := m["checked"].(float64)
			v, _ := m["violations"].(float64)
			tornChecked += c
			tornViolations += v
		}
		if v, ok := m["exactly_once_violations"].(float64); ok {
			e := exactlyOnce[fam]
			exactlyOnce[fam] = [2]float64{e[0] + 1, e[1] + v}
		}
	}
	fmt.Fprintf(&b, "- Multi-word payload reads checked for tearing: %s; torn: %s.\n", thousandsSep(tornChecked), thousandsSep(tornViolations))
	for _, f := range families {
		if e, ok := exactlyOnce[f]; ok {
			fmt.Fprintf(&b, "- %s: %s records carry an exactly-once count; violations in total: %s.\n", f, thousandsSep(e[0]), thousandsSep(e[1]))
		}
	}
	failed := []string{}
	for _, f := range families {
		for impl, s := range dd.Scores[f] {
			if s.Integrity == "failed" {
				failed = append(failed, f+": "+impl)
			}
		}
	}
	sort.Strings(failed)
	if len(failed) == 0 {
		b.WriteString("- No implementation failed an integrity check.\n\n")
	} else {
		b.WriteString("- Implementations that failed an integrity check: " + mdCell(strings.Join(failed, "; ")) + ".\n\n")
	}

	// --- Ranked families ---
	dimOrder := []string{"throughput", "latency", "allocations", "churn", "scaling", "stall_resilience"}
	for _, f := range families {
		scores := dd.Scores[f]
		if _, no := dd.Unranked[f]; no {
			continue
		}
		classes := scoreClasses(scores)
		classed := len(classes) > 1 || classes[0] != ""
		if classed {
			fmt.Fprintf(&b, "## Family %s: %d implementations in delivery classes\n\n", f, len(scores))
			b.WriteString("An implementation of this family either hands every published value to every subscriber, or it overwrites, drops or coalesces what a subscriber has not yet taken. The throughput battery publishes as fast as it can to subscribers that do nothing but receive, and records how much of it arrived. An implementation is in the class \"" + deliveryClassLabel(classDeliversAll) + "\" when everything arrived in every cell and every repetition, and in \"" + deliveryClassLabel(classDrops) + "\" otherwise.\n\n")
			b.WriteString("The classes are scored separately, because a publish is cheaper when the value need not arrive. Scored together, as they were before record contract " + deliveryClassContract + ", every place at the top of the family went to an implementation that drops. The delivered share is what happened in this battery, with a publisher that never pauses on this machine. It is not a constant of the implementation: one that drops delivers everything to subscribers that keep up.\n\n")
			rows = rows[:0]
			for _, c := range classes {
				list, _ := scoresOfClass(scores, c)
				label := deliveryClassLabel(c)
				if c == "" {
					label = "no delivery figures"
				}
				delivered := "no record"
				if lo, mid, hi, ok := evalDelivered(list); ok {
					delivered = evalPercent(lo) + " / " + evalPercent(mid) + " / " + evalPercent(hi)
				}
				state := "ranked"
				if !list[0].Ranked {
					state = "not ranked: " + list[0].UnrankedReason
				}
				rows = append(rows, []string{label, strconv.Itoa(len(list)), delivered, state})
			}
			mdTable(&b, []string{"Class", "Implementations", "Delivered with 8 subscribers: least / median / most", "Scoring"}, rows)
		}
		for _, class := range classes {
			list, rank := scoresOfClass(scores, class)
			among := "family"
			if classed {
				among = "class"
				label := deliveryClassLabel(class)
				if class == "" {
					label = "no delivery figures"
				}
				fmt.Fprintf(&b, "### %s, %s: %d implementations\n\n", f, label, len(list))
			} else {
				fmt.Fprintf(&b, "## Family %s: %d implementations\n\n", f, len(list))
			}
			if !list[0].Ranked {
				names := make([]string, len(list))
				for i, s := range list {
					names[i] = s.Impl
				}
				fmt.Fprintf(&b, "Not ranked: %s. They are: %s.\n\n", list[0].UnrankedReason, mdCell(strings.Join(names, "; ")))
				// The measurements a score would have been built on, by
				// publish cost, so that the class can still be read.
				byCost := append([]implScore{}, list...)
				cost := func(s implScore) float64 {
					if v, ok := s.Metrics["publish_ns_op@8"].(float64); ok {
						return v
					}
					return math.Inf(1)
				}
				sort.SliceStable(byCost, func(i, j int) bool { return cost(byCost[i]) < cost(byCost[j]) })
				rows = rows[:0]
				for _, s := range byCost {
					c, at8, least := "no record", "no record", "no record"
					if v := cost(s); !math.IsInf(v, 1) {
						c = humanNs(v)
					}
					if v, ok := s.Metrics["delivered_fraction@8"].(float64); ok {
						at8 = evalPercent(v)
					}
					if v, ok := s.Metrics["delivered_fraction_least"].(float64); ok {
						least = evalPercent(v)
					}
					rows = append(rows, []string{s.Impl, c, at8, least})
				}
				mdTable(&b, []string{"Implementation", "Publish cost, 8 subscribers", "Delivered, 8 subscribers", "Delivered, least over all cells"}, rows)
				continue
			}
			missingAll := []string{}
			for _, d := range dimOrder {
				n := 0
				for _, s := range list {
					for _, m := range s.DimsMissing {
						if m == d {
							n++
						}
					}
				}
				if n == len(list) {
					missingAll = append(missingAll, d)
				}
			}
			fmt.Fprintf(&b, "The score is a weighted mean of percentile ranks within this %s, so it says where an implementation stands among these subjects on this machine and nothing about absolute quality.", among)
			if len(missingAll) > 0 {
				fmt.Fprintf(&b, " No battery in this family produces data for: %s. Those dimensions are left out.", strings.Join(missingAll, ", "))
			}
			uniform := list[0].DimsUniform
			if len(uniform) > 0 {
				fmt.Fprintf(&b, " Every implementation of this %s has the same value for: %s. That distinguishes none of them, so it is left out of the score and shown as \"tied\".", among, strings.Join(uniform, ", "))
			}
			b.WriteString("\n\n")
			verdicts := map[string]int{}
			order := []string{}
			for _, s := range list {
				if verdicts[s.Verdict] == 0 {
					order = append(order, s.Verdict)
				}
				verdicts[s.Verdict]++
			}
			rows = rows[:0]
			for _, v := range order {
				rows = append(rows, []string{v, strconv.Itoa(verdicts[v])})
			}
			mdTable(&b, []string{"Verdict", "Implementations"}, rows)

			header := []string{"Rank", "Implementation", "Score"}
			header = append(header, dimOrder...)
			if classed {
				header = append(header, "Publish cost, 8 subscribers", "Delivered, 8 subscribers", "Delivered, least over all cells")
			}
			row := func(i int) []string {
				s := list[i]
				r := []string{strconv.Itoa(rank[i]), s.Impl, strconv.Itoa(s.Score)}
				for _, d := range dimOrder {
					cell := strconv.Itoa(s.Dims[d])
					for _, m := range s.DimsMissing {
						if m == d {
							cell = "no data"
						}
					}
					for _, m := range s.DimsUniform {
						if m == d {
							cell = "tied"
						}
					}
					r = append(r, cell)
				}
				if classed {
					cost, at8, least := "no record", "no record", "no record"
					if v, ok := s.Metrics["publish_ns_op@8"].(float64); ok {
						cost = humanNs(v)
					}
					if v, ok := s.Metrics["delivered_fraction@8"].(float64); ok {
						at8 = evalPercent(v)
					}
					if v, ok := s.Metrics["delivered_fraction_least"].(float64); ok {
						least = evalPercent(v)
					}
					r = append(r, cost, at8, least)
				}
				return r
			}
			rows = rows[:0]
			for i := 0; i < len(list) && i < 10; i++ {
				rows = append(rows, row(i))
			}
			fmt.Fprintf(&b, "Highest %d scores:\n\n", len(rows))
			mdTable(&b, header, rows)
			rows = rows[:0]
			for i := range list {
				if list[i].Baseline {
					rows = append(rows, row(i))
				}
			}
			if len(rows) > 0 {
				b.WriteString("The baselines:\n\n")
				mdTable(&b, header, rows)
			}
		}
	}

	// --- Scenarios against their baselines ---
	evalScenarioBaselines(&b, in, families)

	// --- Unranked families ---
	// A family whose measurement is not valid under the run's record contract
	// is named, with the reason, and its figures are not printed: a table of
	// them would be read as a comparison.
	small := []string{}
	invalid := map[string][]string{} // reason -> families
	reasons := []string{}
	for _, f := range families {
		reason, no := dd.Unranked[f]
		if !no {
			continue
		}
		if dd.UnrankedSince[f] == "" {
			small = append(small, f)
			continue
		}
		if invalid[reason] == nil {
			reasons = append(reasons, reason)
		}
		invalid[reason] = append(invalid[reason], f)
	}
	for _, reason := range reasons {
		list := invalid[reason]
		counts := make([]string, len(list))
		for i, f := range list {
			counts[i] = fmt.Sprintf("%s (%d)", f, len(dd.Scores[f]))
		}
		if len(list) == 1 {
			fmt.Fprintf(&b, "## Family %s: not ranked in this run\n\n", list[0])
		} else {
			fmt.Fprintf(&b, "## Families %s: not ranked in this run\n\n", strings.Join(list, ", "))
		}
		fmt.Fprintf(&b, "Implementations measured: %s. No score or comparison is given, because %s. The records are in the results file; a run under record contract %s or later measures this properly.\n\n", strings.Join(counts, ", "), reason, dd.UnrankedSince[list[0]])
	}
	if len(small) > 0 {
		b.WriteString("## Families that are not ranked\n\n")
		fmt.Fprintf(&b, "These get no score, for one of two reasons. A family of fewer than %d: a percentile rank among two to four subjects only restates their order. A family whose members the measurement does not separate: when the 95 percent intervals of most of them overlap, their order changes from run to run, and a rank would report the noise. The reason is given under each. The measurements follow, as mean and 95 percent confidence half-width over the repetitions.\n\n", minRankedImpls)
		for _, f := range small {
			type col struct{ metric, dims string }
			cols := []col{}
			seen := map[col]bool{}
			cell := map[string]map[col]string{}
			for _, m := range data.metrics {
				if m["family"] != f {
					continue
				}
				if _, ok := m["mean"].(float64); !ok {
					continue
				}
				name, _ := m["metric"].(string)
				impl, _ := m["impl"].(string)
				c := col{name, evalDims(m)}
				if !seen[c] {
					seen[c] = true
					cols = append(cols, c)
				}
				if cell[impl] == nil {
					cell[impl] = map[col]string{}
				}
				cell[impl][c] = evalValue(m)
			}
			sort.Slice(cols, func(i, j int) bool {
				if cols[i].metric != cols[j].metric {
					return cols[i].metric < cols[j].metric
				}
				if len(cols[i].dims) != len(cols[j].dims) {
					return len(cols[i].dims) < len(cols[j].dims)
				}
				return cols[i].dims < cols[j].dims
			})
			impls := make([]string, 0, len(cell))
			for impl := range cell {
				impls = append(impls, impl)
			}
			sort.Strings(impls)
			fmt.Fprintf(&b, "### %s\n\n", f)
			fmt.Fprintf(&b, "Not ranked: %s.\n\n", dd.Unranked[f])
			header := append([]string{"Metric"}, impls...)
			rows = rows[:0]
			for _, c := range cols {
				label := "`" + c.metric + "`"
				if c.dims != "" {
					label += " (" + c.dims + ")"
				}
				r := []string{label}
				for _, impl := range impls {
					v := cell[impl][c]
					if v == "" {
						v = "not measured"
					}
					r = append(r, v)
				}
				rows = append(rows, r)
			}
			mdTable(&b, header, rows)
		}
	}

	// --- Stalled consumer ---
	b.WriteString("## One subscriber that never reads\n\n")
	b.WriteString("The publisher is given a fixed number of publishes and a deadline while one subscriber never receives. `completed` means every publish returned; `publisher_blocked` means the publisher was still blocked at the deadline, which is the specified behaviour of a lossless lock-step design and a defect in one that claims not to block.\n\n")
	type stallKey struct{ family, outcome string }
	stallCount := map[stallKey]int{}
	stallOps := map[stallKey][]float64{}
	outcomes := map[string]bool{}
	hasOps := false
	for _, m := range data.metrics {
		if m["metric"] != "stalled_consumer_ops_completed" {
			continue
		}
		fam, _ := m["family"].(string)
		outcome, _ := m["outcome"].(string)
		k := stallKey{fam, outcome}
		stallCount[k]++
		outcomes[outcome] = true
		if v, ok := m["ops_completed"].(float64); ok {
			stallOps[k] = append(stallOps[k], v)
			hasOps = true
		}
	}
	outcomeList := make([]string, 0, len(outcomes))
	for o := range outcomes {
		outcomeList = append(outcomeList, o)
	}
	sort.Strings(outcomeList)
	rows = rows[:0]
	for _, f := range families {
		for _, o := range outcomeList {
			k := stallKey{f, o}
			if stallCount[k] == 0 {
				continue
			}
			ops := "not recorded"
			if vals := stallOps[k]; len(vals) > 0 {
				sort.Float64s(vals)
				ops = fmt.Sprintf("%s / %s / %s", thousandsSep(vals[0]), thousandsSep(evalQuantile(vals, 0.5)), thousandsSep(vals[len(vals)-1]))
			}
			rows = append(rows, []string{f, o, strconv.Itoa(stallCount[k]), ops})
		}
	}
	mdTable(&b, []string{"Family", "Outcome", "Implementations", "Publishes completed by the deadline: least / median / most"}, rows)
	if !hasOps {
		b.WriteString("This results file does not record how many publishes completed, so the stall-resilience dimension of its scores is a constant.\n\n")
	}
	evalResidue(&b, data)

	// --- Skipped ---
	b.WriteString("## Skipped\n\n")
	if len(data.skipped) == 0 {
		b.WriteString("Nothing was skipped.\n\n")
	} else {
		// One skip record is written per repetition; identical ones are
		// reported once with their count.
		rows = rows[:0]
		at := map[string]int{}
		counts := []int{}
		for _, s := range data.skipped {
			row := []string{evalStr(s, "family"), evalStr(s, "impl"), evalStr(s, "battery"), evalStr(s, "reason")}
			key := strings.Join(row, "\x00")
			if i, ok := at[key]; ok {
				counts[i]++
				continue
			}
			at[key] = len(rows)
			rows = append(rows, row)
			counts = append(counts, 1)
		}
		for i := range rows {
			rows[i] = append(rows[i], strconv.Itoa(counts[i]))
		}
		fmt.Fprintf(&b, "%d skip records, %d distinct.\n\n", len(data.skipped), len(rows))
		mdTable(&b, []string{"Family", "Implementation", "Battery", "Reason recorded", "Records"}, rows)
	}

	// --- Noise ---
	b.WriteString("## Repeatability within the run\n\n")
	b.WriteString("Coefficient of variation (standard deviation over mean) across the repetitions of each sampled record, by metric. A difference between two implementations that is smaller than their confidence intervals in the metrics CSV is not established by this run.\n\n")
	cvs := map[string][]float64{}
	for _, m := range data.metrics {
		cv, ok := m["cv"].(float64)
		if !ok || math.IsNaN(cv) {
			continue
		}
		name, _ := m["metric"].(string)
		cvs[name] = append(cvs[name], cv)
	}
	names := make([]string, 0, len(cvs))
	for n := range cvs {
		names = append(names, n)
	}
	sort.Strings(names)
	rows = rows[:0]
	for _, n := range names {
		vals := cvs[n]
		sort.Float64s(vals)
		over := 0
		for _, v := range vals {
			if v > 0.10 {
				over++
			}
		}
		rows = append(rows, []string{"`" + n + "`", strconv.Itoa(len(vals)),
			fmt.Sprintf("%.1f%%", 100*evalQuantile(vals, 0.5)), fmt.Sprintf("%.1f%%", 100*evalQuantile(vals, 0.9)),
			fmt.Sprintf("%d (%.0f%%)", over, 100*float64(over)/float64(len(vals)))})
	}
	mdTable(&b, []string{"Metric", "Records", "Median CV", "90th percentile CV", "Records with CV above 10%"}, rows)

	evalAgreement(&b, in, families)

	// --- Limits ---
	b.WriteString("## What this run cannot show\n\n")
	fmt.Fprintf(&b, "- It is one run on one machine with %s logical CPUs at GOMAXPROCS %s. Nothing here describes behaviour with more parallelism, on another processor, or on another architecture.\n", evalStr(meta, "num_cpu"), evalStr(meta, "gomaxprocs"))
	fmt.Fprintf(&b, "- Profile `%s`, seed %s. Scores from different profiles, different record contracts or different families are not comparable.\n", evalStr(meta, "profile"), evalStr(meta, "seed"))
	b.WriteString("- A score ranks implementations against each other. It does not say that any of them is correct: correctness is what the gates above and the tests address.\n")
	b.WriteString("- The batteries measure the workloads they define (see `measurement.md`). A workload that is not among them was not measured.\n")
	return b.String()
}

// evalResidue lists the units that left a goroutine using a CPU in their
// measuring process: a publisher that waits for the stalled subscriber by
// spinning. A blocked publisher that is parked costs nothing and is not
// listed.
func evalResidue(b *strings.Builder, data *reportData) {
	if generator, _ := data.meta["generator_version"].(string); generatorBefore(generator, isolationContract) {
		return
	}
	type key struct{ family, impl string }
	worst := map[key]float64{}
	batteries := map[key][]string{}
	for _, r := range data.residue {
		cores, _ := r["cpu_cores"].(float64)
		if cores < 0.5 {
			continue
		}
		family, _ := r["family"].(string)
		impl, _ := r["impl"].(string)
		battery, _ := r["battery"].(string)
		k := key{family, impl}
		worst[k] = math.Max(worst[k], cores)
		batteries[k] = append(batteries[k], battery)
	}
	if len(worst) == 0 {
		b.WriteString("No battery left a goroutine using a CPU in its measuring process: every publisher that was still blocked was parked.\n\n")
		return
	}
	keys := make([]key, 0, len(worst))
	for k := range worst {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].family != keys[j].family {
			return keys[i].family < keys[j].family
		}
		return keys[i].impl < keys[j].impl
	})
	rows := [][]string{}
	for _, k := range keys {
		sort.Strings(batteries[k])
		rows = append(rows, []string{k.family, k.impl, strings.Join(batteries[k], ", "), fmt.Sprintf("%.2f", worst[k])})
	}
	b.WriteString("A blocked publisher is not always an idle one. After these batteries the measuring process was still using at least half a CPU with nothing left to do: the publisher waits for the subscriber that never reads by spinning, and goes on using a CPU until the process ends. Each battery runs in a process of its own, so this costs the other measurements nothing; in a program it would.\n\n")
	mdTable(b, []string{"Family", "Implementation", "Batteries that left it", "CPUs in use afterwards"}, rows)
}

// evalHeadline is the one number a family's implementations are compared on
// per scenario: the family's throughput metric at 8 on its sweep dimension.
var evalHeadline = map[string]string{"broadcast": "publish_ns_op", "payload": "publish_ns_op", "caster": "send_ns_op"}

// evalScenarioBaselines writes, for every scenario of a ranked family, how the
// implementations attached to it as an expected good fit compare with that
// scenario's baseline. The family score ranks everything in a family in one
// table, whatever each implementation promises; this is the like-for-like
// view. "Faster" and "slower" require non-overlapping 95 percent intervals.
func evalScenarioBaselines(b *strings.Builder, in evaluationInput, families []string) {
	asList := func(v any) []map[string]any {
		out := []map[string]any{}
		list, _ := v.([]any)
		for _, e := range list {
			if m, ok := e.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	}
	ranked := map[string]bool{}
	for _, f := range families {
		if _, no := in.derived.Unranked[f]; !no && evalHeadline[f] != "" {
			ranked[f] = true
		}
	}
	type key struct{ family, impl string }
	records := map[key]map[string]any{}
	for _, m := range in.data.metrics {
		family, _ := m["family"].(string)
		if ranked[family] && m["metric"] == evalHeadline[family] && dimsSubs(m) == 8 {
			impl, _ := m["impl"].(string)
			records[key{family, impl}] = m
		}
	}
	attached := map[string][]string{} // scenario -> expected-good-fit implementations
	for _, a := range asList(in.registry["attachments"]) {
		if good, _ := a["expected_good_fit"].(bool); good {
			id, _ := a["scenario"].(string)
			impl, _ := a["impl"].(string)
			attached[id] = append(attached[id], impl)
		}
	}
	bounds := func(m map[string]any) (mean, lo, hi float64, ok bool) {
		mean, ok = m["mean"].(float64)
		lo, okLo := m["ci95_low"].(float64)
		hi, okHi := m["ci95_high"].(float64)
		return mean, lo, hi, ok && okLo && okHi
	}
	rows := [][]string{}
	scenarios := asList(in.registry["scenarios"])
	sort.SliceStable(scenarios, func(i, j int) bool {
		ki, _ := scenarios[i]["kind"].(string)
		kj, _ := scenarios[j]["kind"].(string)
		if ki != kj {
			return ki < kj
		}
		return evalStr(scenarios[i], "id") < evalStr(scenarios[j], "id")
	})
	for _, s := range scenarios {
		family, _ := s["kind"].(string)
		if !ranked[family] {
			continue
		}
		id, baseline := evalStr(s, "id"), evalStr(s, "baseline")
		base, haveBase := records[key{family, baseline}]
		baseMean, baseLo, baseHi, okBase := 0.0, 0.0, 0.0, false
		if haveBase {
			baseMean, baseLo, baseHi, okBase = bounds(base)
		}
		type subject struct {
			impl string
			mean float64
		}
		subjects := []subject{}
		faster, slower, same := 0, 0, 0
		// Without a score for the baseline its class is not known, and
		// nothing is compared anyway: every measured attachment is listed.
		baseScore, haveBaseScore := in.derived.Scores[family][baseline]
		baseClass, otherClass := baseScore.Class, 0
		for _, impl := range attached[id] {
			if impl == baseline {
				continue
			}
			m, ok := records[key{family, impl}]
			if !ok {
				continue
			}
			if haveBaseScore && in.derived.Scores[family][impl].Class != baseClass {
				otherClass++
				continue
			}
			mean, lo, hi, okM := bounds(m)
			if !okM {
				continue
			}
			subjects = append(subjects, subject{impl, mean})
			if !okBase {
				continue
			}
			switch {
			case hi < baseLo:
				faster++
			case lo > baseHi:
				slower++
			default:
				same++
			}
		}
		sort.Slice(subjects, func(i, j int) bool {
			if subjects[i].mean != subjects[j].mean {
				return subjects[i].mean < subjects[j].mean
			}
			return subjects[i].impl < subjects[j].impl
		})
		fastest := []string{}
		for i := 0; i < len(subjects) && i < 3; i++ {
			fastest = append(fastest, fmt.Sprintf("%s (%s)", subjects[i].impl, humanNs(subjects[i].mean)))
		}
		delivers := "all (checked exactly-once)"
		if hasDeliveryClasses(family) {
			delivers = "no record"
			if f, ok := in.derived.Scores[family][baseline].Metrics["delivered_fraction@8"].(float64); ok {
				delivers = evalPercent(f)
			}
		}
		row := []string{"`" + id + "`", family, baseline, "no record", delivers, strconv.Itoa(len(subjects)), "", "", "", strconv.Itoa(otherClass), strings.Join(fastest, "; ")}
		if okBase {
			row[3] = fmt.Sprintf("%s (%s to %s)", humanNs(baseMean), humanNs(baseLo), humanNs(baseHi))
			row[6], row[7], row[8] = strconv.Itoa(faster), strconv.Itoa(same), strconv.Itoa(slower)
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return
	}
	b.WriteString("## Each scenario against its own baseline\n\n")
	b.WriteString("A score ranks every implementation of a family, or of a delivery class, in one table, whatever else it promises: a publisher that blocks behind a stalled subscriber loses the stall dimension even where blocking is the contract. This table narrows the comparison. For each scenario it takes the implementations attached to it as an expected good fit and compares each with that scenario's baseline on one number: the mean cost of one publish (or send) with 8 subscribers. Faster and slower mean the 95 percent confidence intervals do not overlap. An implementation is compared with the baseline only when it is in the baseline's delivery class: the cost of a publish that delivers is not set against the cost of one that drops. Those in the other class are counted and not compared.\n\n")
	b.WriteString("That number is the steady-state cost only. It does not time what distinguishes the scenario (churn, close, replay, a deadline); the tests exercise those and the batteries do not time them per scenario. An implementation attached to several scenarios appears in each.\n\n")
	mdTable(b, []string{"Scenario", "Family", "Baseline", "Baseline cost (95% interval)", "Baseline delivered", "Good-fit implementations in the baseline's class", "Faster", "Not distinguishable", "Slower", "In the other class, not compared", "Fastest three compared"}, rows)
}
