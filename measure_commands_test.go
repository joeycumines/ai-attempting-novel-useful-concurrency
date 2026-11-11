package bigbuff

// bigbuff-measure subcommands: list, dump-registry, run, verify.
// (report is implemented in measure_report_test.go.)

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

func measureBatteriesByID() map[string]measureBattery {
	m := make(map[string]measureBattery, len(measureBatteries))
	for _, b := range measureBatteries {
		m[b.ID] = b
	}
	return m
}

func cmdMeasureList(args []string) int {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fam := fs.String("family", "", "only list one family (broadcast|caster|scatter|gather|payload|fanin|exchange|barrier)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	sets := expectedImplSets()
	for _, set := range sets {
		if *fam != "" && set.family != *fam {
			continue
		}
		names := append([]string{}, set.names...)
		sort.Strings(names)
		fmt.Printf("%s: %d implementations\n", set.family, len(names))
		for _, n := range names {
			fmt.Printf("  %s\n", n)
		}
	}
	fmt.Printf("\nbatteries: %d\n", len(measureBatteries))
	for _, b := range measureBatteries {
		fmt.Printf("  %-36s %s\n", b.ID, b.Description)
	}
	fmt.Print("\nprofiles: quick (smoke), standard (default), full (exhaustive)\n")
	return 0
}

// scenarioKindName / lossPolicyName provide string spellings for the registry dump.
func scenarioKindName(k ScenarioKind) string {
	switch k {
	case KindBroadcast:
		return "broadcast"
	case KindCaster:
		return "caster"
	case KindScatter:
		return "scatter"
	case KindGather:
		return "gather"
	case KindPayload:
		return "payload"
	case KindFanin:
		return "fanin"
	case KindExchange:
		return "exchange"
	case KindBarrier:
		return "barrier"
	default:
		return fmt.Sprintf("unknown(%d)", int(k))
	}
}

func lossPolicyName(l LossPolicy) string {
	switch l {
	case Lossless:
		return "lossless"
	case CoalescingLatest:
		return "coalescing-latest"
	case LossyDetectable:
		return "lossy-detectable"
	case LossySnapToLatest:
		return "lossy-snap-to-latest"
	case ExactlyOne:
		return "exactly-one"
	case GatherAll:
		return "gather-all"
	default:
		return fmt.Sprintf("unknown(%d)", int(l))
	}
}

// registryDump renders the live init-validated registry as a JSON object.
// It is the exact same data the report joins against, so the report can show
// scenario context without duplicating the registry sources.
func registryDump() (map[string]any, error) {
	scenarios := []map[string]any{}
	for _, s := range Scenarios {
		scenarios = append(scenarios, map[string]any{
			"id":         string(s.ID),
			"title":      s.Title,
			"use_case":   s.UseCase,
			"tradeoffs":  s.Tradeoffs,
			"kind":       scenarioKindName(s.Kind),
			"loss":       lossPolicyName(s.Loss),
			"baseline":   s.Baseline,
			"invariants": s.Invariants,
		})
	}
	cases := []map[string]any{}
	for _, c := range Cases {
		cases = append(cases, map[string]any{
			"id":       string(c.ID),
			"scenario": string(c.Scenario),
			"kind":     string(c.Kind),
			"title":    c.Title,
			"runnable": c.Runnable,
		})
	}
	attachments := []map[string]any{}
	addAll := func(family string, list []Attachment) {
		for _, a := range list {
			attachments = append(attachments, map[string]any{
				"family":            family,
				"impl":              a.Impl,
				"scenario":          string(a.Scenario),
				"remarks":           a.Remarks,
				"expected_good_fit": a.ExpectedGoodFit,
				"exposure_limited":  a.ExposureLimited,
				"exposure":          a.Exposure,
			})
		}
	}
	addAll("broadcast", broadcastAttachments)
	addAll("caster", casterAttachments)
	addAll("scatter", scatterAttachments)
	addAll("gather", gatherAttachments)
	addAll("payload", payloadAttachments)
	addAll("fanin", faninAttachments)
	addAll("exchange", exchangeAttachments)
	addAll("barrier", barrierAttachments)
	// The coverage contract: which implementations a measurement run made
	// from this registry must have records (or reasoned skips) for. Recording
	// it in the dump makes every run group verifiable against the registry
	// that produced it, however the registry changes afterwards.
	measured := map[string]any{}
	for _, set := range expectedImplSets() {
		names := append([]string{}, set.names...)
		sort.Strings(names)
		measured[set.family] = names
	}
	// Every top-level runnable with the scenarios it bears on (see
	// scenario_runnables_test.go). Read from the source files, so the dump
	// must be made from the package directory.
	all, err := Runnables()
	if err != nil {
		return nil, err
	}
	runnables := []map[string]any{}
	for _, r := range all {
		rec := map[string]any{
			"name":      r.Name,
			"file":      r.File,
			"kind":      string(r.Kind),
			"class":     string(r.Class),
			"scenarios": r.Scenarios,
		}
		switch r.Class {
		case RunnableCase:
			rec["cases"] = r.Cases
		case RunnableUnit:
			rec["source"] = r.Source
			rec["implementations"] = r.Implementations
		case RunnableHarness:
			rec["reason"] = r.Reason
			rec["scenarios"] = []ScenarioID{}
		}
		runnables = append(runnables, rec)
	}
	return map[string]any{
		"type":        "registry",
		"schema":      measureSchemaVersion,
		"scenarios":   scenarios,
		"cases":       cases,
		"attachments": attachments,
		"measured":    measured,
		"runnables":   runnables,
	}, nil
}

// measureFamilyOrder is the order families are reported in.
var measureFamilyOrder = []string{"broadcast", "caster", "scatter", "gather", "payload", "fanin", "exchange", "barrier"}

// legacyMeasuredFamilies are the families that had measurement batteries
// before registry dumps recorded their coverage contract.
var legacyMeasuredFamilies = map[string]bool{"broadcast": true, "caster": true, "scatter": true, "gather": true, "payload": true, "fanin": true}

// measuredSetsOf returns the coverage contract recorded in a registry dump.
//
// Dumps written before the contract was recorded (no "measured" key) are read
// by the rule that held when they were written: only the six families in
// legacyMeasuredFamilies had batteries, and a family's measured
// implementations were the implementations attached to it, except the
// "Caster:"-prefixed broadcast attachments, which are caster-family
// implementations exercised through the broadcast scenarios and were never
// subjects of the broadcast batteries.
func measuredSetsOf(reg map[string]any) []expectedImplSet {
	byFamily := map[string][]string{}
	if m, ok := reg["measured"].(map[string]any); ok {
		for family, v := range m {
			list, _ := v.([]any)
			for _, n := range list {
				if name, ok := n.(string); ok {
					byFamily[family] = append(byFamily[family], name)
				}
			}
		}
	} else {
		seen := map[string]bool{}
		atts, _ := reg["attachments"].([]any)
		for _, a := range atts {
			att, _ := a.(map[string]any)
			family, _ := att["family"].(string)
			impl, _ := att["impl"].(string)
			if !legacyMeasuredFamilies[family] || impl == "" || strings.HasPrefix(impl, "Caster:") || seen[family+"\x00"+impl] {
				continue
			}
			seen[family+"\x00"+impl] = true
			byFamily[family] = append(byFamily[family], impl)
		}
	}
	sets := []expectedImplSet{}
	for _, family := range measureFamilyOrder {
		if names, ok := byFamily[family]; ok {
			sort.Strings(names)
			sets = append(sets, expectedImplSet{family, names})
			delete(byFamily, family)
		}
	}
	rest := make([]string, 0, len(byFamily))
	for family := range byFamily {
		rest = append(rest, family)
	}
	sort.Strings(rest)
	for _, family := range rest {
		sort.Strings(byFamily[family])
		sets = append(sets, expectedImplSet{family, byFamily[family]})
	}
	return sets
}

// readRegistryDump loads a registry dump file.
func readRegistryDump(path string) (map[string]any, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var reg map[string]any
	if err := json.Unmarshal(raw, &reg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return reg, nil
}

func cmdDumpRegistry(args []string) int {
	fs := flag.NewFlagSet("dump-registry", flag.ContinueOnError)
	out := fs.String("out", "registry.json", "output path for registry JSON (make passes results-NN-02-registry.json)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	dump, err := registryDump()
	if err != nil {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: dump-registry: %v\n", err)
		return 1
	}
	if dir := filepathDir(*out); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "bigbuff-measure: dump-registry: %v\n", err)
			return 1
		}
	}
	data, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: dump-registry: %v\n", err)
		return 1
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: dump-registry: %v\n", err)
		return 1
	}
	fmt.Printf("registry: %d scenarios, %d cases, %d attachments -> %s\n",
		len(Scenarios), len(Cases), len(attachmentsCount()), *out)
	return 0
}

func attachmentsCount() []Attachment {
	return allAttachments()
}

func filepathDir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return ""
}

func cmdMeasureRun(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	out := fs.String("out", "results.jsonl", "output path for JSONL results (make passes results-NN-01-results.jsonl)")
	profileName := fs.String("profile", "standard", "measurement profile: quick|standard|full")
	implGlob := fs.String("impl", "", "only measure implementations matching this glob (or substring)")
	batteryGlob := fs.String("battery", "", "only run batteries matching this glob (or substring), e.g. broadcast.*")
	seed := fs.Int64("seed", 1, "seed recorded in metadata (batteries are deterministic; the seed is informational)")
	noVerify := fs.Bool("no-verify", false, "skip post-run coverage verification")
	inProcess := fs.Bool("in-process", false, "measure everything in this one process (for debugging a battery; such a run does not verify)")
	unitFamily := fs.String("unit-family", "", "internal: the family of the one unit this process measures")
	unitImpl := fs.String("unit-impl", "", "internal: the implementation of the one unit this process measures")
	unitBattery := fs.String("unit-battery", "", "internal: the battery of the one unit this process measures")
	unitIndex := fs.Int("unit-index", -1, "internal: the position of that implementation in its family")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	profile, ok := profileByName(*profileName)
	if !ok {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: unknown profile %q (want quick|standard|full)\n", *profileName)
		return 2
	}
	if *unitFamily != "" {
		return runMeasureUnit(measureUnit{*unitFamily, *unitImpl, *unitBattery, *unitIndex}, profile, *seed, *out)
	}

	ctx := &measureContext{
		profile:     profile,
		implGlob:    *implGlob,
		batteryGlob: *batteryGlob,
		seed:        *seed,
		started:     time.Now(),
	}
	w, err := newMeasureWriter(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: run: %v\n", err)
		return 1
	}
	ctx.out = w
	isolation := measureIsolationPerUnit
	if *inProcess {
		isolation = "none"
	}
	ctx.write(measureMetaRecords(profile.Name, *seed, isolation))

	fmt.Fprintf(os.Stderr, "bigbuff-measure: profile=%s out=%s isolation=%s\n", profile.Name, *out, isolation)
	if *inProcess {
		for _, fam := range measureFamilies() {
			fam.run(ctx)
		}
	} else {
		runMeasureIsolated(ctx)
	}

	dur := time.Since(ctx.started)
	ctx.write(map[string]any{
		"type":        "summary",
		"schema":      measureSchemaVersion,
		"profile":     profile.Name,
		"duration_s":  dur.Seconds(),
		"records":     w.records + 1,
		"skipped":     ctx.skipped,
		"failures":    len(ctx.failures),
		"failure_msg": ctx.failures,
	})
	if err := w.close(); err != nil {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: run: close: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "bigbuff-measure: wrote %d records in %s (skipped=%d failures=%d)\n",
		w.records, dur.Round(time.Millisecond), ctx.skipped, len(ctx.failures))

	if len(ctx.failures) > 0 {
		return 1
	}
	if *noVerify {
		return 0
	}
	return verifyResults(*out, os.Stdout)
}

// verification scans a results file and checks coverage against a coverage
// contract (the measured sets of a registry): every expected implementation
// must have at least one record (metric or trace) or at least one skip with a
// reason. The contract is either the live registry's (verifyResults: "does
// this file cover what the code defines now?") or the one recorded in a
// registry dump (index: "is this run complete for the registry that produced
// it?"). The two questions have different answers as soon as the registry
// grows, and only the second is a property of a committed run.
type verification struct {
	perFamily  map[string]int
	expected   map[string]int
	skipped    map[string][]string
	measured   map[string][]string
	missing    map[string][]string
	badRecords int
	seenMeta   bool
}

func verifyResults(path string, out io.Writer) int {
	return verifyResultsAgainst(path, expectedImplSets(), "the live registry", out)
}

func verifyResultsAgainst(path string, sets []expectedImplSet, against string, out io.Writer) int {
	v := verification{
		perFamily: map[string]int{},
		expected:  map[string]int{},
		skipped:   map[string][]string{},
		measured:  map[string][]string{},
		missing:   map[string][]string{},
	}
	families := map[string]map[string]bool{}
	valueless := map[string]int{} // metric name -> records carrying no value
	generator, isolation := "", ""
	for _, set := range sets {
		v.expected[set.family] = len(set.names)
		families[set.family] = map[string]bool{}
		for _, n := range set.names {
			families[set.family][n] = false
		}
	}
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: %v\n", err)
		return 1
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			v.badRecords++
			continue
		}
		typ, _ := rec["type"].(string)
		if typ == "meta" {
			v.seenMeta = true
			generator, _ = rec["generator_version"].(string)
			isolation, _ = rec["isolation"].(string)
			continue
		}
		if typ == "metric" && !metricRecordHasValue(rec) {
			name, _ := rec["metric"].(string)
			valueless[name]++
		}
		family, _ := rec["family"].(string)
		impl, _ := rec["impl"].(string)
		known := families[family] != nil
		if !known {
			continue // registry record or family not under verification
		}
		v.perFamily[family]++
		switch typ {
		case "metric", "trace":
			if _, ok := families[family][impl]; ok {
				families[family][impl] = true
			}
		case "skipped":
			v.skipped[family] = append(v.skipped[family], impl+": "+fmt.Sprint(rec["reason"]))
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "verify: read: %v\n", err)
		return 1
	}
	fail := 0
	for family, impls := range families {
		for impl, seen := range impls {
			if seen {
				v.measured[family] = append(v.measured[family], impl)
				continue
			}
			// a skip record also satisfies coverage, provided it carries a reason
			hasSkip := false
			for _, s := range v.skipped[family] {
				if strings.HasPrefix(s, impl+": ") {
					hasSkip = true
					break
				}
			}
			if !hasSkip {
				v.missing[family] = append(v.missing[family], impl)
			}
		}
		sort.Strings(v.measured[family])
		sort.Strings(v.missing[family])
	}
	fmt.Fprintf(out, "verification of %s against %s\n", path, against)
	fmt.Fprintf(out, "  meta record present: %v; malformed records: %d\n", v.seenMeta, v.badRecords)
	if !v.seenMeta {
		fail++
	}
	if v.badRecords > 0 {
		fail++
	}
	if generator != "" && !generatorBefore(generator, isolationContract) && isolation != measureIsolationPerUnit {
		// A run measured in one process is not a run: what one unit leaves
		// running is still running while the next is timed (see
		// measure_isolate_test.go). Runs older than the contract are what
		// they are, and the reports do not rank them.
		fmt.Fprintf(out, "  not isolated: the meta record says isolation=%q, and record contract %s requires %q\n", isolation, generator, measureIsolationPerUnit)
		fail++
	}
	if len(valueless) > 0 {
		names := make([]string, 0, len(valueless))
		for name, n := range valueless {
			names = append(names, fmt.Sprintf("%s (%d)", name, n))
		}
		sort.Strings(names)
		if generator == "1.0.0" {
			// Known defect of the 1.0.0 record contract; reported, and not a
			// failure, so that the committed runs keep verifying as what
			// they are.
			fmt.Fprintf(out, "  note: metric records without a value (generator 1.0.0): %s\n", strings.Join(names, ", "))
		} else {
			fmt.Fprintf(out, "  metric records without a value: %s\n", strings.Join(names, ", "))
			fail++
		}
	}
	for _, set := range sets {
		family := set.family
		miss := v.missing[family]
		meas := len(v.measured[family])
		fmt.Fprintf(out, "  %-9s expected=%d measured=%d skipped=%d missing=%d\n",
			family, v.expected[family], meas, len(v.skipped[family]), len(miss))
		if len(miss) > 0 {
			fmt.Fprintf(out, "    missing: %s\n", strings.Join(miss, ", "))
			fail++
		}
	}
	if fail > 0 {
		fmt.Fprintf(out, "verify: FAIL (%d problem(s))\n", fail)
		return 1
	}
	fmt.Fprintf(out, "verify: PASS\n")
	return 0
}

// metricRecordHasValue reports whether a metric record carries the number it
// is named for: a mean (sampled metrics), ops_completed (stalled-consumer
// probes) or violations with checked (torn-read probes).
func metricRecordHasValue(rec map[string]any) bool {
	if _, ok := rec["mean"].(float64); ok {
		return true
	}
	if _, ok := rec["ops_completed"].(float64); ok {
		return true
	}
	_, hasViolations := rec["violations"].(float64)
	_, hasChecked := rec["checked"].(float64)
	return hasViolations && hasChecked
}

func cmdMeasureVerify(args []string) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	in := fs.String("in", "", "results file to verify (default: newest results-*-01-results.jsonl)")
	registryPath := fs.String("registry", "", "verify against this registry dump instead of the live registry")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *in == "" {
		*in = latestRunResultsJSONL(".")
		if *in == "" {
			fmt.Fprintf(os.Stderr, "verify: no results-*-01-results.jsonl found; pass --in explicitly\n")
			return 2
		}
		fmt.Fprintf(os.Stderr, "verify: defaulting to newest results file: %s\n", *in)
	}
	if *registryPath != "" {
		reg, err := readRegistryDump(*registryPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "verify: %v\n", err)
			return 1
		}
		return verifyResultsAgainst(*in, measuredSetsOf(reg), *registryPath, os.Stdout)
	}
	return verifyResults(*in, os.Stdout)
}

// --- bigbuff-measure index: flat-tree run manifests ---

// Run-group file grammar (repository root stays flat):
//
//	results-<rr>-<ss>-<slug>.<ext>
//
//	rr  run sequence, zero-padded (00 = documentation group, 01.. = runs)
//	ss  slot within the run, zero-padded (00 = manifest, 01 = JSONL results,
//	    02 = registry dump, 03 = HTML report, 04 = metrics CSV, 05 = scores CSV,
//	    06 = gates record, 07 = generated evaluation)
//
// Enumerating a directory listing of results-*-*-* and iterating it in sorted
// order yields, per run, the manifest first and then its artifacts; the run
// consists of exactly the files sharing its rr segment.

var runFileRe = regexp.MustCompile(`^results-([0-9]{2})-([0-9]{2})-[^/]+$`)

func cmdMeasureIndex(args []string) int {
	fs := flag.NewFlagSet("index", flag.ContinueOnError)
	dir := fs.String("dir", ".", "directory holding the flat results-*-*-* files")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	entries, err := os.ReadDir(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: index: %v\n", err)
		return 1
	}
	type member struct {
		path, name, slot string
		run              int
	}
	runs := map[int][]member{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		m := runFileRe.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		rr, _ := strconv.Atoi(m[1])
		if rr == 0 {
			continue // documentation group (e.g. results-00-00-readme.md)
		}
		runs[rr] = append(runs[rr], member{path: filepath.Join(*dir, name), name: name, slot: m[2], run: rr})
	}
	runNums := make([]int, 0, len(runs))
	for rr := range runs {
		runNums = append(runNums, rr)
	}
	sort.Ints(runNums)
	if len(runNums) == 0 {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: index: no results-<rr>-<ss>-* files found under %s\n", *dir)
		return 1
	}
	hostname, _ := os.Hostname()
	anyFailed := false
	// A run is verified against the registry that produced it: the dump in its
	// own slot 02. A run group that carries no dump is verified against the
	// nearest earlier run's dump, and its manifest says so. Only when no dump
	// exists at or before a run is the live registry used.
	snapshotSets := expectedImplSets()
	snapshotName := "live registry (no registry dump at or before this run)"
	for _, rr := range runNums {
		members := runs[rr]
		sort.Slice(members, func(i, j int) bool { return members[i].name < members[j].name })
		ownSnapshot := false
		for _, m := range members {
			if m.slot != "00" && strings.HasSuffix(m.name, "registry.json") {
				reg, err := readRegistryDump(m.path)
				if err != nil {
					fmt.Fprintf(os.Stderr, "bigbuff-measure: index: %v\n", err)
					return 1
				}
				snapshotSets, snapshotName, ownSnapshot = measuredSetsOf(reg), m.name, true
			}
		}
		verifiedAgainst := snapshotName
		if !ownSnapshot && strings.HasSuffix(snapshotName, "registry.json") {
			verifiedAgainst = snapshotName + " (nearest earlier registry dump; this run group has none)"
		}
		// Guard the reserved slot: anything other than the manifest occupying
		// slot 00 is a naming error, reported loudly.
		for _, m := range members {
			if m.slot == "00" && m.name != fmt.Sprintf("results-%02d-00-manifest.json", rr) {
				fmt.Fprintf(os.Stderr, "bigbuff-measure: index: run %02d: slot 00 is reserved for the manifest, found %s\n", rr, m.name)
				return 1
			}
		}
		files := []map[string]any{}
		fingerprint := map[string]any{}
		type summaryT struct {
			records   any
			failures  float64
			skipped   float64
			duration  any
			failureMu any
		}
		var totalSummary *summaryT
		allVerified := true
		hasJSONL := false
		for _, m := range members {
			if m.slot == "00" {
				continue // the manifest does not index itself
			}
			raw, err := os.ReadFile(m.path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "bigbuff-measure: index: %v\n", err)
				return 1
			}
			sum := sha256.Sum256(raw)
			e := map[string]any{
				"path":   m.name,
				"slot":   m.slot,
				"sha256": hex.EncodeToString(sum[:]),
				"bytes":  len(raw),
			}
			switch {
			case strings.HasSuffix(m.name, ".jsonl"):
				hasJSONL = true
				counts, meta, summary := indexJSONL(m.path)
				if counts != nil {
					e["records"] = counts
				}
				if meta != nil {
					for _, k := range []string{"profile", "seed", "timestamp", "commit", "go", "goos", "goarch", "num_cpu", "gomaxprocs", "cpu", "generator_version"} {
						if v, ok := meta[k]; ok {
							e[k] = v
							fingerprint[k] = v
						}
					}
				}
				if summary != nil {
					e["summary"] = map[string]any{
						"records":     summary["records"],
						"failures":    summary["failures"],
						"skipped":     summary["skipped"],
						"duration_s":  summary["duration_s"],
						"failure_msg": summary["failure_msg"],
					}
					if f, ok := summary["failures"].(float64); ok && totalSummary == nil {
						totalSummary = &summaryT{records: summary["records"], failures: f, duration: summary["duration_s"], failureMu: summary["failure_msg"]}
						if s, ok := summary["skipped"].(float64); ok {
							totalSummary.skipped = s
						}
					}
				}
				e["verified_against"] = verifiedAgainst
				if verifyResultsAgainst(m.path, snapshotSets, verifiedAgainst, io.Discard) == 0 {
					e["verified"] = true
				} else {
					e["verified"] = false
					allVerified = false
					anyFailed = true
					fmt.Fprintf(os.Stderr, "bigbuff-measure: index: run %02d: %s FAILS verification:\n", rr, m.name)
					verifyResultsAgainst(m.path, snapshotSets, verifiedAgainst, os.Stderr)
				}
			case strings.HasSuffix(m.name, ".html"):
				e["generator"] = "bigbuff-measure report"
				e["kind"] = "interactive-report"
			case strings.HasSuffix(m.name, "registry.json"):
				e["generator"] = "bigbuff-measure dump-registry"
				e["kind"] = "registry-dump"
			case strings.HasSuffix(m.name, ".csv"):
				e["generator"] = "bigbuff-measure report"
				e["kind"] = "csv-export"
			case strings.HasSuffix(m.name, "gates.json"):
				e["generator"] = "bigbuff-measure gates"
				e["kind"] = "gates-record"
			case strings.HasSuffix(m.name, "evaluation.md"):
				e["generator"] = "bigbuff-measure evaluate"
				e["kind"] = "evaluation"
			case strings.HasSuffix(m.name, ".md"):
				e["kind"] = "documentation"
			default:
				e["kind"] = "artifact"
			}
			files = append(files, e)
		}
		manifest := map[string]any{
			"type":              "manifest",
			"schema":            measureSchemaVersion,
			"generator":         "bigbuff-measure",
			"generator_version": "1.0.0",
			"generated":         time.Now().UTC().Format(time.RFC3339Nano),
			"indexed_hostname":  hostname,
			"prefix":            "results",
			"run":               rr,
			"files":             files,
		}
		for k, v := range fingerprint {
			manifest[k] = v
		}
		if totalSummary != nil {
			manifest["duration_s"] = totalSummary.duration
			manifest["failures"] = totalSummary.failures
			manifest["skipped"] = totalSummary.skipped
		}
		if hasJSONL {
			manifest["verified"] = allVerified
			manifest["verified_against"] = verifiedAgainst
		}
		data, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "bigbuff-measure: index: marshal run %02d: %v\n", rr, err)
			return 1
		}
		outPath := filepath.Join(*dir, fmt.Sprintf("results-%02d-00-manifest.json", rr))
		// The index is idempotent: a manifest whose content would change only
		// in when and where it was generated is left untouched, so indexing a
		// tree that is already indexed never dirties it.
		action := "wrote"
		if sameManifest(outPath, data) {
			action = "unchanged"
		} else if err := os.WriteFile(outPath, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "bigbuff-measure: index: %v\n", err)
			return 1
		}
		fmt.Fprintf(os.Stderr, "bigbuff-measure: index: run %02d: %s %s (%d files, verified=%v against %s)\n", rr, action, filepath.Base(outPath), len(files), manifest["verified"], verifiedAgainst)
	}
	// Informational: whether the newest run still covers the live registry.
	// This is a statement about the working tree, not about any committed
	// run, so it is reported and never written to a manifest.
	if newest := latestRunResultsJSONL(*dir); newest != "" {
		if verifyResults(newest, io.Discard) == 0 {
			fmt.Fprintf(os.Stderr, "bigbuff-measure: index: %s covers the live registry\n", filepath.Base(newest))
		} else {
			fmt.Fprintf(os.Stderr, "bigbuff-measure: index: note: %s does not cover the live registry (implementations or families were added since); 'make measure' produces a run that does\n", filepath.Base(newest))
		}
	}
	if anyFailed {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: index: FAIL: at least one run is incomplete for the registry that produced it\n")
		return 1
	}
	return 0
}

// sameManifest reports whether the manifest at path equals data once the
// fields that record when and where indexing happened are ignored.
func sameManifest(path string, data []byte) bool {
	old, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var a, b map[string]any
	if json.Unmarshal(old, &a) != nil || json.Unmarshal(data, &b) != nil {
		return false
	}
	for _, m := range []map[string]any{a, b} {
		delete(m, "generated")
		delete(m, "indexed_hostname")
	}
	return reflect.DeepEqual(a, b)
}

// latestRunResultsJSONL returns the newest (highest run sequence) results
// JSONL under dir, or "" if none exists. Used by verify/report defaults.
func latestRunResultsJSONL(dir string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	best := -1
	bestName := ""
	for _, e := range entries {
		m := runFileRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		if !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		rr, _ := strconv.Atoi(m[1])
		if rr > best {
			best, bestName = rr, e.Name()
		}
	}
	if bestName == "" {
		return ""
	}
	return filepath.Join(dir, bestName)
}

func indexJSONL(path string) (map[string]int, map[string]any, map[string]any) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, nil
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 256<<20)
	counts := map[string]int{}
	var meta, summary map[string]any
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			counts["malformed"]++
			continue
		}
		t, _ := rec["type"].(string)
		counts[t]++
		switch t {
		case "meta":
			meta = rec
		case "summary":
			summary = rec
		}
	}
	if err := sc.Err(); err != nil {
		counts["read_error"]++
	}
	return counts, meta, summary
}

// registryDumpForResults returns the registry dump a results file is read
// against when none is named: the dump in its own run group (slot 02), else
// the dump of the nearest earlier run group, with a note saying so. It
// returns "" when the file is not part of the run log or no dump exists at or
// before its run. `index` applies the same rule when it verifies.
func registryDumpForResults(resultsPath string) (path, note string) {
	dir, base := filepath.Dir(resultsPath), filepath.Base(resultsPath)
	m := regexp.MustCompile(`^results-([0-9]{2})-01-results\.jsonl$`).FindStringSubmatch(base)
	if m == nil {
		return "", ""
	}
	run, _ := strconv.Atoi(m[1])
	for rr := run; rr >= 1; rr-- {
		p := filepath.Join(dir, fmt.Sprintf("results-%02d-02-registry.json", rr))
		if fileExists(p) {
			if rr != run {
				note = " (nearest earlier registry dump; this run group has none)"
			}
			return p, note
		}
	}
	return "", ""
}
