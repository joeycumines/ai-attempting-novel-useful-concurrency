package bigbuff

// bigbuff-measure subcommands: list, dump-registry, run, verify.
// (report is implemented in measure_report_test.go.)

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
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
	fam := fs.String("family", "", "only list one family (broadcast|caster|scatter|gather|payload)")
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
	return map[string]any{
		"type":        "registry",
		"schema":      measureSchemaVersion,
		"scenarios":   scenarios,
		"cases":       cases,
		"attachments": attachments,
	}, nil
}

func cmdDumpRegistry(args []string) int {
	fs := flag.NewFlagSet("dump-registry", flag.ContinueOnError)
	out := fs.String("out", "results/registry.json", "output path for registry JSON")
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
	out := fs.String("out", "results/measurements.jsonl", "output path for JSONL results")
	profileName := fs.String("profile", "standard", "measurement profile: quick|standard|full")
	implGlob := fs.String("impl", "", "only measure implementations matching this glob (or substring)")
	batteryGlob := fs.String("battery", "", "only run batteries matching this glob (or substring), e.g. broadcast.*")
	seed := fs.Int64("seed", 1, "seed recorded in metadata (batteries are deterministic; the seed is informational)")
	noVerify := fs.Bool("no-verify", false, "skip post-run coverage verification")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	profile, ok := profileByName(*profileName)
	if !ok {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: unknown profile %q (want quick|standard|full)\n", *profileName)
		return 2
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
	ctx.write(measureMetaRecords(profile.Name, *seed))

	fmt.Fprintf(os.Stderr, "bigbuff-measure: profile=%s out=%s\n", profile.Name, *out)
	runBroadcastBatteries(ctx)
	runCasterBatteries(ctx)
	runScatterBatteries(ctx)
	runGatherBatteries(ctx)
	runPayloadBatteries(ctx)

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

	if *noVerify {
		return 0
	}
	return verifyResults(*out, os.Stdout)
}

// verification scans a results file and checks coverage against the live
// registry: every expected implementation must have at least one record
// (metric or trace) or at least one skip with a reason.
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
	v := verification{
		perFamily: map[string]int{},
		expected:  map[string]int{},
		skipped:   map[string][]string{},
		measured:  map[string][]string{},
		missing:   map[string][]string{},
	}
	families := map[string]map[string]bool{}
	for _, set := range expectedImplSets() {
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
			continue
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
	fmt.Fprintf(out, "verification of %s\n", path)
	fmt.Fprintf(out, "  meta record present: %v; malformed records: %d\n", v.seenMeta, v.badRecords)
	if !v.seenMeta {
		fail++
	}
	if v.badRecords > 0 {
		fail++
	}
	for _, family := range []string{"broadcast", "caster", "scatter", "gather", "payload"} {
		miss := v.missing[family]
		meas := len(v.measured[family])
		skipN := len(v.skipped[family])
		_ = skipN
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

func cmdMeasureVerify(args []string) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	in := fs.String("in", "results/measurements.jsonl", "results file to verify")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	return verifyResults(*in, os.Stdout)
}
