package bigbuff

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

// readMeasureRecords returns the records of a results file by type.
func readMeasureRecords(t *testing.T, path string) map[string][]map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string][]map[string]any{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("malformed record %q: %v", sc.Text(), err)
		}
		typ, _ := rec["type"].(string)
		out[typ] = append(out[typ], rec)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// The plan is the run: every battery of every implementation of every family
// is one unit, and nothing is in it twice.
func TestMeasurePlanCoversEveryUnit(t *testing.T) {
	plan := measurePlan(&measureContext{})
	seen := map[measureUnit]bool{}
	perFamily := map[string]map[string]bool{}
	batteries := map[string]bool{}
	for _, u := range plan {
		key := measureUnit{u.family, u.impl, u.battery, 0}
		if seen[key] {
			t.Errorf("unit planned twice: %s", u)
		}
		seen[key] = true
		if !strings.HasPrefix(u.battery, u.family+".") {
			t.Errorf("battery %s planned for family %s", u.battery, u.family)
		}
		if perFamily[u.family] == nil {
			perFamily[u.family] = map[string]bool{}
		}
		perFamily[u.family][u.impl] = true
		batteries[u.battery] = true
	}
	for _, b := range measureBatteries {
		if !batteries[b.ID] {
			t.Errorf("battery %s is in no unit", b.ID)
		}
	}
	for _, set := range expectedImplSets() {
		for _, name := range set.names {
			if !perFamily[set.family][name] {
				t.Errorf("%s implementation %q is in no unit", set.family, name)
			}
			for _, b := range measureBatteries {
				if strings.HasPrefix(b.ID, set.family+".") && !seen[measureUnit{set.family, name, b.ID, 0}] {
					t.Errorf("no unit for %s of %q", b.ID, name)
				}
			}
		}
		if got, want := len(perFamily[set.family]), len(set.names); got != want {
			t.Errorf("family %s: %d implementations planned, %d expected by verify", set.family, got, want)
		}
	}
	// The globs narrow the plan as they narrow a run.
	narrow := measurePlan(&measureContext{implGlob: "BaselineFanout (standard)", batteryGlob: "broadcast.c*"})
	if len(narrow) != 1 || narrow[0].battery != "broadcast.churn" || narrow[0].impl != "BaselineFanout (standard)" {
		t.Errorf("narrowed plan = %v", narrow)
	}
	// A broadcast unit carries the position of its factory, and that factory
	// is the one with the unit's name.
	factories := AllBroadcasters()
	for _, u := range plan {
		if u.family != "broadcast" || u.battery != "broadcast.throughput" {
			continue
		}
		p := factories[u.index]()
		if p.Name() != u.impl {
			t.Errorf("unit %s has index %d, which is %q", u, u.index, p.Name())
		}
		closeWithWatchdog(p, 2*time.Second)
	}
}

// What a unit leaves running stays in the unit's process. The stalled-consumer
// probe of EpochCaster (13-OpenRouter) ends with its publisher spinning; run
// in this process it would use a CPU until the test binary exits, which is
// what it did to every run before record contract 1.3.0.
func TestMeasureRunIsolatesEachUnit(t *testing.T) {
	if testing.Short() {
		t.Skip("starts measuring processes")
	}
	const impl = "EpochCaster (13-OpenRouter)"
	out := filepath.Join(t.TempDir(), "results.jsonl")
	before := runtime.NumGoroutine()
	if rc := cmdMeasureRun([]string{"--profile", "quick", "--impl", impl, "--battery", "broadcast.stalled_consumer", "--no-verify", "--out", out}); rc != 0 {
		t.Fatalf("run exited %d", rc)
	}
	for i := 0; i < 200 && runtime.NumGoroutine() > before; i++ {
		time.Sleep(time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("the run left %d goroutine(s) in the parent process", after-before)
	}
	recs := readMeasureRecords(t, out)
	if n := len(recs["meta"]); n != 1 || recs["meta"][0]["isolation"] != measureIsolationPerUnit || recs["meta"][0]["generator_version"] != measureGeneratorVersion {
		t.Errorf("meta records: %v", recs["meta"])
	}
	if n := len(recs["unit_summary"]); n != 0 {
		t.Errorf("%d unit summaries reached the results file; they are for the parent only", n)
	}
	if n := len(recs["summary"]); n != 1 || recs["summary"][0]["failures"] != 0.0 {
		t.Errorf("summary records: %v", recs["summary"])
	}
	blocked := false
	for _, m := range recs["metric"] {
		if m["impl"] != impl {
			t.Errorf("record of another implementation: %v", m["impl"])
		}
		if m["metric"] == "stalled_consumer_ops_completed" && m["outcome"] == "publisher_blocked" {
			blocked = true
		}
	}
	if !blocked {
		t.Fatalf("no publisher_blocked record for %s: %v", impl, recs["metric"])
	}
	if len(recs["residue"]) != 1 {
		t.Fatalf("residue records: %v", recs["residue"])
	}
	res := recs["residue"][0]
	if res["impl"] != impl || res["battery"] != "broadcast.stalled_consumer" || res["goroutines"].(float64) < 1 {
		t.Errorf("residue record: %v", res)
	}
	if _, ok := processCPUTime(); ok {
		// A parked publisher uses no CPU; this one spins. The bound is loose
		// because the test may share the machine with others.
		if cores, _ := res["cpu_cores"].(float64); cores < 0.2 {
			t.Errorf("the blocked publisher used %.2f CPUs; it was expected to spin", cores)
		}
	}
}

// A unit that leaves nothing behind writes no residue record.
func TestMeasureRunRecordsNoResidueForACleanUnit(t *testing.T) {
	if testing.Short() {
		t.Skip("starts measuring processes")
	}
	out := filepath.Join(t.TempDir(), "results.jsonl")
	if rc := cmdMeasureRun([]string{"--profile", "quick", "--impl", "BaselineFanout (standard)", "--battery", "broadcast.zero_subscriber", "--no-verify", "--out", out}); rc != 0 {
		t.Fatalf("run exited %d", rc)
	}
	recs := readMeasureRecords(t, out)
	if len(recs["residue"]) != 0 {
		t.Errorf("residue records: %v", recs["residue"])
	}
	if len(recs["metric"]) != 1 || recs["metric"][0]["metric"] != "zero_subscriber_ns_op" {
		t.Errorf("metric records: %v", recs["metric"])
	}
}

// A measuring process that dies is a failure of the run, the run says which
// unit it was, and the units after it are still measured.
func TestMeasureRunReportsADeadMeasuringProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("starts measuring processes")
	}
	const impl = "BaselineFanout (standard)"
	t.Setenv(measureCrashUnitEnv, "broadcast/"+impl+"/broadcast.churn")
	out := filepath.Join(t.TempDir(), "results.jsonl")
	if rc := cmdMeasureRun([]string{"--profile", "quick", "--impl", impl, "--battery", "broadcast.[cz]*", "--no-verify", "--out", out}); rc == 0 {
		t.Fatalf("a run with a dead measuring process exited 0")
	}
	recs := readMeasureRecords(t, out)
	if len(recs["summary"]) != 1 {
		t.Fatalf("summary records: %v", recs["summary"])
	}
	sum := recs["summary"][0]
	msgs, _ := sum["failure_msg"].([]any)
	if sum["failures"] != 1.0 || len(msgs) != 1 || !strings.Contains(msgs[0].(string), "broadcast.churn of "+impl) || !strings.Contains(msgs[0].(string), measureCrashUnitEnv) {
		t.Errorf("summary: %v", sum)
	}
	after := false
	for _, m := range recs["metric"] {
		if m["metric"] == "churn_ns_op" {
			t.Errorf("the dead unit has a record: %v", m)
		}
		if m["metric"] == "zero_subscriber_ns_op" {
			after = true
		}
	}
	if !after {
		t.Errorf("the unit after the dead one was not measured: %v", recs["metric"])
	}
}

// A run that was not isolated does not verify from the contract that
// requires it on; older runs are what they are.
func TestVerifyRejectsARunThatWasNotIsolated(t *testing.T) {
	sets := []expectedImplSet{{"broadcast", []string{"A"}}}
	const record = `{"type":"metric","family":"broadcast","impl":"A","metric":"publish_ns_op","mean":12.5,"publishes":8,"receives":8}`
	verify := func(meta string) (int, string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "results-01-01-results.jsonl")
		if err := os.WriteFile(path, []byte(meta+"\n"+record+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		return verifyResultsAgainst(path, sets, "a test registry", &out), out.String()
	}
	for _, c := range []struct {
		name, meta string
		pass       bool
	}{
		{"current contract, isolated", `{"type":"meta","generator_version":"` + measureGeneratorVersion + `","isolation":"` + measureIsolationPerUnit + `"}`, true},
		{"current contract, one process", `{"type":"meta","generator_version":"` + measureGeneratorVersion + `","isolation":"none"}`, false},
		{"current contract, not stated", `{"type":"meta","generator_version":"` + measureGeneratorVersion + `"}`, false},
		{"contract before isolation", `{"type":"meta","generator_version":"1.2.0"}`, true},
	} {
		rc, out := verify(c.meta)
		if (rc == 0) != c.pass {
			t.Errorf("%s: rc=%d\n%s", c.name, rc, out)
		}
		if !c.pass && !strings.Contains(out, "not isolated") {
			t.Errorf("%s: the failure does not say why:\n%s", c.name, out)
		}
	}
}

// No family of a run made in one process is ranked, whatever its size, and
// the reason says from which contract on a run is valid.
func TestFamiliesOfUnisolatedRunsAreNotRanked(t *testing.T) {
	metrics := []map[string]any{}
	for i := range minRankedImpls + 1 {
		metrics = append(metrics, map[string]any{"type": "metric", "family": "scatter", "impl": "B" + string(rune('a'+i)),
			"metric": "dispatch_ns_op", "dims": map[string]any{"receivers": 8.0}, "mean": float64(10 + i)})
	}
	for i := range 2 {
		metrics = append(metrics, map[string]any{"type": "metric", "family": "gather", "impl": "G" + string(rune('a'+i)),
			"metric": "wait_ns_op", "dims": map[string]any{"senders": 8.0}, "mean": float64(10 + i)})
	}
	for generator, isolated := range map[string]bool{"1.0.0": false, "1.2.0": false, isolationContract: true, "1.4.0": true} {
		// The scatter family: it has no delivery classes, so isolation is
		// the only thing that decides whether a family of six is ranked.
		dd := deriveReport(&reportData{meta: map[string]any{"generator_version": generator}, metrics: metrics}, nil)
		b, g := dd.Scores["scatter"]["Ba"], dd.Scores["gather"]["Ga"]
		if b.Ranked != isolated {
			t.Errorf("generator %s: scatter ranked = %v, want %v", generator, b.Ranked, isolated)
		}
		if g.Ranked {
			t.Errorf("generator %s: a family of two is ranked", generator)
		}
		wantSmall := "Not ranked (family of 2)"
		if !isolated {
			wantSmall = "Not ranked (measurement not valid)"
			if b.Verdict != wantSmall || b.Score != 0 || !strings.Contains(b.UnrankedReason, "one process") {
				t.Errorf("generator %s: scatter score %+v", generator, b)
			}
			if dd.UnrankedSince["scatter"] != isolationContract || dd.UnrankedSince["gather"] != isolationContract {
				t.Errorf("generator %s: valid since %v", generator, dd.UnrankedSince)
			}
		} else if len(dd.UnrankedSince) != 0 {
			t.Errorf("generator %s: valid since %v, want none", generator, dd.UnrankedSince)
		}
		if g.Verdict != wantSmall {
			t.Errorf("generator %s: gather verdict %q, want %q", generator, g.Verdict, wantSmall)
		}
	}
}

// A repetition sized by the clock ends near its target whatever an operation
// costs, and however the cost changes while it runs.
func TestOpBudgetEndsNearItsTarget(t *testing.T) {
	const target = 20 * time.Millisecond
	run := func(minOps, maxOps int, cost func(i int) time.Duration) (int, time.Duration, time.Duration) {
		start := time.Now()
		budget := newOpBudget(target, minOps, maxOps)
		last := -1
		for from, to, ok := budget.next(); ok; from, to, ok = budget.next() {
			if from != last+1 || to <= from {
				t.Fatalf("range [%d,%d) after index %d", from, to, last)
			}
			for i := from; i < to; i++ {
				if d := cost(i); d > 0 {
					time.Sleep(d)
				}
			}
			last = to - 1
		}
		n, d := budget.done()
		if n != last+1 {
			t.Fatalf("done reports %d operations, %d were run", n, last+1)
		}
		return n, d, time.Since(start)
	}
	// Free operations: stops at maxOps.
	if n, _, _ := run(64, 100000, func(int) time.Duration { return 0 }); n < 64 || n > 100000 {
		t.Errorf("free operations: ran %d", n)
	}
	// Slow operations: the minimum count is honoured, then it stops.
	if n, d, _ := run(8, 1000000, func(int) time.Duration { return 2 * time.Millisecond }); n < 8 || n > 64 || d < target {
		t.Errorf("slow operations: ran %d in %s", n, d)
	}
	// Cheap at first, then 500 µs each: an estimate taken from the cheap
	// start would run all million. The budget must stop within a few targets.
	n, d, wall := run(64, 1000000, func(i int) time.Duration {
		if i < 3000 {
			return 0
		}
		return 500 * time.Microsecond
	})
	if d < target || wall > 2*time.Second {
		t.Errorf("cost jump: ran %d operations in %s (wall %s); the target is %s", n, d, wall, target)
	}
}

// "/" is an ordinary character in an implementation's name.
func TestMeasureGlobTreatsSlashAsOrdinary(t *testing.T) {
	for _, c := range []struct {
		pattern, name string
		want          bool
	}{
		{"*", "Fanout45ChatGPT/sync (45-ChatGPT)", true},
		{"Fanout45ChatGPT*", "Fanout45ChatGPT/sync (45-ChatGPT)", true},
		{"Fanout45ChatGPT/sync*", "Fanout45ChatGPT/sync (45-ChatGPT)", true},
		{"Fanout45ChatGPT/buffered*", "Fanout45ChatGPT/sync (45-ChatGPT)", false},
		{"*.throughput", "broadcast.throughput", true},
		{"*.throughput", "broadcast.latency", false},
		{"ChanPubSub (Baseline)", "ChanPubSub (Baseline)", true},
		{"[", "a[b", true}, // malformed: substring
		{"[", "ab", false},
	} {
		if got := measureGlob(c.pattern, c.name); got != c.want {
			t.Errorf("measureGlob(%q, %q) = %v, want %v", c.pattern, c.name, got, c.want)
		}
	}
	// The plan under --impl '*' is the whole plan.
	all := measurePlan(&measureContext{})
	star := measurePlan(&measureContext{implGlob: "*", batteryGlob: "*"})
	if len(star) != len(all) || len(all) == 0 {
		t.Errorf("--impl '*' --battery '*' plans %d units, no filter plans %d", len(star), len(all))
	}
}

// A unit that names nothing the family has is a failure, not an empty success.
func TestMeasureUnitThatMatchesNothingFails(t *testing.T) {
	for _, u := range []measureUnit{
		{family: "broadcast", impl: "NoSuchImplementation", battery: "broadcast.zero_subscriber", index: -1},
		{family: "broadcast", impl: "ChanPubSub (Baseline)", battery: "broadcast.no_such_battery", index: 0},
	} {
		out := filepath.Join(t.TempDir(), "unit.jsonl")
		quick, _ := profileByName("quick")
		if code := runMeasureUnit(u, quick, 1, out); code != 0 {
			t.Fatalf("%s: exit code %d; the failure belongs in the unit's summary", u, code)
		}
		recs := readMeasureRecords(t, out)
		if len(recs["unit_summary"]) != 1 {
			t.Fatalf("%s: %d summaries", u, len(recs["unit_summary"]))
		}
		msgs, _ := recs["unit_summary"][0]["failure_msg"].([]any)
		if len(msgs) != 1 || !strings.Contains(msgs[0].(string), "measured nothing") {
			t.Errorf("%s: failure messages %v, want one saying the unit measured nothing", u, msgs)
		}
	}
}

// The warm-up is sized by the clock too: it ends near an eighth of the target
// when an operation is slow, and at the cap when it is free.
func TestWarmOpsIsSizedByTheClock(t *testing.T) {
	const target = 80 * time.Millisecond
	start := time.Now()
	calls := 0
	n := warmOps(target, 1000000, func(i int) {
		if i != calls {
			t.Fatalf("operation %d ran as number %d", i, calls)
		}
		calls++
		time.Sleep(time.Millisecond)
	})
	if n != calls {
		t.Errorf("warmOps reports %d operations, %d ran", n, calls)
	}
	if n < opBudgetMinBatch || time.Since(start) > 2*time.Second {
		t.Errorf("slow operation: %d operations in %s; the warm-up budget is %s", n, time.Since(start), target/8)
	}
	if n := warmOps(target, 500, func(int) {}); n != 500 {
		t.Errorf("free operation: %d operations, want the cap of 500", n)
	}
}

// What a throughput record says about delivery.
func TestDeliveryFieldsRecordTheShareDelivered(t *testing.T) {
	m := map[string]any{}
	deliveryFields(m, 1000, 6000, 8)
	if m["publishes"] != int64(1000) || m["receives"] != int64(6000) || m["delivered_fraction"] != 0.75 {
		t.Errorf("1000 publishes to 8 subscribers, 6000 received: %v", m)
	}
	m = map[string]any{}
	deliveryFields(m, 1000, 8000, 8)
	if m["delivered_fraction"] != 1.0 {
		t.Errorf("everything delivered: %v", m)
	}
	// No publishes, or no subscribers: there is no share to state.
	for _, c := range [][3]int64{{0, 0, 8}, {1000, 0, 0}} {
		m = map[string]any{}
		deliveryFields(m, c[0], c[1], int(c[2]))
		if _, ok := m["delivered_fraction"]; ok {
			t.Errorf("publishes=%d subs=%d: a delivered fraction was recorded: %v", c[0], c[2], m)
		}
		if m["publishes"] != c[0] || m["receives"] != c[1] {
			t.Errorf("publishes=%d subs=%d: counts %v", c[0], c[2], m)
		}
	}
}

// The evaluation lists a unit that left a CPU in use, and not one that left a
// parked goroutine.
func TestEvaluationListsOnlyResidueThatUsesACPU(t *testing.T) {
	render := func(generator string, residue ...map[string]any) string {
		var b strings.Builder
		evalResidue(&b, &reportData{meta: map[string]any{"generator_version": generator}, residue: residue})
		return b.String()
	}
	spinning := map[string]any{"family": "broadcast", "impl": "Spinner", "battery": "broadcast.stalled_consumer", "goroutines": 2.0, "cpu_cores": 1.08}
	again := map[string]any{"family": "broadcast", "impl": "Spinner", "battery": "broadcast.trace_stalled_consumer", "goroutines": 2.0, "cpu_cores": 0.9}
	parked := map[string]any{"family": "broadcast", "impl": "Parker", "battery": "broadcast.stalled_consumer", "goroutines": 1.0, "cpu_cores": 0.0}
	unmeasured := map[string]any{"family": "caster", "impl": "Unknown", "battery": "caster.churn", "goroutines": 1.0}

	out := render(isolationContract, spinning, again, parked, unmeasured)
	if !strings.Contains(out, "| broadcast | Spinner | broadcast.stalled_consumer, broadcast.trace_stalled_consumer | 1.08 |") {
		t.Errorf("the spinning implementation is not listed once with both batteries and its worst figure:\n%s", out)
	}
	if strings.Contains(out, "Parker") || strings.Contains(out, "Unknown") {
		t.Errorf("a parked or unmeasured residue is listed:\n%s", out)
	}
	if out := render(isolationContract, parked); !strings.Contains(out, "No battery left a goroutine using a CPU") || strings.Contains(out, "Parker") {
		t.Errorf("only parked residue:\n%s", out)
	}
	// A run that was one process recorded no residue; the section says nothing.
	if out := render("1.2.0", spinning); out != "" {
		t.Errorf("a run before process isolation printed a residue section:\n%s", out)
	}
}

// A test binary built for another architecture runs under an emulator, and
// the processes it starts need the same one.
func TestMeasuringProcessesStartThroughTheNamedCommand(t *testing.T) {
	// An emulator, if this binary is running under one, has to stay in front
	// of the executable in the whole runs further down.
	outer := os.Getenv(measureExecEnv)
	t.Setenv(measureExecEnv, "")
	if got := measureChildCommand(context.Background(), "/x/bb.test", "-a", "b").Args; !reflect.DeepEqual(got, []string{"/x/bb.test", "-a", "b"}) {
		t.Errorf("with no command named: %q", got)
	}
	t.Setenv(measureExecEnv, "  /opt/qemu-aarch64   -cpu max ")
	cmd := measureChildCommand(context.Background(), "/x/bb.test", "-a", "b")
	if want := []string{"/opt/qemu-aarch64", "-cpu", "max", "/x/bb.test", "-a", "b"}; !reflect.DeepEqual(cmd.Args, want) || cmd.Path != "/opt/qemu-aarch64" {
		t.Errorf("through a command: path %q, args %q, want %q", cmd.Path, cmd.Args, want)
	}

	// And a whole run through one: `env` starts the executable it is given,
	// and a command that cannot start it fails every unit.
	env, err := exec.LookPath("env")
	if err != nil {
		t.Skip("no env command on this system")
	}
	const impl = "BaselineFanout (standard)"
	run := func(prefix string) (int, map[string][]map[string]any) {
		t.Setenv(measureExecEnv, prefix)
		out := filepath.Join(t.TempDir(), "results.jsonl")
		rc := cmdMeasureRun([]string{"--profile", "quick", "--impl", impl, "--battery", "broadcast.zero_subscriber", "--no-verify", "--out", out})
		return rc, readMeasureRecords(t, out)
	}
	if rc, recs := run(strings.TrimSpace(env + " " + outer)); rc != 0 || len(recs["metric"]) == 0 {
		t.Errorf("through env: exit %d, %d metric records", rc, len(recs["metric"]))
	}
	if rc, recs := run(env + " --no-such-option-of-env"); rc == 0 || len(recs["metric"]) != 0 {
		t.Errorf("through a command that fails: exit %d, %d metric records", rc, len(recs["metric"]))
	}
}

// The evaluation of a run says what the run before it said, when the two are
// the same measurement, and says that nothing was reproduced when they are
// not.
func TestEvaluationComparesWithThePreviousRun(t *testing.T) {
	// Six implementations, 100 to 600 ns; in the earlier run the fastest and
	// the slowest had each other's figures.
	run := func(generator string, swap bool) *reportData {
		data := &reportData{meta: map[string]any{"generator_version": generator, "profile": "standard", "commit": "abc"}}
		for i := 0; i < 6; i++ {
			j := i
			if swap && (i == 0 || i == 5) {
				j = 5 - i
			}
			mean := 100 * float64(j+1)
			data.metrics = append(data.metrics, map[string]any{"type": "metric", "family": "caster", "impl": fmt.Sprintf("C%d", i), "metric": "send_ns_op",
				"dims": map[string]any{"receivers": 8.0}, "mean": mean, "ci95_low": mean - 5, "ci95_high": mean + 5, "unit": "ns"})
		}
		return data
	}
	render := func(prev *evalPrevious) string {
		data := run(measureGeneratorVersion, false)
		page := renderEvaluation(evaluationInput{data: data, derived: deriveReport(data, nil), resultsName: "results-09-01-results.jsonl", registryName: "r.json", previous: prev})
		i := strings.Index(page, "## Agreement with the previous run")
		j := strings.Index(page, "## What this run cannot show")
		if i < 0 || j < i {
			t.Fatalf("no agreement section before the limits:\n%s", page)
		}
		return page[i:j]
	}
	if out := render(nil); !strings.Contains(out, "There is no earlier run") || !strings.Contains(out, "has been reproduced") {
		t.Errorf("no previous run:\n%s", out)
	}
	old := run("0.9.0", true)
	if out := render(&evalPrevious{label: "08", data: old, derived: deriveReport(old, nil)}); !strings.Contains(out, "Run 08 was taken under record contract 0.9.0") || !strings.Contains(out, "not compared") || strings.Contains(out, "| caster |") {
		t.Errorf("a previous run under another contract:\n%s", out)
	}
	same := run(measureGeneratorVersion, true)
	out := render(&evalPrevious{label: "08", data: same, derived: deriveReport(same, nil)})
	// Places 1..6 against 6,2,3,4,5,1: Spearman 1 - 6*50/(6*35) = -0.43; the
	// four in the middle keep their verdicts and places; C0 went from 6th to
	// 1st; of this run's top five (C0..C4) four were in the other's top five.
	for _, want := range []string{"| caster | 6 | -0.43 |", "C0 (6 to 1)", "No implementation changed delivery class", "| caster | 6 | 4 (67%) |"} {
		if !strings.Contains(out, want) {
			t.Errorf("the agreement section lacks %q:\n%s", want, out)
		}
	}
	if rho, ok := spearman([]float64{1, 2, 3, 4}, []float64{10, 20, 30, 40}); !ok || rho != 1 {
		t.Errorf("spearman of an identical order = %v, %v", rho, ok)
	}
	if rho, ok := spearman([]float64{1, 2, 2, 3}, []float64{3, 2, 2, 1}); !ok || rho != -1 {
		t.Errorf("spearman of a reversed order with a tie = %v, %v", rho, ok)
	}
	if _, ok := spearman([]float64{1, 1, 1}, []float64{1, 2, 3}); ok {
		t.Error("spearman of a constant series is defined")
	}
}
