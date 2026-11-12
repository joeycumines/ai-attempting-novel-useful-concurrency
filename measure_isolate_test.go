package bigbuff

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Process isolation of the measurement run.
//
// A stalled-consumer probe ends with a publisher that cannot be stopped: it is
// blocked inside a Send that will never return, and nothing can unpark it. For
// most implementations that goroutine is parked and costs nothing. For an
// implementation whose Send waits by spinning it is not parked: it keeps a CPU
// busy for as long as the process lives. Up to record contract 1.2.0 the whole
// run was one process, so from the first such probe on, every battery of every
// later implementation was timed on a machine it shared with the leftovers of
// earlier ones (on a 2-CPU machine, two spinning publishers took both CPUs),
// while the implementations measured before it had the machine to themselves.
//
// From contract 1.3.0 a run is a parent process that measures nothing. It
// starts one child process per unit (one battery of one implementation),
// copies the child's records into the results file, and the child exits, which
// is the only thing that ends a goroutine that will not end itself. Every unit
// therefore starts from the same state: a new process with no goroutines, no
// heap and no garbage left behind by anything measured before it.

// isolationContract is the first record contract whose runs are isolated.
const isolationContract = "1.3.0"

// measureIsolationPerUnit is the value of the meta record's "isolation" field
// for a run made of one process per unit.
const measureIsolationPerUnit = "process-per-battery"

// measureUnit is one battery of one implementation.
type measureUnit struct {
	family, impl, battery string
	// index is the implementation's position among its family's names.
	index int
}

func (u measureUnit) String() string { return u.battery + " of " + u.impl }

// measureFamilies lists the families in run order with the function that runs
// a family's batteries and the one that names its implementations.
func measureFamilies() []struct {
	family string
	run    func(*measureContext)
	names  func() []string
} {
	return []struct {
		family string
		run    func(*measureContext)
		names  func() []string
	}{
		{"broadcast", runBroadcastBatteries, func() []string {
			var names []string
			for _, f := range AllBroadcasters() {
				p := f()
				names = append(names, p.Name())
				closeWithWatchdog(p, 2*time.Second)
			}
			return names
		}},
		{"caster", runCasterBatteries, func() []string { return sortedKeys(casterLikes()) }},
		{"scatter", runScatterBatteries, func() []string { return sortedKeys(scatterLikes()) }},
		{"gather", runGatherBatteries, func() []string { return sortedKeys(gatherLikes()) }},
		{"payload", runPayloadBatteries, func() []string { return sortedKeys(asyncImpls()) }},
		{"fanin", runFaninBatteries, func() []string { return sortedKeys(faninLikes()) }},
		{"exchange", runExchangeBatteries, func() []string { return sortedKeys(exchangeLikes()) }},
		{"barrier", runBarrierBatteries, func() []string { return sortedKeys(barrierLikes()) }},
	}
}

// measurePlan lists the units a run consists of, in the order their records
// appear in the results file: families in run order, implementations in
// registration order (broadcast) or by name, batteries in registry order.
// The globs of ctx narrow it exactly as they narrow an in-process run.
func measurePlan(ctx *measureContext) []measureUnit {
	var plan []measureUnit
	for _, fam := range measureFamilies() {
		var batteries []string
		for _, b := range measureBatteries {
			if strings.HasPrefix(b.ID, fam.family+".") && ctx.batteryAllowed(b.ID) {
				batteries = append(batteries, b.ID)
			}
		}
		if len(batteries) == 0 {
			continue
		}
		for i, name := range fam.names() {
			if !ctx.implAllowed(name) {
				continue
			}
			for _, b := range batteries {
				plan = append(plan, measureUnit{fam.family, name, b, i})
			}
		}
	}
	return plan
}

// measureUnitTimeout bounds one unit. The longest unit of the full profile
// takes well under a minute; a unit that reaches this has hung, and is killed
// and recorded as a failure instead of hanging the run.
var measureUnitTimeout = 10 * time.Minute

// measureCrashUnitEnv names a unit ("family/impl/battery") whose child exits
// without measuring. It exists so that the tests can show what the parent does
// with a child that dies.
const measureCrashUnitEnv = "BIGBUFF_MEASURE_CRASH_UNIT"

// measureResidueWindow is how long a child watches its own CPU use after a
// unit that left goroutines behind.
const measureResidueWindow = 100 * time.Millisecond

// runMeasureUnit is the child: it measures one unit into outPath and reports
// what the unit left behind.
func runMeasureUnit(u measureUnit, profile measureProfile, seed int64, outPath string) int {
	if os.Getenv(measureCrashUnitEnv) == u.family+"/"+u.impl+"/"+u.battery {
		fmt.Fprintln(os.Stderr, "bigbuff-measure: unit asked to crash by "+measureCrashUnitEnv)
		return 3
	}
	var run func(*measureContext)
	for _, fam := range measureFamilies() {
		if fam.family == u.family {
			run = fam.run
		}
	}
	if run == nil {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: unit: unknown family %q\n", u.family)
		return 2
	}
	w, err := newMeasureWriter(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: unit: %v\n", err)
		return 1
	}
	ctx := &measureContext{
		profile:        profile,
		implGlob:       u.impl,
		batteryGlob:    u.battery,
		exact:          true,
		broadcastIndex: -1,
		out:            w,
		seed:           seed,
		started:        time.Now(),
	}
	if u.family == "broadcast" {
		ctx.broadcastIndex = u.index
	}
	before := runtime.NumGoroutine()
	run(ctx)
	if w.records == 0 && len(ctx.failures) == 0 {
		// Every unit of the plan measures or skips something. One that does
		// neither named an implementation or a battery the family does not
		// have, and a run that let it pass would be short without saying so.
		ctx.fail("%s: nothing in family %q answers to that implementation and battery; the unit measured nothing", u, u.family)
	}
	if left, cores, measured := measureResidue(before); left > 0 {
		rec := map[string]any{
			"type":       "residue",
			"family":     u.family,
			"impl":       u.impl,
			"battery":    u.battery,
			"goroutines": left,
		}
		if measured {
			rec["cpu_cores"] = cores
		}
		ctx.write(rec)
	}
	ctx.write(map[string]any{
		"type":        "unit_summary",
		"skipped":     ctx.skipped,
		"failure_msg": ctx.failures,
	})
	if err := w.close(); err != nil {
		fmt.Fprintf(os.Stderr, "bigbuff-measure: unit: close: %v\n", err)
		return 1
	}
	return 0
}

// measureResidue reports what a unit left running in this process: the number
// of goroutines above the count taken before the unit, and, when there are
// any, how many CPUs' worth of time the process used while this goroutine
// slept. A parked goroutine uses none; a publisher that waits for a stalled
// subscriber by spinning uses one whole CPU.
func measureResidue(before int) (goroutines int, cpuCores float64, measured bool) {
	// Goroutines that are on their way out get a moment to finish.
	for i := 0; i < 20 && runtime.NumGoroutine() > before; i++ {
		time.Sleep(time.Millisecond)
	}
	goroutines = runtime.NumGoroutine() - before
	if goroutines <= 0 {
		return 0, 0, false
	}
	c0, ok := processCPUTime()
	if !ok {
		return goroutines, 0, false
	}
	t0 := time.Now()
	time.Sleep(measureResidueWindow)
	c1, _ := processCPUTime()
	return goroutines, float64(c1-c0) / float64(time.Since(t0)), true
}

// measureExecEnv names a command that the measuring processes are started
// through: the parent runs `<command> <this executable> <arguments>`. It is
// for a test binary that the host cannot start by itself, which is the case
// when it was built for another architecture and is running under an
// emulator (`go test -exec`): the parent runs inside the emulator and sees
// nothing unusual, and its children need the same emulator. `make
// verify-arm64` sets it.
const measureExecEnv = "BIGBUFF_MEASURE_EXEC"

// measureChildCommand is the command that runs this executable with args as a
// measuring process.
func measureChildCommand(ctx context.Context, exe string, args ...string) *exec.Cmd {
	if prefix := strings.Fields(os.Getenv(measureExecEnv)); len(prefix) > 0 {
		argv := append(append(append([]string{}, prefix[1:]...), exe), args...)
		return exec.CommandContext(ctx, prefix[0], argv...)
	}
	return exec.CommandContext(ctx, exe, args...)
}

// runMeasureIsolated is the parent: it runs every unit of the plan in a child
// process of its own and copies the records into ctx.out in plan order.
func runMeasureIsolated(ctx *measureContext) {
	exe, err := os.Executable()
	if err != nil {
		ctx.fail("cannot find this executable to start measuring processes: %v", err)
		return
	}
	dir, err := os.MkdirTemp("", "bigbuff-measure-")
	if err != nil {
		ctx.fail("cannot create a scratch directory: %v", err)
		return
	}
	defer os.RemoveAll(dir)

	lastImpl := ""
	for i, u := range measurePlan(ctx) {
		if key := u.family + "\x00" + u.impl; key != lastImpl {
			lastImpl = key
			fmt.Fprintf(os.Stderr, "  %s: %s\n", u.family, u.impl)
		}
		out := filepath.Join(dir, "unit-"+strconv.Itoa(i)+".jsonl")
		runCtx, cancel := context.WithTimeout(context.Background(), measureUnitTimeout)
		cmd := measureChildCommand(runCtx, exe, "-test.run=NONE", measureSentinel, "run",
			"--profile", ctx.profile.Name,
			"--seed", strconv.FormatInt(ctx.seed, 10),
			"--out", out,
			"--unit-family", u.family,
			"--unit-impl", u.impl,
			"--unit-battery", u.battery,
			"--unit-index", strconv.Itoa(u.index))
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		runErr := cmd.Run()
		timedOut := errors.Is(runCtx.Err(), context.DeadlineExceeded)
		cancel()

		summarised, copyErr := copyUnitRecords(ctx, out)
		_ = os.Remove(out)
		switch {
		case timedOut:
			ctx.fail("%s: the measuring process was killed after %s", u, measureUnitTimeout)
		case runErr != nil:
			ctx.fail("%s: the measuring process failed (%v): %s", u, runErr, lastLines(stderr.String(), 6))
		case copyErr != nil:
			ctx.fail("%s: %v", u, copyErr)
		case !summarised:
			ctx.fail("%s: the measuring process wrote no summary: %s", u, lastLines(stderr.String(), 6))
		}
	}
}

// copyUnitRecords appends a child's records to the results file byte for
// byte, so that no number is re-encoded on the way, and folds the child's
// summary into the parent's counts. It reports whether the summary was there:
// a child that died part-way leaves records and no summary.
func copyUnitRecords(ctx *measureContext, path string) (summarised bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var head struct {
			Type     string   `json:"type"`
			Skipped  int      `json:"skipped"`
			Failures []string `json:"failure_msg"`
		}
		if err := json.Unmarshal(line, &head); err != nil {
			return summarised, fmt.Errorf("the measuring process wrote a malformed record: %v", err)
		}
		if head.Type == "unit_summary" {
			summarised = true
			ctx.skipped += head.Skipped
			for _, msg := range head.Failures {
				ctx.fail("%s", msg)
			}
			continue
		}
		if err := ctx.out.writeRaw(line); err != nil {
			return summarised, err
		}
	}
	return summarised, sc.Err()
}

// lastLines returns the last n non-empty lines of s on one line.
func lastLines(s string, n int) string {
	var lines []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	if len(lines) == 0 {
		return "(no output)"
	}
	return strings.Join(lines, " | ")
}
