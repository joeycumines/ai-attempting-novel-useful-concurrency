package bigbuff

// Registry completeness: every factory and case must be attached and runnable.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// --- Registry completeness tests ---

func TestScenarioRegistryComplete(t *testing.T) {
	// Every Broadcaster factory must be attached to at least one scenario.
	attached := map[string]bool{}
	for _, a := range broadcastAttachments {
		attached[a.Impl] = true
	}
	for _, f := range AllBroadcasters() {
		probe := f()
		name := probe.Name()
		probe.Close()
		if !attached[name] {
			t.Errorf("broadcaster %q is not attached to any scenario", name)
		}
	}
	// Every attachment must resolve to a real factory of its family.
	factories := scenarioBroadcastFactories()
	for _, a := range broadcastAttachments {
		if _, ok := factories[a.Impl]; !ok {
			t.Errorf("broadcast attachment %q does not resolve to a factory", a.Impl)
		}
	}
	casters := casterImpls()
	casterAttached := map[string]bool{}
	// The vacuum caster baseline is driven through its own adapter (below),
	// not through casterImpls.
	casterAttached["BaselineRendezvousCaster"] = true
	for _, a := range casterAttachments {
		casterAttached[a.Impl] = true
		if a.Impl == "BaselineRendezvousCaster" {
			continue
		}
		if _, ok := casters[a.Impl]; !ok {
			t.Errorf("caster attachment %q does not resolve", a.Impl)
		}
	}
	for name := range casters {
		if !casterAttached[name] {
			t.Errorf("caster %q is not attached to the rendezvous scenario", name)
		}
	}
	// Every scenario's declared baseline must itself be attached to it.
	for _, s := range Scenarios {
		found := false
		for _, a := range allAttachments() {
			if a.Scenario == s.ID && a.Impl == s.Baseline {
				found = true
			}
		}
		if !found {
			t.Errorf("scenario %q baseline %q is not attached to it", s.ID, s.Baseline)
		}
	}
	payloadAttached := map[string]bool{}
	for _, a := range payloadAttachments {
		payloadAttached[a.Impl] = true
		if scenarioByID[a.Scenario].Kind != KindPayload {
			t.Errorf("payload attachment in non-payload scenario %q", a.Scenario)
		}
	}
	for name := range asyncImpls() {
		if !payloadAttached[name] {
			t.Errorf("async/payload impl %q is not attached to the payload scenario", name)
		}
	}
	// Non-broadcast families are covered by their dedicated runners; assert
	// their scenario kinds line up.
	for _, a := range casterAttachments {
		if scenarioByID[a.Scenario].Kind != KindCaster {
			t.Errorf("caster attachment in non-caster scenario %q", a.Scenario)
		}
	}
	for _, a := range scatterAttachments {
		if scenarioByID[a.Scenario].Kind != KindScatter {
			t.Errorf("scatter attachment in non-scatter scenario %q", a.Scenario)
		}
	}
	for _, a := range gatherAttachments {
		if scenarioByID[a.Scenario].Kind != KindGather {
			t.Errorf("gather attachment in non-gather scenario %q", a.Scenario)
		}
	}
	fanins := faninLikes()
	faninAttached := map[string]bool{}
	for _, a := range faninAttachments {
		faninAttached[a.Impl] = true
		if _, ok := fanins[a.Impl]; !ok {
			t.Errorf("fanin attachment %q does not resolve to a factory", a.Impl)
		}
		if scenarioByID[a.Scenario].Kind != KindFanin {
			t.Errorf("fanin attachment in non-fanin scenario %q", a.Scenario)
		}
	}
	for name := range fanins {
		if !faninAttached[name] {
			t.Errorf("fanin %q is not attached to any scenario", name)
		}
	}
	checkFamily := func(family string, kind ScenarioKind, atts []Attachment, factories map[string]bool) {
		seen := map[string]bool{}
		for _, a := range atts {
			seen[a.Impl] = true
			if !factories[a.Impl] {
				t.Errorf("%s attachment %q does not resolve to a factory", family, a.Impl)
			}
			if scenarioByID[a.Scenario].Kind != kind {
				t.Errorf("%s attachment in non-%s scenario %q", family, family, a.Scenario)
			}
		}
		for name := range factories {
			if !seen[name] {
				t.Errorf("%s %q is not attached to any scenario", family, name)
			}
		}
	}
	exchangers := map[string]bool{}
	for name := range exchangeLikes() {
		exchangers[name] = true
	}
	checkFamily("exchange", KindExchange, exchangeAttachments, exchangers)
	barriers := map[string]bool{}
	for name := range barrierLikes() {
		barriers[name] = true
	}
	checkFamily("barrier", KindBarrier, barrierAttachments, barriers)
}

func TestScenarioCaseRunnablesExist(t *testing.T) {
	funcs := make(map[string]bool)
	re := regexp.MustCompile(`(?m)^func\s+((?:Test|Benchmark|Fuzz)[A-Za-z0-9_]*)\s*\(`)
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range re.FindAllSubmatch(data, -1) {
			funcs[string(m[1])] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range Cases {
		if !funcs[c.Runnable] {
			t.Errorf("case %q references missing runnable %q", c.ID, c.Runnable)
		}
		switch c.Kind {
		case CaseTest:
			if !strings.HasPrefix(c.Runnable, "Test") {
				t.Errorf("case %q test runnable %q", c.ID, c.Runnable)
			}
		case CaseBenchmark:
			if !strings.HasPrefix(c.Runnable, "Benchmark") {
				t.Errorf("case %q benchmark runnable %q", c.ID, c.Runnable)
			}
		case CaseFuzz:
			if !strings.HasPrefix(c.Runnable, "Fuzz") {
				t.Errorf("case %q fuzz runnable %q", c.ID, c.Runnable)
			}
		}
	}
}

// --- Deterministic scenario runners ---
//

// harnessRunnables lists every top-level runnable that is deliberately not a
// scenario case and does not live in an implementation's own test file, with
// the reason. TestScenarioCaseRunnablesExhaustive keeps the list exact in
// both directions.
var harnessRunnables = map[string]string{
	// Adapter conformance: these run every Broadcaster adapter regardless of
	// scenario. They establish that an adapter honours the harness contract;
	// the per-scenario facets are what compare implementations.
	"TestStandardHarness_SingleSubscriber":      "adapter conformance across all scenarios",
	"TestStandardHarness_MultiSubscriberFanout": "adapter conformance across all scenarios",
	"TestStandardHarness_InOrderDelivery":       "adapter conformance across all scenarios",
	"TestStandardHarness_TryReceive":            "adapter conformance across all scenarios",
	"FuzzUnifiedBroadcaster":                    "adapter conformance across all scenarios (superseded for comparison by FuzzScenarioBroadcast)",
	"BenchmarkUnified_Select":                   "adapter-level select cost; not tied to one scenario",
	"TestAsyncFamily":                           "family conformance for the asyncAPI adapters",
	"TestCasterFamily":                          "family conformance for the caster adapters",
	"TestPubSubFamily":                          "family conformance for the pubsub adapters",
	"BenchmarkLockStep":                         "historical lock-step comparison kept for continuity with results-01; superseded by the measure batteries",

	// Regressions for individual implementations that are kept in a shared
	// file because each needs harness helpers.
	"TestAsyncCoreVariants":                               "implementation regressions (shared file)",
	"TestFanoutSubscribeContextCancel":                    "implementation regression (shared file)",
	"TestChanBeaconSeqAfterClose":                         "implementation regression (shared file)",
	"TestChanRelayCountDrift":                             "implementation regression (shared file)",
	"TestChanScatterStealRace":                            "implementation regression (shared file)",
	"TestGenCasterLaggardRetention":                       "implementation regression (shared file)",
	"TestCroutonStalledSubscriberSpin":                    "implementation regression (shared file)",
	"TestBaselineScatterQueueWithdrawalReleasesAdmission": "unit test of the scatter baselines (its file name matches no single source file)",

	// The registry and the measurement tooling test themselves.
	"TestScenarioRegistryComplete":                      "registry self-check",
	"TestScenarioCaseRunnablesExist":                    "registry self-check",
	"TestScenarioCaseRunnablesExhaustive":               "registry self-check",
	"TestMain":                                          "test binary entry point (hosts the bigbuff-measure CLI)",
	"TestIntegrityOfGates":                              "measurement report tooling",
	"TestMeasuredSetsOfRecordedContract":                "measurement index tooling",
	"TestMeasuredSetsOfLegacyDump":                      "measurement index tooling",
	"TestIndexVerifiesEachRunAgainstItsOwnRegistry":     "measurement index tooling",
	"TestCommittedRunsAreIndexed":                       "measurement index tooling",
	"TestVerifyRejectsMetricRecordsWithoutAValue":       "measurement verify tooling",
	"TestSmallFamiliesAreNotRanked":                     "measurement report tooling",
	"TestUnrankedFamilyStillReportsIntegrityFailure":    "measurement report tooling",
	"TestScoreUsesOnlyDimensionsWithData":               "measurement report tooling",
	"TestExchangePairingViolations":                     "measurement battery tooling",
	"TestGateRecordReadsTheStamp":                       "measurement gates tooling",
	"TestGateCommitComparison":                          "measurement gates tooling",
	"TestEvaluationStatesWhatChangedBeforeTheGates":     "measurement evaluation tooling",
	"TestEvaluationComparesEachScenarioWithItsBaseline": "measurement evaluation tooling",
	"TestGateLogParsing":                                "measurement gates tooling",
	"TestEvaluationIsGeneratedFromTheRecords":           "measurement evaluation tooling",
	"TestTrackerCoversEveryResponse":                    "site tooling",
	"TestTrackerFilesResolve":                           "site tooling",
	"TestRegistryDumpForResults":                        "measure tooling",
	"TestReportScriptDrawsOutlinesAsPaths":              "report tooling",
	"TestLandingPageCoversEveryRunAndReport":            "site tooling",
	"TestCommittedLandingPageIsCurrent":                 "site tooling",
	"TestSiteWiringIsIdempotent":                        "site tooling",
	"TestPagesMarkdownLiquidDelimitersAreEscaped":       "site tooling",
	"TestCasterRoundsDeliverEverySend":                  "measurement tooling",
	"TestCasterBatteriesRecordDeliveries":               "measurement tooling",
	"TestCasterBatteriesExposeSendsThatDeliverNothing":  "measurement battery tooling",
	"TestCasterBatteriesFailOnAPhantomDelivery":         "measurement tooling",
	"TestCasterTraceShowsDeliveries":                    "measurement tooling",
	"TestCasterFamilyOfOlderContractsIsNotRanked":       "measurement tooling",
}

// implementationSource returns the non-test source file a test file belongs
// to: the longest name obtained by dropping trailing _words from the test
// file's stem that exists as a .go file. "ring_62_haiku_bench_test.go"
// belongs to "ring_62_haiku.go"; "harness_targeted_test.go" belongs to none.
func implementationSource(testFile string) string {
	stem := strings.TrimSuffix(testFile, "_test.go")
	for stem != "" {
		if _, err := os.Stat(stem + ".go"); err == nil {
			return stem + ".go"
		}
		i := strings.LastIndexByte(stem, '_')
		if i < 0 {
			break
		}
		stem = stem[:i]
	}
	return ""
}

// TestScenarioCaseRunnablesExhaustive accounts for every top-level Test,
// Benchmark and Fuzz function in the package. Each must be exactly one of:
// a registered scenario case; a unit runnable in an implementation's own
// test file (not comparable across implementations by construction); or an
// entry in harnessRunnables with a reason. A new cross-implementation
// runnable therefore cannot be added without either registering it against
// a scenario or stating why it is not one.
func TestScenarioCaseRunnablesExhaustive(t *testing.T) {
	runnables, err := Runnables()
	if err != nil {
		t.Fatal(err)
	}
	count := map[RunnableClass]int{}
	listed := map[string]bool{}
	perScenario := map[ScenarioID]int{}
	for _, r := range runnables {
		count[r.Class]++
		switch r.Class {
		case RunnableHarness:
			listed[r.Name] = true
			if r.Reason == "" {
				t.Errorf("%s (%s): listed in harnessRunnables without a reason", r.Name, r.File)
			}
		case RunnableUnit:
			if len(r.Implementations) == 0 {
				t.Errorf("%s (%s): its source file %s provides no attached implementation; attach one, or name the implementations in sourceImplementations", r.Name, r.File, r.Source)
			}
			if len(r.Scenarios) == 0 {
				t.Errorf("%s (%s): resolves to no scenario", r.Name, r.File)
			}
		case RunnableCase:
			if len(r.Scenarios) == 0 {
				t.Errorf("%s (%s): registered case without a scenario", r.Name, r.File)
			}
		}
		for _, id := range r.Scenarios {
			if _, ok := scenarioByID[id]; !ok {
				t.Errorf("%s (%s): unknown scenario %q", r.Name, r.File, id)
			}
			perScenario[id]++
		}
	}
	for name := range harnessRunnables {
		if !listed[name] {
			t.Errorf("harnessRunnables lists %s, which no longer exists or is now a registered case", name)
		}
	}
	for source, impls := range sourceImplementations {
		if _, err := os.Stat(source); err != nil {
			t.Errorf("sourceImplementations names %s: %v", source, err)
		}
		known := map[string]bool{}
		for _, a := range allAttachments() {
			known[a.Impl] = true
		}
		for _, impl := range impls {
			if !known[impl] {
				t.Errorf("sourceImplementations[%s] names %q, which is not an attached implementation", source, impl)
			}
		}
	}
	for _, s := range Scenarios {
		if perScenario[s.ID] == 0 {
			t.Errorf("scenario %q has no runnable", s.ID)
		}
	}
	t.Logf("%d runnables: %d registered cases, %d implementation unit runnables, %d listed harness runnables",
		len(runnables), count[RunnableCase], count[RunnableUnit], count[RunnableHarness])
}
