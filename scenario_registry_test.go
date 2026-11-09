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
