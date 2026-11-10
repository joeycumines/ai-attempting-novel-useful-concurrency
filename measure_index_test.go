package bigbuff

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestMeasuredSetsOfRecordedContract(t *testing.T) {
	dump, err := registryDump()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(dump)
	if err != nil {
		t.Fatal(err)
	}
	var reg map[string]any
	if err := json.Unmarshal(raw, &reg); err != nil {
		t.Fatal(err)
	}
	got := map[string][]string{}
	for _, set := range measuredSetsOf(reg) {
		got[set.family] = set.names
	}
	want := map[string][]string{}
	for _, set := range expectedImplSets() {
		names := append([]string{}, set.names...)
		sort.Strings(names)
		want[set.family] = names
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("a dump of the live registry does not round-trip its coverage contract:\n got %v\nwant %v", got, want)
	}
	// Every measured family is reported in a defined position.
	for family := range want {
		found := false
		for _, f := range measureFamilyOrder {
			found = found || f == family
		}
		if !found {
			t.Errorf("family %q is measured but missing from measureFamilyOrder", family)
		}
	}
}

func TestMeasuredSetsOfLegacyDump(t *testing.T) {
	att := func(family, impl string) any { return map[string]any{"family": family, "impl": impl} }
	reg := map[string]any{"attachments": []any{
		att("broadcast", "B"), att("broadcast", "A"), att("broadcast", "A"),
		att("broadcast", "Caster:C"), // exercised through the scenario, never a battery subject
		att("fanin", "F"),
		att("exchange", "X"), // attached, but the family had no battery before the contract was recorded
	}}
	got := measuredSetsOf(reg)
	want := []expectedImplSet{{"broadcast", []string{"A", "B"}}, {"fanin", []string{"F"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("legacy dump: got %v, want %v", got, want)
	}
}

func TestIndexVerifiesEachRunAgainstItsOwnRegistry(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	manifest := func(run string) map[string]any {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, "results-"+run+"-00-manifest.json"))
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	const meta = `{"type":"meta","profile":"quick"}` + "\n"
	record := func(impl string) string {
		return `{"type":"metric","family":"broadcast","impl":"` + impl + `","metric":"m"}` + "\n"
	}
	// Run 01 measured everything its registry defined. Run 02 carries no
	// dump. Neither covers the live registry, which is irrelevant to them.
	write("results-01-01-results.jsonl", meta+record("A"))
	write("results-01-02-registry.json", `{"measured":{"broadcast":["A"]}}`)
	write("results-02-01-results.jsonl", meta+record("A"))
	if rc := cmdMeasureIndex([]string{"--dir", dir}); rc != 0 {
		t.Fatalf("index of two complete runs exited %d", rc)
	}
	for _, run := range []string{"01", "02"} {
		if m := manifest(run); m["verified"] != true {
			t.Errorf("run %s: verified = %v, want true", run, m["verified"])
		}
	}
	if against, _ := manifest("01")["verified_against"].(string); against != "results-01-02-registry.json" {
		t.Errorf("run 01 verified against %q, want its own dump", against)
	}
	if against, _ := manifest("02")["verified_against"].(string); !strings.HasPrefix(against, "results-01-02-registry.json (nearest earlier") {
		t.Errorf("run 02 verified against %q, want the nearest earlier dump, stated as such", against)
	}

	// Indexing an indexed tree changes nothing, byte for byte.
	before, _ := os.ReadFile(filepath.Join(dir, "results-01-00-manifest.json"))
	if rc := cmdMeasureIndex([]string{"--dir", dir}); rc != 0 {
		t.Fatalf("second index exited %d", rc)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "results-01-00-manifest.json"))
	if string(before) != string(after) {
		t.Error("re-indexing rewrote an up-to-date manifest")
	}
	// ...but a changed artifact is picked up.
	write("results-01-01-results.jsonl", meta+record("A")+record("A"))
	if rc := cmdMeasureIndex([]string{"--dir", dir}); rc != 0 {
		t.Fatalf("index after an artifact changed exited %d", rc)
	}
	if changed, _ := os.ReadFile(filepath.Join(dir, "results-01-00-manifest.json")); string(changed) == string(before) {
		t.Error("manifest not rewritten after its JSONL changed")
	}

	// A run that is incomplete for its own registry fails the index, and
	// later runs do not hide it.
	write("results-03-01-results.jsonl", meta+record("A"))
	write("results-03-02-registry.json", `{"measured":{"broadcast":["A","B"]}}`)
	write("results-04-01-results.jsonl", meta+record("A")+record("B"))
	write("results-04-02-registry.json", `{"measured":{"broadcast":["A","B"]}}`)
	if rc := cmdMeasureIndex([]string{"--dir", dir}); rc == 0 {
		t.Fatal("index accepted a run that lacks an implementation its own registry measures")
	}
	if m := manifest("03"); m["verified"] != false {
		t.Errorf("run 03: verified = %v, want false", m["verified"])
	}
	if m := manifest("04"); m["verified"] != true {
		t.Errorf("run 04: verified = %v, want true", m["verified"])
	}
}

// TestCommittedRunsAreIndexed fails when the committed manifests do not match
// the committed artifacts: a report rewired, a results file replaced, or a
// run added without re-indexing.
func TestCommittedRunsAreIndexed(t *testing.T) {
	manifests, err := filepath.Glob("results-[0-9][0-9]-00-manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) == 0 {
		t.Skip("no committed measurement runs")
	}
	// Index a copy, so the test never writes to the working tree.
	dir := t.TempDir()
	files, _ := filepath.Glob("results-[0-9][0-9]-[0-9][0-9]-*")
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, f), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if rc := cmdMeasureIndex([]string{"--dir", dir}); rc != 0 {
		t.Errorf("a committed run is incomplete for the registry that produced it (index exited %d)", rc)
	}
	for _, m := range manifests {
		committed, _ := os.ReadFile(m)
		if !sameManifest(filepath.Join(dir, m), committed) {
			t.Errorf("%s is stale: run 'make measure-index' and commit the result", m)
		}
	}
}
