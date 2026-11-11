package bigbuff

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// deliveryClassMetrics is a broadcast family of six implementations that
// deliver everything (L0 fastest) and six that deliver a tenth (D0 fastest),
// every one of the second six cheaper to publish to than any of the first.
func deliveryClassMetrics() []map[string]any {
	var metrics []map[string]any
	add := func(impl string, ns, slope, pubs, recv float64, stalled float64) {
		for _, subs := range []float64{1, 8, 64} {
			r := recv * subs
			metrics = append(metrics, map[string]any{"type": "metric", "family": "broadcast", "impl": impl, "metric": "publish_ns_op",
				"dims": map[string]any{"subs": subs}, "mean": ns + slope*(subs-1), "publishes": pubs, "receives": r, "delivered_fraction": r / (pubs * subs)})
		}
		metrics = append(metrics, map[string]any{"type": "metric", "family": "broadcast", "impl": impl,
			"metric": "stalled_consumer_ops_completed", "ops_completed": stalled})
	}
	for i := range 6 {
		add(fmt.Sprintf("L%d", i), float64(1000+100*i), float64(i+1), 1000, 1000, float64(10*i))
		add(fmt.Sprintf("D%d", i), float64(10+i), float64(i+1), 1000, 100, 1000)
	}
	return metrics
}

// An implementation that delivers every value is scored among those that do,
// and one that drops among those that drop. Scored together, as they were up
// to record contract 1.3.0, the six that drop take the top six places on
// throughput because discarding a value is cheaper than delivering it.
func TestDeliveryClassesAreScoredSeparately(t *testing.T) {
	metrics := deliveryClassMetrics()
	dd := deriveReport(&reportData{meta: map[string]any{"generator_version": deliveryClassContract}, metrics: metrics}, nil)
	if reason, no := dd.Unranked["broadcast"]; no {
		t.Fatalf("the family is unranked: %s", reason)
	}
	for impl, want := range map[string]struct {
		class      string
		throughput int
	}{
		"L0": {classDeliversAll, 100}, "L5": {classDeliversAll, 0},
		"D0": {classDrops, 100}, "D5": {classDrops, 0},
	} {
		s := dd.Scores["broadcast"][impl]
		if !s.Ranked || s.Class != want.class || s.Peers != 6 || s.Dims["throughput"] != want.throughput {
			t.Errorf("%s: ranked=%v class=%q peers=%d throughput=%d, want class %q among 6 with throughput %d",
				impl, s.Ranked, s.Class, s.Peers, s.Dims["throughput"], want.class, want.throughput)
		}
		if f, _ := s.Metrics["delivered_fraction@8"].(float64); (want.class == classDeliversAll) != (f == 1) {
			t.Errorf("%s: delivered fraction at 8 subscribers = %v in class %q", impl, s.Metrics["delivered_fraction@8"], s.Class)
		}
	}
	// Every member of the dropping class completed the stalled-consumer
	// probe: the dimension distinguishes none of them and is left out, so
	// the fastest of them is not held to 80 by a fifth of the weight
	// scoring nothing.
	d0 := dd.Scores["broadcast"]["D0"]
	if len(d0.DimsUniform) != 1 || d0.DimsUniform[0] != "stall_resilience" {
		t.Errorf("D0: uniform dimensions %v, want stall_resilience", d0.DimsUniform)
	}
	if d0.Score != 100 {
		t.Errorf("D0 scored %d, want 100: best of its class on every dimension that distinguishes its members", d0.Score)
	}
	if l0 := dd.Scores["broadcast"]["L0"]; len(l0.DimsUniform) != 0 {
		t.Errorf("L0: uniform dimensions %v, want none", l0.DimsUniform)
	}

	// The same records under the contract before: no delivery classes, so
	// no ranking, and the reason says from which contract there is one.
	old := deriveReport(&reportData{meta: map[string]any{"generator_version": isolationContract}, metrics: metrics}, nil)
	s := old.Scores["broadcast"]["D0"]
	if s.Ranked || s.Score != 0 || s.Verdict != "Not ranked (measurement not valid)" || !strings.Contains(s.UnrankedReason, "do not say how much") {
		t.Errorf("contract %s: %+v", isolationContract, s)
	}
	if old.UnrankedSince["broadcast"] != deliveryClassContract {
		t.Errorf("contract %s: valid since %v, want %s", isolationContract, old.UnrankedSince, deliveryClassContract)
	}
}

// A class with fewer than minRankedImpls members is not ranked, and the other
// class of the same family still is.
func TestSmallDeliveryClassIsNotRanked(t *testing.T) {
	var metrics []map[string]any
	for _, m := range deliveryClassMetrics() {
		if impl := m["impl"].(string); impl == "D4" || impl == "D5" {
			continue
		}
		metrics = append(metrics, m)
	}
	dd := deriveReport(&reportData{meta: map[string]any{"generator_version": deliveryClassContract}, metrics: metrics}, nil)
	if _, no := dd.Unranked["broadcast"]; no {
		t.Error("the family is unranked although one of its classes has six members")
	}
	d, l := dd.Scores["broadcast"]["D0"], dd.Scores["broadcast"]["L0"]
	if d.Ranked || d.Verdict != "Not ranked (class of 4)" || !strings.Contains(d.UnrankedReason, "drops or coalesces") {
		t.Errorf("D0: %+v", d)
	}
	if !l.Ranked || l.Peers != 6 {
		t.Errorf("L0: ranked=%v peers=%d", l.Ranked, l.Peers)
	}
}

// One repetition that loses a value puts the implementation in the dropping
// class: the class is "delivered everything, every time".
func TestDeliveryClassNeedsEveryCell(t *testing.T) {
	rec := func(subs, pubs, recv float64) map[string]any {
		return map[string]any{"family": "payload", "impl": "X", "metric": "publish_ns_op", "dims": map[string]any{"subs": subs},
			"publishes": pubs, "receives": recv, "delivered_fraction": recv / (pubs * subs)}
	}
	class, least := deliveryClasses([]map[string]any{rec(1, 100, 100), rec(8, 100, 800)}, "payload")
	if class["X"] != classDeliversAll || least["X"] != 1 {
		t.Errorf("class %q, least %v", class["X"], least["X"])
	}
	class, least = deliveryClasses([]map[string]any{rec(1, 100, 100), rec(8, 100, 799), rec(64, 100, 6400)}, "payload")
	if class["X"] != classDrops || least["X"] >= 1 {
		t.Errorf("one value short: class %q, least %v", class["X"], least["X"])
	}
	if class, _ := deliveryClasses([]map[string]any{{"family": "payload", "impl": "X", "metric": "publish_ns_op"}}, "payload"); len(class) != 0 {
		t.Errorf("a record without delivery fields has a class: %v", class)
	}
}

// A tie counts as half a place, so a value shared by the whole upper half of
// a field ranks at 75 and not at 50, and a value shared by everyone at 50.
func TestPercentileRankCountsATieAsHalf(t *testing.T) {
	half := []float64{0, 1, 2, 3, 4, 9, 9, 9, 9, 9}
	if got := percentileRank(half, 9, false); got != 78 { // (5 + 4/2) / 9
		t.Errorf("upper half tied: %d, want 78", got)
	}
	if got := percentileRank(half, 0, false); got != 0 {
		t.Errorf("worst: %d, want 0", got)
	}
	if got := percentileRank([]float64{1, 2, 3, 4, 5}, 1, true); got != 100 {
		t.Errorf("best, no ties: %d, want 100", got)
	}
	if got := percentileRank([]float64{7, 7, 7, 7}, 7, true); got != 50 {
		t.Errorf("all tied: %d, want 50", got)
	}
	if !uniformValues([]float64{7, 7, 7}) || uniformValues([]float64{7, 7, 8}) || uniformValues([]float64{7}) {
		t.Error("uniformValues")
	}
}

// verify rejects a run under the current contract whose throughput records do
// not say what was delivered, and accepts the same file under the contract
// before, which had no such fields.
func TestVerifyRejectsThroughputRecordsWithoutDelivery(t *testing.T) {
	sets := []expectedImplSet{{"broadcast", []string{"A"}}}
	run := func(generator, record string) (int, string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "results-01-01-results.jsonl")
		meta := `{"type":"meta","isolation":"` + measureIsolationPerUnit + `","generator_version":"` + generator + `"}` + "\n"
		if err := os.WriteFile(path, []byte(meta+record+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		return verifyResultsAgainst(path, sets, "a test registry", &out), out.String()
	}
	const bare = `{"type":"metric","family":"broadcast","impl":"A","metric":"publish_ns_op","mean":12.5}`
	const full = `{"type":"metric","family":"broadcast","impl":"A","metric":"publish_ns_op","mean":12.5,"publishes":10,"receives":3}`
	if rc, out := run(measureGeneratorVersion, bare); rc == 0 || !strings.Contains(out, "do not say what was delivered") {
		t.Errorf("current contract, no delivery fields: rc=%d\n%s", rc, out)
	}
	if rc, out := run(measureGeneratorVersion, full); rc != 0 {
		t.Errorf("current contract, delivery fields: rc=%d\n%s", rc, out)
	}
	if rc, out := run(isolationContract, bare); rc != 0 {
		t.Errorf("contract %s, no delivery fields: rc=%d\n%s", isolationContract, rc, out)
	}
}

// The throughput battery records what was delivered: all of it for a
// lock-step broadcaster, and less for a publisher that never waits for a
// subscriber.
func TestThroughputRecordsSayWhatWasDelivered(t *testing.T) {
	factories := map[string]func() Broadcaster{}
	for _, f := range AllBroadcasters() {
		factories[f().Name()] = f
	}
	for name, wantAll := range map[string]bool{"ChanPubSub (Baseline)": true, "RingPubSub TrySend (18-ChatGPT)": false} {
		factory := factories[name]
		if factory == nil {
			t.Fatalf("%s is not registered", name)
		}
		path := filepath.Join(t.TempDir(), "out.jsonl")
		w, err := newMeasureWriter(path)
		if err != nil {
			t.Fatal(err)
		}
		ctx := &measureContext{out: w, started: time.Now(), profile: measureProfile{
			Name: "test", Reps: 2, TargetPerRep: 5 * time.Millisecond, MinOps: 2000, MaxOps: 200000, ScaleSubs: []int{4},
		}}
		measureBroadcastThroughput(ctx, factory, name)
		if err := w.f.Close(); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
		for sc.Scan() {
			var rec map[string]any
			if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
				t.Fatal(err)
			}
			if rec["metric"] != "publish_ns_op" {
				continue
			}
			found = true
			pubs, _ := rec["publishes"].(float64)
			recv, _ := rec["receives"].(float64)
			frac, _ := rec["delivered_fraction"].(float64)
			if pubs <= 0 || frac != recv/(pubs*4) {
				t.Errorf("%s: publishes=%v receives=%v delivered_fraction=%v", name, rec["publishes"], rec["receives"], rec["delivered_fraction"])
			}
			if all := recv == pubs*4; all != wantAll {
				t.Errorf("%s: %v of %v values delivered, want everything delivered = %v", name, recv, pubs*4, wantAll)
			}
		}
		f.Close()
		if !found {
			t.Errorf("%s: no throughput record", name)
		}
	}
}
