package bigbuff

// Tests of what the caster throughput and latency batteries measure: a Send
// that delivers to every registered receiver, with the deliveries counted.

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Every measured caster delivers each armed Send to all its receivers, and
// the receivers take exactly what the Sends report.
func TestCasterRoundsDeliverEverySend(t *testing.T) {
	const receivers, sends = 8, 300
	for name, mk := range casterLikes() {
		if casterExposureLimited(name) {
			continue
		}
		t.Run(name, func(t *testing.T) {
			rounds := startCasterRounds(mk(), receivers)
			for i := range sends {
				_, k, ok := rounds.send(i + 1)
				if !ok {
					t.Fatalf("send %d: a receiver did not register again", i)
				}
				if k != receivers {
					t.Fatalf("send %d reported %d deliveries with %d receivers registered", i, k, receivers)
				}
			}
			if !rounds.arm() {
				t.Fatal("the receivers of the last Send did not register again")
			}
			if got := rounds.received.Load(); got != receivers*sends {
				t.Errorf("receivers took %d values, want %d", got, receivers*sends)
			}
			if !rounds.finish() {
				t.Fatal("receivers did not stop")
			}
		})
	}
}

// casterBatteryRecords runs the throughput and latency batteries on one
// caster with a small profile and returns the metric records by name and
// receiver count, with the harness failures.
func casterBatteryRecords(t *testing.T, mk func() casterLike, name string) (map[string]map[string]any, []string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "out.jsonl")
	w, err := newMeasureWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &measureContext{out: w, started: time.Now(), profile: measureProfile{
		Name: "test", Reps: 2, TargetPerRep: 2 * time.Millisecond, MinOps: 32, MaxOps: 400,
		CasterScaleSubs: []int{1, 4}, LatencySamples: 64,
	}}
	measureCasterThroughput(ctx, mk, name)
	measureCasterLatency(ctx, mk, name)
	if err := w.f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]map[string]any{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		if rec["type"] != "metric" {
			continue
		}
		dims, _ := rec["dims"].(map[string]any)
		out[rec["metric"].(string)+"/"+evalNum(dims["receivers"])] = rec
	}
	return out, ctx.failures
}

func evalNum(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// The records of a correct caster state that every Send delivered to every
// receiver, and the time is that of a Send that did so.
func TestCasterBatteriesRecordDeliveries(t *testing.T) {
	mk := casterLikes()["BaselineRendezvousCaster"]
	if mk == nil {
		t.Fatal("BaselineRendezvousCaster is not registered")
	}
	records, failures := casterBatteryRecords(t, mk, "BaselineRendezvousCaster")
	if len(failures) != 0 {
		t.Fatalf("harness failures: %v", failures)
	}
	for key, receivers := range map[string]float64{"send_ns_op/1": 1, "send_ns_op/4": 4, "send_latency_ns/8": 8} {
		rec := records[key]
		if rec == nil {
			t.Errorf("no record %s; have %d records", key, len(records))
			continue
		}
		if got := rec["delivered_per_send"]; got != receivers {
			t.Errorf("%s: delivered_per_send = %v, want %v", key, got, receivers)
		}
		if rec["deliveries_missing"] != 0.0 || rec["exactly_once_violations"] != 0.0 {
			t.Errorf("%s: deliveries_missing = %v, exactly_once_violations = %v, want 0 and 0", key, rec["deliveries_missing"], rec["exactly_once_violations"])
		}
		if mean, _ := rec["mean"].(float64); mean <= 0 {
			t.Errorf("%s: mean = %v, want a positive duration", key, rec["mean"])
		}
		if checked, _ := rec["checked"].(float64); checked <= 0 {
			t.Errorf("%s: checked = %v, want the number of deliveries expected", key, rec["checked"])
		}
	}
	// With 4 receivers a Send hands over four values, so it cannot cost less
	// than a Send to one. The batteries this replaced reported a few
	// nanoseconds at every receiver count, which is what gave them away.
	one, _ := records["send_ns_op/1"]["mean"].(float64)
	four, _ := records["send_ns_op/4"]["mean"].(float64)
	if four <= one {
		t.Errorf("send to 4 receivers took %.0f ns and to 1 receiver %.0f ns; delivering more must cost more", four, one)
	}
}

// A caster whose Send returns at once without delivering is recorded as
// having delivered nothing, and fails the integrity gate of the report. Its
// short send time is not a result.
func TestCasterBatteriesExposeSendsThatDeliverNothing(t *testing.T) {
	mk := func() casterLike {
		return casterLike{send: func(int) int { return 0 }, add: func(int) int { return 0 }, c: make(chan int), rearmEach: true}
	}
	records, failures := casterBatteryRecords(t, mk, "Dropper")
	if len(failures) != 0 {
		t.Fatalf("harness failures: %v", failures)
	}
	rec := records["send_ns_op/4"]
	if rec == nil {
		t.Fatalf("no send_ns_op record at 4 receivers; have %d records", len(records))
	}
	if rec["delivered_per_send"] != 0.0 {
		t.Errorf("delivered_per_send = %v, want 0", rec["delivered_per_send"])
	}
	if missing, _ := rec["deliveries_missing"].(float64); missing <= 0 || missing != rec["checked"] {
		t.Errorf("deliveries_missing = %v with checked = %v, want every expected delivery missing", rec["deliveries_missing"], rec["checked"])
	}
	metrics := []map[string]any{}
	for _, r := range records {
		metrics = append(metrics, r)
	}
	if status, n := integrityOf(metrics, "Dropper", "caster"); status != "failed" || n <= 0 {
		t.Errorf("integrityOf = %q, %v; want failed with a positive count", status, n)
	}
}

// A caster whose Send reports a delivery that no receiver took is a harness
// failure: the run is marked failed instead of recording a time.
func TestCasterBatteriesFailOnAPhantomDelivery(t *testing.T) {
	saved := casterArmGuard
	casterArmGuard = 50 * time.Millisecond
	defer func() { casterArmGuard = saved }()
	mk := func() casterLike {
		return casterLike{send: func(int) int { return 1 }, add: func(int) int { return 0 }, c: make(chan int), rearmEach: true}
	}
	_, failures := casterBatteryRecords(t, mk, "Phantom")
	if len(failures) == 0 {
		t.Fatal("no harness failure for a Send that reported a delivery nobody took")
	}
}

// The steady-state trace of a caster shows deliveries: every Send is followed
// by its receivers taking the value.
func TestCasterTraceShowsDeliveries(t *testing.T) {
	const ops = 40
	for _, name := range []string{"BaselineRendezvousCaster", "ChanCaster"} {
		mk := casterLikes()[name]
		if mk == nil {
			t.Fatalf("%s is not registered", name)
		}
		res := collectCasterTrace(mk(), ops, 1200, 2*time.Second)
		if res.outcome != "completed" || res.opsCompleted != ops {
			t.Fatalf("%s: outcome %q after %d sends, want completed after %d", name, res.outcome, res.opsCompleted, ops)
		}
		pubs, recvs := 0, 0
		for _, e := range res.events {
			switch e.Kind {
			case "pub":
				pubs++
			case "recv":
				recvs++
			}
		}
		if pubs != ops || recvs != ops*res.subs {
			t.Errorf("%s: trace has %d sends and %d receives, want %d and %d", name, pubs, recvs, ops, ops*res.subs)
		}
	}
}

// Runs taken before the caster batteries measured delivery get no caster
// ranking, and the reason is carried to every score.
func TestCasterFamilyOfOlderContractsIsNotRanked(t *testing.T) {
	metrics := []map[string]any{}
	for i := range minRankedImpls + 1 {
		metrics = append(metrics, map[string]any{"type": "metric", "family": "caster", "impl": "C" + string(rune('a'+i)),
			"metric": "send_ns_op", "dims": map[string]any{"receivers": 8.0}, "mean": float64(10 + i)})
	}
	for generator, wantRanked := range map[string]bool{"1.0.0": false, "1.1.0": false, casterDeliveryContract: false, isolationContract: true, "1.10.0": true, "2.0.0": true} {
		dd := deriveReport(&reportData{meta: map[string]any{"generator_version": generator}, metrics: metrics}, nil)
		s := dd.Scores["caster"]["Ca"]
		if s.Ranked != wantRanked {
			t.Errorf("generator %s: ranked = %v, want %v", generator, s.Ranked, wantRanked)
		}
		reason, unranked := dd.Unranked["caster"]
		if unranked == wantRanked || (!wantRanked && (reason == "" || s.UnrankedReason != reason || s.Score != 0)) {
			t.Errorf("generator %s: unranked reason %q, score %+v", generator, reason, s)
		}
	}
	for have, want := range map[string]bool{"": false, "1.1.0": true, "1.2.0": false, "1.10.0": false, "0.9.9": true} {
		if got := generatorBefore(have, casterDeliveryContract); got != want {
			t.Errorf("generatorBefore(%q, %q) = %v, want %v", have, casterDeliveryContract, got, want)
		}
	}
}
