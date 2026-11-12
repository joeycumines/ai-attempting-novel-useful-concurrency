package bigbuff

// Tests of the latency batteries' record (record contract 1.6.0): several
// episodes, the interval taken across them, and the figure the latency
// dimension ranks.

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// latencyTestRecords runs fn against a context that writes to a temporary
// file and returns the metric records it wrote, with the harness failures.
func latencyTestRecords(t *testing.T, profile measureProfile, fn func(*measureContext)) ([]map[string]any, []string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "out.jsonl")
	w, err := newMeasureWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &measureContext{out: w, started: time.Now(), profile: profile}
	fn(ctx)
	if err := w.f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		if rec["type"] == "metric" {
			out = append(out, rec)
		}
	}
	return out, ctx.failures
}

// The interval of a latency record is across episodes. Three episodes that
// each repeat one value differ only from each other: an interval across the
// twelve samples is ±1.08, the one across the three episodes is ±4.97.
func TestLatencyIntervalIsTakenAcrossEpisodes(t *testing.T) {
	records, _ := latencyTestRecords(t, measureProfile{Name: "test"}, func(ctx *measureContext) {
		lat := newLatencyEpisodes()
		for _, v := range []time.Duration{1, 3, 5} {
			lat.begin(4)
			for range 4 {
				lat.add(v)
			}
		}
		lat.write(ctx, map[string]any{"metric": "publish_latency_ns", "family": "broadcast", "impl": "X"})

		one := newLatencyEpisodes()
		one.begin(4)
		for _, v := range []time.Duration{1, 2, 3, 4} {
			one.add(v)
		}
		one.write(ctx, map[string]any{"metric": "publish_latency_ns", "family": "broadcast", "impl": "Single"})
	})
	if len(records) != 2 {
		t.Fatalf("%d records, want 2", len(records))
	}
	rec, single := records[0], records[1]
	num := func(m map[string]any, k string) float64 { f, _ := m[k].(float64); return f }
	if got := num(rec, "mean"); got != 3 {
		t.Errorf("mean = %v, want 3 (pooled over the twelve samples)", got)
	}
	if num(rec, "n") != 12 || num(rec, "ops") != 12 || num(rec, "episodes") != 3 {
		t.Errorf("n = %v, ops = %v, episodes = %v; want 12, 12, 3", rec["n"], rec["ops"], rec["episodes"])
	}
	want := tCritical975(2) * 2 / math.Sqrt(3)
	if got := num(rec, "ci95_halfwidth"); math.Abs(got-want) > 1e-9 {
		t.Errorf("ci95_halfwidth = %v, want %v (across the three episode means)", got, want)
	}
	if lo, hi := num(rec, "ci95_low"), num(rec, "ci95_high"); math.Abs(lo-(3-want)) > 1e-9 || math.Abs(hi-(3+want)) > 1e-9 {
		t.Errorf("ci95 = [%v, %v], want [%v, %v]", lo, hi, 3-want, 3+want)
	}
	if rec["ci95_basis"] != "episodes" {
		t.Errorf("ci95_basis = %v, want episodes", rec["ci95_basis"])
	}
	p90, _ := rec["episode_p90"].([]any)
	if len(p90) != 3 || p90[0] != 1.0 || p90[1] != 3.0 || p90[2] != 5.0 {
		t.Errorf("episode_p90 = %v, want [1 3 5]", rec["episode_p90"])
	}
	if got := num(rec, "episode_p90_mean"); got != 3 {
		t.Errorf("episode_p90_mean = %v, want 3", got)
	}

	// One episode: nothing to take the interval across, and the record says
	// that it was taken across the samples.
	if single["ci95_basis"] != "samples" || num(single, "episodes") != 1 {
		t.Errorf("single episode: ci95_basis = %v, episodes = %v; want samples, 1", single["ci95_basis"], single["episodes"])
	}
	wantSingle := summarize([]float64{1, 2, 3, 4}).CI95HalfWidth
	if got := num(single, "ci95_halfwidth"); math.Abs(got-wantSingle) > 1e-9 {
		t.Errorf("single episode: ci95_halfwidth = %v, want %v", got, wantSingle)
	}
	if _, has := single["episode_p90_mean"]; has {
		t.Errorf("single episode: the record carries episode_p90_mean, which needs two episodes")
	}
}

// Each latency battery times the number of episodes the profile asks for,
// every one a fresh instance, and counts every timed operation.
func TestLatencyBatteriesTimeEveryEpisode(t *testing.T) {
	const episodes, samples = 3, 16
	profile := measureProfile{Name: "test", LatencySamples: samples, LatencyEpisodes: episodes}

	var bfactory func() Broadcaster
	built := 0
	for _, f := range AllBroadcasters() {
		probe := f()
		name := probe.Name()
		closeWithWatchdog(probe, 2*time.Second)
		if name == "BaselineFanout (standard)" {
			bfactory = func() Broadcaster { built++; return f() }
		}
	}
	if bfactory == nil {
		t.Fatal("BaselineFanout (standard) is not registered")
	}
	caster := casterLikes()["BaselineRendezvousCaster"]
	if caster == nil {
		t.Fatal("BaselineRendezvousCaster is not registered")
	}
	casters := 0

	records, failures := latencyTestRecords(t, profile, func(ctx *measureContext) {
		measureBroadcastLatency(ctx, bfactory, "BaselineFanout (standard)")
		measureCasterLatency(ctx, func() casterLike { casters++; return caster() }, "BaselineRendezvousCaster")
	})
	if len(failures) != 0 {
		t.Fatalf("harness failures: %v", failures)
	}
	if len(records) != 2 {
		t.Fatalf("%d records, want 2", len(records))
	}
	if built != episodes || casters != episodes {
		t.Errorf("built %d broadcasters and %d casters, want %d of each: an episode is a fresh instance", built, casters, episodes)
	}
	for _, rec := range records {
		id := rec["metric"].(string)
		if got, _ := rec["episodes"].(float64); got != episodes {
			t.Errorf("%s: episodes = %v, want %d", id, rec["episodes"], episodes)
		}
		for _, k := range []string{"episode_p50", "episode_p90", "episode_p99", "episode_mean"} {
			if list, _ := rec[k].([]any); len(list) != episodes {
				t.Errorf("%s: %s has %d entries, want %d", id, k, len(list), episodes)
			}
		}
		if n, _ := rec["n"].(float64); n != episodes*samples {
			t.Errorf("%s: n = %v, want %d", id, rec["n"], episodes*samples)
		}
		if rec["ops"] != rec["n"] {
			t.Errorf("%s: ops = %v and n = %v; both count the timed operations", id, rec["ops"], rec["n"])
		}
		if rec["ci95_basis"] != "episodes" {
			t.Errorf("%s: ci95_basis = %v, want episodes", id, rec["ci95_basis"])
		}
	}

	// A profile that does not say how many episodes still times one.
	if got := (measureProfile{}).latencyEpisodeCount(); got != 1 {
		t.Errorf("latencyEpisodeCount of an empty profile = %d, want 1", got)
	}
}

// The latency dimension ranks the mean over episodes of the 90th percentile.
// "Spiky" has the lower pooled mean and the higher 90th percentile; "Flat"
// the reverse. A record without episodes (a run before 1.6.0) is ranked on
// its mean, which is what that run's report ranked.
func TestLatencyDimensionRanksTheEpisodeP90(t *testing.T) {
	type lat struct {
		mean float64
		p90  []any
	}
	impls := map[string]lat{
		"Flat":   {mean: 500, p90: []any{510.0, 520.0, 530.0}},
		"Spiky":  {mean: 300, p90: []any{900.0, 950.0, 1000.0}},
		"Slow":   {mean: 2000, p90: []any{2100.0, 2200.0, 2300.0}},
		"Slower": {mean: 3000, p90: []any{3100.0, 3200.0, 3300.0}},
		"Worst":  {mean: 4000, p90: []any{4100.0, 4200.0, 4300.0}},
		"Last":   {mean: 5000, p90: []any{5100.0, 5200.0, 5300.0}},
	}
	derive := func(withEpisodes bool) map[string]implScore {
		data := &reportData{meta: map[string]any{"generator_version": measureGeneratorVersion, "profile": "quick"}}
		for i, name := range []string{"Flat", "Spiky", "Slow", "Slower", "Worst", "Last"} {
			l, ns := impls[name], float64(100+40*i)
			for _, subs := range []float64{1, 8, 64} {
				data.metrics = append(data.metrics, map[string]any{
					"type": "metric", "metric": "publish_ns_op", "family": "broadcast", "impl": name,
					"dims": map[string]any{"subs": subs}, "mean": ns, "ci95_low": ns - 1, "ci95_high": ns + 1,
					"samples": []any{ns - 1, ns, ns + 1}, "publishes": 1000.0, "receives": 1000.0 * subs, "delivered_fraction": 1.0,
				})
			}
			rec := map[string]any{
				"type": "metric", "metric": "publish_latency_ns", "family": "broadcast", "impl": name,
				"dims": map[string]any{"subs": 8.0}, "mean": l.mean,
			}
			if withEpisodes {
				rec["episode_p90"] = l.p90
			}
			data.metrics = append(data.metrics, rec)
		}
		return deriveReport(data, nil).Scores["broadcast"]
	}
	dim := func(s map[string]implScore, name string) int { return s[name].Dims["latency"] }

	now := derive(true)
	for name := range impls {
		if !now[name].Ranked {
			t.Fatalf("%s is not ranked (%s); the fixture is meant to be", name, now[name].UnrankedReason)
		}
	}
	if !(dim(now, "Flat") > dim(now, "Spiky")) {
		t.Errorf("with episodes: Flat scores %d and Spiky %d on latency; Flat has the lower 90th percentile", dim(now, "Flat"), dim(now, "Spiky"))
	}
	if got := now["Flat"].Metrics["publish_latency_ns@8"]; got != 520.0 {
		t.Errorf("Flat: reported latency %v, want 520 (the mean of its episodes' 90th percentiles)", got)
	}

	before := derive(false)
	if !(dim(before, "Spiky") > dim(before, "Flat")) {
		t.Errorf("without episodes: Spiky scores %d and Flat %d on latency; Spiky has the lower mean", dim(before, "Spiky"), dim(before, "Flat"))
	}
	if got := before["Flat"].Metrics["publish_latency_ns@8"]; got != 500.0 {
		t.Errorf("Flat without episodes: reported latency %v, want its mean 500", got)
	}
}
