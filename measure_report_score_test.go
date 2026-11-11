package bigbuff

// Scoring rules of the report: the integrity gates that cap an adoptability score.

import "testing"

func TestIntegrityOfGates(t *testing.T) {
	metrics := []map[string]any{
		{"impl": "Clean", "family": "scatter", "metric": "dispatch_ns_op", "exactly_once_violations": float64(0)},
		{"impl": "Clean", "family": "scatter", "metric": "dispatch_ns_op", "exactly_once_violations": float64(0)},
		{"impl": "Dup", "family": "scatter", "metric": "dispatch_ns_op", "exactly_once_violations": float64(3)},
		{"impl": "Dup", "family": "scatter", "metric": "dispatch_ns_op", "exactly_once_violations": float64(4)},
		{"impl": "Other", "family": "broadcast", "metric": "publish_ns_op"},
		{"impl": "Lane", "family": "payload", "metric": "torn_reads", "violations": float64(0), "checked": float64(500)},
		{"impl": "Tear", "family": "payload", "metric": "torn_reads", "violations": float64(1), "checked": float64(500)},
	}
	cases := []struct {
		name, impl, family, want string
		value                    float64
	}{
		{"exactly-once clean across sweep points", "Clean", "scatter", "verified", 0},
		{"exactly-once violations summed across sweep points", "Dup", "scatter", "failed", 7},
		{"no accounting field, not scatter", "Other", "broadcast", "verified", 0},
		{"clean torn-read probe", "Lane", "payload", "verified", 0},
		{"torn read fails the payload lane", "Tear", "payload", "failed", 1},
		{"unknown implementation has no payload record", "Missing", "payload", "insufficient-data", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, value := integrityOf(metrics, tc.impl, tc.family)
			if got != tc.want || value != tc.value {
				t.Fatalf("integrityOf(%q, %q) = (%q, %v), want (%q, %v)", tc.impl, tc.family, got, value, tc.want, tc.value)
			}
		})
	}
}

// A family with fewer than minRankedImpls measured implementations gets no
// composite score: with two subjects the faster would be 100 and the other 0
// whatever separates them.
func TestSmallFamiliesAreNotRanked(t *testing.T) {
	var metrics []map[string]any
	add := func(family, metric, impl string, mean float64) {
		metrics = append(metrics, map[string]any{
			"type": "metric", "family": family, "impl": impl, "metric": metric,
			"dims": map[string]any{"senders": float64(8)}, "mean": mean,
		})
	}
	// Two exchangers 1 percent apart.
	add("exchange", "exchange_ns_op", "X1", 1000)
	add("exchange", "exchange_ns_op", "X2", 1010)
	// A family exactly at the threshold.
	for i := range minRankedImpls {
		add("caster", "send_ns_op", "C"+string(rune('a'+i)), float64(100*(i+1)))
	}
	dd := deriveReport(&reportData{metrics: metrics}, nil)

	for _, impl := range []string{"X1", "X2"} {
		s, ok := dd.Scores["exchange"][impl]
		if !ok {
			t.Fatalf("no score entry for %s", impl)
		}
		if s.Ranked || s.Score != 0 || s.Peers != 2 || s.Verdict != "Not ranked (family of 2)" {
			t.Errorf("%s: ranked=%v score=%d peers=%d verdict=%q, want an unranked entry", impl, s.Ranked, s.Score, s.Peers, s.Verdict)
		}
		for dim, v := range s.Dims {
			if v != 0 {
				t.Errorf("%s: dimension %s = %d, want 0 for an unranked family", impl, dim, v)
			}
		}
		if got := s.Metrics["exchange_ns_op@8"]; got == nil {
			t.Errorf("%s: the measurement itself must still be reported, metrics = %v", impl, s.Metrics)
		}
	}
	best, worst := dd.Scores["caster"]["Ca"], dd.Scores["caster"]["C"+string(rune('a'+minRankedImpls-1))]
	if !best.Ranked || !worst.Ranked || best.Score <= worst.Score {
		t.Errorf("a family of %d must be ranked with the faster implementation ahead: best %+v, worst %+v", minRankedImpls, best, worst)
	}
}

// An integrity failure is an absolute finding, not a rank: it is reported
// for an unranked family too.
func TestUnrankedFamilyStillReportsIntegrityFailure(t *testing.T) {
	metrics := []map[string]any{
		{"type": "metric", "family": "exchange", "impl": "Bad", "metric": "exchange_ns_op", "dims": map[string]any{"senders": float64(8)}, "mean": float64(10), "exactly_once_violations": float64(2)},
		{"type": "metric", "family": "exchange", "impl": "Good", "metric": "exchange_ns_op", "dims": map[string]any{"senders": float64(8)}, "mean": float64(20), "exactly_once_violations": float64(0)},
	}
	dd := deriveReport(&reportData{metrics: metrics}, nil)
	if got := dd.Scores["exchange"]["Bad"]; got.Verdict != "Do not adopt (integrity failure)" || got.Ranked {
		t.Errorf("Bad: %+v, want the integrity verdict on an unranked entry", got)
	}
	if got := dd.Scores["exchange"]["Good"].Verdict; got != "Not ranked (family of 2)" {
		t.Errorf("Good: verdict %q", got)
	}
}

func TestExchangePairingViolations(t *testing.T) {
	cases := []struct {
		name      string
		calls     []exchangeCall
		withdrawn []int
		want      int64
	}{
		{"two consistent swaps", []exchangeCall{{1, 2}, {2, 1}, {3, 4}, {4, 3}}, []int{5}, 0},
		{"nothing completed", nil, []int{1, 2}, 0},
		{"one-sided: the partner never completed", []exchangeCall{{1, 2}}, nil, 1},
		{"got its own value", []exchangeCall{{1, 1}}, nil, 1},
		{"value delivered to two callers", []exchangeCall{{1, 2}, {2, 1}, {3, 2}}, nil, 1},
		{"a withdrawn value was delivered", []exchangeCall{{1, 9}, {2, 1}}, []int{9}, 3},
	}
	for _, tc := range cases {
		if got := exchangePairingViolations(tc.calls, tc.withdrawn); got != tc.want {
			t.Errorf("%s: %d violations, want %d", tc.name, got, tc.want)
		}
	}
}
