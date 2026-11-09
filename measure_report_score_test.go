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
