package bigbuff

// Key findings: the data-driven observations shown on the report's findings cards.

import (
	"fmt"
	"math"
	"strings"
)

// --- Observations: data-driven key findings ---

func computeObservations(data *reportData, dd derivedData) []map[string]any {
	obs := []map[string]any{}
	add := func(kind, title, detail, anchor string) {
		obs = append(obs, map[string]any{"kind": kind, "title": title, "detail": detail, "anchor": anchor})
	}
	metrics := data.metrics

	// 1. Stalled-consumer blocking fraction (broadcast family).
	blocked, completed, total := 0, 0, 0
	for _, m := range metrics {
		if m["metric"] == "stalled_consumer_ops_completed" && m["family"] == "broadcast" {
			total++
			switch m["outcome"] {
			case "publisher_blocked":
				blocked++
			case "completed":
				completed++
			}
		}
	}
	if total > 0 {
		add("pathology",
			fmt.Sprintf("%d of %d broadcast implementations park the publisher behind a stalled consumer", blocked, total),
			"Lock-step and backpressured designs cannot proceed past a consumer that never receives; lossy and coalescing designs degrade instead. Both behaviours are documented in the attachment registry and measured in the Pathology Explorer.",
			"#pathology")
	}

	// 2. Allocation-free publishers at 8 subscribers.
	allocFree := []string{}
	for _, m := range metrics {
		if m["metric"] == "publish_allocs_op" && m["family"] == "broadcast" {
			if dimsSubs(m) == 8 {
				if v, ok := recMean(m); ok && v == 0 {
					allocFree = append(allocFree, fmt.Sprint(m["impl"]))
				}
			}
		}
	}
	if len(allocFree) > 0 {
		add("efficiency",
			fmt.Sprintf("%d broadcast implementations publish at 8 subscribers with zero allocation per op", len(allocFree)),
			strings.Join(allocFree, ", "),
			"#memory")
	}

	// 3. Fanout flatness champion.
	var bestScale float64 = math.Inf(1)
	var bestScaleImpl string
	for family, implScores := range dd.Scores {
		if family != "broadcast" {
			continue
		}
		for impl, s := range implScores {
			if v, ok := s.Metrics["scaling_ratio_64_over_1"].(float64); ok && v < bestScale {
				bestScale, bestScaleImpl = v, impl
			}
		}
	}
	if !math.IsInf(bestScale, 1) {
		add("scaling",
			fmt.Sprintf("%s is the flattest publisher (%.2fx cost growth from 1 to 64 subscribers)", bestScaleImpl, bestScale),
			"Publish cost at 64 subscribers relative to 1 subscriber, geometric mean of reps. Flat designs scale to high fan-out without amortisation tricks.",
			"#scaling")
	}

	// 4. Highest pinned backlog in the stalled-consumer probe.
	var maxRetained uint64
	var maxRetainedImpl string
	var maxRetainedOps float64
	for _, m := range metrics {
		if m["metric"] == "stalled_consumer_ops_completed" && m["family"] == "broadcast" {
			if v, ok := m["approx_retained_bytes"].(float64); ok {
				if uint64(v) > maxRetained {
					maxRetained, maxRetainedImpl = uint64(v), fmt.Sprint(m["impl"])
					maxRetainedOps, _ = m["ops_planned"].(float64)
				}
			}
		}
	}
	if maxRetained > 0 {
		add("memory",
			fmt.Sprintf("%s pins ~%s per stalled consumer after %d publishes", maxRetainedImpl, humanBytes(float64(maxRetained)), int(maxRetainedOps)),
			"Retained heap attributable to the backlog of one permanently stalled subscriber, measured after a GC of the probe. Unbounded-retention designs pin everything; ring and coalescing designs pin capacity or nothing.",
			"#pathology")
	}

	// 5. Withdrawal compensation across caster family.
	roundsTotal, ok := 0, false
	cwdImpls, allPass := []string{}, true
	for _, m := range metrics {
		if m["metric"] == "withdrawal_compensation_success" {
			v, vok := m["mean"].(float64)
			ro, rok := m["dims"].(map[string]any)["rounds"].(float64)
			if !vok || !rok {
				continue
			}
			roundsTotal += int(ro)
			ok = true
			if v < 1.0 {
				allPass = false
				cwdImpls = append(cwdImpls, fmt.Sprint(m["impl"]))
			}
		}
	}
	if ok {
		if allPass {
			add("correctness",
				fmt.Sprintf("Every eligible rendezvous caster compensated mid-flight withdrawal in 100%% of %d rounds", roundsTotal),
				"With one registered intent that never receives, an Add(-1) during a blocked Send released it with the compensated count in every round, for every caster except those excluded as exposure-limited.",
				"#leaderboard")
		} else {
			add("correctness",
				fmt.Sprintf("Withdrawal compensation failed for: %s", strings.Join(cwdImpls, ", ")),
				"A registered intent that never receives leaves Send parked in these designs even after Add(-1). See the withdrawal_compensation_success metric.",
				"#leaderboard")
		}
	}

	// 6. Torn reads.
	var tornTotal, checkedTotal float64
	for _, m := range metrics {
		if m["metric"] == "torn_reads" {
			tornTotal += mustFloat(m, "violations")
			checkedTotal += mustFloat(m, "checked")
		}
	}
	add("correctness",
		fmt.Sprintf("%0.0f torn multi-word reads observed across %0.0f sampled payload deliveries", tornTotal, checkedTotal),
		"Multi-word payload integrity under 8-way concurrent fan-out. Seqlock-style implementations are the risk class this scenario probes; any violation is a hard failure for adoption.",
		"#data")

	// 7. Churn champion.
	var bestChurn float64 = math.Inf(1)
	var bestChurnImpl string
	for _, m := range metrics {
		if m["metric"] == "churn_ns_op" && m["family"] == "broadcast" {
			if v, ok := recMean(m); ok && v < bestChurn {
				bestChurn, bestChurnImpl = v, fmt.Sprint(m["impl"])
			}
		}
	}
	if !math.IsInf(bestChurn, 1) {
		add("efficiency",
			fmt.Sprintf("%s has the cheapest subscribe/unsubscribe pair (~%s)", bestChurnImpl, humanNs(bestChurn)),
			"Relevant for high-churn topologies (connection-per-request, presence fan-out) where registration churn dominates steady-state cost.",
			"#churn")
	}

	// 8. Zero-subscriber cost champion.
	var bestZero float64 = math.Inf(1)
	var bestZeroImpl string
	for _, m := range metrics {
		if m["metric"] == "zero_subscriber_ns_op" {
			if v, ok := recMean(m); ok && v < bestZero {
				bestZero, bestZeroImpl = v, fmt.Sprint(m["impl"])
			}
		}
	}
	if !math.IsInf(bestZero, 1) {
		add("efficiency",
			fmt.Sprintf("%s publishes to an empty topic for ~%s", bestZeroImpl, humanNs(bestZero)),
			"Idle-heavy topics (config distribution with no listeners) measure the cost of publishing into the void; some designs amortise it to nearly nothing.",
			"#memory")
	}

	// 9. Exactly-once violations in scatter dispatch.
	var scatterViolations float64
	for _, m := range metrics {
		if m["metric"] == "dispatch_ns_op" {
			scatterViolations += mustFloat(m, "exactly_once_violations")
		}
	}
	add("correctness",
		fmt.Sprintf("%0.0f exactly-once violations across scatter dispatch runs", scatterViolations),
		"Every dispatched value must be received by exactly one worker; the count covers all receivers across all repetitions (warmup included).",
		"#data")

	// 10. Lossy drop-oldest delivery under burst contention.
	worstDelivery, worstDeliveryImpl := math.Inf(1), ""
	for _, m := range metrics {
		if m["metric"] == "stalled_consumer_ops_completed" && m["family"] == "payload" {
			// no per-sub delivery ratio here; skip
			_ = m
		}
	}
	for _, m := range metrics {
		if m["metric"] == "torn_reads" {
			_ = worstDelivery
			_ = worstDeliveryImpl
		}
	}

	return obs
}

func mustFloat(m map[string]any, key string) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return 0
}

func humanNs(ns float64) string {
	switch {
	case ns >= 1e6:
		return fmt.Sprintf("%.2f ms", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.2f µs", ns/1e3)
	default:
		return fmt.Sprintf("%.0f ns", ns)
	}
}

func humanBytes(b float64) string {
	switch {
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", b/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", b/(1<<10))
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}
