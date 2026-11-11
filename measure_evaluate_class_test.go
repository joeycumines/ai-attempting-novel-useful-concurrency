package bigbuff

import (
	"fmt"
	"sort"
	"strings"
)

// scoreClasses returns the delivery classes present among a family's scores,
// in the order they are presented. A family without classes yields one empty
// class.
func scoreClasses(scores map[string]implScore) []string {
	present := map[string]bool{}
	for _, s := range scores {
		present[s.Class] = true
	}
	out := []string{}
	for _, c := range []string{classDeliversAll, classDrops, ""} {
		if present[c] {
			out = append(out, c)
		}
	}
	return out
}

// scoresOfClass returns the scores of one class, best first, with the rank
// of each (equal scores share a rank).
func scoresOfClass(scores map[string]implScore, class string) (list []implScore, rank []int) {
	for _, s := range scores {
		if s.Class == class {
			list = append(list, s)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Score != list[j].Score {
			return list[i].Score > list[j].Score
		}
		return list[i].Impl < list[j].Impl
	})
	rank = make([]int, len(list))
	for i := range list {
		rank[i] = i + 1
		if i > 0 && list[i].Score == list[i-1].Score {
			rank[i] = rank[i-1]
		}
	}
	return list, rank
}

// evalPercent renders a delivered fraction. Everything is "100%" only when
// it is everything: 0.99996 is not rounded up to it.
func evalPercent(f float64) string {
	switch {
	case f >= 1:
		return "100%"
	case f >= 0.9995:
		return "under 100%"
	case f >= 0.1:
		return fmt.Sprintf("%.0f%%", 100*f)
	case f >= 0.01:
		return fmt.Sprintf("%.1f%%", 100*f)
	default:
		return fmt.Sprintf("%.2f%%", 100*f)
	}
}

// evalClassScoring is the Scoring cell of the run table for a family: how
// many implementations each of its delivery classes has, and which of them
// are ranked.
func evalClassScoring(scores map[string]implScore) string {
	classes := scoreClasses(scores)
	if len(classes) == 1 && classes[0] == "" {
		return "ranked"
	}
	parts := []string{}
	for _, c := range classes {
		list, _ := scoresOfClass(scores, c)
		label := deliveryClassLabel(c)
		if c == "" {
			label = "no delivery figures"
		}
		state := "ranked"
		if !list[0].Ranked {
			state = "not ranked"
		}
		parts = append(parts, fmt.Sprintf("%s: %d, %s", label, len(list), state))
	}
	return "in delivery classes (" + strings.Join(parts, "; ") + ")"
}

// evalDelivered returns the least, median and greatest delivered fraction at
// 8 subscribers over a list of scores, and whether any carries one.
func evalDelivered(list []implScore) (least, median, most float64, ok bool) {
	vals := []float64{}
	for _, s := range list {
		if f, has := s.Metrics["delivered_fraction@8"].(float64); has {
			vals = append(vals, f)
		}
	}
	if len(vals) == 0 {
		return 0, 0, 0, false
	}
	sort.Float64s(vals)
	return vals[0], vals[len(vals)/2], vals[len(vals)-1], true
}
