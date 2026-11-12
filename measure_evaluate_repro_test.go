package bigbuff

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// evalPrevious is the run before the one being evaluated, loaded so that the
// evaluation can say whether this run's rankings were there last time too.
type evalPrevious struct {
	label   string // "08"
	data    *reportData
	derived derivedData
}

// previousRunResults returns the results file of the run numbered one below
// resultsPath's, and that run's label, or "" when there is none.
func previousRunResults(resultsPath string) (path, label string) {
	m := regexp.MustCompile(`^results-(\d\d)-01-results\.jsonl$`).FindStringSubmatch(filepath.Base(resultsPath))
	if m == nil {
		return "", ""
	}
	n, _ := strconv.Atoi(m[1])
	if n <= 1 {
		return "", ""
	}
	label = fmt.Sprintf("%02d", n-1)
	path = filepath.Join(filepath.Dir(resultsPath), "results-"+label+"-01-results.jsonl")
	if _, err := os.Stat(path); err != nil {
		return "", ""
	}
	return path, label
}

// loadPreviousRun loads the run before resultsPath's, or nil when there is
// none or it cannot be read.
func loadPreviousRun(resultsPath string) *evalPrevious {
	path, label := previousRunResults(resultsPath)
	if path == "" {
		return nil
	}
	data, err := loadReportData(path)
	if err != nil {
		return nil
	}
	var registry map[string]any
	if dump, _ := registryDumpForResults(path); dump != "" {
		registry, _ = readRegistryDump(dump)
	}
	return &evalPrevious{label: label, data: data, derived: deriveReport(data, registry)}
}

// spearman is the rank correlation of two equally long series, with tied
// values sharing the mean of their ranks. ok is false when either series is
// constant or there are fewer than three points.
func spearman(a, b []float64) (rho float64, ok bool) {
	if len(a) != len(b) || len(a) < 3 {
		return 0, false
	}
	ranks := func(v []float64) []float64 {
		idx := make([]int, len(v))
		for i := range idx {
			idx[i] = i
		}
		sort.Slice(idx, func(i, j int) bool { return v[idx[i]] < v[idx[j]] })
		out := make([]float64, len(v))
		for i := 0; i < len(idx); {
			j := i
			for j+1 < len(idx) && v[idx[j+1]] == v[idx[i]] {
				j++
			}
			mean := float64(i+j)/2 + 1
			for k := i; k <= j; k++ {
				out[idx[k]] = mean
			}
			i = j + 1
		}
		return out
	}
	ra, rb := ranks(a), ranks(b)
	var ma, mb float64
	for i := range ra {
		ma += ra[i]
		mb += rb[i]
	}
	ma /= float64(len(ra))
	mb /= float64(len(rb))
	var num, da, db float64
	for i := range ra {
		num += (ra[i] - ma) * (rb[i] - mb)
		da += (ra[i] - ma) * (ra[i] - ma)
		db += (rb[i] - mb) * (rb[i] - mb)
	}
	if da == 0 || db == 0 {
		return 0, false
	}
	return num / math.Sqrt(da*db), true
}

// headlineRecords returns each implementation's headline record of a family
// that has one (see evalHeadline): its throughput metric at 8 on the sweep
// dimension.
func headlineRecords(data *reportData, family string) map[string]map[string]any {
	out := map[string]map[string]any{}
	metric := evalHeadline[family]
	if metric == "" {
		return out
	}
	for _, m := range data.metrics {
		if m["family"] != family || m["metric"] != metric || dimsSubs(m) != 8 {
			continue
		}
		impl, _ := m["impl"].(string)
		out[impl] = m
	}
	return out
}

// agreementDims are the dimensions of the score, in the order the evaluation
// prints them.
var agreementDims = []string{"throughput", "latency", "allocations", "churn", "scaling", "stall_resilience"}

// dimMissing reports whether an implementation has no data for a dimension.
func dimMissing(s implScore, dim string) bool {
	for _, m := range s.DimsMissing {
		if m == dim {
			return true
		}
	}
	return false
}

// evalAgreement writes the section that says what of this run was also true
// of the run before it. A ranking that one run produced and the next did not
// is not a finding, and one run cannot tell which kind it has.
func evalAgreement(b *strings.Builder, in evaluationInput, families []string) {
	b.WriteString("## Agreement with the previous run\n\n")
	prev := in.previous
	if prev == nil {
		b.WriteString("There is no earlier run beside this one to compare it with. Nothing below or above has been reproduced.\n\n")
		return
	}
	meta, pmeta := in.data.meta, prev.data.meta
	gen, pgen := evalStr(meta, "generator_version"), evalStr(pmeta, "generator_version")
	prof, pprof := evalStr(meta, "profile"), evalStr(pmeta, "profile")
	if gen != pgen || prof != pprof {
		fmt.Fprintf(b, "Run %s was taken under record contract %s with the `%s` profile; this run is under %s with `%s`. Their numbers are not the same measurement, so they are not compared, and no ranking of this run has been reproduced.\n\n", prev.label, pgen, pprof, gen, prof)
		return
	}
	fmt.Fprintf(b, "Run %s was taken under the same record contract (%s) and profile (`%s`), at commit `%s`; this run is at `%s`. The table compares the two, group by group, over the implementations ranked in both. Rank correlation is Spearman's, on the scores: 1 means the same order, 0 means no relation. A place is a position in the group by score, ties sharing one.\n\n", prev.label, gen, prof, evalStr(pmeta, "commit"), evalStr(meta, "commit"))

	rows := [][]string{}
	dimRows := [][]string{}
	var moved []string
	for _, f := range families {
		cur, old := in.derived.Scores[f], prev.derived.Scores[f]
		if len(old) == 0 {
			continue
		}
		for impl, s := range cur {
			if o, ok := old[impl]; ok && o.Class != s.Class {
				moved = append(moved, fmt.Sprintf("%s `%s`: %s in run %s, %s in this run", f, impl, deliveryClassLabel(o.Class), prev.label, deliveryClassLabel(s.Class)))
			}
		}
		for _, class := range scoreClasses(cur) {
			group := f
			if class != "" {
				group += ", " + deliveryClassLabel(class)
			}
			list, rank := scoresOfClass(cur, class)
			oldList, oldRank := scoresOfClass(old, class)
			if len(list) == 0 || !list[0].Ranked {
				continue
			}
			if len(oldList) == 0 || !oldList[0].Ranked {
				rows = append(rows, []string{group, "not ranked in run " + prev.label, "", "", "", "", ""})
				continue
			}
			oldPlace, oldScore := map[string]int{}, map[string]implScore{}
			for i, s := range oldList {
				oldPlace[s.Impl], oldScore[s.Impl] = oldRank[i], s
			}
			var a, c []float64
			sameVerdict, common := 0, 0
			var shifts []float64
			worst, worstImpl := 0, ""
			for i, s := range list {
				o, ok := oldScore[s.Impl]
				if !ok {
					continue
				}
				common++
				a, c = append(a, float64(s.Score)), append(c, float64(o.Score))
				if s.Verdict == o.Verdict {
					sameVerdict++
				}
				d := rank[i] - oldPlace[s.Impl]
				if d < 0 {
					d = -d
				}
				shifts = append(shifts, float64(d))
				if d > worst || (d == worst && worstImpl == "") {
					worst, worstImpl = d, fmt.Sprintf("%s (%d to %d)", s.Impl, oldPlace[s.Impl], rank[i])
				}
			}
			if common == 0 {
				continue
			}
			top := 0
			for i, s := range list {
				if rank[i] > 5 {
					break
				}
				if p, ok := oldPlace[s.Impl]; ok && p <= 5 {
					top++
				}
			}
			rho := "not defined"
			if r, ok := spearman(a, c); ok {
				rho = fmt.Sprintf("%.2f", r)
			}
			// The same for each dimension the score is made of. A score can
			// reproduce while one of its parts does not: the parts that
			// do carry it.
			dimRow := []string{group}
			for _, dim := range agreementDims {
				left := false
				for _, lists := range [][]implScore{list, oldList} {
					for _, u := range lists[0].DimsUniform {
						left = left || u == dim
					}
				}
				var x, y []float64
				for _, s := range list {
					o, ok := oldScore[s.Impl]
					if !ok || dimMissing(s, dim) || dimMissing(o, dim) {
						continue
					}
					x, y = append(x, float64(s.Dims[dim])), append(y, float64(o.Dims[dim]))
				}
				switch r, ok := spearman(x, y); {
				case left:
					dimRow = append(dimRow, "left out")
				case len(x) == 0:
					dimRow = append(dimRow, "no data")
				case !ok:
					dimRow = append(dimRow, "not defined")
				default:
					dimRow = append(dimRow, fmt.Sprintf("%.2f", r))
				}
			}
			dimRows = append(dimRows, dimRow)
			sort.Float64s(shifts)
			rows = append(rows, []string{group, strconv.Itoa(common), rho,
				fmt.Sprintf("%d of %d", sameVerdict, common),
				fmt.Sprintf("%.0f", evalQuantile(shifts, 0.5)), fmt.Sprintf("%d: %s", worst, worstImpl),
				fmt.Sprintf("%d", top)})
		}
	}
	if len(rows) == 0 {
		b.WriteString("No group is ranked in both runs.\n\n")
	} else {
		mdTable(b, []string{"Group", "Ranked in both", "Rank correlation of scores", "Same verdict", "Median places moved", "Most places moved", "Of this run's top five places, in the other's top five"}, rows)
	}
	if len(dimRows) > 0 {
		b.WriteString("The same rank correlation for each dimension of the score, on the dimension's own percentile ranks. A dimension that reproduces poorly contributes its weight in noise; \"left out\" is a dimension on which the whole group is tied in one of the runs, which is not part of the score.\n\n")
		mdTable(b, append([]string{"Group"}, agreementDims...), dimRows)
	}
	sort.Strings(moved)
	if len(moved) > 0 {
		b.WriteString("Implementations in a different delivery class in the two runs:\n\n")
		for _, m := range moved {
			b.WriteString("- " + m + "\n")
		}
		b.WriteString("\n")
	} else {
		b.WriteString("No implementation changed delivery class between the two runs.\n\n")
	}

	// The headline cost itself, before any ranking is made of it.
	rows = rows[:0]
	for _, f := range families {
		cur, old := headlineRecords(in.data, f), headlineRecords(prev.data, f)
		var ratios []float64
		overlap, n := 0, 0
		for impl, m := range cur {
			o, ok := old[impl]
			if !ok {
				continue
			}
			mean, ok1 := m["mean"].(float64)
			omean, ok2 := o["mean"].(float64)
			if !ok1 || !ok2 || mean <= 0 || omean <= 0 {
				continue
			}
			n++
			r := mean / omean
			if r < 1 {
				r = 1 / r
			}
			ratios = append(ratios, r)
			lo, okLo := m["ci95_low"].(float64)
			hi, okHi := m["ci95_high"].(float64)
			olo, okOLo := o["ci95_low"].(float64)
			ohi, okOHi := o["ci95_high"].(float64)
			if okLo && okHi && okOLo && okOHi && lo <= ohi && olo <= hi {
				overlap++
			}
		}
		if n == 0 {
			continue
		}
		sort.Float64s(ratios)
		rows = append(rows, []string{f, strconv.Itoa(n), fmt.Sprintf("%d (%.0f%%)", overlap, 100*float64(overlap)/float64(n)),
			fmt.Sprintf("%.2f×", evalQuantile(ratios, 0.5)), fmt.Sprintf("%.2f×", evalQuantile(ratios, 0.9)), fmt.Sprintf("%.2f×", ratios[len(ratios)-1])})
	}
	if len(rows) > 0 {
		b.WriteString("The headline cost of each implementation (one publish or send at 8 subscribers), this run against the other, for the families that have one. The ratio is the larger figure over the smaller.\n\n")
		mdTable(b, []string{"Family", "Implementations in both", "95% intervals of the two runs overlap", "Median ratio", "90th percentile ratio", "Largest ratio"}, rows)
	}
	b.WriteString("Two runs agreeing is evidence about this machine on these days. It is not evidence about another machine.\n\n")
}
