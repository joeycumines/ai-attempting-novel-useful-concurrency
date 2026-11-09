package bigbuff

// Statistical primitives for the measurement harness (see measurement.md for
// the methodology). Everything here is dependency-free and deterministic.

import (
	"math"
	"sort"
)

// studentT975[df] is the two-tailed 97.5% critical value of Student's t
// distribution (i.e. the 95% confidence interval multiplier) for small
// degrees of freedom; for df >= 30 the normal approximation 1.959964 is used.
var studentT975 = [31]float64{
	0: 12.706205, 1: 4.302653, 2: 3.182446, 3: 2.776445, 4: 2.570582,
	5: 2.446912, 6: 2.364624, 7: 2.306004, 8: 2.262157, 9: 2.228139,
	10: 2.200985, 11: 2.178813, 12: 2.160369, 13: 2.144787, 14: 2.131450,
	15: 2.119905, 16: 2.109816, 17: 2.100922, 18: 2.093024, 19: 2.085963,
	20: 2.079614, 21: 2.073873, 22: 2.068658, 23: 2.063899, 24: 2.059539,
	25: 2.055529, 26: 2.051830, 27: 2.048407, 28: 2.045230, 29: 2.042272,
	30: 2.039514,
}

const normalZ975 = 1.959964

func tCritical975(df int) float64 {
	if df <= 0 {
		return normalZ975
	}
	if df < len(studentT975) {
		return studentT975[df-1]
	}
	return normalZ975
}

// summary is the canonical aggregate record for one measurement cell
// (one implementation, one metric, one dimension set, across R reps).
type summary struct {
	N             int     `json:"n"`
	Mean          float64 `json:"mean"`
	StdDev        float64 `json:"stddev"` // sample standard deviation (n-1)
	CV            float64 `json:"cv"`     // coefficient of variation, std/mean (0 if mean==0)
	Min           float64 `json:"min"`
	P50           float64 `json:"p50"`
	P90           float64 `json:"p90"`
	P99           float64 `json:"p99"`
	Max           float64 `json:"max"`
	CI95HalfWidth float64 `json:"ci95_halfwidth"` // t(0.975, n-1) * stddev / sqrt(n)
	CI95Low       float64 `json:"ci95_low"`
	CI95High      float64 `json:"ci95_high"`
}

func percentileSorted(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	pos := q * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func summarize(samples []float64) summary {
	s := summary{N: len(samples)}
	if len(samples) == 0 {
		return s
	}
	sorted := make([]float64, len(samples))
	copy(sorted, samples)
	sort.Float64s(sorted)
	var sum float64
	for _, v := range samples {
		sum += v
	}
	s.Mean = sum / float64(len(samples))
	var sq float64
	for _, v := range samples {
		d := v - s.Mean
		sq += d * d
	}
	if len(samples) > 1 {
		s.StdDev = math.Sqrt(sq / float64(len(samples)-1))
	}
	if s.Mean != 0 {
		s.CV = s.StdDev / math.Abs(s.Mean)
	}
	s.Min = sorted[0]
	s.Max = sorted[len(sorted)-1]
	s.P50 = percentileSorted(sorted, 0.50)
	s.P90 = percentileSorted(sorted, 0.90)
	s.P99 = percentileSorted(sorted, 0.99)
	if len(samples) > 1 {
		s.CI95HalfWidth = tCritical975(len(samples)-1) * s.StdDev / math.Sqrt(float64(len(samples)))
	}
	s.CI95Low = s.Mean - s.CI95HalfWidth
	s.CI95High = s.Mean + s.CI95HalfWidth
	return sanitizeSummary(s)
}

func sanitizeSummary(s summary) summary {
	f := func(v float64) float64 {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0
		}
		return v
	}
	s.Mean, s.StdDev, s.CV = f(s.Mean), f(s.StdDev), f(s.CV)
	s.Min, s.Max = f(s.Min), f(s.Max)
	s.P50, s.P90, s.P99 = f(s.P50), f(s.P90), f(s.P99)
	s.CI95HalfWidth, s.CI95Low, s.CI95High = f(s.CI95HalfWidth), f(s.CI95Low), f(s.CI95High)
	return s
}

// --- Welch's t-test (unequal variances) ---

// logGamma via Lanczos approximation (Numerical Recipes, g=7, 9-term series).
func logGamma(z float64) float64 {
	g := 7.0
	coef := [9]float64{
		0.99999999999980993, 676.5203681218851, -1259.1392167224028,
		771.32342877765313, -176.61502916214059, 12.507343278686905,
		-0.13857109526572012, 9.9843695780195716e-6, 1.5056327351493116e-7,
	}
	if z < 0.5 {
		// reflection formula
		return math.Log(math.Pi/math.Sin(math.Pi*z)) - logGamma(1-z)
	}
	z -= 1
	x := coef[0]
	for i := 1; i < 9; i++ {
		x += coef[i] / (z + float64(i))
	}
	t := z + g + 0.5
	return 0.5*math.Log(2*math.Pi) + (z+0.5)*math.Log(t) - t + math.Log(x)
}

func betacf(a, b, x float64) float64 {
	const maxIter = 300
	const eps = 3.0e-14
	const fpmin = 1.0e-300
	qab := a + b
	qap := a + 1
	qam := a - 1
	c := 1.0
	d := 1 - qab*x/qap
	if math.Abs(d) < fpmin {
		d = fpmin
	}
	d = 1 / d
	h := d
	for m := 1; m <= maxIter; m++ {
		m2 := 2 * m
		aa := float64(m) * (b - float64(m)) * x / ((qam + float64(m2)) * (a + float64(m2)))
		d = 1 + aa*d
		if math.Abs(d) < fpmin {
			d = fpmin
		}
		c = 1 + aa/c
		if math.Abs(c) < fpmin {
			c = fpmin
		}
		d = 1 / d
		h *= d * c
		aa = -(a + float64(m)) * (qab + float64(m)) * x / ((a + float64(m2)) * (qap + float64(m2)))
		d = 1 + aa*d
		if math.Abs(d) < fpmin {
			d = fpmin
		}
		c = 1 + aa/c
		if math.Abs(c) < fpmin {
			c = fpmin
		}
		d = 1 / d
		del := d * c
		h *= del
		if math.Abs(del-1) <= eps {
			break
		}
	}
	return h
}

// regularizedIncompleteBeta I_x(a, b).
func regularizedIncompleteBeta(a, b, x float64) float64 {
	if x <= 0 {
		return 0
	}
	if x >= 1 {
		return 1
	}
	lnBt := logGamma(a+b) - logGamma(a) - logGamma(b) + a*math.Log(x) + b*math.Log(1-x)
	bt := math.Exp(lnBt)
	if x < (a+1)/(a+b+2) {
		return bt * betacf(a, b, x) / a
	}
	return 1 - bt*betacf(b, a, 1-x)/b
}

// studentTCDF returns the two-tailed p-value for t with df degrees of freedom.
func studentTTwoTailedP(t, df float64) float64 {
	if df <= 0 || math.IsNaN(t) {
		return math.NaN()
	}
	x := df / (df + t*t)
	return regularizedIncompleteBeta(df/2, 0.5, x)
}

// welchTTest compares two samples (as summaries) and returns the Welch t
// statistic, the Satterthwaite degrees of freedom, and the two-tailed p.
func welchTTest(a, b summary) (t, df, p float64) {
	if a.N < 2 || b.N < 2 {
		return math.NaN(), math.NaN(), math.NaN()
	}
	va := a.StdDev * a.StdDev / float64(a.N)
	vb := b.StdDev * b.StdDev / float64(b.N)
	denom := va + vb
	if denom == 0 {
		return math.NaN(), math.NaN(), math.NaN()
	}
	t = (a.Mean - b.Mean) / math.Sqrt(denom)
	df = denom * denom / (va*va/float64(a.N-1) + vb*vb/float64(b.N-1))
	p = studentTTwoTailedP(math.Abs(t), df)
	return t, df, p
}

// --- Fixed log2-scaled latency histogram ---

// histogramBuckets covers 2^k ns for k in [7, 39] (~100ns .. ~500s), i.e.
// bucket i holds [2^(7+i), 2^(8+i)) ns. Counts has len(edges)+1 entries.
const histMinExp = 7
const histBucketCount = 33

type histogram struct {
	// Edges[i] is the upper bound (exclusive) of bucket i, in nanoseconds;
	// the final implicit bucket is overflow.
	Edges  []float64 `json:"edges"`
	Counts []int     `json:"counts"`
	Total  int       `json:"total"`
}

func newHistogram() *histogram {
	h := &histogram{}
	for i := 0; i < histBucketCount; i++ {
		h.Edges = append(h.Edges, math.Pow(2, float64(histMinExp+i)))
	}
	h.Counts = make([]int, histBucketCount+1)
	return h
}

func (h *histogram) add(ns float64) {
	lo, hi := 0, len(h.Edges)
	for lo < hi {
		mid := (lo + hi) / 2
		if ns < h.Edges[mid] {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	h.Counts[lo]++
	h.Total++
}
