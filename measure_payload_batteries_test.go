package bigbuff

// Payload (async API, multi-word payload) and gather batteries.

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// --- Payload family ---

type payloadDrain struct {
	counts    []atomic.Int64
	torn      atomic.Int64
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	stopped   bool
}

func startPayloadDrains(x asyncAPI, n int) *payloadDrain {
	g := &payloadDrain{}
	g.counts = make([]atomic.Int64, n)
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	type iterator struct {
		i    int
		subs func(yield func(pv) bool)
	}
	iters := make([]iterator, n)
	for i := 0; i < n; i++ {
		iters[i] = iterator{i: i, subs: x.subscribe(ctx)}
	}
	for _, it := range iters {
		it := it
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			defer func() {
				// drain iterators exit on ctx cancel
			}()
			it.subs(func(v pv) bool {
				if v[0] != v[1] || v[1] != v[2] || v[2] != v[3] {
					g.torn.Add(1)
				}
				g.counts[it.i].Add(1)
				return true
			})
		}()
	}
	return g
}

func (g *payloadDrain) stop(timeout time.Duration) bool {
	g.cancel()
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (g *payloadDrain) minReceived() int64 {
	if len(g.counts) == 0 {
		return 0
	}
	m := g.counts[0].Load()
	for i := 1; i < len(g.counts); i++ {
		if v := g.counts[i].Load(); v < m {
			m = v
		}
	}
	return m
}

func measurePayloadThroughput(ctx *measureContext, mk func() asyncAPI, name string) {
	scen, goodFit, expLimit := payloadAttachmentMeta(name)
	for _, subs := range ctx.profile.ScaleSubs {
		samples := make([]float64, 0, ctx.profile.Reps)
		lastOps := 0
		for r := 0; r < ctx.profile.Reps; r++ {
			x := mk()
			g := startPayloadDrains(x, subs)
			const warmup = 16
			w0 := time.Now()
			for i := 0; i < warmup; i++ {
				x.send(mkpv(i + 1))
			}
			est := time.Since(w0) / warmup
			n := calibrateOps(ctx.profile.TargetPerRep, ctx.profile.MinOps, ctx.profile.MaxOps, est)
			s := time.Now()
			for i := 0; i < n; i++ {
				x.send(mkpv(i + 1 + warmup))
			}
			d := time.Since(s)
			if !g.stop(3 * time.Second) {
				ctx.fail("%s: payload drains did not exit (subs=%d)", name, subs)
			}
			lastOps = n
			samples = append(samples, float64(d)/float64(n))
		}
		ctx.writeMetric(map[string]any{
			"metric": "publish_ns_op",
			"family": "payload",
			"impl":   name,
			"unit":   "ns",
			"dims":   map[string]any{"subs": subs},
			"ops":    lastOps,
			"reps":   len(samples),
			"lower_is_better":   true,
			"scenarios":         scen,
			"expected_good_fit": goodFit,
			"exposure_limited":  expLimit,
			"payload_words":     4,
		}, samples)
	}
}

func measurePayloadLatency(ctx *measureContext, mk func() asyncAPI, name string) {
	scen, goodFit, expLimit := payloadAttachmentMeta(name)
	const subs = 8
	x := mk()
	g := startPayloadDrains(x, subs)
	const warmup = 32
	for i := 0; i < warmup; i++ {
		x.send(mkpv(i + 1))
	}
	n := ctx.profile.LatencySamples
	durations := make([]float64, n)
	hist := newHistogram()
	for i := 0; i < n; i++ {
		t0 := time.Now()
		x.send(mkpv(i + 1 + warmup))
		d := time.Since(t0)
		durations[i] = float64(d)
		hist.add(float64(d))
	}
	if !g.stop(3 * time.Second) {
		ctx.fail("%s: payload drains did not exit after latency run", name)
	}
	ctx.writeMetric(map[string]any{
		"metric": "publish_latency_ns",
		"family": "payload",
		"impl":   name,
		"unit":   "ns",
		"dims":   map[string]any{"subs": subs},
		"ops":    n,
		"histogram":           hist,
		"lower_is_better":     true,
		"scenarios":           scen,
		"expected_good_fit":   goodFit,
		"exposure_limited":    expLimit,
		"instrumented":        true,
		"payload_words":       4,
	}, durations)
}

// measurePayloadIntegrity hammers the implementation with multi-word payloads
// and counts torn reads observed by 8 concurrent subscribers. The count is a
// measurement, not an assertion: it appears verbatim in the report.
func measurePayloadIntegrity(ctx *measureContext, mk func() asyncAPI, name string) {
	scen, goodFit, expLimit := payloadAttachmentMeta(name)
	const subs = 8
	x := mk()
	g := startPayloadDrains(x, subs)
	n := ctx.profile.IntegrityPublishes
	for i := 1; i <= n; i++ {
		x.send(mkpv(i))
		if i%16 == 0 {
			// pace a fully unthrottled burst so drop-on-full subscribers are
			// actually sampled (otherwise torn-read coverage is zero)
			runtime.Gosched()
		}
	}
	// Give drains a bounded window to consume before cancelling; cancel is the
	// synchronisation point that ends each iterator.
	if !g.stop(3 * time.Second) {
		ctx.fail("%s: payload drains did not exit after integrity run", name)
	}
	var delivered int64
	for i := range g.counts {
		delivered += g.counts[i].Load()
	}
	ctx.writeMetric(map[string]any{
		"metric": "torn_reads",
		"family": "payload",
		"impl":   name,
		"unit":   "count",
		"dims":   map[string]any{"subs": subs},
		"checked":               delivered,
		"violations":            g.torn.Load(),
		"publishes":             n,
		"lower_is_better":       true,
		"scenarios":             scen,
		"expected_good_fit":     goodFit,
		"exposure_limited":      expLimit,
	}, nil)
}

func measurePayloadStalledConsumer(ctx *measureContext, mk func() asyncAPI, name string) {
	scen, goodFit, expLimit := payloadAttachmentMeta(name)
	debug.SetGCPercent(-1)
	defer debug.SetGCPercent(100)
	x := mk()
	const fastSubs = 2
	fastCtx, cancelFast := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var fastCounts []atomic.Int64
	var torn atomic.Int64
	fastCounts = make([]atomic.Int64, fastSubs)
	fastIters := make([]func(yield func(pv) bool), fastSubs)
	for i := 0; i < fastSubs; i++ {
		fastIters[i] = x.subscribe(fastCtx)
	}
	for i := 0; i < fastSubs; i++ {
		i := i
		it := fastIters[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			it(func(v pv) bool {
				if v[0] != v[1] || v[1] != v[2] || v[2] != v[3] {
					torn.Add(1)
				}
				fastCounts[i].Add(1)
				return true
			})
		}()
	}
	_ = x.subscribe(fastCtx) // permanently stalled lane; iterator never ranged
	runtime.GC()
	m0 := heapAllocBytes()
	r := ctx.profile.StalledOps
	var opsDone atomic.Int64
	var panicked atomic.Bool
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		defer func() {
			if rec := recover(); rec != nil {
				panicked.Store(true)
			}
		}()
		for i := 1; i <= r; i++ {
			x.send(mkpv(i))
			opsDone.Add(1)
		}
	}()
	outcome := "completed"
	select {
	case <-pubDone:
		if panicked.Load() {
			outcome = "publisher_panicked"
		}
	case <-time.After(ctx.profile.StalledDeadline):
		outcome = "publisher_blocked"
	}
	cancelFast()
	drained := make(chan struct{})
	go func() { wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
	}
	runtime.GC()
	m1 := heapAllocBytes()
	fastMin := int64(0)
	if fastSubs > 0 {
		fastMin = fastCounts[0].Load()
		for i := 1; i < fastSubs; i++ {
			if v := fastCounts[i].Load(); v < fastMin {
				fastMin = v
			}
		}
	}
	retained := uint64(0)
	if m1 > m0 {
		retained = m1 - m0
	}
	ctx.writeMetric(map[string]any{
		"metric": "stalled_consumer_ops_completed",
		"family": "payload",
		"impl":   name,
		"unit":   "ops",
		"dims":   map[string]any{"stalled_subscribers": 1, "fast_subscribers": fastSubs},
		"ops_planned":           r,
		"outcome":               outcome,
		"fast_min_delivered":    fastMin,
		"approx_retained_bytes": retained,
		"torn_reads":            torn.Load(),
		"deadline_ms":           ctx.profile.StalledDeadline.Milliseconds(),
		"higher_is_better":      true,
		"scenarios":             scen,
		"expected_good_fit":     goodFit,
		"exposure_limited":      expLimit,
	}, nil)
}

func measurePayloadChurn(ctx *measureContext, mk func() asyncAPI, name string) {
	scen, goodFit, expLimit := payloadAttachmentMeta(name)
	samples := make([]float64, 0, ctx.profile.ChurnReps)
	lastOps := 0
	for r := 0; r < ctx.profile.ChurnReps; r++ {
		x := mk()
		for i := 0; i < 4; i++ {
			c, cancel := context.WithCancel(context.Background())
			_ = x.subscribe(c)
			cancel()
		}
		w0 := time.Now()
		for i := 0; i < 8; i++ {
			c, cancel := context.WithCancel(context.Background())
			_ = x.subscribe(c)
			cancel()
		}
		est := time.Since(w0) / 16
		n := calibrateOps(10*time.Millisecond, 16, 20000, est*2)
		s := time.Now()
		for i := 0; i < n; i++ {
			c, cancel := context.WithCancel(context.Background())
			_ = x.subscribe(c)
			cancel()
		}
		d := time.Since(s)
		lastOps = n
		samples = append(samples, float64(d)/float64(n))
	}
	ctx.writeMetric(map[string]any{
		"metric": "churn_ns_op",
		"family": "payload",
		"impl":   name,
		"unit":   "ns",
		"dims":   map[string]any{},
		"ops":    lastOps,
		"reps":   len(samples),
		"lower_is_better":   true,
		"scenarios":         scen,
		"expected_good_fit": goodFit,
		"exposure_limited":  expLimit,
	}, samples)
}

func runPayloadBatteries(ctx *measureContext) {
	for name, mk := range asyncImpls() {
		name := name
		mk := mk
		if !ctx.implAllowed(name) {
			continue
		}
		fmt.Fprintf(os.Stderr, "  payload: %s\n", name)
		if ctx.batteryAllowed("payload.throughput") {
			measurePayloadThroughput(ctx, mk, name)
		}
		if ctx.batteryAllowed("payload.latency") {
			measurePayloadLatency(ctx, mk, name)
		}
		if ctx.batteryAllowed("payload.integrity") {
			measurePayloadIntegrity(ctx, mk, name)
		}
		if ctx.batteryAllowed("payload.stalled_consumer") {
			measurePayloadStalledConsumer(ctx, mk, name)
		}
		if ctx.batteryAllowed("payload.churn") {
			measurePayloadChurn(ctx, mk, name)
		}
	}
}

// --- Gather family ---

func measureGatherWait(ctx *measureContext, name string, mk func() gatherLike) {
	scen, goodFit, expLimit := gatherAttachmentMeta(name)
	const senders = 8
	samples := make([]float64, 0, ctx.profile.ChurnReps*2)
	waits := 0
	lastOps := 0
	for r := 0; r < ctx.profile.ChurnReps; r++ {
		for inner := 0; inner < 4; inner++ {
			x := mk()
			var wg sync.WaitGroup
			start := make(chan struct{})
			for s := 0; s < senders; s++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					x.send()
				}()
			}
			x.add(senders)
			t0 := time.Now()
			close(start)
			got := x.wait()
			d := time.Since(t0)
			wg.Wait()
			if got != senders {
				ctx.fail("%s: gather wait returned %d, want %d", name, got, senders)
			}
			samples = append(samples, float64(d))
			waits++
		}
	}
	_ = lastOps
	ctx.writeMetric(map[string]any{
		"metric": "wait_ns_op",
		"family": "gather",
		"impl":   name,
		"unit":   "ns",
		"dims":   map[string]any{"senders": senders},
		"ops":    waits,
		"reps":   len(samples),
		"lower_is_better":   true,
		"scenarios":         scen,
		"expected_good_fit": goodFit,
		"exposure_limited":  expLimit,
	}, samples)
}

func runGatherBatteries(ctx *measureContext) {
	for name, mk := range gatherLikes() {
		name := name
		mk := mk
		if !ctx.implAllowed(name) {
			continue
		}
		fmt.Fprintf(os.Stderr, "  gather: %s\n", name)
		if ctx.batteryAllowed("gather.wait") {
			measureGatherWait(ctx, name, mk)
		}
		if ctx.batteryAllowed("gather.churn") {
			scen, goodFit, expLimit := gatherAttachmentMeta(name)
			samples := make([]float64, 0, ctx.profile.ChurnReps)
			lastOps := 0
			for r := 0; r < ctx.profile.ChurnReps; r++ {
				x := mk()
				for i := 0; i < 4; i++ {
					x.add(1)
					x.add(-1)
				}
				n := calibrateOps(10*time.Millisecond, 16, 50000, 300*time.Nanosecond)
				s := time.Now()
				for i := 0; i < n; i++ {
					x.add(1)
					x.add(-1)
				}
				d := time.Since(s)
				lastOps = n
				samples = append(samples, float64(d)/float64(n))
			}
			ctx.writeMetric(map[string]any{
				"metric": "churn_ns_op",
				"family": "gather",
				"impl":   name,
				"unit":   "ns",
				"dims":   map[string]any{},
				"ops":    lastOps,
				"reps":   len(samples),
				"lower_is_better":   true,
				"scenarios":         scen,
				"expected_good_fit": goodFit,
				"exposure_limited":  expLimit,
			}, samples)
		}
	}
}
