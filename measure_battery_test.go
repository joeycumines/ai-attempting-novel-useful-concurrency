package bigbuff

// Measurement batteries (see measurement.md). Each battery is a self-contained
// experiment over one family of implementations, writing schema-versioned
// JSONL records through measureContext. Batteries must be bounded: every
// blocking probe uses a watchdog, and a measurement must never wedge the
// test binary.

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

// measureBattery describes one battery for `list` and coverage accounting.
type measureBattery struct {
	ID          string
	Family      string
	Description string
}

var measureBatteries = []measureBattery{
	{"broadcast.throughput", "broadcast", "Publish ns/op at 1..512 subscribers, calibrated per cell, R reps; includes scaling curve data"},
	{"broadcast.latency", "broadcast", "Per-publish latency distribution at 8 subscribers (p50/p90/p99/max, log2 histogram)"},
	{"broadcast.allocations", "broadcast", "allocs/op and bytes/op per Publish at 8 draining subscribers and at zero subscribers (GC disabled during measurement)"},
	{"broadcast.churn", "broadcast", "Subscribe+Unsubscribe pair cost in ns/op, allocs/op and bytes/op"},
	{"broadcast.mem_per_subscriber", "broadcast", "Retained heap bytes per live subscription (GC-normalised)"},
	{"broadcast.stalled_consumer", "broadcast", "Behaviour with one permanently stalled subscriber: ops completed before publish blocked, fast-subscriber delivery, retained backlog bytes"},
	{"broadcast.zero_subscriber", "broadcast", "Publish cost with no subscribers registered"},
	{"broadcast.trace_steady_state", "broadcast", "Timeline trace of publishes and per-subscriber receives at 3 subscribers (swimlane-visualisation input)"},
	{"broadcast.trace_stalled_consumer", "broadcast", "Timeline trace of the stalled-consumer experiment (swimlane-visualisation input)"},
	{"caster.throughput", "caster", "Send ns/op at 1..64 receivers, calibrated per cell, R reps"},
	{"caster.latency", "caster", "Per-send latency distribution at 8 receivers"},
	{"caster.churn", "caster", "Add(+1)+Add(-1) registration pair cost"},
	{"caster.withdrawal_compensation", "caster", "Mid-flight withdrawal during a blocked Send: fraction of rounds in which Send returns the compensated count within the guard, and recovery latency"},
	{"caster.trace_steady_state", "caster", "Timeline trace of sends and receives at 3 receivers"},
	{"scatter.dispatch", "scatter", "Dispatch ns/op at 1..16 competing receivers; records exactly-one delivery accounting"},
	{"scatter.churn", "scatter", "Add(+1)+Add(-1) registration pair cost"},
	{"gather.wait", "gather", "Wait ns/op with 8 signalling senders (start-barrier synchronised)"},
	{"gather.churn", "gather", "Add(+1)+Add(-1) registration pair cost"},
	{"payload.throughput", "payload", "Publish ns/op at 1..256 subscribers carrying 32-byte multi-word payloads"},
	{"payload.latency", "payload", "Per-publish latency distribution at 8 subscribers"},
	{"payload.integrity", "payload", "Torn multi-word payload reads under concurrent 8-subscriber load (violations counted, not asserted)"},
	{"payload.stalled_consumer", "payload", "Behaviour with one stalled subscriber: ops completed, fast-subscriber torn reads, retained backlog"},
	{"payload.churn", "payload", "SubscribeContext+cancel pair cost"},
}

func batteryIDs() []string {
	ids := make([]string, 0, len(measureBatteries))
	for _, b := range measureBatteries {
		ids = append(ids, b.ID)
	}
	return ids
}

// calibrateOps sizes the operation count for a timed cell given an observed
// per-op estimate, clamped to [minOps, maxOps] and the per-rep time target.
func calibrateOps(target time.Duration, minOps, maxOps int, est time.Duration) int {
	if est <= 0 {
		est = time.Microsecond
	}
	n := int(target / est)
	if n < minOps {
		n = minOps
	}
	if n > maxOps {
		n = maxOps
	}
	return n
}

// drainGroup owns the drain goroutines for one measurement instance.
type drainGroup struct {
	ctx       context.Context
	cancel    context.CancelFunc
	counts    []atomic.Int64
	wg        sync.WaitGroup
	skipUnsub atomic.Bool // set when a probe left the publisher parked: Unsubscribe would block forever
}

// startBroadcastDrains subscribes n subscribers and drains them in goroutines.
// Per-subscriber received counts are recorded (summed) by stop().
// A subscriber-creation panic (e.g. an implementation with a fixed subscriber
// capacity) is converted into an error; partially-created subscribers are
// abandoned and the discarded instance is left for the GC.
func startBroadcastDrains(b Broadcaster, n int) (g *drainGroup, err error) {
	defer func() {
		if r := recover(); r != nil {
			if g != nil {
				g.cancel()
			}
			g = nil
			err = fmt.Errorf("subscribe failed at capacity %d: %v", n, r)
		}
	}()
	g = &drainGroup{}
	g.ctx, g.cancel = context.WithCancel(context.Background())
	g.counts = make([]atomic.Int64, n)
	subs := make([]BroadcastSubscriber, n)
	for i := 0; i < n; i++ {
		subs[i] = b.Subscribe()
	}
	g.wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		s := subs[i]
		go func() {
			defer g.wg.Done()
			defer func() {
				if g.skipUnsub.Load() {
					// A parked publisher means Unsubscribe may never return
					// (pump parked in a pending wait); leave the instance as-is.
					return
				}
				defer func() { _ = recover() }()
				s.Unsubscribe()
			}()
			for {
				if _, ok := s.Receive(g.ctx); !ok {
					return
				}
				g.counts[i].Add(1)
			}
		}()
	}
	return g, nil
}

// stop cancels the drains and waits up to timeout for them to finish.
// It reports whether all drains exited (false means a drained-goroutine leak;
// the caller records the fact rather than hanging).
func (g *drainGroup) stop(timeout time.Duration) bool {
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

func (g *drainGroup) totalReceived() int64 {
	var total int64
	for i := range g.counts {
		total += g.counts[i].Load()
	}
	return total
}

func (g *drainGroup) minReceived() int64 {
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

// closeWithWatchdog closes a Broadcaster in a goroutine with a timeout so a
// wedged Close cannot hang the battery. Reports whether Close returned.
func closeWithWatchdog(b Broadcaster, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer func() { _ = recover() }() // a panicking Close must not kill the battery
		b.Close()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func heapAllocBytes() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// heapDeltaGC measures retained heap growth around fn (GC before and after),
// clamped at zero. It is approximate by design (see measurement.md) and is
// used comparatively across implementations, never as an absolute.
func heapDeltaGC(fn func()) uint64 {
	runtime.GC()
	m0 := heapAllocBytes()
	fn()
	runtime.GC()
	m1 := heapAllocBytes()
	if m1 <= m0 {
		return 0
	}
	return m1 - m0
}

// --- Broadcast family ---

func measureBroadcastThroughput(ctx *measureContext, factory func() Broadcaster, name string) {
	scen, goodFit, expLimit := broadcastAttachmentMeta(name)
	for _, subs := range ctx.profile.ScaleSubs {
		samples := make([]float64, 0, ctx.profile.Reps)
		repsRun := 0
		lastOps := 0
		for r := 0; r < ctx.profile.Reps; r++ {
			b := factory()
			g, drainErr := startBroadcastDrains(b, subs)
			if drainErr != nil {
				ctx.skip("broadcast", name, "broadcast.throughput", fmt.Sprintf("subs=%d: %v", subs, drainErr))
				_ = closeWithWatchdog(b, 2*time.Second)
				continue
			}
			// warmup + per-op estimate from 16 timed publishes
			const warmup = 16
			w0 := time.Now()
			for i := 0; i < warmup; i++ {
				b.Publish(i + 1)
			}
			est := time.Since(w0) / warmup
			n := calibrateOps(ctx.profile.TargetPerRep, ctx.profile.MinOps, ctx.profile.MaxOps, est)
			s := time.Now()
			for i := 0; i < n; i++ {
				b.Publish(i + 1 + warmup)
			}
			d := time.Since(s)
			clean := g.stop(3 * time.Second)
			lastOps = n
			if !clean {
				ctx.fail("%s: drain goroutines did not exit after stop (subs=%d)", name, subs)
			}
			if !closeWithWatchdog(b, 2*time.Second) {
				ctx.fail("%s: Close did not return (subs=%d)", name, subs)
			}
			samples = append(samples, float64(d)/float64(n))
			repsRun++
		}
		_ = repsRun
		ctx.writeMetric(map[string]any{
			"metric": "publish_ns_op",
			"family": "broadcast",
			"impl":   name,
			"unit":   "ns",
			"dims":   map[string]any{"subs": subs},
			"ops":    lastOps,
			"reps":   len(samples),
			"lower_is_better":       true,
			"scenarios":             scen,
			"expected_good_fit":     goodFit,
			"exposure_limited":      expLimit,
			"instrumented":          false,
		}, samples)
	}
}

func measureBroadcastLatency(ctx *measureContext, factory func() Broadcaster, name string) {
	scen, goodFit, expLimit := broadcastAttachmentMeta(name)
	const subs = 8
	b := factory()
	g, drainErr := startBroadcastDrains(b, subs)
	if drainErr != nil {
		ctx.skip("broadcast", name, "broadcast.latency", drainErr.Error())
		_ = closeWithWatchdog(b, 2*time.Second)
		return
	}
	const warmup = 32
	for i := 0; i < warmup; i++ {
		b.Publish(i + 1)
	}
	n := ctx.profile.LatencySamples
	durations := make([]float64, n)
	hist := newHistogram()
	for i := 0; i < n; i++ {
		t0 := time.Now()
		b.Publish(i + 1 + warmup)
		d := time.Since(t0)
		durations[i] = float64(d)
		hist.add(float64(d))
	}
	clean := g.stop(3 * time.Second)
	if !clean {
		ctx.fail("%s: drain goroutines did not exit after latency run", name)
	}
	closeWithWatchdog(b, 2*time.Second)
	ctx.writeMetric(map[string]any{
		"metric": "publish_latency_ns",
		"family": "broadcast",
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
	}, durations)
}

func measureBroadcastAllocations(ctx *measureContext, factory func() Broadcaster, name string) {
	scen, goodFit, expLimit := broadcastAttachmentMeta(name)
	debug.SetGCPercent(-1)
	defer debug.SetGCPercent(100)
	for _, subs := range []int{0, 8} {
		allocSamples := make([]float64, 0, ctx.profile.AllocReps)
		byteSamples := make([]float64, 0, ctx.profile.AllocReps)
		lastOps := 0
		for r := 0; r < ctx.profile.AllocReps; r++ {
			b := factory()
			var g *drainGroup
			if subs > 0 {
				var drainErr error
				g, drainErr = startBroadcastDrains(b, subs)
				if drainErr != nil {
					ctx.skip("broadcast", name, "broadcast.allocs", drainErr.Error())
					_ = closeWithWatchdog(b, 2*time.Second)
					break
				}
			} else {
				// keep a live instance without subscribers
				_ = b
			}
			const warmup = 16
			w0 := time.Now()
			for i := 0; i < warmup; i++ {
				b.Publish(i + 1)
			}
			est := time.Since(w0) / warmup
			n := calibrateOps(25*time.Millisecond, 32, 30000, est)
			runtime.GC()
			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)
			for i := 0; i < n; i++ {
				b.Publish(i + 1 + warmup)
			}
			runtime.ReadMemStats(&m1)
			lastOps = n
			if g != nil {
				clean := g.stop(3 * time.Second)
				if !clean {
					ctx.fail("%s: drain goroutines did not exit after alloc run (subs=%d)", name, subs)
				}
			}
			closeWithWatchdog(b, 2*time.Second)
			byteSamples = append(byteSamples, float64(m1.TotalAlloc-m0.TotalAlloc)/float64(n))
			allocSamples = append(allocSamples, float64(m1.Mallocs-m0.Mallocs)/float64(n))
		}
		metric := "publish_allocs_op"
		if subs == 0 {
			metric = "publish_allocs_op_zerosubs"
		}
		ctx.writeMetric(map[string]any{
			"metric": metric,
			"family": "broadcast",
			"impl":   name,
			"unit":   "allocs",
			"dims":   map[string]any{"subs": subs},
			"ops":    lastOps,
			"reps":   len(allocSamples),
			"lower_is_better":   true,
			"scenarios":         scen,
			"expected_good_fit": goodFit,
			"exposure_limited":  expLimit,
		}, allocSamples)
		metric = "publish_bytes_op"
		if subs == 0 {
			metric = "publish_bytes_op_zerosubs"
		}
		ctx.writeMetric(map[string]any{
			"metric": metric,
			"family": "broadcast",
			"impl":   name,
			"unit":   "bytes",
			"dims":   map[string]any{"subs": subs},
			"ops":    lastOps,
			"reps":   len(byteSamples),
			"lower_is_better":   true,
			"scenarios":         scen,
			"expected_good_fit": goodFit,
			"exposure_limited":  expLimit,
		}, byteSamples)
	}
}

func measureBroadcastChurn(ctx *measureContext, factory func() Broadcaster, name string) {
	scen, goodFit, expLimit := broadcastAttachmentMeta(name)
	samples := make([]float64, 0, ctx.profile.ChurnReps)
	allocSamples := make([]float64, 0, ctx.profile.ChurnReps)
	byteSamples := make([]float64, 0, ctx.profile.ChurnReps)
	lastOps := 0
	for r := 0; r < ctx.profile.ChurnReps; r++ {
		b := factory()
		// warmup
		for i := 0; i < 4; i++ {
			s := b.Subscribe()
			s.Unsubscribe()
		}
		w0 := time.Now()
		for i := 0; i < 8; i++ {
			s := b.Subscribe()
			s.Unsubscribe()
		}
		est := time.Since(w0) / 8
		n := calibrateOps(20*time.Millisecond, 16, 4000, est)
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		t0 := time.Now()
		for i := 0; i < n; i++ {
			s := b.Subscribe()
			s.Unsubscribe()
		}
		d := time.Since(t0)
		runtime.ReadMemStats(&m1)
		closeWithWatchdog(b, 2*time.Second)
		lastOps = n
		samples = append(samples, float64(d)/float64(n))
		allocSamples = append(allocSamples, float64(m1.Mallocs-m0.Mallocs)/float64(n))
		byteSamples = append(byteSamples, float64(m1.TotalAlloc-m0.TotalAlloc)/float64(n))
	}
	for _, rec := range []struct {
		metric string
		unit   string
		data   []float64
	}{
		{"churn_ns_op", "ns", samples},
		{"churn_allocs_op", "allocs", allocSamples},
		{"churn_bytes_op", "bytes", byteSamples},
	} {
		ctx.writeMetric(map[string]any{
			"metric": rec.metric,
			"family": "broadcast",
			"impl":   name,
			"unit":   rec.unit,
			"dims":   map[string]any{},
			"ops":    lastOps,
			"reps":   len(rec.data),
			"lower_is_better":   true,
			"scenarios":         scen,
			"expected_good_fit": goodFit,
			"exposure_limited":  expLimit,
		}, rec.data)
	}
}

func measureBroadcastMemPerSubscriber(ctx *measureContext, factory func() Broadcaster, name string) {
	scen, goodFit, expLimit := broadcastAttachmentMeta(name)
	k := ctx.profile.MemProbeSubs
	b := factory()
	runtime.GC()
	m0 := heapAllocBytes()
	subs := make([]BroadcastSubscriber, 0, k)
	for i := 0; i < k; i++ {
		subs = append(subs, b.Subscribe())
	}
	runtime.GC()
	m1 := heapAllocBytes()
	for _, s := range subs {
		s.Unsubscribe()
	}
	closeWithWatchdog(b, 2*time.Second)
	per := float64(0)
	if m1 > m0 {
		per = float64(m1-m0) / float64(k)
	}
	ctx.writeMetric(map[string]any{
		"metric": "mem_per_subscriber_bytes",
		"family": "broadcast",
		"impl":   name,
		"unit":   "bytes",
		"dims":   map[string]any{"subs": k},
		"ops":    k,
		"reps":   1,
		"mean":   per,
		"lower_is_better":   true,
		"scenarios":         scen,
		"expected_good_fit": goodFit,
		"exposure_limited":  expLimit,
	}, nil)
}

// measureBroadcastStalledConsumer runs one permanently stalled subscriber
// against two continuously drained subscribers and records what happens to
// the publisher: how many publishes complete before it blocks (or never),
// how much the fast subscribers observed, and how much backlog memory is
// retained. The probe is bounded by a watchdog deadline; a blocked publisher
// is a valid, recorded outcome, never a hang.
func measureBroadcastStalledConsumer(ctx *measureContext, factory func() Broadcaster, name string) {
	scen, goodFit, expLimit := broadcastAttachmentMeta(name)
	const fastSubs = 2
	b := factory()
	runtime.GC()
	m0 := heapAllocBytes()
	fastCtx, cancelFast := context.WithCancel(context.Background())
	fast := make([]BroadcastSubscriber, fastSubs)
	var fastCounts []atomic.Int64
	for i := 0; i < fastSubs; i++ {
		fast[i] = b.Subscribe()
	}
	fastCounts = make([]atomic.Int64, fastSubs)
	var wg sync.WaitGroup
	for i := 0; i < fastSubs; i++ {
		i := i
		s := fast[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if _, ok := s.Receive(fastCtx); !ok {
					return
				}
				fastCounts[i].Add(1)
			}
		}()
	}
	stalled := b.Subscribe() // never Receives
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
			b.Publish(i)
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
	if outcome == "completed" {
		// Only safe when the publisher returned: parked publishers mean the
		// wait-phase never completes and Unsubscribe may block forever
		// (observed as a process-wide deadlock).
		for _, s := range fast {
			func(s BroadcastSubscriber) {
				defer func() { _ = recover() }()
				s.Unsubscribe()
			}(s)
		}
		stalledUnsub := make(chan struct{})
		go func() {
			defer func() { _ = recover() }()
			stalled.Unsubscribe()
			close(stalledUnsub)
		}()
		select {
		case <-stalledUnsub:
		case <-time.After(2 * time.Second):
			ctx.fail("%s: stalled subscriber Unsubscribe did not return", name)
		}
	}
	if outcome == "completed" {
		closeWithWatchdog(b, 2*time.Second)
	}
	fastMin := int64(r)
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
		"family": "broadcast",
		"impl":   name,
		"unit":   "ops",
		"dims":   map[string]any{"stalled_subscribers": 1, "fast_subscribers": fastSubs},
		"ops_planned":           r,
		"outcome":               outcome,
		"fast_min_delivered":    fastMin,
		"approx_retained_bytes": retained,
		"deadline_ms":           ctx.profile.StalledDeadline.Milliseconds(),
		"higher_is_better":      true,
		"scenarios":             scen,
		"expected_good_fit":     goodFit,
		"exposure_limited":      expLimit,
	}, nil)
}

func measureBroadcastZeroSubscriber(ctx *measureContext, factory func() Broadcaster, name string) {
	scen, goodFit, expLimit := broadcastAttachmentMeta(name)
	b := factory()
	const warmup = 64
	for i := 0; i < warmup; i++ {
		b.Publish(i + 1)
	}
	samples := make([]float64, 0, 3)
	lastOps := 0
	for r := 0; r < 3; r++ {
		w0 := time.Now()
		for i := 0; i < warmup; i++ {
			b.Publish(i + 1 + warmup*r)
		}
		est := time.Since(w0) / warmup
		n := calibrateOps(15*time.Millisecond, 64, 100000, est)
		s := time.Now()
		for i := 0; i < n; i++ {
			b.Publish(i + 1 + warmup*(r+3))
		}
		d := time.Since(s)
		lastOps = n
		samples = append(samples, float64(d)/float64(n))
	}
	closeWithWatchdog(b, 2*time.Second)
	ctx.writeMetric(map[string]any{
		"metric": "zero_subscriber_ns_op",
		"family": "broadcast",
		"impl":   name,
		"unit":   "ns",
		"dims":   map[string]any{"subs": 0},
		"ops":    lastOps,
		"reps":   len(samples),
		"lower_is_better":   true,
		"scenarios":         scen,
		"expected_good_fit": goodFit,
		"exposure_limited":  expLimit,
	}, samples)
}

// runBroadcastBatteries executes every enabled broadcast battery for every
// implementation. Coverage of an implementation without a single metric or
// skip record is a verification failure (checked by measure verify / at the
// end of run).
func runBroadcastBatteries(ctx *measureContext) {
	for _, factory := range AllBroadcasters() {
		probe := factory()
		name := probe.Name()
		closeWithWatchdog(probe, 2*time.Second)
		if !ctx.implAllowed(name) {
			continue
		}
		fmt.Fprintf(os.Stderr, "  broadcast: %s\n", name)
		if ctx.batteryAllowed("broadcast.throughput") {
			measureBroadcastThroughput(ctx, factory, name)
		}
		if ctx.batteryAllowed("broadcast.latency") {
			measureBroadcastLatency(ctx, factory, name)
		}
		if ctx.batteryAllowed("broadcast.allocations") {
			measureBroadcastAllocations(ctx, factory, name)
		}
		if ctx.batteryAllowed("broadcast.churn") {
			measureBroadcastChurn(ctx, factory, name)
		}
		if ctx.batteryAllowed("broadcast.mem_per_subscriber") {
			measureBroadcastMemPerSubscriber(ctx, factory, name)
		}
		if ctx.batteryAllowed("broadcast.stalled_consumer") {
			measureBroadcastStalledConsumer(ctx, factory, name)
		}
		if ctx.batteryAllowed("broadcast.zero_subscriber") {
			measureBroadcastZeroSubscriber(ctx, factory, name)
		}
		if ctx.batteryAllowed("broadcast.trace_steady_state") {
			measureBroadcastTrace(ctx, factory, name, false)
		}
		if ctx.batteryAllowed("broadcast.trace_stalled_consumer") {
			measureBroadcastTrace(ctx, factory, name, true)
		}
	}
}
