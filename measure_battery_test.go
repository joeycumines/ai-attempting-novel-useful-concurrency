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
	{"fanin.merge", "fanin", "End-to-end concurrent merge cost per value at 4 producers draining into one consumer (blocking Next, per-lane FIFO exactly-once accounting)"},
	{"fanin.batch_drain", "fanin", "Drain(32) cost against a fixture prefilled to capacity (256 values); producer-side cost excluded"},
	{"exchange.pair", "exchange", "Wall-clock cost of one completed swap with 2 and 8 callers exchanging continuously; every completed call checked to be half of a consistent swap"},
	{"exchange.cancel_race", "exchange", "One caller cancelled while its partner arrives: time to resolve, how often the offer was withdrawn or swapped, and one-sided outcomes counted as violations"},
	{"barrier.send", "barrier", "Barrier send ns/op (hand to every member, wait for every acknowledgement) at 1..64 prompt members; in-order exactly-once accounting per member"},
	{"barrier.deadline", "barrier", "One of 4 members never takes the value: how long after its deadline the send returns, and how long the next send takes once that member has left"},
}

func batteryIDs() []string {
	ids := make([]string, 0, len(measureBatteries))
	for _, b := range measureBatteries {
		ids = append(ids, b.ID)
	}
	return ids
}

// opBudget sizes a timed repetition by the clock instead of by an estimate
// taken before it: the caller runs batches of operations until the target
// time has passed, with at least minOps and at most maxOps operations.
//
// Every timed repetition of every battery is sized this way from record
// contract 1.5.0. Before it only the broadcast and payload throughput and
// latency batteries were; the rest fixed the count from a short estimate
// (calibrateOps, now gone), which is what the next paragraph describes.
//
// The throughput batteries of the families whose subscribers run freely
// (broadcast, payload) used to estimate the cost of a publish from 16
// publishes made straight after subscribing, before any drain goroutine had
// run, and fix the operation count from that. For an implementation whose
// publish is cheap until its subscribers are awake the estimate was the cheap
// cost, the count went to maxOps, and a repetition then ran for as long as
// maxOps publishes at the real cost took: 400,000 publishes at 75 µs is half
// a minute, where the target is 60 ms. Worse, how much of such a repetition
// was spent in the cheap regime depended on when the scheduler first ran the
// subscribers. A repetition sized by the clock ends close to its target
// whatever the cost turns out to be, and warmOps before it brings the
// subscribers up.
//
// The clock is read once per batch. A batch doubles (to at most 4096
// operations) while batches take under 1/32 of the target and halves when one
// takes over 1/8 of it, so the reads cost nothing measurable per operation
// and a cost that jumps mid-repetition overruns by one batch at most.
type opBudget struct {
	target         time.Duration
	minOps, maxOps int
	start, last    time.Time
	n, batch       int
	elapsed        time.Duration
}

const opBudgetMinBatch, opBudgetMaxBatch = 16, 4096

func newOpBudget(target time.Duration, minOps, maxOps int) *opBudget {
	now := time.Now()
	return &opBudget{target: target, minOps: minOps, maxOps: maxOps, batch: opBudgetMinBatch, start: now, last: now}
}

// next returns the half-open range of operation indexes to run now, or
// ok == false when the repetition is over. The caller must run every index of
// a range it is given before calling next again.
func (b *opBudget) next() (from, to int, ok bool) {
	now := time.Now()
	if b.n > 0 {
		b.elapsed = now.Sub(b.start)
		if (b.elapsed >= b.target && b.n >= b.minOps) || b.n >= b.maxOps {
			return 0, 0, false
		}
		switch took := now.Sub(b.last); {
		case took < b.target/32 && b.batch < opBudgetMaxBatch:
			b.batch *= 2
		case took > b.target/8 && b.batch > opBudgetMinBatch:
			b.batch /= 2
		}
	}
	b.last = now
	from, to = b.n, b.n+b.batch
	if to > b.maxOps {
		to = b.maxOps
	}
	b.n = to
	return from, to, true
}

// done returns the operations run and the time they took. It is valid once
// next has returned ok == false.
func (b *opBudget) done() (ops int, elapsed time.Duration) { return b.n, b.elapsed }

// warmOps runs op for an eighth of the repetition target (and at least 16
// times) without timing it, and returns how many times it ran. The first
// publishes after subscribing reach subscribers that the scheduler has not
// run yet; timing has to start after they have.
func warmOps(target time.Duration, maxOps int, op func(i int)) int {
	budget := newOpBudget(target/8, opBudgetMinBatch, maxOps)
	for from, to, ok := budget.next(); ok; from, to, ok = budget.next() {
		for i := from; i < to; i++ {
			op(i)
		}
	}
	n, _ := budget.done()
	return n
}

// The settling of a throughput cell. A publisher can finish with values still
// queued for its subscribers: a design that buffers without bound delivers
// everything, later. Counting receives the moment the publisher returns would
// call that design lossy, so the count is read only once the subscribers have
// stopped receiving for settleQuiet, and a cell whose subscribers are still
// receiving at settleLimit is a harness failure, because its count would be a
// guess.
var (
	settleQuiet = 50 * time.Millisecond
	settleLimit = 20 * time.Second
)

// settledReceives returns the receive count once the subscribers have taken
// what they are going to take: it has reached want, or it has not moved for
// settleQuiet. settled is false when it was still moving at settleLimit. It
// is called after the timed loop, so it costs the measurement nothing. A
// lossless implementation reaches want; a lossy one stops short, and the
// difference is what it dropped.
func settledReceives(read func() int64, want int64) (n int64, settled bool) {
	start := time.Now()
	last, lastMove := read(), start
	for last < want {
		time.Sleep(time.Millisecond)
		now := time.Now()
		if got := read(); got != last {
			last, lastMove = got, now
		} else if now.Sub(lastMove) >= settleQuiet {
			return last, true
		}
		if now.Sub(start) >= settleLimit {
			return last, false
		}
	}
	return last, true
}

// deliveryFields are the fields a throughput record carries about what its
// publishes delivered: the publishes made (warm-up included), the values the
// subscribers received, and received / (publishes x subscribers). The cost
// in the record is the cost of calling Publish, whatever became of the
// value; an implementation that drops or coalesces by contract publishes
// cheaply and delivers a fraction, and the record says which fraction.
func deliveryFields(m map[string]any, publishes, receives int64, subs int) {
	m["publishes"] = publishes
	m["receives"] = receives
	if publishes > 0 && subs > 0 {
		m["delivered_fraction"] = float64(receives) / (float64(publishes) * float64(subs))
	}
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
	// The caller gets the group back once every drain goroutine is running,
	// so that what it does next is not done to subscribers the scheduler has
	// not started yet.
	var started sync.WaitGroup
	started.Add(n)
	g.wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		s := subs[i]
		go func() {
			defer g.wg.Done()
			started.Done()
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
	started.Wait()
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
		var publishes, receives int64
		for r := 0; r < ctx.profile.Reps; r++ {
			b := factory()
			g, drainErr := startBroadcastDrains(b, subs)
			if drainErr != nil {
				ctx.skip("broadcast", name, "broadcast.throughput", fmt.Sprintf("subs=%d: %v", subs, drainErr))
				_ = closeWithWatchdog(b, 2*time.Second)
				continue
			}
			warmup := warmOps(ctx.profile.TargetPerRep, ctx.profile.MaxOps, func(i int) { b.Publish(i + 1) })
			budget := newOpBudget(ctx.profile.TargetPerRep, ctx.profile.MinOps, ctx.profile.MaxOps)
			for from, to, ok := budget.next(); ok; from, to, ok = budget.next() {
				for i := from; i < to; i++ {
					b.Publish(i + 1 + warmup)
				}
			}
			n, d := budget.done()
			publishes += int64(warmup + n)
			got, settled := settledReceives(g.totalReceived, int64(warmup+n)*int64(subs))
			if !settled {
				ctx.fail("%s: subscribers were still receiving %v after the last publish (subs=%d): the delivered count is not final", name, settleLimit, subs)
			}
			receives += got
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
		rec := map[string]any{
			"metric":            "publish_ns_op",
			"family":            "broadcast",
			"impl":              name,
			"unit":              "ns",
			"dims":              map[string]any{"subs": subs},
			"ops":               lastOps,
			"reps":              len(samples),
			"lower_is_better":   true,
			"scenarios":         scen,
			"expected_good_fit": goodFit,
			"exposure_limited":  expLimit,
			"instrumented":      false,
		}
		deliveryFields(rec, publishes, receives, subs)
		ctx.writeMetric(rec, samples)
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
		"metric":            "publish_latency_ns",
		"family":            "broadcast",
		"impl":              name,
		"unit":              "ns",
		"dims":              map[string]any{"subs": subs},
		"ops":               n,
		"histogram":         hist,
		"lower_is_better":   true,
		"scenarios":         scen,
		"expected_good_fit": goodFit,
		"exposure_limited":  expLimit,
		"instrumented":      true,
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
			const allocTarget, allocMinOps, allocMaxOps = 25 * time.Millisecond, 32, 30000
			warmup := warmOps(allocTarget, allocMaxOps, func(i int) { b.Publish(i + 1) })
			runtime.GC()
			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)
			budget := newOpBudget(allocTarget, allocMinOps, allocMaxOps)
			for from, to, ok := budget.next(); ok; from, to, ok = budget.next() {
				for i := from; i < to; i++ {
					b.Publish(i + 1 + warmup)
				}
			}
			runtime.ReadMemStats(&m1)
			n, _ := budget.done()
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
			"metric":            metric,
			"family":            "broadcast",
			"impl":              name,
			"unit":              "allocs",
			"dims":              map[string]any{"subs": subs},
			"ops":               lastOps,
			"reps":              len(allocSamples),
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
			"metric":            metric,
			"family":            "broadcast",
			"impl":              name,
			"unit":              "bytes",
			"dims":              map[string]any{"subs": subs},
			"ops":               lastOps,
			"reps":              len(byteSamples),
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
		const churnTarget, churnMinOps, churnMaxOps = 20 * time.Millisecond, 16, 4000
		warmOps(churnTarget, churnMaxOps, func(int) {
			s := b.Subscribe()
			s.Unsubscribe()
		})
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		budget := newOpBudget(churnTarget, churnMinOps, churnMaxOps)
		for from, to, ok := budget.next(); ok; from, to, ok = budget.next() {
			for i := from; i < to; i++ {
				s := b.Subscribe()
				s.Unsubscribe()
			}
		}
		runtime.ReadMemStats(&m1)
		n, d := budget.done()
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
			"metric":            rec.metric,
			"family":            "broadcast",
			"impl":              name,
			"unit":              rec.unit,
			"dims":              map[string]any{},
			"ops":               lastOps,
			"reps":              len(rec.data),
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
		"metric":            "mem_per_subscriber_bytes",
		"family":            "broadcast",
		"impl":              name,
		"unit":              "bytes",
		"dims":              map[string]any{"subs": k},
		"ops":               k,
		"reps":              1,
		"mean":              per,
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
// A publisher that is blocked behind a subscriber that never reads can wait
// in two ways: parked, which costs nothing, or in a loop that polls, which
// holds a CPU for as long as the subscriber stays away. The stalled-consumer
// probes tell the two apart by reading the process's CPU time over
// blockedCPUWindow while the publisher is still blocked, before anything is
// released. The figure is for the whole process, which at that moment is the
// blocked publisher, the subscribers waiting for a value and this goroutine
// asleep; whichever of them is busy, the implementation is burning a CPU
// because one subscriber stopped reading.
const (
	blockedCPUWindow = 100 * time.Millisecond
	// stallBusyCores is the CPU use, in CPUs, from which a blocked publisher
	// counts as spinning. A parked process measures 0.00 to 0.02; one
	// spinning goroutine measures about 1.
	stallBusyCores = 0.5
)

// blockedCPUCores returns the CPUs' worth of time this process uses over
// blockedCPUWindow. ok is false where the platform cannot say.
func blockedCPUCores() (cores float64, ok bool) {
	c0, ok := processCPUTime()
	if !ok {
		return 0, false
	}
	t0 := time.Now()
	time.Sleep(blockedCPUWindow)
	c1, ok := processCPUTime()
	if !ok {
		return 0, false
	}
	return float64(c1-c0) / float64(time.Since(t0)), true
}

// withBlockedCPU adds blocked_cpu_cores to a stalled-consumer record when the
// publisher was blocked and the platform could measure it.
func withBlockedCPU(rec map[string]any, cores float64, measured bool) map[string]any {
	if measured {
		rec["blocked_cpu_cores"] = cores
	}
	return rec
}

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
	// Read at the deadline. Releasing the fast subscribers below can let a
	// blocked publisher run on, and that progress is not what is measured.
	opsCompleted := opsDone.Load()
	blockedCores, blockedMeasured := 0.0, false
	if outcome == "publisher_blocked" {
		blockedCores, blockedMeasured = blockedCPUCores()
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
	ctx.writeMetric(withBlockedCPU(map[string]any{
		"metric":                "stalled_consumer_ops_completed",
		"family":                "broadcast",
		"impl":                  name,
		"unit":                  "ops",
		"dims":                  map[string]any{"stalled_subscribers": 1, "fast_subscribers": fastSubs},
		"ops_planned":           r,
		"ops_completed":         opsCompleted,
		"outcome":               outcome,
		"fast_min_delivered":    fastMin,
		"approx_retained_bytes": retained,
		"deadline_ms":           ctx.profile.StalledDeadline.Milliseconds(),
		"higher_is_better":      true,
		"scenarios":             scen,
		"expected_good_fit":     goodFit,
		"exposure_limited":      expLimit,
	}, blockedCores, blockedMeasured), nil)
}

func measureBroadcastZeroSubscriber(ctx *measureContext, factory func() Broadcaster, name string) {
	scen, goodFit, expLimit := broadcastAttachmentMeta(name)
	b := factory()
	const zeroTarget, zeroMinOps, zeroMaxOps = 15 * time.Millisecond, 64, 100000
	published := warmOps(zeroTarget, zeroMaxOps, func(i int) { b.Publish(i + 1) })
	samples := make([]float64, 0, 3)
	lastOps := 0
	for r := 0; r < 3; r++ {
		budget := newOpBudget(zeroTarget, zeroMinOps, zeroMaxOps)
		for from, to, ok := budget.next(); ok; from, to, ok = budget.next() {
			for i := from; i < to; i++ {
				b.Publish(published + i + 1)
			}
		}
		n, d := budget.done()
		published += n
		lastOps = n
		samples = append(samples, float64(d)/float64(n))
	}
	closeWithWatchdog(b, 2*time.Second)
	ctx.writeMetric(map[string]any{
		"metric":            "zero_subscriber_ns_op",
		"family":            "broadcast",
		"impl":              name,
		"unit":              "ns",
		"dims":              map[string]any{"subs": 0},
		"ops":               lastOps,
		"reps":              len(samples),
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
	for i, factory := range AllBroadcasters() {
		if ctx.exact && ctx.broadcastIndex >= 0 && i != ctx.broadcastIndex {
			// A measuring process is told which factory is its
			// implementation, so that it does not construct and close the
			// other ones to learn their names before it measures.
			continue
		}
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
