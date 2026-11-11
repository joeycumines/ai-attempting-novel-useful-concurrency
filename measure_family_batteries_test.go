package bigbuff

// Caster / scatter / gather / payload batteries. Same contract as the
// broadcast batteries: bounded, watchdog-guarded, JSONL out.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// --- Caster family ---

// casterRounds drives a caster in lock step so that every measured Send has
// receivers to deliver to.
//
// A caster delivers to the receivers registered at the time of the Send, and
// a receiver registers again (Add(1)) for each value it wants. A sender that
// simply loops therefore finds nobody registered on almost every call: the
// receivers have not got round to registering again. The batteries used to do
// exactly that, and what they timed was the empty fast path. With 8 receivers
// and 20,000 sends, the reference ChanCaster delivered 13 values and one
// candidate delivered none; their "send cost" was 2 to 5 ns, and the baseline,
// which delivered more, ranked last for it.
//
// Here the sender waits before each Send until every receiver that took the
// previous value has registered again (arm), so a Send is timed with all its
// receivers registered. What is recorded with the time is how many deliveries
// the Sends reported, how many values the receivers actually took, and how
// many deliveries were missing.
type casterRounds struct {
	x         casterLike
	receivers int
	ready     chan struct{} // one token per receiver registration
	stop      chan struct{}
	wg        sync.WaitGroup
	received  atomic.Int64
	toArm     int // receivers that still owe a registration before the next Send
	guard     *time.Timer
}

// casterArmGuard bounds the wait for a receiver to register again. It expires
// only when a Send reported a delivery that no receiver took.
var casterArmGuard = 5 * time.Second

func startCasterRounds(x casterLike, receivers int) *casterRounds {
	r := &casterRounds{x: x, receivers: receivers, ready: make(chan struct{}, receivers), stop: make(chan struct{}), toArm: receivers}
	if !x.rearmEach {
		for i := 0; i < receivers; i++ {
			x.add(1)
		}
	}
	for i := 0; i < receivers; i++ {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			for {
				if x.rearmEach {
					x.add(1)
				}
				select {
				case r.ready <- struct{}{}:
				case <-r.stop:
					x.add(-1)
					return
				}
				select {
				case <-x.c:
					if x.ack != nil {
						x.ack()
					}
					r.received.Add(1)
				case <-r.stop:
					x.add(-1)
					return
				}
			}
		}()
	}
	return r
}

// arm waits until every receiver that owes a registration has made it. It
// reports false if one has not within the guard, which means a Send reported a
// delivery that no receiver took.
func (r *casterRounds) arm() bool {
	for ; r.toArm > 0; r.toArm-- {
		select {
		case <-r.ready:
			continue
		default:
		}
		// One timer for the life of the rounds: a timer per wait would put
		// the harness's allocations into the benchmark that uses this.
		if r.guard == nil {
			r.guard = time.NewTimer(casterArmGuard)
		} else {
			r.guard.Reset(casterArmGuard)
		}
		select {
		case <-r.ready:
			r.guard.Stop()
		case <-r.guard.C:
			return false
		}
	}
	return true
}

// send performs one armed Send and returns its duration and reported count.
func (r *casterRounds) send(v int) (time.Duration, int, bool) {
	if !r.arm() {
		return 0, 0, false
	}
	t0 := time.Now()
	k := r.x.send(v)
	d := time.Since(t0)
	if k > 0 {
		r.toArm = k // exactly the receivers that took the value register again
		if r.toArm > r.receivers {
			r.toArm = r.receivers
		}
	}
	return d, k, true
}

// finish stops the receivers and reports whether they all returned.
func (r *casterRounds) finish() bool {
	close(r.stop)
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		return false
	}
	if r.x.close != nil {
		r.x.close()
	}
	return true
}

// casterDelivery is the delivery accounting of one caster measurement.
type casterDelivery struct {
	sends, reported, expected int64
}

func (d *casterDelivery) add(receivers, reported int) {
	d.sends++
	d.reported += int64(reported)
	d.expected += int64(receivers)
}

// fields returns the record fields: deliveries per Send, deliveries missing
// (a Send with every receiver registered that reported fewer than all of
// them), and the difference between what the Sends reported and what the
// receivers took.
func (d *casterDelivery) fields(received int64) map[string]any {
	perSend := 0.0
	if d.sends > 0 {
		perSend = float64(d.reported) / float64(d.sends)
	}
	missing := d.expected - d.reported
	if missing < 0 {
		missing = 0
	}
	mismatch := d.reported - received
	if mismatch < 0 {
		mismatch = -mismatch
	}
	return map[string]any{
		"delivered_per_send":      perSend,
		"deliveries_missing":      missing,
		"exactly_once_violations": mismatch,
		"checked":                 d.expected,
	}
}

func measureCasterThroughput(ctx *measureContext, mk func() casterLike, name string) {
	scen, goodFit, expLimit := casterAttachmentMeta(name)
	for _, receivers := range ctx.profile.CasterScaleSubs {
		samples := make([]float64, 0, ctx.profile.Reps)
		lastOps := 0
		var delivery casterDelivery
		var received int64
		for r := 0; r < ctx.profile.Reps; r++ {
			rounds := startCasterRounds(mk(), receivers)
			const warmup = 16
			var warm time.Duration
			ok := true
			for i := 0; i < warmup && ok; i++ {
				var d time.Duration
				d, _, ok = rounds.send(i + 1)
				warm += d
			}
			warmReceived := int64(0)
			n := calibrateOps(ctx.profile.TargetPerRep, ctx.profile.MinOps, ctx.profile.MaxOps, warm/warmup)
			var total time.Duration
			if ok {
				// Every warm-up value has been taken once the receivers that
				// took the last one have registered again.
				ok = rounds.arm()
				warmReceived = rounds.received.Load()
			}
			for i := 0; i < n && ok; i++ {
				var d time.Duration
				var k int
				d, k, ok = rounds.send(i + 1 + warmup)
				total += d
				delivery.add(receivers, k)
			}
			if ok {
				ok = rounds.arm()
			}
			if !ok {
				ctx.fail("%s: a Send reported a delivery that no receiver took (receivers=%d)", name, receivers)
			}
			if !rounds.finish() {
				ctx.fail("%s: caster receivers did not stop (receivers=%d)", name, receivers)
			}
			received += rounds.received.Load() - warmReceived
			lastOps = n
			samples = append(samples, float64(total)/float64(n))
		}
		rec := map[string]any{
			"metric":            "send_ns_op",
			"family":            "caster",
			"impl":              name,
			"unit":              "ns",
			"dims":              map[string]any{"receivers": receivers},
			"ops":               lastOps,
			"reps":              len(samples),
			"lower_is_better":   true,
			"scenarios":         scen,
			"expected_good_fit": goodFit,
			"exposure_limited":  expLimit,
			"instrumented":      true,
		}
		for k, v := range delivery.fields(received) {
			rec[k] = v
		}
		ctx.writeMetric(rec, samples)
	}
}

func measureCasterLatency(ctx *measureContext, mk func() casterLike, name string) {
	scen, goodFit, expLimit := casterAttachmentMeta(name)
	const receivers = 8
	rounds := startCasterRounds(mk(), receivers)
	const warmup = 32
	ok := true
	for i := 0; i < warmup && ok; i++ {
		_, _, ok = rounds.send(i + 1)
	}
	warmReceived := int64(0)
	if ok {
		ok = rounds.arm()
		warmReceived = rounds.received.Load()
	}
	n := ctx.profile.LatencySamples
	durations := make([]float64, 0, n)
	hist := newHistogram()
	var delivery casterDelivery
	for i := 0; i < n && ok; i++ {
		var d time.Duration
		var k int
		d, k, ok = rounds.send(i + 1 + warmup)
		if !ok {
			break
		}
		durations = append(durations, float64(d))
		hist.add(float64(d))
		delivery.add(receivers, k)
	}
	if ok {
		ok = rounds.arm()
	}
	if !ok {
		ctx.fail("%s: a Send reported a delivery that no receiver took during the latency run", name)
	}
	if !rounds.finish() {
		ctx.fail("%s: caster receivers did not stop after latency run", name)
	}
	rec := map[string]any{
		"metric":            "send_latency_ns",
		"family":            "caster",
		"impl":              name,
		"unit":              "ns",
		"dims":              map[string]any{"receivers": receivers},
		"ops":               len(durations),
		"histogram":         hist,
		"lower_is_better":   true,
		"scenarios":         scen,
		"expected_good_fit": goodFit,
		"exposure_limited":  expLimit,
		"instrumented":      true,
	}
	for k, v := range delivery.fields(rounds.received.Load() - warmReceived) {
		rec[k] = v
	}
	ctx.writeMetric(rec, durations)
}

func measureCasterChurn(ctx *measureContext, mk func() casterLike, name string) {
	scen, goodFit, expLimit := casterAttachmentMeta(name)
	samples := make([]float64, 0, ctx.profile.ChurnReps)
	lastOps := 0
	for r := 0; r < ctx.profile.ChurnReps; r++ {
		x := mk()
		for i := 0; i < 4; i++ {
			x.add(1)
			x.add(-1)
		}
		w0 := time.Now()
		for i := 0; i < 8; i++ {
			x.add(1)
			x.add(-1)
		}
		est := time.Since(w0) / 16
		n := calibrateOps(15*time.Millisecond, 32, 50000, est*2)
		s := time.Now()
		for i := 0; i < n; i++ {
			x.add(1)
			x.add(-1)
		}
		d := time.Since(s)
		if x.close != nil {
			x.close()
		}
		lastOps = n
		samples = append(samples, float64(d)/float64(n)) // per pair
	}
	ctx.writeMetric(map[string]any{
		"metric":            "churn_ns_op",
		"family":            "caster",
		"impl":              name,
		"unit":              "ns",
		"dims":              map[string]any{},
		"ops":               lastOps,
		"reps":              len(samples),
		"lower_is_better":   true,
		"scenarios":         scen,
		"expected_good_fit": goodFit,
		"exposure_limited":  expLimit,
	}, samples)
}

// measureCasterWithdrawalCompensation measures the headline invariant of the
// rendezvous-caster family: with one registered intent that never receives,
// a mid-flight Add(-1) must release a blocked Send with the compensated
// count. 16 intents are registered; Send(7) and one Add(-1) race; 15 real
// receivers drain; the round succeeds iff Send returns exactly 15 within the
// guard. 24 rounds per repetition; the metric is the success fraction and the
// median round latency.
// measureCasterWithdrawalCompensation measures the headline invariant of the
// rendezvous-caster family: with one registered intent that never receives, a
// mid-flight Add(-1) must release a blocked Send with the compensated count.
// 16 intents are registered; Send(7) and one Add(-1) race; intents-1 real
// receivers drain; the round succeeds iff Send returns exactly intents-1
// within the guard. 24 rounds per invocation; the metric is the success
// fraction and the median successful-round latency.
func measureCasterWithdrawalCompensation(ctx *measureContext, mk func() casterLike, name string) {
	scen, goodFit, expLimit := casterAttachmentMeta(name)
	const intents = 16
	const rounds = 24
	successes := 0
	roundLat := make([]float64, 0, rounds)
	for round := 0; round < rounds; round++ {
		x := mk()
		for i := 0; i < intents; i++ {
			x.add(1)
		}
		start := make(chan struct{})
		sendDone := make(chan int, 1)
		go func() {
			defer func() {
				sendDone <- -1 << 31 // sentinel: send panicked
			}()
			<-start
			sendDone <- x.send(7)
		}()
		withdrawDone := make(chan struct{})
		go func() {
			defer close(withdrawDone)
			<-start
			x.add(-1)
		}()
		roundStart := time.Now()
		close(start)
		guard := time.After(150 * time.Millisecond)
		drained := true
		for i := 0; i < intents-1; i++ {
			select {
			case v := <-x.c:
				if v != 7 {
					ctx.fail("%s: withdrawal compensation round saw value %d", name, v)
				}
				if x.ack != nil {
					x.ack()
				}
			case <-guard:
				drained = false
			}
			if !drained {
				break
			}
		}
		sendRet, sendOK := -1, false
		select {
		case sendRet = <-sendDone:
			sendOK = true
		case <-time.After(150 * time.Millisecond):
		}
		<-withdrawDone
		if x.close != nil && sendOK {
			// A still-parked send must not have its instance closed.
			x.close()
		}
		if drained && sendOK && sendRet == intents-1 {
			successes++
			roundLat = append(roundLat, float64(time.Since(roundStart).Nanoseconds()))
		}
	}
	fraction := float64(successes) / float64(rounds)
	medianLat := 0.0
	if len(roundLat) > 0 {
		medianLat = summarize(roundLat).P50
	}
	ctx.writeMetric(map[string]any{
		"metric":            "withdrawal_compensation_success",
		"family":            "caster",
		"impl":              name,
		"unit":              "fraction",
		"dims":              map[string]any{"intents": intents, "rounds": rounds},
		"scenarios":         scen,
		"expected_good_fit": goodFit,
		"exposure_limited":  expLimit,
		"higher_is_better":  true,
		"mean":              fraction,
		"successes":         successes,
		"median_round_ns":   medianLat,
	}, nil)
}

func runCasterBatteries(ctx *measureContext) {
	for name, mk := range casterLikes() {
		name := name
		mk := mk
		if !ctx.implAllowed(name) {
			continue
		}
		unsafe := casterExposureLimited(name)
		fmt.Fprintf(os.Stderr, "  caster: %s\n", name)
		type job struct {
			id string
			fn func(ctx *measureContext, mk func() casterLike, name string)
		}
		for _, j := range []job{
			{"caster.throughput", measureCasterThroughput},
			{"caster.latency", measureCasterLatency},
			{"caster.churn", measureCasterChurn},
			{"caster.withdrawal_compensation", measureCasterWithdrawalCompensation},
			{"caster.trace_steady_state", func(ctx *measureContext, mk func() casterLike, name string) {
				measureCasterTrace(ctx, mk, name)
			}},
		} {
			if !ctx.batteryAllowed(j.id) {
				continue
			}
			if unsafe {
				ctx.skip("caster", name, j.id, "exposure-limited attachment: concurrent withdrawal during Send cannot be acknowledged by this design; see attachment remarks in the registry")
				continue
			}
			j.fn(ctx, mk, name)
		}
	}
}

func casterExposureLimited(name string) bool {
	for _, a := range casterAttachments {
		if a.Impl == name && a.ExposureLimited {
			return true
		}
	}
	return false
}

// --- Scatter family ---

func measureScatterDispatch(ctx *measureContext, name string, mk func() scatterLike) {
	scen, goodFit, expLimit := scatterAttachmentMeta(name)
	for _, receivers := range ctx.profile.ScatterScaleSubs {
		samples := make([]float64, 0, ctx.profile.Reps)
		var violations, evicted, sentTotal int64
		lastOps := 0
		for r := 0; r < ctx.profile.Reps; r++ {
			x := mk()
			// Queues that are bounded by contract evict the oldest pending
			// item when full and count each eviction. For them "exactly
			// once" means delivered or evicted, never both and never
			// neither: received + evicted must equal sent. Implementations
			// with no eviction counter must deliver every item.
			droppedNow := func() int64 {
				if x.dropped == nil {
					return 0
				}
				return int64(x.dropped())
			}
			ctx8, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			var received atomic.Int64
			ready := make(chan struct{}, receivers)
			for w := 0; w < receivers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					x.add(1)
					defer x.add(-1)
					ready <- struct{}{}
					for {
						v, ok := x.wait(ctx8)
						if !ok {
							return
						}
						_ = v
						received.Add(1)
					}
				}()
			}
			for w := 0; w < receivers; w++ {
				<-ready
			}
			const warmup = 16
			w0 := time.Now()
			for i := 0; i < warmup; i++ {
				x.send(i + 1)
			}
			est := time.Since(w0) / warmup
			n := calibrateOps(ctx.profile.TargetPerRep, ctx.profile.MinOps, ctx.profile.MaxOps, est)
			s := time.Now()
			for i := 0; i < n; i++ {
				x.send(i + 1 + warmup)
			}
			d := time.Since(s)
			sent := int64(warmup + n)
			// A receiver increments `received` after its wait returns; a send
			// returns at handoff, so there is a small window before the count
			// settles. Bounded wait (not a retry loop) with violation accounting.
			deadline := time.Now().Add(3 * time.Second)
			for received.Load()+droppedNow() < sent && time.Now().Before(deadline) {
				time.Sleep(50 * time.Microsecond)
			}
			dropped := droppedNow()
			if got := received.Load() + dropped; got != sent {
				v := sent - got
				if v < 0 {
					v = -v
				}
				violations += v
			}
			cancel()
			wg.Wait()
			lastOps = n
			evicted += dropped
			sentTotal += sent
			// Cost is per delivered item. A queue that admits without
			// waiting and evicts under a saturating sender would otherwise
			// be credited with the items it threw away.
			delivered := sent - dropped
			if delivered < 1 {
				delivered = 1
			}
			samples = append(samples, float64(d)/float64(delivered))
		}
		deliveredFraction := 1.0
		if sentTotal > 0 {
			deliveredFraction = float64(sentTotal-evicted) / float64(sentTotal)
		}
		ctx.writeMetric(map[string]any{
			"metric":                  "dispatch_ns_op",
			"family":                  "scatter",
			"impl":                    name,
			"unit":                    "ns",
			"dims":                    map[string]any{"receivers": receivers},
			"ops":                     lastOps,
			"reps":                    len(samples),
			"lower_is_better":         true,
			"scenarios":               scen,
			"expected_good_fit":       goodFit,
			"exposure_limited":        expLimit,
			"exactly_once_violations": violations,
			"evicted":                 evicted,
			"delivered_fraction":      deliveredFraction,
		}, samples)
	}
}

func runScatterBatteries(ctx *measureContext) {
	for name, mk := range scatterLikes() {
		name := name
		mk := mk
		if !ctx.implAllowed(name) {
			continue
		}
		_, _, expLimit := scatterAttachmentMeta(name)
		fmt.Fprintf(os.Stderr, "  scatter: %s\n", name)
		if ctx.batteryAllowed("scatter.dispatch") {
			if expLimit {
				ctx.skip("scatter", name, "scatter.dispatch", "exposure-limited attachment")
			} else {
				measureScatterDispatch(ctx, name, mk)
			}
		}
		if ctx.batteryAllowed("scatter.churn") {
			samples := make([]float64, 0, ctx.profile.ChurnReps)
			scen, goodFit, _ := scatterAttachmentMeta(name)
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
				"metric":            "churn_ns_op",
				"family":            "scatter",
				"impl":              name,
				"unit":              "ns",
				"dims":              map[string]any{},
				"ops":               lastOps,
				"reps":              len(samples),
				"lower_is_better":   true,
				"scenarios":         scen,
				"expected_good_fit": goodFit,
				"exposure_limited":  expLimit,
			}, samples)
		}
	}
}
