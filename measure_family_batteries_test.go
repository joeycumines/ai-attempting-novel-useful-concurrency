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

func startCasterReceivers(x casterLike, n int) (stop func(), waitDone func(time.Duration) bool, received *atomic.Int64) {
	var receivedCount atomic.Int64
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	if !x.rearmEach {
		for i := 0; i < n; i++ {
			x.add(1)
		}
	}
	for r := 0; r < n; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if x.rearmEach {
					x.add(1)
				}
				select {
				case <-x.c:
					if x.ack != nil {
						x.ack()
					}
					receivedCount.Add(1)
				case <-stopCh:
					x.add(-1)
					return
				}
			}
		}()
	}
	return func() {
			close(stopCh)
		}, func(timeout time.Duration) bool {
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				return true
			case <-time.After(timeout):
				return false
			}
		}, &receivedCount
}

func measureCasterThroughput(ctx *measureContext, mk func() casterLike, name string) {
	scen, goodFit, expLimit := casterAttachmentMeta(name)
	for _, receivers := range ctx.profile.CasterScaleSubs {
		samples := make([]float64, 0, ctx.profile.Reps)
		lastOps := 0
		for r := 0; r < ctx.profile.Reps; r++ {
			x := mk()
			stop, waitDone, _ := startCasterReceivers(x, receivers)
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
			stop()
			if !waitDone(3 * time.Second) {
				ctx.fail("%s: caster receivers did not stop (receivers=%d)", name, receivers)
			}
			if x.close != nil {
				x.close()
			}
			lastOps = n
			samples = append(samples, float64(d)/float64(n))
		}
		ctx.writeMetric(map[string]any{
			"metric": "send_ns_op",
			"family": "caster",
			"impl":   name,
			"unit":   "ns",
			"dims":   map[string]any{"receivers": receivers},
			"ops":    lastOps,
			"reps":   len(samples),
			"lower_is_better":   true,
			"scenarios":         scen,
			"expected_good_fit": goodFit,
			"exposure_limited":  expLimit,
		}, samples)
	}
}

func measureCasterLatency(ctx *measureContext, mk func() casterLike, name string) {
	scen, goodFit, expLimit := casterAttachmentMeta(name)
	const receivers = 8
	x := mk()
	stop, waitDone, _ := startCasterReceivers(x, receivers)
	const warmup = 32
	for i := 0; i < warmup; i++ {
		x.send(i + 1)
	}
	n := ctx.profile.LatencySamples
	durations := make([]float64, n)
	hist := newHistogram()
	for i := 0; i < n; i++ {
		t0 := time.Now()
		x.send(i + 1 + warmup)
		d := time.Since(t0)
		durations[i] = float64(d)
		hist.add(float64(d))
	}
	stop()
	if !waitDone(3 * time.Second) {
		ctx.fail("%s: caster receivers did not stop after latency run", name)
	}
	if x.close != nil {
		x.close()
	}
	ctx.writeMetric(map[string]any{
		"metric": "send_latency_ns",
		"family": "caster",
		"impl":   name,
		"unit":   "ns",
		"dims":   map[string]any{"receivers": receivers},
		"ops":    n,
		"histogram":           hist,
		"lower_is_better":     true,
		"scenarios":           scen,
		"expected_good_fit":   goodFit,
		"exposure_limited":    expLimit,
		"instrumented":        true,
	}, durations)
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
		"metric": "churn_ns_op",
		"family": "caster",
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
		"metric": "withdrawal_compensation_success",
		"family": "caster",
		"impl":   name,
		"unit":   "fraction",
		"dims":   map[string]any{"intents": intents, "rounds": rounds},
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
		var violations int64
		lastOps := 0
		for r := 0; r < ctx.profile.Reps; r++ {
			x := mk()
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
			for received.Load() < sent && time.Now().Before(deadline) {
				time.Sleep(50 * time.Microsecond)
			}
			if got := received.Load(); got != sent {
				v := sent - got
				if v < 0 {
					v = -v
				}
				violations += v
			}
			cancel()
			wg.Wait()
			lastOps = n
			samples = append(samples, float64(d)/float64(n))
		}
		ctx.writeMetric(map[string]any{
			"metric": "dispatch_ns_op",
			"family": "scatter",
			"impl":   name,
			"unit":   "ns",
			"dims":   map[string]any{"receivers": receivers},
			"ops":    lastOps,
			"reps":   len(samples),
			"lower_is_better":       true,
			"scenarios":             scen,
			"expected_good_fit":     goodFit,
			"exposure_limited":      expLimit,
			"exactly_once_violations": violations,
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
				"metric": "churn_ns_op",
				"family": "scatter",
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
