package bigbuff

// Exchange and barrier families: measurement batteries for the two use cases
// added in the completion phase (symmetric-pairwise-exchange and
// deadline-bounded-barrier-broadcast).
//
// Both families are small (three and four implementations). Their numbers
// are reported with confidence intervals, and they are not given a composite
// score: a percentile rank over three or four subjects only restates their
// order (see minRankedImpls).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// apexGuard bounds every wait in these batteries. Reaching it is recorded as
// a run failure; it is never part of a measurement.
const apexGuard = 10 * time.Second

func exchangeAttachmentMeta(impl string) ([]string, bool, bool) {
	return attachmentsOfImpl(exchangeAttachments, impl)
}

func barrierAttachmentMeta(impl string) ([]string, bool, bool) {
	return attachmentsOfImpl(barrierAttachments, impl)
}

// --- Exchange ---

var exchangeParties = []int{2, 8}

type exchangeCall struct{ gave, got int }

// exchangePairingViolations counts completed calls that are not one half of
// a consistent swap: a call that gave g and got h needs exactly one call
// that gave h and got g. withdrawn lists values whose call returned an
// error; such a value must not have reached anybody.
func exchangePairingViolations(calls []exchangeCall, withdrawn []int) int64 {
	gotBy := make(map[int]int, len(calls)) // gave -> got
	dup := map[int]bool{}
	for _, c := range calls {
		if _, seen := gotBy[c.gave]; seen {
			dup[c.gave] = true
		}
		gotBy[c.gave] = c.got
	}
	var bad int64
	for _, c := range calls {
		back, ok := gotBy[c.got]
		if !ok || back != c.gave || c.got == c.gave || dup[c.gave] {
			bad++
		}
	}
	received := make(map[int]bool, len(calls))
	for _, c := range calls {
		received[c.got] = true
	}
	for _, v := range withdrawn {
		if received[v] {
			bad++
		}
	}
	return bad
}

// measureExchangePair measures the wall-clock cost of one completed swap
// with P callers exchanging continuously. Each caller offers values unique
// to it; when the target number of calls has completed the context is
// cancelled, which releases the callers still waiting for a partner. Every
// completed call is then checked to be half of a consistent swap, and every
// call the cancellation ended is checked not to have delivered its value.
func measureExchangePair(ctx *measureContext, name string, mk func() exchangeLike) {
	scen, goodFit, expLimit := exchangeAttachmentMeta(name)
	for _, parties := range exchangeParties {
		samples := make([]float64, 0, ctx.profile.Reps)
		var violations int64
		lastOps := 0
		target := int64(ctx.profile.IntegrityPublishes) * 4 // completed calls per rep
		for r := 0; r < ctx.profile.Reps; r++ {
			x := mk()
			runCtx, cancel := context.WithCancel(context.Background())
			var completed atomic.Int64
			reached := make(chan struct{})
			var once sync.Once
			calls := make([][]exchangeCall, parties)
			withdrawn := make([][]int, parties)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for p := 0; p < parties; p++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for seq := 0; ; seq++ {
						v := p*1_000_000 + seq
						got, err := x.exchange(runCtx, v)
						if err != nil {
							withdrawn[p] = append(withdrawn[p], v)
							return
						}
						calls[p] = append(calls[p], exchangeCall{v, got})
						if completed.Add(1) >= target {
							once.Do(func() { close(reached) })
						}
						if runCtx.Err() != nil {
							return
						}
					}
				}()
			}
			t0 := time.Now()
			close(start)
			var d time.Duration
			select {
			case <-reached:
				d = time.Since(t0)
			case <-time.After(apexGuard):
				ctx.fail("%s: %d parties completed %d of %d exchanges within the guard", name, parties, completed.Load(), target)
			}
			cancel()
			stopped := make(chan struct{})
			go func() { wg.Wait(); close(stopped) }()
			select {
			case <-stopped:
			case <-time.After(apexGuard):
				ctx.fail("%s: callers did not return after cancellation (parties=%d)", name, parties)
				x.close()
				continue
			}
			x.close()
			var all []exchangeCall
			var gone []int
			for p := range calls {
				all = append(all, calls[p]...)
				gone = append(gone, withdrawn[p]...)
			}
			violations += exchangePairingViolations(all, gone)
			if d > 0 && len(all) >= 2 {
				// Calls complete in pairs; the pairs completed by the time
				// the target was reached are target/2.
				samples = append(samples, float64(d)/float64(target/2))
				lastOps = int(target / 2)
			}
		}
		ctx.writeMetric(map[string]any{
			"metric":                  "exchange_ns_op",
			"family":                  "exchange",
			"impl":                    name,
			"unit":                    "ns",
			"dims":                    map[string]any{"senders": parties},
			"ops":                     lastOps,
			"reps":                    len(samples),
			"lower_is_better":         true,
			"scenarios":               scen,
			"expected_good_fit":       goodFit,
			"exposure_limited":        expLimit,
			"exactly_once_violations": violations,
		}, samples)
	}
}

// measureExchangeCancelRace runs rounds in which one caller's context is
// cancelled while a second caller arrives. Each round must resolve one of two
// ways: the two swapped, or the first withdrew and the second is still
// waiting with its value (shown by pairing it with a third caller). Anything
// else, such as a value delivered to one side only, is a violation. The
// sample is the time for the round to resolve; the record also carries how
// often each outcome occurred.
func measureExchangeCancelRace(ctx *measureContext, name string, mk func() exchangeLike) {
	scen, goodFit, expLimit := exchangeAttachmentMeta(name)
	rounds := ctx.profile.StalledOps
	samples := make([]float64, 0, rounds)
	var violations int64
	swapped, withdrawn := 0, 0
	type res struct {
		got int
		err error
	}
	call := func(x exchangeLike, c context.Context, v int) <-chan res {
		out := make(chan res, 1)
		go func() { got, err := x.exchange(c, v); out <- res{got, err} }()
		return out
	}
	await := func(ch <-chan res, what string) (res, bool) {
		select {
		case r := <-ch:
			return r, true
		case <-time.After(apexGuard):
			ctx.fail("%s: cancel race: %s did not return", name, what)
			return res{}, false
		}
	}
	for round := 0; round < rounds; round++ {
		x := mk()
		base := round * 4
		raceCtx, cancel := context.WithCancel(context.Background())
		t0 := time.Now()
		a := call(x, raceCtx, base+1)
		b := call(x, context.Background(), base+2)
		cancel()
		ra, ok := await(a, "the cancelled caller")
		if !ok {
			x.close()
			break
		}
		if ra.err == nil {
			rb, ok := await(b, "the partner of a completed exchange")
			if !ok {
				x.close()
				break
			}
			samples = append(samples, float64(time.Since(t0)))
			if ra.got != base+2 || rb.err != nil || rb.got != base+1 {
				violations++
			}
			swapped++
		} else {
			if !errors.Is(ra.err, context.Canceled) {
				violations++
			}
			c := call(x, context.Background(), base+3)
			rb, ok1 := await(b, "the waiting caller after a withdrawal")
			rc, ok2 := await(c, "the third caller")
			if !ok1 || !ok2 {
				x.close()
				break
			}
			samples = append(samples, float64(time.Since(t0)))
			if rb.err != nil || rc.err != nil || rb.got != base+3 || rc.got != base+2 {
				violations++
			}
			withdrawn++
		}
		x.close()
	}
	ctx.writeMetric(map[string]any{
		"metric":                  "cancel_race_ns_op",
		"family":                  "exchange",
		"impl":                    name,
		"unit":                    "ns",
		"ops":                     swapped + withdrawn,
		"reps":                    len(samples),
		"lower_is_better":         true,
		"scenarios":               scen,
		"expected_good_fit":       goodFit,
		"exposure_limited":        expLimit,
		"swapped":                 swapped,
		"withdrawn":               withdrawn,
		"exactly_once_violations": violations,
	}, samples)
}

func runExchangeBatteries(ctx *measureContext) {
	likes := exchangeLikes()
	for _, name := range sortedKeys(likes) {
		mk := likes[name]
		if !ctx.implAllowed(name) {
			continue
		}
		fmt.Fprintf(os.Stderr, "  exchange: %s\n", name)
		if ctx.batteryAllowed("exchange.pair") {
			measureExchangePair(ctx, name, mk)
		}
		if ctx.batteryAllowed("exchange.cancel_race") {
			measureExchangeCancelRace(ctx, name, mk)
		}
	}
}

// --- Barrier ---

// barrierMeasureWorker receives and acknowledges until its context ends,
// counting values that did not arrive in order, exactly once.
type barrierMeasureWorker struct {
	done       chan struct{}
	next       int
	violations int64
}

func barrierMeasureStart(c context.Context, m barrierMember, first int) *barrierMeasureWorker {
	w := &barrierMeasureWorker{done: make(chan struct{}), next: first}
	go func() {
		defer close(w.done)
		for {
			v, ok := m.recv(c)
			if !ok {
				return
			}
			if v != w.next {
				w.violations++
			}
			w.next = v + 1
			m.ack()
		}
	}()
	return w
}

// measureBarrierSend measures the cost of one barrier send (hand the value
// to every member, wait for every acknowledgement) with M members that
// acknowledge at once. Nothing is abandoned, so every member must see every
// value once, in order; anything else is a violation.
func measureBarrierSend(ctx *measureContext, name string, mk func() barrierLike) {
	scen, goodFit, expLimit := barrierAttachmentMeta(name)
	for _, members := range ctx.profile.CasterScaleSubs {
		samples := make([]float64, 0, ctx.profile.Reps)
		var violations int64
		lastOps := 0
		for r := 0; r < ctx.profile.Reps; r++ {
			x := mk()
			runCtx, cancel := context.WithCancel(context.Background())
			handles := make([]barrierMember, members)
			workers := make([]*barrierMeasureWorker, members)
			for i := range handles {
				handles[i] = x.join()
				workers[i] = barrierMeasureStart(runCtx, handles[i], 0)
			}
			const warmup = 16
			failed := false
			for i := 0; i < warmup; i++ {
				if err := x.send(context.Background(), i); err != nil {
					ctx.fail("%s: barrier send failed during warm-up (members=%d): %v", name, members, err)
					failed = true
					break
				}
			}
			n := 0
			if !failed {
				budget := newOpBudget(ctx.profile.TargetPerRep, ctx.profile.MinOps, ctx.profile.MaxOps)
				for from, to, ok := budget.next(); ok && !failed; from, to, ok = budget.next() {
					for i := from; i < to; i++ {
						if err := x.send(context.Background(), warmup+i); err != nil {
							ctx.fail("%s: barrier send %d failed (members=%d): %v", name, i, members, err)
							failed = true
							break
						}
					}
				}
				if !failed {
					var d time.Duration
					n, d = budget.done()
					samples = append(samples, float64(d)/float64(n))
					lastOps = n
				}
			}
			cancel()
			for i, w := range workers {
				select {
				case <-w.done:
					violations += w.violations
					if !failed && w.next != warmup+n {
						violations++ // values missing at the end
					}
				case <-time.After(apexGuard):
					ctx.fail("%s: barrier member %d did not stop (members=%d)", name, i, members)
				}
				handles[i].leave()
			}
			x.close()
		}
		ctx.writeMetric(map[string]any{
			"metric":                  "send_ns_op",
			"family":                  "barrier",
			"impl":                    name,
			"unit":                    "ns",
			"dims":                    map[string]any{"receivers": members},
			"ops":                     lastOps,
			"reps":                    len(samples),
			"lower_is_better":         true,
			"scenarios":               scen,
			"expected_good_fit":       goodFit,
			"exposure_limited":        expLimit,
			"exactly_once_violations": violations,
		}, samples)
	}
}

const barrierDeadlineMembers = 4

// measureBarrierDeadline measures what the use case is about: a member that
// never takes the value must not hold the sender past its deadline. Three
// members acknowledge at once and a fourth does nothing. A send with a
// deadline must return an error (a nil return would claim a barrier that
// never completed, and is a violation); the sample is how long after the
// deadline it returned. The idle member then leaves and a send without a
// deadline must complete; that time is recorded as the recovery metric.
func measureBarrierDeadline(ctx *measureContext, name string, mk func() barrierLike) {
	scen, goodFit, expLimit := barrierAttachmentMeta(name)
	deadline := ctx.profile.StalledDeadline / 10
	reps := ctx.profile.Reps * 4
	overshoot := make([]float64, 0, reps)
	recovery := make([]float64, 0, reps)
	var violations int64
	for r := 0; r < reps; r++ {
		x := mk()
		runCtx, cancel := context.WithCancel(context.Background())
		var prompt []barrierMember
		for i := 0; i < barrierDeadlineMembers-1; i++ {
			m := x.join()
			prompt = append(prompt, m)
			go func() {
				for {
					if _, ok := m.recv(runCtx); !ok {
						return
					}
					m.ack()
				}
			}()
		}
		idle := x.join()
		sendCtx, sendCancel := context.WithTimeout(context.Background(), deadline)
		due, _ := sendCtx.Deadline()
		done := barrierSend(x, sendCtx, 1)
		ok := true
		select {
		case err := <-done:
			overshoot = append(overshoot, float64(time.Since(due)))
			if err == nil {
				violations++
			}
		case <-time.After(apexGuard):
			ctx.fail("%s: a barrier send with a %v deadline did not return within the guard", name, deadline)
			ok = false
		}
		sendCancel()
		if ok {
			idle.leave()
			t0 := time.Now()
			done = barrierSend(x, context.Background(), 2)
			select {
			case err := <-done:
				recovery = append(recovery, float64(time.Since(t0)))
				if err != nil {
					violations++
				}
			case <-time.After(apexGuard):
				ctx.fail("%s: a barrier send after the idle member left did not return within the guard", name)
			}
		}
		cancel()
		for _, m := range prompt {
			m.leave()
		}
		x.close()
	}
	base := func(metric string) map[string]any {
		return map[string]any{
			"metric":                  metric,
			"family":                  "barrier",
			"impl":                    name,
			"unit":                    "ns",
			"dims":                    map[string]any{"receivers": barrierDeadlineMembers},
			"reps":                    reps,
			"lower_is_better":         true,
			"scenarios":               scen,
			"expected_good_fit":       goodFit,
			"exposure_limited":        expLimit,
			"deadline_ns":             float64(deadline),
			"exactly_once_violations": violations,
		}
	}
	m := base("deadline_overshoot_ns")
	m["ops"] = len(overshoot)
	ctx.writeMetric(m, overshoot)
	m = base("deadline_recovery_ns")
	m["ops"] = len(recovery)
	m["exactly_once_violations"] = int64(0) // counted once, on the overshoot record
	ctx.writeMetric(m, recovery)
}

func runBarrierBatteries(ctx *measureContext) {
	likes := barrierLikes()
	for _, name := range sortedKeys(likes) {
		mk := likes[name]
		if !ctx.implAllowed(name) {
			continue
		}
		fmt.Fprintf(os.Stderr, "  barrier: %s\n", name)
		if ctx.batteryAllowed("barrier.send") {
			measureBarrierSend(ctx, name, mk)
		}
		if ctx.batteryAllowed("barrier.deadline") {
			measureBarrierDeadline(ctx, name, mk)
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func exchangeNames() []string { return sortedKeys(exchangeLikes()) }
func barrierNames() []string  { return sortedKeys(barrierLikes()) }
