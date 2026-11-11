package bigbuff

// Timeline traces: the raw material for the swimlane visualisations in the
// HTML report. A trace is one bounded scripted experiment; every event carries
// a nanosecond offset from trace start, a lane (0 = publisher/sender, 1..N =
// subscribers), a kind ("pub", "recv") and the published sequence number.
// Traces are capped in size so they can be embedded in the self-contained
// report, and every blocking script has a watchdog: a blocked publisher is a
// recorded outcome, never a hang.

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type traceEvent struct {
	T    int64  `json:"t"`    // nanoseconds since trace start
	Lane int    `json:"lane"` // 0 = publisher, 1..N = subscribers
	Kind string `json:"kind"` // "pub" | "recv"
	Seq  int    `json:"seq"`  // value published (1-based)
}

type traceResult struct {
	events       []traceEvent
	opsCompleted int64
	outcome      string
	duration     time.Duration
	truncated    bool
	subs         int
}

func collectBroadcastTrace(b Broadcaster, ops, maxEvents int, stalled bool, deadline time.Duration) traceResult {
	start := time.Now()
	var mu sync.Mutex
	var skipUnsub atomic.Bool
	events := make([]traceEvent, 0, 512)
	truncated := false
	appendEvent := func(e traceEvent) {
		mu.Lock()
		if len(events) < maxEvents {
			events = append(events, e)
		} else {
			truncated = true
		}
		mu.Unlock()
	}

	fastSubs := 3
	if stalled {
		fastSubs = 2 // lane 3 is the permanently stalled subscriber
	}
	fastCtx, cancelFast := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < fastSubs; i++ {
		i := i
		s := b.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if skipUnsub.Load() {
					return
				}
				defer func() { _ = recover() }()
				s.Unsubscribe()
			}()
			for {
				v, ok := s.Receive(fastCtx)
				if !ok {
					return
				}
				appendEvent(traceEvent{T: time.Since(start).Nanoseconds(), Lane: i + 1, Kind: "recv", Seq: v})
			}
		}()
	}
	if stalled {
		_ = b.Subscribe() // permanently stalled lane; never Receives
	}

	var opsDone atomic.Int64
	var panicked atomic.Bool
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		defer func() {
			if r := recover(); r != nil {
				panicked.Store(true)
			}
		}()
		for i := 1; i <= ops; i++ {
			b.Publish(i)
			opsDone.Add(1)
			appendEvent(traceEvent{T: time.Since(start).Nanoseconds(), Lane: 0, Kind: "pub", Seq: i})
		}
	}()
	outcome := "completed"
	select {
	case <-pubDone:
		if panicked.Load() {
			outcome = "publisher_panicked"
		}
	case <-time.After(deadline):
		outcome = "publisher_blocked"
	}
	if outcome != "completed" {
		skipUnsub.Store(true)
	}
	cancelFast()
	drained := make(chan struct{})
	go func() { wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
	}
	mu.Lock()
	sort.SliceStable(events, func(i, j int) bool { return events[i].T < events[j].T })
	mu.Unlock()
	subs := fastSubs
	if stalled {
		subs++
	}
	return traceResult{events: events, opsCompleted: opsDone.Load(), outcome: outcome, duration: time.Since(start), truncated: truncated, subs: subs}
}

func collectCasterTrace(x casterLike, ops, maxEvents int, deadline time.Duration) traceResult {
	start := time.Now()
	var mu sync.Mutex
	events := make([]traceEvent, 0, 512)
	truncated := false
	appendEvent := func(e traceEvent) {
		mu.Lock()
		if len(events) < maxEvents {
			events = append(events, e)
		} else {
			truncated = true
		}
		mu.Unlock()
	}

	// The sender waits, before each Send, for the receivers that took the
	// previous value to register again (see casterRounds). Without that the
	// trace of a caster is a column of Sends that deliver nothing.
	const receivers = 3
	if !x.rearmEach {
		for i := 0; i < receivers; i++ {
			x.add(1)
		}
	}
	stop := make(chan struct{})
	ready := make(chan struct{}, receivers)
	var wg sync.WaitGroup
	for r := 0; r < receivers; r++ {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if x.rearmEach {
					x.add(1)
				}
				select {
				case ready <- struct{}{}:
				case <-stop:
					x.add(-1)
					return
				}
				select {
				case v := <-x.c:
					if x.ack != nil {
						x.ack()
					}
					appendEvent(traceEvent{T: time.Since(start).Nanoseconds(), Lane: r + 1, Kind: "recv", Seq: v})
				case <-stop:
					x.add(-1)
					return
				}
			}
		}()
	}

	var opsDone atomic.Int64
	var panicked atomic.Bool
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		defer func() {
			if r := recover(); r != nil {
				panicked.Store(true)
			}
		}()
		toArm := receivers
		for i := 1; i <= ops; i++ {
			for ; toArm > 0; toArm-- {
				select {
				case <-ready:
				case <-stop:
					return
				}
			}
			if k := x.send(i); k > 0 {
				toArm = min(k, receivers)
			}
			opsDone.Add(1)
			appendEvent(traceEvent{T: time.Since(start).Nanoseconds(), Lane: 0, Kind: "pub", Seq: i})
		}
	}()
	outcome := "completed"
	select {
	case <-sendDone:
		if panicked.Load() {
			outcome = "sender_panicked"
		}
	case <-time.After(deadline):
		// Either a Send is blocked, or the sender is waiting for a receiver
		// that a Send reported as served and that never took the value.
		outcome = "sender_blocked"
	}
	close(stop)
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
	}
	if x.close != nil && outcome == "completed" {
		// A still-parked sender must not have channels closed underneath it.
		x.close()
	}
	mu.Lock()
	sort.SliceStable(events, func(i, j int) bool { return events[i].T < events[j].T })
	mu.Unlock()
	return traceResult{events: events, opsCompleted: opsDone.Load(), outcome: outcome, duration: time.Since(start), truncated: truncated, subs: receivers}
}

func writeTraceRecord(ctx *measureContext, family, impl, traceName string, stalled bool, scen []string, goodFit, expLimit bool, res traceResult) {
	ctx.write(map[string]any{
		"type":              "trace",
		"schema":            measureSchemaVersion,
		"family":            family,
		"impl":              impl,
		"trace":             traceName,
		"stalled":           stalled,
		"subs":              res.subs,
		"ops_planned":       ctx.profile.TraceOps,
		"ops_completed":     res.opsCompleted,
		"outcome":           res.outcome,
		"duration_ns":       res.duration.Nanoseconds(),
		"truncated":         res.truncated,
		"scenarios":         scen,
		"expected_good_fit": goodFit,
		"exposure_limited":  expLimit,
		"events":            res.events,
	})
}

func measureBroadcastTrace(ctx *measureContext, factory func() Broadcaster, name string, stalled bool) {
	scen, goodFit, expLimit := broadcastAttachmentMeta(name)
	b := factory()
	traceName := "steady_state"
	if stalled {
		traceName = "stalled_consumer"
	}
	res := collectBroadcastTrace(b, ctx.profile.TraceOps, ctx.profile.TraceMaxEvents, stalled, ctx.profile.StalledDeadline)
	if res.outcome == "completed" {
		closeWithWatchdog(b, 2*time.Second)
	}
	writeTraceRecord(ctx, "broadcast", name, traceName, stalled, scen, goodFit, expLimit, res)
}

func measureCasterTrace(ctx *measureContext, mk func() casterLike, name string) {
	scen, goodFit, expLimit := casterAttachmentMeta(name)
	x := mk()
	res := collectCasterTrace(x, ctx.profile.TraceOps, ctx.profile.TraceMaxEvents, ctx.profile.StalledDeadline)
	writeTraceRecord(ctx, "caster", name, "steady_state", false, scen, goodFit, expLimit, res)
}
