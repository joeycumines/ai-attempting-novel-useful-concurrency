package bigbuff

// Scatter facets: work distribution with exactly-once accounting.

import (
	"context"
	"sync"
	"testing"
	"time"
)

type scatterLike struct {
	send    func(int) int
	add     func(int) int
	wait    func(context.Context) (int, bool)
	try     func() (int, bool)
	dropped func() uint64
}

func scatterLikes() map[string]func() scatterLike {
	return map[string]func() scatterLike{
		"ChanScatter": func() scatterLike {
			x := NewChanScatter(make(chan int))
			return scatterLike{send: x.Send, add: x.Add,
				wait: func(ctx context.Context) (int, bool) {
					select {
					case <-ctx.Done():
						return 0, false
					case v := <-x.C:
						return v, true
					}
				},
				try: func() (int, bool) {
					select {
					case v := <-x.C:
						return v, true
					default:
						return 0, false
					}
				},
				dropped: func() uint64 { return 0 }}
		},
		"BaselineScatterQueue": func() scatterLike {
			x := NewBaselineScatterQueue[int]()
			return scatterLike{send: x.Send, add: x.Add,
				wait: func(ctx context.Context) (int, bool) {
					select {
					case <-ctx.Done():
						return 0, false
					case v := <-x.C:
						return v, true
					}
				},
				try: func() (int, bool) {
					select {
					case v := <-x.C:
						return v, true
					default:
						return 0, false
					}
				},
				dropped: func() uint64 { return 0 }}
		},
		"StableRing32Nano (32-Nano)": func() scatterLike {
			x := NewStableRing32Nano(make(chan int), 8)
			return scatterLike{send: x.Send, add: x.Add, wait: x.Wait, try: x.TryWait, dropped: x.Dropped}
		},
		"BaselineBoundedScatterQueue (baseline)": func() scatterLike {
			x := NewBaselineBoundedScatterQueue[int](8)
			return scatterLike{send: x.Send, add: x.Add, wait: x.Wait, try: x.TryWait, dropped: x.Dropped}
		},
	}
}

func TestScenarioScatter(t *testing.T) {
	for _, a := range scatterAttachments {
		t.Run(a.Impl, func(t *testing.T) {
			mk, ok := scatterLikes()[a.Impl]
			if !ok {
				t.Fatalf("no scatter factory %q", a.Impl)
			}
			x := mk()
			if got := x.send(1); got != 0 {
				t.Fatalf("send with no receivers = %d, want 0", got)
			}
			runScatterFacet(t, a, 8, 200)
		})
	}
}

// runScatterFacet starts a registered pool, then dispatches one bounded value
// at a time and observes its single completion before admitting the next. This
// is intentionally not an overflow test: it isolates exactly-one dispatch for
// both rendezvous and queued implementations without scheduler-dependent loss.
func runScatterFacet(t *testing.T, a Attachment, receivers, sends int) {
	t.Helper()
	x := scatterLikes()[a.Impl]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan int, sends)
	ready := make(chan struct{}, receivers)
	var wg sync.WaitGroup
	for r := 0; r < receivers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			x.add(1)
			ready <- struct{}{}
			defer x.add(-1)
			for {
				v, ok := x.wait(ctx)
				if !ok {
					return
				}
				received <- v
			}
		}()
	}
	for r := 0; r < receivers; r++ {
		<-ready
	}
	for v := 1; v <= sends; v++ {
		done := make(chan int, 1)
		go func(v int) { done <- x.send(v) }(v)
		select {
		case n := <-done:
			if n != 1 {
				t.Fatalf("%s: Send(%d)=%d want 1", a.Impl, v, n)
			}
		case <-time.After(scenarioGuard):
			t.Fatalf("%s: Send(%d) blocked", a.Impl, v)
		}
		select {
		case got := <-received:
			if got != v {
				t.Fatalf("%s: received %d want %d", a.Impl, got, v)
			}
		case <-time.After(scenarioGuard):
			t.Fatalf("%s: value %d was not dispatched exactly once", a.Impl, v)
		}
	}
	cancel()
	wg.Wait()
	if got := x.dropped(); got != 0 {
		t.Fatalf("%s: unexpected eviction in non-overflow facet: %d", a.Impl, got)
	}
	// Last-receiver withdrawal must not hang admission. A queued design may
	// legitimately have accepted the item before withdrawal; drain it below.
	for i := 0; i < 50; i++ {
		x.add(1)
		done := make(chan int, 1)
		go func() { done <- x.send(777) }()
		x.add(-1)
		select {
		case <-done:
		case <-time.After(scenarioGuard):
			t.Fatalf("%s: admission hung after last receiver withdrew", a.Impl)
		}
		for {
			if _, ok := x.try(); !ok {
				break
			}
		}
	}
}
