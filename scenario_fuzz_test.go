package bigbuff

import (
	"context"
	"sync"
	"testing"
	"time"
)

// FuzzScenarioBroadcast drives every broadcast attachment through a bounded,
// deterministic op stream derived from the fuzz input: at most 24 ops, at
// most 4 live subscribers, publishers always in goroutines, and completion
// bounded by context cancellation + unsubscribe. Assertions: no panic, no
// deadlock (guard), values per session never duplicate/reorder.
func FuzzScenarioBroadcast(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 1, 2})
	f.Add([]byte{1, 1, 1, 0, 2, 4})
	f.Add([]byte{3, 2, 1, 0, 4, 4, 1})
	f.Add([]byte("\x00\x01\x02te"))
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 || len(ops) > 24 {
			return
		}
		// Dedupe by implementation: the same factory attached to many
		// scenarios is fuzzed once per input.
		seenImpl := map[string]bool{}
		var uniq []Attachment
		for _, a := range broadcastAttachments {
			// This active-concurrency stream deliberately excludes attachments
			// whose metadata forbids mid-send withdrawal/close or concurrent
			// slot lifecycle. Those implementations receive dedicated, safe
			// model fuzz targets instead of being silently exercised here.
			if a.ExposureLimited {
				continue
			}
			probe := scenarioBroadcastFactories()[a.Impl]()
			limited, skip := probe.(interface{ SkipBroadcastFuzz() bool })
			skip = skip && limited.SkipBroadcastFuzz()
			probe.Close()
			if skip {
				continue
			}
			if !seenImpl[a.Impl] {
				seenImpl[a.Impl] = true
				uniq = append(uniq, a)
			}
		}
		for _, a := range uniq {
			b := scenarioBroadcastFactories()[a.Impl]()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			var subs []BroadcastSubscriber
			var wg sync.WaitGroup
			val := 0
			for _, op := range ops {
				switch op % 5 {
				case 0:
					if len(subs) < 4 {
						subs = append(subs, b.Subscribe())
					}
				case 1:
					val++
					v := val
					wg.Add(1)
					go func() { defer wg.Done(); b.Publish(v) }()
				case 2:
					if len(subs) > 0 {
						subs[len(subs)-1].Receive(ctx)
					}
				case 3:
					if len(subs) > 0 {
						subs[0].TryReceive()
					}
				case 4:
					if len(subs) > 0 {
						last := subs[len(subs)-1]
						subs = subs[:len(subs)-1]
						last.Unsubscribe()
					}
				}
			}
			cancel()
			for _, s := range subs {
				s.Unsubscribe()
			}
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s/%s: publishers did not finish (deadlock)", a.Scenario, a.Impl)
			}
			b.Close()
		}
	})
}

// FuzzScenarioCasterScatterGather drives caster, scatter and gather
// attachments through bounded op streams in their documented patterns.
func FuzzScenarioCasterScatterGather(f *testing.F) {
	f.Add([]byte{0, 1, 2, 1, 0})
	f.Add([]byte{1, 1, 0, 2, 2})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 || len(ops) > 24 {
			return
		}
		// Scatter: serial operations through each implementation's own wait
		// abstraction, so queued rings and unbuffered channels are both legal.
		// Values are unique and each accepted value must be consumed once.
		for _, a := range scatterAttachments {
			if a.ExposureLimited {
				continue
			}
			x := scatterLikes()[a.Impl]()
			registered := 0
			next, expected := 1, 1
			drain := func() {
				for {
					v, ok := x.try()
					if !ok {
						return
					}
					if v != expected {
						t.Fatalf("scatter %s: got %d want %d", a.Impl, v, expected)
					}
					expected++
				}
			}
			for _, op := range ops {
				switch op % 3 {
				case 0:
					if registered < 4 {
						x.add(1)
						registered++
					}
				case 1:
					if registered == 0 {
						if n := x.send(next); n != 0 {
							t.Fatalf("scatter %s: no receivers but Send=%d", a.Impl, n)
						}
						continue
					}
					sendDone := make(chan int, 1)
					go func(v int) { sendDone <- x.send(v) }(next)
					ctx, cancel := context.WithTimeout(context.Background(), scenarioGuard)
					v, ok := x.wait(ctx)
					cancel()
					if !ok || v != next {
						t.Fatalf("scatter %s: wait=(%d,%v), want %d", a.Impl, v, ok, next)
					}
					select {
					case n := <-sendDone:
						if n != 1 {
							t.Fatalf("scatter %s: Send=%d want 1", a.Impl, n)
						}
					case <-time.After(scenarioGuard):
						t.Fatalf("scatter %s: Send did not complete after receipt", a.Impl)
					}
					next++
					drain()
				case 2:
					if registered > 0 {
						x.add(-1)
						registered--
					}
					drain()
				}
			}
			for registered > 0 {
				x.add(-1)
				registered--
			}
			drain()
		}
		// Gather: ops only register/withdraw; signals happen exclusively
		// inside a proper Wait round (a lone send on the unbuffered channel
		// with no Wait in flight would park forever by design).
		for _, a := range gatherAttachments {
			x := gatherLikes()[a.Impl]()
			registered := 0
			doWait := func() {
				if registered == 0 {
					return
				}
				var wg sync.WaitGroup
				for i := 0; i < registered; i++ {
					wg.Add(1)
					go func() { defer wg.Done(); x.send() }()
				}
				done := make(chan int, 1)
				go func() { done <- x.wait() }()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatalf("gather %s: Wait blocked", a.Impl)
				}
				wg.Wait()
				registered = 0
			}
			for _, op := range ops {
				switch op % 3 {
				case 0:
					x.add(1)
					registered++
				case 1:
					doWait()
				case 2:
					if registered > 0 {
						// Withdraw one sender, then complete the round for
						// the rest so no registration leaks into the next op.
						registered--
						withdrawDone := make(chan int, 1)
						go func() { withdrawDone <- x.wait() }()
						// Compensation for the withdrawn sender is supplied
						// by Add(-1) itself in both implementations.
						x.add(-1)
						var wg sync.WaitGroup
						for i := 0; i < registered; i++ {
							wg.Add(1)
							go func() { defer wg.Done(); x.send() }()
						}
						select {
						case <-withdrawDone:
						case <-time.After(5 * time.Second):
							t.Fatalf("gather %s: Wait after withdraw blocked", a.Impl)
						}
						wg.Wait()
						registered = 0
					}
				}
			}
			doWait()
		}
		// Casters are covered op-by-op by TestScenarioCaster churn facets;
		// here a single bounded round per caster per input keeps fuzz fast.
		for _, a := range casterAttachments {
			if a.ExposureLimited {
				continue
			}
			x := casterLikes()[a.Impl]()
			x.add(1)
			done := make(chan int, 1)
			go func() { done <- x.send(1) }()
			select {
			case v := <-x.c:
				if x.ack != nil {
					x.ack()
				}
				if v != 1 {
					t.Fatalf("caster %s: got %d want 1", a.Impl, v)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("caster %s: receive blocked", a.Impl)
			}
			select {
			case n := <-done:
				if n != 1 {
					t.Fatalf("caster %s: Send=%d want 1", a.Impl, n)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("caster %s: Send blocked", a.Impl)
			}
		}
	})
}
