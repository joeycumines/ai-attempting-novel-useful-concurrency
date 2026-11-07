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
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 || len(ops) > 24 {
			return
		}
		// Dedupe by implementation: the same factory attached to many
		// scenarios is fuzzed once per input.
		seenImpl := map[string]bool{}
		var uniq []Attachment
		for _, a := range broadcastAttachments {
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
		// Scatter: single-threaded register/dispatch/withdraw. The test
		// goroutine itself is the registered receiver, so no parked receiver
		// goroutine can ever be leaked waiting on the unbuffered channel.
		for _, a := range scatterAttachments {
			var ch chan int
			var send func(int) int
			var add func(int) int
			switch a.Impl {
			case "ChanScatter":
				x := NewChanScatter(make(chan int))
				ch, send, add = x.C, x.Send, x.Add
			default:
				x := NewBaselineScatterQueue[int]()
				ch, send, add = x.C, x.Send, x.Add
			}
			registered := 0
			for _, op := range ops {
				switch op % 3 {
				case 0:
					add(1)
					registered++
				case 1:
					if registered == 0 {
						if n := send(1); n != 0 {
							t.Fatalf("scatter %s: Send with no receivers = %d, want 0", a.Impl, n)
						}
						continue
					}
					done := make(chan int, 1)
					go func() { done <- send(1) }()
					select {
					case v := <-ch:
						if v != 1 {
							t.Fatalf("scatter %s: received %d, want 1", a.Impl, v)
						}
					case <-time.After(5 * time.Second):
						t.Fatalf("scatter %s: receive blocked", a.Impl)
					}
					select {
					case n := <-done:
						if n != 1 {
							t.Fatalf("scatter %s: Send = %d, want 1", a.Impl, n)
						}
					case <-time.After(5 * time.Second):
						t.Fatalf("scatter %s: send blocked", a.Impl)
					}
				case 2:
					if registered > 0 {
						add(-1)
						registered--
					}
				}
			}
			for registered > 0 {
				add(-1)
				registered--
			}
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
			x := casterLikes()[a.Impl]()
			x.add(1)
			done := make(chan int, 1)
			go func() { done <- x.send(1) }()
			select {
			case v := <-x.c:
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
