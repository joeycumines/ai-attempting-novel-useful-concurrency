package bigbuff

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A lock-step (lossless, sender-waits) pubsub. subscribe returns an iterator
// bound to ctx. Each implementation is driven through its documented consumer
// pattern.
type pubsubAPI struct {
	send      func(int) int
	subscribe func(ctx context.Context) func(yield func(int) bool)
	// lossless reports whether the implementation guarantees every sent value
	// is delivered to every active subscriber session (lock-step pubsubs).
	// Beacon-style impls (single latest-value slot + ack barrier) are
	// lossy under churn by design: gaps are permitted, but duplicates,
	// reordering, and stalls are always failures.
	lossless bool
}

func seqOf[T any](f func(context.Context) func(func(T) bool)) func(context.Context) func(func(T) bool) {
	return f
}

func pubsubImpls() map[string]func() pubsubAPI {
	return map[string]func() pubsubAPI{
		"ChanPubSub": func() pubsubAPI {
			x := NewChanPubSub(make(chan int))
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) { return x.SubscribeContext(ctx) }, true}
		},
		"ChanPubSubV2_06meta": func() pubsubAPI {
			x := NewChanPubSubV2(make(chan int))
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) {
				x.Subscribe()
				return func(yield func(int) bool) {
					defer x.Unsubscribe()
					for {
						select {
						case <-ctx.Done():
							return
						case v := <-x.C():
							x.Wait()
							if !yield(v) {
								return
							}
						}
					}
				}
			}, true}
		},
		"ChanPubSub09Gemini": func() pubsubAPI {
			x := NewChanPubSub09Gemini(make(chan int))
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) { return x.SubscribeContext(ctx) }, true}
		},
		"ChanPubSub10Gemini": func() pubsubAPI {
			x := NewChanPubSub10Gemini(make(chan int))
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) { return x.SubscribeContext(ctx) }, true}
		},
		"ChanPubSub14Xing": func() pubsubAPI {
			x := NewChanPubSub14Xing(make(chan int))
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) { return x.SubscribeContext(ctx) }, true}
		},
		"ChanPubSub16DeepSeek": func() pubsubAPI {
			x := NewChanPubSub16DeepSeek(make(chan int))
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) { return x.SubscribeContext(ctx) }, true}
		},
		"ChanPubSub17Gemini": func() pubsubAPI {
			x := NewChanPubSub17Gemini(make(chan int))
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) { return x.SubscribeContext(ctx) }, true}
		},
		"GenPubSub50Meta": func() pubsubAPI {
			x := NewGenPubSub50Meta(make(chan int))
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) { return x.SubscribeContext(ctx) }, true}
		},
		"ChanPubSub58Spark": func() pubsubAPI {
			x := NewChanPubSub58Spark(make(chan int))
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) { return x.SubscribeContext(ctx) }, true}
		},
		"ChanGathererPubSub": func() pubsubAPI {
			x := NewChanGathererPubSub(make(chan int), make(chan struct{}))
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) { return x.SubscribeContext(ctx) }, true}
		},
		"ReliableBeacon": func() pubsubAPI {
			x := NewReliableBeacon[int]()
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) { return x.SubscribeContext(ctx) }, false}
		},
		"EpochPubSub_11meta": func() pubsubAPI {
			x := NewEpochPubSub[int]()
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) { return x.SubscribeContext(ctx) }, false}
		},
		"ChanRotor": func() pubsubAPI {
			x := MakeChanRotor[int]()
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) { return x.SubscribeContext(ctx) }, true}
		},
		"PhasePubSub": func() pubsubAPI {
			x := NewPhasePubSub[int]()
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) {
				sub := x.Subscribe()
				return func(yield func(int) bool) {
					defer sub.Unsubscribe()
					for {
						select {
						case <-ctx.Done():
							return
						case <-sub.C():
							if !yield(sub.Read()) {
								return
							}
						}
					}
				}
			}, true}
		},
		"RelayPubSub_w4": func() pubsubAPI {
			x := NewRelayPubSub[int](4)
			return pubsubAPI{x.Send, func(ctx context.Context) func(func(int) bool) {
				sub := x.Subscribe()
				return func(yield func(int) bool) {
					defer sub.Unsubscribe()
					for {
						select {
						case <-ctx.Done():
							return
						case <-sub.C():
							if !yield(sub.Read()) {
								return
							}
						}
					}
				}
			}, true}
		},
	}
}

// runPubSubStress uses the same exact ack/ready round handoff as the async
// family. The ack releases a lock-step Send; the ready signal confirms the
// subscriber is parked on the next round or has registered its churn replacement.
// Values are exact and ordered within each session; the steady facet additionally
// checks Send's reported count.
func runPubSubStress(t *testing.T, mk func() pubsubAPI, subs, sends int, churn bool) {
	t.Helper()
	x := mk()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acks := make(chan struct{}, subs)
	ready := make(chan struct{}, subs)
	initialReady := make(chan struct{}, subs)
	var wg sync.WaitGroup
	var total atomic.Int64
	var churnOps atomic.Int64
	var firstViolation atomic.Value
	fail := func(format string, args ...any) {
		if firstViolation.CompareAndSwap(nil, fmt.Sprintf(format, args...)) {
			cancel()
		}
	}
	for id := 0; id < subs; id++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := 0
			pendingReady := false
			for session := 0; session < 4; session++ {
				sessionCtx, retire := context.WithCancel(ctx)
				seq := x.subscribe(sessionCtx) // concrete factories register synchronously
				if pendingReady {
					select {
					case ready <- struct{}{}:
					case <-sessionCtx.Done():
						retire()
						return
					}
					pendingReady = false
				} else if session == 0 {
					initialReady <- struct{}{}
				}
				received := 0
				resubscribe := false
				seq(func(v int) bool {
					if v != last+1 {
						fail("duplicate/reorder/gap: got %d after %d", v, last)
						return false
					}
					last = v
					received++
					total.Add(1)
					select {
					case acks <- struct{}{}:
					case <-sessionCtx.Done():
						return false
					}
					if churn && session < 3 && received == 4 {
						resubscribe = true
						pendingReady = true
						return false
					}
					select {
					case ready <- struct{}{}:
					case <-sessionCtx.Done():
						return false
					}
					return v != sends
				})
				retire()
				if ctx.Err() != nil {
					return
				}
				if resubscribe {
					churnOps.Add(1)
					continue
				}
				return
			}
		}()
	}
	for i := 0; i < subs; i++ {
		select {
		case <-initialReady:
		case <-ctx.Done():
			t.Fatal("subscriber registration cancelled")
		}
	}
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		for v := 1; v <= sends; v++ {
			if ctx.Err() != nil {
				return
			}
			n := x.send(v)
			if !churn && n != subs {
				fail("Send(%d)=%d want %d", v, n, subs)
				return
			}
			for i := 0; i < subs; i++ {
				select {
				case <-acks:
				case <-ctx.Done():
					return
				}
			}
			// Wait for every subscriber to be parked on the next round (or,
			// at a churn boundary, to have registered its replacement). This
			// is essential for lock-step impls whose Add/Subscribe fences
			// against an in-flight Send: the ack releases Send before the
			// replacement registers, and ready prevents the next Send from
			// racing that registration.
			for i := 0; i < subs; i++ {
				select {
				case <-ready:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	select {
	case <-pubDone:
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("publisher did not finish")
	}
	cancel()
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(30 * time.Second):
		t.Fatal("subscribers did not terminate")
	}
	if v, ok := firstViolation.Load().(string); ok {
		t.Fatal(v)
	}
	if got := total.Load(); got != int64(subs*sends) {
		t.Fatalf("total=%d want %d", got, subs*sends)
	}
	if churn && churnOps.Load() != int64(subs*3) {
		t.Fatalf("churn transitions=%d want %d", churnOps.Load(), subs*3)
	}
}

func TestPubSubFamily(t *testing.T) {
	for name, mk := range pubsubImpls() {
		t.Run(name, func(t *testing.T) {
			t.Run("steady", func(t *testing.T) { runPubSubStress(t, mk, 16, 3000, false) })
			t.Run("churn", func(t *testing.T) { runPubSubStress(t, mk, 16, 3000, true) })
		})
	}
}
