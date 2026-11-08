package bigbuff

import (
	"context"
	"fmt"
	"math/rand"
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

// runPubSubStress: S subscriber goroutines each run consecutive subscription
// sessions. Within a session, a lossless lock-step broadcast must yield a
// strictly consecutive run of values (no gaps, no duplicates).
func runPubSubStress(t *testing.T, mk func() pubsubAPI, subs, sends int, churn bool) {
	x := mk()
	lossless := x.lossless
	stop := make(chan struct{})
	var gaps, dups, yielded atomic.Int64
	var first atomic.Value
	var wg sync.WaitGroup
	ready := make(chan struct{}, subs)
	for s := 0; s < subs; s++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				var ctx context.Context
				var cancel context.CancelFunc
				if churn {
					ctx, cancel = context.WithTimeout(context.Background(), time.Duration(rng.Intn(300))*time.Microsecond)
				} else {
					ctx, cancel = context.WithCancel(context.Background())
				}
				go func() {
					select {
					case <-stop:
						cancel()
					case <-ctx.Done():
					}
				}()
				last := -1
				seq := x.subscribe(ctx)
				if !churn {
					ready <- struct{}{}
				}
				for v := range seq {
					yielded.Add(1)
					if last != -1 {
						if v <= last {
							if dups.Add(1) == 1 {
								first.Store(fmt.Sprintf("dup/reorder: %d after %d", v, last))
							}
						} else if v != last+1 && lossless {
							if gaps.Add(1) == 1 {
								first.Store(fmt.Sprintf("gap: %d after %d", v, last))
							}
						}
					}
					last = v
				}
				cancel()
			}
		}(int64(s))
	}
	if !churn {
		for s := 0; s < subs; s++ {
			<-ready
		}
	}
	var total int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= sends; i++ {
			total += int64(x.send(i))
			if !churn && i == 1 {
				// let everybody subscribe for the steady test
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("DEADLOCK: sender stuck (subs=%d churn=%v, yielded=%d)", subs, churn, yielded.Load())
	}
	close(stop)
	wdone := make(chan struct{})
	go func() { wg.Wait(); close(wdone) }()
	select {
	case <-wdone:
	case <-time.After(10 * time.Second):
		t.Fatalf("DEADLOCK: subscribers stuck after sender finished")
	}
	if yielded.Load() > total {
		t.Logf("yielded %d > Send-reported deliveries %d", yielded.Load(), total)
	}
	if gaps.Load() > 0 || dups.Load() > 0 {
		v, _ := first.Load().(string)
		t.Fatalf("MISDELIVERY: gaps=%d dups=%d (%s) yielded=%d reported=%d", gaps.Load(), dups.Load(), v, yielded.Load(), total)
	}
	t.Logf("ok: reported deliveries=%d yielded=%d", total, yielded.Load())
}

func TestPubSubFamily(t *testing.T) {
	for name, mk := range pubsubImpls() {
		t.Run(name, func(t *testing.T) {
			t.Run("steady", func(t *testing.T) { runPubSubStress(t, mk, 16, 3000, false) })
			t.Run("churn", func(t *testing.T) { runPubSubStress(t, mk, 16, 3000, true) })
		})
	}
}
