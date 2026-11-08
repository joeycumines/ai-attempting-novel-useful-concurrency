package bigbuff

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pv is a multi-word payload: a torn read shows up as unequal words.
type pv [4]uint64

func mkpv(i int) pv { u := uint64(i); return pv{u, u, u, u} }

type asyncAPI struct {
	send      func(pv)
	subscribe func(ctx context.Context) func(yield func(pv) bool)
	lossless  bool
}

func asyncImpls() map[string]func() asyncAPI {
	return map[string]func() asyncAPI{
		"BaselineFanout_c64": func() asyncAPI {
			x := NewBaselineFanout[pv](64)
			return asyncAPI{send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) { return x.SubscribeContext(ctx) }}
		},
		"Beacon": func() asyncAPI {
			x := NewBeacon[pv]()
			return asyncAPI{send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) { return x.SubscribeContext(ctx) }}
		},
		"ChanBeacon_c64": func() asyncAPI {
			x := NewChanBeacon[pv](64)
			return asyncAPI{send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) {
				s := x.Subscribe()
				return func(y func(pv) bool) {
					defer s.Unsubscribe()
					for v := range s.Seq(ctx) {
						if !y(v) {
							return
						}
					}
				}
			}}
		},
		"ChanFanout_c64": func() asyncAPI {
			x := NewChanFanout[pv](64)
			return asyncAPI{send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) { return x.SubscribeContext(ctx) }}
		},
		"ChanFeed_drop_c64": func() asyncAPI {
			x := NewChanFeed[pv](64, ChanFeedDropOldest)
			return asyncAPI{send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) { return x.SubscribeContext(ctx) }}
		},
		"ChanFeed_block_c64": func() asyncAPI {
			x := NewChanFeed[pv](64, ChanFeedBlock)
			return asyncAPI{lossless: true, send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) { return x.SubscribeContext(ctx) }}
		},
		"ChanLatest": func() asyncAPI {
			x := NewChanLatest[pv]()
			return asyncAPI{send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) {
				s := x.Subscribe()
				return func(y func(pv) bool) {
					defer s.Unsubscribe()
					for {
						select {
						case <-ctx.Done():
							return
						case v, ok := <-s.C():
							if !ok || !y(v) {
								return
							}
						}
					}
				}
			}}
		},
		"EpochCasterGemini_c64": func() asyncAPI {
			x := NewEpochCasterGemini[pv](64)
			return asyncAPI{send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) {
				it := x.SubscribeContext(ctx)
				return func(y func(pv) bool) {
					for v := range it {
						if !y(v) {
							return
						}
					}
				}
			}}
		},
		"EpochCaster13Crouton_r64": func() asyncAPI {
			x := NewEpochCasterWithRingSize13Crouton[pv](64)
			return asyncAPI{lossless: true, send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) { return x.SubscribeContext(ctx) }}
		},
		"EpochTape_c64": func() asyncAPI {
			x := NewEpochTape[pv](64)
			return asyncAPI{send: func(v pv) { x.Publish(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) {
				it := x.SubscribeContext(ctx)
				return func(y func(pv) bool) {
					for v := range it {
						if !y(v) {
							return
						}
					}
				}
			}}
		},
		"GenCaster": func() asyncAPI {
			x := NewGenCaster[pv]()
			return asyncAPI{lossless: true, send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) {
				s := x.Subscribe()
				return func(y func(pv) bool) {
					for {
						for v, ok := s.TryNext(); ok; v, ok = s.TryNext() {
							if !y(v) {
								return
							}
						}
						select {
						case <-ctx.Done():
							return
						case <-s.C():
						}
					}
				}
			}}
		},
		"RingBeacon_c64": func() asyncAPI {
			x := NewRingBeacon[pv](64)
			return asyncAPI{send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) { return x.SubscribeContext(ctx) }}
		},
		"RingPubSub_c64": func() asyncAPI {
			x := NewRingPubSub[pv](64)
			return asyncAPI{lossless: true, send: func(v pv) { x.Send(nil, v) }, subscribe: func(ctx context.Context) func(func(pv) bool) {
				s := x.Subscribe()
				return func(y func(pv) bool) {
					defer s.Close()
					for {
						v, err := s.Recv(ctx)
						if err != nil || !y(v) {
							return
						}
					}
				}
			}}
		},
		"ChanRelay_sync": func() asyncAPI {
			x := NewChanRelay[pv]()
			return asyncAPI{lossless: true, send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) { return x.SubscribeContext(ctx) }}
		},
		"ChanRelay_w16": func() asyncAPI {
			x := NewChanRelay[pv](WithRelayWindow(16))
			return asyncAPI{lossless: true, send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) { return x.SubscribeContext(ctx) }}
		},
		"Broadcaster58Spark_c64": func() asyncAPI {
			x := NewBroadcaster58Spark[pv](64)
			return asyncAPI{send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) {
				return x.SubscribeContext(ctx)
			}}
		},
		"RingFanOut14Xing_c64": func() asyncAPI {
			x := NewRingFanOut14Xing[chan pv, pv](64, 16, 16)
			return asyncAPI{send: func(v pv) { _ = x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) { return x.SubscribeContext(ctx) }}
		},
		"BaselineAsyncFanout_c64": func() asyncAPI {
			x := NewBaselineAsyncFanout[pv](64, 16)
			return asyncAPI{send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) { return x.SubscribeContext(ctx) }}
		},
		"ChanHub_c16": func() asyncAPI {
			x := NewChanHub[pv]()
			return asyncAPI{lossless: true, send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) {
				ch := make(chan pv, 16)
				id := x.Subscribe(ch)
				return func(y func(pv) bool) {
					defer x.Unsubscribe(id)
					for {
						select {
						case <-ctx.Done():
							return
						case v := <-ch:
							if !y(v) {
								return
							}
						}
					}
				}
			}}
		},
	}
}

// runAsyncStress drives an exact, round-based ack/ready handoff. The publisher
// does not advance until every subscriber has acknowledged the current value and
// signalled readiness for the next; a churn boundary acknowledges first (releasing
// any lock-step Send), then retires and re-registers before signalling readiness.
// This keeps counts exact and bounded without sleeps, timers, or a shared receipt
// queue that can fill while a backpressured publisher is waiting for readers.
// Overflow/eviction behaviour is covered by the dedicated burst, model and fuzz
// cases; this facet checks payload integrity, ordering, liveness and lifecycle.
func runAsyncStress(t *testing.T, mk func() asyncAPI, subs, sends int, churn bool, slow bool) {
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
			last := int64(0)
			pendingReady := false
			for session := 0; session < 4; session++ {
				sessionCtx, retire := context.WithCancel(ctx)
				seq := x.subscribe(sessionCtx)
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
				seq(func(v pv) bool {
					if v[0] != v[1] || v[0] != v[2] || v[0] != v[3] {
						fail("torn payload: %v", v)
						return false
					}
					n := int64(v[0])
					if n != last+1 {
						fail("duplicate/reorder/gap: got %d after %d", n, last)
						return false
					}
					last = n
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
					if slow {
						runtime.Gosched()
					}
					return n != int64(sends)
				})
				retire()
				if ctx.Err() != nil {
					return
				}
				if resubscribe {
					churnOps.Add(1)
					// Register the replacement before acknowledging the round,
					// so the publisher cannot publish the next value into a gap.
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
			x.send(mkpv(v))
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
			if slow {
				runtime.Gosched()
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

func TestAsyncFamily(t *testing.T) {
	for name, mk := range asyncImpls() {
		t.Run(name, func(t *testing.T) {
			t.Run("steady", func(t *testing.T) { runAsyncStress(t, mk, 16, 5000, false, true) })
			t.Run("churn", func(t *testing.T) { runAsyncStress(t, mk, 16, 5000, true, false) })
		})
	}
}
