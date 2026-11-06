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

func runAsyncStress(t *testing.T, mk func() asyncAPI, subs, sends int, churn bool, slow bool) {
	x := mk()
	stop := make(chan struct{})
	var gaps, dups, torn, yielded atomic.Int64
	var first atomic.Value
	lastSeen := make([]atomic.Int64, subs)
	var wg sync.WaitGroup
	for s := 0; s < subs; s++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
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
				last := int64(-1)
				for v := range x.subscribe(ctx) {
					yielded.Add(1)
					if v[0] != v[1] || v[1] != v[2] || v[2] != v[3] {
						if torn.Add(1) == 1 {
							first.Store(fmt.Sprintf("torn read %v", v))
						}
					}
					n := int64(v[0])
					if last != -1 {
						if n <= last {
							if dups.Add(1) == 1 {
								first.Store(fmt.Sprintf("dup/reorder %d after %d", n, last))
							}
						} else if n != last+1 && x.lossless {
							if gaps.Add(1) == 1 {
								first.Store(fmt.Sprintf("gap %d after %d", n, last))
							}
						}
					}
					last = n
					lastSeen[id].Store(n)
					if slow && id == 0 && rng.Intn(8) == 0 {
						time.Sleep(20 * time.Microsecond)
					}
				}
				cancel()
			}
		}(s)
	}
	time.Sleep(30 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= sends; i++ {
			x.send(mkpv(i))
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("DEADLOCK: sender stuck (yielded=%d)", yielded.Load())
	}
	if !churn {
		// liveness: every long-lived subscriber must observe the final value
		deadline := time.Now().Add(5 * time.Second)
		for s := 0; s < subs; s++ {
			for lastSeen[s].Load() != int64(sends) {
				if time.Now().After(deadline) {
					t.Fatalf("STALL/LOST-WAKEUP: subscriber %d stuck at %d of %d", s, lastSeen[s].Load(), sends)
				}
				time.Sleep(time.Millisecond)
			}
		}
	}
	close(stop)
	wdone := make(chan struct{})
	go func() { wg.Wait(); close(wdone) }()
	select {
	case <-wdone:
	case <-time.After(10 * time.Second):
		t.Fatalf("DEADLOCK: subscribers stuck after sender finished")
	}
	if gaps.Load()+dups.Load()+torn.Load() > 0 {
		v, _ := first.Load().(string)
		t.Fatalf("MISDELIVERY: gaps=%d dups=%d torn=%d (%s)", gaps.Load(), dups.Load(), torn.Load(), v)
	}
	t.Logf("ok: yielded=%d (%.1f%% of max %d)", yielded.Load(), 100*float64(yielded.Load())/float64(subs*sends), subs*sends)
}

func TestAsyncFamily(t *testing.T) {
	for name, mk := range asyncImpls() {
		t.Run(name, func(t *testing.T) {
			t.Run("steady", func(t *testing.T) { runAsyncStress(t, mk, 16, 5000, false, true) })
			t.Run("churn", func(t *testing.T) { runAsyncStress(t, mk, 16, 5000, true, false) })
		})
	}
}
