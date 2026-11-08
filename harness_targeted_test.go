package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	impls := asyncImpls
	_ = impls
}

// Core-API variants for impls whose SubscribeContext is suspected buggy.
func TestAsyncCoreVariants(t *testing.T) {
	variants := map[string]func() asyncAPI{
		"ChanFanout_manual": func() asyncAPI {
			x := NewChanFanout[pv](64)
			return asyncAPI{send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) {
				s := x.Subscribe()
				return func(y func(pv) bool) {
					defer s.Close()
					for {
						if v, _, ok := s.TryRecv(); ok {
							if !y(v) {
								return
							}
							continue
						}
						select {
						case <-ctx.Done():
							return
						case <-s.Ready():
						}
					}
				}
			}}
		},
		"EpochCasterGemini_manual": func() asyncAPI {
			x := NewEpochCasterGemini[pv](64)
			return asyncAPI{send: func(v pv) { x.Send(v) }, subscribe: func(ctx context.Context) func(func(pv) bool) {
				s := x.Subscribe()
				return func(y func(pv) bool) {
					defer s.Disarm()
					for {
						if v, _, ok := s.TryRecv(); ok {
							if !y(v) {
								return
							}
							continue
						}
						select {
						case <-ctx.Done():
							return
						case <-s.Arm():
							s.AckPulse()
						}
					}
				}
			}}
		},
	}
	for name, mk := range variants {
		t.Run(name, func(t *testing.T) {
			t.Run("steady", func(t *testing.T) { runAsyncStress(t, mk, 16, 5000, false, true) })
			t.Run("churn", func(t *testing.T) { runAsyncStress(t, mk, 16, 5000, true, false) })
		})
	}
}

// ChanFanout.SubscribeContext: cancellation while blocked waiting for a value.
func TestFanoutSubscribeContextCancel(t *testing.T) {
	x := NewChanFanout[int](8)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	started := make(chan struct{}, 1)
	go func() {
		seq := x.SubscribeContext(ctx)
		started <- struct{}{}
		for range seq {
		}
		close(done)
	}()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("BUG: SubscribeContext iterator does not return on ctx cancel while idle (goroutine leak)")
	}
}

// ChanBeacon: Seq after Close busy-spins instead of terminating.
func TestChanBeaconSeqAfterClose(t *testing.T) {
	x := NewChanBeacon[int](8)
	s := x.Subscribe()
	x.Send(1)
	x.Close()
	done := make(chan int)
	go func() {
		n := 0
		for range s.Seq(context.Background()) {
			n++
		}
		done <- n
	}()
	select {
	case n := <-done:
		t.Logf("terminated after %d values", n)
	case <-time.After(2 * time.Second):
		t.Fatal("BUG: RelaySub.Seq spins forever after ChanBeacon.Close (doc says it terminates)")
	}
}

// ChanRelay: live-count drift under concurrent close + compaction.
func TestChanRelayCountDrift(t *testing.T) {
	x := NewChanRelay[int](WithRelayWindow(1))
	const keep = 4
	kept := make([]*RelaySubscription[int], keep)
	for i := range kept {
		kept[i] = x.Subscribe()
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20000; i++ {
				s := x.Subscribe()
				s.Close()
			}
		}()
	}
	wg.Wait()
	got := x.Len()
	n := x.TrySend(42)
	if got != keep || n != keep {
		t.Fatalf("BUG: Len()=%d TrySend delivered=%d, want %d live subscriptions", got, n, keep)
	}
}

// ChanScatter: steal path uses non-blocking receive; if the sender is not yet
// parked, the steal misses and Send blocks forever with zero receivers.
func TestChanScatterStealRace(t *testing.T) {
	for iter := 0; iter < 20000; iter++ {
		x := NewChanScatter(make(chan int))
		x.Add(1)
		done := make(chan struct{})
		go func() { x.Send(1); close(done) }()
		if iter%2 == 0 {
			runtime.Gosched()
		}
		// receiver gives up without receiving
		x.Add(-1)
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("BUG: Send stuck with zero receivers after %d iterations", iter)
		}
	}
}

// GenCaster: a stalled subscriber pins every value published since (unbounded).
func TestGenCasterLaggardRetention(t *testing.T) {
	x := NewGenCaster[[1024]byte]()
	sub := x.Subscribe() // never reads
	defer runtime.KeepAlive(&sub)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < 20000; i++ {
		x.Send([1024]byte{})
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	t.Logf("retained heap growth with one stalled subscriber over 20k x 1KiB sends: %.1f MiB",
		float64(after.HeapAlloc-before.HeapAlloc)/(1<<20))
}

// EpochCaster13Crouton: a stalled subscriber makes Send spin (Gosched) forever.
func TestCroutonStalledSubscriberSpin(t *testing.T) {
	x := NewEpochCasterWithRingSize13Crouton[int](16)
	_ = x.Subscribe() // never reads
	var sent atomic.Int64
	go func() {
		for i := 0; i < 100; i++ {
			x.Send(i)
			sent.Add(1)
		}
	}()
	for i := 0; i < 1000; i++ {
		runtime.Gosched()
	}
	t.Logf("sends completed before blocking: %d (expected ring size 16); publisher burns CPU via Gosched loop while blocked, no ctx", sent.Load())
}
