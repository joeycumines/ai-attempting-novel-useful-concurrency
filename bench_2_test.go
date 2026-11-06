package bigbuff

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// benchBroadcast: N subscribers consume as fast as they can; the clock covers
// b.N publishes AND every subscriber observing the final value. ns/op is
// therefore end-to-end cost per broadcast to all N subscribers.
func benchBroadcast[T any](b *testing.B, n int, send func(int), subscribe func(context.Context) func(func(T) bool), val func(T) int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	last := make([]atomic.Int64, n)
	var got atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		it := subscribe(ctx) // subscribe synchronously
		go func(i int) {
			defer wg.Done()
			for v := range it {
				last[i].Store(int64(val(v)))
				got.Add(1)
			}
		}(i)
	}
	time.Sleep(10 * time.Millisecond)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		send(i)
	}
	for i := 0; i < n; i++ {
		for last[i].Load() != int64(b.N) {
			time.Sleep(5 * time.Microsecond)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(got.Load())/float64(b.N*n)*100, "%delivered")
	cancel()
	// unblock any lock-step iterators parked on a final receive
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	for k := 0; ; k++ {
		select {
		case <-done:
			return
		case <-time.After(20 * time.Millisecond):
			if k > 50 {
				return // leak in bench only; acceptable
			}
			send(b.N + 1)
		}
	}
}

var benchNs = []int{1, 8, 64, 512}

func BenchmarkLockStep(b *testing.B) {
	names := []string{"ChanPubSub", "ChanPubSub10Gemini", "ChanPubSub17Gemini", "ChanRotor", "PhasePubSub", "RelayPubSub_w4"}
	impls := pubsubImpls()
	impls["RelayPubSub_w64"] = func() pubsubAPI {
		x := NewRelayPubSub[int](64)
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
	}
	names = append(names, "RelayPubSub_w64")
	for _, name := range names {
		for _, n := range benchNs {
			b.Run(fmt.Sprintf("%s/N=%d", name, n), func(b *testing.B) {
				x := impls[name]()
				benchBroadcast(b, n, func(i int) { x.send(i) }, x.subscribe, func(v int) int { return v })
			})
		}
	}
}

func BenchmarkAsync(b *testing.B) {
	impls := asyncImpls()
	impls["ChanFanout_manual"] = func() asyncAPI {
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
	}
	names := []string{
		// lossless
		"ChanRelay_sync", "ChanRelay_w16", "ChanHub_c16", "GenCaster", "RingPubSub_c64", "EpochCaster13Crouton_r64", "ChanFeed_block_c64",
		// lossy
		"ChanFanout_manual", "EpochTape_c64", "ChanLatest", "Beacon", "ChanFeed_drop_c64",
	}
	for _, name := range names {
		for _, n := range benchNs {
			b.Run(fmt.Sprintf("%s/N=%d", name, n), func(b *testing.B) {
				x := impls[name]()
				benchBroadcast(b, n, func(i int) { x.send(mkpv(i)) }, x.subscribe, func(v pv) int { return int(v[0]) })
			})
		}
	}
}
