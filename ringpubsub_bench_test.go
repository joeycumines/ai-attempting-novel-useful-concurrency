/*
   Copyright 2026 Joseph Cumines

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bigbuff

import (
	"fmt"
	"sync"
	"testing"
)

// BenchmarkRingPubSub_Broadcast measures complete delivery, including the final drain, not
// merely publication/admission. Every receiver checks every value's order and
// computes a checksum. Setup, goroutine creation, channel creation, warmup, and
// final unsubscription are outside the timed region. Receivers use an actual
// two-case select, with a live cancellation channel, when waiting.
//
// LinearChannels is a synthetic best-case linear fan-out over a fixed slice of
// per-subscriber channels; it has no registry lookup/lock overhead. It is not a
// reconstruction of an unspecified callback implementation. A buffer of B gives
// each subscriber B slots, whereas RingPubSub shares B payload slots in total.
//
// RingFast attempts TryRecv before waiting. RingSelect always uses Ready in a
// select first. Both accept false/spurious readiness by retrying TryRecv.
func BenchmarkRingPubSub_Broadcast(b *testing.B) {
	for _, n := range []int{1, 8, 64, 512} {
		b.Run(fmt.Sprintf("ChanPubSub/N=%d", n), func(b *testing.B) {
			benchmarkChanPubSub(b, n)
		})
		for _, capacity := range []int{1, 64} {
			b.Run(fmt.Sprintf("RingFast/B=%d/N=%d", capacity, n), func(b *testing.B) {
				benchmarkRingPubSub(b, n, capacity, false)
			})
			b.Run(fmt.Sprintf("RingSelect/B=%d/N=%d", capacity, n), func(b *testing.B) {
				benchmarkRingPubSub(b, n, capacity, true)
			})
		}
		for _, capacity := range []int{0, 64} {
			b.Run(fmt.Sprintf("LinearChannels/B=%d/N=%d", capacity, n), func(b *testing.B) {
				benchmarkLinearChannels(b, n, capacity)
			})
		}
	}
}

type broadcastBenchResult struct {
	sum uint64
	bad bool
}

func benchmarkCheckResults(b *testing.B, results []broadcastBenchResult) {
	b.Helper()
	n := uint64(b.N)
	want := n * ((n - 1) / 2)
	if n%2 == 0 {
		want = (n / 2) * (n - 1)
	}
	for i, got := range results {
		if got.bad || got.sum != want {
			b.Fatalf("receiver %d: order failure=%v checksum=%d; want %d", i, got.bad, got.sum, want)
		}
	}
}

func benchmarkChanPubSub(b *testing.B, n int) {
	x := NewChanPubSub(make(chan uint64))
	c := x.C()
	x.Add(n)
	stop := make(chan struct{})
	results := make([]broadcastBenchResult, n)
	var warmed, done sync.WaitGroup
	warmed.Add(n)
	done.Add(n)
	for id := range n {
		go func() {
			defer done.Done()
			select {
			case <-c:
				x.Wait()
			case <-stop:
				panic("benchmark stopped during warmup")
			}
			warmed.Done()
			var result broadcastBenchResult
			for i := 0; i < b.N; i++ {
				select {
				case v := <-c:
					x.Wait()
					result.sum += v
					if v != uint64(i) {
						result.bad = true
					}
				case <-stop:
					panic("benchmark stopped before delivery completed")
				}
			}
			results[id] = result
		}()
	}
	x.Send(0)
	warmed.Wait()
	b.ReportAllocs()
	b.SetBytes(int64(n * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := x.Send(uint64(i)); got != n {
			b.Fatalf("sent to %d subscribers; want %d", got, n)
		}
	}
	done.Wait()
	b.StopTimer()
	x.Add(-n)
	close(stop)
	benchmarkCheckResults(b, results)
}

func benchmarkRingPubSub(b *testing.B, n, capacity int, alwaysSelect bool) {
	x := NewRingPubSub[uint64](capacity)
	subs := make([]RingSubscription[uint64], n)
	for i := range subs {
		subs[i] = x.Subscribe()
	}
	stop := make(chan struct{})
	results := make([]broadcastBenchResult, n)
	var warmed, done sync.WaitGroup
	warmed.Add(n)
	done.Add(n)
	for id := range n {
		go func() {
			defer done.Done()
			s := &subs[id]
			benchmarkRingReceive(s, stop, alwaysSelect)
			warmed.Done()
			var result broadcastBenchResult
			for i := 0; i < b.N; i++ {
				v := benchmarkRingReceive(s, stop, alwaysSelect)
				result.sum += v
				if v != uint64(i) {
					result.bad = true
				}
			}
			results[id] = result
		}()
	}
	if _, err := x.Send(nil, 0); err != nil {
		b.Fatal(err)
	}
	warmed.Wait()
	b.ReportAllocs()
	b.SetBytes(int64(n * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got, err := x.Send(nil, uint64(i)); err != nil || got != n {
			b.Fatalf("sent to %d subscribers, error %v; want %d", got, err, n)
		}
	}
	done.Wait()
	b.StopTimer()
	for i := range subs {
		subs[i].Close()
	}
	x.Close()
	close(stop)
	benchmarkCheckResults(b, results)
}

func benchmarkRingReceive(s *RingSubscription[uint64], stop <-chan struct{}, alwaysSelect bool) uint64 {
	for {
		if !alwaysSelect {
			if v, ok := s.TryRecv(); ok {
				return v
			}
		}
		select {
		case <-s.Ready():
			if v, ok := s.TryRecv(); ok {
				return v
			}
		case <-stop:
			panic("benchmark stopped before delivery completed")
		}
	}
}

func benchmarkLinearChannels(b *testing.B, n, capacity int) {
	channels := make([]chan uint64, n)
	for i := range channels {
		channels[i] = make(chan uint64, capacity)
	}
	stop := make(chan struct{})
	results := make([]broadcastBenchResult, n)
	var warmed, done sync.WaitGroup
	warmed.Add(n)
	done.Add(n)
	for id, c := range channels {
		go func() {
			defer done.Done()
			select {
			case <-c:
			case <-stop:
				panic("benchmark stopped during warmup")
			}
			warmed.Done()
			var result broadcastBenchResult
			for i := 0; i < b.N; i++ {
				select {
				case v := <-c:
					result.sum += v
					if v != uint64(i) {
						result.bad = true
					}
				case <-stop:
					panic("benchmark stopped before delivery completed")
				}
			}
			results[id] = result
		}()
	}
	for _, c := range channels {
		c <- 0
	}
	warmed.Wait()
	b.ReportAllocs()
	b.SetBytes(int64(n * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, c := range channels {
			c <- uint64(i)
		}
	}
	done.Wait()
	b.StopTimer()
	close(stop)
	benchmarkCheckResults(b, results)
}

// BenchmarkRingPubSub_InlineCallbacks is a sequential dispatch floor: every callback runs
// synchronously inside the publisher goroutine. It has neither independent
// receiver goroutines nor native per-callback select. It is intentionally kept
// separate from BenchmarkRingPubSub_Broadcast because those are material semantic changes.
func BenchmarkRingPubSub_InlineCallbacks(b *testing.B) {
	for _, n := range []int{1, 8, 64, 512} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			results := make([]broadcastBenchResult, n)
			expected := make([]uint64, n)
			callbacks := make([]func(uint64), n)
			for id := range callbacks {
				callbacks[id] = func(v uint64) {
					results[id].sum += v
					if v != expected[id] {
						results[id].bad = true
					}
					expected[id]++
				}
			}
			b.ReportAllocs()
			b.SetBytes(int64(n * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, callback := range callbacks {
					callback(uint64(i))
				}
			}
			b.StopTimer()
			benchmarkCheckResults(b, results)
		})
	}
}

// BenchmarkRingPubSub_SubscriptionChurn isolates membership lifetimes with no backlog.
// FreshChannel is a deliberately optimistic lower bound for a design creating a
// channel per lifetime: it omits registry management and any helper goroutine.
// It is not a claim that all channel-based designs must allocate on each reuse.
func BenchmarkRingPubSub_SubscriptionChurn(b *testing.B) {
	b.Run("ChanCaster", func(b *testing.B) {
		x := NewChanCaster(make(chan uint64))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			x.Add(1)
			x.Add(-1)
		}
	})
	b.Run("ChanPubSub", func(b *testing.B) {
		x := NewChanPubSub(make(chan uint64))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			x.Subscribe()
			x.Unsubscribe()
		}
	})
	b.Run("RingPubSub", func(b *testing.B) {
		x := NewRingPubSub[uint64](64)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s := x.Subscribe()
			s.Close()
		}
	})
	for _, capacity := range []int{0, 64} {
		b.Run(fmt.Sprintf("FreshChannel/B=%d", capacity), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c := make(chan uint64, capacity)
				close(c)
			}
		})
	}
}

// BenchmarkRingPubSub_RingQueuedLifecycle includes Subscribe, filling B messages, and
// abandoning all B through Close. Thus ns/op is a complete queued lifecycle,
// not cancellation latency alone. No sleeping receiver or helper goroutine is
// needed to retire unread obligations.
func BenchmarkRingPubSub_RingQueuedLifecycle(b *testing.B) {
	for _, capacity := range []int{1, 64, 1024} {
		b.Run(fmt.Sprintf("B=%d", capacity), func(b *testing.B) {
			x := NewRingPubSub[uint64](capacity)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s := x.Subscribe()
				for j := 0; j < capacity; j++ {
					if got, err := x.TrySend(uint64(j)); err != nil || got != 1 {
						b.Fatalf("sent to %d subscribers, error %v; want 1", got, err)
					}
				}
				s.Close()
			}
			b.StopTimer()
			b.ReportMetric(float64(capacity), "queued/op")
		})
	}
}

var (
	benchmarkConstructedCaster   *ChanCaster[chan uint64, uint64]
	benchmarkConstructedPubSub   *ChanPubSub[chan uint64, uint64]
	benchmarkConstructedRing     *RingPubSub[uint64]
	benchmarkConstructedChannels []chan uint64
)

// BenchmarkRingPubSub_Construction measures allocation at construction, separately from
// steady-state operation. Global sinks deliberately model an escaping hub.
// The originals include construction of their externally supplied channel.
// LinearChannels includes a slice and one channel per subscriber; RingPubSub
// includes only shared infrastructure, not caller-owned cursor/goroutine storage.
func BenchmarkRingPubSub_Construction(b *testing.B) {
	b.Run("ChanCaster", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkConstructedCaster = NewChanCaster(make(chan uint64))
		}
	})
	b.Run("ChanPubSub", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkConstructedPubSub = NewChanPubSub(make(chan uint64))
		}
	})
	for _, capacity := range []int{1, 64, 1024} {
		b.Run(fmt.Sprintf("RingPubSub/B=%d", capacity), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				benchmarkConstructedRing = NewRingPubSub[uint64](capacity)
			}
		})
	}
	for _, n := range []int{1, 64, 512} {
		for _, capacity := range []int{0, 64} {
			b.Run(fmt.Sprintf("LinearChannels/B=%d/N=%d", capacity, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					channels := make([]chan uint64, n)
					for j := range channels {
						channels[j] = make(chan uint64, capacity)
					}
					benchmarkConstructedChannels = channels
				}
			})
		}
	}
}
