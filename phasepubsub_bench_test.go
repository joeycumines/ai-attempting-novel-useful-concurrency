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
	"sync/atomic"
	"testing"
)

// All implementations use this same adapter, warmup, goroutine lifecycle and
// value validation. Setup, registration, and teardown are outside the timer.
// LinearChannels is a static per-subscriber-channel reference, not a claim to
// reproduce an unseen callback implementation or its churn behavior.
type broadcastBench[V any] struct {
	send        func(V) int
	receive     []func() V
	unsubscribe []func()
}

func makeBroadcastBench[V any](kind string, n int, done <-chan struct{}) broadcastBench[V] {
	x := broadcastBench[V]{receive: make([]func() V, n), unsubscribe: make([]func(), n)}
	switch kind {
	case "ChanPubSub":
		p := NewChanPubSub(make(chan V))
		x.send = p.Send
		c := p.C()
		for i := range n {
			p.Subscribe()
			x.receive[i] = func() V { v := <-c; p.Wait(); return v }
			if done != nil {
				x.receive[i] = func() V {
					select {
					case v := <-c:
						p.Wait()
						return v
					case <-done:
						panic("unexpected benchmark cancellation")
					}
				}
			}
			x.unsubscribe[i] = p.Unsubscribe
		}
	case "PhasePubSub":
		p := NewPhasePubSub[V]()
		x.send = p.Send
		for i := range n {
			s := p.Subscribe()
			x.receive[i] = func() V { <-s.C(); return s.Read() }
			if done != nil {
				x.receive[i] = func() V {
					select {
					case <-s.C():
						return s.Read()
					case <-done:
						panic("unexpected benchmark cancellation")
					}
				}
			}
			x.unsubscribe[i] = s.Unsubscribe
		}
	case "LinearChannels":
		cs := make([]chan V, n)
		for i := range n {
			c := make(chan V)
			cs[i] = c
			x.receive[i] = func() V { return <-c }
			if done != nil {
				x.receive[i] = func() V {
					select {
					case v := <-c:
						return v
					case <-done:
						panic("unexpected benchmark cancellation")
					}
				}
			}
			x.unsubscribe[i] = func() {}
		}
		x.send = func(v V) int {
			for _, c := range cs {
				c <- v
			}
			return n
		}
	default:
		var width int
		if _, err := fmt.Sscanf(kind, "RelayW%d", &width); err != nil {
			panic(err)
		}
		p := NewRelayPubSub[V](width)
		x.send = p.Send
		for i := range n {
			s := p.Subscribe()
			x.receive[i] = func() V { <-s.C(); return s.Read() }
			if done != nil {
				x.receive[i] = func() V {
					select {
					case <-s.C():
						return s.Read()
					case <-done:
						panic("unexpected benchmark cancellation")
					}
				}
			}
			x.unsubscribe[i] = s.Unsubscribe
		}
	}
	return x
}

func runBroadcastBench[V any](b *testing.B, kind string, n int, useSelect bool, value func(uint64) V, sequence func(V) uint64) {
	const warmup = 32
	var done <-chan struct{}
	if useSelect {
		done = make(chan struct{})
	}
	x := makeBroadcastBench[V](kind, n, done)
	var wg, started, copied sync.WaitGroup
	var bad atomic.Int64
	cleanup := make(chan struct{})
	wg.Add(n)
	started.Add(n)
	copied.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			defer x.unsubscribe[i]()
			started.Done()
			for j := range b.N + warmup {
				if sequence(x.receive[i]()) != uint64(j) {
					bad.Add(1)
				}
			}
			copied.Done()
			<-cleanup
		}()
	}
	started.Wait()
	for i := range warmup {
		if got := x.send(value(uint64(i))); got != n {
			b.Fatalf("warmup send count %d, want %d", got, n)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if got := x.send(value(uint64(i + warmup))); got != n {
			b.Fatalf("send count %d, want %d", got, n)
		}
	}
	// PhasePubSub returns after notification handoffs, before copies finish.
	// Drain the final copies for every variant before stopping the timer, but
	// keep unsubscribe/goroutine teardown outside it. This is one final join,
	// not an extra per-publication barrier.
	copied.Wait()
	b.StopTimer()
	close(cleanup)
	wg.Wait()
	if got := bad.Load(); got != 0 {
		b.Fatalf("%d incorrect deliveries", got)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/delivery")
}

func BenchmarkPhasePubSub_Broadcast(b *testing.B) {
	for _, n := range []int{1, 8, 64, 512, 4096} {
		b.Run(fmt.Sprintf("N%d", n), func(b *testing.B) {
			for _, kind := range []string{"ChanPubSub", "PhasePubSub", "LinearChannels", "RelayW1", "RelayW4", "RelayW16", "RelayW64"} {
				b.Run(kind, func(b *testing.B) {
					runBroadcastBench(b, kind, n, false, func(i uint64) uint64 { return i }, func(i uint64) uint64 { return i })
				})
			}
		})
	}
}

// Every receiver selects between its subscription and the same never-canceled
// shutdown channel, as with a shared context.Done in an application.
func BenchmarkPhasePubSub_Select(b *testing.B) {
	for _, n := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("N%d", n), func(b *testing.B) {
			for _, kind := range []string{"ChanPubSub", "PhasePubSub", "LinearChannels", "RelayW1", "RelayW4", "RelayW16", "RelayW64"} {
				b.Run(kind, func(b *testing.B) {
					runBroadcastBench(b, kind, n, true, func(i uint64) uint64 { return i }, func(i uint64) uint64 { return i })
				})
			}
		})
	}
}

type benchPayload struct {
	Sequence uint64
	Padding  [1016]byte
}

func BenchmarkPhasePubSub_Payload1KiB(b *testing.B) {
	for _, n := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("N%d", n), func(b *testing.B) {
			for _, kind := range []string{"ChanPubSub", "PhasePubSub", "LinearChannels", "RelayW1", "RelayW4", "RelayW16", "RelayW64"} {
				b.Run(kind, func(b *testing.B) {
					runBroadcastBench(b, kind, n, false,
						func(i uint64) benchPayload { return benchPayload{Sequence: i} },
						func(v benchPayload) uint64 { return v.Sequence })
				})
			}
		})
	}
}

var idleChurnSink chan uint64

var (
	constructorPhaseSink *PhasePubSub[uint64]
	constructorRelaySink *RelayPubSub[uint64]
	constructorChanSink  *ChanPubSub[chan uint64, uint64]
)

func BenchmarkPhasePubSub_Construct(b *testing.B) {
	b.Run("ChanPubSub", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			constructorChanSink = NewChanPubSub(make(chan uint64))
		}
	})
	b.Run("PhasePubSub", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			constructorPhaseSink = NewPhasePubSub[uint64]()
		}
	})
	b.Run("RelayPubSub", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			constructorRelaySink = NewRelayPubSub[uint64](16)
		}
	})
}

func BenchmarkPhasePubSub_IdleChurn(b *testing.B) {
	b.Run("ChanPubSub", func(b *testing.B) {
		p := NewChanPubSub(make(chan uint64))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			p.Subscribe()
			p.Unsubscribe()
		}
	})
	b.Run("PhasePubSub", func(b *testing.B) {
		p := NewPhasePubSub[uint64]()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			s := p.Subscribe()
			s.Unsubscribe()
		}
	})
	b.Run("RelayPubSub", func(b *testing.B) {
		p := NewRelayPubSub[uint64](4)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			s := p.Subscribe()
			s.Unsubscribe()
		}
	})
	// This measures the channel allocation for fresh subscribers only. It is
	// deliberately not a registry-removal benchmark or a pooled alternative.
	b.Run("FreshChannel", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			idleChurnSink = make(chan uint64)
		}
	})
}

