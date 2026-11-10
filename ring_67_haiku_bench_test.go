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
	"context"
	"fmt"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// benchEvent is a 16-byte payload: a sequence number and a publish timestamp.
type benchEvent struct {
	Seq   uint64
	Stamp int64
}

// benchLatencySample is the stride at which subscribers record latency. Sampling keeps the
// measurement from dominating the hot path, and the stride is the same for every primitive.
const benchLatencySample = 16

// mutexRing is the idiomatic lock-based baseline: a fixed slice behind a sync.Mutex.
type mutexRing[V any] struct {
	mu   sync.Mutex
	buf  []V
	head uint64
	mask uint64
}

func newMutexRing[V any](capacity int) *mutexRing[V] {
	n := uint64(1) << bits.Len(uint(capacity-1))
	return &mutexRing[V]{buf: make([]V, n), mask: n - 1}
}

func (m *mutexRing[V]) Publish(v V) {
	m.mu.Lock()
	m.buf[m.head&m.mask] = v
	m.head++
	m.mu.Unlock()
}

// BenchmarkPublishSerial measures Publish cost with no subscribers. It covers the slot layout
// (striped and unstriped), a payload that is too large for a cache line, and the mutex
// baseline.
func BenchmarkPublishSerial(b *testing.B) {
	b.Run("ring-uint64-striped", func(b *testing.B) {
		r := NewRing67Haiku[uint64](4096)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			r.Publish(uint64(i))
		}
	})
	b.Run("ring-uint64-unstriped", func(b *testing.B) {
		r := newRing67Haiku[uint64](4096, false)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			r.Publish(uint64(i))
		}
	})
	b.Run("ring-64B-element", func(b *testing.B) {
		r := NewRing67Haiku[ringWide67Haiku](4096)
		v := newRingWide67Haiku(1, 1)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			r.Publish(v)
		}
	})
	b.Run("mutex-ring-uint64", func(b *testing.B) {
		m := newMutexRing[uint64](4096)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			m.Publish(uint64(i))
		}
	})
}

// BenchmarkPublishParallel measures multi-producer throughput with no subscribers. Each
// variant uses GOMAXPROCS producers. Striping is ablated because it targets exactly this
// case: adjacent claims from concurrent producers landing on one cache line.
func BenchmarkPublishParallel(b *testing.B) {
	run := func(b *testing.B, publish func(uint64)) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			var i uint64
			for pb.Next() {
				publish(i)
				i++
			}
		})
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/sec")
	}
	b.Run("ring-uint64-striped", func(b *testing.B) {
		r := NewRing67Haiku[uint64](4096)
		run(b, func(v uint64) { r.Publish(v) })
	})
	b.Run("ring-uint64-unstriped", func(b *testing.B) {
		r := newRing67Haiku[uint64](4096, false)
		run(b, func(v uint64) { r.Publish(v) })
	})
	b.Run("mutex-ring-uint64", func(b *testing.B) {
		m := newMutexRing[uint64](4096)
		run(b, func(v uint64) { m.Publish(v) })
	})
}

// BenchmarkUncontendedPubNext measures one publish plus one consume on a single goroutine,
// with no parking. This isolates the fixed per-message overhead of each primitive.
func BenchmarkUncontendedPubNext(b *testing.B) {
	ctx := context.Background()
	b.Run("ring", func(b *testing.B) {
		r := NewRing67Haiku[uint64](1024)
		c := r.Subscribe()
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			r.Publish(uint64(i))
			if _, _, err := c.Next(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("chan-buffered", func(b *testing.B) {
		ch := make(chan uint64, 1024)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			ch <- uint64(i)
			<-ch
		}
	})
}

// BenchmarkPingPong measures the blocking path. Two goroutines bounce a value back and forth,
// so every round trip parks and wakes both sides. This is where the wake-cycle channel and the
// wake mutex are exercised.
func BenchmarkPingPong(b *testing.B) {
	ctx := context.Background()
	b.Run("ring", func(b *testing.B) {
		n := b.N
		ab := NewRing67Haiku[uint64](64)
		ba := NewRing67Haiku[uint64](64)
		cb := ab.Subscribe()
		ca := ba.Subscribe()
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range n {
				v, _, err := cb.Next(ctx)
				if err != nil {
					b.Error(err)
					return
				}
				ba.Publish(v)
			}
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for i := range n {
			ab.Publish(uint64(i))
			v, _, err := ca.Next(ctx)
			if err != nil || v != uint64(i) {
				b.Fatalf("round trip %d: got (%d, %v)", i, v, err)
			}
		}
		b.StopTimer()
		<-done
	})
	b.Run("chan-unbuffered", func(b *testing.B) {
		n := b.N
		ab := make(chan uint64)
		ba := make(chan uint64)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for v := range ab {
				ba <- v
			}
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for i := range n {
			ab <- uint64(i)
			if v := <-ba; v != uint64(i) {
				b.Fatalf("round trip %d: got %d", i, v)
			}
		}
		b.StopTimer()
		close(ab)
		<-done
	})
}

// BenchmarkFanout67Haiku measures one publisher feeding K cursors that each run flat out. Each
// cursor keeps a sampled latency record. Loss is the fraction of (ticket, cursor) pairs
// that were skipped or dropped. Both primitives use a 1024-entry per-cursor window, so the loss
// figures compare like with like. The chan baseline is the idiomatic lossy fan-out: a
// non-blocking send to each subscriber's buffered channel.
func BenchmarkFanout67Haiku(b *testing.B) {
	for _, subs := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("ring/subs=%d", subs), func(b *testing.B) {
			benchRingFanout67Haiku(b, subs, 1024)
		})
		b.Run(fmt.Sprintf("chan-drop/subs=%d", subs), func(b *testing.B) {
			benchChanFanout67Haiku(b, subs, 1024)
		})
	}
}

func benchRingFanout67Haiku(b *testing.B, subs, capacity int) {
	ctx := context.Background()
	r := NewRing67Haiku[benchEvent](capacity)
	cursors := make([]*RingCursor67Haiku[benchEvent], subs)
	for i := range cursors {
		cursors[i] = r.Subscribe()
	}
	samples := make([][]int64, subs)
	for i := range samples {
		samples[i] = make([]int64, 0, b.N/benchLatencySample+16)
	}
	var delivered, skipped atomic.Uint64
	var wg sync.WaitGroup
	for i := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := cursors[i]
			local := samples[i]
			var d, sk uint64
			for {
				ev, s, err := c.Next(ctx)
				sk += s
				if err != nil {
					break
				}
				d++
				if ev.Seq%benchLatencySample == 0 {
					local = append(local, time.Now().UnixNano()-ev.Stamp)
				}
			}
			samples[i] = local
			delivered.Add(d)
			skipped.Add(sk)
		}()
	}

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := range b.N {
		r.Publish(benchEvent{Seq: uint64(i), Stamp: time.Now().UnixNano()})
	}
	r.Close()
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()

	total := uint64(b.N) * uint64(subs)
	if got := delivered.Load() + skipped.Load(); got != total {
		b.Fatalf("accounting: delivered+skipped = %d, want %d", got, total)
	}
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msgs/sec")
	b.ReportMetric(float64(delivered.Load())/elapsed.Seconds(), "deliveries/sec")
	b.ReportMetric(100*float64(skipped.Load())/float64(total), "loss-pct")
	reportLatencyQuantiles(b, samples)
}

func benchChanFanout67Haiku(b *testing.B, subs, capacity int) {
	chans := make([]chan benchEvent, subs)
	for i := range chans {
		chans[i] = make(chan benchEvent, capacity)
	}
	samples := make([][]int64, subs)
	for i := range samples {
		samples[i] = make([]int64, 0, b.N/benchLatencySample+16)
	}
	var delivered atomic.Uint64
	var wg sync.WaitGroup
	for i := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := samples[i]
			var d uint64
			for ev := range chans[i] {
				d++
				if ev.Seq%benchLatencySample == 0 {
					local = append(local, time.Now().UnixNano()-ev.Stamp)
				}
			}
			samples[i] = local
			delivered.Add(d)
		}()
	}

	var dropped uint64 // written only by the publisher goroutine
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := range b.N {
		ev := benchEvent{Seq: uint64(i), Stamp: time.Now().UnixNano()}
		for _, ch := range chans {
			select {
			case ch <- ev:
			default:
				dropped++
			}
		}
	}
	for _, ch := range chans {
		close(ch)
	}
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()

	total := uint64(b.N) * uint64(subs)
	if got := delivered.Load() + dropped; got != total {
		b.Fatalf("accounting: delivered+dropped = %d, want %d", got, total)
	}
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msgs/sec")
	b.ReportMetric(float64(delivered.Load())/elapsed.Seconds(), "deliveries/sec")
	b.ReportMetric(100*float64(dropped)/float64(total), "loss-pct")
	reportLatencyQuantiles(b, samples)
}

// reportLatencyQuantiles merges the per-cursor samples and reports p50, p99 and p99.9. The
// sorting happens after the timer has stopped.
func reportLatencyQuantiles(b *testing.B, parts [][]int64) {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	if n == 0 {
		return
	}
	all := make([]int64, 0, n)
	for _, p := range parts {
		all = append(all, p...)
	}
	slices.Sort(all)
	at := func(q float64) float64 { return float64(all[int(q*float64(len(all)-1))]) }
	b.ReportMetric(at(0.50), "p50-ns")
	b.ReportMetric(at(0.99), "p99-ns")
	b.ReportMetric(at(0.999), "p999-ns")
}
