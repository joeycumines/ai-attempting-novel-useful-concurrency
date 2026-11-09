# Lossy broadcast ring: design, verification and benchmarks

Package `bigbuff` · module `github.com/joeycumines/go-bigbuff` (assumed) · Go 1.24 · benchmarks run on a 2-vCPU Linux VM (Intel Xeon 2.1 GHz)

## Summary

I built `Ring[V]`, a multi-producer, lossy broadcast ring for small pointer-free values, with independent cursor readers. It is correct under every check I ran, and it wins clearly on fan-out. Publish cost stays between about 240 and 310 ns from 8 to 64 subscribers, while the buffered-channel baseline climbs to about 3.8 µs. It does not win everywhere: it is slower than a mutex for publish-only traffic and slower than an unbuffered channel for blocking handoff. Those losses are reported below, not hidden.

## Scenario

Many producers emit fixed-size telemetry records. Any number of cursors read the stream at their own pace. Publishers never wait on readers. A reader that falls more than the capacity behind is not blocked. It is told exactly how many tickets it missed, resumes from the oldest ticket still retained, and never sees a torn value.

Why not the standard primitives: a buffered-channel fan-out costs the publisher one send per subscriber and gives no count of what was dropped. A mutex-guarded ring serialises every producer and consumer on one lock. A `sync.Cond` broadcast wakes every waiter and cannot be selected on. `atomic.Value` or `atomic.Pointer` per publish allocates.

## Design

- **Tickets.** One fetch-add on a shared head counter gives each publish a dense ticket. That counter is the only coordination point between producers.
- **Seqlock slots.** Each slot's sequence word is `2t+1` while ticket `t` is being written and `2t+2` once it is committed. A reader keeps its copy only if the sequence word is unchanged on both sides of the copy. An overwrite is detected as a lap with no extra state, which gives an exact skip count.
- **Lock-free slot claim.** A writer claims its slot with a CAS from the committed state of the ticket one ring back. Laps of the same slot are therefore strictly ordered without a lock. A writer waits only while that earlier ticket has not committed, which requires a stalled publisher to have been overtaken by a full ring of later publishes.
- **Lane striping.** Tickets are mapped so that consecutive tickets fall on different 64-byte lanes, so concurrent producers claiming adjacent tickets do not share a cache line. Striping is skipped when an element is larger than a line.
- **Select-compatible blocking.** A cursor increments the waiter count, takes the current wake channel, and re-checks its slot before parking. A publisher that commits after the re-check must observe the waiter and close that channel. Publishers touch the wake lock only when someone is parked, and one close wakes every cursor parked on the same cycle. A new wake channel is allocated only when a new cycle starts.
- **Element constraints.** The element type must be pointer-free, at most 64 bytes, and at most 8-byte aligned. The constructor checks this with reflection and panics otherwise. Pointer-freedom is what makes the discarded torn copies invisible to the garbage collector.

## Verification

- `go vet` and `gofmt` are clean. The only change to the existing `chancaster.go` is gofmt comment alignment.
- `go test ./...` and `go test -race ./...` both pass. The concurrency tests also passed 10 repeated runs across GOMAXPROCS 1, 2 and 4 in the default build, and 4 repeated runs in the race build.
- **Invariants under concurrent producers:** per-producer ordering is preserved; tickets are dense and unique; every payload is self-checking, so a torn read fails; for each cursor, `delivered + skipped` equals the total ticket count; `Latest` never goes backwards; no waiters or goroutines remain after shutdown.
- **Mutation check.** In a scratch copy I removed the post-copy seqlock validation. The stress test then failed with corrupt payloads, so the torn-read check does detect corruption.
- **Allocations.** `Publish` and `Next` (with data already available) allocate nothing, per `testing.AllocsPerRun`.
- These are invariant tests, not a linearizability checker such as Porcupine.

## Benchmarks

One final full run, 1 second per benchmark. Latency is sampled from every 16th event, from publish timestamp to receipt.

|Scenario|Ring|Baseline|
|---|---|---|
|Serial publish, no readers|24.8 ns/op, 0 allocs|mutex ring 17.5 ns/op|
|Parallel publish, 2 producers|130.9 ns/op (7.6M msgs/s)|mutex ring 25.1 ns/op (39.8M msgs/s)|
|Publish + consume, one goroutine|31.7 ns/op|buffered channel 42.6 ns/op|
|Ping-pong round trip (blocking)|763 ns/op, 1 alloc (223 B)|unbuffered channel 397 ns/op|
|Fan-out, 1 subscriber|148 ns/op, 4.9M deliveries/s, p99 112 µs|chan-drop 140 ns/op, 4.1M/s, p99 2.47 ms|
|Fan-out, 8 subscribers|311 ns/op, 20.9M deliveries/s, p99 170 µs|chan-drop 1023 ns/op, 7.0M/s, p99 945 µs|
|Fan-out, 64 subscribers|242 ns/op, 62.0M deliveries/s, p99 345 µs|chan-drop 3770 ns/op, 15.6M/s, p99 3.67 ms|

**How to read these.**

- **The fan-out win is real and grows with width.** Publish cost barely moves from 8 to 64 subscribers. The channel baseline pays one send per subscriber, so its cost scales with width. Delivered throughput is about 3–4× higher at 8 and 64 subscribers. At one subscriber the two are roughly even.
- **Loss percentages are not a fair comparison.** The ring publishes faster, so its cursors are lapped more: 77% loss at 64 subscribers, against 8% for the channel baseline. The channel's loss at one subscriber also moved from 27% to 43% between runs on this VM.
- **Latency has the same confound.** Tail latency depends on publish rate, which differs by 3–15× between the two. I have not claimed a latency win.
- **Publish-only throughput loses to the mutex.** With two producers, the ring costs about 5× more per publish here. My explanation is a hypothesis, not a measured result: each publish moves two cache lines between cores (the shared head and the slot), while Go's mutex lets the current holder re-acquire and keep its lines local. Counting spins ruled out the claim loop as the cause: about 0.02 per publish. Striping shows no measurable gain in this setup (130.9 vs 132.4 ns).
- **Blocking handoff loses to channels** by about 1.9×. Each round trip parks and wakes both sides, and each park takes the wake lock and allocates when a new cycle starts.
- **A failed experiment.** Spinning briefly before parking made ping-pong slower (714 to 1293 ns), and spinning subscribers starved the publisher on 2 vCPUs. I dropped it.

## Limits

- Pointer-free elements only, at most 64 bytes. Use `ChanPubSub` or `ChanCaster` for pointerful payloads.
- The default build copies payloads with plain memory operations. The Go memory model does not sanction that for racing accesses. The protocol tolerates it because of post-copy validation. The `-race` build uses atomic word accesses instead, so the race detector can check the rest of the protocol.
- A publisher waits only when the ticket one ring back has not committed. That needs a publisher to stall through a full ring's worth of publishes.
- A cursor waiting on a ticket whose publisher stalls mid-write waits for that publisher.
- `Close` requires publishers to be quiesced first. A cursor is single-goroutine.
- Multi-producer publish is the weakest regime. Fixing it would need ticket batching, which I have not built.

**Not verified:** ARM64 and Apple silicon (I only ran on x86-64 Linux), any hardware other than this 2-vCPU VM, and a formal linearizability proof or checker.

---------

````txtar
-- ring_bench_test.go --
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
		r := NewRing[uint64](4096)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			r.Publish(uint64(i))
		}
	})
	b.Run("ring-uint64-unstriped", func(b *testing.B) {
		r := newRing[uint64](4096, false)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			r.Publish(uint64(i))
		}
	})
	b.Run("ring-64B-element", func(b *testing.B) {
		r := NewRing[ringWide](4096)
		v := newRingWide(1, 1)
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
		r := NewRing[uint64](4096)
		run(b, func(v uint64) { r.Publish(v) })
	})
	b.Run("ring-uint64-unstriped", func(b *testing.B) {
		r := newRing[uint64](4096, false)
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
		r := NewRing[uint64](1024)
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
		ab := NewRing[uint64](64)
		ba := NewRing[uint64](64)
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

// BenchmarkFanout measures one publisher feeding K cursors that each run flat out. Each
// cursor keeps a sampled latency record. Loss is the fraction of (ticket, cursor) pairs
// that were skipped or dropped. Both primitives use a 1024-entry per-cursor window, so the loss
// figures compare like with like. The chan baseline is the idiomatic lossy fan-out: a
// non-blocking send to each subscriber's buffered channel.
func BenchmarkFanout(b *testing.B) {
	for _, subs := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("ring/subs=%d", subs), func(b *testing.B) {
			benchRingFanout(b, subs, 1024)
		})
		b.Run(fmt.Sprintf("chan-drop/subs=%d", subs), func(b *testing.B) {
			benchChanFanout(b, subs, 1024)
		})
	}
}

func benchRingFanout(b *testing.B, subs, capacity int) {
	ctx := context.Background()
	r := NewRing[benchEvent](capacity)
	cursors := make([]*RingCursor[benchEvent], subs)
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

func benchChanFanout(b *testing.B, subs, capacity int) {
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
-- ring_payload_fast.go --
//go:build !race

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

// ringRaceEnabled reports whether the -race payload path is compiled in.
const ringRaceEnabled = false

// ringStore writes v into the slot payload with a plain copy. The seqlock in Ring.Publish
// brackets this write.
//
// Under the Go memory model a plain write racing with a plain read is a data race. The
// protocol tolerates it because a reader keeps a copy only after validating seq on both
// sides, and the element is pointer-free, so a discarded torn copy is invisible to the
// garbage collector. The -race build replaces this with atomic word accesses so the race
// detector can check the rest of the protocol. The algorithm is the same in both builds.
func ringStore[V any](sl *ringSlot[V], v V) {
	sl.val = v
}

// ringLoad returns a copy of the slot payload. The caller must validate it with the seqlock
// before using it.
func ringLoad[V any](sl *ringSlot[V]) V {
	return sl.val
}
-- ring_payload_race.go --
//go:build race

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
	"sync/atomic"
	"unsafe"
)

// ringRaceEnabled reports whether the -race payload path is compiled in.
const ringRaceEnabled = true

// ringStore writes v into the slot payload one 8-byte word at a time with atomic stores.
// Every access to the payload is then atomic, so the seqlock is data-race free under the
// memory model and the race detector can verify the rest of the protocol. The algorithm is
// the same as in the fast build.
//
// The payload starts at offset 8 in ringSlot and the slot size is a multiple of 8. The
// words written here therefore lie inside the slot, and the seq word is never touched.
func ringStore[V any](sl *ringSlot[V], v V) {
	var buf [ringMaxElemBytes / 8]uint64
	*(*V)(unsafe.Pointer(&buf)) = v
	n := (unsafe.Sizeof(v) + 7) / 8
	base := unsafe.Pointer(&sl.val)
	for i := uintptr(0); i < n; i++ {
		atomic.StoreUint64((*uint64)(unsafe.Add(base, i*8)), buf[i])
	}
}

// ringLoad returns a copy of the slot payload, read one 8-byte word at a time with atomic
// loads. The caller must validate it with the seqlock before using it.
func ringLoad[V any](sl *ringSlot[V]) V {
	var buf [ringMaxElemBytes / 8]uint64
	var zero V
	n := (unsafe.Sizeof(zero) + 7) / 8
	base := unsafe.Pointer(&sl.val)
	for i := uintptr(0); i < n; i++ {
		buf[i] = atomic.LoadUint64((*uint64)(unsafe.Add(base, i*8)))
	}
	return *(*V)(unsafe.Pointer(&buf))
}
-- ring_test.go --
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
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// ringMix is a splitmix64-style finaliser used to build self-checking payloads.
func ringMix(x, salt uint64) uint64 {
	x += salt * 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// ringEvent is a 16-byte self-checking payload. A torn copy almost certainly fails Check.
type ringEvent struct {
	Producer uint32
	Local    uint32
	Check    uint64
}

func newRingEvent(p, l uint32) ringEvent {
	return ringEvent{Producer: p, Local: l, Check: ringMix(uint64(p)<<32|uint64(l), 1)}
}

func (e ringEvent) decode() (uint32, uint32, bool) {
	return e.Producer, e.Local, e.Check == ringMix(uint64(e.Producer)<<32|uint64(e.Local), 1)
}

// ringWide is a 64-byte payload that spans eight words, so a torn copy has many more ways to
// go wrong than ringEvent. It is also too big for lane striping.
type ringWide struct {
	Producer uint32
	Local    uint32
	Words    [6]uint64
	Check    uint64
}

func newRingWide(p, l uint32) ringWide {
	k := uint64(p)<<32 | uint64(l)
	w := ringWide{Producer: p, Local: l, Check: ringMix(k, 99)}
	for i := range w.Words {
		w.Words[i] = ringMix(k, uint64(i))
	}
	return w
}

func (w ringWide) decode() (uint32, uint32, bool) {
	k := uint64(w.Producer)<<32 | uint64(w.Local)
	if w.Check != ringMix(k, 99) {
		return 0, 0, false
	}
	for i, word := range w.Words {
		if word != ringMix(k, uint64(i)) {
			return 0, 0, false
		}
	}
	return w.Producer, w.Local, true
}

func mustPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}

// waitUntil polls cond until it holds, failing the test after a generous deadline.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

func TestRingRejectsUnsupportedElements(t *testing.T) {
	type withString struct {
		A int
		B string
	}
	type withIface struct{ A any }
	cases := map[string]func(){
		"string":       func() { NewRing[string](4) },
		"pointer":      func() { NewRing[*int](4) },
		"slice":        func() { NewRing[[]int](4) },
		"map":          func() { NewRing[map[int]int](4) },
		"interface":    func() { NewRing[any](4) },
		"func":         func() { NewRing[func()](4) },
		"chan":         func() { NewRing[chan int](4) },
		"unsafe ptr":   func() { NewRing[unsafe.Pointer](4) },
		"nested ptr":   func() { NewRing[withString](4) },
		"iface field":  func() { NewRing[withIface](4) },
		"oversize":     func() { NewRing[[65]byte](4) },
		"zero cap":     func() { NewRing[uint64](0) },
		"negative cap": func() { NewRing[uint64](-1) },
		"huge cap":     func() { NewRing[uint64](ringMaxCapacity + 1) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) { mustPanic(t, fn) })
	}
}

func TestRingAcceptsSupportedElements(t *testing.T) {
	type padded struct {
		A uint8
		_ [3]byte
		B float32
		C [2]complex64
	}
	NewRing[uint8](1)
	NewRing[padded](3)
	NewRing[[8]uint64](5) // exactly 64 bytes
	NewRing[[7]byte](2)   // size not a multiple of 8
	NewRing[struct{}](2)  // zero-sized
	NewRing[ringWide](16)
}

func TestRingSequentialAndReplay(t *testing.T) {
	ctx := context.Background()
	r := NewRing[uint64](8)
	if r.Cap() != 8 {
		t.Fatalf("Cap() = %d, want 8", r.Cap())
	}
	for i := range 5 {
		seq, err := r.Publish(uint64(i))
		if err != nil || seq != uint64(i) {
			t.Fatalf("Publish %d = (%d, %v), want (%d, nil)", i, seq, err, i)
		}
	}
	c := r.SubscribeReplay(100) // capped at Cap and at the 5 published
	for i := range 5 {
		v, skipped, err := c.Next(ctx)
		if err != nil || skipped != 0 || v != uint64(i) {
			t.Fatalf("Next = (%d, %d, %v), want (%d, 0, nil)", v, skipped, err, i)
		}
	}

	// Caught up: a cancelled context surfaces immediately with no value.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := c.Next(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("caught-up Next err = %v, want context.Canceled", err)
	}

	// Subscribe starts at the next publish, so it must not replay history.
	fresh := r.Subscribe()
	if _, _, err := fresh.Next(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("fresh subscriber replayed history: err = %v", err)
	}
	if _, err := r.Publish(77); err != nil {
		t.Fatal(err)
	}
	if v, skipped, err := fresh.Next(ctx); err != nil || skipped != 0 || v != 77 {
		t.Fatalf("fresh Next = (%d, %d, %v), want (77, 0, nil)", v, skipped, err)
	}

	// Replay is bounded by the window.
	r2 := NewRing[uint64](4)
	for i := range 10 {
		r2.Publish(uint64(i))
	}
	c2 := r2.SubscribeReplay(3)
	for want := uint64(7); want < 10; want++ {
		v, skipped, err := c2.Next(ctx)
		if err != nil || skipped != 0 || v != want {
			t.Fatalf("replay Next = (%d, %d, %v), want (%d, 0, nil)", v, skipped, err, want)
		}
	}
}

func TestRingLappedCursorAccounting(t *testing.T) {
	ctx := context.Background()
	r := NewRing[uint64](8)
	c := r.Subscribe() // positioned at ticket 0
	const total = 100
	for i := range total {
		r.Publish(uint64(i))
	}
	// The window is the last 8 tickets, so the first readable ticket is 93 and 93 are skipped.
	want := uint64(total - 8 + 1)
	v, skipped, err := c.Next(ctx)
	if err != nil || v != want || skipped != want {
		t.Fatalf("first lapped Next = (%d, %d, %v), want (%d, %d, nil)", v, skipped, err, want, want)
	}
	for i := want + 1; i < total; i++ {
		v, skipped, err := c.Next(ctx)
		if err != nil || v != i || skipped != 0 {
			t.Fatalf("Next = (%d, %d, %v), want (%d, 0, nil)", v, skipped, err, i)
		}
	}
}

func TestRingCloseDrainsThenErrors(t *testing.T) {
	ctx := context.Background()
	r := NewRing[uint64](4)
	for i := range 3 {
		r.Publish(uint64(i))
	}
	c := r.SubscribeReplay(4)
	r.Close()
	r.Close() // idempotent
	if _, err := r.Publish(9); !errors.Is(err, ErrRingClosed) {
		t.Fatalf("Publish after Close err = %v, want ErrRingClosed", err)
	}
	for i := range 3 {
		v, skipped, err := c.Next(ctx)
		if err != nil || skipped != 0 || v != uint64(i) {
			t.Fatalf("drain Next = (%d, %d, %v), want (%d, 0, nil)", v, skipped, err, i)
		}
	}
	if _, _, err := c.Next(ctx); !errors.Is(err, ErrRingClosed) {
		t.Fatalf("after drain err = %v, want ErrRingClosed", err)
	}
}

func TestRingBlockedNextWakesOnPublish(t *testing.T) {
	r := NewRing[uint64](16)
	c := r.Subscribe()

	type result struct {
		v   uint64
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, _, err := c.Next(context.Background())
		done <- result{v, err}
	}()
	waitUntil(t, "cursor to park", func() bool { return r.waiters.Load() == 1 })
	if _, err := r.Publish(42); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.v != 42 {
			t.Fatalf("woken Next = (%d, %v), want (42, nil)", got.v, got.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("parked Next was not woken by Publish")
	}
	waitUntil(t, "waiters to drain", func() bool { return r.waiters.Load() == 0 })
}

func TestRingCloseWakesParkedCursors(t *testing.T) {
	r := NewRing[uint64](16)
	const cursors = 8
	errs := make(chan error, cursors)
	for range cursors {
		c := r.Subscribe()
		go func() {
			_, _, err := c.Next(context.Background())
			errs <- err
		}()
	}
	waitUntil(t, "all cursors to park", func() bool { return r.waiters.Load() == cursors })
	r.Close()
	for range cursors {
		select {
		case err := <-errs:
			if !errors.Is(err, ErrRingClosed) {
				t.Fatalf("parked Next err = %v, want ErrRingClosed", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Close did not wake every parked cursor")
		}
	}
}

func TestRingCancelledWaitsDoNotLeak(t *testing.T) {
	r := NewRing[uint64](8)
	c := r.Subscribe()
	before := runtime.NumGoroutine()
	for range 200 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, _, err := c.Next(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Next err = %v, want DeadlineExceeded", err)
		}
	}
	if w := r.waiters.Load(); w != 0 {
		t.Fatalf("waiters = %d after cancelled waits, want 0", w)
	}
	// The ring starts no goroutines of its own, so the count must return to its baseline.
	waitUntil(t, "goroutine count to return to baseline", func() bool {
		return runtime.NumGoroutine() <= before
	})
}

func TestRingLatest(t *testing.T) {
	r := NewRing[uint64](4)
	if _, _, ok := r.Latest(); ok {
		t.Fatal("Latest on an empty ring reported a value")
	}
	for i := range 10 {
		r.Publish(uint64(i))
	}
	v, tk, ok := r.Latest()
	if !ok || v != 9 || tk != 9 {
		t.Fatalf("Latest = (%d, %d, %v), want (9, 9, true)", v, tk, ok)
	}
}

func TestRingIndexIsBijectiveAndLaneStriped(t *testing.T) {
	for _, capacity := range []int{1, 2, 4, 8, 64, 1024} {
		for _, striped := range []bool{false, true} {
			r := newRing[uint64](capacity, striped)
			perLine := uint64(1) << r.slotBits
			lanes := r.n / perLine
			seen := make([]bool, r.n)
			for tk := range r.n {
				idx := r.index(tk)
				if idx >= r.n || seen[idx] {
					t.Fatalf("cap=%d striped=%v: index(%d)=%d is out of range or duplicated", capacity, striped, tk, idx)
				}
				seen[idx] = true
				if r.index(tk+r.n) != idx {
					t.Fatalf("cap=%d striped=%v: tickets %d and %d map to different slots", capacity, striped, tk, tk+r.n)
				}
				if striped && lanes >= 2 && (idx>>r.slotBits) == (r.index(tk+1)>>r.slotBits) && tk+1 < r.n {
					t.Fatalf("cap=%d: consecutive tickets %d and %d share a lane", capacity, tk, tk+1)
				}
			}
		}
	}
	// Elements too big for a line get no striping.
	if r := newRing[ringWide](64, true); r.slotBits != 0 {
		t.Fatalf("72-byte slot striped with %d slots per lane", 1<<r.slotBits)
	}
}

func TestRingPayloadRoundTrip(t *testing.T) {
	type shape struct {
		A uint8
		B int16
		C [3]uint32
		D float64
		E complex64
	}
	testRingRoundTrip(t, shape{1, -2, [3]uint32{3, 4, 5}, 6.5, 7 + 8i}, shape{A: 255, D: -1})
	testRingRoundTrip(t, [8]uint64{1, 2, 3, 4, 5, 6, 7, 8}, [8]uint64{})
	testRingRoundTrip(t, struct{ X [7]byte }{[7]byte{1, 2, 3, 4, 5, 6, 7}}, struct{ X [7]byte }{})
	testRingRoundTrip(t, uint8(200), uint8(1))
	testRingRoundTrip(t, struct{}{}, struct{}{})
	testRingRoundTrip(t, newRingWide(9, 9), newRingWide(1, 2))
}

func testRingRoundTrip[V comparable](t *testing.T, values ...V) {
	t.Helper()
	ctx := context.Background()
	r := NewRing[V](4)
	for _, v := range values {
		if _, err := r.Publish(v); err != nil {
			t.Fatal(err)
		}
	}
	c := r.SubscribeReplay(len(values))
	for i, want := range values {
		got, skipped, err := c.Next(ctx)
		if err != nil || skipped != 0 || got != want {
			t.Fatalf("%T value %d: got (%v, %d, %v), want (%v, 0, nil)", want, i, got, skipped, err, want)
		}
	}
}

func TestRingPublishIsAllocationFree(t *testing.T) {
	if ringRaceEnabled {
		t.Skip("the race detector instruments and allocates")
	}
	r := NewRing[ringEvent](64)
	ev := newRingEvent(1, 2)
	if allocs := testing.AllocsPerRun(10000, func() { r.Publish(ev) }); allocs != 0 {
		t.Fatalf("Publish allocated %v times per call, want 0", allocs)
	}
	// Next on a cursor with data available is also allocation-free.
	big := NewRing[ringEvent](1 << 14)
	for range 1 << 13 {
		big.Publish(ev)
	}
	c := big.SubscribeReplay(1 << 13)
	ctx := context.Background()
	if allocs := testing.AllocsPerRun(4000, func() {
		if _, _, err := c.Next(ctx); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("Next allocated %v times per call, want 0", allocs)
	}
}

// runRingStress runs several producers against a deliberately small ring with fast and slow
// cursors. It checks that:
//   - every payload is intact (no torn reads);
//   - each producer's values arrive in the order it published them;
//   - tickets are dense and unique across producers;
//   - for every cursor, delivered + skipped equals the total number of tickets;
//   - Latest never goes backwards.
func runRingStress[V any](t *testing.T, mk func(p, l uint32) V, decode func(V) (uint32, uint32, bool)) {
	t.Helper()
	producers := max(2, runtime.GOMAXPROCS(0))
	perProducer := 150_000
	switch {
	case ringRaceEnabled:
		perProducer = 15_000
	case testing.Short():
		perProducer = 5_000
	}
	total := producers * perProducer
	r := NewRing[V](256)

	const fast, slow = 2, 2
	cursors := make([]*RingCursor[V], fast+slow)
	for i := range cursors {
		cursors[i] = r.Subscribe() // all positioned at ticket 0, before any publish
	}

	type report struct {
		delivered, skipped, reordered, corrupt uint64
		err                                    error
	}
	reports := make([]report, len(cursors))
	var cwg sync.WaitGroup
	for i := range cursors {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			c := cursors[i]
			slowCursor := i >= fast
			last := make([]int64, producers)
			for p := range last {
				last[p] = -1
			}
			var rep report
			for {
				v, skipped, err := c.Next(context.Background())
				rep.skipped += skipped
				if err != nil {
					rep.err = err
					break
				}
				p, l, ok := decode(v)
				if !ok || int(p) >= producers {
					rep.corrupt++
					continue
				}
				if int64(l) <= last[p] {
					rep.reordered++
				}
				last[p] = int64(l)
				rep.delivered++
				if slowCursor && rep.delivered%256 == 0 {
					time.Sleep(50 * time.Microsecond) // fall behind so the ring laps us
				}
			}
			reports[i] = rep
		}()
	}

	stopWatch := make(chan struct{})
	var watchWG sync.WaitGroup
	watchWG.Add(1)
	go func() {
		defer watchWG.Done()
		var last uint64
		seenAny := false
		for {
			select {
			case <-stopWatch:
				return
			default:
			}
			v, tk, ok := r.Latest()
			if !ok {
				continue
			}
			if _, _, valid := decode(v); !valid {
				t.Errorf("Latest returned a corrupt value at ticket %d", tk)
			}
			if seenAny && tk < last {
				t.Errorf("Latest went backwards: %d after %d", tk, last)
			}
			last, seenAny = tk, true
		}
	}()

	seen := make([]atomic.Bool, total)
	var pwg sync.WaitGroup
	for p := range producers {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			lastTicket := int64(-1)
			for l := range perProducer {
				tk, err := r.Publish(mk(uint32(p), uint32(l)))
				if err != nil {
					t.Errorf("producer %d: Publish: %v", p, err)
					return
				}
				if int64(tk) <= lastTicket {
					t.Errorf("producer %d: ticket %d after %d", p, tk, lastTicket)
				}
				lastTicket = int64(tk)
				if tk >= uint64(total) || seen[tk].Swap(true) {
					t.Errorf("ticket %d duplicated or out of range", tk)
				}
			}
		}()
	}
	pwg.Wait()
	close(stopWatch)
	watchWG.Wait()
	r.Close()
	cwg.Wait()

	var totalSkipped uint64
	for i, rep := range reports {
		if !errors.Is(rep.err, ErrRingClosed) {
			t.Errorf("cursor %d: terminal err = %v, want ErrRingClosed", i, rep.err)
		}
		if rep.corrupt != 0 {
			t.Errorf("cursor %d: %d corrupt payloads (torn reads)", i, rep.corrupt)
		}
		if rep.reordered != 0 {
			t.Errorf("cursor %d: %d per-producer ordering violations", i, rep.reordered)
		}
		if got := rep.delivered + rep.skipped; got != uint64(total) {
			t.Errorf("cursor %d: delivered %d + skipped %d = %d, want %d",
				i, rep.delivered, rep.skipped, got, total)
		}
		totalSkipped += rep.skipped
		t.Logf("cursor %d (slow=%v): delivered %d, skipped %d", i, i >= fast, rep.delivered, rep.skipped)
	}
	if totalSkipped == 0 {
		t.Error("no cursor was ever lapped; the stress test did not exercise overwrite")
	}
	for tk := range seen {
		if !seen[tk].Load() {
			t.Fatalf("ticket %d was never issued", tk)
		}
	}
	if w := r.waiters.Load(); w != 0 {
		t.Fatalf("waiters = %d after shutdown, want 0", w)
	}
}

func TestRingStressSmallPayload(t *testing.T) {
	runRingStress(t, newRingEvent, ringEvent.decode)
}

func TestRingStressLargePayload(t *testing.T) {
	runRingStress(t, newRingWide, ringWide.decode)
}

// TestRingChurn mixes cancelled waits, cursor creation and teardown, and close while publishers run.
func TestRingChurn(t *testing.T) {
	r := NewRing[ringEvent](64)
	const publishers = 4
	perPublisher := 50_000
	if ringRaceEnabled {
		perPublisher = 5_000
	}

	var cwg sync.WaitGroup
	var badValues atomic.Uint64
	for range 8 {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for {
				c := r.Subscribe() // a fresh cursor each round, then discarded
				for range 4 {
					ctx, cancel := context.WithTimeout(context.Background(), 50*time.Microsecond)
					v, _, err := c.Next(ctx)
					cancel()
					if errors.Is(err, ErrRingClosed) {
						return
					}
					if err == nil {
						if _, _, ok := v.decode(); !ok {
							badValues.Add(1)
						}
					}
				}
			}
		}()
	}

	var pwg sync.WaitGroup
	for p := range publishers {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for l := range perPublisher {
				if _, err := r.Publish(newRingEvent(uint32(p), uint32(l))); err != nil {
					t.Errorf("Publish: %v", err)
					return
				}
			}
		}()
	}
	pwg.Wait()
	r.Close()
	cwg.Wait()

	if n := badValues.Load(); n != 0 {
		t.Fatalf("%d corrupt values observed under churn", n)
	}
	if w := r.waiters.Load(); w != 0 {
		t.Fatalf("waiters = %d after churn, want 0", w)
	}
}
-- ring.go --
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
	"errors"
	"fmt"
	"math/bits"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ErrRingClosed is returned by Ring.Publish after Close, and by RingCursor.Next once the
// ring is closed and the cursor has drained every ticket that was published before Close.
var ErrRingClosed = errors.New("bigbuff: ring: closed")

const (
	ringCacheLine    = 64
	ringMaxElemBytes = 64 // also sizes the fixed scratch buffer on the -race path
	ringMaxElemAlign = 8  // slot words are 8-byte aligned
	ringMaxCapacity  = 1 << 30
)

// Ring is a bounded, lossy, multi-producer broadcast ring of small pointer-free values.
//
// Scenario. Many producers emit fixed-size telemetry records (counters, gauges, latest
// state). Any number of cursors read the stream at their own pace. Publish is wait-free
// with respect to cursors: a stalled or slow reader never delays a writer or another reader.
// A reader that falls more than Cap tickets behind is not blocked. It is told exactly how
// many tickets it missed, resumes from the oldest ticket still retained, and never sees a
// torn value.
//
// Why not chan, sync.Mutex or sync.Map. A buffered-channel fan-out costs the publisher one
// locked send per subscriber and forces a per-subscriber drop decision with no count. A
// mutex-guarded ring serialises every producer and every consumer on one line. A
// sync.Cond broadcast wakes all waiters and cannot be selected on. atomic.Value or
// atomic.Pointer per publish allocates.
//
// Mechanics.
//   - One fetch-add on head assigns each publish a dense ticket t. The ticket is the only
//     coordination point between producers.
//   - Each slot is a seqlock. seq is 2t+1 while ticket t is being written and 2t+2 once it
//     is committed. A reader copies the payload and keeps the copy only if seq is unchanged
//     on both sides of the copy. A seq greater than the reader's expected value means the
//     slot was overwritten, which is detected with no extra state and reported as an exact
//     skip count.
//   - A writer for ticket t claims its slot by CAS from the committed state of ticket t-Cap.
//     Laps of the same slot are therefore strictly ordered and no lock is taken. A writer
//     waits only while ticket t-Cap has not committed, which requires a stalled publisher to
//     have been overtaken by a full ring of later publishes.
//   - Tickets are mapped to slots so that consecutive tickets fall on different 64-byte cache
//     lines (lane striping). Concurrent producers claiming adjacent tickets therefore do not
//     false-share a line. Striping is a no-op when an element does not fit a line.
//   - Blocking is select-compatible. A cursor increments waiters, obtains the current wake
//     channel, and re-checks its slot before parking. A publisher that commits after the
//     re-check must observe waiters > 0 and close that channel. Publish only touches the
//     mutex when someone is parked. A single close wakes every cursor parked on the cycle.
//     Parking allocates only when a new wake cycle starts.
//
// Constraints. V must be pointer-free, at most 64 bytes, and at most 8-byte aligned. These
// are checked at construction and the constructor panics otherwise. Pointer-freedom is what
// makes the seqlock's discarded torn copies invisible to the garbage collector. Use
// ChanPubSub or ChanCaster for pointerful payloads.
//
// Guarantees.
//   - Publish never waits for cursors.
//   - A cursor delivers tickets in increasing order. For each returned value, the skipped
//     count plus the number of values delivered equals the number of tickets passed over.
//   - Values are never torn. Tests verify this with self-checking payloads.
//   - Publish is allocation-free on the steady-state path.
//
// Limitations.
//   - A cursor waiting on a ticket whose publisher stalls mid-write waits for that publisher.
//   - Close is a quiescent shutdown. See Close.
//   - The non-race build copies payloads with plain loads and stores. The Go memory model
//     does not sanction that for racing accesses. The protocol tolerates it only because of
//     post-copy validation, as described in ring_payload_fast.go. The -race build uses atomic
//     word accesses so the race detector can check the rest of the protocol.
type Ring[V any] struct {
	// head is the ticket dispenser that every publisher hits, so it sits on its own line.
	head atomic.Uint64
	_    [ringCacheLine - 8]byte

	// closed and waiters are read on every Publish but written only on Close and when a
	// cursor parks. mu and wake are touched only on the blocking path.
	closed  atomic.Bool
	waiters atomic.Int64
	mu      sync.Mutex
	wake    chan struct{}
	_       [ringCacheLine]byte

	// Read-only after construction.
	slots    []ringSlot[V]
	n        uint64 // len(slots), a power of two
	laneMask uint64 // lanes-1
	slotMask uint64 // slots-per-lane minus 1
	laneBits uint
	slotBits uint
}

// ringSlot is one ring entry. seq is the seqlock word: 2t+1 while ticket t is being written
// into the slot, 2t+2 once it is committed, and 0 before the first write. Because seq is
// 8 bytes, val always starts at offset 8 and the slot size is a multiple of 8.
type ringSlot[V any] struct {
	seq atomic.Uint64
	val V
}

type ringRead uint8

const (
	ringOK      ringRead = iota // the slot holds the requested ticket and the copy is validated
	ringPending                 // the ticket is claimed but not yet committed, or not yet claimed
	ringLapped                  // a later ticket has overwritten the slot
)

// NewRing returns a Ring that retains the most recent capacity tickets, rounded up to a
// power of two. It panics if V is not a supported element type.
func NewRing[V any](capacity int) *Ring[V] {
	return newRing[V](capacity, true)
}

// newRing is NewRing with lane striping switchable. The unstriped form is the ablation
// baseline used by the benchmarks.
func newRing[V any](capacity int, striped bool) *Ring[V] {
	checkRingElem[V]()
	if capacity < 1 || capacity > ringMaxCapacity {
		panic(fmt.Sprintf("bigbuff: ring: capacity %d out of range [1, %d]", capacity, ringMaxCapacity))
	}
	n := uint64(1) << bits.Len(uint(capacity-1))

	// Slots per cache-line lane is the largest power of two that fits in a line. It stays at
	// 1 when striping is off or the slot is larger than a line.
	perLine := uint64(1)
	if stride := uint64(unsafe.Sizeof(ringSlot[V]{})); striped && stride <= ringCacheLine {
		perLine = uint64(1) << (bits.Len64(ringCacheLine/stride) - 1)
		if perLine > n {
			perLine = n
		}
	}
	slotBits := uint(bits.TrailingZeros64(perLine))
	laneBits := uint(bits.TrailingZeros64(n)) - slotBits
	return &Ring[V]{
		slots:    make([]ringSlot[V], n),
		n:        n,
		laneMask: (uint64(1) << laneBits) - 1,
		slotMask: perLine - 1,
		laneBits: laneBits,
		slotBits: slotBits,
	}
}

// Cap reports how many tickets the ring retains.
func (r *Ring[V]) Cap() int { return int(r.n) }

// Published reports how many tickets have been claimed. Some may still be in flight.
func (r *Ring[V]) Published() uint64 { return r.head.Load() }

// Publish appends v as the next ticket and returns its sequence number. Tickets are dense
// and start at 0. Publish never waits for cursors. It waits only while the previous occupant
// of its slot is still being written, which requires a full ring of later publishes to have
// overtaken a publisher that is stalled mid-write.
func (r *Ring[V]) Publish(v V) (uint64, error) {
	if r.closed.Load() {
		return 0, ErrRingClosed
	}
	t := r.head.Add(1) - 1
	sl := &r.slots[r.index(t)]
	var prev uint64 // a never-written slot holds 0, which is the committed state of "lap -1"
	if t >= r.n {
		prev = 2*(t-r.n) + 2
	}
	for !sl.seq.CompareAndSwap(prev, 2*t+1) {
		runtime.Gosched()
	}
	ringStore(sl, v)
	sl.seq.Store(2*t + 2)
	if r.waiters.Load() != 0 {
		r.wakeAll()
	}
	return t, nil
}

// Latest returns the newest committed value and its ticket without creating a cursor. ok is
// false when no ticket in the retained window is committed yet.
func (r *Ring[V]) Latest() (value V, ticket uint64, ok bool) {
	for {
		h := r.head.Load()
		retry := false
		for back := uint64(1); back <= h && back <= r.n && !retry; back++ {
			v, st := r.load(h - back)
			switch st {
			case ringOK:
				return v, h - back, true
			case ringLapped:
				retry = true // a newer ticket landed during the scan, so rescan from a fresh head
			}
		}
		if !retry {
			return value, 0, false
		}
	}
}

// Close stops Publish, which then returns ErrRingClosed, and wakes parked cursors. Cursors
// drain the tickets published before Close and then receive ErrRingClosed. Close is
// idempotent. Quiesce publishers before calling it. A Publish that races with Close can
// return nil while cursors have already observed the ring as closed and do not see its
// ticket.
func (r *Ring[V]) Close() {
	r.closed.Store(true)
	r.wakeAll()
}

// index maps a ticket to its slot. Tickets that differ in their low bits land on different
// lanes, so back-to-back claims by concurrent publishers do not share a cache line. With
// striping off, slotBits is 0 and this reduces to t mod n. Tickets t and t+n always map to
// the same slot.
func (r *Ring[V]) index(t uint64) uint64 {
	return (t&r.laneMask)<<r.slotBits | (t>>r.laneBits)&r.slotMask
}

// load reads ticket t under the seqlock protocol. It returns a copy only when the slot
// carries the same committed tag before and after the copy, so a torn copy is never returned.
func (r *Ring[V]) load(t uint64) (V, ringRead) {
	sl := &r.slots[r.index(t)]
	want := 2*t + 2
	for {
		got := sl.seq.Load()
		if got != want {
			var zero V
			if got > want {
				return zero, ringLapped
			}
			return zero, ringPending
		}
		v := ringLoad(sl)
		if sl.seq.Load() == want {
			return v, ringOK
		}
		// A writer started on this slot during the copy. Reclassify on the next pass.
	}
}

// wakeAll closes the current wake cycle, if anyone is parked on it. The next parker starts a
// new cycle, so one close wakes every cursor parked on the same cycle.
func (r *Ring[V]) wakeAll() {
	r.mu.Lock()
	ch := r.wake
	r.wake = nil
	r.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// parkChan returns the wake channel for the current cycle, creating it if needed. Callers
// must have incremented waiters first. Then any publisher that commits after the caller's
// re-check is guaranteed to observe the increment and close the channel returned here.
func (r *Ring[V]) parkChan() chan struct{} {
	r.mu.Lock()
	if r.wake == nil {
		r.wake = make(chan struct{})
	}
	ch := r.wake
	r.mu.Unlock()
	return ch
}

// RingCursor is a single-consumer position in a Ring. It is not safe for concurrent use, so
// give each consuming goroutine its own cursor. Cursors are independent. A slow cursor never
// delays a publisher or another cursor.
type RingCursor[V any] struct {
	ring    *Ring[V]
	next    uint64 // next ticket to deliver
	pending uint64 // tickets passed over since the last return from Next
}

// Subscribe returns a cursor positioned at the next ticket to be published.
func (r *Ring[V]) Subscribe() *RingCursor[V] {
	return &RingCursor[V]{ring: r, next: r.head.Load()}
}

// SubscribeReplay returns a cursor positioned at the most recent n tickets. n is capped at
// Cap and at the number of tickets published so far.
func (r *Ring[V]) SubscribeReplay(n int) *RingCursor[V] {
	h := r.head.Load()
	back := uint64(0)
	if n > 0 {
		back = uint64(n)
	}
	if back > r.n {
		back = r.n
	}
	if back > h {
		back = h
	}
	return &RingCursor[V]{ring: r, next: h - back}
}

// Next returns the next value in ticket order, blocking until it is published or ctx is
// done. The second return is the number of tickets this cursor passed over, because they
// were overwritten before it read them, since the previous return. The count is exact and is
// reported with the next return of any kind, including errors. Next returns ErrRingClosed
// once the ring is closed and the cursor has caught up. A nil ctx is treated as
// context.Background.
func (c *RingCursor[V]) Next(ctx context.Context) (V, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r := c.ring
	for {
		v, st := r.load(c.next)
		switch st {
		case ringOK:
			c.next++
			return v, c.take(), nil
		case ringLapped:
			c.fastForward()
			continue
		}

		// Pending. Announce first, then re-check. A publisher that commits after our re-check
		// must observe waiters > 0 and close the channel we are about to select on.
		r.waiters.Add(1)
		wake := r.parkChan()
		v, st = r.load(c.next)
		switch st {
		case ringOK:
			r.waiters.Add(-1)
			c.next++
			return v, c.take(), nil
		case ringLapped:
			r.waiters.Add(-1)
			c.fastForward()
			continue
		}
		// closed is read before head. Every Publish that returned before Close is therefore
		// visible to the head load below.
		if r.closed.Load() && c.next >= r.head.Load() {
			r.waiters.Add(-1)
			var zero V
			return zero, c.take(), ErrRingClosed
		}
		select {
		case <-wake:
			r.waiters.Add(-1)
		case <-ctx.Done():
			r.waiters.Add(-1)
			var zero V
			return zero, c.take(), ctx.Err()
		}
	}
}

// fastForward moves the cursor to the oldest ticket whose slot can still hold data, and
// records the tickets passed over. A lapped read means some ticket at least n ahead of
// c.next has been claimed, so the target is always strictly ahead of the cursor.
func (c *RingCursor[V]) fastForward() {
	h := c.ring.head.Load()
	target := uint64(0)
	if h >= c.ring.n {
		target = h - c.ring.n + 1
	}
	if target <= c.next {
		panic(`bigbuff: ring: cursor invariant violation`)
	}
	c.pending += target - c.next
	c.next = target
}

// take returns and clears the skipped-ticket count.
func (c *RingCursor[V]) take() uint64 {
	s := c.pending
	c.pending = 0
	return s
}

// checkRingElem panics if V cannot be stored in a ring slot.
func checkRingElem[V any]() {
	t := reflect.TypeFor[V]()
	if t.Size() > ringMaxElemBytes || t.Align() > ringMaxElemAlign || !ringPointerFree(t) {
		panic(fmt.Sprintf("bigbuff: ring: element type %v must be pointer-free, at most %d bytes, and at most %d-byte aligned",
			t, ringMaxElemBytes, ringMaxElemAlign))
	}
}

// ringPointerFree reports whether values of type t contain no GC-visible pointers.
func ringPointerFree(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array:
		return t.Len() == 0 || ringPointerFree(t.Elem())
	case reflect.Struct:
		for i := range t.NumField() {
			if !ringPointerFree(t.Field(i).Type) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

````
