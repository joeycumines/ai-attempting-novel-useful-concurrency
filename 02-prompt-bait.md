# Challenge: Apex-Performance & Novel Concurrency Primitives (`package bigbuff`)

## Objective
Your goal is to design and implement **genuinely novel, production-grade, and ruthlessly optimized** concurrent data structures or synchronization/coordination primitives in Go (`package bigbuff`). 

You have complete autonomy over **what** you choose to build and **how** you choose to win:
1. **Derive Your Own Target Scenario:** Define the exact operational envelope, contention profile, delivery semantics, and fitness-for-purpose criteria you are optimizing for.
2. **Choose Your Path:** You may invent an entirely new concurrency primitive from scratch, radicalize a theoretical algorithm for modern hardware, or take an existing archetype (e.g., broadcast/caster, pubsub, bounded/lossy ring, fanout/gather, epoch/watch state, or hybrid channel-orchestration) and push it to its absolute theoretical hardware limits.

**"Novelty" and "Maximum Fitness-for-Purpose Optimization" are dual, non-negotiable mandates.** A textbook lock-free ring buffer or a naive mutex-wrapped slice will score poorly. We are looking for implementations that dominate their chosen performance envelope—achieving the highest possible composite throughput, lowest tail latency, zero steady-state allocations, and pristine scalability under adversarial contention.

---

## The Optimization Mandate: Chasing the Hardware Ceiling

Go's runtime (`runtime.hchan`, `sync.Mutex`, and the work-stealing `g`/`m`/`p` scheduler) is already one of the most heavily optimized concurrency engines in existence. To justify a custom primitive—and to achieve a top-tier score—you must **out-specialize** the general-purpose runtime for your chosen scenario.

Think deeply about mechanical sympathy, memory hierarchy, and runtime overhead:
* **Cache-Coherency & Interconnect Traffic:** Eliminate false sharing via strict cache-line padding (64B/128B depending on target microarchitecture). Minimize MESI/MOESI invalidation storms on hot atomic counters via techniques like local batching, hierarchical/sharded state, seqlocks, or lazy cursor publication.
* **Memory Layout & GC Pressure:** Zero heap allocations (`0 B/op`, `0 allocs/op`) on the hot path is the baseline expectation. Consider pointer-free slab/arena layouts that bypass GC write barriers (`gcWriteBarrier`), value-packing into 64-bit/128-bit atomic words, and contiguous memory layouts that trigger hardware L1/L2 prefetcher streams.
* **Scheduler & Wakeup Sympathy:** Avoid thundering herds and `futex` / `sema` thrashing. When parking or waking goroutines (or OS threads), consider adaptive spinning, wakeup coalescing, handoff batching, or `select`-compatible zero-callback signaling (such as closing/swapping lightweight epoch channels vs. per-waiter signaling).

---

## Special Consideration: Low-Level Escape Hatches (`unsafe`, Assembly, Portable C)

To elicit exceptionally interesting, boundary-pushing solutions, **all low-level optimization avenues are explicitly permitted**—provided you can justify that they actually beat idiomatic/atomic Go (remembering that `cgo` and non-inlined calls carry their own boundary penalties!):

1. **`unsafe` & Runtime Tricks:** Full access to `unsafe.Pointer`, custom memory layout arithmetic, string/slice header manipulation, and `//go:nosplit` / compiler directives where valid.
2. **Go Plan 9 Assembly (`.s`) / SIMD:** You may provide architecture-specific assembly (e.g., `amd64` / `arm64` with a portable pure-Go fallback) to exploit instructions the Go compiler won't emit:
   * Hardware cache prefetching (`PREFETCHNTA` / `PRFM`) and non-temporal streaming stores.
   * Wide atomic operations (`CMPXCHG16B` / `CASP` 128-bit atomics) for ABA-immune pointer+epoch swaps.
   * SIMD vectorized slot scanning (AVX2/AVX-512/NEON) for multi-slot state discovery or batch copying.
   * Pause/yield hints (`PAUSE` / `YIELD` / `WFE`) for power- and pipeline-friendly spin loops.
3. **Portable C (C11 Atomics / Intrinsics):** You may include portable C implementations (via `cgo` or raw memory-mapped ring structures) **if and only if** your scenario amortizes or bypasses the `cgocall` overhead (e.g., coarse-grained vectorized batch operations, lock-free shared-memory IPC rings, or custom thread-pinned engines). If `cgo` hurts your latency, stay in pure Go or Go assembly!

---

## Inspiration: Orthogonal Scenario Spread (Do NOT Feel Constrained to These)

To illustrate the breadth of valid targets—without shoe-horning your design—consider how wildly different "maximum fitness for purpose" looks across distinct operational regimes:

* **Regime A: Ultra-High-Fanout Zero-Callback Broadcast (`1:N` / `M:N` State & Event Horizon)**
  * *The Bottleneck:* Waking 1,000+ `select`-waiting goroutines or distributing high-frequency events without $O(N)$ lock contention, callback inversion, or per-message heap allocations.
  * *Optimization Vector:* Read-mostly cache residency, seqlock/epoch-based optimistic reads, or hierarchical wakeup trees where slow consumers never stall the publisher's L1 cache line.
* **Regime B: Asymmetric Contention & Micro-Batching (`M:1` / `M:N` Burst Ingestion)**
  * *The Bottleneck:* Hundreds of concurrent producers hammering a single tail pointer (CAS retry storms) feeding a small pool of consumers that need amortized batch processing.
  * *Optimization Vector:* Elimination trees, flat-combining, per-P / sharded ingress lanes with wait-free opportunism, and zero-copy slice handoff.
* **Regime C: Lossy / Latest-Value / Bounded-History Telemetry (`Ring` / `Watch` / `Replay`)**
  * *The Bottleneck:* Guaranteed wait-free $O(1)$ publishing under extreme backpressure where consumers dynamically join, lag, catch up via replay, or skip to the latest state without tearing or blocking producers.
  * *Optimization Vector:* Overwrite-safe sequence locks, bit-packed slot metadata, ABA-free epoch counters, and branchless reader fast-paths.
* **Regime D: Deterministic / Bounded-Latency Work-Stealing or Rendezvous (`Scatter` / `Relay`)**
  * *The Bottleneck:* Minimizing P99.99 tail latency in request/task handoff across goroutines where standard channel locks cause cache-line ping-pong.
  * *Optimization Vector:* Direct register/slot handoff, symmetric rendezvous, or cache-aware slot probing.

*Again: these are merely points in a vast design space. Define your own target scenario and dominate it.*

---

## Deliverables & Rigor

1. **Scenario & Mechanical Sympathy Thesis:**
   * State the exact concurrency problem and workload profile your implementation targets.
   * Explain *why* standard Go primitives (`chan`, `sync.Mutex`, `sync.Map`, etc.) are suboptimal here, and detail the hardware/runtime mechanics your design exploits to win.
2. **Production-Complete Implementation (`package bigbuff`):**
   * Clean, self-contained, compilable code.
   * Graceful lifecycle management (cancellation, `Close()`, zero goroutine leaks, deterministic teardown).
   * Correctness under the Go Race Detector (`-race`). *(Note: If you use intentional lock-free seqlocks, raw `unsafe` memory, or `.s`/C code that trips ThreadSanitizer's happens-before heuristics, provide a `//go:build !race` ultra-fast path alongside a `-race`-compatible atomic fallback so automated verification passes cleanly).*
3. **Verification & Adversarial Benchmarks (`_test.go`):**
   * Concurrency stress tests proving linearizability, ordering/loss semantics, and leak-free shutdown under high churn.
   * `testing.B` benchmarks comparing your primitive against a idiomatic baseline across varying contention levels, payload sizes, and producer/consumer ratios (reporting `ns/op`, `B/op`, `allocs/op`, and custom throughput metrics like `msgs/sec`).

---
## IMPLEMENTATIONS/CONTEXT
---

context root: `/Users/joeyc/dev/go-bigbuff`

`````txtar
-- chancaster.go --
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
	"math"
	"sync"
	"sync/atomic"
)
type ChanCaster[C ~chan V, V any] struct {
	C C
	mutex sync.RWMutex
	state atomic.Uint64
}
func NewChanCaster[C ~chan V, V any](channel C) *ChanCaster[C, V] {
	if cap(channel) != 0 {
		panic("bigbuff: chancaster: must use an unbuffered channel")
	}
	return &ChanCaster[C, V]{C: channel}
}
func (x *ChanCaster[C, V]) Send(value V) int {
	if x.state.Load() == 0 {
		return 0 // no receivers (fast path)
	}
	x.mutex.Lock()
	defer x.mutex.Unlock()
	var (
		state              uint64
		receivers, tracker uint32
	)
	for {
		state = x.state.Load()
		if state == 0 {
			return 0 // no receivers (slow path)
		}

		receivers = uint32(state >> 32) // initialize from hi
		tracker = uint32(state)         // initialize from lo
		if tracker != receivers || receivers > math.MaxInt32 {
			panic(`bigbuff: chancaster: send: state invariant violation`)
		}
		tracker += math.MaxInt32
		if x.state.CompareAndSwap(state, uint64(receivers)<<32|uint64(tracker)) {
			break
		}
	}
	for range receivers {
		x.C <- value // may end up received by negative Add calls
	}
	state = x.state.Load()
	tracker = uint32(state >> 32) // actually the final receivers (used as scratch)
	if tracker > receivers ||     // receivers should be unchanged or decreased (and also lower than math.MaxInt32)
		uint32(state) != tracker+math.MaxInt32 || // lo still exactly math.MaxInt32 more than hi
		!x.state.CompareAndSwap(state, 0) { // failing this indicates one or more unregistered receivers
		panic(`bigbuff: chancaster: send: state invariant violation`)
	}
	return int(tracker)
}
func (x *ChanCaster[C, V]) Add(delta int) int {
	const maxReceivers = math.MaxInt32

	switch {
	case delta >= 0:
		var state uint64
		if delta == 0 {
			state = x.state.Load() // no change
		} else if delta > maxReceivers {
			panic(`bigbuff: chancaster: add: positive delta out of bounds`)
		} else {
			x.mutex.RLock()
			defer x.mutex.RUnlock()
			state = x.state.Add(uint64(delta)<<32 | uint64(uint32(delta)))
		}
		if receivers, tracker := uint32(state>>32), uint32(state); receivers <= maxReceivers &&
			receivers >= uint32(delta) &&
			(receivers == tracker || (delta == 0 && receivers+maxReceivers == tracker)) {
			return int(receivers)
		}

	case delta < -maxReceivers:
		panic(`bigbuff: chancaster: add: negative delta out of bounds`)

	default:
		delta = -delta
		state := x.state.Add(^(uint64(delta)<<32 | uint64(uint32(delta)) - 1))
		if receivers := uint32(state >> 32); receivers <= maxReceivers &&
			maxReceivers-receivers >= uint32(delta) {
			switch uint32(state) { // lo
			case receivers:
				return int(receivers) // note: already subtracted delta

			case maxReceivers + receivers:
				for range delta {
					<-x.C
				}
				return int(receivers) // note: already subtracted delta
			}
		}
	}
	panic(`bigbuff: chancaster: add: state invariant violation`)
}
-- chanpubsub.go --
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
	"iter"
	"math"
	"sync"
	"sync/atomic"
)

const chanPubSubStateInvariantViolation = `bigbuff: chanpubsub: state invariant violation`
type ChanPubSub[C ~chan V, V any] struct {
	ping ChanCaster[C, V]
	pongC *sync.Cond
	pongN int
	sendMu sync.Mutex
	sendingMu sync.RWMutex
	subscribers atomic.Int32
	broken chan struct{}
}
func NewChanPubSub[C ~chan V, V any](c C) *ChanPubSub[C, V] {
	if cap(c) != 0 {
		panic(`bigbuff: chanpubsub: must use an unbuffered channel`)
	}
	var x ChanPubSub[C, V]
	x.ping.C = c
	x.pongC = sync.NewCond(new(sync.Mutex))
	x.broken = make(chan struct{})
	return &x
}
func (x *ChanPubSub[C, V]) C() C {
	x.checkUsedFactoryFunction()
	x.checkBroken()
	return x.ping.C
}
func (x *ChanPubSub[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	x.Subscribe() // N.B. handles guarding on check* functions
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, x.Unsubscribe)

	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				x.Unsubscribe()
			}
			panic(`bigbuff: chanpubsub: iterator yield func is nil`)
		}

		if !stop() {
			return
		}
		defer x.Unsubscribe()

		for {
			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanPubSubStateInvariantViolation)
			default:
			}

			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanPubSubStateInvariantViolation)
			case v, ok := <-x.ping.C:
				if !ok {
					return
				}

				x.Wait()

				if !yield(v) {
					return
				}
			}
		}
	}
}
func (x *ChanPubSub[C, V]) Send(value V) (sent int) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	if x.subscribers.Load() == 0 {
		return 0 // no subscribers (fast path)
	}
	x.sendMu.Lock()
	defer x.sendMu.Unlock()
	x.sendingMu.Lock()
	var skipSendingUnlock bool
	defer func() {
		if !skipSendingUnlock {
			x.sendingMu.Unlock()
		}
	}()

	x.checkBroken() // again (attempt to mitigate deadlocks caused by borked state)
	subscribers := int(x.subscribers.Load())
	if subscribers == 0 {
		return 0 // no subscribers (slow path)
	}

	x.sanityCheckSubscribersDelta(subscribers, 0) // just because

	var success bool
	defer func() {
		if !success {
			x.markBroken() // example: closed channel caused panic
		}
	}()
	if x.ping.Add(subscribers) != subscribers {
		panic(chanPubSubStateInvariantViolation)
	}
	sent = x.ping.Send(value) // N.B. supports concurrent decrements

	skipSendingUnlock = true
	x.sendingMu.Unlock() // we can add subscribers while waiting for pongs
	if sent != 0 {
		x.pongC.L.Lock()
		defer x.pongC.L.Unlock()

		x.checkBroken() // AFTER lock (broken state is only broadcast once)

		x.pongN = sent
		x.pongC.Broadcast() // wake up any blocking Wait calls
		for x.pongN != 0 {
			x.pongC.Wait()
			x.checkBroken() // ALWAYS checkBroken after a wait
		}
	}

	success = true

	return
}
func (x *ChanPubSub[C, V]) Add(delta int) (subscribers int) {
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic(`bigbuff: chanpubsub: delta out of bounds`)
	}

	x.checkUsedFactoryFunction()
	x.checkBroken()

	switch {
	case delta == 0:
		subscribers = int(x.subscribers.Load())
		x.sanityCheckSubscribersDelta(subscribers, 0)

	case delta < 0:
		ok := x.sendingMu.TryRLock()
		for !ok && x.ping.Add(0) == 0 {
			x.checkBroken() // attempts to mitigate deadlock risk on misuse...
			ok = x.sendingMu.TryRLock()
		}
		subscribers = x.addSubscribers(delta)
		if ok {
			x.sendingMu.RUnlock() // unlock, before possible panics
		}
		x.sanityCheckSubscribersDelta(subscribers, delta)
		if !ok {
			var success bool
			defer func() {
				if !success {
					x.markBroken()
				}
			}()
			x.ping.Add(delta)
			success = true
		}

	default:
		x.sendingMu.RLock()
		defer x.sendingMu.RUnlock()
		subscribers = x.addSubscribers(delta)
		x.sanityCheckSubscribersDelta(subscribers, delta)
	}

	return
}
func (x *ChanPubSub[C, V]) Wait() {
	x.checkUsedFactoryFunction()

	x.pongC.L.Lock()
	defer x.pongC.L.Unlock()

	x.checkBroken() // AFTER lock (broken state is only broadcast once)
	for x.pongN == 0 {
		x.pongC.Wait()
		x.checkBroken() // ALWAYS checkBroken after a wait
	}

	x.pongN-- // consume a pong

	if x.pongN == 0 {
		x.pongC.Broadcast() // wake up Send call
	}
}
func (x *ChanPubSub[C, V]) Subscribe() {
	x.Add(1)
}
func (x *ChanPubSub[C, V]) Unsubscribe() {
	x.Add(-1)
}
func (x *ChanPubSub[C, V]) checkBroken() {
	select {
	case <-x.broken:
		panic(chanPubSubStateInvariantViolation)
	default:
	}
}

func (x *ChanPubSub[C, V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: chanpubsub: must use factory function`)
	}
}
func (x *ChanPubSub[C, V]) markBroken() {
	defer func() {
		recover() // we're going to panic shortly
	}()
	x.pongC.L.Lock() // avoid missing the broadcast
	defer x.pongC.L.Unlock()
	close(x.broken)
	x.pongC.Broadcast() // wake up any waiting goroutines (once)
}
func (x *ChanPubSub[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
	oldSubscribers := int(int32(subscribers) - int32(delta))
	if (delta > 0 && oldSubscribers >= subscribers) ||
		(delta < 0 && oldSubscribers <= subscribers) {
		x.markBroken()
		panic("bigbuff: chanpubsub: addition overflowed or underflowed")
	}
	if subscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative subscribers`)
	}
	if oldSubscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative old subscribers`)
	}
}
func (x *ChanPubSub[C, V]) addSubscribers(delta int) int {
	return int(x.subscribers.Add(int32(delta)))
}

`````
