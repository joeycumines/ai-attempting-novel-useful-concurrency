// Pulse61Deepseek: wait-free latest-value broadcast.
//
// Pulse61Deepseek[T] is a conflated, MPMC latest-value broadcaster for workloads
// where publish cadence is low and read cadence is high (telemetry
// snapshots, config hot-reload, "current known" cache state).
//
// Semantics:
//   - Every Set atomically publishes a strictly-increasing version.
//   - Latest-wins: only the most recent value is retained.
//   - Snapshot reads the current value+version with wait-free semantics
//     (seqlock fast path is wait-free in the absence of a stalled writer).
//   - WaitFor blocks until version > since, or ctx terminates.
//   - Multiple waiters coalesce on a single Broadcast.
//   - Close wakes all waiters with ErrPulseClosed61Deepseek.
//
// Fast-path guarantees:
//   - Set: 0 allocations, lock-free CAS (mutex only when waking waiters).
//   - Snapshot: 0 allocations, no locks.
//   - WaitFor (version already advanced): 0 allocations, no locks.
//   - WaitFor (must block): 1 allocation per registration (context.AfterFunc).
//
// Not designed for:
//   - Ordered delivery of every publish (lossy).
//   - Backpressure (publishers never block).
//   - Cross-process / IPC durability.

package bigbuff

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

const pulsePad61Deepseek = 128 // generous SSE for both x86-64 (64B) and modern ARM64 (128B)

// ErrPulseClosed61Deepseek is returned once the Pulse61Deepseek has been closed.
var ErrPulseClosed61Deepseek = errors.New("bigbuff: pulse closed")

// Pulse61Deepseek is a wait-free latest-value broadcast primitive.
//
// Field layout is deliberate: each hot atomic group is separated by a
// full cacheLinePad to isolate coherency traffic. Set and Snapshot
// touch disjoint cache-line regions (except the seqlock line, which is
// the intended synchronisation point).
type Pulse61Deepseek[T any] struct {
	// --- Publish / version line ---
	_      [pulsePad61Deepseek]byte
	ver    atomic.Uint64 // monotonic version, increments on every Set
	active atomic.Int32  // index of the visible buffer slot (0 or 1)

	// --- Seqlock line (the only unavoidable line of shared cache traffic) ---
	_      [pulsePad61Deepseek]byte
	seq    atomic.Uint64 // seqlock: even = stable, odd = writer active
	wa     atomic.Int32  // waiter count; Set skips wake when 0
	closed atomic.Bool

	// --- Value storage (double-buffered, no per-Set allocation) ---
	_   [pulsePad61Deepseek]byte
	buf [2]T

	// --- Race-build-only value mutex (unused in !race builds) ---
	_     [pulsePad61Deepseek]byte
	valMu sync.RWMutex

	// --- Waiter parking (coalesced broadcast) ---
	_        [pulsePad61Deepseek]byte
	waitMu   sync.Mutex
	waitCond *sync.Cond
	_        [pulsePad61Deepseek]byte
}

// NewPulse61Deepseek constructs an initialised Pulse61Deepseek with version 0 and the zero
// value of T visible.
func NewPulse61Deepseek[T any]() *Pulse61Deepseek[T] {
	p := new(Pulse61Deepseek[T])
	p.waitCond = sync.NewCond(&p.waitMu)
	return p
}

// Set atomically publishes v. Returns ErrPulseClosed61Deepseek if Close has run.
//
// Fast path (no watchers): lock-free CAS on the seqlock, zero allocs.
func (p *Pulse61Deepseek[T]) Set(v T) error {
	if p.waitCond == nil {
		panic("bigbuff: pulse: use NewPulse61Deepseek")
	}
	if p.closed.Load() {
		return ErrPulseClosed61Deepseek
	}
	if err := p.snSet(v); err != nil {
		return err
	}
	// Waiter-aware wake: cost is zero when nobody is registered.
	if p.wa.Load() > 0 {
		p.broadcast()
	}
	return nil
}

// Snapshot reads the current value+version without blocking (except for
// an in-progress writer, which is bounded by writer progress). After Close
// it returns the retained final value and version rather than the zero T;
// only WaitFor treats a closed Pulse as an error.
func (p *Pulse61Deepseek[T]) Load() (T, uint64) {
	v, ver, _ := p.snapshot()
	return v, ver
}

// Version returns the last-completed version number.
func (p *Pulse61Deepseek[T]) Version() uint64 { return p.ver.Load() }

// Waiters returns the count of currently-registered waiters.
func (p *Pulse61Deepseek[T]) Waiters() int { return int(p.wa.Load()) }

// Close marks the Pulse61Deepseek terminal and wakes all registered waiters.
// Idempotent. Subsequent Set calls return ErrPulseClosed61Deepseek.
func (p *Pulse61Deepseek[T]) Close() error {
	if p.waitCond == nil {
		panic("bigbuff: pulse: use NewPulse61Deepseek")
	}
	if p.closed.Swap(true) {
		return nil
	}
	p.broadcast()
	return nil
}

func (p *Pulse61Deepseek[T]) broadcast() {
	p.waitMu.Lock()
	p.waitCond.Broadcast()
	p.waitMu.Unlock()
}

// WaitFor blocks until version > since, ctx terminates, or the Pulse61Deepseek
// is closed. Fast path (version already > since): zero allocations.
func (p *Pulse61Deepseek[T]) WaitFor(ctx context.Context, since uint64) (T, uint64, error) {
	if p.waitCond == nil {
		panic("bigbuff: pulse: use NewPulse61Deepseek")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		// Fast path: seqlock read + version check. No locks.
		v, ver, err := p.snapshot()
		if err != nil {
			return v, ver, err
		}
		if ver > since {
			return v, ver, nil
		}
		if e := ctx.Err(); e != nil {
			return v, ver, e
		}

		// Slow path: register + park. Once inside waitMu we re-check
		// under the coherence domain of a just-about-to-Wait goroutine.
		p.wa.Add(1)
		p.waitMu.Lock()

		v, ver, err = p.snapshot()
		if err != nil {
			p.waitMu.Unlock()
			p.wa.Add(-1)
			return v, ver, err
		}
		if ver > since {
			p.waitMu.Unlock()
			p.wa.Add(-1)
			return v, ver, nil
		}
		if e := ctx.Err(); e != nil {
			p.waitMu.Unlock()
			p.wa.Add(-1)
			return v, ver, e
		}

		// Arm cancellation through the same Broadcast primitive.
		var stop func() bool
		if ctx.Done() != nil {
			stop = context.AfterFunc(ctx, p.broadcast)
		}
		p.waitCond.Wait() // atomically: releases waitMu, parks; re-acquires on wake
		p.waitMu.Unlock()
		if stop != nil {
			stop()
		}
		p.wa.Add(-1)
	}
}

// WaitChan returns a channel that closes on the next version > since,
// or on ctx termination. One allocation per call — not on the hot path,
// but useful when composing with select{...}.
func (p *Pulse61Deepseek[T]) WaitChan(ctx context.Context, since uint64) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		_, _, _ = p.WaitFor(ctx, since)
	}()
	return ch
}

// WatchContext returns an iter.Seq-style callback loop that yields each
// new version. Suitable for `for v, ver := range p.WatchContext(ctx) {}`.
func (p *Pulse61Deepseek[T]) WatchContext(ctx context.Context) func(yield func(T, uint64) bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(T, uint64) bool) {
		var since uint64
		if v, ver, err := p.snapshot(); err == nil {
			if !yield(v, ver) {
				return
			}
			since = ver
		}
		for {
			v, ver, err := p.WaitFor(ctx, since)
			if err != nil {
				return
			}
			if !yield(v, ver) {
				return
			}
			since = ver
		}
	}
}
