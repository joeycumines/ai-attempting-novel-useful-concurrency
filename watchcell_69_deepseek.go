// WatchCell69Deepseek: lossy, latest-value, single-writer broadcast cell with
// coalesced condvar wakeups and iter.Seq consumption.
//
// A single writer (or externally serialised writers via StoreSync) publishes
// via Store; any number of watchers consume the latest value via Watch (an
// iter.Seq[V]) or pull it via Load. Watchers are notified on every publish and
// slow watchers may skip intermediate values; the publisher never blocks on
// watchers. Close wakes all watchers and disables further stores.
//
// Epoch = seq/2 advances by one per completed Store. Under !race the value is
// guarded by a seqlock with a torn-read retry loop; under -race it is guarded
// by a RWMutex (see watchcell_69_deepseek_fast.go / watchcell_69_deepseek_race.go).

package bigbuff

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
)

// watchCellPad69Deepseek is the cache-line padding placed between hot-field groups. Using
// 128 (> cache line) guarantees the two sides of the pad land on distinct cache
// lines regardless of the base alignment malloc gives us.
const watchCellPad69Deepseek = 128

// watchCellSpinIterations69Deepseek bounds the tight atomic spin before we fall back to
// a futex block. ~50 iters ≈ 100–200 ns on modern x86/arm64: enough to catch
// "imminent" republishes without wasting CPU on long waits.
const watchCellSpinIterations69Deepseek = 50

// WatchCell69Deepseek is a lossy, latest-value, multi-subscriber broadcast primitive.
//
// A single writer (or externally serialised writers via StoreSync) publishes
// values via Store. Any number of watchers consume the latest value via Watch
// (an iter.Seq[V]) or pull it via Load. Watchers are notified on every publish;
// slow watchers may skip intermediate values. The publisher never blocks on
// watchers.
//
// Invariants and contracts:
//   - Constructed via NewWatchCell69Deepseek. Zero value panics on use.
//   - Store requires a single writer. Use StoreSync for concurrent writers.
//   - Close wakes all watchers and disables further Store/StoreSync.
//   - Watch is safe to call before any Store; it simply blocks.
//
// Correctness under -race: WatchCell69Deepseek swaps to a sync.RWMutex-protected value
// access path. Under !race it uses a seqlock with a torn-read retry loop. The
// two paths are selected by build tags in watchcell_69_deepseek_fast.go / watchcell_69_deepseek_race.go.
type WatchCell69Deepseek[V any] struct {
	// Hot path: seqlock sequence and value, kept adjacent so a reader's two
	// seq loads and the value read all hit one cache line for small V.
	seq atomic.Uint64
	val V

	_ [watchCellPad69Deepseek]byte

	// Notification: condvar for blocked waiters. notifyMu serialises broadcast
	// vs. the check-then-Wait critical section.
	notifyMu   sync.Mutex
	notifyCond *sync.Cond

	_ [watchCellPad69Deepseek]byte

	// waiters is incremented by each active Watch call. Read on every Store to
	// short-circuit Broadcast when nobody is listening; written only at Watch
	// entry/exit.
	waiters atomic.Int32

	_ [watchCellPad69Deepseek]byte

	// Lifecycle.
	closed    atomic.Bool
	closeOnce sync.Once

	_ [watchCellPad69Deepseek]byte

	// storeMu serialises StoreSync writers. Unused by Store (caller contract).
	storeMu sync.Mutex

	_ [watchCellPad69Deepseek]byte

	// valMu is used only under -race to fence val reads/writes. See
	// watchcell_race.go.
	valMu sync.RWMutex
}

// NewWatchCell69Deepseek constructs a WatchCell69Deepseek. Close must be called to release the
// condvar when the cell is no longer needed.
func NewWatchCell69Deepseek[V any]() *WatchCell69Deepseek[V] {
	x := &WatchCell69Deepseek[V]{}
	x.notifyCond = sync.NewCond(&x.notifyMu)
	return x
}

func (x *WatchCell69Deepseek[V]) checkInit() {
	if x.notifyCond == nil {
		panic(`bigbuff: watchcell: must use NewWatchCell69Deepseek`)
	}
}

// Store publishes v as the latest value. It requires a single concurrent
// writer; use StoreSync for concurrent writers. Non-blocking with respect to
// watchers. Zero allocations.
func (x *WatchCell69Deepseek[V]) Store(v V) {
	x.checkInit()
	if x.closed.Load() {
		return
	}
	x.storeValue(v)
	x.wake()
}

// StoreSync is Store serialised by an internal mutex, safe for concurrent
// writers. Slightly slower than Store but still alloc-free.
func (x *WatchCell69Deepseek[V]) StoreSync(v V) {
	x.checkInit()
	x.storeMu.Lock()
	defer x.storeMu.Unlock()
	if x.closed.Load() {
		return
	}
	x.storeValue(v)
	x.wake()
}

// wake notifies all currently blocked watchers. Called on every Store and on
// ctx cancellation.
func (x *WatchCell69Deepseek[V]) wake() {
	if x.waiters.Load() == 0 {
		return
	}
	x.notifyMu.Lock()
	x.notifyCond.Broadcast()
	x.notifyMu.Unlock()
}

// Load returns the latest published value and its epoch. The epoch advances by
// one per completed Store call. Zero allocations.
func (x *WatchCell69Deepseek[V]) Load() (V, uint64) {
	x.checkInit()
	return x.loadValue()
}

// Epoch returns the current published epoch without touching the value. Cheap
// and useful for change detection in tight polls. Zero allocations.
func (x *WatchCell69Deepseek[V]) Epoch() uint64 {
	return x.seq.Load() / 2
}

// Watchers returns the number of currently active Watch iterators. Useful for
// tests and monitoring.
func (x *WatchCell69Deepseek[V]) Watchers() int { return int(x.waiters.Load()) }

// Closed reports whether Close has been called.
func (x *WatchCell69Deepseek[V]) Closed() bool { return x.closed.Load() }

// Close disables further publishes and wakes all watchers so they can exit.
// Idempotent.
func (x *WatchCell69Deepseek[V]) Close() {
	x.checkInit()
	x.closeOnce.Do(func() {
		x.closed.Store(true)
		x.notifyMu.Lock()
		x.notifyCond.Broadcast()
		x.notifyMu.Unlock()
	})
}

// Watch returns a lossy iterator over published values. It yields the current
// value immediately (if any has ever been published) and then blocks until the
// next publish, ctx cancellation, or Close.
//
// The iterator yields only values it can observe; under heavy publication the
// iterator may skip intermediate values (this is the lossy contract — it
// always yields the latest value it observed on each wake).
func (x *WatchCell69Deepseek[V]) Watch(ctx context.Context) iter.Seq[V] {
	x.checkInit()
	return func(yield func(V) bool) {
		if yield == nil {
			panic(`bigbuff: watchcell: iterator yield func is nil`)
		}
		if ctx == nil {
			ctx = context.Background()
		}

		x.waiters.Add(1)
		defer x.waiters.Add(-1)

		// Registering AfterFunc is only worthwhile when the context has a Done
		// channel. Background/TODO skip this to avoid an allocation.
		if ctx.Done() != nil {
			stop := context.AfterFunc(ctx, x.wake)
			defer stop()
		}

		var lastEp uint64
		for {
			if x.closed.Load() {
				return
			}
			v, ep := x.loadValue()
			if ep != lastEp {
				lastEp = ep
				if !yield(v) {
					return
				}
			}
			if !x.waitForChange(lastEp, ctx) {
				return
			}
		}
	}
}

// waitForChange blocks until Epoch() advances past lastEp, the context is
// cancelled, or the cell is closed. Returns true if the caller should re-check
// the value (epoch advanced OR a spurious wake), false if the loop must exit.
func (x *WatchCell69Deepseek[V]) waitForChange(lastEp uint64, ctx context.Context) bool {
	// Fast path — already advanced.
	if x.Epoch() > lastEp {
		return true
	}
	if x.closed.Load() || ctx.Err() != nil {
		return false
	}

	// Tight spin: catches short publish intervals without a futex round-trip.
	for i := 0; i < watchCellSpinIterations69Deepseek; i++ {
		if x.Epoch() > lastEp {
			return true
		}
	}
	if x.closed.Load() || ctx.Err() != nil {
		return false
	}

	// Block on the condvar. Broadcast in wake() and Close() releases us; the
	// check-then-Wait is held under notifyMu to close the broadcast/wait race.
	x.notifyMu.Lock()
	for x.Epoch() <= lastEp && !x.closed.Load() {
		if ctx.Err() != nil {
			x.notifyMu.Unlock()
			return false
		}
		x.notifyCond.Wait()
	}
	x.notifyMu.Unlock()
	return !x.closed.Load()
}
