# Lossy Broadcast Ring with Doorbell Parking: `bigbuff.Ring[V]`

I did not build on `ChanCaster` or `ChanPubSub`. Those are synchronous rendezvous primitives: the publisher blocks until every subscriber has received. The primitive below is asynchronous, bounded, and lossy, and it reports loss exactly.

## 1. Target scenario and contract

**Regime:** one hot publisher (market ticks, sensor frames, metric snapshots) with many consumers that join and leave at arbitrary times, run at very different speeds, and must never slow the publisher. Payloads are plain data (no pointers) of up to a few hundred bytes.

**Contract:**
- `Publish` is O(1) and independent of the number of consumers. It never waits for, copies for, or inspects any consumer except one atomic load.
- Each consumer owns a `Cursor` and reads without writing shared memory.
- Overwrites are detected per slot. Each read reports how many positions were skipped since the previous read, so for every cursor `delivered + dropped` equals the number of positions published after its start. Tests check this exactly.
- Consumers can block with `context`, `select`-compatible wakeups, and batch drains. A `Latest()` mode skips straight to the newest value.

## 2. Why Go's primitives are suboptimal here

- **Unbuffered `chan`:** rendezvous. The publisher is paced by the slowest receiver.
- **Buffered `chan` per subscriber:** the publisher takes N channel locks per message, drop-on-full is silent, loss cannot be counted, and `close` is one-shot.
- **`sync.Cond` / `Mutex`:** every publish takes a shared lock, the broadcast causes a mutex reacquisition herd, and nothing is `select`-compatible.
- **`atomic.Pointer[T]` / `atomic.Value`:** one heap allocation per publish, and no lag or replay semantics.

## 3. Mechanical design

**3.1 Word-granular seqlock, race-clean without build-tag forks.** Classic seqlocks copy the payload with plain loads. That is a data race in the Go memory model, so the race detector flags it and a second implementation is needed. Here the payload moves as `atomic.Load/StoreUint64` words. Every shared access is atomic, so one implementation is race-clean and `checkptr`-clean. A torn copy is detected by the stamp re-check and discarded. The arena is `[]uint64`, so the GC never scans it, and a reflection check at construction enforces the pointer-free precondition that makes word copying GC-safe.

**3.2 Lap-stamped slots and consumer-owned cursors.** A slot's sequence word is `2p+1` while position `p` is being written and `2p+2` once committed. Stamps only increase per slot, so one load tells a consumer whether its position is intact, not yet written, or overwritten by a later lap. There are no reader counters, no registration, and the publisher never waits.

**3.3 Doorbell parking.** Parked consumers share one channel, created lazily by the first parker. The publisher closes and clears it only when `waiters != 0`, so `Publish` costs one load when nobody is parked. Notification cost scales with wake epochs, not messages. Lost-wakeup freedom comes from a Dekker-style ordering over sequentially consistent atomics plus the park mutex (proof sketch in the code). Context cancellation unparks individually.

**3.4 Cache geometry.** The head counter sits alone on a 128-byte pad. Publisher-private state has its own line, and the control plane has another. The arena is 64-byte aligned and each slot occupies whole lines, so a writer never invalidates a line a reader is using for a neighbouring slot.

**3.5 Batch drain** (`Drain`) amortizes the head load across a batch and writes into caller-owned memory, so consumers allocate nothing.

## 4. Implementation: `ring.go`

```go
package bigbuff

import (
	"context"
	"errors"
	"math/bits"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ErrClosed is returned by Cursor.Next once the ring is closed and the cursor has
// consumed or skipped every entry committed before Close.
var ErrClosed = errors.New("bigbuff: ring: closed")

const (
	ringPadBytes    = 128     // two 64B lines: defeats adjacent-line prefetch pairing; equals an Apple M-series line
	ringSlotWords   = 8       // slots are rounded up to whole 64-byte lines
	ringMaxCapacity = 1 << 30 // keeps position arithmetic far from overflow
)

type ringPad [ringPadBytes]byte

// Ring is a fixed-capacity, lossy, single-publisher broadcast ring of plain-data values.
//
// Protocol (publisher P at position p, slot s = p & mask, N = capacity):
//
//	P:  q[s] = 2p+1; store payload words; q[s] = 2p+2; head = p+1; if waiters != 0 { wake }
//	C:  at position p with head snapshot h > p:
//	      if h-p > N            -> provably lapped: jump to h-N
//	      q[s] == 2p+2          -> copy payload words, re-load q[s]; unchanged => valid
//	      q[s] >  2p+2          -> lapped: jump to h'-N
//	Parking (Dekker over seq-cst atomics + park mutex):
//	  waiter: lock; waiters++; re-check head; install bell; unlock; select
//	  P:      head store; load waiters; if non-zero, lock; close bell; unlock
//	  Either P observes the waiter (and closes the bell it installed) or the
//	  waiter's head re-check observes the new head. No lost wakeups.
//
// Publish and Close are called by the producer. Cursors are consumer-owned and
// write no shared memory on the consume path.
type Ring[V any] struct {
	arena    []uint64 // 64B-aligned slot storage; noscan (V is plain data); never reallocated
	capacity uint64   // power of two
	mask     uint64
	stride   int // words per slot: 1 stamp + payload words, rounded to whole lines
	size     int // unsafe.Sizeof(V)

	_    ringPad
	head atomic.Uint64 // committed entries; the only word consumers poll

	_     ringPad
	tail  uint64        // next position to write; publisher-private, guarded by guard
	guard atomic.Uint32 // single-publisher exclusion

	_       ringPad
	closed  atomic.Bool
	waiters atomic.Int32 // parked consumers; read by every Publish, written only when parking
	mu      sync.Mutex   // slow path only: parking and waking
	bell    chan struct{}
}

// Cursor is a consumer's private read position in a Ring. A Cursor must not be used by
// more than one goroutine at a time. Distinct cursors on one ring are independent.
type Cursor[V any] struct {
	r   *Ring[V]
	pos uint64 // next position to read; consumer-owned
}

// NewRing returns a ring retaining the most recent `capacity` entries (rounded up to a
// power of two). It panics if V is not plain data: payloads move as atomic 64-bit words,
// which is only sound and GC-invisible for pointer-free types.
func NewRing[V any](capacity int) *Ring[V] {
	if capacity < 1 || capacity > ringMaxCapacity {
		panic(`bigbuff: ring: capacity out of range`)
	}
	if !plainData(reflect.TypeFor[V]()) {
		panic(`bigbuff: ring: element type must be plain data (no pointers, strings, slices, maps, chans, funcs or interfaces)`)
	}
	n := uint64(1) << bits.Len(uint(capacity-1))
	var zero V
	size := int(unsafe.Sizeof(zero))
	stride := (1 + (size+7)/8 + ringSlotWords - 1) / ringSlotWords * ringSlotWords
	total := int(n) * stride

	raw := make([]uint64, total+ringSlotWords)
	off := 0
	if rem := uintptr(unsafe.Pointer(&raw[0])) & 63; rem != 0 {
		off = int((64 - rem) >> 3) // raw is 8-aligned, so this lands on a 64B boundary
	}
	return &Ring[V]{
		arena:    raw[off : off+total : off+total],
		capacity: n,
		mask:     n - 1,
		stride:   stride,
		size:     size,
	}
}

// plainData reports whether values of t contain no pointers. Conservative: anything
// unrecognized is rejected.
func plainData(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array:
		return t.Len() == 0 || plainData(t.Elem())
	case reflect.Struct:
		for i := range t.NumField() {
			if !plainData(t.Field(i).Type) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// Cap returns the number of entries the ring retains.
func (r *Ring[V]) Cap() int { return int(r.capacity) }

// Publish commits v as the next entry, overwriting the oldest retained entry when full.
// It never waits for consumers. It returns false once the ring is closed.
//
// Publish has single-publisher semantics: overlapping calls from different goroutines
// violate the contract and panic. Consumers are unaffected by this check.
func (r *Ring[V]) Publish(v V) bool {
	if r.closed.Load() {
		return false
	}
	if !r.guard.CompareAndSwap(0, 1) {
		panic(`bigbuff: ring: concurrent Publish violates the single-publisher contract`)
	}
	pos := r.tail
	slot := r.slot(pos)
	atomic.StoreUint64(&slot[0], 2*pos+1) // in flight: odd stamp makes any reader copy fail validation
	storeWords(slot[1:], &v, r.size)
	atomic.StoreUint64(&slot[0], 2*pos+2) // committed: lap-stamped even value
	r.tail = pos + 1
	r.head.Store(pos + 1)
	r.guard.Store(0)
	if r.waiters.Load() != 0 {
		r.wake()
	}
	return true
}

// Close marks the ring closed, unparks every parked consumer, and makes subsequent Publish
// calls return false. Entries committed before Close remain readable. Close is idempotent.
func (r *Ring[V]) Close() {
	if !r.closed.Swap(true) {
		r.wake()
	}
}

// Subscribe returns a cursor positioned at the next entry published after this call.
func (r *Ring[V]) Subscribe() *Cursor[V] {
	return &Cursor[V]{r: r, pos: r.head.Load()}
}

// SubscribeReplay returns a cursor positioned at the oldest retained entry.
func (r *Ring[V]) SubscribeReplay() *Cursor[V] {
	return &Cursor[V]{r: r, pos: r.oldest(r.head.Load())}
}

// oldest is the first position guaranteed to be retained for head snapshot h. Positions
// >= h-N may still be overwritten by the in-flight write of h, which the stamp check catches.
func (r *Ring[V]) oldest(h uint64) uint64 {
	if h < r.capacity {
		return 0
	}
	return h - r.capacity
}

func (r *Ring[V]) slot(pos uint64) []uint64 {
	off := int(pos&r.mask) * r.stride
	return r.arena[off : off+r.stride]
}

// wake closes and clears the doorbell so every parked consumer resumes. Parkers install the
// bell under mu, so this cannot miss a waiter that is about to sleep.
func (r *Ring[V]) wake() {
	r.mu.Lock()
	if r.bell != nil {
		close(r.bell)
		r.bell = nil
	}
	r.mu.Unlock()
}

// Lag returns the number of published entries not yet consumed by this cursor. Entries
// that were already overwritten still count until a read reports them as dropped.
func (c *Cursor[V]) Lag() uint64 {
	return c.r.head.Load() - c.pos
}

// skipTo advances the cursor to target and returns the number of positions skipped.
func (c *Cursor[V]) skipTo(target uint64) uint64 {
	if target <= c.pos {
		target = c.pos + 1 // always make progress
	}
	skipped := target - c.pos
	c.pos = target
	return skipped
}

// step attempts to read the entry at c.pos given head snapshot h (c.pos < h). It returns
// ok=false when the caller must retry with a fresh snapshot: a torn copy, or a lap that
// already moved c.pos forward.
func (c *Cursor[V]) step(h uint64, out *V) (bool, uint64) {
	r := c.r
	var dropped uint64
	if h-c.pos > r.capacity { // slot for pos has been reused by pos+N or later: provably lost
		dropped += c.skipTo(r.oldest(h))
	}
	slot := r.slot(c.pos)
	want := 2*c.pos + 2
	switch s := atomic.LoadUint64(&slot[0]); {
	case s == want:
		var tmp V
		loadWords(slot[1:], &tmp, r.size)
		if atomic.LoadUint64(&slot[0]) != want {
			return false, dropped // a writer started reusing the slot during our copy
		}
		*out = tmp
		c.pos++
		return true, dropped
	case s > want: // lapped between our head snapshot and this load
		dropped += c.skipTo(r.oldest(r.head.Load()))
		return false, dropped
	default: // s < want cannot occur while c.pos < h: commit is ordered before the head publish
		runtime.Gosched()
		return false, dropped
	}
}

// TryNext returns the next entry without blocking. dropped counts positions skipped since
// the previous call (non-zero only when this cursor fell more than capacity entries behind).
// ok is false when nothing is pending; dropped may still be non-zero in that case.
func (c *Cursor[V]) TryNext() (v V, dropped uint64, ok bool) {
	for {
		h := c.r.head.Load()
		if c.pos >= h {
			return v, dropped, false
		}
		var d uint64
		ok, d = c.step(h, &v)
		dropped += d
		if ok {
			return v, dropped, true
		}
	}
}

// Next blocks until an entry is available, the ring is closed and drained (ErrClosed), or
// ctx is done. dropped reports positions skipped since the previous return, including on
// error returns, so the accounting never loses positions.
func (c *Cursor[V]) Next(ctx context.Context) (V, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var skipped uint64
	for {
		v, d, ok := c.TryNext()
		skipped += d
		if ok {
			return v, skipped, nil
		}
		if c.r.closed.Load() {
			// Observing closed means every pre-Close publish is visible; re-check once more.
			v, d, ok = c.TryNext()
			skipped += d
			if ok {
				return v, skipped, nil
			}
			var zero V
			return zero, skipped, ErrClosed
		}
		if err := c.park(ctx); err != nil {
			var zero V
			return zero, skipped, err
		}
	}
}

// park blocks until the ring may have news for this cursor, the ring closes, or ctx is done.
func (c *Cursor[V]) park(ctx context.Context) error {
	r := c.r
	r.mu.Lock()
	r.waiters.Add(1)
	if r.head.Load() > c.pos || r.closed.Load() {
		r.waiters.Add(-1)
		r.mu.Unlock()
		return nil
	}
	if r.bell == nil {
		r.bell = make(chan struct{})
	}
	bell := r.bell
	r.mu.Unlock()

	var err error
	select {
	case <-bell:
	case <-ctx.Done():
		err = ctx.Err()
	}
	r.waiters.Add(-1)
	return err
}

// Drain copies up to len(dst) pending entries into dst without blocking. It returns the
// number copied and the number of positions skipped. The head load is amortized over the batch.
func (c *Cursor[V]) Drain(dst []V) (n int, dropped uint64) {
	h := c.r.head.Load()
	var tmp V
	for n < len(dst) {
		if c.pos >= h {
			if h = c.r.head.Load(); c.pos >= h {
				break
			}
		}
		ok, d := c.step(h, &tmp)
		dropped += d
		if ok {
			dst[n] = tmp
			n++
		}
	}
	return n, dropped
}

// Latest skips to the newest committed entry and returns it. The skipped positions are
// reported in dropped. ok is false when nothing has been published since the last read.
func (c *Cursor[V]) Latest() (V, uint64, bool) {
	if h := c.r.head.Load(); c.pos < h {
		var skipped uint64
		if newest := h - 1; newest > c.pos {
			skipped = c.skipTo(newest)
		}
		v, d, ok := c.TryNext()
		return v, skipped + d, ok
	}
	var zero V
	return zero, 0, false
}

// storeWords copies the bytes of *v into dst as individually atomic 64-bit words. The
// tail word is zero-padded. Every shared access is atomic, so there are no data races.
func storeWords[V any](dst []uint64, v *V, size int) {
	src := unsafe.Slice((*byte)(unsafe.Pointer(v)), size)
	for i := 0; i < size; i += 8 {
		var w uint64
		copy((*[8]byte)(unsafe.Pointer(&w))[:], src[i:])
		atomic.StoreUint64(&dst[i>>3], w)
	}
}

// loadWords is the inverse of storeWords. It writes into a caller-private value only.
func loadWords[V any](src []uint64, out *V, size int) {
	dst := unsafe.Slice((*byte)(unsafe.Pointer(out)), size)
	for i := 0; i < size; i += 8 {
		w := atomic.LoadUint64(&src[i>>3])
		copy(dst[i:], (*[8]byte)(unsafe.Pointer(&w))[:])
	}
}
```

## 5. Verification: `ring_test.go`

```go
package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

const ringTestMagic = 0x9E3779B97F4A7C15

type ringPayload struct {
	Seq   uint64
	Check uint64
	Tag   [6]byte
}

func newRingPayload(seq uint64) ringPayload {
	p := ringPayload{Seq: seq, Check: seq * ringTestMagic}
	for i := range p.Tag {
		p.Tag[i] = byte(seq) + byte(i)
	}
	return p
}

// intact detects torn payloads: every field is a function of Seq.
func (p ringPayload) intact() bool {
	if p.Check != p.Seq*ringTestMagic {
		return false
	}
	for i, b := range p.Tag {
		if b != byte(p.Seq)+byte(i) {
			return false
		}
	}
	return true
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Microsecond)
	}
}

func TestRingInOrder(t *testing.T) {
	ctx := context.Background()
	r := NewRing[uint64](8)
	c := r.Subscribe()
	for i := range uint64(5) {
		r.Publish(i)
	}
	for want := range uint64(5) {
		got, dropped, err := c.Next(ctx)
		if err != nil || dropped != 0 || got != want {
			t.Fatalf("got (%d, %d, %v), want (%d, 0, nil)", got, dropped, err, want)
		}
	}
	if lag := c.Lag(); lag != 0 {
		t.Fatalf("lag = %d, want 0", lag)
	}
}

func TestRingLapAccounting(t *testing.T) {
	const capacity, total = 8, 20
	r := NewRing[uint64](capacity)
	c := r.Subscribe()
	for i := range uint64(total) {
		r.Publish(i)
	}
	var dropped uint64
	var got []uint64
	for c.Lag() > 0 {
		v, d, err := c.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		dropped += d
		got = append(got, v)
	}
	if dropped != total-capacity {
		t.Fatalf("dropped = %d, want %d", dropped, total-capacity)
	}
	if len(got) != capacity {
		t.Fatalf("delivered %d entries, want %d", len(got), capacity)
	}
	for i, v := range got {
		if want := uint64(total - capacity + i); v != want {
			t.Fatalf("entry %d = %d, want %d", i, v, want)
		}
	}
}

func TestRingReplayAndLatest(t *testing.T) {
	ctx := context.Background()
	r := NewRing[uint64](8)
	for i := range uint64(20) {
		r.Publish(i)
	}
	rc := r.SubscribeReplay() // retained window is 12..19
	if v, d, err := rc.Next(ctx); err != nil || v != 12 || d != 0 {
		t.Fatalf("replay first = (%d, %d, %v), want (12, 0, nil)", v, d, err)
	}

	lc := r.Subscribe() // starts at 20
	for i := range uint64(10) {
		r.Publish(20 + i)
	}
	if v, d, ok := lc.Latest(); !ok || v != 29 || d != 9 {
		t.Fatalf("Latest = (%d, %d, %v), want (29, 9, true)", v, d, ok)
	}
	if _, _, ok := lc.Latest(); ok {
		t.Fatal("Latest returned a value with nothing new published")
	}
	r.Publish(30)
	if v, d, ok := lc.Latest(); !ok || v != 30 || d != 0 {
		t.Fatalf("Latest after publish = (%d, %d, %v), want (30, 0, true)", v, d, ok)
	}
}

func TestRingDrain(t *testing.T) {
	r := NewRing[uint64](16)
	c := r.Subscribe()
	for i := range uint64(10) {
		r.Publish(i)
	}
	buf := make([]uint64, 4)
	var all []uint64
	for {
		n, dropped := c.Drain(buf)
		if dropped != 0 {
			t.Fatalf("unexpected drops: %d", dropped)
		}
		if n == 0 {
			break
		}
		all = append(all, buf[:n]...)
	}
	if len(all) != 10 {
		t.Fatalf("drained %d entries, want 10", len(all))
	}
	for i, v := range all {
		if v != uint64(i) {
			t.Fatalf("entry %d = %d, want %d", i, v, i)
		}
	}
}

func TestRingRejectsPointerTypes(t *testing.T) {
	cases := map[string]func(){
		"string":    func() { NewRing[string](4) },
		"slice":     func() { NewRing[[]byte](4) },
		"pointer":   func() { NewRing[*int](4) },
		"map":       func() { NewRing[map[int]int](4) },
		"interface": func() { NewRing[any](4) },
		"nested":    func() { NewRing[struct{ S string }](4) },
		"array":     func() { NewRing[[2]*int](4) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected a panic for a pointerful element type")
				}
			}()
			fn()
		})
	}
}

type ringOddLayout struct {
	A byte
	B uint16
	C [3]byte
}

type ringNineBytes [9]byte

func roundTrip[V comparable](t *testing.T, in []V) {
	t.Helper()
	r := NewRing[V](len(in))
	c := r.Subscribe()
	for _, v := range in {
		if !r.Publish(v) {
			t.Fatal("publish failed")
		}
	}
	for i, want := range in {
		got, dropped, err := c.Next(context.Background())
		if err != nil || dropped != 0 || got != want {
			t.Fatalf("entry %d: got (%v, %d, %v), want %v", i, got, dropped, err, want)
		}
	}
}

func TestRingRoundTripLayouts(t *testing.T) {
	roundTrip(t, []struct{}{{}, {}, {}})                                                   // size 0
	roundTrip(t, []byte{1, 2, 3})                                                          // sub-word
	roundTrip(t, []ringOddLayout{{1, 2, [3]byte{3, 4, 5}}, {255, 65535, [3]byte{9, 8, 7}}}) // tail word
	roundTrip(t, []ringNineBytes{{1, 2, 3, 4, 5, 6, 7, 8, 9}, {9, 9, 9, 9, 9, 9, 9, 9, 0}})
	roundTrip(t, []complex128{1 + 2i, -3.5i})
	roundTrip(t, []ringPayload{newRingPayload(1), newRingPayload(1 << 40)})
}

func TestRingCloseSemantics(t *testing.T) {
	ctx := context.Background()
	r := NewRing[int](4)
	c := r.Subscribe()
	r.Publish(7)
	r.Close()
	if r.Publish(8) {
		t.Fatal("Publish succeeded after Close")
	}
	if v, _, err := c.Next(ctx); err != nil || v != 7 {
		t.Fatalf("pre-close entry = (%d, %v), want (7, nil)", v, err)
	}
	if _, _, err := c.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("after drain: err = %v, want ErrClosed", err)
	}
	r.Close() // idempotent
}

func TestRingCloseWakesParkedConsumer(t *testing.T) {
	r := NewRing[int](4)
	c := r.Subscribe()
	errc := make(chan error, 1)
	go func() {
		_, _, err := c.Next(context.Background())
		errc <- err
	}()
	waitUntil(t, "consumer to park", func() bool { return r.waiters.Load() == 1 })
	r.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not unpark the consumer")
	}
	waitUntil(t, "parked count to drop", func() bool { return r.waiters.Load() == 0 })
}

func TestRingContextCancelUnparks(t *testing.T) {
	r := NewRing[int](4)
	c := r.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, _, err := c.Next(ctx)
		errc <- err
	}()
	waitUntil(t, "consumer to park", func() bool { return r.waiters.Load() == 1 })
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled consumer did not unpark")
	}
	waitUntil(t, "parked count to drop", func() bool { return r.waiters.Load() == 0 })
	r.Close()
}

// TestRingStressLateJoinersAndLaps exercises the strongest invariant: for every cursor,
// each delivered entry's Seq equals the running position (start + all prior dropped +
// delivered). This proves no torn reads, no duplicates, no reordering, and exact loss
// accounting, with late joiners, slow consumers that lap, and frequent parking.
func TestRingStressLateJoinersAndLaps(t *testing.T) {
	total := uint64(100_000)
	if testing.Short() {
		total = 10_000
	}
	const joins = 8
	joinEvery := total / joins

	r := NewRing[ringPayload](64)
	ctx := context.Background()
	errs := make(chan error, joins)
	var wg sync.WaitGroup

	consume := func(id int, c *Cursor[ringPayload], start uint64, slow bool) {
		defer wg.Done()
		next := start
		var got uint64
		for {
			p, dropped, err := c.Next(ctx)
			next += dropped
			if errors.Is(err, ErrClosed) {
				break
			}
			if err != nil {
				errs <- err
				return
			}
			if !p.intact() || p.Seq != next {
				errs <- fmt.Errorf("consumer %d: got seq %d intact=%v, want seq %d", id, p.Seq, p.intact(), next)
				return
			}
			next++
			got++
			if slow && got%256 == 0 {
				time.Sleep(100 * time.Microsecond)
			}
		}
		if next != total {
			errs <- fmt.Errorf("consumer %d: accounted up to %d, want %d", id, next, total)
		}
	}

	for i := range total {
		if i%joinEvery == 0 {
			id := int(i / joinEvery)
			c := r.Subscribe() // taken on the publisher goroutine: starts exactly at position i
			wg.Add(1)
			go consume(id, c, i, id%2 == 0)
		}
		r.Publish(newRingPayload(i))
		if i%997 == 0 {
			time.Sleep(20 * time.Microsecond)
		}
	}
	r.Close()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if w := r.waiters.Load(); w != 0 {
		t.Fatalf("%d consumers still registered as parked", w)
	}
}

// TestRingSteadyStateZeroAlloc verifies the hot path (publish + read with data pending)
// does not allocate.
func TestRingSteadyStateZeroAlloc(t *testing.T) {
	r := NewRing[ringPayload](256)
	c := r.Subscribe()
	p := newRingPayload(3)
	allocs := testing.AllocsPerRun(1000, func() {
		r.Publish(p)
		if _, _, ok := c.TryNext(); !ok {
			t.Error("expected a pending entry")
		}
	})
	if allocs != 0 {
		t.Fatalf("publish+read allocates %v times per operation", allocs)
	}
}
```

## 6. Benchmarks: `ring_bench_test.go`

```go
package bigbuff

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkRingPublish measures the publisher alone (no parked consumers) for two payload sizes.
func BenchmarkRingPublish(b *testing.B) {
	b.Run("u64", func(b *testing.B) { benchRingPublish(b, uint64(42)) })
	b.Run("64B", func(b *testing.B) { benchRingPublish(b, [8]uint64{1, 2, 3, 4, 5, 6, 7, 8}) })
}

func benchRingPublish[V any](b *testing.B, v V) {
	r := NewRing[V](1024)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.Publish(v)
	}
}

// BenchmarkRingFanout: one publisher, N consumers using blocking Next.
func BenchmarkRingFanout(b *testing.B) {
	for _, subs := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
			benchRingFanout(b, subs, false)
		})
	}
}

// BenchmarkRingFanoutDrain: same, but consumers batch with Drain and only park when empty.
func BenchmarkRingFanoutDrain(b *testing.B) {
	for _, subs := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
			benchRingFanout(b, subs, true)
		})
	}
}

func benchRingFanout(b *testing.B, subs int, batch bool) {
	r := NewRing[uint64](4096)
	var (
		wg        sync.WaitGroup
		delivered atomic.Uint64
	)
	ctx := context.Background()
	for range subs {
		c := r.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n uint64
			if batch {
				buf := make([]uint64, 64)
				for {
					k, _ := c.Drain(buf)
					n += uint64(k)
					if k == 0 {
						if _, _, err := c.Next(ctx); err != nil {
							break
						}
						n++
					}
				}
			} else {
				for {
					if _, _, err := c.Next(ctx); err != nil {
						break
					}
					n++
				}
			}
			delivered.Add(n)
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		r.Publish(uint64(i))
	}
	r.Close()
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(delivered.Load())/b.Elapsed().Seconds(), "deliveries/s")
}

// BenchmarkChanFanoutBaseline is the idiomatic lossy broadcast: one buffered channel per
// subscriber, non-blocking send, drop on full.
func BenchmarkChanFanoutBaseline(b *testing.B) {
	for _, subs := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("subs=%d", subs), func(b *testing.B) {
			benchChanFanout(b, subs)
		})
	}
}

func benchChanFanout(b *testing.B, subs int) {
	chans := make([]chan uint64, subs)
	var (
		wg        sync.WaitGroup
		delivered atomic.Uint64
	)
	for i := range chans {
		ch := make(chan uint64, 4096)
		chans[i] = ch
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n uint64
			for range ch {
				n++
			}
			delivered.Add(n)
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		v := uint64(i)
		for _, ch := range chans {
			select {
			case ch <- v:
			default: // lossy: drop when this subscriber's buffer is full
			}
		}
	}
	for _, ch := range chans {
		close(ch)
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(delivered.Load())/b.Elapsed().Seconds(), "deliveries/s")
}
```

## 7. Running it and honest limits

```sh
go vet ./...
go test -race -count=1 -run 'Ring' ./...        # race detector + checkptr
go test -count=1 -run 'Ring' -v ./...
go test -run xxx -bench 'Ring|ChanFanout' -benchmem -count 5 -cpu 1,4,8 ./...
```

I have not run these in this response, so I am not quoting numbers. The design predicts the following, and the benchmarks are there to confirm or refute it:

- **Publish** costs a closed-flag load, one CAS guard, `4 + W` seq-cst stores (on amd64 each is an XCHG), a waiters load, and no allocation. It does not depend on subscriber count. Compare it to a buffered-channel send, which costs a lock and a sudog handoff.
- **Fan-out cost** is the one that should separate the two designs. The channel baseline does N locked sends per message, while the ring does one publish and each consumer pulls its own copy.
- **Parking** allocates one 96-byte channel per wake epoch. In a fully synchronous ping-pong this is one allocation per publish, the same as channel notify. Steady-state zero allocation holds whenever consumers have data pending. `TestRingSteadyStateZeroAlloc` enforces that.

Known limits, stated plainly:

- **Plain data only.** Enforced at construction. Pointerful payloads belong on a channel or behind an index into a separate store.
- **Single publisher.** Enforced by a CAS guard that panics on overlap. Removing the guard saves one locked operation per publish and is a reasonable build-specific variant once the contract is trusted.
- **Per-word atomics** cost one full barrier per 8 bytes on amd64. This suits payloads up to a few hundred bytes. Beyond that, a bulk `memmove` seqlock or an assembly path with non-temporal stores would win, but I left those out because they cannot be validated without measurement.
- **Wakeups are a broadcast by design.** N parked consumers mean N wakeups per epoch. That is the fan-out semantics, but it means the herd cost is proportional to the number of parked consumers.
- **Cursors are not goroutine-safe.** One goroutine per cursor is the contract.
