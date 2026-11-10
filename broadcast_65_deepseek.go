// Broadcaster65Deepseek: wait-free single-writer, multi-consumer bounded
// broadcast ring with lossy, skip-accounted semantics.
//
// Design summary:
//
//   - One immutable slot per ring cell, each padded to a cache line. Every
//     slot carries a monotonically increasing seqlock (`seq`) plus TWO value
//     buffers (`buf[0]`, `buf[1]`). A generation g writes to `buf[g&1]`.
//     The odd/even seqlock transitions are:
//         seq = 2*g+1  (writer is writing buf[g&1])
//         seq = 2*g+2  (generation g complete)
//     Because the writer alternates buffers, the other buffer (holding
//     generation g-1) is never touched while the writer mutates g. A reader
//     that observes even `seq = 2*g+2` may read `buf[g&1]` safely until the
//     writer next touches that buffer, which happens only when seq reaches
//     2*g+5 (start of generation g+2). This is checked with one extra
//     atomic load, so caught-up readers perform exactly ONE value copy.
//
//   - Consumers keep independent absolute-position cursors and advance at
//     their own rate. When a cursor falls more than the ring capacity
//     behind, the reader detects that the slot's current generation is
//     greater than the requested generation and JUMPS to the newest
//     generation directly — lossy skip semantics with skip accounting
//     exposed via Subscription65Deepseek.Skipped.
//
//   - Wakeup uses per-consumer 1-buffered `notify` channels driven by the
//     writer's loop over the copy-on-write consumer slice. A per-consumer
//     `waiting` flag (SC atomic) is used to avoid spurious channel sends.
//
//   - Single-writer discipline is enforced by documentation; concurrent
//     Publish calls are undefined behaviour (a serialising mutex would add
//     an uncontended ~20 ns per publish — see benchmarks).
//
// V may be any type; hot-path copies of V go through the slotBuffer
// indirection so the -race build can substitute a mutex-backed cell.

package bigbuff

import (
	"context"
	"errors"
	"iter"
	"math/bits"
	"sync"
	"sync/atomic"
)

// bbCacheLine65Deepseek is 128 bytes — a safe upper bound that covers both 64-byte
// (Intel/AMD) and 128-byte (Apple Silicon) cache-line granularity.
const bbCacheLine65Deepseek = 128

// ErrBroadcasterClosed65Deepseek is returned by Publish after Close.
var ErrBroadcasterClosed65Deepseek = errors.New("bigbuff: broadcaster closed")

// Broadcaster65Deepseek is the wait-free broadcast ring. See the package comment.
//
// V may be any type; hot-path reads and writes of V are performed via the
// slotBuffer65Deepseek indirection so the -race build can substitute a mutex-backed
// implementation.
type Broadcaster65Deepseek[V any] struct {
	// head is the number of completed publishes = position of the NEXT
	// publish. It is published AFTER the slot write completes, so any
	// consumer that observes head > p may trust that position p is
	// visible (subject to seqlock verification for skip-ahead reads).
	_      [bbCacheLine65Deepseek]byte
	head   atomic.Uint64
	closed atomic.Bool
	_      [bbCacheLine65Deepseek]byte

	// Immutable after construction. Kept on their own line (read-shared by
	// producers and consumers, never written).
	capacity uint64
	log2cap  uint
	mask     uint64
	slots    []broadcastSlot65Deepseek[V]

	_ [bbCacheLine65Deepseek]byte

	// Copy-on-write consumer registry. Writes are rare (Subscribe/Close),
	// reads (Publish) are lock-free.
	subsMu  sync.Mutex
	subsPtr atomic.Pointer[[]*Subscription65Deepseek[V]]
	_       [bbCacheLine65Deepseek]byte
}

// broadcastSlot65Deepseek is one ring cell. See the seq encoding in the package comment.
type broadcastSlot65Deepseek[V any] struct {
	// seq: 0 = never written; 2g+1 = writing generation g; 2g+2 = generation
	// g complete. Monotonic non-decreasing.
	seq atomic.Uint64

	// buf is double-buffered: buf[g&1] holds generation g.
	buf [2]slotBuffer65Deepseek[V]

	// Trailing pad separates this slot's seq/buf from the next slot's seq/buf
	// (avoids false sharing of the hot seq line between neighbouring cells).
	_ [bbCacheLine65Deepseek]byte
}

// NewBroadcaster65Deepseek returns a Broadcaster65Deepseek with a ring capacity. capacity is
// rounded UP to the next power of two (min 1). Panics if capacity <= 0.
func NewBroadcaster65Deepseek[V any](capacity int) *Broadcaster65Deepseek[V] {
	if capacity <= 0 {
		panic("bigbuff: NewBroadcaster65Deepseek: capacity must be > 0")
	}
	if capacity&(capacity-1) != 0 {
		capacity = 1 << bits.Len(uint(capacity-1))
	}
	b := &Broadcaster65Deepseek[V]{
		capacity: uint64(capacity),
		log2cap:  uint(bits.TrailingZeros(uint(capacity))),
		mask:     uint64(capacity - 1),
		slots:    make([]broadcastSlot65Deepseek[V], capacity),
	}
	empty := make([]*Subscription65Deepseek[V], 0)
	b.subsPtr.Store(&empty)
	return b
}

// Head returns the current publish head (number of completed publishes).
func (b *Broadcaster65Deepseek[V]) Head() uint64 { return b.head.Load() }

// Cap returns the ring capacity.
func (b *Broadcaster65Deepseek[V]) Cap() int { return int(b.capacity) }

// Closed reports whether Close has been called.
func (b *Broadcaster65Deepseek[V]) Closed() bool { return b.closed.Load() }

// Publish writes v at the current head position and returns its absolute
// position. Publish is single-writer: concurrent calls are undefined
// behaviour. It is wait-free: a bounded sequence of atomic stores and copies.
func (b *Broadcaster65Deepseek[V]) Publish(v V) (uint64, error) {
	if b.closed.Load() {
		return 0, ErrBroadcasterClosed65Deepseek
	}

	// Single-writer: Load + Store, no CAS.
	pos := b.head.Load()
	slot := &b.slots[pos&b.mask]
	gen := pos >> b.log2cap

	// Seqlock write: odd → copy → even.
	slot.seq.Store(2*gen + 1)
	slot.buf[gen&1].store(v)
	slot.seq.Store(2*gen + 2)

	// Publish head AFTER slot write.
	b.head.Store(pos + 1)

	// Wake registered waiters (SC atomics + buffered channel → no alloc).
	subs := b.subsPtr.Load()
	for _, s := range *subs {
		if s.waiting.Load() {
			select {
			case s.notify <- struct{}{}:
			default:
			}
		}
	}
	return pos, nil
}

// Close marks the broadcaster closed, wakes all consumers, and returns nil.
// Idempotent. After Close, Publish returns ErrBroadcasterClosed65Deepseek and consumers
// drain any remaining published positions before Next returns false.
func (b *Broadcaster65Deepseek[V]) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	subs := b.subsPtr.Load()
	for _, s := range *subs {
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
	return nil
}

// Subscribe registers a new consumer that will observe only publishes issued
// AFTER this call. ctx is used for cancellation; nil is treated as
// context.Background().
func (b *Broadcaster65Deepseek[V]) Subscribe(ctx context.Context) *Subscription65Deepseek[V] {
	return b.newSubscription(ctx, b.head.Load())
}

// SubscribeFrom registers a consumer that starts at position pos. If pos is
// in the future it is clamped to the current head; if pos is too old, the
// first read will detect the lapping and skip forward.
func (b *Broadcaster65Deepseek[V]) SubscribeFrom(ctx context.Context, pos uint64) *Subscription65Deepseek[V] {
	if h := b.head.Load(); pos > h {
		pos = h
	}
	return b.newSubscription(ctx, pos)
}

func (b *Broadcaster65Deepseek[V]) newSubscription(ctx context.Context, start uint64) *Subscription65Deepseek[V] {
	if ctx == nil {
		ctx = context.Background()
	}
	s := &Subscription65Deepseek[V]{
		b:      b,
		ctx:    ctx,
		notify: make(chan struct{}, 1),
		cursor: start,
	}
	b.subsMu.Lock()
	old := b.subsPtr.Load()
	next := make([]*Subscription65Deepseek[V], len(*old)+1)
	copy(next, *old)
	next[len(*old)] = s
	b.subsPtr.Store(&next)
	b.subsMu.Unlock()
	return s
}

// ReadResult65Deepseek reports the outcome of a non-blocking read attempt.
type ReadResult65Deepseek int

const (
	// ReadOK65Deepseek indicates a value was returned.
	ReadOK65Deepseek ReadResult65Deepseek = iota
	// ReadNotReady65Deepseek indicates no new value is available yet; retry later.
	ReadNotReady65Deepseek
	// ReadClosed65Deepseek indicates the subscription or broadcaster is finished.
	ReadClosed65Deepseek
)

// Subscription65Deepseek is a consumer handle onto a Broadcaster65Deepseek. Each Subscription65Deepseek has
// an independent cursor. Methods on a Subscription65Deepseek (except Close) must be
// called from a SINGLE consumer goroutine.
type Subscription65Deepseek[V any] struct {
	// Immutable after construction.
	b      *Broadcaster65Deepseek[V]
	ctx    context.Context
	notify chan struct{}
	done   atomic.Bool
	_      [bbCacheLine65Deepseek]byte

	// Consumer-hot: only the consuming goroutine touches cursor / skipped
	// (except Close, which does not touch them). Kept on their own line.
	cursor  uint64
	skipped atomic.Uint64
	_       [bbCacheLine65Deepseek]byte

	// waiting is written by the consumer (before parking) and read by the
	// producer. Its own line avoids false sharing with the consumer's hot
	// cursor.
	waiting atomic.Bool
	_       [bbCacheLine65Deepseek]byte
}

// TryNext attempts to read the next value without blocking.
func (s *Subscription65Deepseek[V]) TryNext() (V, ReadResult65Deepseek) {
	return s.read()
}

// Next blocks until a value is available, the context is cancelled, or the
// broadcaster / subscription is closed. Returns false on termination.
func (s *Subscription65Deepseek[V]) Next() (V, bool) {
	return s.NextContext(nil)
}

// NextContext is Next with an additional per-call context: when ctx is done
// the call returns false but, unlike cancellation of the subscription's own
// context, the subscription stays open and may be read again. A nil ctx is
// equivalent to Next.
//
// Integration repair: as generated, Next could only be cancelled through
// the context fixed at Subscribe time, which forced callers needing per-call
// cancellation to poll TryNext on a timer. The wait itself is unchanged (arm
// the waiter bit, re-check, park on the subscription's notify channel).
func (s *Subscription65Deepseek[V]) NextContext(ctx context.Context) (V, bool) {
	var callDone <-chan struct{}
	if ctx != nil {
		callDone = ctx.Done()
	}
	for {
		v, r := s.read()
		switch r {
		case ReadOK65Deepseek:
			return v, true
		case ReadClosed65Deepseek:
			s.Close()
			return v, false
		}

		if s.done.Load() {
			var zero V
			return zero, false
		}
		select {
		case <-s.ctx.Done():
			s.Close()
			var zero V
			return zero, false
		case <-callDone:
			var zero V
			return zero, false
		default:
		}

		// Arm the waiter bit, then re-check for the classic lost-wakeup race.
		s.waiting.Store(true)
		if s.done.Load() || s.b.closed.Load() || s.hasData() {
			s.waiting.Store(false)
			continue
		}

		select {
		case <-s.notify:
			s.waiting.Store(false)
		case <-s.ctx.Done():
			s.waiting.Store(false)
			s.Close()
			var zero V
			return zero, false
		case <-callDone:
			s.waiting.Store(false)
			var zero V
			return zero, false
		}
	}
}

// hasData is a cheap "might have new data" predicate used only to close the
// lost-wakeup window. False negatives are fine: they lead to a re-check.
func (s *Subscription65Deepseek[V]) hasData() bool {
	return s.cursor < s.b.head.Load()
}

// completeGen65Deepseek decodes the newest fully-written generation from a slot seq.
// Returns (gen, true) iff at least one generation has been fully written.
func completeGen65Deepseek(seq uint64) (uint64, bool) {
	if seq < 2 {
		return 0, false
	}
	// even  = 2g+2  → g  = (seq-2)/2
	// odd   = 2g+1  → g' = g-1 = (seq-2)/2   (newest COMPLETE is g-1)
	return (seq - 2) >> 1, true
}

func (s *Subscription65Deepseek[V]) read() (V, ReadResult65Deepseek) {
	b := s.b
	// Bounded retries in the (extremely rare) fully-lapping case keeps the
	// goroutine from spinning forever against an adversarial writer.
	for retries := 0; retries <= 8; retries++ {
		if s.done.Load() {
			var zero V
			return zero, ReadClosed65Deepseek
		}

		// Load closed FIRST, then head: if closed was true, head is final
		// (no future publishes will ever bump it) and we can safely report
		// ReadClosed65Deepseek once the cursor reaches it.
		closed := b.closed.Load()
		head := b.head.Load()
		p := s.cursor
		if p >= head {
			if closed {
				var zero V
				return zero, ReadClosed65Deepseek
			}
			var zero V
			return zero, ReadNotReady65Deepseek
		}

		slot := &b.slots[p&b.mask]
		gen := p >> b.log2cap
		s1 := slot.seq.Load()
		g, ok := completeGen65Deepseek(s1)
		if !ok {
			// Cannot happen while head > p, but stay defensive.
			var zero V
			return zero, ReadNotReady65Deepseek
		}

		if g > gen {
			// Lapped: jump directly to the newest generation in this slot.
			newPos := (g << b.log2cap) | (p & b.mask)
			s.skipped.Add(newPos - p)
			v := slot.buf[g&1].load()
			s3 := slot.seq.Load()
			// The writer overwrites buf[g&1] only when it starts generation
			// g+2, i.e. when seq reaches 2*g+5. Any s3 below that means our
			// copy of generation g is still intact.
			if s3 >= 2*g+5 {
				s.cursor = newPos
				continue
			}
			s.cursor = newPos + 1
			return v, ReadOK65Deepseek
		}

		// g == gen: normal read from the current buffer.
		v := slot.buf[gen&1].load()
		s2 := slot.seq.Load()
		if s2 >= 2*gen+5 {
			// Writer began (or completed) generation gen+2 — our buffer may
			// have been concurrently overwritten. Retry; next iteration will
			// detect the lapping and jump.
			continue
		}
		s.cursor = p + 1
		return v, ReadOK65Deepseek
	}
	var zero V
	return zero, ReadNotReady65Deepseek
}

// Cursor returns the current read cursor (the next position to be read).
// Consumer goroutine only; safe without atomics because only one goroutine
// touches the cursor field.
func (s *Subscription65Deepseek[V]) Cursor() uint64 { return s.cursor }

// Skipped returns the total number of positions that this subscription
// skipped due to overrun. Atomic and safe from any goroutine.
func (s *Subscription65Deepseek[V]) Skipped() uint64 { return s.skipped.Load() }

// Close deregisters the subscription and unblocks any goroutine in Next.
// Idempotent; safe from any goroutine.
func (s *Subscription65Deepseek[V]) Close() error {
	if !s.done.CompareAndSwap(false, true) {
		return nil
	}
	b := s.b
	b.subsMu.Lock()
	old := b.subsPtr.Load()
	next := make([]*Subscription65Deepseek[V], 0, len(*old))
	found := false
	for _, x := range *old {
		if x == s {
			found = true
			continue
		}
		next = append(next, x)
	}
	if found {
		b.subsPtr.Store(&next)
	}
	b.subsMu.Unlock()

	select {
	case s.notify <- struct{}{}:
	default:
	}
	return nil
}

// Iter returns an iterator (Go 1.23 range-over-func) that yields successive
// values from this subscription until Next returns false, the yield callback
// returns false, or the subscription is closed.
func (s *Subscription65Deepseek[V]) Iter() iter.Seq[V] {
	return func(yield func(V) bool) {
		defer s.Close()
		for {
			v, ok := s.Next()
			if !ok {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}
