// Source: 73-meta transcript (doc.go, cohort.go). Identifiers carry the
// 73Meta suffix. Two integration repairs are marked "Integration repair" in
// the code (Snapshot epoch/value pairing; Close no longer completes an
// in-flight barrier); everything else is as generated. Measured notes are in
// 01-readme.md: Broadcast allocates one barrier channel per call with at
// least one member, and broadcasts are serialised by a semaphore channel.
//
// Cohort73Meta (73-Meta), in the words of its transcript, is "a genuinely novel,
// production-grade coordination primitive": a select-composable, barrier-synchronized
// broadcast to a dynamic cohort of receivers with mid-flight
// cancellation compensation.
//
// # The problem
//
// Go's standard primitives break down exactly where high-performance
// state transitions collide with runtime composability:
//
//   - A naive broadcast loop over caller-owned unbuffered channels
//     deadlocks when a receiver cancels, leaves, or breaks out of a
//     range loop mid-broadcast. The sender is stuck in a rendezvous
//     send with no receiver, and there is no way to compensate.
//   - A per-subscriber buffered queue per receiver still cannot
//     express "wait until every currently-joined receiver has
//     processed this value, or has deterministically departed,"
//     and a rendezvous send to it still deadlocks on mid-flight
//     leave. Cohort73Meta pairs a single-slot coalesced wakeup channel
//     per member (never a queue, never a blocking send) with an
//     explicit barrier counter, so departure compensates.
//   - sync.Cond thundering-herds every waiter on every publish,
//     and callback registries break select ergonomics: the caller
//     cannot write `select { case <-m.Notify(): ... case
//     <-ctx.Done(): }` because the primitive owns the wakeup.
//
// # What Cohort73Meta is
//
// A Cohort73Meta[V] is a synchronous, barrier-synchronized broadcast:
//
//   - Join() admits a Member73Meta. Members are cheap, pointer-stable,
//     one small buffered channel each, and no goroutine.
//   - Broadcast(ctx, v) publishes v as a new epoch and blocks
//     until every member that was joined at publish time has
//     either Acked that epoch or deterministically departed via
//     Leave(). If there are no members it returns immediately.
//   - A Member73Meta observes values without any channel rendezvous:
//     either by blocking in Member73Meta.Wait(ctx), by composing
//     Member73Meta.Notify() directly inside its own select, by polling
//     Member73Meta.Snapshot(), or by ranging over Member73Meta.All(ctx)
//     (iter.Seq[V]).
//   - Cancellation is compensating, not leaking. If a Member73Meta
//     leaves mid-broadcast, Leave() atomically decrements the
//     pending barrier count, so the publisher never stalls. If
//     the publisher's context is cancelled mid-broadcast, the
//     barrier is abandoned, the value stays published, and the
//     next broadcast proceeds. If a consumer breaks out of
//     Member73Meta.All early, the iterator acks the last value it
//     yielded before returning.
//   - Close() is deterministic and fail-fast. It unblocks every
//     waiter and every in-flight broadcast, closes no channel
//     twice, and leaves no goroutine behind.
//
// # Why it beats chan / Mutex / Cond
//
//   - Per-member coalesced wakeup channels. Each member owns
//     one `chan struct{}` of capacity 1, stable for its lifetime
//     so callers may cache Notify() and select on it in a loop.
//     Publish does one non-blocking send per joined member.
//     A single shared channel would deliver its one token to a
//     single waiter and starve the rest; per-member channels
//     wake every waiter. Multiple rapid broadcasts coalesce
//     into one token per member; waiters re-check the epoch,
//     so no wakeup is lost, there is no thundering herd, and
//     there is no per-broadcast channel allocation on the
//     receive path.
//   - Barrier, not rendezvous. The publisher never selects over
//     N receiver channels and never blocks in a rendezvous send.
//     It waits on one per-broadcast done channel while receivers
//     ack via a single mutex-protected counter. Publish does
//     O(N) non-blocking coalesced sends (never a blocking send
//     or an O(N) select), each ack is O(1), and there is no
//     goroutine per receiver.
//   - Epoch + RWMutex snapshot, not torn reads. The value is
//     guarded by an RWMutex and the epoch is an atomic counter.
//     Snapshot reads epoch, copies the value under RLock, then
//     re-validates the epoch. If a writer interleaved, it retries.
//     Readers never block each other, writers never block readers
//     except for the instant of the copy, and a torn
//     value+epoch pair is impossible.
//   - Cache-line isolation. Hot atomics (epoch, member count,
//     per-member ack watermark) live on their own 64-byte lines
//     with explicit padding, so ack traffic from different
//     goroutines does not false-share.
//   - Cancellable publisher admission. Publishers serialize on a
//     capacity-1 semaphore channel, acquired via select on
//     ctx.Done() and Closed(). A publisher that loses a race or
//     times out never blocks forever holding a mutex.
//   - Fail-fast invariants. Pending counts, epoch monotonicity,
//     and member-count consistency are checked under the lock
//     and panic on violation instead of silently hanging. See
//     Cohort73Meta.CheckInvariants.
//
// # Correct usage
//
// Publisher:
//
//	cohort := bigbuff.NewCohort73Meta[int]()
//	n, err := cohort.Broadcast(ctx, 42)
//
// Receiver, explicit ack (barrier semantics):
//
//	m, ok := cohort.Join()
//	if !ok { /* closed */ }
//	defer m.Leave()
//	for {
//	    v, epoch, err := m.Wait(ctx)
//	    if err != nil { break }
//	    process(v)
//	    m.Ack(epoch)
//	}
//
// Receiver, select-composable:
//
//	select {
//	case <-m.Notify():
//	    if v, epoch, ok := m.TryWait(); ok {
//	        process(v)
//	        m.Ack(epoch)
//	    }
//	case <-ctx.Done():
//	    m.Leave() // compensates any in-flight broadcast
//	    return ctx.Err()
//	case <-m.Closed():
//	    return bigbuff.ErrClosed73Meta
//	}
//
// Receiver, iterator (break is compensating):
//
//	for v := range m.All(ctx) {
//	    if done(v) { break } // iterator acks v before returning
//	    process(v)
//	}
//
// Join does not replay old epochs. A member that joins after a
// broadcast will only observe broadcasts published after it joined.
// A member must call Leave exactly once (it is idempotent) or the
// cohort retains it and future broadcasts will wait for it.
//
// Hot path allocations: Notify, Snapshot, TryWait, Ack, and
// Leave allocate nothing. Join allocates one small channel per
// member, amortized over the member's lifetime. Broadcast
// allocates one done channel per broadcast that has at least
// one member, amortized over N deliveries. There are no
// background goroutines.
package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

// ErrClosed73Meta is returned when an operation is attempted on a closed
// Cohort73Meta, or when a waiter is unblocked because the Cohort73Meta closed.
var ErrClosed73Meta = errors.New("bigbuff: cohort closed")

// ErrBusy73Meta is returned by TryBroadcast when another broadcast is
// already in flight.
var ErrBusy73Meta = errors.New("bigbuff: another broadcast in flight")

// paddedUint6473Meta is a 64-byte cache-line isolated atomic counter.
// Ack watermarks and the epoch are written by different goroutines
// at high frequency; without padding they would false-share and
// bounce the same cache line between cores on every ack.
type paddedUint6473Meta struct {
	v atomic.Uint64
	_ [56]byte
}

// paddedInt6473Meta is the signed counterpart of paddedUint6473Meta.
type paddedInt6473Meta struct {
	v atomic.Int64
	_ [56]byte
}

// Cohort73Meta is a barrier-synchronized broadcast to a dynamic set of
// Members.
//
// The zero value is not usable; use NewCohort73Meta.
//
// A Cohort73Meta must not be copied after first use. Always use the
// *Cohort73Meta returned by NewCohort73Meta.
type Cohort73Meta[V any] struct {
	// Hot atomics, each on its own cache line.
	epoch        paddedUint6473Meta
	membersCount paddedInt6473Meta

	// mu serializes membership, pending-barrier state, and the
	// epoch increment + member snapshot in Broadcast. It is never
	// held while waiting on a channel or context.
	mu sync.Mutex
	// valMu guards value. Broadcast takes it for writing (exclusive)
	// while holding mu; Snapshot takes it for reading (shared)
	// without mu. Lock ordering is always mu -> valMu, never the
	// reverse, so there is no deadlock cycle.
	valMu sync.RWMutex
	value V
	// closedCh is closed exactly once by Close.
	closedCh chan struct{}
	closed   atomic.Bool
	// closedUnderMu mirrors closed, but under mu so Broadcast/Join
	// can linearize against Close while holding mu.
	closedUnderMu bool
	// bcastSem serializes publishers. Capacity 1, initially full.
	// Acquired via select on ctx.Done() / closedCh so a publisher
	// never blocks forever on a mutex.
	bcastSem chan struct{}
	// members is the current join set. Only accessed under mu.
	members map[*Member73Meta[V]]struct{}
	// pendingEpoch is the epoch currently awaiting acks, or 0 if
	// no broadcast is pending.
	pendingEpoch uint64
	// pendingCount is the number of members joined at publish time
	// that have not yet acked pendingEpoch or left.
	pendingCount int
	// pendingDone is closed when pendingCount reaches 0. Only the
	// publisher waits on it.
	pendingDone chan struct{}
}

// Member73Meta is a single receiver joined to a Cohort73Meta.
//
// A Member73Meta must not be copied after first use. Always use the
// *Member73Meta returned by Join.
type Member73Meta[V any] struct {
	cohort *Cohort73Meta[V]
	// notify is this member's own coalesced wakeup channel
	// (capacity 1, never closed). Broadcast does one
	// non-blocking send to each joined member, so every waiter
	// wakes: a single shared channel would deliver its one token
	// to a single waiter and starve the rest. The channel is
	// stable for the member's lifetime, so callers may cache the
	// result of Notify() and select on it in a loop. Rapid
	// broadcasts coalesce into one token; waiters re-check the
	// epoch, so no wakeup is lost.
	notify chan struct{}
	// acked is the highest epoch this member has acked (or, at
	// Join time, the current epoch, so a joiner is exempt from
	// any in-flight broadcast). Isolated on its own cache line so
	// concurrent acks from different members do not false-share.
	acked paddedUint6473Meta
	left  atomic.Bool
	_     [44]byte
}

// NewCohort73Meta returns an empty Cohort73Meta.
func NewCohort73Meta[V any]() *Cohort73Meta[V] {
	c := &Cohort73Meta[V]{
		closedCh: make(chan struct{}),
		bcastSem: make(chan struct{}, 1),
		members:  make(map[*Member73Meta[V]]struct{}),
	}
	c.bcastSem <- struct{}{}
	return c
}

// Join admits a new Member73Meta. It does not replay old epochs: the
// member will only observe broadcasts published after Join returns.
//
// The second result is false if the Cohort73Meta is already closed.
func (c *Cohort73Meta[V]) Join() (*Member73Meta[V], bool) {
	if c.closed.Load() {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closedUnderMu {
		return nil, false
	}
	m := &Member73Meta[V]{cohort: c, notify: make(chan struct{}, 1)}
	// Set the ack watermark to the current epoch under mu, so
	// Join is atomic with Broadcast's snapshot: if Broadcast
	// snapshots after us, we are counted (acked == old epoch <
	// new pendingEpoch) and must Ack or Leave to satisfy the
	// barrier; if Broadcast snapshots before us, we are not
	// counted (acked == new pendingEpoch) and are exempt.
	// Either way the barrier cannot over-count.
	m.acked.v.Store(c.epoch.v.Load())
	c.members[m] = struct{}{}
	c.membersCount.v.Add(1)
	return m, true
}

// Members returns the number of currently joined members.
func (c *Cohort73Meta[V]) Members() int {
	return int(c.membersCount.v.Load())
}

// Epoch returns the latest published epoch, or 0 if nothing has
// been broadcast yet. Epochs are strictly monotonic.
func (c *Cohort73Meta[V]) Epoch() uint64 {
	return c.epoch.v.Load()
}

// Pending returns the number of members that have not yet acked
// the current in-flight broadcast, or 0 if none is in flight.
func (c *Cohort73Meta[V]) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pendingCount
}

// Closed returns a channel closed exactly once when Close is called.
func (c *Cohort73Meta[V]) Closed() <-chan struct{} {
	return c.closedCh
}

// CheckInvariants panics if any internal invariant is violated.
// It is intended for tests and for fail-fast debugging; production
// code does not need to call it on the hot path.
func (c *Cohort73Meta[V]) CheckInvariants() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pendingCount < 0 {
		panic(fmt.Sprintf("bigbuff: invariant violated: pendingCount=%d < 0", c.pendingCount))
	}
	if c.pendingCount > 0 {
		if c.pendingDone == nil {
			panic("bigbuff: invariant violated: pendingCount>0 but pendingDone is nil")
		}
		if c.pendingEpoch == 0 {
			panic("bigbuff: invariant violated: pendingCount>0 but pendingEpoch==0")
		}
		if c.pendingEpoch > c.epoch.v.Load() {
			panic(fmt.Sprintf("bigbuff: invariant violated: pendingEpoch=%d > epoch=%d", c.pendingEpoch, c.epoch.v.Load()))
		}
	} else {
		if c.pendingDone != nil {
			panic("bigbuff: invariant violated: pendingCount==0 but pendingDone != nil")
		}
		if c.pendingEpoch != 0 {
			panic(fmt.Sprintf("bigbuff: invariant violated: pendingCount==0 but pendingEpoch=%d", c.pendingEpoch))
		}
	}
	if c.pendingCount > len(c.members) {
		panic(fmt.Sprintf("bigbuff: invariant violated: pendingCount=%d > members=%d", c.pendingCount, len(c.members)))
	}
	if int(c.membersCount.v.Load()) != len(c.members) {
		panic(fmt.Sprintf("bigbuff: invariant violated: membersCount=%d != len(members)=%d", c.membersCount.v.Load(), len(c.members)))
	}
	if c.closedUnderMu != c.closed.Load() {
		panic("bigbuff: invariant violated: closedUnderMu != closed")
	}
}

// Broadcast publishes v as a new epoch and blocks until every
// member joined at publish time has Acked that epoch or left.
//
// If there are no members it publishes and returns (0, nil)
// immediately.
//
// If ctx is cancelled while waiting for the barrier, Broadcast
// abandons the barrier, leaves the value published, and returns
// ctx.Err(). Members that later Ack the epoch are treated as
// stale acks (their watermark advances, but the barrier is
// already gone). The next Broadcast proceeds normally.
//
// If the Cohort73Meta is closed while waiting, Broadcast returns
// ErrClosed73Meta (unless the barrier already completed, in which case
// it returns success).
//
// Broadcasts are serialized: concurrent publishers queue on the
// broadcast semaphore, which is acquired via select on ctx.Done()
// and Closed(), so a publisher never blocks forever on a mutex.
func (c *Cohort73Meta[V]) Broadcast(ctx context.Context, v V) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.closed.Load() {
		return 0, ErrClosed73Meta
	}
	select {
	case <-c.bcastSem:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-c.closedCh:
		return 0, ErrClosed73Meta
	}
	defer func() {
		// We hold the token; the channel is empty, so this cannot block.
		c.bcastSem <- struct{}{}
	}()
	return c.broadcastHoldingSem(ctx, v)
}

// TryBroadcast is like Broadcast, but if another broadcast is
// already in flight it returns (0, ErrBusy73Meta) immediately instead of
// queuing. Once it acquires the publisher slot it still waits for
// the barrier like Broadcast.
func (c *Cohort73Meta[V]) TryBroadcast(v V) (int, error) {
	if c.closed.Load() {
		return 0, ErrClosed73Meta
	}
	select {
	case <-c.bcastSem:
	default:
		return 0, ErrBusy73Meta
	}
	defer func() {
		c.bcastSem <- struct{}{}
	}()
	if c.closed.Load() {
		return 0, ErrClosed73Meta
	}
	return c.broadcastHoldingSem(context.Background(), v)
}

// broadcastHoldingSem publishes and waits for the barrier.
// The caller must hold bcastSem.
func (c *Cohort73Meta[V]) broadcastHoldingSem(ctx context.Context, v V) (int, error) {
	// If the context was already cancelled before we acquired the
	// publisher slot, do not publish. A cancellation that races
	// with publish is still linearizable as publish-then-abandon.
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	if c.closedUnderMu {
		c.mu.Unlock()
		return 0, ErrClosed73Meta
	}
	if c.pendingCount != 0 {
		// Fail-fast: bcastSem serializes publishers and every path
		// clears pending before releasing it, so this is unreachable
		// unless the state machine is corrupted.
		n := c.pendingCount
		c.mu.Unlock()
		panic(fmt.Sprintf("bigbuff: invariant violated: pendingCount=%d at broadcast start", n))
	}
	// Hold mu across value write + epoch increment + member
	// snapshot so Join cannot interleave and be counted without
	// having its ack watermark set (which would over-count the
	// barrier and hang the publisher forever).
	//
	// Integration repair: the epoch is advanced while valMu is still
	// write-locked. As generated it was advanced after the unlock, so
	// Snapshot's epoch/value/epoch validation could observe the new value
	// between two loads of the old epoch and return (v[E+1], E); a member
	// whose barrier for E had been abandoned would then receive v[E+1]
	// twice (once labelled E, once E+1).
	c.valMu.Lock()
	c.value = v
	newEpoch := c.epoch.v.Add(1)
	c.valMu.Unlock()
	n := len(c.members)
	var done chan struct{}
	if n > 0 {
		done = make(chan struct{})
		c.pendingEpoch = newEpoch
		c.pendingCount = n
		c.pendingDone = done
		// Per-member coalesced wakeup: one non-blocking send to
		// each joined member while still holding mu, so the
		// wakeup set is exactly the barrier set. Never blocks,
		// never allocates. A member that joins after this point
		// has acked == newEpoch and needs no wakeup for it.
		for mm := range c.members {
			select {
			case mm.notify <- struct{}{}:
			default:
			}
		}
	}
	c.mu.Unlock()

	if n == 0 {
		return 0, nil
	}

	select {
	case <-done:
		return n, nil
	case <-ctx.Done():
		// Prefer a completed barrier over cancellation if both
		// are ready: the broadcast actually succeeded.
		select {
		case <-done:
			return n, nil
		default:
		}
		// Abandon the barrier. The value stays published.
		c.mu.Lock()
		if c.pendingDone == done {
			c.pendingEpoch = 0
			c.pendingCount = 0
			c.pendingDone = nil
		}
		c.mu.Unlock()
		return 0, ctx.Err()
	case <-c.closedCh:
		select {
		case <-done:
			return n, nil
		default:
		}
		c.mu.Lock()
		if c.pendingDone == done {
			c.pendingEpoch = 0
			c.pendingCount = 0
			c.pendingDone = nil
		}
		c.mu.Unlock()
		return 0, ErrClosed73Meta
	}
}

// Close deterministically tears down the Cohort73Meta. It unblocks
// every waiter and every in-flight broadcast, is idempotent, and
// leaves no goroutine behind. Members that have not yet left
// remain joined (Members still counts them) but all waits return
// ErrClosed73Meta unless they have an unacked value to return first.
func (c *Cohort73Meta[V]) Close() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	c.mu.Lock()
	c.closedUnderMu = true
	c.pendingEpoch = 0
	c.pendingCount = 0
	c.pendingDone = nil
	// Wake any waiter blocked only on Notify (select-composable
	// callers that do not also watch Closed).
	for mm := range c.members {
		select {
		case mm.notify <- struct{}{}:
		default:
		}
	}
	c.mu.Unlock()
	// Integration repair: the in-flight barrier channel is deliberately NOT
	// closed here. As generated Close closed it, so a Broadcast blocked in
	// its three-way select could take the barrier case and report (n, nil)
	// although not every member had acked. The publisher is released through
	// closedCh instead and reports ErrClosed73Meta unless the barrier really
	// completed.
	close(c.closedCh)
}

// Notify returns this member's coalesced wakeup channel. It is
// stable for the member's lifetime, so callers may cache it and
// select on it in a loop. Broadcast signals it once per broadcast
// (coalesced if the member is slow); Close signals it once.
// A wakeup alone does not guarantee a new epoch: callers must
// re-check TryWait / Snapshot after waking. The channel is never
// closed; use Closed to observe teardown.
func (m *Member73Meta[V]) Notify() <-chan struct{} {
	return m.notify
}

// Closed returns the cohort's closed channel. See Cohort73Meta.Closed.
func (m *Member73Meta[V]) Closed() <-chan struct{} {
	return m.cohort.closedCh
}

// Acked returns the highest epoch this member has acked.
func (m *Member73Meta[V]) Acked() uint64 {
	return m.acked.v.Load()
}

// IsLeft reports whether Leave has been called.
func (m *Member73Meta[V]) IsLeft() bool {
	return m.left.Load()
}

// Snapshot returns the latest published value and its epoch.
// It returns ok=false if nothing has been published yet or if the
// member has left.
//
// Snapshot is consistent: it reads the epoch, copies the value
// under RLock, then re-validates the epoch. If a Broadcast
// interleaved, it retries, so a torn value+epoch pair is
// impossible. Concurrent Snapshots never block each other.
func (m *Member73Meta[V]) Snapshot() (V, uint64, bool) {
	if m.left.Load() {
		var zero V
		return zero, 0, false
	}
	c := m.cohort
	for {
		e1 := c.epoch.v.Load()
		if e1 == 0 {
			var zero V
			return zero, 0, false
		}
		c.valMu.RLock()
		v := c.value
		c.valMu.RUnlock()
		e2 := c.epoch.v.Load()
		if e1 == e2 {
			if m.left.Load() {
				var zero V
				return zero, 0, false
			}
			return v, e1, true
		}
		// A writer interleaved between the epoch read and the
		// value copy. Retry. With a single serialized publisher
		// this is bounded and rare; yield so a spinning reader
		// does not starve the writer.
		runtime.Gosched()
		if m.left.Load() {
			var zero V
			return zero, 0, false
		}
	}
}

// TryWait is the non-blocking form of Wait: it returns the latest
// value only if it has not yet been acked.
func (m *Member73Meta[V]) TryWait() (V, uint64, bool) {
	v, e, ok := m.Snapshot()
	if !ok {
		var zero V
		return zero, 0, false
	}
	if e <= m.acked.v.Load() {
		var zero V
		return zero, 0, false
	}
	return v, e, true
}

// Wait blocks until a new, unacked epoch is available, the
// context is cancelled, or the Cohort73Meta is closed.
//
// Wait does not Ack. The caller must call Ack(epoch) after it has
// processed the value, or the publisher's barrier will wait
// until the member leaves or the publisher's context expires.
// Use Next for receive-and-ack in one call, or All for an
// iterator that acks automatically.
func (m *Member73Meta[V]) Wait(ctx context.Context) (V, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if m.left.Load() {
		var zero V
		return zero, 0, ErrClosed73Meta
	}
	// Fast path: a value is already waiting.
	if v, e, ok := m.TryWait(); ok {
		return v, e, nil
	}
	if m.cohort.closed.Load() {
		var zero V
		return zero, 0, ErrClosed73Meta
	}
	for {
		select {
		case <-m.notify:
			// Fall through to the snapshot check below.
		case <-m.cohort.closedCh:
			// A value published just before Close must still be
			// deliverable; otherwise the waiter is done.
			if v, e, ok := m.TryWait(); ok {
				return v, e, nil
			}
			var zero V
			return zero, 0, ErrClosed73Meta
		case <-ctx.Done():
			var zero V
			return zero, 0, ctx.Err()
		}
		if v, e, ok := m.TryWait(); ok {
			return v, e, nil
		}
		// Spurious / coalesced wakeup: the notify token may have
		// been for an epoch this member already acked, or for
		// Close. If the cohort is closed and there is no new
		// value, stop instead of spinning on notify vs closed.
		if m.left.Load() {
			var zero V
			return zero, 0, ErrClosed73Meta
		}
		if m.cohort.closed.Load() {
			// Re-check once: a value may have landed with Close.
			if v, e, ok := m.TryWait(); ok {
				return v, e, nil
			}
			var zero V
			return zero, 0, ErrClosed73Meta
		}
		// Otherwise loop and wait for the next notify.
	}
}

// Next is Wait followed by Ack. It is appropriate when
// receive == processed for the caller, i.e. the caller does not
// need to hold the barrier open while it does further work with
// the value. For strict "process then ack" semantics, use
// Wait + Ack explicitly.
func (m *Member73Meta[V]) Next(ctx context.Context) (V, uint64, error) {
	v, e, err := m.Wait(ctx)
	if err != nil {
		var zero V
		return zero, 0, err
	}
	m.Ack(e)
	return v, e, nil
}

// Ack marks epoch as processed by this member. It returns true
// only if this Ack completed the barrier contribution for the
// currently pending epoch (i.e. it decremented the pending
// count). It returns false for stale epochs, duplicate acks,
// acks after Leave, and acks for an epoch whose barrier was
// already abandoned or completed.
//
// Ack with an epoch greater than the cohort's current epoch is a
// protocol violation and panics, because such an epoch can never
// have been observed via Snapshot / Wait.
func (m *Member73Meta[V]) Ack(epoch uint64) bool {
	if m.left.Load() {
		return false
	}
	c := m.cohort
	if epoch <= m.acked.v.Load() {
		return false
	}
	c.mu.Lock()
	var toClose chan struct{}
	acked := false
	if _, ok := c.members[m]; ok {
		if epoch == c.pendingEpoch {
			// Re-check under mu: a concurrent Ack for this same
			// member is serialized on mu, so the first one wins.
			if epoch > m.acked.v.Load() {
				m.acked.v.Store(epoch)
				c.pendingCount--
				if c.pendingCount < 0 {
					n := c.pendingCount
					c.mu.Unlock()
					panic(fmt.Sprintf("bigbuff: invariant violated: pendingCount=%d after Ack", n))
				}
				acked = true
				if c.pendingCount == 0 {
					toClose = c.pendingDone
					c.pendingEpoch = 0
					c.pendingDone = nil
				}
			}
		} else {
			// Stale or already-completed epoch: advance the
			// watermark so Wait does not return it again.
			// A future epoch is impossible without a bug.
			if epoch > c.epoch.v.Load() {
				cur := c.epoch.v.Load()
				c.mu.Unlock()
				panic(fmt.Sprintf("bigbuff: Ack(%d) > current epoch %d: protocol violation", epoch, cur))
			}
			if epoch > m.acked.v.Load() {
				m.acked.v.Store(epoch)
			}
		}
	}
	c.mu.Unlock()
	if toClose != nil {
		close(toClose)
	}
	return acked
}

// Leave departs the Cohort73Meta. It is idempotent.
//
// Leave is compensating: if this member had not yet acked the
// in-flight broadcast, Leave atomically decrements the pending
// barrier count, so the publisher never stalls waiting for a
// departed member. A member that joins after a broadcast was
// published has acked == pendingEpoch, so Leave does not
// decrement for it (it was never counted in a way that needs
// compensation, or rather it was counted but its watermark
// already covers the epoch; see Join).
//
// After Leave, Snapshot / Wait / Ack all become no-ops returning
// false / ErrClosed73Meta.
func (m *Member73Meta[V]) Leave() {
	if !m.left.CompareAndSwap(false, true) {
		return
	}
	c := m.cohort
	c.mu.Lock()
	var toClose chan struct{}
	if _, ok := c.members[m]; ok {
		delete(c.members, m)
		c.membersCount.v.Add(-1)
		if c.pendingEpoch != 0 && m.acked.v.Load() < c.pendingEpoch {
			c.pendingCount--
			if c.pendingCount < 0 {
				n := c.pendingCount
				c.mu.Unlock()
				panic(fmt.Sprintf("bigbuff: invariant violated: pendingCount=%d after Leave", n))
			}
			if c.pendingCount == 0 {
				toClose = c.pendingDone
				c.pendingEpoch = 0
				c.pendingDone = nil
			}
		}
	}
	c.mu.Unlock()
	if toClose != nil {
		close(toClose)
	}
}

// All returns an iterator over newly published values. Each value
// is yielded exactly once, in epoch order, until ctx is cancelled
// or the Cohort73Meta is closed.
//
// The iterator acks each value after the consumer has processed
// it: if yield returns true, the value is acked before waiting
// for the next one; if yield returns false (break), the last
// yielded value is acked before All returns. Breaking out of the
// loop therefore compensates the publisher exactly like an
// explicit Ack, and never stalls the barrier.
func (m *Member73Meta[V]) All(ctx context.Context) iter.Seq[V] {
	return func(yield func(V) bool) {
		var pending uint64
		var hasPending bool
		defer func() {
			if hasPending {
				m.Ack(pending)
			}
		}()
		for {
			v, e, err := m.Wait(ctx)
			if err != nil {
				return
			}
			pending = e
			hasPending = true
			if !yield(v) {
				return
			}
			m.Ack(e)
			hasPending = false
		}
	}
}
