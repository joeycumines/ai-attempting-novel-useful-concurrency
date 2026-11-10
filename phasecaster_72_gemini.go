// Source: 72-gemini transcript (bigbuff.go, first listing). Identifiers carry
// the 72Gemini suffix. As extracted the package did not compile (see
// bitmap_72_gemini.go) and its own test suite deadlocked in
// TestSeqAndSeqPhasedEarlyBreakCompensation. Integration repairs, each marked
// "Integration repair" at the site or described here:
//
//   - Ack() racing the broadcaster's slotSendingPhased -> slotAwaitingAck
//     transition poisoned the caster; it now yields and retries.
//   - The "late ack owed" marker was a status value erased by the next
//     broadcast's claim; it is now a flag bit (slotLateAck72Gemini) that
//     survives claims, and the broadcaster's post-send resolution
//     (resolveSend72Gemini) tolerates the flag being cleared concurrently.
//   - Close() during the acknowledgement wait could make BroadcastPhased
//     return (n, nil) with acknowledgements outstanding; it now returns
//     ErrClosed72Gemini.
//   - Seq() subscriptions never acknowledge, so a BroadcastPhased with a
//     Seq() consumer attached waited forever (the deadlock above). Seq()
//     subscriptions are now marked plain and receive phased broadcasts as
//     1-phase deliveries.
//
//   - The per-broadcast mask snapshot heap-allocated whenever the caster had
//     more than four banks (more than 256 slots); it is now a turnstile-
//     guarded buffer owned by the caster.
//   - Every broadcast, phased or not, heap-allocated the phase-gate variable
//     (declared outside the phased branch with its address stored). One-phase
//     Broadcast is now allocation-free; BroadcastPhased allocates its gate
//     channel and the cell holding it, as designed.
//
// Recorded, not changed: capacity is fixed at construction (ErrCapacity72Gemini
// beyond it); Close() waits for, and does not interrupt, a broadcast blocked
// on a receiver; calling Ack() for a value
// delivered by a non-phased broadcast poisons the caster by contract.

package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/bits"
	"runtime"
	"sync/atomic"
)

// Errors returned by PhaseCaster72Gemini operations.
var (
	ErrClosed72Gemini   = errors.New("bigbuff: caster is closed")
	ErrCapacity72Gemini = errors.New("bigbuff: subscriber capacity exhausted")
)

// Bit-packed layout for PhaseCaster72Gemini.coord (64-bit atomic word):
//
// +----------+--------+-------+--------------------+--------------------+--------------------+
// | Bit 63   | Bit 62 | Bit 61| Bits 60..40 (21b)  | Bits 39..20 (20b)  | Bits 19..0 (20b)   |
// | POISONED | CLOSED | PHASE |   EPOCH COUNTER    | IN-FLIGHT DELIVER  | ACTIVE SUBSCRIBERS |
// +----------+--------+-------+--------------------+--------------------+--------------------+
const (
	activeBits72Gemini   = 20
	inFlightBits72Gemini = 20
	epochBits72Gemini    = 21

	activeShift72Gemini   = 0
	inFlightShift72Gemini = 20
	epochShift72Gemini    = 40

	activeMask72Gemini   uint64 = (1 << activeBits72Gemini) - 1   // Max 1,048,575 subscribers
	inFlightMask72Gemini uint64 = (1 << inFlightBits72Gemini) - 1 // Max 1,048,575 in-flight deliveries
	epochMask72Gemini    uint64 = (1 << epochBits72Gemini) - 1

	activeUnit72Gemini   uint64 = 1 << activeShift72Gemini
	inFlightUnit72Gemini uint64 = 1 << inFlightShift72Gemini
	epochUnit72Gemini    uint64 = 1 << epochShift72Gemini

	bitPhase72Gemini    uint64 = 1 << 61 // 0 = Ping (Delivery), 1 = Pong (Quiescence Ack)
	bitClosed72Gemini   uint64 = 1 << 62 // Graceful shutdown flag
	bitPoisoned72Gemini uint64 = 1 << 63 // Deterministic fail-fast invariant violation flag
)

func unpackActive72Gemini(c uint64) int32 {
	return int32((c >> activeShift72Gemini) & activeMask72Gemini)
}
func unpackInFlight72Gemini(c uint64) int32 {
	return int32((c >> inFlightShift72Gemini) & inFlightMask72Gemini)
}
func unpackEpoch72Gemini(c uint64) uint32 {
	return uint32((c >> epochShift72Gemini) & epochMask72Gemini)
}
func unpackPhase72Gemini(c uint64) int { return int((c >> 61) & 1) }

// Per-slot atomic state word layout (64-bit):
//
// +-------------------------------+-------------------------------+------------------+
// | Bits 63..32 (32 bits)         | Bits 31..4 (28 bits)          | Bits 3..0 (4b)   |
// |      EPOCH TAG (uint32)       |   ABA GENERATION (28 bits)    |   SLOT STATUS    |
// +-------------------------------+-------------------------------+------------------+
//
// Integration repair: the status proper occupies bits 2..0; bit 3 is the
// slotLateAck72Gemini flag ("one late Ack() is still owed for a barrier the
// broadcaster abandoned"). As generated this was a status value
// (slotAckLate == 7) which the next broadcast's claim overwrote, so the late
// Ack() then found the slot in a sending state and poisoned the caster. As a
// flag it survives every status transition until Ack() consumes it or the
// slot is freed.
const (
	slotStatusMask72Gemini uint64 = 0x7
	slotLateAck72Gemini    uint64 = 0x8
	slotLowMask72Gemini    uint64 = slotStatusMask72Gemini | slotLateAck72Gemini
	slotGenShift72Gemini          = 4
	slotGenBits72Gemini           = 28
	slotGenMask72Gemini    uint64 = (1 << slotGenBits72Gemini) - 1
	slotEpochShift72Gemini        = 32

	slotFree72Gemini          uint64 = 0 // Unallocated in allocMask
	slotIdle72Gemini          uint64 = 1 // Subscribed and ready for next broadcast epoch
	slotSending72Gemini       uint64 = 2 // Committed to 1-phase unbuffered channel send
	slotSendingPhased72Gemini uint64 = 3 // Committed to 2-phase Ping delivery
	slotAwaitingAck72Gemini   uint64 = 4 // Received Ping delivery; owes Pong Ack() or Unsubscribe()
	slotCanceling72Gemini     uint64 = 5 // Unsubscribe() active mid-send; draining caller channel
	slotCancelAck72Gemini     uint64 = 6 // Broadcaster finished send/abort; handed slot to Unsubscribe()
)

func packSlot72Gemini(gen uint32, status uint64, epoch uint32) uint64 {
	return (uint64(epoch) << slotEpochShift72Gemini) |
		((uint64(gen) & slotGenMask72Gemini) << slotGenShift72Gemini) |
		(status & slotLowMask72Gemini)
}

func unpackSlotStatus72Gemini(s uint64) uint64 { return s & slotStatusMask72Gemini }
func unpackSlotGen72Gemini(s uint64) uint32 {
	return uint32((s >> slotGenShift72Gemini) & slotGenMask72Gemini)
}
func unpackSlotEpoch72Gemini(s uint64) uint32 { return uint32(s >> slotEpochShift72Gemini) }

func nextSlotGen72Gemini(gen uint32) uint32 {
	next := (gen + 1) & uint32(slotGenMask72Gemini)
	if next == 0 {
		next = 1
	}
	return next
}

const (
	slotsPerBank72Gemini = 64
	defaultBanks72Gemini = 4  // 256 slots default (16 KiB arena), configurable
	spinTries72Gemini    = 24 // Adaptive spin iterations before parking on slow receiver
)

// slot represents a single subscriber coordination cell padded to 128 bytes
// to eliminate false sharing across both 64B cache lines and 128B spatial prefetcher pairs.
type slot72Gemini[V any] struct {
	state      atomic.Uint64
	ch         chan V
	internalCh chan V
	// plain marks a subscription that never acknowledges (Seq): a phased
	// broadcast delivers to it as a 1-phase send. Written before the slot
	// becomes slotIdle72Gemini, read by the broadcaster only after it claimed the slot.
	plain bool
	_pad  [128 - 8 - 16 - 8]byte
}

// slotBank72Gemini manages 64 contiguous subscriber slots indexed via 64-bit hardware bitmaps.
type slotBank72Gemini[V any] struct {
	activeMask atomic.Uint64 // Set bits indicate subscribed, non-canceling slots
	_pad0      [120]byte
	allocMask  atomic.Uint64 // Set bits indicate allocated slots
	_pad1      [120]byte
	slots      [slotsPerBank72Gemini]slot72Gemini[V]
}

// Subscription72Gemini is a 16-byte value-type capability handle passed in CPU registers
// (0 B/op, 0 allocs/op) with a 28-bit ABA-immune generation tag.
type Subscription72Gemini[V any] struct {
	caster  *PhaseCaster72Gemini[V]
	slotIdx uint32
	gen     uint32
}

// Valid reports whether the subscription handle is non-zero.
func (s Subscription72Gemini[V]) Valid() bool {
	return s.caster != nil && s.gen != 0
}

// PhaseCaster72Gemini is a lock-free bit-packed synchronous barrier broadcaster and
// caller-owned unbuffered channel orchestrator with mid-flight cancellation compensation.
type PhaseCaster72Gemini[V any] struct {
	// Hot coordination word isolated on its own 128B cache line.
	coord atomic.Uint64
	_pad0 [120]byte

	// Turnstile serializing M:N broadcasters while allowing wait-free/lock-free subscriber churn.
	bcastTurnstile chan struct{}
	doneCh         chan struct{}

	// Monotonic full 64-bit epoch counter (only mutated inside broadcaster turnstile).
	fullEpoch atomic.Uint64
	_pad1     [120]byte

	// Two-phase Ping/Pong barrier gates (alternating by epoch parity).
	pongGates [2]atomic.Pointer[chan struct{}]

	// Lazily allocated select-composable epoch completion notification channel.
	epochNotify atomic.Pointer[chan struct{}]

	// Wait-free latest value snapshot (seqlock on !race, atomic pointer on -race).
	snapshot latestSnapshot72Gemini[V]
	_pad2    [64]byte

	// Diagnostic poison message stored before panicking on invariant violation.
	poisonMsg atomic.Pointer[string]

	// Pre-allocated slot banks for O(active) TZCNT/BLSR bit-scan traversal.
	banks []slotBank72Gemini[V]

	// maskScratch is the per-broadcast activeMask snapshot; turnstile-guarded.
	maskScratch []uint64
}

// NewPhaseCaster72Gemini initializes a PhaseCaster72Gemini supporting up to maxSubscribers concurrent
// subscribers (rounded up to a multiple of 64; defaults to 256 if maxSubscribers <= 0).
func NewPhaseCaster72Gemini[V any](maxSubscribers int) *PhaseCaster72Gemini[V] {
	if maxSubscribers <= 0 {
		maxSubscribers = defaultBanks72Gemini * slotsPerBank72Gemini
	}
	numBanks := (maxSubscribers + slotsPerBank72Gemini - 1) / slotsPerBank72Gemini
	if numBanks > int(activeMask72Gemini)/slotsPerBank72Gemini {
		numBanks = int(activeMask72Gemini) / slotsPerBank72Gemini
	}

	c := &PhaseCaster72Gemini[V]{
		bcastTurnstile: make(chan struct{}, 1),
		doneCh:         make(chan struct{}),
		banks:          make([]slotBank72Gemini[V], numBanks),
		maskScratch:    make([]uint64, numBanks),
	}
	c.bcastTurnstile <- struct{}{}

	for b := range c.banks {
		for i := range c.banks[b].slots {
			c.banks[b].slots[i].state.Store(packSlot72Gemini(1, slotFree72Gemini, 0))
			c.banks[b].slots[i].internalCh = make(chan V)
		}
	}
	return c
}

// poison atomically transitions the caster into an unrecoverable poisoned state,
// wakes all barriers so no waiter is left stranded, and panics with the violation reason.
func (c *PhaseCaster72Gemini[V]) poison(reason string) {
	msg := reason
	c.poisonMsg.CompareAndSwap(nil, &msg)
	for {
		cur := c.coord.Load()
		if cur&bitPoisoned72Gemini != 0 {
			break
		}
		if c.coord.CompareAndSwap(cur, cur|bitPoisoned72Gemini|bitClosed72Gemini) {
			break
		}
	}
	c.signalAllWaiters()
	panic(reason)
}

func (c *PhaseCaster72Gemini[V]) checkHealthy() uint64 {
	cur := c.coord.Load()
	if cur&bitPoisoned72Gemini != 0 {
		if msgPtr := c.poisonMsg.Load(); msgPtr != nil {
			panic(*msgPtr)
		}
		panic("bigbuff: operation on poisoned PhaseCaster")
	}
	return cur
}

func (c *PhaseCaster72Gemini[V]) signalAllWaiters() {
	for i := 0; i < 2; i++ {
		if p := c.pongGates[i].Swap(nil); p != nil {
			close(*p)
		}
	}
	if ep := c.epochNotify.Swap(nil); ep != nil {
		close(*ep)
	}
}

// StateSnapshot72Gemini represents an atomic point-in-time view of the coordination state word.
type StateSnapshot72Gemini struct {
	Active   int32
	InFlight int32
	Epoch    uint64
	Phase    int
	Closed   bool
	Poisoned bool
}

// Inspect atomically reads and unpacks the bit-packed coordination state word.
func (c *PhaseCaster72Gemini[V]) Inspect() StateSnapshot72Gemini {
	cur := c.checkHealthy()
	return StateSnapshot72Gemini{
		Active:   unpackActive72Gemini(cur),
		InFlight: unpackInFlight72Gemini(cur),
		Epoch:    c.fullEpoch.Load(),
		Phase:    unpackPhase72Gemini(cur),
		Closed:   (cur & bitClosed72Gemini) != 0,
		Poisoned: (cur & bitPoisoned72Gemini) != 0,
	}
}

// Latest performs a wait-free read of the most recently completed broadcast value and its epoch.
// Returns (zero, 0, false) if no broadcast has completed yet.
func (c *PhaseCaster72Gemini[V]) Latest() (V, uint64, bool) {
	c.checkHealthy()
	return c.snapshot.load()
}

// Done returns a channel that is closed when Close() is called on the PhaseCaster72Gemini.
func (c *PhaseCaster72Gemini[V]) Done() <-chan struct{} {
	c.checkHealthy()
	return c.doneCh
}

// EpochDone returns a select-composable channel that closes when the current or next
// broadcast epoch finishes quiescence. Channel allocation is lazy (0 allocs when unused).
func (c *PhaseCaster72Gemini[V]) EpochDone() <-chan struct{} {
	cur := c.checkHealthy()
	if cur&bitClosed72Gemini != 0 {
		return c.doneCh
	}
	for {
		if existing := c.epochNotify.Load(); existing != nil {
			return *existing
		}
		ch := make(chan struct{})
		if c.epochNotify.CompareAndSwap(nil, &ch) {
			if c.coord.Load()&bitClosed72Gemini != 0 {
				if swapped := c.epochNotify.Swap(nil); swapped != nil {
					close(*swapped)
				}
			}
			return ch
		}
	}
}

// Subscribe registers a caller-owned unbuffered channel (cap(ch) == 0) for synchronous
// barrier broadcast participation. Panics immediately if ch is nil or buffered (cap(ch) != 0).
// Returns a value-type Subscription72Gemini[V] handle with 0 heap allocations.
func (c *PhaseCaster72Gemini[V]) Subscribe(ch chan V) (Subscription72Gemini[V], error) {
	c.checkHealthy()
	if ch == nil {
		c.poison("bigbuff: invariant violation: Subscribe called with nil channel")
	}
	if cap(ch) != 0 {
		c.poison(fmt.Sprintf("bigbuff: invariant violation: caller channel must be unbuffered (cap == 0), got cap == %d", cap(ch)))
	}
	return c.subscribeWithChan(ch, false, false)
}

// SubscribeManaged allocates a slot using its pre-allocated unbuffered internal channel,
// returning both the value-type Subscription72Gemini[V] and the receive channel with 0 heap allocations.
func (c *PhaseCaster72Gemini[V]) SubscribeManaged() (Subscription72Gemini[V], <-chan V, error) {
	c.checkHealthy()
	return c.subscribeManaged(false)
}

func (c *PhaseCaster72Gemini[V]) subscribeManaged(plain bool) (Subscription72Gemini[V], <-chan V, error) {
	sub, err := c.subscribeWithChan(nil, true, plain)
	if err != nil {
		return Subscription72Gemini[V]{}, nil, err
	}
	bIdx := sub.slotIdx / slotsPerBank72Gemini
	sIdx := sub.slotIdx % slotsPerBank72Gemini
	return sub, c.banks[bIdx].slots[sIdx].internalCh, nil
}

func (c *PhaseCaster72Gemini[V]) subscribeWithChan(callerCh chan V, useInternal, plain bool) (Subscription72Gemini[V], error) {
	// Step 1: Reserve active count in coord word while checking closed/poisoned invariants.
	for {
		cur := c.checkHealthy()
		if cur&bitClosed72Gemini != 0 {
			return Subscription72Gemini[V]{}, ErrClosed72Gemini
		}
		act := unpackActive72Gemini(cur)
		if uint64(act+1) > activeMask72Gemini {
			return Subscription72Gemini[V]{}, ErrCapacity72Gemini
		}
		if c.coord.CompareAndSwap(cur, cur+activeUnit72Gemini) {
			break
		}
	}

	// Step 2: Claim a free slot across banks using hardware TZCNT on inverted allocMask.
	for bIdx := range c.banks {
		bank := &c.banks[bIdx]
		for {
			mask := bank.allocMask.Load()
			freeBits := ^mask
			if freeBits == 0 {
				break
			}
			sIdx := uint32(bits.TrailingZeros64(freeBits))
			bit := uint64(1) << sIdx
			if bank.allocMask.CompareAndSwap(mask, mask|bit) {
				sl := &bank.slots[sIdx]
				oldState := sl.state.Load()
				if unpackSlotStatus72Gemini(oldState) != slotFree72Gemini {
					c.subActive(1)
					c.poison("bigbuff: invariant violation: allocated slot not in slotFree state")
				}
				gen := unpackSlotGen72Gemini(oldState)
				if gen == 0 {
					gen = 1
				}
				if useInternal {
					sl.ch = sl.internalCh
				} else {
					sl.ch = callerCh
				}
				sl.plain = plain
				// Release store on sl.state establishes happens-before for sl.ch write.
				sl.state.Store(packSlot72Gemini(gen, slotIdle72Gemini, 0))
				bank.activeMask.Or(bit)

				globalIdx := uint32(bIdx*slotsPerBank72Gemini) + sIdx
				return Subscription72Gemini[V]{
					caster:  c,
					slotIdx: globalIdx,
					gen:     gen,
				}, nil
			}
		}
	}

	// Roll back active count reservation if all banks are full.
	c.subActive(1)
	return Subscription72Gemini[V]{}, ErrCapacity72Gemini
}

// Unsubscribe detaches the subscription from the PhaseCaster72Gemini.
// If a broadcast is currently mid-flight to this subscriber's channel, Unsubscribe
// executes a compensating non-blocking drain/handoff protocol so active broadcasters
// never stall, and guarantees that the broadcaster will never touch the channel again
// once Unsubscribe returns. Panics on double-Unsubscribe or stale token invariant violations.
func (s Subscription72Gemini[V]) Unsubscribe() {
	c := s.caster
	if c == nil || s.gen == 0 {
		panic("bigbuff: invariant violation: Unsubscribe called on zero Subscription")
	}
	c.checkHealthy()

	bIdx := s.slotIdx / slotsPerBank72Gemini
	sIdx := s.slotIdx % slotsPerBank72Gemini
	if int(bIdx) >= len(c.banks) {
		c.poison("bigbuff: invariant violation: out-of-bounds subscription slot index")
	}
	bank := &c.banks[bIdx]
	sl := &bank.slots[sIdx]
	bit := uint64(1) << sIdx
	nextGen := nextSlotGen72Gemini(s.gen)

	for {
		st := sl.state.Load()
		curGen := unpackSlotGen72Gemini(st)
		status := unpackSlotStatus72Gemini(st)
		epoch := unpackSlotEpoch72Gemini(st)

		if curGen != s.gen || status == slotFree72Gemini || status == slotCanceling72Gemini || status == slotCancelAck72Gemini {
			c.poison("bigbuff: invariant violation: double Unsubscribe or stale subscription token")
		}

		switch status {
		case slotIdle72Gemini:
			// Fast path: subscriber is between broadcasts (an owed late ack, if any, dies with the slot).
			// Transition directly to slotFree72Gemini before releasing allocMask.
			if sl.state.CompareAndSwap(st, packSlot72Gemini(nextGen, slotFree72Gemini, 0)) {
				bank.activeMask.And(^bit)
				sl.ch = nil
				bank.allocMask.And(^bit)
				c.subActive(1)
				return
			}

		case slotAwaitingAck72Gemini:
			// Subscriber already received the value in Phase 0 of BroadcastPhased,
			// and is now departing before calling Ack(). Compensate both active AND inFlight!
			if sl.state.CompareAndSwap(st, packSlot72Gemini(nextGen, slotFree72Gemini, 0)) {
				bank.activeMask.And(^bit)
				sl.ch = nil
				bank.allocMask.And(^bit)
				c.subActiveAndInFlight(1, 1, epoch)
				return
			}

		case slotSending72Gemini, slotSendingPhased72Gemini:
			// Broadcaster has committed to sending to sl.ch in the current epoch.
			// Read sl.ch before transitioning to slotCanceling72Gemini.
			ch := sl.ch
			if sl.state.CompareAndSwap(st, packSlot72Gemini(s.gen, slotCanceling72Gemini, epoch)) {
				// Hide from future broadcast snapshots immediately.
				bank.activeMask.And(^bit)

				// Compensating Mid-Flight Drain Loop:
				// Drain <-ch if the broadcaster is parked in ch <- v, or yield briefly if
				// the broadcaster is completing its post-send transition to slotCancelAck72Gemini.
				targetAck := packSlot72Gemini(s.gen, slotCancelAck72Gemini, epoch)
				for sl.state.Load() != targetAck {
					select {
					case <-ch:
					default:
						runtime.Gosched()
					}
				}

				// Broadcaster has stored slotCancelAck72Gemini and will never touch sl.ch again.
				sl.ch = nil
				sl.state.Store(packSlot72Gemini(nextGen, slotFree72Gemini, 0))
				bank.allocMask.And(^bit)
				c.subActive(1)
				return
			}
		}
	}
}

// Ack completes Phase 1 (Pong quiescence barrier) for a subscriber after processing a
// message received from a BroadcastPhased call. Panics if called when no phase ack is owed.
func (s Subscription72Gemini[V]) Ack() {
	c := s.caster
	if c == nil || s.gen == 0 {
		panic("bigbuff: invariant violation: Ack called on zero Subscription")
	}
	c.checkHealthy()

	bIdx := s.slotIdx / slotsPerBank72Gemini
	sIdx := s.slotIdx % slotsPerBank72Gemini
	if int(bIdx) >= len(c.banks) {
		c.poison("bigbuff: invariant violation: out-of-bounds subscription slot index")
	}
	sl := &c.banks[bIdx].slots[sIdx]

	for {
		st := sl.state.Load()
		if unpackSlotGen72Gemini(st) != s.gen {
			c.poison("bigbuff: invariant violation: Ack called on unsubscribed or stale token")
		}
		status := unpackSlotStatus72Gemini(st)
		epoch := unpackSlotEpoch72Gemini(st)

		if st&slotLateAck72Gemini != 0 {
			// Broadcaster's context timed out during Phase 1 and already reclaimed inFlight;
			// absorb exactly one late Ack(), leaving whatever the slot is doing now untouched.
			if sl.state.CompareAndSwap(st, st&^slotLateAck72Gemini) {
				return
			}
			continue
		}

		switch status {
		case slotAwaitingAck72Gemini:
			if sl.state.CompareAndSwap(st, packSlot72Gemini(s.gen, slotIdle72Gemini, epoch)) {
				c.decInFlight(1, epoch)
				return
			}
		case slotSendingPhased72Gemini:
			// Integration repair: the receiver has taken the value off the
			// channel but the broadcaster has not yet recorded the hand-off
			// (slotSendingPhased72Gemini -> slotAwaitingAck72Gemini). As generated this
			// window poisoned the caster; the transition is imminent, so
			// yield and retry.
			runtime.Gosched()
		default:
			c.poison("bigbuff: invariant violation: Ack called when not awaiting phase acknowledgment")
		}
	}
}

// Broadcast synchronously delivers v to all currently active subscribers over their
// unbuffered channels. Returns the number of subscribers that received v.
func (c *PhaseCaster72Gemini[V]) Broadcast(v V) (int, error) {
	return c.broadcastInternal(context.Background(), v, false, false)
}

// BroadcastContext synchronously delivers v to all currently active subscribers,
// aborting remaining slow deliveries if ctx is canceled mid-broadcast.
func (c *PhaseCaster72Gemini[V]) BroadcastContext(ctx context.Context, v V) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return c.broadcastInternal(ctx, v, ctx.Done() != nil, false)
}

// BroadcastPhased executes a Two-Phase Ping/Pong Barrier Broadcast:
//   - Phase 0 (Ping): Synchronously delivers v to all active subscribers' unbuffered channels.
//   - Phase 1 (Pong): Blocks until every subscriber that received v calls sub.Ack()
//     (or compensates via sub.Unsubscribe()).
func (c *PhaseCaster72Gemini[V]) BroadcastPhased(ctx context.Context, v V) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return c.broadcastInternal(ctx, v, ctx.Done() != nil, true)
}

func (c *PhaseCaster72Gemini[V]) broadcastInternal(ctx context.Context, v V, hasCtx bool, phased bool) (int, error) {
	c.checkHealthy()
	if hasCtx {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}

	// Acquire broadcaster turnstile.
	if hasCtx {
		select {
		case <-c.bcastTurnstile:
		case <-c.doneCh:
			c.checkHealthy()
			return 0, ErrClosed72Gemini
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	} else {
		select {
		case <-c.bcastTurnstile:
		case <-c.doneCh:
			c.checkHealthy()
			return 0, ErrClosed72Gemini
		}
	}
	defer func() {
		c.bcastTurnstile <- struct{}{}
	}()

	// Re-verify health and closed state inside turnstile.
	cur := c.checkHealthy()
	if cur&bitClosed72Gemini != 0 {
		return 0, ErrClosed72Gemini
	}
	if unpackInFlight72Gemini(cur) != 0 {
		c.poison(fmt.Sprintf("bigbuff: invariant violation: non-zero inFlight (%d) at broadcast start", unpackInFlight72Gemini(cur)))
	}

	// Snapshot active bitmaps across banks and count participating slots via POPCNT.
	// Integration repair: the snapshot buffer is owned by the caster and
	// guarded by the broadcast turnstile (exactly one broadcaster holds it).
	// As generated, casters with more than four banks allocated this slice
	// on every broadcast, contradicting the zero-allocation claim beyond 256
	// subscribers.
	masks := c.maskScratch

	totalTargets := 0
	for bIdx := range c.banks {
		m := c.banks[bIdx].activeMask.Load()
		masks[bIdx] = m
		totalTargets += bits.OnesCount64(m)
	}

	newEpoch64 := c.fullEpoch.Add(1)
	epoch32 := uint32(newEpoch64)
	if epoch32 == 0 {
		epoch32 = 1
	}
	parity := int(epoch32 & 1)

	// If phased, install the Pong barrier gate for this epoch parity before latching inFlight.
	// Integration repair: the gate variable is declared inside the branch.
	// As generated it was declared outside and its address stored, so escape
	// analysis heap-allocated it on every broadcast, including the one-phase
	// Broadcast whose documentation promises zero allocations.
	var pongCh chan struct{}
	if phased && totalTargets > 0 {
		gate := make(chan struct{})
		c.pongGates[parity].Store(&gate)
		pongCh = gate
	}

	// Latch inFlight = totalTargets, advance 21-bit epoch field, and set Phase = 0 (Ping) in coord.
	for {
		cur = c.checkHealthy()
		if cur&bitClosed72Gemini != 0 {
			if phased && totalTargets > 0 {
				c.pongGates[parity].Store(nil)
			}
			return 0, ErrClosed72Gemini
		}
		next := (cur &^ (inFlightMask72Gemini<<inFlightShift72Gemini | epochMask72Gemini<<epochShift72Gemini | bitPhase72Gemini)) |
			(uint64(totalTargets) << inFlightShift72Gemini) |
			((newEpoch64 & epochMask72Gemini) << epochShift72Gemini)
		if c.coord.CompareAndSwap(cur, next) {
			break
		}
	}

	if totalTargets == 0 {
		c.publishEpochCompletion(v, newEpoch64)
		return 0, nil
	}

	sendStatus := slotSending72Gemini
	if phased {
		sendStatus = slotSendingPhased72Gemini
	}

	deliveredCount := 0
	var abortedErr error
	unclaimedInFlight := uint32(0)

	// Phase 0 (Ping): Traverse only set bits in each bank using TZCNT and BLSR (mask &= mask - 1).
	for bIdx, mask := range masks {
		bank := &c.banks[bIdx]
		for mask != 0 {
			sIdx := uint32(bits.TrailingZeros64(mask))
			mask &= mask - 1
			sl := &bank.slots[sIdx]

			// If broadcaster context already aborted earlier in this loop, skip remaining slots.
			if abortedErr != nil {
				unclaimedInFlight++
				continue
			}

			// Claim slot for sending in this epoch.
			var claimedGen uint32
			claimed := false
			for {
				st := sl.state.Load()
				status := unpackSlotStatus72Gemini(st)
				if status != slotIdle72Gemini {
					// Subscriber unsubscribed between mask snapshot and slot visit.
					break
				}
				gen := unpackSlotGen72Gemini(st)
				if sl.state.CompareAndSwap(st, packSlot72Gemini(gen, sendStatus|(st&slotLateAck72Gemini), epoch32)) {
					claimedGen = gen
					claimed = true
					break
				}
			}

			if !claimed {
				unclaimedInFlight++
				continue
			}

			// Slot is now in slotSending72Gemini / slotSendingPhased72Gemini; sl.ch cannot be cleared until we release it.
			ch := sl.ch
			sent := false

			// Tier 1: Ultra-fast non-blocking send (compiled to runtime.selectnbsend, bypassing selectgo).
			select {
			case ch <- v:
				sent = true
			default:
			}

			// Tier 2: Brief adaptive spin for receivers cycling through their loop body.
			if !sent {
				for spin := 0; spin < spinTries72Gemini; spin++ {
					if unpackSlotStatus72Gemini(sl.state.Load()) == slotCanceling72Gemini {
						break
					}
					select {
					case ch <- v:
						sent = true
					default:
						runtime.Gosched()
					}
					if sent {
						break
					}
				}
			}

			// Tier 3: Blocking rendezvous (woken either by receiver reading <-ch or Unsubscribe draining <-ch).
			if !sent && unpackSlotStatus72Gemini(sl.state.Load()) != slotCanceling72Gemini {
				if !hasCtx {
					ch <- v
					sent = true
				} else {
					select {
					case ch <- v:
						sent = true
					case <-ctx.Done():
						abortedErr = ctx.Err()
					}
				}
			}

			// Post-send slot state resolution. A subscription that never
			// acknowledges (plain) is resolved as a 1-phase delivery even
			// inside a phased broadcast.
			if sent && phased && !sl.plain {
				// Transition to slotAwaitingAck72Gemini for Phase 1 (Pong).
				// If this succeeds, the subscriber now owns 1 unit of inFlight debt.
				if resolveSend72Gemini(sl, claimedGen, slotAwaitingAck72Gemini, epoch32) {
					deliveredCount++
				} else {
					// Unsubscribe() set slotCanceling72Gemini concurrently while we were sending.
					unclaimedInFlight++
				}
			} else {
				// 1-Phase delivery OR failed/aborted send: return slot to slotIdle72Gemini
				// (or acknowledge the hand-off to a concurrent Unsubscribe()).
				if resolveSend72Gemini(sl, claimedGen, slotIdle72Gemini, epoch32) && sent {
					deliveredCount++
				}
				unclaimedInFlight++
			}
		}
	}

	// Batch-decrement all inFlight units that completed (or were skipped) in Phase 0.
	if unclaimedInFlight > 0 {
		c.decInFlight(unclaimedInFlight, epoch32)
	}

	// Phase 1 (Pong): If phased, wait for remaining subscribers in slotAwaitingAck72Gemini to call Ack() or Unsubscribe().
	if phased {
		// Flip Phase bit to 1 (Pong) if any subscribers are still in-flight.
		for {
			cur = c.checkHealthy()
			if unpackInFlight72Gemini(cur) == 0 || (cur&bitPhase72Gemini) != 0 {
				break
			}
			if c.coord.CompareAndSwap(cur, cur|bitPhase72Gemini) {
				break
			}
		}

		if unpackInFlight72Gemini(c.coord.Load()) > 0 {
			if !hasCtx {
				select {
				case <-pongCh:
				case <-c.doneCh:
					c.checkHealthy()
					abortedErr = ErrClosed72Gemini
				}
			} else {
				select {
				case <-pongCh:
				case <-c.doneCh:
					c.checkHealthy()
					abortedErr = ErrClosed72Gemini
				case <-ctx.Done():
					abortedErr = ctx.Err()
				}
			}

			// Integration repair: the pong gate is closed either by the last
			// Ack()/Unsubscribe() (inFlight == 0) or by Close()/poison waking
			// all waiters. As generated, the latter could win the select
			// against doneCh and the broadcast reported success with
			// acknowledgements still outstanding.
			if abortedErr == nil && unpackInFlight72Gemini(c.coord.Load()) > 0 {
				c.checkHealthy()
				abortedErr = ErrClosed72Gemini
			}

			// If aborted during Phase 1 wait, reclaim any remaining slotAwaitingAck72Gemini slots
			// (slotIdle72Gemini + slotLateAck72Gemini) so inFlight returns cleanly to 0 before we exit the turnstile.
			if unpackInFlight72Gemini(c.coord.Load()) > 0 {
				reclaimed := uint32(0)
				for bIdx, mask := range masks {
					bank := &c.banks[bIdx]
					for mask != 0 {
						sIdx := uint32(bits.TrailingZeros64(mask))
						mask &= mask - 1
						sl := &bank.slots[sIdx]
						for {
							st := sl.state.Load()
							if unpackSlotStatus72Gemini(st) != slotAwaitingAck72Gemini || unpackSlotEpoch72Gemini(st) != epoch32 {
								break
							}
							gen := unpackSlotGen72Gemini(st)
							if sl.state.CompareAndSwap(st, packSlot72Gemini(gen, slotIdle72Gemini|slotLateAck72Gemini, epoch32)) {
								reclaimed++
								break
							}
						}
					}
				}
				if reclaimed > 0 {
					c.decInFlight(reclaimed, epoch32)
				}
				// Wait briefly if a concurrent Ack()/Unsubscribe() claimed the last slot and is about to decrement inFlight.
				for unpackInFlight72Gemini(c.coord.Load()) != 0 {
					runtime.Gosched()
				}
			}
		}
		c.pongGates[parity].Store(nil)
	}

	// Verify strict post-broadcast invariant: inFlight must be 0.
	if rem := unpackInFlight72Gemini(c.coord.Load()); rem != 0 {
		c.poison(fmt.Sprintf("bigbuff: invariant violation: non-zero inFlight (%d) at broadcast end", rem))
	}

	if abortedErr == nil {
		c.publishEpochCompletion(v, newEpoch64)
	}
	return deliveredCount, abortedErr
}

// resolveSend72Gemini moves a slot this broadcaster claimed out of its sending
// status. While a slot is in a sending status only two other parties may
// touch its state word: Ack(), which clears slotLateAck72Gemini, and Unsubscribe(),
// which moves it to slotCanceling72Gemini and then waits for slotCancelAck72Gemini. It
// reports false when the slot was handed to Unsubscribe().
func resolveSend72Gemini[V any](sl *slot72Gemini[V], gen uint32, to uint64, epoch32 uint32) bool {
	for {
		st := sl.state.Load()
		if unpackSlotStatus72Gemini(st) == slotCanceling72Gemini {
			sl.state.Store(packSlot72Gemini(gen, slotCancelAck72Gemini, epoch32))
			return false
		}
		if sl.state.CompareAndSwap(st, packSlot72Gemini(gen, to|(st&slotLateAck72Gemini), epoch32)) {
			return true
		}
	}
}

func (c *PhaseCaster72Gemini[V]) publishEpochCompletion(v V, epoch uint64) {
	c.snapshot.store(v, epoch)
	if notifyPtr := c.epochNotify.Swap(nil); notifyPtr != nil {
		close(*notifyPtr)
	}
}

func (c *PhaseCaster72Gemini[V]) subActive(delta uint32) {
	sub := uint64(delta) * activeUnit72Gemini
	for {
		cur := c.checkHealthy()
		act := unpackActive72Gemini(cur)
		if int32(delta) > act {
			c.poison(fmt.Sprintf("bigbuff: invariant violation: active subscriber count underflow (%d - %d)", act, delta))
		}
		if c.coord.CompareAndSwap(cur, cur-sub) {
			return
		}
	}
}

func (c *PhaseCaster72Gemini[V]) decInFlight(delta uint32, epoch32 uint32) {
	sub := uint64(delta) * inFlightUnit72Gemini
	for {
		cur := c.checkHealthy()
		inf := unpackInFlight72Gemini(cur)
		if int32(delta) > inf {
			c.poison(fmt.Sprintf("bigbuff: invariant violation: inFlight counter underflow (%d - %d)", inf, delta))
		}
		next := cur - sub
		if c.coord.CompareAndSwap(cur, next) {
			if unpackInFlight72Gemini(next) == 0 {
				parity := int(epoch32 & 1)
				if gatePtr := c.pongGates[parity].Swap(nil); gatePtr != nil {
					close(*gatePtr)
				}
			}
			return
		}
	}
}

func (c *PhaseCaster72Gemini[V]) subActiveAndInFlight(actDelta, infDelta uint32, epoch32 uint32) {
	sub := uint64(actDelta)*activeUnit72Gemini + uint64(infDelta)*inFlightUnit72Gemini
	for {
		cur := c.checkHealthy()
		act := unpackActive72Gemini(cur)
		inf := unpackInFlight72Gemini(cur)
		if int32(actDelta) > act || int32(infDelta) > inf {
			c.poison(fmt.Sprintf("bigbuff: invariant violation: dual counter underflow (active=%d-%d, inFlight=%d-%d)", act, actDelta, inf, infDelta))
		}
		next := cur - sub
		if c.coord.CompareAndSwap(cur, next) {
			if unpackInFlight72Gemini(next) == 0 {
				parity := int(epoch32 & 1)
				if gatePtr := c.pongGates[parity].Swap(nil); gatePtr != nil {
					close(*gatePtr)
				}
			}
			return
		}
	}
}

// Seq returns a Go 1.23+ push iterator (iter.Seq[V]) that yields broadcast items
// using the slot's pre-allocated unbuffered channel. Breaking out of the loop or
// canceling ctx mid-broadcast automatically triggers compensating Unsubscribe().
func (c *PhaseCaster72Gemini[V]) Seq(ctx context.Context) iter.Seq[V] {
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(V) bool) {
		sub, ch, err := c.subscribeManaged(true)
		if err != nil {
			return
		}
		defer sub.Unsubscribe()

		ctxDone := ctx.Done()
		casterDone := c.doneCh
		for {
			select {
			case v := <-ch:
				if !yield(v) {
					return
				}
			case <-ctxDone:
				return
			case <-casterDone:
				c.checkHealthy()
				return
			}
		}
	}
}

// SeqPhased returns a Go 1.23+ push iterator (iter.Seq2[uint64, V]) that participates
// in Two-Phase Ping/Pong broadcasts: each loop body execution runs inside Phase 1 (Pong),
// automatically calling sub.Ack() when the iteration body finishes, or compensating via
// sub.Unsubscribe() if the caller breaks out of the loop or cancels ctx.
func (c *PhaseCaster72Gemini[V]) SeqPhased(ctx context.Context) iter.Seq2[uint64, V] {
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(uint64, V) bool) {
		sub, ch, err := c.SubscribeManaged()
		if err != nil {
			return
		}
		defer sub.Unsubscribe()

		ctxDone := ctx.Done()
		casterDone := c.doneCh
		for {
			select {
			case v := <-ch:
				ep := c.fullEpoch.Load()
				cont := yield(ep, v)
				if !cont {
					// Defer sub.Unsubscribe() will atomically compensate both active and inFlight!
					return
				}
				sub.Ack()
			case <-ctxDone:
				return
			case <-casterDone:
				c.checkHealthy()
				return
			}
		}
	}
}

// Close gracefully shuts down the PhaseCaster72Gemini after any currently executing broadcast
// completes, waking all Seq iterators and EpochDone waiters. Close is idempotent.
func (c *PhaseCaster72Gemini[V]) Close() error {
	c.checkHealthy()
	for {
		cur := c.checkHealthy()
		if cur&bitClosed72Gemini != 0 {
			return nil
		}
		if c.coord.CompareAndSwap(cur, cur|bitClosed72Gemini) {
			close(c.doneCh)
			c.signalAllWaiters()
			return nil
		}
	}
}
