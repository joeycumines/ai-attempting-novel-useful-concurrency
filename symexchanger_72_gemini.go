// Source: 72-gemini transcript (exchanger.go). Identifiers carry the 72Gemini
// suffix. Integration repairs, each marked "Integration repair" at the site:
//
//   - Lost pairing: two callers that both scanned before either published
//     parked in different slots forever (Exchange, BeginExchange).
//   - Stale wake token: the wake token was sent after the fulfilment was
//     published and could land in a recycled slot (tryMatch72Gemini).
//   - Immediate tickets busy-looped for a second free slot and never
//     returned when none existed (ExchangeTicket72Gemini).
//
// Added: Done(), so ticket holders can observe Close in a select. Recorded,
// not changed: the "active offerers" bits of the control word are never
// written; BeginExchange heap-allocates its ticket.

package bigbuff

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
)

// 64-bit packed state layout for exchangeSlot72Gemini.state:
//
// +-------------------------------+-------------------------------+------------------+
// | Bits 63..32 (32 bits)         | Bits 31..4 (28 bits)          | Bits 3..0 (4b)   |
// |     MATCH EPOCH (uint32)      |   ABA GENERATION (28 bits)    |  EXCHANGE STATE  |
// +-------------------------------+-------------------------------+------------------+
const (
	exStateMask72Gemini uint64 = 0xF
	exGenShift72Gemini         = 4
	exGenBits72Gemini          = 28
	exGenMask72Gemini   uint64 = (1 << exGenBits72Gemini) - 1

	exFree72Gemini       uint64 = 0 // Slot is unallocated and available for an Offerer
	exPublishing72Gemini uint64 = 1 // Offerer won slot reservation and is writing itemA
	exWaiting72Gemini    uint64 = 2 // Offerer published itemA; waiting for Matcher or Abort
	exClaimed72Gemini    uint64 = 3 // Matcher won single linearization CAS; writing itemB
	exFulfilled72Gemini  uint64 = 4 // Matcher published itemB and signaled wakeCh
	exAborted72Gemini    uint64 = 5 // Offerer won rollback CAS; cleaning up itemA
)

func packExState72Gemini(gen uint32, status uint64) uint64 {
	return ((uint64(gen) & exGenMask72Gemini) << exGenShift72Gemini) | (status & exStateMask72Gemini)
}

func unpackExStatus72Gemini(s uint64) uint64 { return s & exStateMask72Gemini }
func unpackExGen72Gemini(s uint64) uint32 {
	return uint32((s >> exGenShift72Gemini) & exGenMask72Gemini)
}

func nextExGen72Gemini(gen uint32) uint32 {
	next := (gen + 1) & uint32(exGenMask72Gemini)
	if next == 0 {
		next = 1
	}
	return next
}

const (
	defaultArenaSlots72Gemini = 32
	exSpinIterations72Gemini  = 48
)

// exchangeSlot72Gemini is a 128-byte cache-line-isolated symmetric rendezvous cell.
// All reads and writes to itemA and itemB are strictly ordered by release-acquire
// transitions on state, making it 100% compliant with -race and zero-alloc on hot paths.
type exchangeSlot72Gemini[V any] struct {
	state  atomic.Uint64
	wakeCh chan struct{} // Pre-allocated 1-buffered wakeup signal for parked Offerer
	itemA  V
	itemB  V
	_pad   [64]byte
}

// ExchangeTicket72Gemini represents an in-flight rendezvous offer that can be composed
// directly inside a caller's native Go select statement via Ready(), Commit(), and Abort().
//
// Integration repair: a ticket that matched immediately inside BeginExchange
// carries the partner's value inline (immediate / peer). As generated it
// parked the value in a second arena slot found by an unbounded busy loop,
// which never terminates when no slot is free (always, with a one-slot arena).
type ExchangeTicket72Gemini[V any] struct {
	ex        *SymExchanger72Gemini[V]
	peer      V
	slotIdx   uint32
	gen       uint32
	done      atomic.Uint32 // 0 = active, 1 = resolved via Commit() or Abort()
	immediate bool
}

// SymExchanger72Gemini is a contention-eliminating symmetric rendezvous exchange primitive
// with atomic cancellation rollback and wait-bounded commit completion.
type SymExchanger72Gemini[V any] struct {
	// Bit 0: closed, Bit 1: poisoned, Bits 63..2: active offerers count.
	ctl     atomic.Uint64
	_pad0   [120]byte
	probeID atomic.Uint64
	_pad1   [120]byte

	doneCh    chan struct{}
	poisonMsg atomic.Pointer[string]
	slots     []exchangeSlot72Gemini[V]
}

const (
	exBitClosed72Gemini   uint64 = 1 << 0
	exBitPoisoned72Gemini uint64 = 1 << 1
	exActiveUnit72Gemini  uint64 = 1 << 2
)

// NewSymExchanger72Gemini creates a symmetric rendezvous arena with the specified number of
// collision-elimination slots (rounded up to a power of two; defaults to 32).
func NewSymExchanger72Gemini[V any](numSlots int) *SymExchanger72Gemini[V] {
	if numSlots <= 0 {
		numSlots = defaultArenaSlots72Gemini
	}
	capSlots := 1
	for capSlots < numSlots {
		capSlots <<= 1
	}
	ex := &SymExchanger72Gemini[V]{
		doneCh: make(chan struct{}),
		slots:  make([]exchangeSlot72Gemini[V], capSlots),
	}
	for i := range ex.slots {
		ex.slots[i].state.Store(packExState72Gemini(1, exFree72Gemini))
		ex.slots[i].wakeCh = make(chan struct{}, 1)
	}
	return ex
}

func (ex *SymExchanger72Gemini[V]) checkHealthy() uint64 {
	c := ex.ctl.Load()
	if c&exBitPoisoned72Gemini != 0 {
		if msg := ex.poisonMsg.Load(); msg != nil {
			panic(*msg)
		}
		panic("bigbuff: operation on poisoned SymExchanger")
	}
	return c
}

func (ex *SymExchanger72Gemini[V]) poison(reason string) {
	msg := reason
	ex.poisonMsg.CompareAndSwap(nil, &msg)
	for {
		c := ex.ctl.Load()
		if c&exBitPoisoned72Gemini != 0 {
			break
		}
		if ex.ctl.CompareAndSwap(c, c|exBitPoisoned72Gemini|exBitClosed72Gemini) {
			break
		}
	}
	select {
	case <-ex.doneCh:
	default:
		close(ex.doneCh)
	}
	panic(reason)
}

// tryMatch attempts to become the Matcher of the offer waiting in sl.
//
// Integration repair: the wake token is sent while the slot is still
// exClaimed72Gemini, before exFulfilled72Gemini is stored. As generated the order was the
// reverse, so a spinning Offerer could consume the fulfilment and free the
// slot before the token was sent; the token then woke the slot's next
// occupant, which entered a wait-for-fulfilment spin that ignores both
// context and Close (and made a ticket's Ready() fire without a match).
func (ex *SymExchanger72Gemini[V]) tryMatch(sl *exchangeSlot72Gemini[V], v V) (V, bool) {
	st := sl.state.Load()
	if unpackExStatus72Gemini(st) != exWaiting72Gemini {
		var zero V
		return zero, false
	}
	gen := unpackExGen72Gemini(st)
	// Single linearization point for the match.
	if !sl.state.CompareAndSwap(st, packExState72Gemini(gen, exClaimed72Gemini)) {
		var zero V
		return zero, false
	}
	peerVal := sl.itemA
	sl.itemB = v
	select {
	case sl.wakeCh <- struct{}{}:
	default:
	}
	// Release store publishes itemB to the Offerer.
	sl.state.Store(packExState72Gemini(gen, exFulfilled72Gemini))
	return peerVal, true
}

// tryPublish attempts to reserve sl and publish v as a waiting offer.
func (ex *SymExchanger72Gemini[V]) tryPublish(sl *exchangeSlot72Gemini[V], v V) (uint32, bool) {
	st := sl.state.Load()
	if unpackExStatus72Gemini(st) != exFree72Gemini {
		return 0, false
	}
	gen := unpackExGen72Gemini(st)
	if !sl.state.CompareAndSwap(st, packExState72Gemini(gen, exPublishing72Gemini)) {
		return 0, false
	}
	// Drain any stale wake token before publishing.
	select {
	case <-sl.wakeCh:
	default:
	}
	sl.itemA = v
	sl.state.Store(packExState72Gemini(gen, exWaiting72Gemini))
	return gen, true
}

// otherWaiting reports the index of a slot other than own holding a waiting
// offer, or -1.
func (ex *SymExchanger72Gemini[V]) otherWaiting(own, start uint32) int {
	mask := uint32(len(ex.slots) - 1)
	for i := uint32(0); i <= mask; i++ {
		idx := (start + i) & mask
		if idx == own {
			continue
		}
		if unpackExStatus72Gemini(ex.slots[idx].state.Load()) == exWaiting72Gemini {
			return int(idx)
		}
	}
	return -1
}

// Exchange pairs the caller with another concurrent caller in the arena, atomically
// swapping v with the partner's value and returning the partner's value.
// If ctx is canceled before a partner claims the offer, Exchange atomically rolls back
// the offer so v is never orphaned or duplicated.
//
// Integration repair (lost pairing): as generated, a caller scanned for a
// waiting offer, then published its own and parked. Two callers that both
// scanned before either published therefore parked in different slots and
// never met. After publishing, the caller now rescans; on sight of another
// waiting offer it withdraws its own (or, if it was claimed meanwhile,
// completes as Offerer) and tries to claim the other. Of any two published
// offers the later publisher's rescan must observe the earlier one, so two
// offers can no longer both stay parked.
func (ex *SymExchanger72Gemini[V]) Exchange(ctx context.Context, v V) (V, error) {
	var zero V
	if ctx == nil {
		ctx = context.Background()
	}
	ex.checkHealthy()
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	mask := uint32(len(ex.slots) - 1)
	probe := uint32(ex.probeID.Add(1))
	start := probe & mask

	for round := uint32(0); ; round++ {
		if ex.checkHealthy()&exBitClosed72Gemini != 0 {
			return zero, ErrClosed72Gemini
		}
		if err := ctx.Err(); err != nil {
			return zero, err
		}

		// Phase 1: Attempt to find a waiting Offerer and match as Matcher.
		for i := uint32(0); i <= mask; i++ {
			if peerVal, ok := ex.tryMatch(&ex.slots[(start+i)&mask], v); ok {
				return peerVal, nil
			}
		}

		// Phase 2: No waiting Offerer found; attempt to claim a free slot as Offerer.
		for i := uint32(0); i <= mask; i++ {
			idx := (start + i) & mask
			sl := &ex.slots[idx]
			gen, ok := ex.tryPublish(sl, v)
			if !ok {
				continue
			}

			// Phase 3: rescan for an offer published concurrently with ours.
			if other := ex.otherWaiting(idx, start); other >= 0 {
				if peerVal, matched := ex.resolveCancelOrMatch(sl, gen); matched {
					return peerVal, nil
				}
				// Our offer is withdrawn; v is ours again.
				if peerVal, ok := ex.tryMatch(&ex.slots[other], v); ok {
					return peerVal, nil
				}
				break // lost the claim: back off and start over
			}

			// Spin briefly waiting for a Matcher.
			for spin := 0; spin < exSpinIterations72Gemini; spin++ {
				cur := sl.state.Load()
				status := unpackExStatus72Gemini(cur)
				if status == exFulfilled72Gemini {
					return ex.completeFulfilledSlot(sl, gen), nil
				}
				if status == exClaimed72Gemini {
					return ex.waitClaimedToFulfilled(sl, gen), nil
				}
				cpuPause72Gemini()
			}

			// Park on the slot's wake channel. A token is only ever sent
			// while this offer is exClaimed72Gemini, so a wake implies a match.
			select {
			case <-sl.wakeCh:
				return ex.waitClaimedToFulfilled(sl, gen), nil
			case <-ctx.Done():
				if peerVal, matched := ex.resolveCancelOrMatch(sl, gen); matched {
					return peerVal, nil
				}
				return zero, ctx.Err()
			case <-ex.doneCh:
				ex.checkHealthy()
				if peerVal, matched := ex.resolveCancelOrMatch(sl, gen); matched {
					return peerVal, nil
				}
				return zero, ErrClosed72Gemini
			}
		}

		// Arena momentarily full, or a withdrawn offer lost its claim. The
		// number of yields differs per caller (probe is unique per call) so
		// that two callers which withdrew for each other do not retry in
		// lockstep.
		for k := uint32(0); k <= (probe+round)&3; k++ {
			runtime.Gosched()
		}
	}
}

// BeginExchange initiates a non-blocking rendezvous offer for native Go select composition.
// If another peer is already waiting in the arena, BeginExchange matches immediately and
// returns a ticket whose Ready() channel is already closed.
// Returns (nil, ErrCapacity72Gemini) if all arena slots are momentarily occupied.
func (ex *SymExchanger72Gemini[V]) BeginExchange(v V) (*ExchangeTicket72Gemini[V], error) {
	mask := uint32(len(ex.slots) - 1)
	probe := uint32(ex.probeID.Add(1))
	start := probe & mask

	for round := uint32(0); ; round++ {
		if ex.checkHealthy()&exBitClosed72Gemini != 0 {
			return nil, ErrClosed72Gemini
		}

		// Phase 1: Attempt to find a waiting Offerer.
		for i := uint32(0); i <= mask; i++ {
			if peerVal, ok := ex.tryMatch(&ex.slots[(start+i)&mask], v); ok {
				return &ExchangeTicket72Gemini[V]{ex: ex, peer: peerVal, immediate: true}, nil
			}
		}

		// Phase 2: Attempt to publish a ticket offer in a free slot.
		published := false
		for i := uint32(0); i <= mask; i++ {
			idx := (start + i) & mask
			sl := &ex.slots[idx]
			gen, ok := ex.tryPublish(sl, v)
			if !ok {
				continue
			}
			published = true

			// Phase 3: rescan (see Exchange).
			other := ex.otherWaiting(idx, start)
			if other < 0 {
				return &ExchangeTicket72Gemini[V]{ex: ex, slotIdx: idx, gen: gen}, nil
			}
			if peerVal, matched := ex.resolveCancelOrMatch(sl, gen); matched {
				return &ExchangeTicket72Gemini[V]{ex: ex, peer: peerVal, immediate: true}, nil
			}
			if peerVal, ok := ex.tryMatch(&ex.slots[other], v); ok {
				return &ExchangeTicket72Gemini[V]{ex: ex, peer: peerVal, immediate: true}, nil
			}
			break
		}
		if !published {
			return nil, ErrCapacity72Gemini
		}
		for k := uint32(0); k <= (probe+round)&3; k++ {
			runtime.Gosched()
		}
	}
}

var closedReadyChan72Gemini = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// Done returns a channel closed when the exchanger is closed. Ticket holders
// select on it next to Ready(): Close does not make Ready() fire.
func (ex *SymExchanger72Gemini[V]) Done() <-chan struct{} {
	return ex.doneCh
}

// Ready returns a channel selectable inside a native Go select statement that
// becomes readable when a peer claims and fulfills this exchange ticket.
func (t *ExchangeTicket72Gemini[V]) Ready() <-chan struct{} {
	if t == nil || t.ex == nil {
		panic("bigbuff: invariant violation: Ready called on nil ExchangeTicket")
	}
	t.ex.checkHealthy()
	if t.done.Load() != 0 {
		t.ex.poison("bigbuff: invariant violation: Ready called on already resolved ExchangeTicket")
	}
	if t.immediate {
		return closedReadyChan72Gemini
	}
	return t.ex.slots[t.slotIdx].wakeCh
}

// Commit harvests the partner's value after <-ticket.Ready() has fired in a select statement.
// Panics deterministically if called more than once or if called when the ticket was not matched.
func (t *ExchangeTicket72Gemini[V]) Commit() V {
	if t == nil || t.ex == nil {
		panic("bigbuff: invariant violation: Commit called on nil ExchangeTicket")
	}
	t.ex.checkHealthy()
	if !t.done.CompareAndSwap(0, 1) {
		t.ex.poison("bigbuff: invariant violation: double Commit/Abort on ExchangeTicket")
	}
	if t.immediate {
		return t.peer
	}
	sl := &t.ex.slots[t.slotIdx]
	st := sl.state.Load()
	if unpackExGen72Gemini(st) != t.gen {
		t.ex.poison("bigbuff: invariant violation: stale generation on ExchangeTicket.Commit")
	}
	status := unpackExStatus72Gemini(st)
	if status != exClaimed72Gemini && status != exFulfilled72Gemini {
		t.ex.poison(fmt.Sprintf("bigbuff: invariant violation: Commit called on unmatched ticket in state %d", status))
	}
	return t.ex.waitClaimedToFulfilled(sl, t.gen)
}

// Abort attempts to atomically roll back an in-flight ExchangeTicket72Gemini when another branch
// of the caller's select statement wins.
//   - If the rollback CAS (exWaiting72Gemini -> exAborted72Gemini) succeeds, Abort returns (zero, false) and
//     the caller retains full ownership of their offered item.
//   - If a peer claimed the slot nanoseconds before Abort(), Abort completes the handoff
//     and returns (peerVal, true) so no matched item is ever orphaned!
func (t *ExchangeTicket72Gemini[V]) Abort() (V, bool) {
	if t == nil || t.ex == nil {
		panic("bigbuff: invariant violation: Abort called on nil ExchangeTicket")
	}
	t.ex.checkHealthy()
	if !t.done.CompareAndSwap(0, 1) {
		t.ex.poison("bigbuff: invariant violation: double Commit/Abort on ExchangeTicket")
	}
	if t.immediate {
		return t.peer, true
	}
	sl := &t.ex.slots[t.slotIdx]
	return t.ex.resolveCancelOrMatch(sl, t.gen)
}

func (ex *SymExchanger72Gemini[V]) completeFulfilledSlot(sl *exchangeSlot72Gemini[V], gen uint32) V {
	var zero V
	peerVal := sl.itemB
	sl.itemA = zero
	sl.itemB = zero
	select {
	case <-sl.wakeCh:
	default:
	}
	sl.state.Store(packExState72Gemini(nextExGen72Gemini(gen), exFree72Gemini))
	return peerVal
}

func (ex *SymExchanger72Gemini[V]) waitClaimedToFulfilled(sl *exchangeSlot72Gemini[V], gen uint32) V {
	target := packExState72Gemini(gen, exFulfilled72Gemini)
	for sl.state.Load() != target {
		cpuPause72Gemini()
		runtime.Gosched()
	}
	return ex.completeFulfilledSlot(sl, gen)
}

func (ex *SymExchanger72Gemini[V]) resolveCancelOrMatch(sl *exchangeSlot72Gemini[V], gen uint32) (V, bool) {
	var zero V
	waitState := packExState72Gemini(gen, exWaiting72Gemini)
	if sl.state.CompareAndSwap(waitState, packExState72Gemini(gen, exAborted72Gemini)) {
		// Rollback succeeded! Nobody claimed itemA.
		sl.itemA = zero
		select {
		case <-sl.wakeCh:
		default:
		}
		sl.state.Store(packExState72Gemini(nextExGen72Gemini(gen), exFree72Gemini))
		return zero, false
	}

	// A Matcher already won CAS(exWaiting72Gemini -> exClaimed72Gemini)! Complete the bounded handoff.
	st := sl.state.Load()
	if unpackExGen72Gemini(st) != gen {
		ex.poison("bigbuff: invariant violation: ABA generation mismatch during cancel resolution")
	}
	return ex.waitClaimedToFulfilled(sl, gen), true
}

// Close gracefully closes the SymExchanger72Gemini, waking any parked Offerers so they can
// roll back cleanly.
func (ex *SymExchanger72Gemini[V]) Close() error {
	ex.checkHealthy()
	for {
		c := ex.checkHealthy()
		if c&exBitClosed72Gemini != 0 {
			return nil
		}
		if ex.ctl.CompareAndSwap(c, c|exBitClosed72Gemini) {
			close(ex.doneCh)
			return nil
		}
	}
}
