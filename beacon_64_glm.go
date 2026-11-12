/*
   Copyright 2026

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

// Package bigbuff — Beacon64GLM: an epoch-gated, seqlock-replicated broadcast ring.
//
// # Target scenario (fitness-for-purpose envelope)
//
// Beacon64GLM targets the "state horizon + event stream" regime:
//
//   - Producers: 1-4 goroutines publishing a high-frequency stream
//     (tick data, metrics, game state frames, control-plane snapshots).
//   - Watchers: 1-10000+ goroutines that must observe the stream — each
//     either keeping up value-by-value, or coalescing to the latest when
//     it falls behind — with bounded-history replay for late joiners.
//   - Contention profile: EXTREME fanout asymmetry. The write side is
//     low-concurrency; the read side is massive, churning (watchers
//     attach/detach constantly), and read-mostly.
//   - Delivery semantics: per-watcher ordered delivery with explicit
//     loss accounting (Skip64GLM) when a watcher laps the ring; never a torn
//     value; producers are NEVER blocked by consumers (publish is
//     wait-free regardless of N).
//
// # Why the standard runtime loses here
//
//   - chan fanout (one chan per subscriber): O(N) channel-lock
//     acquisitions and up to N semaphore wakeups per value; each hchan is
//     a heap object with its own lock and wait queue; slow receivers
//     either block the sender (unbuffered) or force unbounded buffering.
//     The publisher's cache footprint grows with N.
//   - sync.RWMutex-guarded state: every RLock dirties the shared
//     readerCount line — 1000 readers ping-pong one line with the writer.
//   - sync.Map / atomic.Pointer snapshot: latest-value only, no replay,
//     no ordering, no loss accounting.
//   - close(chan): broadcast but single-use and payload-less.
//
// # Mechanical sympathy thesis
//
//  1. PUBLISHER TOUCHES TWO LINES: the claim word (one 64-bit packed
//     cursor|epoch) and the target slot64GLM. Everything scales with the
//     producer count, not the watcher count.
//  2. SELF-SERVE READERS: each watcher keeps a private cursor in its own
//     stack frame and performs plain (fenced) loads against slots. There
//     is NO shared read-modify-write on the read path — zero MESI
//     invalidation traffic from fanout. A watcher's reads leave cache
//     lines in Shared state.
//  3. SEQLOCK SLOTS: each slot64GLM is a padded {seq, val} cell; readers do
//     the classic seqlock optimistic read (seq-even check, copy, re-check)
//     which is branch-light and needs no per-reader registration.
//  4. CLOSE-AND-SWAP GATES: blocking watchers wait on a generation gate64GLM —
//     a *chan struct{} that a publisher swaps for a fresh one and closes.
//     ONE close wakes ALL watchers of that generation (the runtime's
//     closechan releases the whole wait list in one pass under one lock).
//     Watchers coalesce arbitrarily many missed values into a single
//     gate64GLM wait. Wakeup cost is O(1) close per publish, amortized O(1)
//     per watcher, and it is natively select-compatible (no callbacks).
//  5. ZERO ALLOCATIONS on the publish and read hot paths when no
//     watcher is parked: publish allocates nothing (the gate64GLM swap is
//     skipped while parked==0) and readers never register, so the only
//     allocations in the lifecycle are the ring (at New) and, while
//     watchers are parked, one small gate64GLM object per publish. Gates are
//     deliberately NOT pooled: a waiter released by a close may still be
//     running its release bookkeeping, so reusing a gate64GLM would let a
//     stale waiter corrupt a freshly re-armed one.
//
// # Synchronization protocol (the whole contract)
//
//   - claim word: [epoch:32 | cursor:32]. cursor = number of values ever
//     published (the next slot64GLM to write is cursor&mask). epoch =
//     cursor>>log2(size), maintained through 32-bit cursor wrap so slot64GLM
//     validation survives 4 billion publishes.
//   - slot64GLM: [seq:64] where seq/2 = number of times THIS slot64GLM was written,
//     and seq odd means a writer is mid-write. A slot64GLM with seq s (even)
//     currently holds the value published at absolute sequence
//     (s/2-1)*size + (idx+size) ... equivalently: slot64GLM idx holds value
//     seq-1 iff idx == (seq-1)&mask. Readers validate exactly this.
//   - gates: publisher arms gate64GLM(epoch) then closes the previous gate64GLM.
//     A gate64GLM armed for epoch E fires (is closed) when the FIRST value of
//     epoch E (or any later epoch) is published.
//
// # Race detector
//
// The seqlock protocol is invisible to ThreadSanitizer by design (it has
// no happens-before edges on the payload). This package therefore ships
// TWO reader/writer slot64GLM protocols:
//
//   - fence builds (!race): plain loads/stores bracketed by load/store
//     fences (single instructions on amd64/arm64 via .s files).
//   - race builds: fully atomic slot64GLM accesses (atomic.Uint64 seq +
//     word-typed atomic payload loads), giving TSan the edges it needs.
//
// Both share all control logic; only slotread_*.go differs.
package bigbuff

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ---------------------------------------------------------------------------
// Fences: fenceAcquire64GLM / fenceRelease64GLM are defined per build:
//   fence_amd64.s   (!race && amd64)   : LFENCE / SFENCE
//   fence_arm64.s   (!race && arm64)   : DMB ISHLD / DMB ISH
//   fence_generic.go(!race && other)   : atomic load/store scratch word
//   slotread_race.go(race)             : atomic scratch word (TSan edges are
//                                        carried by the atomic slot64GLM protocol)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Slot layout
// ---------------------------------------------------------------------------

// slotHeaderSize64GLM keeps the seq word on its own cache line, away from the
// payload, so reader re-loads of seq don't invalidate the line the writer
// is streaming the payload into (and vice versa).
const slotHeaderSize64GLM = 64

// padConst64GLM is a fixed 63-byte tail pad. Generic payloads vary in size, and
// Go can't express a per-instantiation constant expression in a struct, so
// we pay up to 63 bytes of slack per slot64GLM. For the telemetry-grade payload
// sizes this package targets (8-64 bytes), total slot64GLM cost stays within
// 1-2 lines, and the seq header NEVER shares a line with a payload.
const padConst64GLM = slotHeaderSize64GLM - 1

// slot64GLM is one cell of the ring: a seqlock header line followed by the value.
type slot64GLM[V any] struct {
	// seq names the value in THIS slot64GLM. An even seq s means the slot64GLM
	// holds the value of lap s/2 (the first lap is 1), whose absolute
	// sequence number is (s/2-1)*size + idx + 1 (mod 2^32); 0 means never
	// written. An odd seq 2k-1 means a writer holds the slot and is storing
	// the value of lap k. See storeClaimed for why this is not a count of
	// writes, which is what the response had.
	seq atomic.Uint64
	_   [slotHeaderSize64GLM - 8]byte // header line: seq alone
	val V
	// tail padding so consecutive slots' headers don't share a line:
	_ [padConst64GLM]byte
}

// ---------------------------------------------------------------------------
// Gates: close-and-swap wakeup generations
// ---------------------------------------------------------------------------

// gate64GLM is a wakeup generation. A gate64GLM armed at generation gen is closed when
// a later value is published (or the Beacon64GLM closes); all waiters parked on it
// observe the close in one channel close.
//
// Gates are NEVER reused. Recycling was tried and removed: a waiter released
// by close(ch) may execute its bookkeeping only after the publisher observes
// it as done, so a pooled gate64GLM can be re-armed while a stale waiter still
// touches it (observed by the race detector). A closed gate64GLM is simply
// dropped for the GC. Gate allocation happens only while at least one
// watcher is parked, so the publish hot path with no watchers allocates
// nothing at all.
type gate64GLM struct {
	ch  chan struct{}
	gen uint32
}

// ---------------------------------------------------------------------------
// Beacon64GLM
// ---------------------------------------------------------------------------

// Beacon64GLM is an epoch-gated, seqlock-replicated broadcast ring. Construct
// with New; the zero value is unusable.
type Beacon64GLM[V any] struct {
	// claim packs [epoch:32 | cursor:32]. cursor = total values ever
	// published (mod 2^32); the next slot64GLM to write is cursor&mask.
	// epoch = cursor>>log2(size) (mod 2^32) — used for slot64GLM validation
	// across cursor wrap and for gate64GLM arming.
	claim atomic.Uint64

	// wake is the current gate64GLM; publishers swap-then-close under pubMu.
	wake atomic.Pointer[gate64GLM]

	slots []slot64GLM[V]
	mask  uint32
	log2  uint32

	// pubMu serializes the claim-CAS winners' gate64GLM swap between multiple
	// producers. It is ONLY taken after the value is fully written to its
	// slot64GLM (slot64GLM seq already even), so it never gates payload visibility;
	// it only orders gate64GLM arming. Single-producer workloads never block.
	pubMu sync.Mutex

	closed atomic.Bool
	// parked is the global count of watchers blocked inside waitGate.
	// Publishers use it as a zero-cost fast path: with no parked watchers
	// there is nobody to wake, so no gate64GLM swap/close allocation at all.
	parked atomic.Int32

	// closingGate is closed once by Close to wake every blocked watcher.
	closingGate chan struct{}

	// pad the control words away from the slots slice header
	_ [40]byte
}

// New returns a Beacon64GLM retaining the last `capacity` values (rounded up to
// a power of two, minimum 2).
func NewBeacon64GLM[V any](capacity int) *Beacon64GLM[V] {
	if capacity < 2 {
		capacity = 2
	}
	log2 := uint32(0)
	for 1<<log2 < capacity {
		log2++
	}
	size := uint32(1) << log2
	b := &Beacon64GLM[V]{
		slots:       make([]slot64GLM[V], size),
		mask:        size - 1,
		log2:        log2,
		closingGate: make(chan struct{}),
	}
	b.wake.Store(&gate64GLM{ch: make(chan struct{}), gen: 0})
	return b
}

// ErrClosed64GLM is returned by Publish after Close; watchers see it as the
// terminal ok=false.
var ErrClosed64GLM = errors.New("bigbuff: beacon closed")

// Skip64GLM reports how many values a watcher missed because it lapped the ring.
// It is returned alongside the NEXT successfully read (newest) value, or as
// the terminal state of a NonBlock poll.
type Skip64GLM uint32

func (s Skip64GLM) Error() string {
	if s == 1 {
		return "bigbuff: skipped 1 value"
	}
	// small manual itoa to keep the package dependency-free and alloc-free
	var buf [10]byte
	n := uint32(s)
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return "bigbuff: skipped " + string(buf[i:]) + " values"
}

// pack64GLM combines epoch (hi) and cursor (lo).
func pack64GLM(epoch, cursor uint32) uint64 {
	return uint64(epoch)<<32 | uint64(cursor)
}

// unpack64GLM splits the claim word.
func unpack64GLM(w uint64) (epoch, cursor uint32) {
	return uint32(w >> 32), uint32(w)
}

// Publish appends value to the ring, overwriting the oldest retained value
// if the history is full, and wakes all blocked watchers whose target
// generation is covered. It never waits for a consumer. It retries under
// producer-vs-producer claim contention, and it can wait for another producer
// that is mid-write in the same slot, which happens only when one of the two
// has been lapped by a whole ring between its claim and its write (see
// storeClaimed); a value lapped in that way is not stored. Returns the absolute sequence number assigned to the value,
// or ErrClosed64GLM after Close.
func (b *Beacon64GLM[V]) Publish(value V) (seq uint32, err error) {
	if b.closed.Load() {
		return 0, ErrClosed64GLM
	}
	for attempts := 0; ; attempts++ {
		if attempts > 1<<20 { // wait-free guard against pathological livelock
			runtime.Gosched()
			attempts = 0
		}
		old := b.claim.Load()
		if b.closed.Load() {
			return 0, ErrClosed64GLM
		}
		_, cur := unpack64GLM(old)
		next := cur + 1
		if !b.claim.CompareAndSwap(old, pack64GLM(next>>b.log2, next)) {
			continue // producer contention only; retry
		}
		// We own absolute sequence `next` (first value = 1; 0 = empty).
		b.storeClaimed(next, value)

		// Wake watchers (only when someone is parked): swap in a fresh gate64GLM
		// and close the old one, releasing ALL parked waiters in ONE channel
		// close. A released waiter re-polls the claim word: it either
		// consumes values or parks on the new gate64GLM. Gate gen only orders
		// multi-producer arm races (a producer must never close a gate64GLM
		// that a LATER publish already replaced), so gen = the sequence
		// this publish completed, not an epoch.
		b.armGate(next)
		return next, nil
	}
}

// storeClaimed writes value into the slot of the absolute sequence next, which
// the caller has claimed and nobody else can claim.
//
// seqlock write: odd -> write payload -> even. The payload write is fenced so
// no watcher can observe seq-even with a stale payload.
//
// Repair (run 09 pass). The response wrote the slot with two unconditional
// seq.Add(1) calls, which makes the slot's counter a count of writes, and
// slotHolds64GLM turns that count back into a sequence on the assumption that
// the writes to one slot arrive in claim order. Between the claim and the write
// a producer can be descheduled, and with more producers than CPUs it is: if
// the others publish a whole ring meanwhile, the producer that claimed
// next+size writes this slot first. Its value was then labelled as sequence
// next, so a watcher was handed a value a ring ahead of its cursor, and the
// late producer's write then replaced the newer value and was labelled as
// sequence next+size, so a watcher was handed a stale value as a new one. If
// the two writes overlapped, the counter went odd, even, odd, even while both
// wrote the payload, and a torn value could pass the seqlock check. The
// response's own TestMultiProducerStress reports it (2 rounds in 90 on two
// CPUs).
//
// The counter is now the sequence itself, not a count: a writer takes the slot
// with a compare-and-swap from an even value to its own odd one, so two
// writers never hold a slot together, and it stores the even value that names
// its own sequence, whatever order the writers arrive in. A writer that finds
// a value at or after its own already in the slot has been lapped before it
// wrote, and leaves the slot alone: the value is one the ring had overwritten,
// which every watcher already accounts for as skipped (the claim word says the
// sequence is more than a ring behind). The cost is that a producer can now
// wait for another producer that is mid-write in the same slot, which happens
// only when one has been lapped by a whole ring. A producer still never waits
// for a watcher.
func (b *Beacon64GLM[V]) storeClaimed(next uint32, value V) {
	idx := (next - 1) & b.mask
	sp := &b.slots[idx]
	size := uint32(len(b.slots))
	for spins := 0; ; spins++ {
		s := sp.seq.Load()
		if s&1 == 1 {
			// Another producer is writing this slot. It is a whole ring
			// away from this one, so one of the two was descheduled.
			if spins > 64 {
				runtime.Gosched()
			}
			continue
		}
		// k is the slot's write number that stands for sequence next: the
		// first lap is 1. It is taken forward from the occupant's so that the
		// 64-bit counter never repeats, even when the 32-bit sequence wraps.
		k := uint64((next-1)>>b.log2) + 1
		if held, ok := slotHolds64GLM(idx, uint32(s), size); ok {
			ahead := int32(next - held)
			if ahead <= 0 {
				return // lapped before writing; the newer value stays
			}
			k = s/2 + uint64(uint32(ahead)>>b.log2)
		}
		if !sp.seq.CompareAndSwap(s, 2*k-1) { // odd: this writer owns the slot
			continue
		}
		storeSlotVal64GLM(sp, value) // build-tag dispatch (fast: plain+fence, race: atomic)
		fenceRelease64GLM()          // payload visible BEFORE seq goes even
		sp.seq.Store(2 * k)          // even: the slot holds sequence next
		return
	}
}

// storeSlotVal64GLM / loadSlotVal64GLM / slotSeqEven build-tag dispatch to the fast
// (plain+fence) or race-safe (atomic) slot64GLM protocol.
func storeSlotVal64GLM[V any](sp *slot64GLM[V], v V) { storeSlotValImpl64GLM(sp, v) }

// slotHolds64GLM returns the absolute sequence number currently held by slot64GLM idx
// given its even seq value s, or (ok=false) if s is odd (mid-write).
// Write k (1-based, k>=1) to slot64GLM idx has absolute sequence (k-1)*size+idx
// ... in fact writes go to slots in strict rotation starting at 0, so the
// k-th write to slot64GLM idx has absolute sequence (k-1)*size + idx? NO:
// absolute sequence n goes to slot64GLM n&mask. Slot idx receives sequences
// idx, idx+size, idx+2*size, ... The k-th write (1-based) to slot64GLM idx has
// absolute sequence idx + (k-1)*size. With s = 2k: held sequence =
// idx + (s/2 - 1) * size. (mod 2^32).
// SEQUENCE CONVENTION (one place, one truth):
//
// The FIRST published value gets absolute sequence 1 (sequence 0 means
// "nothing ever published"). Value with absolute sequence n lands in slot64GLM
// (n-1) & mask; a slot64GLM's k-th write (1-based) therefore carries sequence
// (k-1)*size + idx + 1 where idx is the 0-based slot64GLM index. All helpers
// below follow this convention.

// slotHolds64GLM returns the absolute sequence of the value held by slot64GLM idx
// when its (even) seq counter is s, or ok=false when mid-write/never hit.
func slotHolds64GLM(idx, s, size uint32) (uint32, bool) {
	if s&1 == 1 || s == 0 {
		return 0, false
	}
	k := s / 2 // 1-based write count to this slot64GLM
	return (k-1)*size + idx + 1, true
}

// armGate swaps in a gate64GLM for `nextEpoch` (the epoch AFTER the one the
// just-published value belongs to) and closes the previous gate64GLM — which
// releases every waiter parked for values in the current epoch or earlier
// in ONE channel close. Gate invariant: b.wake always holds an OPEN gate64GLM
// with the highest armed epoch; arming a gate64GLM ALWAYS closes its
// predecessor, so a waiter parked on any gate64GLM G is released as soon as a
// value in G's epoch or later is published (the publisher of the FIRST
// value of epoch E arms E+1 and closes the gate64GLM covering <= E).
func (b *Beacon64GLM[V]) armGate(nextGen uint32) {
	// FAST PATH: no parked watchers anywhere -> nobody to wake, and no gate64GLM
	// allocation either. The waiter re-checks the claim word AFTER
	// registering as parked (see waitGate), so a publish that races the
	// registration below either sees parked>0 here and closes the gate64GLM, or
	// is observed by the waiter's post-registration re-check.
	if b.parked.Load() == 0 {
		return
	}
	b.pubMu.Lock()
	old := b.wake.Load()
	if old == nil {
		// Close() stored nil between our closed check and here; its gate64GLM
		// close already releases every parked waiter. Nothing to arm.
		b.pubMu.Unlock()
		return
	}
	if old.gen >= nextGen {
		b.pubMu.Unlock()
		return
	}
	g := &gate64GLM{ch: make(chan struct{}), gen: nextGen}
	if !b.wake.CompareAndSwap(old, g) {
		// Lost a race to another producer arming an even newer gate64GLM; our
		// value's wakeup is covered by whichever gate64GLM won. g is unreferenced
		// and recyclable by the GC.
		b.pubMu.Unlock()
		return
	}
	// Close `old`: every waiter parked on old.ch wakes and re-polls the
	// ring; those whose cursor is satisfied proceed, the rest load the new
	// gate64GLM and re-block. `old` is never referenced again by the Beacon64GLM —
	// just-released waiters may still hold it momentarily, so it is dropped
	// for the GC rather than reused.
	close(old.ch)
	b.pubMu.Unlock()
}

// ---------------------------------------------------------------------------
// Watcher64GLM: self-serve cursor
// ---------------------------------------------------------------------------

// Watcher64GLM is a private, non-registered cursor over the Beacon64GLM's history.
// A Watcher64GLM belongs to exactly one goroutine; it is never touched by the
// producer or by other watchers.
type Watcher64GLM[V any] struct {
	b      *Beacon64GLM[V]
	cursor uint32 // absolute sequence of the NEXT value this watcher reads
	// no gen field: epoch arithmetic is derived on demand
	_ [4]byte
}

// Watch returns a Watcher64GLM positioned at the OLDEST retained value: full
// replay of all history. With cursor total values published and a ring of
// size S, the oldest retained value has sequence max(1, total-S+1).
func (b *Beacon64GLM[V]) Watch() *Watcher64GLM[V] {
	_, cur := unpack64GLM(b.claim.Load())
	hist := uint32(len(b.slots))
	w := &Watcher64GLM[V]{b: b}
	if cur > hist {
		w.cursor = cur - hist + 1 // cur values exist; keep the last hist
	}
	return w
}

// WatchLatest returns a Watcher64GLM positioned after the newest value: it sees
// only values published from this call onward.
func (b *Beacon64GLM[V]) WatchLatest() *Watcher64GLM[V] {
	_, cur := unpack64GLM(b.claim.Load())
	return &Watcher64GLM[V]{b: b, cursor: cur + 1}
}

// Cursor returns the watcher's position (absolute sequence of next read).
func (w *Watcher64GLM[V]) Cursor() uint32 { return w.cursor }

// Mode64GLM selects delivery semantics of Next.
type Mode64GLM int

const (
	// ModeBlock64GLM: deliver every value in order; block until the next value,
	// ctx.Done, or Beacon64GLM close. A watcher that laps gets a Skip64GLM event.
	ModeBlock64GLM Mode64GLM = iota
	// ModeLatest64GLM: coalesce lag — if the watcher laps, skip straight to the
	// newest value, reporting the skipped count with it.
	ModeLatest64GLM
	// ModeNonBlock64GLM: never wait. If a value is available return it; else
	// return ok=false with the lag amount (0 if caught up).
	ModeNonBlock64GLM
)

// Next advances the watcher by one value under the mode's semantics.
//
// In ModeBlock64GLM: blocks; returns (v, 0, true) per value in order; if the
// watcher lapped the ring, returns (zero, Skip64GLM, true) ONCE per lap event
// (the caller should call Next again to read the oldest retained value);
// returns (zero, 0, false) on close or ctx cancellation (a watcher may
// still drain retained history after Close by calling Next until it
// reports caught-up/closed).
//
// In ModeLatest64GLM: never reports a lap as a separate event; lag coalesces
// into (newest v, skipped count, true).
//
// In ModeNonBlock64GLM: if a value is available at the cursor, return it; if
// the watcher lapped, return (zero, lag, false); if merely waiting, return
// (zero, 0, false).
//
// skip semantics: for ModeBlock64GLM, a nonzero skip IS the event (ok=true);
// for ModeLatest64GLM, skip accompanies the coalesced newest value; for
// ModeNonBlock64GLM with lag, skip is the lag and ok=false.
func (w *Watcher64GLM[V]) Next(mode Mode64GLM, ctx context.Context) (v V, skip Skip64GLM, ok bool) {
	b := w.b
	// Normalize the "0 == read from the very first value" sentinel into an
	// absolute sequence once, so all arithmetic below is uniform. (0 is the
	// only sequence that never exists, so it is safe as a sentinel.)
	if w.cursor == 0 {
		w.cursor = 1
	}
	for {
		// Re-read claim each iteration: gives us a monotonic-ish view.
		_, cur := unpack64GLM(b.claim.Load())

		// Values are sequences 1..cur (cur = newest sequence AND count of
		// values ever published; cursor is always >= 1 after the sentinel
		// normalization above). Unread values = [cursor, cur]; caught up
		// exactly when cursor == cur+1, i.e. delta == 0xFFFFFFFF.
		delta := cur - w.cursor
		if delta != 0xFFFFFFFF && delta < math.MaxUint32/2 {
			// Behind: unread count = delta+1 (>= 1).
			hist := uint32(len(b.slots))
			if delta+1 > hist {
				// Lapped: sequences [w.cursor, cur-hist] were overwritten.
				oldest := cur - hist + 1 // oldest retained sequence
				lap := oldest - w.cursor // count of lost values
				w.cursor = oldest
				skip = Skip64GLM(lap)
				if mode == ModeBlock64GLM {
					// The contract for ModeBlock64GLM is a dedicated lap event:
					// (zero, Skip64GLM(lap), true) delivered exactly once; the
					// caller calls Next again to read the oldest retained
					// value.
					var zero V
					return zero, skip, true
				}
			}
			// Read the value at w.cursor (retry while torn). (For ModeBlock64GLM a
			// lap was already reported as a dedicated event above, so the
			// skip count here is always zero.)
			if v2, _, ok2 := b.readSlotAt(w.cursor); ok2 {
				w.cursor++
				return v2, skip, true
			}
			// torn or overwritten mid-read: retry (bounded: writers are
			// wait-free, so the slot64GLM stabilizes quickly).
			runtime.Gosched()
			continue
		}

		// Cursor is at (or beyond) the published frontier: no value now.
		switch mode {
		case ModeNonBlock64GLM:
			return v, 0, false
		}
		// Blocking modes: gate64GLM wait.
		if !b.waitGate(&w.cursor, ctx) {
			if b.closed.Load() {
				return v, 0, false
			}
			return v, 0, false // ctx done
		}
	}
}

// readSlotAt reads the value with absolute sequence seq, retrying torn
// seqlock reads a bounded number of times. Returns ok=false if the slot64GLM's
// current occupant is NOT seq (overwritten) — the caller's lapping logic
// handles that case via the claim-word check.
func (b *Beacon64GLM[V]) readSlotAt(seq uint32) (V, Skip64GLM, bool) {
	idx := (seq - 1) & b.mask
	sp := &b.slots[idx]
	for attempts := 0; attempts < 1<<16; attempts++ {
		s := loadSlotSeq64GLM(sp) // atomic load; plain+fence on fast builds
		if s&1 == 1 {
			runtime.Gosched() // writer mid-write; resolve soon (wait-free writer)
			continue
		}
		var v V
		loadSlotVal64GLM(sp, &v)
		fenceAcquire64GLM()
		s2 := loadSlotSeq64GLM(sp)
		if s2 != s {
			var zero V
			return zero, 0, false // torn; retry
		}
		// Stable read. Validate that the slot64GLM's CURRENT occupant is exactly
		// `seq`: the value with sequence n lives in slot64GLM (n-1)&mask and the
		// slot64GLM's write counter after k writes is 2k, so the occupant's
		// sequence is (k-1)*size+idx+1 with k=s/2. A mismatch means our
		// value was overwritten (a lap) — the caller re-checks claim and
		// applies lapping. NOTE: torn-retry above must come FIRST; a torn
		// read also produces a mismatched occupant, and treating it as a
		// lap would double-count skips.
		size := uint32(len(b.slots))
		if held, valid := slotHolds64GLM(idx, uint32(s), size); !valid || held != seq {
			var zero V
			return zero, 0, false // lapped (or torn with equal s — impossible)
		}
		return v, 0, true
	}
	// Unreachable in practice (writers are wait-free); treat as lap.
	var zero V
	return zero, 0, false
}

// waitGate blocks until a value beyond *cursor is published, the Beacon64GLM
// closes, or ctx is done. Returns true when values may now be available
// (caller re-polls), false when the caller should return (closed/ctx).
func (b *Beacon64GLM[V]) waitGate(cursor *uint32, ctx context.Context) bool {
	for {
		_, cur := unpack64GLM(b.claim.Load())
		// Caught up == every published sequence 1..cur has been read, i.e.
		// the watcher's next-read cursor is cur+1 (cursor >= 1 always,
		// after Next's sentinel normalization).
		if cur != *cursor-1 {
			return true // value(s) available; re-poll
		}
		if b.closed.Load() {
			return false
		}
		g := b.wake.Load()
		if g == nil {
			// Beacon64GLM closed concurrently with wake teardown.
			return false
		}
		// The live gate64GLM is closed by the NEXT publish, which necessarily
		// produces a value beyond cursor-1 (the only way claim can advance
		// past cur). Park on it. Register as parked, then RE-CHECK the claim
		// word before selecting: a publish that lands between the caught-up
		// check above and the registration below would otherwise close a gate64GLM
		// we are not watching and leave us parked forever (lost wakeup). The
		// re-check is ordered after the parked increment, so any publish
		// that could have missed our registration is seen here instead.
		b.parked.Add(1)
		if _, cur2 := unpack64GLM(b.claim.Load()); cur2 != *cursor-1 {
			b.parked.Add(-1)
			return true
		}
		if ctx == nil || ctx.Done() == nil {
			<-g.ch
			b.parked.Add(-1)
			return true
		}
		select {
		case <-g.ch:
			b.parked.Add(-1)
			return true
		case <-b.closingGate:
			b.parked.Add(-1)
			return b.claimAdvanceOrClosed(cursor)
		case <-ctx.Done():
			b.parked.Add(-1)
			return false
		}
	}
}

// claimAdvanceOrClosed reports whether waitGate's caller should re-poll
// (values arrived) or terminate (Beacon64GLM closed with nothing new).
func (b *Beacon64GLM[V]) claimAdvanceOrClosed(cursor *uint32) bool {
	_, cur := unpack64GLM(b.claim.Load())
	if cur != *cursor-1 {
		return true
	}
	return !b.closed.Load()
}

// Close terminates the Beacon64GLM: all blocked watchers wake and observe
// ok=false on their next Next; Publish returns ErrClosed64GLM; retained history
// remains readable via Next until the watcher catches up. Idempotent,
// goroutine-safe, and leaks nothing.
func (b *Beacon64GLM[V]) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	// Wake everyone: close the live wake gate64GLM AND the terminal closingGate
	// (watchers select on the wake gate64GLM; closing it releases them all).
	close(b.closingGate)
	b.pubMu.Lock()
	g := b.wake.Load()
	b.wake.Store(nil) // subsequent waitGate calls take the closed fast path
	b.pubMu.Unlock()
	if g != nil {
		close(g.ch)
	}
}

// Snapshot returns the newest value and the number of values ever
// published. ok=false if nothing was ever published.
func (b *Beacon64GLM[V]) Snapshot() (v V, total uint32, ok bool) {
	for {
		_, cur := unpack64GLM(b.claim.Load())
		if cur == 0 {
			return v, 0, false
		}
		// cur is the newest sequence (first value = 1); no -1 adjustment.
		if v2, _, ok2 := b.readSlotAt(cur); ok2 {
			return v2, cur, true
		}
		runtime.Gosched()
	}
}

// Len returns the number of retained values (0..capacity).
func (b *Beacon64GLM[V]) Len() int {
	_, cur := unpack64GLM(b.claim.Load())
	hist := uint32(len(b.slots))
	if cur > hist {
		return int(hist)
	}
	return int(cur)
}

// Capacity returns the history capacity (rounded-up power of two).
func (b *Beacon64GLM[V]) Capacity() int { return int(b.mask) + 1 }

// Closed reports whether Close was called.
func (b *Beacon64GLM[V]) Closed() bool { return b.closed.Load() }

// ---------------------------------------------------------------------------
// Slot protocol dispatch: the two build-tag files define these.
// ---------------------------------------------------------------------------

// loadSlotSeq64GLM loads a slot64GLM's seq word with acquire semantics.
func loadSlotSeq64GLM[V any](sp *slot64GLM[V]) uint64 { return loadSlotSeqImpl64GLM(sp) }

// loadSlotVal64GLM copies the slot64GLM payload into dst with acquire semantics
// (tear-free is the caller's job via the seqlock re-check).
func loadSlotVal64GLM[V any](sp *slot64GLM[V], dst *V) { loadSlotValImpl64GLM(sp, dst) }

// storeSlotValImpl64GLM writes the payload with release semantics.
// (declared in beacon.go, dispatched here)
//   slotread_race.go  : all-atomic (TSan-friendly)
//   slotread_fast.go  : plain+fence  (needs !race)
//   slotread_plain.go : fallback when no .s fence exists (portable)

// slotSeqRaw64GLM provides the raw address of a slot64GLM's seq word for the atomic
// implementations in the build-tag files.
func slotSeqRaw64GLM[V any](sp *slot64GLM[V]) *uint64 {
	return (*uint64)(unsafe.Pointer(&sp.seq))
}
