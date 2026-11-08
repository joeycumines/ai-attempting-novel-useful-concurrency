// Provenance: 52-arena-claude-opus-5-medium.md (Claude Opus, response #52). Namespaced to preserve existing APIs.
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

const (
	chanCascade52OpusInvariant = `bigbuff: chancascade: state invariant violation`

	// chanCascade52OpusMaxReceivers is the inclusive upper bound on the number of
	// concurrently registered receivers.
	chanCascade52OpusMaxReceivers = math.MaxInt32

	// state (uint64) bit layout:
	//
	//	63..32  receivers  (uint32, <= chanCascade52OpusMaxReceivers)
	//	31      sending    (a broadcast round is in flight)
	//	30..1   reserved   (MUST be zero - cheap corruption detection)
	//	0       phase      (index of the transport channel in use)
	chanCascade52OpusSendingBit  = uint32(1) << 31
	chanCascade52OpusPhaseBit    = uint32(1)
	chanCascade52OpusReservedBit = ^(chanCascade52OpusSendingBit | chanCascade52OpusPhaseBit)

	// chanCascade52OpusSpin bounds the sender's adaptive spin, prior to parking. It
	// is deliberately only applied for small rounds, where the round latency
	// is comparable to a park/unpark pair, and where the spin cannot
	// meaningfully interfere with receivers contending on the same line.
	chanCascade52OpusSpin         = 128
	chanCascade52OpusSpinMaxRecvs = 8
)

// chanCascade52OpusPad separates mutable hot fields onto distinct cache lines,
// preventing false sharing between the (sender-owned) registration word and
// the (receiver-hammered) round word.
type chanCascade52OpusPad [64]byte

// ChanCascade52Opus is an allocation-free, select-compatible, exactly-counted
// broadcast ("pub/sub") primitive, in which receivers cooperatively relay each
// value onwards, reducing broadcast latency from O(n) sequential channel
// handoffs to O(log n) handoff depth, executed in parallel.
//
// It occupies the same niche as [ChanPubSub] - every registered receiver
// observes every value, and [ChanCascade52Opus.Send] returns only once delivery is
// complete - but does so without a secondary barrier (no [sync.Cond], no
// [sync.WaitGroup]), without any allocation on the warmed steady-state send
// path. Subscribe does not wait for an in-flight broadcast; Unsubscribe may
// wait for the outstanding handoff it absorbs.
//
// # How it works
//
// Two unbuffered transport channels are used, alternately. The channel in use
// for a given broadcast is identified by a single "phase" bit; each receiver
// tracks its own phase, flipping it on each value consumed. Because a receiver
// that has consumed the current round has already flipped away from the
// channel still carrying that round's relays, it is structurally incapable of
// consuming the same value twice, or of stealing a value owed to a slower
// peer. This is what replaces [ChanPubSub]'s mandatory ping-pong barrier.
//
// [ChanCascade52Opus.Send] performs exactly one channel send. Each receiver, upon
// consuming a value, atomically claims a relay duty; if one is owed, it sends
// the value back onto the same channel. The set of value-holders therefore
// doubles each round, giving a critical path of ceil(log2(n))+1 handoffs for n
// receivers, while performing exactly n handoffs in total (the minimum
// possible, since each receiver must be woken exactly once).
//
// # Contract
//
//  1. Instances MUST be created via [NewChanCascade52Opus] or [MakeChanCascade52Opus].
//  2. The transport channels MUST be unbuffered, distinct, and MUST NOT be
//     sent to, received from, or closed, by anything other than this type and
//     its receivers. (Both may be nil, yielding a permanently "disabled"
//     instance; it is then an error to register any receiver.)
//  3. A receiver handle, obtained from [ChanCascade52Opus.Subscribe], is owned by
//     exactly one goroutine at a time, and MUST NOT be copied after use.
//  4. [ChanCascadeReceiver52Opus.Received] MUST be called immediately after - and
//     only after - each value received from [ChanCascadeReceiver52Opus.C], before
//     any further use of the handle. Failing to do so stalls all senders, and
//     all other receivers.
//  5. A receiver MUST promptly and continually either receive-then-Received,
//     or unsubscribe. It MAY use [ChanCascadeReceiver52Opus.C] in a select
//     statement, and - unlike [ChanCaster] - needs no registration churn to do
//     so: an unselected case costs nothing and changes no state.
//  6. [ChanCascadeReceiver52Opus.Unsubscribe] MUST be called exactly once per
//     handle, and MUST NOT be called concurrently with any other use of that
//     handle. Membership changes use lock-free CAS; if this receiver still
//     owes the current round, Unsubscribe waits for and absorbs its handoff.
//
// Violations of (4) or of the exclusive-ownership rules are detected where
// cheaply possible, and surface as panics rather than silent corruption.
//
// # Caveats
//
// The cascade converts latency into parallelism; with GOMAXPROCS=1 there is no
// parallelism to exploit, and the cascade is marginally slower than a linear
// broadcast (by a handful of atomics). It is strictly superior whenever
// receivers can run concurrently.
type ChanCascade52Opus[C ~chan V, V any] struct {
	// c holds the two alternating transports, indexed by phase.
	c [2]C

	// wake is a cap-1 signalling slot, allocated once, used to unpark a
	// sender that failed to complete its round within its spin budget.
	wake chan struct{}

	// sendMu serialises broadcast rounds. It has no other purpose; notably,
	// it is NOT held against registration, which is wholly lock-free.
	sendMu sync.Mutex

	_ chanCascade52OpusPad

	// state is the registration word; see the bit layout above. Mutated by
	// senders (sending bit, phase bit) and by registration (receiver count),
	// always via CAS, so that "capture the round membership" and "join/leave"
	// are totally ordered against one another.
	state atomic.Uint64

	_ chanCascade52OpusPad

	// relays is the round word. Seeded to n-1 at the start of each round, and
	// decremented once per receipt; a non-negative result confers the duty to
	// relay the value onwards. Reaching exactly -1 means "round complete":
	// all n receipts have occurred, which (by the counting argument in the
	// package docs) implies no claimed relay remains unsent.
	relays atomic.Int64

	// parked indicates the sender has exhausted its spin budget and is (or is
	// about to be) blocked on wake.
	parked atomic.Bool

	// dropped is a monotonic count of receipts absorbed by mid-round
	// unsubscribes; rounds take a difference of snapshots, so that it is never
	// reset (and therefore never races with an in-flight absorber).
	dropped atomic.Uint64

	_ chanCascade52OpusPad
}

// ChanCascadeReceiver52Opus is a registered receiver of a [ChanCascade52Opus]. It is a
// value type, two words wide, which does not escape under normal use, making
// the entire subscribe/receive/unsubscribe lifecycle allocation-free.
//
// A handle MUST NOT be copied once used, and MUST be used by only one
// goroutine at a time. See [ChanCascade52Opus] for the full contract.
type ChanCascadeReceiver52Opus[C ~chan V, V any] struct {
	x     *ChanCascade52Opus[C, V]
	phase uint32
}

// NewChanCascade52Opus constructs a [ChanCascade52Opus] over the two provided transport
// channels. Both must be unbuffered and distinct, or both nil. It panics
// otherwise.
func NewChanCascade52Opus[C ~chan V, V any](a, b C) *ChanCascade52Opus[C, V] {
	switch {
	case cap(a) != 0 || cap(b) != 0:
		panic(`bigbuff: chancascade: must use unbuffered channels`)
	case (a == nil) != (b == nil):
		panic(`bigbuff: chancascade: both channels must be nil, or neither`)
	case a != nil && a == b:
		panic(`bigbuff: chancascade: channels must be distinct`)
	}
	x := new(ChanCascade52Opus[C, V])
	x.c[0], x.c[1] = a, b
	x.wake = make(chan struct{}, 1)
	return x
}

// MakeChanCascade52Opus constructs a [ChanCascade52Opus] with freshly made transports. It
// is the idiomatic constructor, where the channel type need not be named.
func MakeChanCascade52Opus[V any]() *ChanCascade52Opus[chan V, V] {
	return NewChanCascade52Opus(make(chan V), make(chan V))
}

// Receivers returns the number of currently registered receivers. Note that
// this is inherently racy, for anything other than diagnostics.
func (x *ChanCascade52Opus[C, V]) Receivers() int {
	return int(uint32(x.state.Load() >> 32))
}

// Subscribe registers a new receiver, and returns its handle. It is lock-free,
// never blocks, and never allocates - including while a broadcast is in
// flight, in which case the new receiver cleanly joins from the next round.
//
// The caller MUST eventually call [ChanCascadeReceiver52Opus.Unsubscribe].
func (x *ChanCascade52Opus[C, V]) Subscribe() ChanCascadeReceiver52Opus[C, V] {
	x.checkUsedFactoryFunction()
	if x.c[0] == nil {
		panic(`bigbuff: chancascade: cannot subscribe to disabled instance`)
	}
	for {
		state := x.state.Load()
		receivers, flags := uint32(state>>32), uint32(state)
		if receivers >= chanCascade52OpusMaxReceivers || flags&chanCascade52OpusReservedBit != 0 {
			panic(chanCascade52OpusInvariant)
		}
		phase := flags & chanCascade52OpusPhaseBit
		if flags&chanCascade52OpusSendingBit != 0 {
			// A round is in flight, and this CAS is ordered after the CAS that
			// captured its membership - so we are NOT part of it. Join on the
			// opposite transport, which nothing will send on until that round
			// has fully completed.
			phase ^= 1
		}
		if x.state.CompareAndSwap(state, state+1<<32) {
			return ChanCascadeReceiver52Opus[C, V]{x: x, phase: phase}
		}
	}
}

// Send broadcasts a value to every registered receiver, blocking until every
// one of them has received it (or has unsubscribed mid-flight), and returns
// the number of receivers that actually received it.
//
// Exactly one send is performed by the calling goroutine; the remaining n-1
// deliveries are cascaded by the receivers themselves, in parallel.
//
// Send never allocates. Concurrent calls are serialised.
func (x *ChanCascade52Opus[C, V]) Send(value V) int {
	if x.state.Load()>>32 == 0 {
		return 0 // no receivers (fast path: single relaxed load)
	}

	x.checkUsedFactoryFunction()

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	// Snapshot the absorb counter. Safe here, and only here: absorbers may
	// only increment it while the sending bit is set, and the previous round's
	// absorbers are all guaranteed to have completed (see await).
	dropped := x.dropped.Load()

	// Atomically capture round membership and claim the sending bit. This
	// single CAS is what totally orders membership against Subscribe.
	var receivers, phase uint32
	for {
		state := x.state.Load()
		receivers, phase = uint32(state>>32), uint32(state)
		if receivers > chanCascade52OpusMaxReceivers ||
			phase&(chanCascade52OpusSendingBit|chanCascade52OpusReservedBit) != 0 {
			panic(chanCascade52OpusInvariant)
		}
		if receivers == 0 {
			return 0 // no receivers (slow path)
		}
		phase &= chanCascade52OpusPhaseBit
		if x.state.CompareAndSwap(state, state|uint64(chanCascade52OpusSendingBit)) {
			break
		}
	}

	// Publish the round. Receivers cannot observe any of this before the seed
	// send below, which is the only thing that can hand them a value.
	x.parked.Store(false)
	x.relays.Store(int64(receivers) - 1)

	// Seed the cascade. From here, delivery is self-propelled.
	x.c[phase] <- value

	x.await(receivers)

	if x.relays.Load() != -1 {
		panic(chanCascade52OpusInvariant)
	}
	dropped = x.dropped.Load() - dropped
	if dropped > uint64(receivers) {
		panic(chanCascade52OpusInvariant)
	}

	// Close the round: clear the sending bit and flip the phase. The receiver
	// count may legitimately have changed (joins and leaves are lock-free), so
	// only the flags are validated.
	for {
		state := x.state.Load()
		if uint32(state) != chanCascade52OpusSendingBit|phase {
			panic(chanCascade52OpusInvariant)
		}
		if x.state.CompareAndSwap(state, state&^uint64(math.MaxUint32)|uint64(phase^1)) {
			break
		}
	}

	return int(receivers) - int(dropped)
}

// await blocks until the current round is complete, i.e. until every receipt
// has been accounted for. Small rounds spin (so that the single-receiver case
// costs exactly one handoff, matching a direct channel send); larger rounds
// park immediately, amortising one park/unpark across n deliveries.
func (x *ChanCascade52Opus[C, V]) await(receivers uint32) {
	if receivers <= chanCascade52OpusSpinMaxRecvs {
		for i := 0; i < chanCascade52OpusSpin; i++ {
			if x.relays.Load() == -1 {
				return
			}
		}
	}

	// Dekker-style handshake against ChanCascade52Opus.signal. Go's sync/atomic
	// operations are sequentially consistent, so if the load below misses the
	// final receipt, that receipt's subsequent load of parked cannot miss this
	// store.
	x.parked.Store(true)
	for x.relays.Load() != -1 {
		<-x.wake
	}
	x.parked.Store(false)
}

// signal wakes a parked sender, if any. Non-blocking: a lost signal is
// impossible (see await), and a redundant signal is harmlessly consumed by a
// later park, which always re-checks the round word.
func (x *ChanCascade52Opus[C, V]) signal() {
	if x.parked.Load() {
		select {
		case x.wake <- struct{}{}:
		default:
		}
	}
}

// ack records a receipt, discharges any relay duty it confers, and completes
// the round if it was the last. This is the heart of the cascade: exactly one
// atomic read-modify-write per receiver, per value.
func (x *ChanCascade52Opus[C, V]) ack(phase uint32, value V) {
	switch remaining := x.relays.Add(-1); {
	case remaining >= 0:
		// We owe a relay. This send cannot deadlock: at any instant, the
		// number of unmatched sends is bounded above by the number of
		// receivers yet to be served.
		x.c[phase] <- value
	case remaining == -1:
		// Final receipt of the round. By the counting argument, no claimed
		// relay can still be unsent, so the round is genuinely complete.
		x.signal()
	default:
		// An unregistered or duplicated receive.
		panic(chanCascade52OpusInvariant)
	}
}

func (x *ChanCascade52Opus[C, V]) checkUsedFactoryFunction() {
	if x.wake == nil {
		panic(`bigbuff: chancascade: must use factory function`)
	}
}

// C returns the transport channel this receiver must currently receive from.
// It is suitable for direct use in a select statement; an unselected case
// costs nothing, and requires no deregistration.
//
// The value returned changes after each value consumed, and MUST be re-read
// each time.
func (r *ChanCascadeReceiver52Opus[C, V]) C() C {
	if r.x == nil {
		panic(`bigbuff: chancascade: receiver is not subscribed`)
	}
	return r.x.c[r.phase]
}

// Received MUST be called immediately after each value received from
// [ChanCascadeReceiver52Opus.C], passing that value, and MUST NOT be called
// otherwise. It discharges this receiver's share of the broadcast - which may
// involve relaying the value onwards to a peer - and advances this receiver to
// the next phase.
//
// It never allocates, and performs exactly one atomic read-modify-write in
// addition to at most one channel send.
func (r *ChanCascadeReceiver52Opus[C, V]) Received(value V) {
	x := r.x
	if x == nil {
		panic(`bigbuff: chancascade: receiver is not subscribed`)
	}
	phase := r.phase
	r.phase = phase ^ 1
	x.ack(phase, value)
}

// Recv blocks until a value is broadcast, consumes it, discharges the
// associated relay duty, and returns the value. It is shorthand for receiving
// from [ChanCascadeReceiver52Opus.C] then calling [ChanCascadeReceiver52Opus.Received].
func (r *ChanCascadeReceiver52Opus[C, V]) Recv() V {
	value := <-r.C()
	r.Received(value)
	return value
}

// Unsubscribe deregisters this receiver, exactly once. Its membership update is
// lock-free and it never allocates; it may still wait for the current handoff
// when absorbing a receipt on behalf of the sender.
//
// If a broadcast is in flight and this receiver still owes it a receipt, that
// receipt is absorbed (and relayed onwards) here, on behalf of the sender,
// which will exclude it from its reported delivery count. Consequently, this
// call may block for the duration of the in-flight round, exactly as a
// well-behaved receive would have.
func (r *ChanCascadeReceiver52Opus[C, V]) Unsubscribe() {
	x := r.x
	if x == nil {
		panic(`bigbuff: chancascade: receiver is not subscribed`)
	}
	r.x = nil

	// The phase bit doubles as an exact ownership oracle: a round is in flight
	// AND it is being broadcast on our phase if and only if we still owe it a
	// receipt. Receivers that have already consumed this round, and receivers
	// that joined after it began, have both already flipped away from it.
	var absorb bool
	for {
		state := x.state.Load()
		receivers, flags := uint32(state>>32), uint32(state)
		if receivers == 0 || receivers > chanCascade52OpusMaxReceivers ||
			flags&chanCascade52OpusReservedBit != 0 {
			panic(chanCascade52OpusInvariant)
		}
		absorb = flags&chanCascade52OpusSendingBit != 0 &&
			flags&chanCascade52OpusPhaseBit == r.phase
		if x.state.CompareAndSwap(state, state-1<<32) {
			break
		}
	}

	if absorb {
		// Ordered before the receipt, and therefore before the sender can
		// observe round completion.
		x.dropped.Add(1)
		x.ack(r.phase, <-x.c[r.phase])
	}
}

// SubscribeContext immediately subscribes, and returns a single-use iterator
// yielding every value broadcast until the context is cancelled, or iteration
// stops. Unsubscription is automatic and exactly-once, on every path.
//
// WARNING: The returned iterator MUST be used promptly, OR the context MUST be
// promptly cancelled, to avoid stalling senders.
func (x *ChanCascade52Opus[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	r := x.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}

	// Covers never-iterated and iterated-after-cancel; disarmed by the
	// iterator, which thereafter owns the (single-goroutine) handle.
	stop := context.AfterFunc(ctx, (&r).Unsubscribe)

	return func(yield func(V) bool) {
		if !stop() {
			return // cancelled, or called more than once
		}
		defer (&r).Unsubscribe()

		if yield == nil {
			panic(`bigbuff: chancascade: iterator yield func is nil`)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case value, ok := <-r.C():
				if !ok {
					return // transport closed: outside the contract
				}
				r.Received(value)
				if !yield(value) {
					return
				}
			}
		}
	}
}
