//go:build go1.22

package bigbuff

import (
	"math"
	"sync/atomic"
)

// ChanCaster15Nemotron is a lock-free, generation-tagged broadcast primitive.
//
// It preserves the original contract:
//
//	* C MUST be unbuffered (cap == 0). A nil channel is permitted.
//	* [Send] performs exactly one synchronous send per registered receiver.
//	* [Add] with a negative delta must consume its share of in-flight sends
//	  so the sender never blocks on a receive that will never arrive.
//
// The state is a single atomic.Uint64:
//
//	high 32 bits : receiver count (int32)
//	low  32 bits : send generation (uint32); bit 0 is the send lock
//
// An even generation means idle; an odd generation means a [Send] is
// actively iterating over the receiver count and performing channel sends.
// Because the generation is updated with atomic compare-and-swap, no
// mutex is required for [Add], and [Send] only contends with other sends.
//
// All public methods allocate zero memory on the hot path.
type ChanCaster15Nemotron[C ~chan V, V any] struct {
	C C

	// state packs receivers (hi) and generation (lo).
	state atomic.Uint64
}

// NewChanCaster15Nemotron constructs a [ChanCaster15Nemotron] for the provided channel. It
// panics if the channel is buffered. A nil channel is permitted.
func NewChanCaster15Nemotron[C ~chan V, V any](channel C) *ChanCaster15Nemotron[C, V] {
	if cap(channel) != 0 {
		panic("bigbuff: chancaster: must use an unbuffered channel")
	}
	return &ChanCaster15Nemotron[C, V]{C: channel}
}

// Send broadcasts value to all registered receivers and returns the number
// of sends performed, captured at the instant the send lock was acquired.
// Receivers added after that instant miss this broadcast; receivers removed
// during the broadcast drain their share via [Add].
//
// This call serialises with other [Send] calls via the generation lock. It
// never blocks on subscriber churn.
func (x *ChanCaster15Nemotron[C, V]) Send(value V) int {
	const sendLock = uint64(1)

	// Acquire the send lock: transition the generation from even to odd.
	// If another send is in progress, spin and retry; sends are expected to
	// be infrequent relative to receives, so contention is minimal.
	var (
		st       uint64
		receivers int32
	)
	for {
		st = x.state.Load()
		receivers = int32(st >> 32)
		if receivers == 0 {
			return 0 // fast path: no receivers
		}
		gen := uint32(st)
		if gen&1 == 1 {
			// Send already in progress; retry.
			continue
		}
		// Mark sending: set the low bit of the generation.
		if x.state.CompareAndSwap(st, (uint64(receivers)<<32)|uint64(gen|1)) {
			break
		}
	}

	n := int(receivers) // snapshot; safe because we hold the send lock

	// One synchronous send per receiver. Because C is unbuffered, each
	// operation pairs with exactly one receiver that is blocked on C, or
	// with a concurrent negative [Add] that is draining its departing
	// share.
	for i := 0; i < n; i++ {
		x.C <- value
	}

	// Release the send lock: transition the generation from odd back to
	// even. Receivers removed by concurrent negative Add calls have already
	// consumed their messages, so the remaining sends were delivered to
	// the survivors.
	for {
		st = x.state.Load()
		if uint32(st)&1 == 0 {
			panic(`bigbuff: chancaster: send: state not locked`)
		}
		if x.state.CompareAndSwap(st, st+sendLock) {
			break
		}
	}

	return n
}

// Add increments or decrements the number of receivers and returns the
// count after the operation. A delta of 0 is a no-op inspection.
//
// Positive deltas are completely lock-free: new receivers added while a
// send is in progress simply miss that send.
//
// Negative deltas are also lock-free with respect to mutexes, but if a
// send is in progress (odd generation) the departing receiver must consume
// its share of the sends from C. Failing to do so would leave the sender
// blocked on a channel operation with no matching receive. This is the
// documented contract: a receiver that unsubscribes while a broadcast is
// underway must be prepared to receive one final value.
func (x *ChanCaster15Nemotron[C, V]) Add(delta int) int {
	const maxInt32 = math.MaxInt32

	switch {
	case delta == 0:
		return int(int32(x.state.Load() >> 32))

	case delta > 0:
		if delta > maxInt32 {
			panic(`bigbuff: chancaster: add: positive delta out of bounds`)
		}
		// Increment receivers without touching the generation.
		for {
			st := x.state.Load()
			receivers := int32(st >> 32)
			gen := uint32(st)
			if receivers > maxInt32-int32(delta) {
				panic(`bigbuff: chancaster: add: receiver overflow`)
			}
			if x.state.CompareAndSwap(st, (uint64(receivers+int32(delta))<<32)|uint64(gen)) {
				return int(receivers) + delta
			}
		}

	default:
		d := -delta
		if d > maxInt32 {
			panic(`bigbuff: chancaster: add: negative delta out of bounds`)
		}

		// Optimistically remove d receivers.
		var (
			st       uint64
			receivers int32
		)
		for {
			st = x.state.Load()
			receivers = int32(st >> 32)
			if receivers < int32(d) {
				panic(`bigbuff: chancaster: add: receiver underflow`)
			}
			gen := uint32(st)
			if x.state.CompareAndSwap(st, (uint64(receivers-int32(d))<<32)|uint64(gen)) {
				break
			}
		}

		// If a send was in progress at the instant we removed ourselves,
		// we must drain our share of the messages so the sender does not
		// block on an orphaned send.
		if uint32(st)&1 == 1 {
			for i := 0; i < d; i++ {
				<-x.C
			}
		}

		return int(receivers - int32(d))
	}
}
