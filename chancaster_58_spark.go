package bigbuff

// Provenance: 58-arena-muse-spark-1.3-xhigh-prompt-safe.md, section 1
// (ChanCaster evolved: padding + safe helpers). This is a distinct response
// implementation, not a modification of the handwritten ChanCaster baseline.
// Send/Add delegate to the unchanged dual-counter protocol. The padding and
// the three helpers are the response's contribution. The original arithmetic
// said 128 bytes but its trailing pad was 32 bytes; the actual trailing pad is
// 24 bytes on a 64-bit target (8+56+40+24). No runtime cache-line-size claim is
// made for other targets. TryRecv is nonblocking with respect to an idle
// caster, NOT linearizable with respect to a Send that starts immediately
// after its gate check: withdrawal may have to consume that Send's rendezvous.

import (
	"context"
	"math"
)

type ChanCaster58Spark[C ~chan V, V any] struct {
	C        C
	_        [56]byte
	protocol ChanCaster[C, V]
	_        [24]byte
}

func NewChanCaster58Spark[C ~chan V, V any](c C) *ChanCaster58Spark[C, V] {
	if cap(c) != 0 {
		panic("bigbuff: chancaster58: must use an unbuffered channel")
	}
	x := &ChanCaster58Spark[C, V]{C: c}
	x.protocol.C = c
	return x
}

func (x *ChanCaster58Spark[C, V]) Len() int          { return x.protocol.Add(0) }
func (x *ChanCaster58Spark[C, V]) Add(delta int) int { return x.protocol.Add(delta) }
func (x *ChanCaster58Spark[C, V]) Send(value V) int  { return x.protocol.Send(value) }

// RecvContext registers one receive and withdraws it on cancellation. A
// cancellation can consume a rendezvous from an already-active Send; that is
// withdrawal compensation, not an extra delivery. Like the underlying caster,
// this helper has no post-receive acknowledgement barrier.
func (x *ChanCaster58Spark[C, V]) RecvContext(ctx context.Context) (V, bool) {
	var zero V
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return zero, false
	}
	x.Add(1)
	select {
	case v, ok := <-x.C:
		if !ok {
			x.Add(-1)
			return zero, false
		}
		return v, true
	case <-ctx.Done():
		x.Add(-1)
		return zero, false
	}
}

// TryRecv never waits for space in an empty caster. If Send wins the gate race,
// Add(-1) can still block briefly to steal the cancelled rendezvous. Callers
// must not receive from C independently while using this helper.
func (x *ChanCaster58Spark[C, V]) TryRecv() (V, bool) {
	var zero V
	if !x.protocol.mutex.TryRLock() {
		return zero, false
	}
	state := x.protocol.state.Add(uint64(1)<<32 | 1)
	r, tracker := uint32(state>>32), uint32(state)
	valid := r <= math.MaxInt32 && r >= 1 && r == tracker
	x.protocol.mutex.RUnlock()
	if !valid {
		panic("bigbuff: chancaster58: state invariant violation")
	}
	select {
	case v, ok := <-x.C:
		if !ok {
			x.Add(-1)
			return zero, false
		}
		return v, true
	default:
		x.Add(-1)
		return zero, false
	}
}
