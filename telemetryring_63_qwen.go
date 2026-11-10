package bigbuff

import (
	"context"
	"runtime"
	"sync/atomic"
)

// Notifier63Qwen is a zero-allocation, coalescing wakeup primitive.
// It eliminates thundering herds by using a single buffered channel to signal
// consumers, allowing them to batch-drain high-frequency updates. It has no
// Close: consumers stop by cancelling their context (see Subscribe).
//
// Repair (integration review): as generated, the single buffered token was
// the only wake path, so with two or more parked consumers one publication
// woke exactly one of them and the others slept until some later publication
// (forever, if it was the last). The token is now relayed: consumers register
// with Arm before their final emptiness check and a consumer woken by Park
// re-posts the token while any other consumer is still registered. Notify is
// unchanged (one atomic add and one non-blocking send, no allocation); the
// relay costs one extra non-blocking send per woken consumer.
type Notifier63Qwen struct {
	epoch   atomic.Uint64
	waiters atomic.Int64
	ch      chan struct{}
}

func NewNotifier63Qwen() *Notifier63Qwen {
	return &Notifier63Qwen{
		ch: make(chan struct{}, 1),
	}
}

// Notify signals waiting consumers. Subsequent calls while the channel is full
// are discarded, achieving O(1) wakeup coalescing.
func (n *Notifier63Qwen) Notify() {
	n.epoch.Add(1)
	select {
	case n.ch <- struct{}{}:
	default:
	}
}

// WaitChan exposes the raw token channel. A single token wakes a single
// receiver, so selecting on it directly is only correct for one consumer;
// multiple consumers must use Arm / Park (or TelemetryRing63Qwen.NextWait).
func (n *Notifier63Qwen) WaitChan() <-chan struct{} {
	return n.ch
}

// Arm registers the caller as a prospective waiter. The caller must re-check
// its wake condition after Arm and then call exactly one of Disarm (condition
// already holds) or Park.
func (n *Notifier63Qwen) Arm() { n.waiters.Add(1) }

// Disarm undoes Arm without parking.
func (n *Notifier63Qwen) Disarm() { n.waiters.Add(-1) }

// Park blocks an armed caller until a token arrives (true) or ctx is done
// (false). On wake the token is relayed if other consumers remain armed, so
// one Notify eventually reaches every consumer that armed before it.
func (n *Notifier63Qwen) Park(ctx context.Context) bool {
	select {
	case <-n.ch:
		if n.waiters.Add(-1) > 0 {
			select {
			case n.ch <- struct{}{}:
			default:
			}
		}
		return true
	case <-ctx.Done():
		n.waiters.Add(-1)
		return false
	}
}

// NextWait returns the next entry for cursor, blocking until one is
// published or ctx is done. It is safe for any number of concurrent
// consumers (each with its own cursor). Without a notifier it yields the
// processor between attempts, as Subscribe always did.
func (r *TelemetryRing63Qwen[V]) NextWait(ctx context.Context, cursor *uint64) (V, bool) {
	n := r.notifier
	for {
		if v, ok := r.Next(cursor); ok {
			return v, true
		}
		if ctx.Err() != nil {
			var zero V
			return zero, false
		}
		if n == nil {
			runtime.Gosched()
			continue
		}
		n.Arm()
		if v, ok := r.Next(cursor); ok {
			n.Disarm()
			return v, true
		}
		if !n.Park(ctx) {
			var zero V
			return zero, false
		}
	}
}
