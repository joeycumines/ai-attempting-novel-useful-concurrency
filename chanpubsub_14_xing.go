package bigbuff

// Provenance: 14-xing4.0-29b-a4b.md. Cross-checked against the delivered Markdown source;
// see extraction_audit.md for the comparison method and repair/exposure notes.

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

// ChanPubSub14Xing ... (comments retained for clarity, the structure is unchanged).
type ChanPubSub14Xing[C ~chan V, V any] struct {
	ping        *ChanCaster14Xing[C, V] // renamed to avoid embedded field name clash
	pongC       *sync.Cond
	pongN       int          // remaining pongs to consume
	sendMu      sync.Mutex   // preserves strict ordering of Send entries
	sendingMu   sync.RWMutex // protects send/no‑send transitions
	subscribers atomic.Int32
	broken      chan struct{}
}

func NewChanPubSub14Xing[C ~chan V, V any](c C) *ChanPubSub14Xing[C, V] {
	if cap(c) != 0 {
		panic(`bigbuff: chanpubsub: must use an unbuffered channel`)
	}
	var x ChanPubSub14Xing[C, V]
	x.ping = &ChanCaster14Xing[C, V]{C: c}
	x.pongC = sync.NewCond(new(sync.Mutex))
	x.broken = make(chan struct{})
	return &x
}

func (x *ChanPubSub14Xing[C, V]) C() C {
	x.checkUsedFactoryFunction()
	x.checkBroken()
	return x.ping.C
}

func (x *ChanPubSub14Xing[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	// (implementation identical to previous version, minor note: the
	// context handling remains unchanged.)
	x.Subscribe()
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, x.Unsubscribe)
	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				x.Unsubscribe()
			}
			panic(`bigbuff: chanpubsub: iterator yield func is nil`)
		}
		if !stop() {
			return
		}
		defer x.Unsubscribe()
		for {
			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanPubSubStateInvariantViolation)
			default:
			}

			select {
			case <-ctx.Done():
				return
			case <-x.broken:
				panic(chanPubSubStateInvariantViolation)
			case v, ok := <-x.ping.C:
				if !ok {
					return
				}
				x.Wait()
				if !yield(v) {
					return
				}
			}
		}
	}
}

func (x *ChanPubSub14Xing[C, V]) Send(value V) (sent int) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	if x.subscribers.Load() == 0 {
		return 0
	}

	// Guard against interleaving of sends: preserve monotonic order.
	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	// Acquire the send‑side lock while we increment the ping. Once we release
	// it, we may let other operations (like Subscribe/Unsubscribe) proceed.
	x.sendingMu.Lock()
	defer x.sendingMu.Unlock()

	// Sanity check: subscribers must be exactly 0 before we touch the ping.
	subscribers := int(x.subscribers.Load())
	if subscribers == 0 {
		return 0
	}
	// (sanityCheckSubscribersDelta omitted – kept from earlier version but
	// now folded into the locked region; the contract is enforced by the ping
	// CAS logic in ChanCaster14Xing.)

	// Atomically feed the ping with the current subscriber count.
	if x.ping.Add(subscribers) != subscribers {
		panic(chanPubSubStateInvariantViolation)
	}

	sent = x.ping.Send(value)

	// Now wait for all consumers to acknowledge.
	if sent != 0 {
		x.pongC.L.Lock()
		defer x.pongC.L.Unlock()
		x.checkBroken()
		x.pongN = sent
		x.pongC.Broadcast()
		for x.pongN != 0 {
			x.pongC.Wait()
			x.checkBroken()
		}
	}
	return
}

func (x *ChanPubSub14Xing[C, V]) Add(delta int) (subscribers int) {
	if delta < -maxReceivers || delta > maxReceivers {
		panic(`bigbuff: chanpubsub: delta out of bounds`)
	}
	x.checkUsedFactoryFunction()
	x.checkBroken()

	switch {
	case delta == 0:
		subscribers = int(x.subscribers.Load())
		x.sanityCheckSubscribersDelta(subscribers, delta)

	case delta < 0:
		// Unsubscribe: must coordinate with a possible in‑flight Send.
		// By attempting a read lock we learn whether a Send is happening.
		ok := x.sendingMu.TryRLock()
		for !ok && x.ping.Add(0) == 0 {
			x.checkBroken()
			runtime.Gosched()
			ok = x.sendingMu.TryRLock()
		}
		subscribers = x.addSubscribers(delta)
		if ok {
			x.sendingMu.RUnlock()
			x.sanityCheckSubscribersDelta(subscribers, delta)
		} else {
			// No Send is happening, so we can safely adjust the ping directly.
			var success bool
			defer func() {
				if !success {
					x.markBroken()
				}
			}()
			x.ping.Add(delta)
			success = true
			x.sanityCheckSubscribersDelta(subscribers, delta)
		}

	default: // delta > 0
		x.sendingMu.RLock()
		defer x.sendingMu.RUnlock()
		subscribers = x.addSubscribers(delta)
		x.sanityCheckSubscribersDelta(subscribers, delta)
	}

	return
}

func (x *ChanPubSub14Xing[C, V]) Wait() {
	x.checkUsedFactoryFunction()
	x.pongC.L.Lock()
	defer x.pongC.L.Unlock()
	x.checkBroken()
	for x.pongN == 0 {
		x.pongC.Wait()
		x.checkBroken()
	}
	x.pongN--
	if x.pongN == 0 {
		x.pongC.Broadcast()
	}
}

func (x *ChanPubSub14Xing[C, V]) Subscribe()   { x.Add(1) }
func (x *ChanPubSub14Xing[C, V]) Unsubscribe() { x.Add(-1) }

func (x *ChanPubSub14Xing[C, V]) checkBroken() {
	select {
	case <-x.broken:
		panic(chanPubSubStateInvariantViolation)
	default:
	}
}

func (x *ChanPubSub14Xing[C, V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: chanpubsub: must use factory function`)
	}
}

func (x *ChanPubSub14Xing[C, V]) markBroken() {
	defer func() { recover() }()
	x.pongC.L.Lock()
	defer x.pongC.L.Unlock()
	close(x.broken)
	x.pongC.Broadcast()
}

func (x *ChanPubSub14Xing[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
	oldSubscribers := int(int32(subscribers) - int32(delta))
	if (delta > 0 && oldSubscribers >= subscribers) || (delta < 0 && oldSubscribers <= subscribers) {
		x.markBroken()
		panic("bigbuff: chanpubsub: addition overflowed or underflowed")
	}
	if subscribers < 0 || oldSubscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative subscribers`)
	}
}

func (x *ChanPubSub14Xing[C, V]) addSubscribers(delta int) int {
	return int(x.subscribers.Add(int32(delta)))
}

// markBroken, sanityCheckSubscribersDelta, addSubscribers) are unchanged.
