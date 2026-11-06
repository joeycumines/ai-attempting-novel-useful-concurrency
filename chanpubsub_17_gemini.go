// -- chanpubsub.go --
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


// ChanPubSub17Gemini is a pub/sub implementation that uses a channel as the underlying
// transport. This implementation's core benefits are scalability, particularly
// with arbitrarily large numbers of concurrent subscribers, and support for use
// in select statements.
type ChanPubSub17Gemini[C ~chan V, V any] struct {
	// ping is used to orchestrate sends.
	ping ChanCaster17Pumped[C, V]

	// pongC is used to orchestrate waits.
	pongC *sync.Cond
	pongN int

	// sendMu prevents concurrent sends.
	sendMu sync.Mutex

	// sendingMu prevents positive subscriber additions while a send is active.
	sendingMu sync.RWMutex

	// stateMu serialises reading/modifying subscribers with arming ping.
	// It is never held across blocking channel operations, eliminating the
	// need for spin-waiting in negative Add calls.
	stateMu sync.Mutex

	// subscribers tracks the total number of subscribers.
	subscribers atomic.Int32

	// isBroken provides a 1-instruction atomic fast-path check for checkBroken,
	// avoiding runtime.selectnbrecv overhead on every method call.
	isBroken atomic.Bool

	// broken is closed on API misuse to unblock select statements.
	broken chan struct{}
}

// NewChanPubSub17Gemini constructs a new instance of [ChanPubSub17Gemini], using the provided
// channel as the underlying transport. The channel must be unbuffered
// (cap == 0). A nil channel is permitted. It panics if the channel is
// buffered.
func NewChanPubSub17Gemini[C ~chan V, V any](c C) *ChanPubSub17Gemini[C, V] {
	if cap(c) != 0 {
		panic(`bigbuff: chanpubsub: must use an unbuffered channel`)
	}
	var x ChanPubSub17Gemini[C, V]
	x.ping.C = c
	x.pongC = sync.NewCond(new(sync.Mutex))
	x.broken = make(chan struct{})
	return &x
}

// C returns the underlying channel, used by the receiver.
func (x *ChanPubSub17Gemini[C, V]) C() C {
	x.checkUsedFactoryFunction()
	x.checkBroken()
	return x.ping.C
}

// SubscribeContext immediately subscribes to the receiver, and returns a
// single-use iterator, which will yield all values sent to the receiver, and
// will automatically unsubscribe when the context is cancelled, the iterator
// stops early, or the receiver is closed.
func (x *ChanPubSub17Gemini[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
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

// Send sends a value to all subscribers, returning the number of subscribers
// that received the value.
func (x *ChanPubSub17Gemini[C, V]) Send(value V) (sent int) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	if x.subscribers.Load() == 0 {
		return 0 // fast path
	}

	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	x.sendingMu.Lock()
	var skipSendingUnlock bool
	defer func() {
		if !skipSendingUnlock {
			x.sendingMu.Unlock()
		}
	}()

	x.checkBroken()

	// Lock stateMu briefly to read subscribers and atomically arm x.ping
	// in a single step. Because stateMu is released before channel sends begin,
	// concurrent negative Add calls never spin-wait.
	x.stateMu.Lock()
	subscribers := int(x.subscribers.Load())
	if subscribers == 0 {
		x.stateMu.Unlock()
		return 0
	}
	x.sanityCheckSubscribersDelta(subscribers, 0)

	var success bool
	defer func() {
		if !success {
			x.markBroken()
		}
	}()

	x.ping.armForSend(uint32(subscribers))
	x.stateMu.Unlock()

	// Pump all channel sends; concurrent negative Add calls may drain items.
	sent = x.ping.pumpAndReset(value, uint32(subscribers))

	skipSendingUnlock = true
	x.sendingMu.Unlock() // allow new subscribers while waiting for pongs

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

	success = true
	return
}

// Add returns the number of subscribers, after adding delta to the count.
func (x *ChanPubSub17Gemini[C, V]) Add(delta int) (subscribers int) {
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic(`bigbuff: chanpubsub: delta out of bounds`)
	}

	x.checkUsedFactoryFunction()
	x.checkBroken()

	switch {
	case delta == 0:
		subscribers = int(x.subscribers.Load())
		x.sanityCheckSubscribersDelta(subscribers, 0)

	case delta < 0:
		// Acquire stateMu (never held across channel sends, so zero spin-wait).
		// Under stateMu, we know deterministically whether ping is armed.
		x.stateMu.Lock()
		inFlight := x.ping.state.Load() != 0
		subscribers = x.addSubscribers(delta)
		x.stateMu.Unlock()

		x.sanityCheckSubscribersDelta(subscribers, delta)

		if inFlight {
			var success bool
			defer func() {
				if !success {
					x.markBroken()
				}
			}()
			x.ping.Add(delta)
			success = true
		}

	default:
		// Subscribe case: blocked while Send is pumping ping, concurrent with
		// other positive Add calls, and serialised against Send's arming step.
		x.sendingMu.RLock()
		defer x.sendingMu.RUnlock()
		x.stateMu.Lock()
		subscribers = x.addSubscribers(delta)
		x.stateMu.Unlock()
		x.sanityCheckSubscribersDelta(subscribers, delta)
	}

	return
}

// Wait is mandatory after each message received by each subscriber.
func (x *ChanPubSub17Gemini[C, V]) Wait() {
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

// Subscribe is an alias for calling [ChanPubSub17Gemini.Add] with a delta of 1.
func (x *ChanPubSub17Gemini[C, V]) Subscribe() {
	x.Add(1)
}

// Unsubscribe is an alias for calling [ChanPubSub17Gemini.Add] with a delta of -1.
func (x *ChanPubSub17Gemini[C, V]) Unsubscribe() {
	x.Add(-1)
}

func (x *ChanPubSub17Gemini[C, V]) checkBroken() {
	if x.isBroken.Load() {
		panic(chanPubSubStateInvariantViolation)
	}
}

func (x *ChanPubSub17Gemini[C, V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: chanpubsub: must use factory function`)
	}
}

func (x *ChanPubSub17Gemini[C, V]) markBroken() {
	defer func() {
		recover()
	}()
	x.pongC.L.Lock()
	defer x.pongC.L.Unlock()
	if !x.isBroken.Swap(true) {
		close(x.broken)
	}
	x.pongC.Broadcast()
}

func (x *ChanPubSub17Gemini[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
	oldSubscribers := int(int32(subscribers) - int32(delta))

	if (delta > 0 && oldSubscribers >= subscribers) ||
		(delta < 0 && oldSubscribers <= subscribers) {
		x.markBroken()
		panic("bigbuff: chanpubsub: addition overflowed or underflowed")
	}

	if subscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative subscribers`)
	}

	if oldSubscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative old subscribers`)
	}
}

func (x *ChanPubSub17Gemini[C, V]) addSubscribers(delta int) int {
	return int(x.subscribers.Add(int32(delta)))
}
