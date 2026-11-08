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

// Provenance: 16-deepseek-v4.1-flash-chat.md. Cross-checked against the delivered Markdown source;
// see extraction_audit.md for the comparison method and repair/exposure notes.

import (
	"context"
	"iter"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

// ChanPubSub16DeepSeek is a pub/sub implementation that uses a channel as the underlying
// transport. This implementation's core benefits are scalability, particularly
// with arbitrarily large numbers of concurrent subscribers, and support for use
// in select statements.
//
// # Usage
//
// The [ChanPubSub16DeepSeek.SubscribeContext] method provides a simple, [iter.Seq]-based
// (iterable) mechanism, to subscribe and consume values. If greater
// flexibility is required (e.g. it is desirable to leverage select
// statements), please ensure you understand the contract, documented below.
//
// The underlying implementation relies on strict adherence to the following
// contract:
//
//  1. The [NewChanPubSub16DeepSeek] factory MUST be used to create instances.
//  2. Values MUST NOT be received prior to incrementing the subscriber count,
//     e.g. by calling [ChanPubSub16DeepSeek.Add] with `1`.
//  3. Subscribers - any logical unit that receives values, after incrementing
//     the subscriber count - MUST call [ChanPubSub16DeepSeek.Wait], immediately after
//     each value received, UNLESS the value was the result of closing the
//     underlying channel (subscribers MUST handle this case).
//  4. Subscribers MUST be able to receive then block on [ChanPubSub16DeepSeek.Wait]
//     calls, that is, the number of subscribers MUST be exactly equal to what
//     was registered, e.g. via [ChanPubSub16DeepSeek.Add].
//  5. [ChanPubSub16DeepSeek.Wait] MUST NOT be called in any other circumstance. Doing so
//     may cause the receiver to panic, because the underlying [sync.WaitGroup]
//     that backs [ChanPubSub16DeepSeek.Wait] does not tolerate unpaired `Done` calls.
//  6. Subscribers MAY call [ChanPubSub16DeepSeek.Add] with a negative delta, to
//     unsubscribe, but MUST ensure that the net total remains >= 0, at all
//     times.
//  7. Subscribers MUST promptly and _continually_ operate on a cycle of
//     receiving (from [ChanPubSub16DeepSeek.C]) then waiting (on [ChanPubSub16DeepSeek.Wait]),
//     until such a time as they are no longer interested in receiving values,
//     in which case they MUST promptly unsubscribe. Subscribers MAY use the
//     channel in a select statement, to trigger unsubscribe, as an example.
//     After unsubscribing, values MUST NOT be received, from the channel.
//
// # Implementation
//
// The implementation is deliberately layered on top of [ChanCaster]: the
// "ping" side of the classic ping-pong is delegated to [ChanCaster.Send] (which
// already handles the "removed mid-send steals from the send queue" subtlety
// via atomic state), and the "pong" side is handled by a single reused
// [sync.WaitGroup].
//
// The choice of [sync.WaitGroup] for the pong side is significant: the classic
// [sync.Cond] based approach requires each Waiter to acquire and release a
// shared mutex, which serialises across all subscribers and becomes a real
// bottleneck at high subscriber counts. A [sync.WaitGroup]'s Done path is a
// single atomic operation on the hot path, and the corresponding Wait path
// parks the sending goroutine via the runtime semaphore without holding any
// user-visible lock. The upshot is that broadcast to N subscribers costs
// roughly O(N) atomics rather than O(N) mutex acquisitions + wakeups.
//
// The WaitGroup is reset per Send via the sequence Add(subscribers) →
// [ChanCaster.Send] → Add(sent - subscribers) → Wait, where the second Add
// compensates for any subscribers that were removed mid-send (and whose
// "steal" therefore reduced the number of actual receivers). Because
// [ChanPubSub16DeepSeek.Send] is serialised by [ChanPubSub16DeepSeek.sendMu] and each Send's
// Wait terminates only once the WaitGroup has drained, there are no stale
// Done calls that could corrupt a subsequent Send's WaitGroup.
type ChanPubSub16DeepSeek[C ~chan V, V any] struct {
	// ping is used to orchestrate sends.
	ping ChanCaster[C, V]
	// wg orchestrates waits. It is reused per send; Add is called before
	// sends are issued, and adjusted after the send returns to account for
	// removed subscribers, then Wait drains it before the next send begins.
	wg sync.WaitGroup
	// sendMu is used to prevent concurrent sends. It serves no other purpose.
	sendMu sync.Mutex
	// sendingMu has two very important roles:
	//
	//	1. Synchronises reading subscribers then incrementing ping, with
	//	   incrementing subscribers, to avoid subscribers from stealing
	//	   messages intended for others (N.B. can still subscribe concurrently)
	//	2. Wait until ping has been incremented (if sends are in progress, i.e.
	//	   attempting to acquire or holding the write lock), or briefly
	//	   prevent sends from progressing (both are about avoiding wonky state)
	sendingMu sync.RWMutex
	// subscribers tracks the total number of subscribers.
	subscribers atomic.Int32
	// broken is used to indicate misuse of the API, and is closed only if
	// such misuse is detected.
	broken chan struct{}
}

// NewChanPubSub16DeepSeek constructs a new instance of [ChanPubSub16DeepSeek], using the provided
// channel as the underlying transport. The channel must be unbuffered
// (cap == 0). A nil channel is permitted. It panics if the channel is
// buffered.
func NewChanPubSub16DeepSeek[C ~chan V, V any](c C) *ChanPubSub16DeepSeek[C, V] {
	if cap(c) != 0 {
		panic(`bigbuff: chanpubsub: must use an unbuffered channel`)
	}
	var x ChanPubSub16DeepSeek[C, V]
	x.ping.C = c
	x.broken = make(chan struct{})
	return &x
}

// C returns the underlying channel, used by the receiver.
// This channel MAY be closed, but MUST NOT be sent to (except by
// [ChanPubSub16DeepSeek.Send]), and receiving from it MUST obey the contract.
// See also [ChanPubSub16DeepSeek], for the contract.
func (x *ChanPubSub16DeepSeek[C, V]) C() C {
	x.checkUsedFactoryFunction()
	x.checkBroken()
	return x.ping.C
}

// SubscribeContext immediately subscribes to the receiver, and returns a
// single-use iterator, which will yield all values sent to the receiver, and
// will automatically unsubscribe when the context is cancelled, the iterator
// stops early, or the receiver is closed.
// Behavior is undefined if the iterator is used more than once.
//
// WARNING: The returned iterator MUST be used immediately, OR, the context
// MUST be promptly cancelled, to avoid leaking resources, and/or potentially
// causing a deadlock.
func (x *ChanPubSub16DeepSeek[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	x.Subscribe() // N.B. handles guarding on check* functions

	// support nil (consistency - not best practice, but this pkg tends to)
	if ctx == nil {
		ctx = context.Background()
	}

	// For if we never call the iterator. Also used to handle multiple calls.
	stop := context.AfterFunc(ctx, x.Unsubscribe)

	return func(yield func(V) bool) {
		if yield == nil {
			// ensure we unsubscribe on all code paths
			if stop() {
				x.Unsubscribe()
			}
			panic(`bigbuff: chanpubsub: iterator yield func is nil`)
		}

		if !stop() {
			// Context was cancelled, or called more than once.
			// WARNING: Does not block on any still-running iterator.
			return
		}

		// always unsubscribe, exactly once, or never, if the context is never
		// canceled, AND the iterator is either never called, or is called at
		// least once, and the (single) call to get past stop() never completes
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
// It should not be necessary to actively consider dynamics such as the risk of
// deadlock, so long as subscribers obey the contract, as documented on
// [ChanPubSub16DeepSeek].
func (x *ChanPubSub16DeepSeek[C, V]) Send(value V) (sent int) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	if x.subscribers.Load() == 0 {
		return 0 // no subscribers (fast path)
	}

	// for sanity of the ping-pong communication pattern
	x.sendMu.Lock()
	defer x.sendMu.Unlock()

	// N.B. released after sending (after pings, before waiting for pongs)
	x.sendingMu.Lock()
	var skipSendingUnlock bool
	defer func() {
		if !skipSendingUnlock {
			x.sendingMu.Unlock()
		}
	}()

	x.checkBroken() // again (attempt to mitigate deadlocks caused by borked state)

	// we need to know the subscribers, so we can add to x.ping
	// synchronisation is important here, so INCREMENTS are mutually exclusive
	subscribers := int(x.subscribers.Load())
	if subscribers == 0 {
		return 0 // no subscribers (slow path)
	}

	x.sanityCheckSubscribersDelta(subscribers, 0) // just because

	var success bool
	defer func() {
		if !success {
			x.markBroken() // example: closed channel caused panic
		}
	}()

	// if nothing goes wrong, we always decrement x.ping back to 0 (send)
	if x.ping.Add(subscribers) != subscribers {
		panic(chanPubSubStateInvariantViolation)
	}

	// Register expected pongs. Under the sendingMu write lock, so no
	// positive Add can race between the subscribers snapshot above and
	// the WaitGroup registration below.
	x.wg.Add(subscribers)

	// ping! (send to channel)
	sent = x.ping.Send(value) // N.B. supports concurrent decrements

	// Adjust the WaitGroup for subscribers that were removed mid-send: their
	// steals reduced the count of actual receivers (== returned `sent`), so
	// we must subtract that difference from the WaitGroup's count to avoid
	// waiting forever for pongs that will never arrive. Safe: the max number
	// of Done calls equals `sent`, so the WaitGroup can never underflow.
	if delta := sent - subscribers; delta != 0 {
		x.wg.Add(delta)
	}

	skipSendingUnlock = true
	x.sendingMu.Unlock() // we can add subscribers while waiting for pongs

	// pong! (await appropriate number of calls to Wait)
	if sent != 0 {
		x.checkBroken() // re-check before blocking in wg.Wait
		x.wg.Wait()
		x.checkBroken() // and again after
	}

	success = true

	return
}

// Add returns the number of subscribers, after adding delta to the count. It
// is intended for more manual use, which requires the caller to be aware of
// and conform to the contract, as documented on [ChanPubSub16DeepSeek].
// This method acts as a low-level means to subscribe or unsubscribe from the
// receiver.
// A panic may occur if the input is invalid (e.g. decrementing subscribers
// below zero), or if the receiver is in a broken state, indicating misuse.
func (x *ChanPubSub16DeepSeek[C, V]) Add(delta int) (subscribers int) {
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic(`bigbuff: chanpubsub: delta out of bounds`)
	}

	x.checkUsedFactoryFunction()
	x.checkBroken()

	switch {
	case delta == 0:
		// Inspect case. No change.
		subscribers = int(x.subscribers.Load())
		x.sanityCheckSubscribersDelta(subscribers, 0)

	case delta < 0:
		// Unsubscribe case. If the contract was obeyed, after we attempt to
		// acquire the (read) lock, we are able to narrow down the state into
		// one of two scenarios:
		//
		//	1. Send in progress
		//	2. Send not in progress
		//
		// In the first scenario, we then pass through to the Add of x.ping, to
		// notify the sender, that we have adjusted number of subscribers.
		//
		// Restating because I am guaranteed to forget this:
		// Part of the API contract is that callers MUST NOT decrement past the
		// number of increments which they themselves have added. Ergo, there
		// MUST be at least one subscriber, and that subscriber MUST NOT go
		// away, since that would violate the contract (as just stated above).
		// An internal instance of [ChanCaster] is used to handle the delicate
		// dance that is sending as many messages as there are subscribers, and
		// [ChanCaster.Add] is used to increment said instance, as part of
		// [ChanPubSub16DeepSeek.Add], having acquired the write side of x.sendingMu.
		// If we succeed in acquiring the read side of x.sendingMu, then x.ping
		// is guaranteed to be zero. If x.ping is _non-zero_, then, as an
		// unobvious consequence of the ping-pong communication pattern, it is
		// GUARANTEED that THIS subscriber HAS NOT received a message, and it
		// is therefore GUARANTEED that x.ping will not be decremented past
		// zero, since we only add to x.ping immediately before sending, send
		// blocks on the same number of receives (aside from decrements) being
		// performed, as x.ping is incremented by, and adding and sending using
		// x.ping is guarded by both x.sendMu and x.sendingMu.
		//
		// ... rather complex, yes.
		ok := x.sendingMu.TryRLock()
		// N.B. this loop is to handle state transition (send in progress)
		for !ok && x.ping.Add(0) == 0 {
			x.checkBroken()   // attempts to mitigate deadlock risk on misuse...
			runtime.Gosched() // do not starve the sender while it arms ping
			ok = x.sendingMu.TryRLock()
		}
		subscribers = x.addSubscribers(delta)
		if ok {
			x.sendingMu.RUnlock() // unlock, before possible panics
		}
		x.sanityCheckSubscribersDelta(subscribers, delta)
		if !ok {
			// decrement, per delta
			var success bool
			defer func() {
				if !success {
					x.markBroken()
				}
			}()
			// WARNING: Relies on the caller ensuring no concurrent receives.
			x.ping.Add(delta)
			success = true
		}

	default:
		// Subscribe case. Mutually exclusive with sending, but able to run
		// concurrently with other attempts to subscribe.
		x.sendingMu.RLock()
		defer x.sendingMu.RUnlock()
		subscribers = x.addSubscribers(delta)
		x.sanityCheckSubscribersDelta(subscribers, delta)
	}

	return
}

// Wait is mandatory after each message received by each subscriber, serving as
// means to synchronise the caller (a subscriber, that received a message) with
// the sender, for the purpose of ensuring that all messages are broadcast, to
// all subscribers.
//
// Wait is a single atomic decrement on the hot path; there is no mutex, no
// channel send, no goroutine parking. The corresponding [ChanPubSub16DeepSeek.Send]
// goroutine parks on the WaitGroup's runtime semaphore until the last pong
// arrives, at which point it wakes.
//
// Because [ChanPubSub16DeepSeek.Wait] delegates to a shared [sync.WaitGroup], misuse
// (calling Wait without a corresponding successful receive, or calling it
// twice for the same receive) will cause the receiver to panic with the
// standard "sync: negative WaitGroup counter" message. This is intentional:
// silent corruption of the send/receive ledger would be far worse.
func (x *ChanPubSub16DeepSeek[C, V]) Wait() {
	x.checkUsedFactoryFunction()
	x.checkBroken()
	// The WaitGroup is shared across rounds, and [ChanPubSub16DeepSeek.Send]
	// both pre-charges it (wg.Add(subscribers)) and compensates it for
	// mid-flight unsubscribes (wg.Add(sent - subscribers)) while holding the
	// write side of sendingMu. Taking the read side here serialises every
	// wg.Done against those wg.Add calls: without this, a pong could land
	// between the pre-charge and the compensation, driving the counter
	// negative (sync: negative WaitGroup counter) or corrupting a later
	// round (WaitGroup reuse). Send never blocks on the WaitGroup while
	// holding sendingMu, so this cannot deadlock.
	x.sendingMu.RLock()
	defer x.sendingMu.RUnlock()
	x.wg.Done()
}

// Subscribe is an alias for calling [ChanPubSub16DeepSeek.Add] with a delta of 1.
func (x *ChanPubSub16DeepSeek[C, V]) Subscribe() {
	x.Add(1)
}

// Unsubscribe is an alias for calling [ChanPubSub16DeepSeek.Add] with a delta of -1.
func (x *ChanPubSub16DeepSeek[C, V]) Unsubscribe() {
	x.Add(-1)
}

// checkBroken panics if the receiver is in a broken state, indicating misuse.
// Called as part of all public methods, as a mitigation to reduce the risk of
// deadlocks, brought about by undefined behavior, caused by API misuse.
func (x *ChanPubSub16DeepSeek[C, V]) checkBroken() {
	select {
	case <-x.broken:
		panic(chanPubSubStateInvariantViolation)
	default:
	}
}

func (x *ChanPubSub16DeepSeek[C, V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic(`bigbuff: chanpubsub: must use factory function`)
	}
}

// markBroken ensures that the broken channel is closed.
// The janky simplistic implementation is deliberate, as this indicates API
// misuse, and is an exceptional scenario, surfacing as panics on method calls.
func (x *ChanPubSub16DeepSeek[C, V]) markBroken() {
	defer func() {
		recover() // we're going to panic shortly, or the channel was already closed
	}()
	close(x.broken)
}

// sanityCheckSubscribersDelta checks conditions such as overflow, underflow,
// and valid bounds, for the subscribers atomic counter, assessing both old and
// new values, after ChanPubSub16DeepSeek.addSubscribers.
func (x *ChanPubSub16DeepSeek[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
	// 1. Compute old value
	oldSubscribers := int(int32(subscribers) - int32(delta))

	// 2. Detect overflow or underflow
	if (delta > 0 && oldSubscribers >= subscribers) ||
		(delta < 0 && oldSubscribers <= subscribers) {
		x.markBroken()
		panic("bigbuff: chanpubsub: addition overflowed or underflowed")
	}

	// 3. Negative subscribers are not allowed (new value)
	if subscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative subscribers`)
	}

	// 4. Negative subscribers are not allowed (old value)
	if oldSubscribers < 0 {
		x.markBroken()
		panic(`bigbuff: chanpubsub: negative old subscribers`)
	}
}

// addSubscribers performs an atomic operation to add delta to the subscribers.
func (x *ChanPubSub16DeepSeek[C, V]) addSubscribers(delta int) int {
	return int(x.subscribers.Add(int32(delta)))
}
