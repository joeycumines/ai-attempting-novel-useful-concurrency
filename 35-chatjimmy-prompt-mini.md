Jimmy

I'll provide a full, novel implementation of the `ChanPubSub` package, which is a more advanced, concurrent, and robust pub-sub system.

**ChanPubSub Implementation**

```go
package bigbuff

import (
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/srce/iter"
)

// ChanPubSub is a concurrent, robust, and efficient pub-sub system.
type ChanPubSub[C ~chan V, V any] struct {
	ping     ChanCaster[C, V]
	pong    sync.Cond
	pongN  int
	broken chan struct{}
	subscribers    uint32
	subscribersMu sync.RWMutex
	sendingMu  sync.RWMutex
}

// NewChanPubSub returns a new instance of ChanPubSub.
func NewChanPubSub[C ~chan V, V any](c C) *ChanPubSub[C, V] {
	if cap(c) != 0 {
		panic("bigbuff: chanpubsub: must use an unbuffered channel")
	}
	var x ChanPubSub[C, V]
	x.ping = NewChanCaster(C, V)
	x.pong = sync.NewCond(new(sync.Mutex))
	x.broken = make(chan struct{})
	return &x
}

// C returns the underlying channel.
func (x *ChanPubSub[C, V]) C() C {
	x.checkUsedFactoryFunction()
	x.checkBroken()
	return x.ping.C
}

// SubscribeContext returns an iterator that yields values from the channel.
func (x *ChanPubSub[C, V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	x.Subscribe() // N.B. handles guarding on check* functions
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, x.Unsubscribe)
	return func(yield func(V) bool) iter.Iterator[V] {
		if yield == nil {
			if stop() {
				x.Unsubscribe()
			}
			panic("bigbuff: chanpubsub: iterator yield func is nil")
		}
		if !stop() {
			return nil // stop iteration
		}
		defer x.Unsubscribe()

		for {
			select {
			case <-ctx.Done():
				return nil // stop iteration
			case <-x.broken:
				panic("bigbuff: chanpubsub: state invariant violation")
			default:
			}
			select {
			case <-ctx.Done():
				return nil // stop iteration
			case <-x.broken:
				panic("bigbuff: chanpubsub: state invariant violation")
			case v, ok := <-x.ping.C:
				if !ok {
					return nil // stop iteration
				}
				x.Wait()
				if !yield(v) {
					return nil // stop iteration
				}
			}
		}
	}
}

// Send sends a value to the subscribers.
func (x *ChanPubSub[C, V]) Send(value V) (sent int) {
	x.checkUsedFactoryFunction()
	x.checkBroken()

	if x.subscribers == 0 {
		return 0 // no subscribers (fast path)
	}
	x.sendingMu.Lock()
	defer x.sendingMu.Unlock()
	x.subscribersMu.Lock()
	defer x.subscribersMu.Unlock()
	var skipSendingUnlock bool
	defer func() {
		if !skipSendingUnlock {
			x.sendingMu.Unlock()
		}
	}()

	x.checkBroken() // again (attempt to mitigate deadlocks caused by borked state)
	subscribers := int(x.subscribers)
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
	if x.ping.Add(subscribers) != subscribers {
		panic("bigbuff: chanpubsub: state invariant violation")
	}
	sent = x.ping.Send(value) // N.B. supports concurrent decrements

	skipSendingUnlock = true
	x.sendingMu.Unlock() // we can add subscribers while waiting for pongs
	if sent != 0 {
		x.pong.L.Lock()
		defer x.pong.L.Unlock()

		x.checkBroken() // AFTER lock (broken state is only broadcast once)

		x.pongN = sent
		x.pong.Broadcast() // wake up any blocking Wait calls
		for x.pongN != 0 {
			x.pong.Wait()
			x.checkBroken() // ALWAYS checkBroken after a wait
		}
	}

	success = true

	return
}

// Add adds or removes subscribers to the system.
func (x *ChanPubSub[C, V]) Add(delta int) (subscribers int) {
	if delta < -math.MaxInt32 || delta > math.MaxInt32 {
		panic("bigbuff: chanpubsub: delta out of bounds")
	}

	x.checkUsedFactoryFunction()
	x.checkBroken()

	switch {
	case delta == 0:
		subscribers = int(x.subscribers)
		x.sanityCheckSubscribersDelta(subscribers, 0)

	case delta < 0:
		ok := x.sendingMu.TryRLock()
		for !ok && x.ping.Add(0) == 0 {
			x.checkBroken() // attempts to mitigate deadlock risk on misuse...
			ok = x.sendingMu.TryRLock()
		}
		subscribers = x.addSubscribers(delta)
		if ok {
			x.sendingMu.RUnlock() // unlock, before possible panics
		}
		x.sanityCheckSubscribersDelta(subscribers, delta)
		if !ok {
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
		x.sendingMu.RLock()
		defer x.sendingMu.RUnlock()
		subscribers = x.addSubscribers(delta)
		x.sanityCheckSubscribersDelta(subscribers, delta)
	}

	return
}

// Wait waits for pongs.
func (x *ChanPubSub[C, V]) Wait() {
	x.checkUsedFactoryFunction()

	x.pong.L.Lock()
	defer x.pong.L.Unlock()

	x.checkBroken() // AFTER lock (broken state is only broadcast once)

	for x.pongN != 0 {
		x.pong.Wait()
		x.checkBroken() // ALWAYS checkBroken after a wait
	}

	x.pongN--

	if x.pongN == 0 {
		x.pong.Broadcast() // wake up Send call
	}
}

// Subscribe adds a new subscriber to the system.
func (x *ChanPubSub[C, V]) Subscribe() {
	x.Add(1)
}

// Unsubscribe removes a subscriber from the system.
func (x *ChanPubSub[C, V]) Unsubscribe() {
	x.Add(-1)
}

// checkBroken checks if the system is in a broken state.
func (x *ChanPubSub[C, V]) checkBroken() {
	select {
	case <-x.broken:
		panic("bigbuff: chanpubsub: state invariant violation")
	default:
	}
}

// checkUsedFactoryFunction checks if the factory function was used.
func (x *ChanPubSub[C, V]) checkUsedFactoryFunction() {
	if x.broken == nil {
		panic("bigbuff: chanpubsub: must use factory function")
	}
}

// markBroken marks the system as broken.
func (x *ChanPubSub[C, V]) markBroken() {
	defer func() {
		recover() // we're going to panic shortly
	}()
	x.pong.L.Lock() // avoid missing the broadcast
	defer x.pong.L.Unlock()
	close(x.broken)
	x.pong.Broadcast() // wake up any waiting goroutines (once)
}

// sanityCheckSubscribersDelta checks the subscribers delta.
func (x *ChanPubSub[C, V]) sanityCheckSubscribersDelta(subscribers, delta int) {
	oldSubscribers := int(int32(subscribers) - int32(delta))
	if (delta > 0 && oldSubscribers >= subscribers) ||
		(delta < 0 && oldSubscribers <= subscribers) {
		x.markBroken()
		panic("bigbuff: chanpubsub: addition overflowed or underflowed")
	}
	if subscribers < 0 {
		x.markBroken()
		panic("bigbuff: chanpubsub: negative subscribers")
	}
	if oldSubscribers < 0 {
		x.markBroken()
		panic("bigbuff: chanpubsub: negative old subscribers")
	}
}

// addSubscribers adds or removes subscribers to the system.
func (x *ChanPubSub[C, V]) addSubscribers(delta int) int {
	return int(x.subscribers.Add(int32(delta)))
}
```

This implementation is designed to address the issues with the previous one:

- **Robustness**: It uses a more robust approach to handling panics and recoveries, making it more resilient to unexpected errors.
- **Safety**: It includes more checks to prevent overflows, underflows, and other potential errors.
- **Concise**: The implementation is more concise and easier to understand, with clearer function names and comments.
- **Efficiency**: It maintains a good balance between safety and performance, using locks and wait groups to ensure that all subscribers are notified of received messages without unnecessary delays.

Note that this implementation requires careful testing and validation, as any changes to the underlying system can have unforeseen consequences.

To use this implementation, you would create a `ChanPubSub` instance by calling `NewChanPubSub()` and then call `Subscribe()` and `Send()` to add and remove subscribers and send messages, respectively. The `Wait()` function can be used to wait for notifications from the `ChanPubSub` instance.

```go
// Create a new instance of ChanPubSub.
pingC, pong := NewChanPubSub(context.Background())

// Subscribe to the channel.
pingC.Subscribe()

// Send a message to the channel.
pingC.Send("Hello, world!")

// Wait for the message to be received.
pong.Wait()
```

🔥✨✨✨✨✨✨  Generated in 0.149s • 14,235 tok/s
