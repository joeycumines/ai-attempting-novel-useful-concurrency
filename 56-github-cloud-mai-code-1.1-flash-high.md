This was a cloud agent session. It required kicking to produce any meaningful output. It was cheap. I guess.

fanout.go:
```go
package bigbuff

import "sync/atomic"

// Fanout is a broadcast fan-out with a low-allocation steady state.
//
// The design is intentionally simple: a single immutable snapshot of the active
// subscribers is replaced atomically on churn, while the common send path only
// walks the current snapshot. This preserves a predictable, near-zero-allocation
// send path while still allowing callers to add or remove subscribers without
// global locks or callback churn.
//
// Each subscription is backed by a channel that can be observed via
// [Subscription.Chan], polled with select statements, and closed independently
// via [Subscription.Close]. A closed subscription stops receiving future values
// without requiring the calling code to close the underlying channel.
type Fanout[V any] struct {
	state atomic.Pointer[fanoutState[V]]
}

type fanoutState[V any] struct {
	subs []*fanoutSub[V]
}

type fanoutSub[V any] struct {
	ch     chan V
	done   chan struct{}
	closed atomic.Bool
}

// Subscription represents a single receiver for the fan-out.
type Subscription[V any] struct {
	fan *Fanout[V]
	sub *fanoutSub[V]
}

// NewFanout constructs a fresh broadcast fan-out.
func NewFanout[V any]() *Fanout[V] {
	var f Fanout[V]
	f.state.Store(&fanoutState[V]{})
	return &f
}

// Subscribe creates a new subscription with a channel sized to one value.
func (f *Fanout[V]) Subscribe() *Subscription[V] {
	return f.SubscribeChan(make(chan V, 1))
}

// SubscribeChan creates a new subscription using the provided channel.
//
// The channel is considered the subscriber's receive path. If ch is nil, a
// fresh unbuffered channel is created automatically.
func (f *Fanout[V]) SubscribeChan(ch chan V) *Subscription[V] {
	if ch == nil {
		ch = make(chan V)
	}
	sub := &fanoutSub[V]{ch: ch, done: make(chan struct{})}
	for {
		old := f.state.Load()
		next := &fanoutState[V]{subs: append(append([]*fanoutSub[V]{}, old.subs...), sub)}
		if f.state.CompareAndSwap(old, next) {
			return &Subscription[V]{fan: f, sub: sub}
		}
	}
}

// Len reports the number of active subscriptions.
func (f *Fanout[V]) Len() int {
	state := f.state.Load()
	if state == nil {
		return 0
	}
	return len(state.subs)
}

// Send broadcasts a value to all active subscriptions and returns the number of
// recipients that accepted the value before their channels were closed or
// unsubscribed.
func (f *Fanout[V]) Send(value V) int {
	state := f.state.Load()
	if state == nil || len(state.subs) == 0 {
		return 0
	}

	delivered := 0
	for _, sub := range state.subs {
		if sub == nil {
			continue
		}
		select {
		case <-sub.done:
			continue
		case sub.ch <- value:
			delivered++
		}
	}
	return delivered
}

// Broadcast is an alias for Send.
func (f *Fanout[V]) Broadcast(value V) int {
	return f.Send(value)
}

// Chan returns the receive channel used by the subscription.
func (s *Subscription[V]) Chan() <-chan V {
	if s == nil || s.sub == nil {
		return nil
	}
	return s.sub.ch
}

// Done closes when the subscription is removed or explicitly closed.
func (s *Subscription[V]) Done() <-chan struct{} {
	if s == nil || s.sub == nil {
		return nil
	}
	return s.sub.done
}

// Close unsubscribes the receiver without closing the underlying channel.
// Values already queued in the channel may still be drained by callers; no
// further values are sent after Close returns.
func (s *Subscription[V]) Close() {
	if s == nil || s.sub == nil {
		return
	}
	if !s.sub.closed.CompareAndSwap(false, true) {
		return
	}
	close(s.sub.done)
	if s.fan != nil {
		s.fan.remove(s.sub)
	}
}

func (f *Fanout[V]) remove(sub *fanoutSub[V]) {
	for {
		old := f.state.Load()
		if old == nil || len(old.subs) == 0 {
			return
		}
		nextSubs := make([]*fanoutSub[V], 0, len(old.subs))
		changed := false
		for _, candidate := range old.subs {
			if candidate == sub {
				changed = true
				continue
			}
			nextSubs = append(nextSubs, candidate)
		}
		if !changed {
			return
		}
		next := &fanoutState[V]{subs: nextSubs}
		if f.state.CompareAndSwap(old, next) {
			return
		}
	}
}
```

fanout_test.go:
```go
package bigbuff

import "testing"

func TestFanoutSendAndReceive(t *testing.T) {
	f := NewFanout[int]()
	s1 := f.SubscribeChan(make(chan int, 4))
	s2 := f.SubscribeChan(make(chan int, 4))

	if got := f.Send(42); got != 2 {
		t.Fatalf("Send() = %d; want 2", got)
	}

	if got := <-s1.Chan(); got != 42 {
		t.Fatalf("s1 got %d; want 42", got)
	}
	if got := <-s2.Chan(); got != 42 {
		t.Fatalf("s2 got %d; want 42", got)
	}
}

func TestFanoutCloseRemovesSubscription(t *testing.T) {
	f := NewFanout[int]()
	s1 := f.SubscribeChan(make(chan int, 1))
	s2 := f.SubscribeChan(make(chan int, 1))

	s1.Close()
	if got := f.Len(); got != 1 {
		t.Fatalf("Len() = %d; want 1", got)
	}

	if got := f.Send(7); got != 1 {
		t.Fatalf("Send() = %d; want 1", got)
	}
	if got := <-s2.Chan(); got != 7 {
		t.Fatalf("s2 got %d; want 7", got)
	}
	select {
	case <-s1.Done():
	default:
		t.Fatal("s1.Done() was not closed")
	}
}

func TestFanoutSelectReady(t *testing.T) {
	f := NewFanout[int]()
	s := f.SubscribeChan(make(chan int, 1))

	go func() {
		f.Send(99)
	}()

	select {
	case v := <-s.Chan():
		if v != 99 {
			t.Fatalf("received %d; want 99", v)
		}
	case <-s.Done():
		t.Fatal("subscription was closed unexpectedly")
	}
}

func TestFanoutConcurrentSend(t *testing.T) {
	f := NewFanout[int]()
	for i := 0; i < 8; i++ {
		ch := make(chan int, 16)
		go func(ch chan int) {
			for range 16 {
				<-ch
			}
		}(ch)
		f.SubscribeChan(ch)
	}

	for i := 0; i < 16; i++ {
		if got := f.Send(i); got != 8 {
			t.Fatalf("iteration %d: Send() = %d; want 8", i, got)
		}
	}
}
```
