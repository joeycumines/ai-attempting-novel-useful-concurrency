// Provenance: 56-github-cloud-mai-code-1.1-flash-high.md (Mai Code 1.1 Flash, response #56). Namespaced to coexist with response #45 Fanout.
package bigbuff

import (
	"sync"
	"sync/atomic"
)

// Fanout56MaiCode is a broadcast fan-out with a low-allocation steady state.
//
// The design is intentionally simple: a single immutable snapshot of the active
// subscribers is replaced atomically on churn, while Send serializes publishers
// and walks the current snapshot. Subscription changes copy the slice on churn;
// the steady Send path allocates no subscriber snapshot. Per-subscriber channel
// sends may block when a buffer is full, and Close waits for any send already in
// progress before returning.
//
// Each subscription is backed by a channel that can be observed via
// [Subscription56MaiCode.Chan], polled with select statements, and closed independently
// via [Subscription56MaiCode.Close]. A closed subscription stops receiving future values
// without requiring the calling code to close the underlying channel.
type Fanout56MaiCode[V any] struct {
	sendMu sync.Mutex // gives concurrent publishers one consistent order for every subscriber
	state  atomic.Pointer[fanout56State[V]]
}

type fanout56State[V any] struct {
	subs []*fanout56Sub[V]
}

type fanout56Sub[V any] struct {
	sendMu sync.Mutex // lets Close wait until any in-flight send has left this subscriber
	ch     chan V
	done   chan struct{}
	closed atomic.Bool
}

// Subscription56MaiCode represents a single receiver for the fan-out.
type Subscription56MaiCode[V any] struct {
	fan *Fanout56MaiCode[V]
	sub *fanout56Sub[V]
}

// NewFanout56MaiCode constructs a fresh broadcast fan-out.
func NewFanout56MaiCode[V any]() *Fanout56MaiCode[V] {
	var f Fanout56MaiCode[V]
	f.state.Store(&fanout56State[V]{})
	return &f
}

// Subscribe creates a new subscription with a channel sized to one value.
func (f *Fanout56MaiCode[V]) ensureState() {
	if f == nil {
		panic("bigbuff: fanout56: nil broadcaster")
	}
	if f.state.Load() == nil {
		f.state.CompareAndSwap(nil, &fanout56State[V]{})
	}
}

func (f *Fanout56MaiCode[V]) Subscribe() *Subscription56MaiCode[V] {
	return f.SubscribeChan(make(chan V, 1))
}

// SubscribeChan creates a new subscription using the provided channel.
//
// The channel is considered the subscriber's receive path. If ch is nil, a
// fresh unbuffered channel is created automatically. Each independent
// subscription should have a distinct channel, and callers must not send to or
// close the channel themselves.
func (f *Fanout56MaiCode[V]) SubscribeChan(ch chan V) *Subscription56MaiCode[V] {
	f.ensureState()
	if ch == nil {
		ch = make(chan V)
	}
	sub := &fanout56Sub[V]{ch: ch, done: make(chan struct{})}
	for {
		old := f.state.Load()
		next := &fanout56State[V]{subs: append(append([]*fanout56Sub[V]{}, old.subs...), sub)}
		if f.state.CompareAndSwap(old, next) {
			return &Subscription56MaiCode[V]{fan: f, sub: sub}
		}
	}
}

// Len reports the number of active subscriptions.
func (f *Fanout56MaiCode[V]) Len() int {
	f.ensureState()
	state := f.state.Load()
	if state == nil {
		return 0
	}
	return len(state.subs)
}

// Send broadcasts a value to all subscriptions in one immutable snapshot and
// returns the number of channel sends accepted. Concurrent Send calls are
// serialized so all subscribers observe the same publication order. Each
// subscriber send may block while its channel is full; Close releases that
// subscriber's blocked send.
func (f *Fanout56MaiCode[V]) Send(value V) int {
	f.ensureState()
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	state := f.state.Load()
	if state == nil || len(state.subs) == 0 {
		return 0
	}

	delivered := 0
	for _, sub := range state.subs {
		if sub == nil {
			continue
		}
		sub.sendMu.Lock()
		if sub.closed.Load() {
			sub.sendMu.Unlock()
			continue
		}
		select {
		case <-sub.done:
		case sub.ch <- value:
			delivered++
		}
		sub.sendMu.Unlock()
	}
	return delivered
}

// Broadcast is an alias for Send.
func (f *Fanout56MaiCode[V]) Broadcast(value V) int {
	return f.Send(value)
}

// Chan returns the receive channel used by the subscription.
func (s *Subscription56MaiCode[V]) Chan() <-chan V {
	if s == nil || s.sub == nil {
		return nil
	}
	return s.sub.ch
}

// Done closes when the subscription is removed or explicitly closed.
func (s *Subscription56MaiCode[V]) Done() <-chan struct{} {
	if s == nil || s.sub == nil {
		return nil
	}
	return s.sub.done
}

// Close unsubscribes the receiver without closing the underlying channel.
// Values already queued in the channel may still be drained by callers; no
// further values are sent after Close returns.
func (s *Subscription56MaiCode[V]) Close() {
	if s == nil || s.sub == nil {
		return
	}
	if !s.sub.closed.CompareAndSwap(false, true) {
		return
	}
	close(s.sub.done)
	s.sub.sendMu.Lock()
	s.sub.sendMu.Unlock()
	if s.fan != nil {
		s.fan.remove(s.sub)
	}
}

func (f *Fanout56MaiCode[V]) remove(sub *fanout56Sub[V]) {
	for {
		old := f.state.Load()
		if old == nil || len(old.subs) == 0 {
			return
		}
		nextSubs := make([]*fanout56Sub[V], 0, len(old.subs))
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
		next := &fanout56State[V]{subs: nextSubs}
		if f.state.CompareAndSwap(old, next) {
			return
		}
	}
}
