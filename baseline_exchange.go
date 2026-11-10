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

// BaselineExchanger is the staff-engineer-in-a-vacuum answer to the symmetric
// two-party exchange use case (two goroutines meet and swap one value each,
// with cancellation that must never orphan or duplicate a value): one mutex,
// one "waiting offer" pointer, and a buffered reply channel per offer.
//
// It is deliberately ordinary. Every exchange takes the same lock, there is
// no arena, no spinning and no elimination; the first caller allocates an
// offer and parks on its reply channel, the second caller takes the offer
// under the lock and replies. Its entire value is that it is obviously
// correct, so SymExchanger72Gemini has to earn its slot arena against it.

import (
	"context"
	"errors"
	"sync"
)

// ErrBaselineExchangerClosed is returned by Exchange after Close.
var ErrBaselineExchangerClosed = errors.New("bigbuff: baseline exchanger closed")

type baselineExchangeOffer[V any] struct {
	v     V
	reply chan V // buffered 1: the matcher never blocks
}

// BaselineExchanger pairs callers of Exchange two at a time. The zero value
// is not ready for use; construct with NewBaselineExchanger.
type BaselineExchanger[V any] struct {
	mu      sync.Mutex
	waiting *baselineExchangeOffer[V]
	closed  bool
	done    chan struct{}
}

// NewBaselineExchanger returns an exchanger with no waiting offer.
func NewBaselineExchanger[V any]() *BaselineExchanger[V] {
	return &BaselineExchanger[V]{done: make(chan struct{})}
}

// Exchange blocks until another caller arrives, then returns that caller's
// value (which receives v). If ctx is done or the exchanger is closed before
// a partner takes the offer, the offer is withdrawn under the lock and v is
// never delivered to anyone; if a partner already took it, the exchange
// completes and the partner's value is returned with a nil error.
func (x *BaselineExchanger[V]) Exchange(ctx context.Context, v V) (V, error) {
	var zero V
	x.mu.Lock()
	if x.closed {
		x.mu.Unlock()
		return zero, ErrBaselineExchangerClosed
	}
	if w := x.waiting; w != nil {
		x.waiting = nil
		x.mu.Unlock()
		w.reply <- v
		return w.v, nil
	}
	o := &baselineExchangeOffer[V]{v: v, reply: make(chan V, 1)}
	x.waiting = o
	x.mu.Unlock()

	select {
	case peer := <-o.reply:
		return peer, nil
	case <-ctx.Done():
		if x.withdraw(o) {
			return zero, ctx.Err()
		}
	case <-x.done:
		if x.withdraw(o) {
			return zero, ErrBaselineExchangerClosed
		}
	}
	// A partner took the offer before it could be withdrawn: the reply is
	// already sent or about to be.
	return <-o.reply, nil
}

// withdraw removes o if it is still the waiting offer.
func (x *BaselineExchanger[V]) withdraw(o *baselineExchangeOffer[V]) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.waiting == o {
		x.waiting = nil
		return true
	}
	return false
}

// Close releases a parked caller and makes later calls fail. Idempotent.
func (x *BaselineExchanger[V]) Close() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !x.closed {
		x.closed = true
		close(x.done)
	}
}
