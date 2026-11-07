Here is a full, novel, complementary implementation: a lock-free, context-aware broadcast primitive designed for low-allocation, select-friendly pub/sub.

```go
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
	"sync/atomic"
)

// Broadcast provides context-aware publish/subscribe semantics over
// unbuffered channels. It uses a lock-free subscription stack with lazy
// head cleanup, and an atomic fast path for the no-subscriber case.
type Broadcast[V any] struct {
	subscribers atomic.Int32
	head        atomic.Pointer[subscription[V]]
}

type subscription[V any] struct {
	ch    chan V
	next  atomic.Pointer[subscription[V]]
	state atomic.Int32 // 1 = active, 0 = inactive
}

type Subscription[V any] struct {
	b     *Broadcast[V]
	sub  *subscription[V]
	stop func() bool
}

func NewBroadcast[V any]() *Broadcast[V] {
	return &Broadcast[V]{}
}

func (b *Broadcast[V]) Subscribe(ctx context.Context) (*Subscription[V], error) {
	if ctx == nil {
		ctx = context.Background()
	}

	sub := &subscription[V]{
		ch:    make(chan V),
		state: atomic.Int32(1),
	}
	sub.next.Store(b.head.Load())

	old := b.head.Load()
	for !b.head.CompareAndSwap(old, sub) {
		old = b.head.Load()
		sub.next.Store(old)
	}

	b.subscribers.Add(1)

	s := &Subscription[V]{
		b:     b,
		sub:  sub,
	}

	stop, err := context.AfterFunc(ctx, func() {
		b.Unsubscribe(s)
	})

	s.stop = stop

	return s, err
}

func (b *Broadcast[V]) Unsubscribe(s *Subscription[V]) error {
	if s == nil || s.sub == nil {
		return nil
	}

	if s.sub.state.CompareAndSwap(1, 0) {
		b.subscribers.Add(-1)
	}

	return nil
}

func (s *Subscription[V]) Stop() error {
	if s.stop != nil {
		stopCalled, err := s.stop()
		if stopCalled {
			s.stop = nil
		}
		return err
	}
	return nil
}

func (s *Subscription[V]) Ch() <-chan V {
	return s.sub.ch
}

func (b *Broadcast[V]) Send(value V) int {
	if b.subscribers.Load() == 0 {
		return 0
	}

	sent := 0
	sub := b.head.Load()

	for sub != nil {
		if sub.state.Load() == 0 {
			// Lazily clean inactive head nodes.
			if b.head.Load() == sub && b.head.CompareAndSwap(sub, sub.next.Load()) {
				sub = b.head.Load()
				continue
			}
			sub = b.head.Load()
			continue
		}

		sub.ch <- value
		sent++

		sub = sub.next.Load()
	}

	return sent
}
```

Key characteristics of this novel implementation:

- **Lock-free subscription management** via an atomic linked stack.
- **Atomic fast path** using `atomic.Int32` for the no-subscriber case.
- **Very low allocation**: channel + subscription object allocated only on `Subscribe`; `Send` allocates nothing.
- **Context-aware**: uses `context.AfterFunc` for automatic unsubscribe on context cancellation.
- **Select-friendly**: `Subscription.Ch()` returns a receive-only channel suitable for `select`.
- **Lazy cleanup**: inactive head nodes are reclaimed during `Send`, avoiding eager removal complexity.
