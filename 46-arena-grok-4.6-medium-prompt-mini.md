```go
/*
   Copyright 2026 Joseph Cumines

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   you may obtain a copy of the License at

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
	"runtime"
	"sync"
	"sync/atomic"
)

const chanHubStateInvariantViolation = `bigbuff: chanhub: state invariant violation`

var chanHubReady = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

type chanHubSlot[V any] struct {
	seq atomic.Uint64
	val V
}

type ChanHub[V any] struct {
	slots []chanHubSlot[V]
	mask  uint64

	mu     sync.Mutex
	closed bool
	nsub   int32
	head   atomic.Uint64
	min    uint64
	dataCh chan struct{}
	roomCh chan struct{}

	subHead *ChanHubCursor[V]
}

type ChanHubCursor[V any] struct {
	h      *ChanHub[V]
	seq    atomic.Uint64
	dead   atomic.Uint32
	inNext atomic.Int32
	prev   *ChanHubCursor[V]
	next   *ChanHubCursor[V]
}

func NewChanHub[V any](size int) *ChanHub[V] {
	if size <= 0 || size > math.MaxInt32 || size&(size-1) != 0 {
		panic(`bigbuff: chanhub: size must be a positive power of two`)
	}
	return &ChanHub[V]{
		slots: make([]chanHubSlot[V], size),
		mask:  uint64(size - 1),
	}
}

func (h *ChanHub[V]) Close() {
	h.checkFactory()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		panic(`bigbuff: chanhub: already closed`)
	}
	h.closed = true
	h.signalLocked(&h.dataCh)
	h.signalLocked(&h.roomCh)
}

func (h *ChanHub[V]) Publish(v V) uint64 {
	for {
		seq, ok := h.TryPublish(v)
		if ok {
			return seq
		}
		<-h.Avail()
	}
}

func (h *ChanHub[V]) TryPublish(v V) (seq uint64, ok bool) {
	h.checkFactory()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		panic(`bigbuff: chanhub: publish on closed hub`)
	}
	seq = h.head.Load()
	if h.nsub != 0 {
		capn := uint64(len(h.slots))
		if seq-h.min >= capn {
			h.min = h.minLocked()
			if seq-h.min >= capn {
				if h.roomCh == nil {
					h.roomCh = make(chan struct{})
				}
				return 0, false
			}
		}
	}
	slot := &h.slots[seq&h.mask]
	slot.val = v
	slot.seq.Store(seq + 1)
	h.head.Store(seq + 1)
	h.signalLocked(&h.dataCh)
	return seq, true
}

func (h *ChanHub[V]) Load() (v V, seq uint64, ok bool) {
	h.checkFactory()
	h.mu.Lock()
	defer h.mu.Unlock()
	head := h.head.Load()
	if head == 0 {
		return
	}
	seq = head - 1
	v = h.slots[seq&h.mask].val
	ok = true
	return
}

func (h *ChanHub[V]) Seq() uint64 {
	h.checkFactory()
	return h.head.Load()
}

func (h *ChanHub[V]) N() int {
	h.checkFactory()
	h.mu.Lock()
	defer h.mu.Unlock()
	return int(h.nsub)
}

func (h *ChanHub[V]) Avail() <-chan struct{} {
	h.checkFactory()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return chanHubReady
	}
	if h.nsub == 0 {
		return chanHubReady
	}
	head := h.head.Load()
	capn := uint64(len(h.slots))
	if head-h.min >= capn {
		h.min = h.minLocked()
	}
	if head-h.min < capn {
		return chanHubReady
	}
	if h.roomCh == nil {
		h.roomCh = make(chan struct{})
	}
	return h.roomCh
}

func (h *ChanHub[V]) Wait(after uint64) <-chan struct{} {
	h.checkFactory()
	if h.head.Load() > after {
		return chanHubReady
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.head.Load() > after {
		return chanHubReady
	}
	if h.dataCh == nil {
		h.dataCh = make(chan struct{})
	}
	return h.dataCh
}

func (h *ChanHub[V]) Attach(c *ChanHubCursor[V]) {
	h.checkFactory()
	if c == nil {
		panic(`bigbuff: chanhub: nil cursor`)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		panic(`bigbuff: chanhub: attach on closed hub`)
	}
	if c.h != nil || c.prev != nil || c.next != nil || c.inNext.Load() != 0 {
		panic(`bigbuff: chanhub: cursor already attached`)
	}
	if h.nsub == math.MaxInt32 {
		panic(`bigbuff: chanhub: subscribers overflow`)
	}
	c.dead.Store(0)
	c.h = h
	c.seq.Store(h.head.Load())
	c.next = h.subHead
	if h.subHead != nil {
		h.subHead.prev = c
	}
	h.subHead = c
	h.nsub++
}

func (h *ChanHub[V]) Detach(c *ChanHubCursor[V]) {
	h.checkFactory()
	if c == nil || c.h != h {
		panic(`bigbuff: chanhub: cursor not attached to this hub`)
	}
	c.dead.Store(1)
	h.mu.Lock()
	h.signalLocked(&h.dataCh)
	h.mu.Unlock()
	for c.inNext.Load() != 0 {
		runtime.Gosched()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if c.h != h {
		panic(chanHubStateInvariantViolation)
	}
	switch {
	case c.prev != nil:
		c.prev.next = c.next
	case h.subHead == c:
		h.subHead = c.next
	default:
		panic(chanHubStateInvariantViolation)
	}
	if c.next != nil {
		c.next.prev = c.prev
	}
	c.prev = nil
	c.next = nil
	c.h = nil
	if h.nsub <= 0 {
		panic(chanHubStateInvariantViolation)
	}
	h.nsub--
	h.min = 0
	h.signalLocked(&h.roomCh)
}

func (h *ChanHub[V]) Cursor() *ChanHubCursor[V] {
	c := new(ChanHubCursor[V])
	h.Attach(c)
	return c
}

func (h *ChanHub[V]) Subscribe() iter.Seq[V] {
	return h.SubscribeContext(context.Background())
}

func (h *ChanHub[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	c := h.Cursor()
	if ctx == nil {
		ctx = context.Background()
	}
	stop := context.AfterFunc(ctx, func() { c.safeDetach() })
	return func(yield func(V) bool) {
		if yield == nil {
			if stop() {
				c.safeDetach()
			}
			panic(`bigbuff: chanhub: iterator yield func is nil`)
		}
		if !stop() {
			return
		}
		defer c.safeDetach()
		c.values(ctx, yield)
	}
}

func (h *ChanHub[V]) checkFactory() {
	if h == nil || h.slots == nil {
		panic(`bigbuff: chanhub: must use factory function`)
	}
}

func (h *ChanHub[V]) minLocked() uint64 {
	min := h.head.Load()
	n := int32(0)
	for c := h.subHead; c != nil; c = c.next {
		n++
		if c.dead.Load() != 0 {
			continue
		}
		if seq := c.seq.Load(); seq < min {
			min = seq
		}
	}
	if n != h.nsub {
		panic(chanHubStateInvariantViolation)
	}
	return min
}

func (h *ChanHub[V]) signalLocked(ch *chan struct{}) {
	if *ch != nil {
		close(*ch)
		*ch = nil
	}
}

func (c *ChanHubCursor[V]) Next() (v V, ok bool) {
	c.inNext.Add(1)
	defer c.inNext.Add(-1)
	if c.dead.Load() != 0 {
		return
	}
	h := c.h
	if h == nil {
		panic(`bigbuff: chanhub: cursor not attached`)
	}
	seq := c.seq.Load()
	head := h.head.Load()
	if seq >= head {
		return
	}
	if head-seq > h.mask+1 {
		panic(chanHubStateInvariantViolation)
	}
	slot := &h.slots[seq&h.mask]
	if slot.seq.Load() != seq+1 {
		panic(chanHubStateInvariantViolation)
	}
	v = slot.val
	if slot.seq.Load() != seq+1 || c.dead.Load() != 0 {
		return *new(V), false
	}
	c.seq.Store(seq + 1)
	h.maybeRoom()
	return v, true
}

func (c *ChanHubCursor[V]) Peek() (v V, ok bool) {
	c.inNext.Add(1)
	defer c.inNext.Add(-1)
	if c.dead.Load() != 0 {
		return
	}
	h := c.h
	if h == nil {
		panic(`bigbuff: chanhub: cursor not attached`)
	}
	seq := c.seq.Load()
	if seq >= h.head.Load() {
		return
	}
	slot := &h.slots[seq&h.mask]
	if slot.seq.Load() != seq+1 {
		panic(chanHubStateInvariantViolation)
	}
	v = slot.val
	if slot.seq.Load() != seq+1 || c.dead.Load() != 0 {
		return *new(V), false
	}
	return v, true
}

func (c *ChanHubCursor[V]) Recv() (v V, ok bool) {
	for {
		if v, ok = c.Next(); ok {
			return v, true
		}
		h := c.hub()
		if c.dead.Load() != 0 || (h.closed && c.seq.Load() >= h.head.Load()) {
			return *new(V), false
		}
		select {
		case <-c.Wait():
		}
	}
}

func (c *ChanHubCursor[V]) Wait() <-chan struct{} {
	if c.dead.Load() != 0 {
		return chanHubReady
	}
	h := c.hub()
	return h.Wait(c.seq.Load())
}

func (c *ChanHubCursor[V]) Seq() uint64 {
	return c.seq.Load()
}

func (c *ChanHubCursor[V]) Detach() {
	h := c.hub()
	h.Detach(c)
}

func (c *ChanHubCursor[V]) Values() iter.Seq[V] {
	return c.ValuesContext(context.Background())
}

func (c *ChanHubCursor[V]) ValuesContext(ctx context.Context) iter.Seq[V] {
	c.hub()
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(V) bool) {
		if yield == nil {
			panic(`bigbuff: chanhub: iterator yield func is nil`)
		}
		c.values(ctx, yield)
	}
}

func (c *ChanHubCursor[V]) values(ctx context.Context, yield func(V) bool) {
	for {
		for {
			v, ok := c.Next()
			if !ok {
				break
			}
			if !yield(v) {
				return
			}
		}
		h := c.hub()
		if c.dead.Load() != 0 || (h.closed && c.seq.Load() >= h.head.Load()) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-c.Wait():
		}
	}
}

func (c *ChanHubCursor[V]) hub() *ChanHub[V] {
	if c == nil {
		panic(`bigbuff: chanhub: nil cursor`)
	}
	h := c.h
	if h == nil || h.slots == nil {
		panic(`bigbuff: chanhub: cursor not attached`)
	}
	return h
}

func (c *ChanHubCursor[V]) safeDetach() {
	defer func() { recover() }()
	if c.h != nil {
		c.h.Detach(c)
	}
}

func (h *ChanHub[V]) maybeRoom() {
	h.mu.Lock()
	if h.roomCh != nil {
		h.min = 0
		h.signalLocked(&h.roomCh)
	}
	h.mu.Unlock()
}
```
