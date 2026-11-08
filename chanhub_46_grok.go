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

// Provenance: 46-arena-grok-4.6-medium-prompt-mini.md (Grok, response #46).
// All candidate API symbols are response-scoped to coexist with the earlier ChanHub.

import (
	"context"
	"iter"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

const chanHub46StateInvariantViolation = `bigbuff: chanhub: state invariant violation`

var chanHub46Ready = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// Values are immutable after publication. An atomic pointer keeps an old
// cursor's read safe while the bounded ring slot is later reused; seq alone
// cannot protect a plain generic value from concurrent overwrite.
type chanHubSlot46Grok[V any] struct {
	seq atomic.Uint64
	val atomic.Pointer[V]
}

type ChanHub46Grok[V any] struct {
	slots []chanHubSlot46Grok[V]
	mask  uint64

	mu     sync.Mutex
	closed atomic.Bool
	nsub   int32
	head   atomic.Uint64
	min    uint64
	dataCh chan struct{}
	roomCh chan struct{}

	subHead *ChanHubCursor46Grok[V]
}

type ChanHubCursor46Grok[V any] struct {
	h      atomic.Pointer[ChanHub46Grok[V]]
	seq    atomic.Uint64
	dead   atomic.Uint32
	inNext atomic.Int32
	prev   *ChanHubCursor46Grok[V]
	next   *ChanHubCursor46Grok[V]
}

func NewChanHub46Grok[V any](size int) *ChanHub46Grok[V] {
	if size <= 0 || size > math.MaxInt32 || size&(size-1) != 0 {
		panic(`bigbuff: chanhub: size must be a positive power of two`)
	}
	return &ChanHub46Grok[V]{
		slots: make([]chanHubSlot46Grok[V], size),
		mask:  uint64(size - 1),
	}
}

func (h *ChanHub46Grok[V]) Close() {
	h.checkFactory()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed.Load() {
		panic(`bigbuff: chanhub: already closed`)
	}
	h.closed.Store(true)
	h.signalLocked(&h.dataCh)
	h.signalLocked(&h.roomCh)
}

func (h *ChanHub46Grok[V]) Publish(v V) uint64 {
	for {
		seq, ok := h.TryPublish(v)
		if ok {
			return seq
		}
		<-h.Avail()
	}
}

func (h *ChanHub46Grok[V]) TryPublish(v V) (seq uint64, ok bool) {
	h.checkFactory()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed.Load() {
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
	value := new(V)
	*value = v
	slot.val.Store(value)
	slot.seq.Store(seq + 1)
	h.head.Store(seq + 1)
	h.signalLocked(&h.dataCh)
	return seq, true
}

func (h *ChanHub46Grok[V]) Load() (v V, seq uint64, ok bool) {
	h.checkFactory()
	h.mu.Lock()
	defer h.mu.Unlock()
	head := h.head.Load()
	if head == 0 {
		return
	}
	seq = head - 1
	v = *h.slots[seq&h.mask].val.Load()
	ok = true
	return
}

func (h *ChanHub46Grok[V]) Seq() uint64 {
	h.checkFactory()
	return h.head.Load()
}

func (h *ChanHub46Grok[V]) N() int {
	h.checkFactory()
	h.mu.Lock()
	defer h.mu.Unlock()
	return int(h.nsub)
}

func (h *ChanHub46Grok[V]) Avail() <-chan struct{} {
	h.checkFactory()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed.Load() {
		return chanHub46Ready
	}
	if h.nsub == 0 {
		return chanHub46Ready
	}
	head := h.head.Load()
	capn := uint64(len(h.slots))
	if head-h.min >= capn {
		h.min = h.minLocked()
	}
	if head-h.min < capn {
		return chanHub46Ready
	}
	if h.roomCh == nil {
		h.roomCh = make(chan struct{})
	}
	return h.roomCh
}

func (h *ChanHub46Grok[V]) Wait(after uint64) <-chan struct{} {
	h.checkFactory()
	if h.head.Load() > after {
		return chanHub46Ready
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed.Load() || h.head.Load() > after {
		return chanHub46Ready
	}
	if h.dataCh == nil {
		h.dataCh = make(chan struct{})
	}
	return h.dataCh
}

func (h *ChanHub46Grok[V]) Attach(c *ChanHubCursor46Grok[V]) {
	h.checkFactory()
	if c == nil {
		panic(`bigbuff: chanhub: nil cursor`)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed.Load() {
		panic(`bigbuff: chanhub: attach on closed hub`)
	}
	if c.h.Load() != nil || c.prev != nil || c.next != nil || c.inNext.Load() != 0 {
		panic(`bigbuff: chanhub: cursor already attached`)
	}
	if h.nsub == math.MaxInt32 {
		panic(`bigbuff: chanhub: subscribers overflow`)
	}
	c.dead.Store(0)
	c.h.Store(h)
	c.seq.Store(h.head.Load())
	c.next = h.subHead
	if h.subHead != nil {
		h.subHead.prev = c
	}
	h.subHead = c
	h.nsub++
}

func (h *ChanHub46Grok[V]) Detach(c *ChanHubCursor46Grok[V]) {
	h.checkFactory()
	if c == nil || c.h.Load() != h {
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
	if c.h.Load() != h {
		panic(chanHub46StateInvariantViolation)
	}
	switch {
	case c.prev != nil:
		c.prev.next = c.next
	case h.subHead == c:
		h.subHead = c.next
	default:
		panic(chanHub46StateInvariantViolation)
	}
	if c.next != nil {
		c.next.prev = c.prev
	}
	c.prev = nil
	c.next = nil
	c.h.Store(nil)
	if h.nsub <= 0 {
		panic(chanHub46StateInvariantViolation)
	}
	h.nsub--
	h.min = 0
	h.signalLocked(&h.roomCh)
}

func (h *ChanHub46Grok[V]) Cursor() *ChanHubCursor46Grok[V] {
	c := new(ChanHubCursor46Grok[V])
	h.Attach(c)
	return c
}

func (h *ChanHub46Grok[V]) Subscribe() iter.Seq[V] {
	return h.SubscribeContext(context.Background())
}

func (h *ChanHub46Grok[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
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
			// The cancellation callback may be detaching concurrently. Joining
			// its cursor cleanup here makes iterator return a synchronization
			// point for callers without leaking the cursor on early cancellation.
			c.safeDetach()
			return
		}
		defer c.safeDetach()
		c.values(ctx, yield)
	}
}

func (h *ChanHub46Grok[V]) checkFactory() {
	if h == nil || h.slots == nil {
		panic(`bigbuff: chanhub: must use factory function`)
	}
}

func (h *ChanHub46Grok[V]) minLocked() uint64 {
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
		panic(chanHub46StateInvariantViolation)
	}
	return min
}

func (h *ChanHub46Grok[V]) signalLocked(ch *chan struct{}) {
	if *ch != nil {
		close(*ch)
		*ch = nil
	}
}

func (c *ChanHubCursor46Grok[V]) Next() (v V, ok bool) {
	c.inNext.Add(1)
	defer c.inNext.Add(-1)
	if c.dead.Load() != 0 {
		return
	}
	h := c.h.Load()
	if h == nil {
		panic(`bigbuff: chanhub: cursor not attached`)
	}
	seq := c.seq.Load()
	head := h.head.Load()
	if seq >= head {
		return
	}
	if head-seq > h.mask+1 {
		panic(chanHub46StateInvariantViolation)
	}
	slot := &h.slots[seq&h.mask]
	if slot.seq.Load() != seq+1 {
		panic(chanHub46StateInvariantViolation)
	}
	value := slot.val.Load()
	if value == nil {
		panic(chanHub46StateInvariantViolation)
	}
	v = *value
	if slot.seq.Load() != seq+1 || c.dead.Load() != 0 {
		return *new(V), false
	}
	c.seq.Store(seq + 1)
	h.maybeRoom()
	return v, true
}

func (c *ChanHubCursor46Grok[V]) Peek() (v V, ok bool) {
	c.inNext.Add(1)
	defer c.inNext.Add(-1)
	if c.dead.Load() != 0 {
		return
	}
	h := c.h.Load()
	if h == nil {
		panic(`bigbuff: chanhub: cursor not attached`)
	}
	seq := c.seq.Load()
	if seq >= h.head.Load() {
		return
	}
	slot := &h.slots[seq&h.mask]
	if slot.seq.Load() != seq+1 {
		panic(chanHub46StateInvariantViolation)
	}
	value := slot.val.Load()
	if value == nil {
		panic(chanHub46StateInvariantViolation)
	}
	v = *value
	if slot.seq.Load() != seq+1 || c.dead.Load() != 0 {
		return *new(V), false
	}
	return v, true
}

func (c *ChanHubCursor46Grok[V]) Recv() (v V, ok bool) {
	for {
		if v, ok = c.Next(); ok {
			return v, true
		}
		if c.dead.Load() != 0 {
			return *new(V), false
		}
		h := c.h.Load()
		if h == nil || c.dead.Load() != 0 || (h.closed.Load() && c.seq.Load() >= h.head.Load()) {
			return *new(V), false
		}
		select {
		case <-c.Wait():
		}
	}
}

func (c *ChanHubCursor46Grok[V]) Wait() <-chan struct{} {
	if c.dead.Load() != 0 {
		return chanHub46Ready
	}
	h := c.h.Load()
	if h == nil || c.dead.Load() != 0 {
		return chanHub46Ready
	}
	return h.Wait(c.seq.Load())
}

func (c *ChanHubCursor46Grok[V]) Seq() uint64 {
	return c.seq.Load()
}

func (c *ChanHubCursor46Grok[V]) Detach() {
	h := c.hub()
	h.Detach(c)
}

func (c *ChanHubCursor46Grok[V]) Values() iter.Seq[V] {
	return c.ValuesContext(context.Background())
}

func (c *ChanHubCursor46Grok[V]) ValuesContext(ctx context.Context) iter.Seq[V] {
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

func (c *ChanHubCursor46Grok[V]) values(ctx context.Context, yield func(V) bool) {
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
		if c.dead.Load() != 0 {
			return
		}
		h := c.h.Load()
		if h == nil || c.dead.Load() != 0 || (h.closed.Load() && c.seq.Load() >= h.head.Load()) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-c.Wait():
		}
	}
}

func (c *ChanHubCursor46Grok[V]) hub() *ChanHub46Grok[V] {
	if c == nil {
		panic(`bigbuff: chanhub: nil cursor`)
	}
	h := c.h.Load()
	if h == nil || h.slots == nil {
		panic(`bigbuff: chanhub: cursor not attached`)
	}
	return h
}

func (c *ChanHubCursor46Grok[V]) safeDetach() {
	defer func() { recover() }()
	if h := c.h.Load(); h != nil {
		h.Detach(c)
	}
}

func (h *ChanHub46Grok[V]) maybeRoom() {
	h.mu.Lock()
	if h.roomCh != nil {
		h.min = 0
		h.signalLocked(&h.roomCh)
	}
	h.mu.Unlock()
}
