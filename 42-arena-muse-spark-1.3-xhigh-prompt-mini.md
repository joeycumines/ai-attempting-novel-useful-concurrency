Broadcast with stateless subscribers, `close`-fanout, and
immutable publication. `O(1)` publish, `0`-alloc receive,
selectable, churn-free.

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
	"errors"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

// DefaultBroadcastCap is used when NewBroadcast is given size <= 0.
const DefaultBroadcastCap = 64

// ErrBroadcastClosed is returned by Receiver.RecvContext when the
// broadcast is closed and drained.
var ErrBroadcastClosed = errors.New(`bigbuff: broadcast: closed`)

// broadcastNode is immutable after publication.
//
// seq is the 1-based sequence, val is the payload, wait is the
// current-generation notify channel. wait is closed exactly once,
// by the next Send/SendBatch/Close. Because seq+wait live in the
// same immutable node, a single atomic.Pointer load gives a
// consistent (seq,wait) snapshot - no lost-wakeup ordering between
// separate seq and notify atomics.
//
// Nodes are never mutated nor reused, only replaced in the ring.
// Hence readers are lock-free with no ABA/hazard: a stale pointer
// is still valid memory kept alive by GC.
type broadcastNode[V any] struct {
	seq  uint64
	val  V
	wait chan struct{}
}

// Broadcast is a selectable, ring-buffered, multi-producer-safe
// (serialized), multi-consumer broadcast.
//
// Design:
//
//   - ring of atomic.Pointer[node], power-of-two, mask for & not %.
//   - latest atomic.Pointer[node] bundles seq+wait+val-head.
//     One atomic load = consistent snapshot.
//   - initWait/ring/mask immutable after construction.
//   - mu serializes Send/SendBatch/Close only. Readers never touch mu.
//   - closed atomic.Bool, write-once under mu.
//   - Send: 1 node alloc + 1 chan alloc = 2 allocs, 2 atomic stores,
//     1 close(oldWait) which wakes ALL selectors on oldWait.
//     No per-subscriber work. O(1) w.r.t N.
//   - SendBatch: N nodes + 1 chan = N+1 allocs, 1 close for N msgs.
//   - Recv: atomic loads + CAS claim + copy, 0 allocs.
//   - Receiver is just (b,next,missed). NewReceiver = 1 alloc +
//     1 atomic load, no global mutation. Discard = GC only.
//     Zero coordination => churn-free, N-scalable.
//
// Allocation (stable, predictable, N-independent):
//
//	construction: 3 (struct + initWait hchan + ring array)
//	Send:         2 (node + hchan)
//	SendBatch:    N+1
//	Recv/TryRecv/Await/WaitCh: 0
//	NewReceiver:  1
//
// Performance: sender never iterates subscribers, never copies
// values N times. Values copied in parallel by receivers.
// close() broadcast is done in runtime, batched. Comparable to
// linear callback-chan fanout for tiny N, superior for large N.
//
// Select: WaitCh returns nil if data ready (no wait), else the
// current wait chan. That chan is closed on next publish/close,
// waking ALL waiters (unlike chan send which wakes one). Pattern:
//
//	for {
//	    if ch := r.WaitCh(); ch != nil {
//	        select {
//	        case <-ch:
//	        case <-ctx.Done():
//	            return
//	        }
//	        continue
//	    }
//	    v, seq, ok := r.TryRecv()
//	    if !ok {
//	        if b.Closed() { return }
//	        continue // competing-consumer race
//	    }
//	    _ = v; _ = seq
//	}
//
// Or just Recv/RecvContext/All/Subscribe which hide it.
//
// Semantics: bounded history (cap). Slow receivers overrun:
// TryRecv auto-jumps to oldest retained (min loss), counts Missed.
// Overrun-during-read (wrap between claim and load) counts 1 and
// retries; second-level overrun jumps again, eventually to latest,
// guaranteeing lock-free progress. Separate Receivers = broadcast
// (each gets all). Shared Receiver = queue (each msg to one claimer
// via CAS on next, wait-free for private, lock-free for shared).
// Close drains: buffered msgs still readable, then Recv=false /
// RecvContext=ErrClosed. Send after Close panics like chan.
type Broadcast[V any] struct {
	latest atomic.Pointer[broadcastNode[V]]
	closed atomic.Bool
	_      [64]byte // isolate hot read-mostly line from writer lock

	mu sync.Mutex
	_  [64]byte // isolate writer lock from immutable

	initWait chan struct{}
	ring     []atomic.Pointer[broadcastNode[V]]
	mask     uint64
}

// Receiver is a cursor over a Broadcast.
//
// Not bound to Broadcast on creation beyond back-pointer. No
// Unsubscribe needed. Use private Receiver per goroutine for
// broadcast (all get all), share one Receiver across goroutines
// for competing (each msg to one).
//
// next/missed are atomic to allow shared competing use.
// Padded to 64B to avoid false-sharing between distinct Receivers.
type Receiver[V any] struct {
	b      *Broadcast[V]
	next   atomic.Uint64
	missed atomic.Uint64
	_      [40]byte
}

func roundUpPow2(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p *= 2
		if p <= 0 {
			panic(`bigbuff: broadcast: size too large`)
		}
	}
	return p
}

func (b *Broadcast[V]) checkInit() {
	if b == nil || b.ring == nil || b.initWait == nil {
		panic(`bigbuff: broadcast: must use NewBroadcast`)
	}
}

func (r *Receiver[V]) checkInit() {
	if r == nil || r.b == nil || r.b.ring == nil {
		panic(`bigbuff: broadcast: receiver must come from NewReceiver`)
	}
}

// NewBroadcast creates a broadcast holding up to size msgs.
// size<=0 => DefaultBroadcastCap. Rounded up to power-of-two.
func NewBroadcast[V any](size int) *Broadcast[V] {
	if size < 0 {
		panic(`bigbuff: broadcast: negative size`)
	}
	if size == 0 {
		size = DefaultBroadcastCap
	}
	c := roundUpPow2(size)
	b := &Broadcast[V]{
		initWait: make(chan struct{}),
		ring:     make([]atomic.Pointer[broadcastNode[V]], c),
		mask:     uint64(c - 1),
	}
	return b
}

// Send publishes one value, wakes all waiters. Never blocks.
// Returns seq. Panics if closed. 2 allocs.
func (b *Broadcast[V]) Send(value V) uint64 {
	b.checkInit()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		panic(`bigbuff: broadcast: send on closed broadcast`)
	}
	cur := b.latest.Load()
	var newSeq uint64 = 1
	oldWait := b.initWait
	if cur != nil {
		if cur.seq == ^uint64(0) {
			panic(`bigbuff: broadcast: sequence overflow`)
		}
		newSeq = cur.seq + 1
		oldWait = cur.wait
	}
	newWait := make(chan struct{})
	newNode := &broadcastNode[V]{seq: newSeq, val: value, wait: newWait}
	b.ring[newSeq&b.mask].Store(newNode)
	b.latest.Store(newNode)
	close(oldWait)
	return newSeq
}

// SendBatch publishes len(values) with one notify rotation.
// 1 chan alloc + N node allocs, 1 wakeup for N msgs.
// Empty => no-op, returns Seq(). Panics if closed.
func (b *Broadcast[V]) SendBatch(values []V) uint64 {
	b.checkInit()
	if len(values) == 0 {
		return b.Seq()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		panic(`bigbuff: broadcast: send on closed broadcast`)
	}
	cur := b.latest.Load()
	var base uint64
	oldWait := b.initWait
	if cur != nil {
		base = cur.seq
		oldWait = cur.wait
	}
	if uint64(len(values)) > ^uint64(0)-base {
		panic(`bigbuff: broadcast: sequence overflow`)
	}
	newWait := make(chan struct{})
	var last *broadcastNode[V]
	for i, v := range values {
		seq := base + uint64(i) + 1
		node := &broadcastNode[V]{seq: seq, val: v, wait: newWait}
		b.ring[seq&b.mask].Store(node)
		last = node
	}
	b.latest.Store(last)
	close(oldWait)
	return base + uint64(len(values))
}

// Close marks closed and wakes all. Drains remain readable.
// Panics on double-close like chan.
func (b *Broadcast[V]) Close() {
	b.checkInit()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		panic(`bigbuff: broadcast: close of closed broadcast`)
	}
	b.closed.Store(true)
	cur := b.latest.Load()
	if cur == nil {
		close(b.initWait)
	} else {
		close(cur.wait)
	}
}

// Closed reports close.
func (b *Broadcast[V]) Closed() bool {
	b.checkInit()
	return b.closed.Load()
}

// Seq returns last published seq, 0 if empty.
func (b *Broadcast[V]) Seq() uint64 {
	b.checkInit()
	cur := b.latest.Load()
	if cur == nil {
		return 0
	}
	return cur.seq
}

// Cap returns ring capacity (power-of-two).
func (b *Broadcast[V]) Cap() int {
	b.checkInit()
	return len(b.ring)
}

// Len returns buffered count: min(Seq,Cap).
func (b *Broadcast[V]) Len() int {
	b.checkInit()
	s := b.Seq()
	c := uint64(len(b.ring))
	if s > c {
		return int(c)
	}
	return int(s)
}

// NewReceiver starts live at Seq()+1 (future only).
func (b *Broadcast[V]) NewReceiver() *Receiver[V] {
	b.checkInit()
	r := &Receiver[V]{b: b}
	s := b.Seq()
	if s == ^uint64(0) {
		r.next.Store(s) // overflow edge, unreachable
	} else {
		r.next.Store(s + 1)
	}
	return r
}

// NewReceiverFrom starts at seq (replay within ring).
// 0 => 1. May overrun-resync on first TryRecv.
func (b *Broadcast[V]) NewReceiverFrom(seq uint64) *Receiver[V] {
	b.checkInit()
	if seq == 0 {
		seq = 1
	}
	r := &Receiver[V]{b: b}
	r.next.Store(seq)
	return r
}

// NewReceiverFromOldest starts at oldest retained.
func (b *Broadcast[V]) NewReceiverFromOldest() *Receiver[V] {
	b.checkInit()
	s := b.Seq()
	c := uint64(len(b.ring))
	var start uint64 = 1
	if s == 0 {
		start = 1
	} else if s >= c {
		start = s - c + 1
	}
	r := &Receiver[V]{b: b}
	r.next.Store(start)
	return r
}

// Subscribe live values until Close. Like ChanPubSub.Subscribe.
func (b *Broadcast[V]) Subscribe() iter.Seq[V] {
	return b.NewReceiver().All()
}

// SubscribeContext live values until ctx or Close.
func (b *Broadcast[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
	return b.NewReceiver().AllContext(ctx)
}

// Broadcast returns parent.
func (r *Receiver[V]) Broadcast() *Broadcast[V] {
	r.checkInit()
	return r.b
}

// NextSeq returns next seq to read.
func (r *Receiver[V]) NextSeq() uint64 {
	r.checkInit()
	return r.next.Load()
}

// Missed returns total skipped via overrun/skip.
func (r *Receiver[V]) Missed() uint64 {
	r.checkInit()
	return r.missed.Load()
}

// Pending returns approx buffered for this cursor, capped by Cap.
func (r *Receiver[V]) Pending() int {
	r.checkInit()
	curSeq := r.b.Seq()
	n := r.next.Load()
	if n == 0 {
		n = 1
	}
	if n > curSeq {
		return 0
	}
	diff := curSeq - n + 1
	c := uint64(len(r.b.ring))
	if diff > c {
		return int(c)
	}
	return int(diff)
}

// WaitCh returns chan to select on for next msg, or nil if ready.
// Nil means TryRecv now (no wait). Non-nil will be closed on next
// publish/close, waking ALL. Never returns closed-ready global to
// avoid thundering-herd lock on one hchan; nil is lock-free.
func (r *Receiver[V]) WaitCh() <-chan struct{} {
	r.checkInit()
	cur := r.b.latest.Load()
	var curSeq uint64
	var curWait chan struct{}
	if cur == nil {
		curSeq = 0
		curWait = r.b.initWait
	} else {
		curSeq = cur.seq
		curWait = cur.wait
	}
	if r.next.Load() <= curSeq {
		return nil
	}
	return curWait
}

// TryRecv non-blocking. ok=true => (v,seq). ok=false => empty.
// Overrun auto-jumps to oldest retained, counts Missed.
// Shared Receiver safe via CAS claim. 0 allocs.
func (r *Receiver[V]) TryRecv() (V, uint64, bool) {
	r.checkInit()
	var zero V
	b := r.b
	capU := uint64(len(b.ring))
	mask := b.mask
	for {
		cur := b.latest.Load()
		var curSeq uint64
		if cur != nil {
			curSeq = cur.seq
		}
		n := r.next.Load()
		if n == 0 {
			if r.next.CompareAndSwap(0, 1) {
				n = 1
			} else {
				runtime.Gosched()
				continue
			}
		}
		if n > curSeq {
			return zero, 0, false
		}
		if curSeq-n >= capU {
			newN := curSeq - capU + 1
			for {
				curN := r.next.Load()
				if newN <= curN {
					break
				}
				if r.next.CompareAndSwap(curN, newN) {
					r.missed.Add(newN - curN)
					break
				}
				runtime.Gosched()
				break
			}
			continue
		}
		if !r.next.CompareAndSwap(n, n+1) {
			runtime.Gosched()
			continue
		}
		ptr := b.ring[n&mask].Load()
		if ptr == nil || ptr.seq != n {
			// wrapped between claim and load
			r.missed.Add(1)
			continue
		}
		return ptr.val, n, true
	}
}

// Recv blocks until value or closed+drained.
// ok=false => closed+empty like chan.
func (r *Receiver[V]) Recv() (V, uint64, bool) {
	r.checkInit()
	var zero V
	for {
		if v, seq, ok := r.TryRecv(); ok {
			return v, seq, true
		}
		if r.b.Closed() {
			if v, seq, ok := r.TryRecv(); ok {
				return v, seq, true
			}
			return zero, 0, false
		}
		ch := r.WaitCh()
		if ch == nil {
			continue
		}
		<-ch
	}
}

// RecvContext blocks until value, ctx, or closed+drained.
func (r *Receiver[V]) RecvContext(ctx context.Context) (V, uint64, error) {
	r.checkInit()
	var zero V
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		select {
		case <-ctx.Done():
			return zero, 0, ctx.Err()
		default:
		}
		if v, seq, ok := r.TryRecv(); ok {
			return v, seq, nil
		}
		if r.b.Closed() {
			if v, seq, ok := r.TryRecv(); ok {
				return v, seq, nil
			}
			return zero, 0, ErrBroadcastClosed
		}
		ch := r.WaitCh()
		if ch == nil {
			continue
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return zero, 0, ctx.Err()
		}
	}
}

// SkipToLatest drops all buffered, returns skipped count.
func (r *Receiver[V]) SkipToLatest() uint64 {
	r.checkInit()
	for {
		curSeq := r.b.Seq()
		n := r.next.Load()
		if n == 0 {
			if r.next.CompareAndSwap(0, 1) {
				n = 1
			} else {
				runtime.Gosched()
				continue
			}
		}
		if n > curSeq {
			return 0
		}
		newN := curSeq + 1 // may wrap to 0, repaired later
		skipped := curSeq - n + 1
		if r.next.CompareAndSwap(n, newN) {
			if skipped != 0 {
				r.missed.Add(skipped)
			}
			return skipped
		}
		runtime.Gosched()
	}
}

// All iterates until Close+drained.
func (r *Receiver[V]) All() iter.Seq[V] {
	r.checkInit()
	return func(yield func(V) bool) {
		if yield == nil {
			panic(`bigbuff: broadcast: nil yield`)
		}
		for {
			v, _, ok := r.Recv()
			if !ok {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}

// AllWithSeq iterates (v,seq) until Close+drained.
func (r *Receiver[V]) AllWithSeq() iter.Seq2[V, uint64] {
	r.checkInit()
	return func(yield func(V, uint64) bool) {
		if yield == nil {
			panic(`bigbuff: broadcast: nil yield`)
		}
		for {
			v, seq, ok := r.Recv()
			if !ok {
				return
			}
			if !yield(v, seq) {
				return
			}
		}
	}
}

// AllContext iterates until ctx or Close+drained.
func (r *Receiver[V]) AllContext(ctx context.Context) iter.Seq[V] {
	r.checkInit()
	return func(yield func(V) bool) {
		if yield == nil {
			panic(`bigbuff: broadcast: nil yield`)
		}
		for {
			v, _, err := r.RecvContext(ctx)
			if err != nil {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}

// AllWithSeqContext iterates (v,seq) until ctx or Close+drained.
func (r *Receiver[V]) AllWithSeqContext(ctx context.Context) iter.Seq2[V, uint64] {
	r.checkInit()
	return func(yield func(V, uint64) bool) {
		if yield == nil {
			panic(`bigbuff: broadcast: nil yield`)
		}
		for {
			v, seq, err := r.RecvContext(ctx)
			if err != nil {
				return
			}
			if !yield(v, seq) {
				return
			}
		}
	}
}
```
