package bigbuff

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
)

// EpochPubSub - O(1) broadcast, select-compatible, zero per-receiver alloc
type EpochPubSub[V any] struct {
    subs    atomic.Int32
    seq     atomic.Uint64
    cur     atomic.Pointer[epochMeta[V]]
    sendMu  sync.Mutex
    pongMu  sync.Mutex
    pongCond *sync.Cond
    pongN   int32
    broken  chan struct{}
}

type epochMeta[V any] struct {
    seq    uint64
    val    V
    notify chan struct{} // closed on supersede
    // reused via sync.Pool for the struct, channel is fresh
}

func NewEpochPubSub[V any]() *EpochPubSub[V] {
    e := &epochMeta[V]{notify: make(chan struct{})}
    x := &EpochPubSub[V]{broken: make(chan struct{})}
    x.cur.Store(e)
    x.pongCond = sync.NewCond(&x.pongMu)
    return x
}

// Notify returns a channel that closes on next Send.
// Use directly in select - no Add needed for wakeup.
func (x *EpochPubSub[V]) Notify() <-chan struct{} {
    return x.cur.Load().notify
}

func (x *EpochPubSub[V]) Current() (V, uint64, bool) {
    e := x.cur.Load()
    return e.val, e.seq, e.seq != 0
}

func (x *EpochPubSub[V]) Send(v V) int {
    n := int(x.subs.Load())
    if n == 0 { return 0 }
    x.sendMu.Lock()
    defer x.sendMu.Unlock()

    newEp := &epochMeta[V]{ // pool this struct in prod
        seq:    x.seq.Add(1),
        val:    v,
        notify: make(chan struct{}),
    }
    old := x.cur.Swap(newEp)
    close(old.notify) // O(1) wake-all - runtime's broadcast

    if n > 0 {
        x.pongMu.Lock()
        x.pongN = int32(n)
        x.pongMu.Unlock()
        // wait for Ack - same semantics as ChanPubSub.Wait
        x.pongMu.Lock()
        for x.pongN != 0 { x.pongCond.Wait() }
        x.pongMu.Unlock()
    }
    return n
}

func (x *EpochPubSub[V]) Ack() {
    if x.pongMu.Lock(); x.pongN > 0 {
        x.pongN--
        if x.pongN == 0 { x.pongCond.Broadcast() }
    }
    x.pongMu.Unlock()
}

func (x *EpochPubSub[V]) Add(d int) int { return int(x.subs.Add(int32(d))) }
func (x *EpochPubSub[V]) Subscribe() { x.Add(1) }
func (x *EpochPubSub[V]) Unsubscribe() { 
    // if waiting on notify, closing will wake us, Ack still needed
    x.Add(-1); x.Ack() 
}

func (x *EpochPubSub[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
    x.Subscribe()
    stop := context.AfterFunc(ctx, func(){ x.Unsubscribe() })
    return func(yield func(V) bool) {
        if !stop() { return }
        defer x.Unsubscribe()
        last := uint64(0)
        for {
            e := x.cur.Load()
            if e.seq != last && e.seq != 0 {
                last = e.seq
                if !yield(e.val) { return }
                x.Ack()
            }
            select {
            case <-ctx.Done(): return
            case <-e.notify: // spin to new epoch
            }
        }
    }
}
