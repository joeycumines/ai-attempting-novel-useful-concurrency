package bigbuff

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

// EpochPubSub - O(1) broadcast, select-compatible, zero per-receiver alloc
type EpochPubSub[V any] struct {
    subs    atomic.Int32
    seq     atomic.Uint64
    cur     atomic.Pointer[epochMeta[V]]
    sendMu  sync.RWMutex
    pongMu  sync.Mutex
    pongCond *sync.Cond
    pongN   atomic.Int32
    armedSeq atomic.Uint64 // seq of the round pongN was armed for
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
    x.sendMu.Lock()
    defer x.sendMu.Unlock()

    newEp := &epochMeta[V]{ // pool this struct in prod
        seq:    x.seq.Add(1),
        val:    v,
        notify: make(chan struct{}),
    }

    // Snapshot the subscriber count and arm the pong barrier under pongMu,
    // so a concurrent Unsubscribe either lands before the snapshot (excluded
    // from n) or after (compensates pongN) - never lost in between. Arming
    // before publishing also means a fast Ack can never be lost to pongN == 0.
    x.pongMu.Lock()
    n := int(x.subs.Load())
    if n == 0 {
        x.pongMu.Unlock()
        return 0
    }
    x.pongN.Store(int32(n))
    x.armedSeq.Store(newEp.seq)
    x.pongCond.Broadcast()
    x.pongMu.Unlock()

    old := x.cur.Swap(newEp)
    close(old.notify) // O(1) wake-all - runtime's broadcast

    // wait for Ack - same semantics as ChanPubSub.Wait
    x.pongMu.Lock()
    for x.pongN.Load() > 0 { x.pongCond.Wait() }
    x.pongMu.Unlock()
    return n
}

// Ack acknowledges the round identified by seq. Acks for any other round are
// ignored, so a late or spurious ack can never corrupt a different round's
// barrier.
func (x *EpochPubSub[V]) Ack(seq uint64) {
    x.ackTracked(seq, nil)
}

// ackTracked is Ack plus optional tracking of the last acked round. Both the
// decrement and the tracking happen under pongMu, so the unsubscribe
// compensation guard observes a consistent value.
func (x *EpochPubSub[V]) ackTracked(seq uint64, lastAcked *atomic.Uint64) {
    x.pongMu.Lock()
    if x.pongN.Load() > 0 && seq == x.armedSeq.Load() {
        if x.pongN.Add(-1) == 0 { x.pongCond.Broadcast() }
    }
    if lastAcked != nil {
        lastAcked.Store(seq)
    }
    x.pongMu.Unlock()
}


func (x *EpochPubSub[V]) Add(d int) int { return int(x.subs.Add(int32(d))) }
func (x *EpochPubSub[V]) Subscribe() {
    // Register between rounds: while a Send is in flight its subscriber
    // snapshot is already fixed, and taking the read lock here guarantees we
    // are counted in the next round's pong barrier (and will ack it).
    x.sendMu.RLock()
    x.subs.Add(1)
    x.sendMu.RUnlock()
}

// unsubscribe withdraws a subscription. If a send is in flight and the
// subscriber has not already acked the armed round (ackedSeq), it was counted
// in that round's pong barrier and will never ack - so the barrier is
// compensated atomically with the decrement.
func (x *EpochPubSub[V]) unsubscribe(lastAcked *atomic.Uint64) {
    ok := x.sendMu.TryRLock()
    // Spin without touching pongMu (atomic load only) so a departing
    // subscriber never starves the sender's barrier lock.
    for !ok && x.pongN.Load() == 0 {
        runtime.Gosched()
        ok = x.sendMu.TryRLock()
    }
    x.pongMu.Lock()
    // lastAcked is written by ackTracked under pongMu, so reading it here
    // makes the "already acked the armed round" check race-free.
    acked := uint64(0)
    if lastAcked != nil {
        acked = lastAcked.Load()
    }
    x.subs.Add(-1)
    if !ok && x.pongN.Load() > 0 && x.armedSeq.Load() != acked {
        if x.pongN.Add(-1) == 0 { x.pongCond.Broadcast() }
    }
    x.pongMu.Unlock()
    if ok {
        x.sendMu.RUnlock()
    }
}
func (x *EpochPubSub[V]) Unsubscribe() {
    x.unsubscribe(nil)
}

func (x *EpochPubSub[V]) pongNLoad() int32 {
    return x.pongN.Load()
}

func (x *EpochPubSub[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
    if ctx == nil {
        ctx = context.Background()
    }
    // Register and capture the current epoch atomically with respect to
    // Send (which holds the write lock for a whole round): the captured
    // sequence is therefore the last *completed* round, never a round this
    // subscription is about to owe an ack for. Values published before
    // this subscription existed are not replayed.
    x.sendMu.RLock()
    x.subs.Add(1)
    startSeq := x.cur.Load().seq
    x.sendMu.RUnlock()
    // lastAcked is read under pongMu by unsubscribe, and written under pongMu
    // by Ack, so the ack/compensation guard is race-free.
    lastAcked := &atomic.Uint64{}
    stop := context.AfterFunc(ctx, func() { x.unsubscribe(lastAcked) })
    return func(yield func(V) bool) {
        if !stop() { return }
        defer x.unsubscribe(lastAcked)
        lastYielded := startSeq
        lastAcked.Store(startSeq)
        for {
            e := x.cur.Load()
            // Ack every round this subscription was counted in, even if a
            // newer epoch superseded the value before we yielded it -
            // otherwise Send can deadlock waiting for a phantom ack.
            // Acknowledge BEFORE yielding, matching ChanPubSub semantics:
            // Send is released once the value has been received, not once
            // the consumer has finished processing it.
            if e.seq != 0 && e.seq != lastAcked.Load() {
                x.ackTracked(e.seq, lastAcked)
                if e.seq != lastYielded {
                    lastYielded = e.seq
                    if !yield(e.val) { return }
                }
            }
            select {
            case <-ctx.Done(): return
            case <-e.notify: // spin to new epoch
            }
        }
    }
}
