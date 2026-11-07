# Ultimate Broadcast: `ChanCaster` + `ChanPubSub` Evolved + `Broadcaster`

A single unbuffered `chan` can broadcast to `N` `select` waiters
with **zero steady-state allocation** — `N` rendezvous — but the sender
is `O(N)` serial, `Add(+1)` blocks for the whole `Send`, and one slow
consumer stalls all. The classic per-subscriber-`chan` linear callback
fixes `select` ergonomics but is `O(N)` sender work plus `O(N)` churn
copy plus per-subscriber allocation.

The ultimate complement is **lossy, non-blocking, `O(1)`-sender
generation broadcast**: one copy into a power-of-two ring, one
`close(old)` to wake *all* via the runtime `hchan.recvq` in parallel,
receivers drain lock-free in parallel. Subscribe is
no-coordination. One alloc per `Send`, zero per receive.

| | `ChanCaster` reliable | `ChanPubSub` reliable | `Broadcaster` lossy |
|---|---|---|---|
|sender|`O(N)` rendezvous|`O(N)` + `O(N)` pongs|`O(1)` user + `O(N)` runtime `goready`, 1 copy|
|recv alloc|0|0|0|
|send alloc|0|0|1 `hchan`|
|select|per-message `Add(1)`/`Add(-1)`|must `Unsubscribe` on fallback|`Notify()` shared, persistent|
|churn|`Add` blocks on `Send`|subscribe blocks on `Send`|lock-free, counter only|
|slow consumer|stalls sender|stalls sender + `Wait`|never stalls, `missed` count|
|use|every message must arrive|every message must arrive|state/ticks/config, resync on gap|

## 1. `ChanCaster` evolved — padding + safe helpers

Isolate read-only `C` from hot `mutex+state`, pad to 2 lines to avoid
heap-neighbour false sharing. Keep the proven dual-counter protocol,
add `Len`, `TryRecv`, `RecvContext` that encapsulate the
`Add(1)` / `select` / `Add(-1)`-steal dance with stack-only `select`.

```go
type ChanCaster[C ~chan V, V any] struct {
    C C
    _ [56]byte // C read-only, isolate from hot line
    mutex sync.RWMutex // 24
    state atomic.Uint64 // 8, shares line with mutex: single miss
    _ [32]byte // pad to 128 = 2 lines
}

func (x *ChanCaster[C, V]) Len() int { return x.Add(0) }

// RecvContext: Add(1) then select on C vs ctx. Zero alloc.
func (x *ChanCaster[C, V]) RecvContext(ctx context.Context) (V, bool) {
    var zero V
    if ctx == nil {
        ctx = context.Background()
    }
    x.Add(1) // RLock parks on concurrent Send: expected for blocking API
    select {
    case v, ok := <-x.C:
        if !ok {
            x.Add(-1)
            return zero, false
        }
        return v, true
    case <-ctx.Done():
        x.Add(-1) // steals <-x.C internally if Send in flight: must block briefly or Send would hang
        return zero, false
    }
}

// TryRecv: true try w.r.t. messages. Uses TryRLock so it never parks
// for a Send, but fallback Add(-1) must still steal-block if a Send
// started after unlock, or that Send would hang. Documented.
func (x *ChanCaster[C, V]) TryRecv() (V, bool) {
    var zero V
    if !x.mutex.TryRLock() {
        return zero, false
    }
    state := x.state.Add(uint64(1)<<32 | uint64(1))
    receivers, tracker := uint32(state>>32), uint32(state)
    if receivers > math.MaxInt32 || receivers < 1 || receivers != tracker {
        x.mutex.RUnlock()
        panic(`bigbuff: chancaster: add: state invariant violation`)
    }
    x.mutex.RUnlock()
    select {
    case v, ok := <-x.C:
        if !ok {
            x.Add(-1)
            return zero, false
        }
        return v, true
    default:
        x.Add(-1)
        return zero, false
    }
}
```
`Send`/`Add` logic unchanged.

## 2. `ChanPubSub` evolved — non-blocking `Wait`

Single `sync.Cond` + `pongN int` under one mutex is `N` mutex
acquisitions per broadcast. Replace with `pongN atomic.Int32`,
sender parks once on `pongMu+pongCond`, waiters do lock-free
`Add(-1)`, last signals. `Add(subs)` *before* `ping.Send` so `Wait`
never parks; compensate stolen sends after with
`Add(sent-subs)`. Embed `sync.Cond` value to save one alloc.
Spin on `TryRLock` yields via `runtime.Gosched` to avoid burning the
sender's line.

```go
type ChanPubSub[C ~chan V, V any] struct {
    ping ChanCaster[C, V]
    sendMu sync.Mutex
    _ [56]byte
    sendingMu sync.RWMutex
    _ [40]byte
    subscribers atomic.Int32
    _ [60]byte
    pongN atomic.Int32 // hot: N decrements per Send, own line
    _ [60]byte
    pongMu sync.Mutex
    _ [56]byte
    pongCond sync.Cond // L=&pongMu, embedded: no alloc
    broken chan struct{}
}

func NewChanPubSub[C ~chan V, V any](c C) *ChanPubSub[C, V] {
    if cap(c) != 0 {
        panic(`bigbuff: chanpubsub: must use an unbuffered channel`)
    }
    var x ChanPubSub[C, V]
    x.ping.C = c
    x.pongCond.L = &x.pongMu
    x.broken = make(chan struct{})
    return &x
}

func (x *ChanPubSub[C, V]) Wait() {
    x.checkUsedFactoryFunction()
    x.checkBroken()
    // Add-before guarantees pongN already >0: just Done, never park.
    n := x.pongN.Add(-1)
    if n < 0 {
        x.markBroken()
        panic(chanPubSubStateInvariantViolation)
    }
    if n == 0 {
        x.pongMu.Lock()
        x.pongCond.Signal() // only sender waits
        x.pongMu.Unlock()
    }
}

// Send sketch:
 // sendMu.Lock; sendingMu.Lock; subs:=load; pongN.Add(subs); ping.Add(subs)
 // sent:=ping.Send(v); if sent!=subs { pongN.Add(sent-subs) }
 // sendingMu.Unlock; if sent!=0 { pongMu.Lock; for pongN.Load()!=0 { pongCond.Wait; checkBroken }; pongMu.Unlock }
```

Unsubscribe loop becomes:

```go
ok := x.sendingMu.TryRLock()
for !ok && x.ping.Add(0) == 0 {
    x.checkBroken()
    runtime.Gosched() // yield: lets write-holder finish, cuts hchan/mutex line contention
    ok = x.sendingMu.TryRLock()
}
```

## 3. `Broadcaster[V]` — the complementary ultimate

Ring + `swap-then-close` generation. `next` reservation vs
`published` publication separation avoids seeing reserved-but-
unpublished slots. `cur atomic.Value` holds current open
`chan struct{}`; `Swap(new)` then `unlock` then `close(old)` narrows
the critical section. `closedChan` pre-closed and reused so `Close`
allocates zero. Seqs padded to 64B; seqlock double-load brackets
value copies; unsigned `published-next` wrap-safe like TCP seqnos.
`load-then-drain-then-select` avoids missed wakeup.

```go
package bigbuff

import (
    "context"
    "iter"
    "math/bits"
    "sync"
    "sync/atomic"
)

type broadcasterSeq struct {
    v atomic.Uint64
    _ [56]byte
}

type Broadcaster[V any] struct {
    vals []V // read-only header, backing written once per seq
    seqs []broadcasterSeq
    mask uint64
    closedChan chan struct{} // pre-closed, reused
    done chan struct{}       // closed on Close
    next atomic.Uint64 // reservation: Senders only
    _ [56]byte
    published atomic.Uint64 // max published: sender writes, N readers
    _ [56]byte
    cur atomic.Value // chan struct{} current open generation, lock-free loads
    _ [48]byte
    closed atomic.Bool
    _ [60]byte
    subscribers atomic.Int32 // metrics only, no coordination
    _ [60]byte
    sendMu sync.Mutex
    _ [56]byte
}

type Subscription[V any] struct {
    b    *Broadcaster[V]
    next uint64 // single-owner, no atomic, no pad needed
    done atomic.Bool
}

func NewBroadcaster[V any](size int) *Broadcaster[V] {
    if size <= 0 {
        size = 64
    }
    n := 1 << bits.Len64(uint64(size-1)) // round to pow2, 1=>1
    b := &Broadcaster[V]{}
    b.vals = make([]V, n)
    b.seqs = make([]broadcasterSeq, n)
    b.mask = uint64(n - 1)
    b.closedChan = make(chan struct{})
    close(b.closedChan)
    b.done = make(chan struct{})
    b.cur.Store(make(chan struct{}))
    return b
}

func (b *Broadcaster[V]) Send(v V) uint64 {
    b.sendMu.Lock() // explicit, no defer: panic paths fatal, saves ~1ns
    if b.closed.Load() {
        b.sendMu.Unlock()
        panic(`bigbuff: broadcaster: send on closed broadcaster`)
    }
    seq := b.next.Add(1)
    idx := seq & b.mask
    b.vals[idx] = v
    b.seqs[idx].v.Store(seq)
    b.published.Store(seq)
    newCh := make(chan struct{}) // 1 alloc per Send, stable
    oldAny := b.cur.Swap(newCh)
    b.sendMu.Unlock() // wakeups outside critical section
    close(oldAny.(chan struct{})) // runtime batch goready, no copies
    return seq
}

func (b *Broadcaster[V]) Subscribe() *Subscription[V] {
    pub := b.published.Load()
    b.subscribers.Add(1)
    return &Subscription[V]{b: b, next: pub + 1}
}

func (s *Subscription[V]) Unsubscribe() {
    if s.done.Swap(true) {
        return
    }
    s.b.subscribers.Add(-1)
}

func (b *Broadcaster[V]) Notify() <-chan struct{} {
    return b.cur.Load().(chan struct{})
}
func (s *Subscription[V]) Notify() <-chan struct{} { return s.b.Notify() }
func (b *Broadcaster[V]) IsClosed() bool           { return b.closed.Load() }
func (b *Broadcaster[V]) Done() <-chan struct{}   { return b.done }

func (s *Subscription[V]) Next() (v V, ok bool, missed uint64) {
    var zero V
    b := s.b
    orig := s.next
    size := b.mask + 1
    next := orig
    for {
        pub := b.published.Load()
        if diff := pub - next; diff == ^uint64(0) {
            return zero, false, 0 // next == pub+1: caught up
        } else if diff >= size {
            next = pub - size + 1 // jump to oldest, minimal skip
        }
        idx := next & b.mask
        if b.seqs[idx].v.Load() != next {
            continue // overwrote race: reload, forward progress guaranteed for sane size
        }
        v = b.vals[idx] // single parallel copy
        if b.seqs[idx].v.Load() != next {
            continue // tore during copy (size==1 + fast sender): retry
        }
        s.next = next + 1
        return v, true, next - orig
    }
}

func (b *Broadcaster[V]) Close() {
    b.sendMu.Lock()
    defer b.sendMu.Unlock()
    if b.closed.Swap(true) {
        panic(`bigbuff: broadcaster: close of closed broadcaster`)
    }
    old := b.cur.Swap(b.closedChan) // reuse, zero alloc
    close(b.done)
    close(old.(chan struct{}))
}

// Manual loop: load-then-drain-then-select, no miss, no spin on close.
func Example() {
    // b := NewBroadcaster[State](128)
    // sub := b.Subscribe(); defer sub.Unsubscribe()
    // for {
    //   cur := sub.Notify() // 1. load
    //   for { // 2. drain coalesced
    //     v, ok, missed := sub.Next()
    //     if !ok { break }
    //     if missed != 0 { resyncFromSnapshot(); continue }
    //     handle(v)
    //   }
    //   if b.IsClosed() { return } // 3. avoid spinning on closedChan
    //   select {
    //   case <-ctx.Done(): return
    //   case <-b.Done(): // loop to drain final
    //   case <-cur:
    //   }
    // }
}

func (b *Broadcaster[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
    sub := b.Subscribe()
    if ctx == nil {
        ctx = context.Background()
    }
    stop := context.AfterFunc(ctx, sub.Unsubscribe)
    return func(yield func(V) bool) {
        if yield == nil {
            if stop() {
                sub.Unsubscribe()
            }
            panic(`bigbuff: broadcaster: iterator yield func is nil`)
        }
        if !stop() {
            return
        }
        defer sub.Unsubscribe()
        for {
            cur := sub.Notify()
            for {
                v, ok, _ := sub.Next() // skip gaps: size ring to avoid, or use Next manually
                if !ok {
                    break
                }
                if !yield(v) {
                    return
                }
            }
            if b.IsClosed() {
                return
            }
            select {
            case <-ctx.Done():
                return
            case <-b.Done():
            case <-cur:
            }
        }
    }
}
```

Why this is novel and fast at the runtime level: `close` takes one
`hchan.lock` and `goready`s the whole `recvq` batch — no per-receiver
user loop, locks, `sudog`s or `memmove(V)` on the sender. Receivers
copy in parallel on their own cores. `atomic.Value` loads never touch
`hchan`. Reservation/publication split removes the
reserved-but-unpublished race without parking. Padded seqs remove
adjacent-line false sharing; read-only headers stay shared.
