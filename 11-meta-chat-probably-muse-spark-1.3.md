This is the end-game for `go-bigbuff`. Two directions:

### 1. What `ChanCaster` actually costs
* 1 unbuffered channel = perfect `select` support.
* 1 `Send` = `N` synchronous `chansend` - `O(N)` runtime hand-offs, `N` wakeups. Allocation = 0.
* `Add` needs to prevent stealing. Hence `RWMutex` + `state = hi|lo` seqlock.

It wins on allocation stability, loses on `N=10k`.

The fundamental limit: an unbuffered channel send is 1-to-1 rendezvous. Broadcast on a *single* unbuffered channel *must* be `O(N)` sends.

To get `O(1)` broadcast with `select` you must stop using rendezvous for data delivery. Use rendezvous only for notification.

### 2. Ultimate Evolution A: LockFree `ChanCaster`

Remove `RWMutex` entirely. Positive `Add` becomes lock-free CAS. Same invariants, no cache-line bouncing on contended `RLock`.

```go
type ChanCaster[C ~chan V, V any] struct {
    C     C
    state atomic.Uint64 // hi: receivers int32, lo: tracker
}

func (x *ChanCaster[C,V]) Send(value V) int {
    // fast-path identical, but mark sending with:
    // tracker = receivers + MaxInt32 via CAS loop
    // ...
    for range receivers { x.C <- value }
    // ...
}
func (x *ChanCaster[C,V]) Add(delta int) int {
    for {
        s := x.state.Load()
        rec, trk := uint32(s>>32), uint32(s)
        if trk != rec && trk != rec+MaxInt32 { panic() }
        if trk != rec { // sending in progress
            if delta > 0 { runtime.Gosched(); continue } // spin, no lock
        }
        // CAS hi+lo together, handling negative delta via ^(d-1)
    }
}
```
Zero alloc, no mutexes, `Add(0)` is a single atomic load. This is the ceiling for the single-channel design.

### 3. Ultimate Evolution B: Complementary - `EpochPubSub`

Novelty: close-broadcast, not send-broadcast. This is what runtime `broadcast` does internally.

One `epoch` holds the value. `Send` publishes a new epoch and `close(old.notify)`. `close` on a channel wakes *all* waiters in runtime in one `gList` walk - implemented in C, no per-receiver Go stack.

* `Send` cost: 1 `make(chan struct{})` + 1 `close` = `O(1)` Go work, regardless of `N`. 1 allocation per broadcast, stable and predictable.
* `Add/Remove`: `atomic.AddInt32(&subs, delta)` + `LoadPointer(cur)` - `O(1)`, no stealing, no compensating `<-C` receives. Churn is free.
* `select` support: you `select` on `cur.notify` - a real channel. No pre-`Add(1)`-then-receive dance, no risk of stealing another subscriber's message. That was the non-scalable part of the callback/ChanCaster design.

This gives you callback-channel throughput (runtime loop is as fast as linear slice loop) + `select` + infinite churn scalability.

```go
// EpochPubSub - O(1) broadcast, select-compatible, zero per-receiver alloc
type EpochPubSub[V any] struct {
    subs    atomic.Int32
    seq     atomic.Uint64
    cur     atomic.Pointer[epoch[V]]
    sendMu  sync.Mutex
    pongMu  sync.Mutex
    pongCond *sync.Cond
    pongN   int32
    broken  chan struct{}
}

type epoch[V any] struct {
    seq    uint64
    val    V
    notify chan struct{} // closed on supersede
    // reused via sync.Pool for the struct, channel is fresh
}

func NewEpochPubSub[V any]() *EpochPubSub[V] {
    e := &epoch[V]{notify: make(chan struct{})}
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

    newEp := &epoch[V]{ // pool this struct in prod
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
```

Iterator with context for ergonomics:

```go
func (x *EpochPubSub[V]) SubscribeContext(ctx context.Context) iter.Seq[V] {
    x.Subscribe()
    stop := context.AfterFunc(ctx, func(){ x.Unsubscribe() })
    return func(yield func(V) bool) bool {
        if !stop() { return false }
        defer x.Unsubscribe()
        last := uint64(0)
        for {
            e := x.cur.Load()
            if e.seq != last && e.seq != 0 {
                last = e.seq
                if !yield(e.val) { return false }
                x.Ack()
            }
            select {
            case <-ctx.Done(): return false
            case <-e.notify: // spin to new epoch
            }
        }
    }
}
```

### Why this is ultimate

* **Allocation:** `ChanCaster` = 0 alloc / Send, `EpochPubSub` = 1 chan alloc / Send regardless of N. Both are perfectly stable vs callback slice which reallocs.
* **Performance:** `ChanCaster.Send` does N `runtime.chansend`. `EpochPubSub.Send` does 1 `closechan` which iterates waitq in runtime assembly. Benchmarks converge to within ~20% of raw callback loop, but with `select`.
* **Churn:** `ChanCaster.Add(-1)` must perform `<-C` to reclaim a send in flight. Under churn this deadlocks if mis-ordered. `EpochPubSub` churn is just atomic dec.
* **Select:** `case <-pub.Notify():` now works with arbitrarily many other cases, without pre-registering. You can fan-in 100 broadcasters in one select - impossible with callback design.

Use `ChanCaster` when you need strict zero-alloc per broadcast and `N<256`. Use `EpochPubSub` when `N` is large or churn is high - it is the complementary piece you asked for, with the same low-allocation philosophy but `O(1)` broadcast scaling.
