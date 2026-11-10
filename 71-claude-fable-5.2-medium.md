I'll add a third, complementary primitive (a buffered, lag-tolerant broadcast ring with select-compatible wake-ups) and test it under the race detector here, since this session isn't linked to your machine.

I added a third primitive, `ChanRing`, in `chanring.go` with tests and benchmarks in `chanring_test.go`; `ChanCaster` and `ChanPubSub` are untouched. It passes its tests under `-race` at GOMAXPROCS 1–8, but only on Go 1.24.7 in a scratch module alongside the two existing files. The 1.26.10 toolchain your `go.mod` requires wasn't downloadable here, so the repo's own test suite has not been run with it. This session isn't linked to your machine, so drop both files into `/Users/joeyc/dev/go-bigbuff` yourself.

## What it is

`ChanRing[V]` is the asynchronous counterpart to `ChanPubSub`'s synchronous rendezvous: a fixed ring of values with per-subscriber cursors and select-compatible parking.

- **Subscribers are positions.** A `ChanRingCursor` is a plain stack value (ring pointer, next sequence number, loss counter). There is no subscriber registry, so subscribing and unsubscribing write no shared state and allocate nothing.
- **Slow subscribers lose data.** A cursor more than one ring-length behind skips forward. The loss shows up as gaps in the sequence numbers and in `cur.Lost()`.
- **Reads are wait-free and race-detector clean.** Each slot has a tiny writer-preferring lock in one atomic word. A reader that sees the writer bit treats the slot as lost (it is being overwritten) and moves on.
- **Send is O(1) in subscriber count.** It writes one slot, publishes the tail, and does at most one channel send, and only if someone is parked. If every subscriber is busy, it does no channel operations.

## The novel part: wake cascade

Parking uses `ChanCaster`'s idea of counted anonymous tokens on one unbuffered channel, but the fan-out is delegated to the receivers. The sender sends one token carrying a budget of N−1. Each receiver splits its budget in half and forwards it with at most two sends on the same channel. Wake-ups spread as a binary tree across the available Ps, and the count stays exact: N armed waiters get exactly N sends. The round's write lock is taken by the sender and released by whichever goroutine receives the last token.

The select contract mirrors `ChanCaster.Add`:

```go
if cur.Arm() {
    select {
    case w := <-ring.C(): cur.Ack(w)   // forwards the wake
    case <-other:         cur.Disarm()
    }
}
```

`Recv(ctx)` and `SubscribeContext(ctx)` (an `iter.Seq2` of sequence number and value) wrap this for simple use.

## Measurements

Two cores, Go 1.24.7, 0.5s per benchmark, so treat these as indicative.

| Benchmark | ChanRing | Comparison |
|---|---|---|
| Send time, 10,000 parked subscribers | 5.6 µs | ChanCaster 4.5 ms |
| Send time, 100,000 parked subscribers | 2.1 µs | ChanCaster 255 ms |
| Lossless broadcast, 8 subscribers (per message) | 179 ns | per-subscriber channels 627 ns |
| Lossless broadcast, 1,000 subscribers | 13 µs | per-subscriber channels 29 µs |
| Lossless broadcast, 10,000 subscribers | 109 µs | per-subscriber channels 1.41 ms |
| Subscribe + unsubscribe with 10,000 parked | 177 ns, 0 allocs | per-subscriber channels 230 ns, 1 alloc; ChanPubSub 278 ns, 0 allocs |
| Send + receive, uncontended | 41 ns, 0 allocs | — |

Free-running `ChanPubSub` broadcast measured 21 ms per message at 10,000 subscribers. In the "lossless" rows the benchmark applies its own batch back-pressure, because the ring has none.

## Limits to know about

- **No built-in back-pressure.** A free-running sender on this box caused 62–99% loss for subscribers that couldn't keep up. If you need lossless delivery, the sender must gate itself, or use `ChanPubSub`.
- **End-to-end wake latency is not better.** Time until all N subscribers have run was about the same as `ChanCaster` (15.8 ms vs 13.0 ms at 10,000); the gain is that the sender isn't held for it.
- **`Ack` is a hard obligation.** A subscriber that arms and then neither receives nor disarms stalls the cascade and blocks all further `Arm` calls.
- **Send can briefly spin.** This happens only when a reader is mid-copy of the exact slot being overwritten, i.e. exactly one lap behind.
- **Values stay referenced until overwritten.** The ring holds up to its capacity in live values.

---------

````txtar
-- chanring_test.go --
package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func ExampleChanRing() {
	ring := NewChanRing[string](8)
	quit := make(chan struct{})
	done := make(chan struct{})

	cur := ring.Cursor() // subscribed, as of now

	go func() {
		defer close(done)
		for !cur.Closed() {
			for v, seq, ok := cur.TryRecv(); ok; v, seq, ok = cur.TryRecv() {
				fmt.Println(seq, v)
			}
			if !cur.Arm() {
				continue
			}
			select {
			case w := <-ring.C():
				cur.Ack(w)
			case <-quit:
				cur.Disarm()
				return
			}
		}
	}()

	ring.Send(`a`)
	ring.Send(`b`)
	ring.Send(`c`)
	ring.Close()
	<-done

	// Output:
	// 0 a
	// 1 b
	// 2 c
}

func chanRingTimeout(t testing.TB, d time.Duration) (stop func()) {
	t.Helper()
	timer := time.AfterFunc(d, func() {
		buf := make([]byte, 1<<20)
		panic("chanring test timed out:\n" + string(buf[:runtime.Stack(buf, true)]))
	})
	return func() { timer.Stop() }
}

func TestNewChanRing_capacity(t *testing.T) {
	for in, out := range map[int]int{1: 1, 2: 2, 3: 4, 5: 8, 1024: 1024, 1025: 2048} {
		if v := NewChanRing[int](in).Cap(); v != out {
			t.Errorf(`cap %d: got %d, want %d`, in, v, out)
		}
	}
	for _, in := range []int{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf(`expected panic for %d`, in)
				}
			}()
			NewChanRing[int](in)
		}()
	}
}

func TestChanRing_zeroValuePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error(`expected panic`)
		}
	}()
	new(ChanRing[int]).Send(1)
}

func TestChanRing_basic(t *testing.T) {
	ring := NewChanRing[int](4)
	if ring.Send(-1) != 0 { // nobody listening
		t.Fatal()
	}
	a := ring.Cursor()
	if _, _, ok := a.TryRecv(); ok {
		t.Fatal(`unexpected value`)
	}
	for i := range 3 {
		if seq := ring.Send(i * 10); seq != uint64(i+1) {
			t.Fatal(seq)
		}
	}
	b := ring.Cursor()
	ring.Send(30)
	if a.Pending() != 4 || b.Pending() != 1 {
		t.Fatal(a.Pending(), b.Pending())
	}
	for i := range 4 {
		v, seq, ok := a.TryRecv()
		if !ok || v != i*10 || seq != uint64(i+1) {
			t.Fatal(i, v, seq, ok)
		}
	}
	if v, seq, ok := b.TryRecv(); !ok || v != 30 || seq != 4 {
		t.Fatal(v, seq, ok)
	}
	if a.Arm() != true {
		t.Fatal(`expected to arm`)
	}
	a.Disarm()
	ring.Close()
	ring.Close()
	if a.Arm() || !a.Closed() || !ring.Closed() || a.Lost() != 0 {
		t.Fatal(`expected closed`)
	}
	if _, _, err := a.Recv(nil); err != ErrChanRingClosed {
		t.Fatal(err)
	}
	// rewind replays retained history, even after close
	b.Rewind()
	if b.Closed() || b.Seq() != 1 {
		t.Fatal(b.Seq())
	}
	for i := range 4 {
		if v, _, err := b.Recv(context.Background()); err != nil || v != i*10 {
			t.Fatal(i, v, err)
		}
	}
	if !b.Closed() {
		t.Fatal()
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error(`expected panic on send after close`)
			}
		}()
		ring.Send(1)
	}()
}

func TestChanRing_lag(t *testing.T) {
	ring := NewChanRing[int](8)
	cur := ring.Cursor()
	for i := range 100 {
		ring.Send(i)
	}
	if cur.Pending() != 100 {
		t.Fatal(cur.Pending())
	}
	for i := 92; i < 100; i++ {
		v, seq, ok := cur.TryRecv()
		if !ok || v != i || seq != uint64(i) {
			t.Fatal(i, v, seq, ok)
		}
	}
	if _, _, ok := cur.TryRecv(); ok || cur.Lost() != 92 {
		t.Fatal(ok, cur.Lost())
	}
}

func TestChanRing_misuse(t *testing.T) {
	ring := NewChanRing[int](1)
	cur := ring.Cursor()
	expectPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf(`%s: expected panic`, name)
			}
		}()
		fn()
	}
	expectPanic(`ack`, func() { cur.Ack(ChanRingWake{}) })
	expectPanic(`disarm`, func() { cur.Disarm() })
	if !cur.Arm() {
		t.Fatal()
	}
	expectPanic(`arm`, func() { cur.Arm() })
	expectPanic(`rewind`, func() { cur.Rewind() })
	cur.Disarm()
	if ring.wake.state.Load() != 0 {
		t.Fatal()
	}
}

func TestChanRing_Recv_context(t *testing.T) {
	ring := NewChanRing[int](1)
	cur := ring.Cursor()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Millisecond * 10)
		cancel()
	}()
	if _, _, err := cur.Recv(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err := cur.Recv(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if ring.wake.state.Load() != 0 {
		t.Fatal(`leaked registration`)
	}
}

// Every parked subscriber must be woken by a single send, exactly once, and
// the sender must not need to be involved past the first token.
func TestChanRing_wakeAll(t *testing.T) {
	defer chanRingTimeout(t, time.Minute)()
	for _, n := range []int{1, 2, 3, 4, 5, 7, 8, 64, 1000, 5000} {
		ring := NewChanRing[int](4)
		var parked, done sync.WaitGroup
		parked.Add(n)
		done.Add(n)
		var sum atomic.Int64
		for range n {
			cur := ring.Cursor()
			go func() {
				defer done.Done()
				if !cur.Arm() {
					panic(`expected to arm`)
				}
				parked.Done()
				cur.Ack(<-ring.C())
				v, _, ok := cur.TryRecv()
				if !ok {
					panic(`expected value`)
				}
				sum.Add(int64(v))
			}()
		}
		parked.Wait()
		if v := ring.wake.state.Load(); v != uint64(n)<<32 {
			t.Fatal(n, v)
		}
		ring.Send(3)
		done.Wait()
		if sum.Load() != int64(n)*3 {
			t.Fatal(n, sum.Load())
		}
		// the round must have completed, releasing the lock
		ring.wake.mu.Lock()
		if v := ring.wake.state.Load(); v != 0 {
			t.Fatal(n, v)
		}
		ring.wake.mu.Unlock()
	}
}

// Disarms racing a wake round must take (and forward) their tokens.
func TestChanRing_disarmDuringWake(t *testing.T) {
	defer chanRingTimeout(t, time.Minute)()
	for range 200 {
		const n = 64
		ring := NewChanRing[int](4)
		var parked, done sync.WaitGroup
		parked.Add(n)
		done.Add(n)
		quit := make(chan struct{})
		for range n {
			cur := ring.Cursor()
			go func() {
				defer done.Done()
				if !cur.Arm() {
					panic(`expected to arm`)
				}
				parked.Done()
				select {
				case w := <-ring.C():
					cur.Ack(w)
				case <-quit:
					cur.Disarm()
				}
			}()
		}
		parked.Wait()
		go close(quit)
		ring.Send(1)
		done.Wait()
		ring.wake.mu.Lock()
		if v := ring.wake.state.Load(); v != 0 {
			t.Fatal(v)
		}
		ring.wake.mu.Unlock()
	}
}

// The full works: multiple senders, a tiny ring (forcing overruns, and
// writer/reader collisions on slots), blocking subscribers, subscriber churn
// via context cancellation, and manual select-based subscribers.
func TestChanRing_stress(t *testing.T) {
	defer chanRingTimeout(t, time.Minute*2)()

	for _, capacity := range []int{1, 2, 16, 1024} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			const (
				senders     = 4
				perSender   = 20_000
				subscribers = 32
				churners    = 8
				total       = senders * perSender
			)

			type msg struct {
				// multi-word, to make torn reads detectable
				a, b, c uint64
				s       string
			}

			ring := NewChanRing[msg](capacity)

			var wg sync.WaitGroup

			check := func(seq uint64, m msg, last *int64) {
				if int64(seq) <= *last {
					panic(fmt.Sprintf(`seq went backwards: %d <= %d`, seq, *last))
				}
				*last = int64(seq)
				if m.a != seq || m.b != ^seq || m.c != seq*3 || m.s != `x` {
					panic(fmt.Sprintf(`torn or misplaced value: seq=%d %+v`, seq, m))
				}
			}

			// blocking subscribers, present for the duration
			wg.Add(subscribers)
			for i := range subscribers {
				cur := ring.Cursor()
				go func() {
					defer wg.Done()
					var (
						last = int64(-1)
						n    uint64
					)
					if i%2 == 0 {
						// Recv-based
						for {
							m, seq, err := cur.Recv(nil)
							if err != nil {
								break
							}
							check(seq, m, &last)
							n++
							if i%4 == 0 && n%64 == 0 {
								runtime.Gosched() // some are slow
							}
						}
					} else {
						// select-based
						idle := make(chan struct{})
						for !cur.Closed() {
							for m, seq, ok := cur.TryRecv(); ok; m, seq, ok = cur.TryRecv() {
								check(seq, m, &last)
								n++
							}
							if !cur.Arm() {
								continue
							}
							select {
							case w := <-ring.C():
								cur.Ack(w)
							case <-idle:
								panic(`unreachable`)
							}
						}
					}
					if n+cur.Lost() != total {
						panic(fmt.Sprintf(`subscriber %d: received %d + lost %d != %d`, i, n, cur.Lost(), total))
					}
				}()
			}

			// churners: subscribe, receive a few, cancel (often while parked)
			stopChurn := make(chan struct{})
			var churnWG sync.WaitGroup
			churnWG.Add(churners)
			for i := range churners {
				go func() {
					defer churnWG.Done()
					rng := rand.New(rand.NewSource(int64(i)))
					for {
						select {
						case <-stopChurn:
							return
						default:
						}
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rng.Intn(200))*time.Microsecond)
						last := int64(-1)
						for seq, m := range ring.SubscribeContext(ctx) {
							check(seq, m, &last)
							if rng.Intn(50) == 0 {
								break
							}
						}
						cancel()
					}
				}()
			}

			var sendWG sync.WaitGroup
			sendWG.Add(senders)
			for range senders {
				go func() {
					defer sendWG.Done()
					for i := range perSender {
						sendChecked(ring, func(seq uint64) msg {
							return msg{seq, ^seq, seq * 3, `x`}
						})
						if i%1024 == 0 {
							time.Sleep(time.Microsecond * 50) // let subscribers park
						}
					}
				}()
			}
			sendWG.Wait()

			if ring.Tail() != total {
				t.Fatal(ring.Tail())
			}

			close(stopChurn)
			churnWG.Wait()

			ring.Close()
			wg.Wait()

			// quiesced: no registrations, no round in flight
			ring.wake.mu.Lock()
			if v := ring.wake.state.Load(); v != 0 {
				t.Fatal(v)
			}
			ring.wake.mu.Unlock()
		})
	}
}

// sendChecked is a test-only variant of Send, for values derived from their
// own sequence number. It mirrors Send exactly, save for calling build.
func sendChecked[V any](x *ChanRing[V], build func(seq uint64) V) {
	x.sendMu.Lock()
	seq := x.tail.Load()
	slot := &x.slots[seq&x.mask]
	slot.lock.Add(chanRingWriter)
	for i := 0; slot.lock.Load() != chanRingWriter; i++ {
		if i >= 32 {
			runtime.Gosched()
		}
	}
	slot.seq = seq
	slot.val = build(seq)
	slot.lock.Add(chanRingWriter)
	x.tail.Store(seq + 1)
	x.sendMu.Unlock()
	x.wake.wake()
}

// As above, but via the real Send, with order verified per-sender.
func TestChanRing_stressSend(t *testing.T) {
	defer chanRingTimeout(t, time.Minute*2)()

	const (
		senders     = 4
		perSender   = 20_000
		subscribers = 16
	)
	type msg struct{ sender, n int }

	ring := NewChanRing[msg](1 << 17) // big enough to never lose
	var wg sync.WaitGroup
	wg.Add(subscribers)
	for range subscribers {
		go func(seq2 func(func(uint64, msg) bool)) {
			defer wg.Done()
			var next [senders]int
			var want uint64
			for seq, m := range seq2 {
				if seq != want || next[m.sender] != m.n {
					panic(fmt.Sprintf(`unexpected: seq=%d want=%d msg=%+v`, seq, want, m))
				}
				want++
				next[m.sender]++
			}
			if want != senders*perSender {
				panic(want)
			}
		}(ring.SubscribeContext(nil))
	}
	var sendWG sync.WaitGroup
	sendWG.Add(senders)
	for s := range senders {
		go func() {
			defer sendWG.Done()
			for i := range perSender {
				ring.Send(msg{s, i})
				if i%512 == 0 {
					runtime.Gosched()
				}
			}
		}()
	}
	sendWG.Wait()
	ring.Close()
	wg.Wait()
}

func TestChanRing_allocs(t *testing.T) {
	ring := NewChanRing[[4]uint64](64)
	cur := ring.Cursor()
	quit := make(chan struct{})
	close(quit)
	if n := testing.AllocsPerRun(1000, func() {
		ring.Send([4]uint64{1, 2, 3, 4})
		if _, _, ok := cur.TryRecv(); !ok {
			panic(`expected value`)
		}
		c := ring.Cursor() // subscribe
		if !c.Arm() {
			panic(`expected arm`)
		}
		select {
		case w := <-ring.C():
			c.Ack(w)
			panic(`unexpected wake`)
		case <-quit:
			c.Disarm() // unsubscribe
		}
	}); n != 0 {
		t.Errorf(`expected zero allocs, got %v`, n)
	}

	// including a full park + wake cycle
	req, res := make(chan struct{}), make(chan struct{})
	go func() {
		for range req {
			for ring.wake.state.Load() == 0 {
				runtime.Gosched()
			}
			ring.Send([4]uint64{})
			res <- struct{}{}
		}
	}()
	defer close(req)
	if n := testing.AllocsPerRun(1000, func() {
		req <- struct{}{}
		if _, _, err := cur.Recv(nil); err != nil {
			panic(err)
		}
		<-res
	}); n != 0 {
		t.Errorf(`expected zero allocs for park/wake, got %v`, n)
	}
}

// ---------------------------------------------------------------------------
// benchmarks

// throughput: one sender, n subscribers which each consume every value (or
// account for its loss), measured until all are done.
func BenchmarkBroadcast(b *testing.B) {
	for _, n := range []int{1, 8, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf(`ChanRing/subs=%d`, n), func(b *testing.B) {
			b.ReportAllocs()
			ring := NewChanRing[int](1024)
			var wg sync.WaitGroup
			wg.Add(n)
			var lost atomic.Uint64
			for range n {
				cur := ring.Cursor()
				go func() {
					defer wg.Done()
					for {
						if _, _, err := cur.Recv(nil); err != nil {
							lost.Add(cur.Lost())
							return
						}
					}
				}()
			}
			b.ResetTimer()
			for i := range b.N {
				ring.Send(i)
			}
			ring.Close()
			wg.Wait()
			b.StopTimer()
			b.ReportMetric(float64(lost.Load())/float64(n)/float64(b.N)*100, `%lost`)
		})

		// as above, but the sender applies its own (batch) back-pressure, so
		// nothing is lost: comparable semantics to the two below
		b.Run(fmt.Sprintf(`ChanRingLossless/subs=%d`, n), func(b *testing.B) {
			b.ReportAllocs()
			const batch = 512 // half the ring
			ring := NewChanRing[int](batch * 2)
			var wg sync.WaitGroup
			wg.Add(n)
			var lost, acked atomic.Uint64
			for range n {
				cur := ring.Cursor()
				go func() {
					defer wg.Done()
					for {
						_, seq, err := cur.Recv(nil)
						if err != nil {
							lost.Add(cur.Lost())
							return
						}
						if seq%batch == batch-1 {
							acked.Add(1)
						}
					}
				}()
			}
			b.ResetTimer()
			for i := range b.N {
				ring.Send(i)
				if i%batch == batch-1 {
					for want := uint64(i/batch+1) * uint64(n); acked.Load() != want; {
						runtime.Gosched()
					}
				}
			}
			ring.Close()
			wg.Wait()
			b.StopTimer()
			if lost.Load() != 0 {
				b.Fatal(lost.Load())
			}
		})

		b.Run(fmt.Sprintf(`ChanPubSub/subs=%d`, n), func(b *testing.B) {
			b.ReportAllocs()
			c := NewChanPubSub(make(chan int))
			var wg sync.WaitGroup
			wg.Add(n)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			for range n {
				seq := c.SubscribeContext(ctx)
				go func() {
					defer wg.Done()
					for range seq {
					}
				}()
			}
			b.ResetTimer()
			for i := range b.N {
				c.Send(i)
			}
			b.StopTimer()
			cancel()
			wg.Wait()
		})

		// the "linear callback channel" design: one buffered channel per
		// subscriber, sender iterates them (blocking, i.e. lossless)
		b.Run(fmt.Sprintf(`PerSubChan/subs=%d`, n), func(b *testing.B) {
			b.ReportAllocs()
			var wg sync.WaitGroup
			wg.Add(n)
			chans := make([]chan int, n)
			for i := range chans {
				ch := make(chan int, 1024)
				chans[i] = ch
				go func() {
					defer wg.Done()
					for range ch {
					}
				}()
			}
			var mu sync.RWMutex
			b.ResetTimer()
			for i := range b.N {
				mu.RLock()
				for _, ch := range chans {
					ch <- i
				}
				mu.RUnlock()
			}
			for _, ch := range chans {
				close(ch)
			}
			wg.Wait()
			b.StopTimer()
		})
	}
}

// latency: all n subscribers parked (in a select), one send, measured until
// every one of them has observed the value, and parked again.
func BenchmarkWakeAll(b *testing.B) {
	for _, n := range []int{1, 100, 10_000, 100_000} {
		b.Run(fmt.Sprintf(`ChanRing/subs=%d`, n), func(b *testing.B) {
			b.ReportAllocs()
			ring := NewChanRing[struct{}](1024)
			stop := make(chan struct{})
			var all, ready sync.WaitGroup
			all.Add(n)
			ready.Add(n)
			var sendNS atomic.Int64
			for range n {
				cur := ring.Cursor()
				go func() {
					defer all.Done()
					for {
						if cur.Arm() {
							ready.Done()
							select {
							case w := <-ring.C():
								cur.Ack(w)
							case <-stop:
								cur.Disarm()
								return
							}
						}
						for _, _, ok := cur.TryRecv(); ok; _, _, ok = cur.TryRecv() {
						}
					}
				}()
			}
			ready.Wait()
			b.ResetTimer()
			for range b.N {
				ready.Add(n)
				t := time.Now()
				ring.Send(struct{}{})
				sendNS.Add(int64(time.Since(t)))
				ready.Wait()
			}
			b.StopTimer()
			b.ReportMetric(float64(sendNS.Load())/float64(b.N), `ns/send`)
			close(stop)
			all.Wait()
		})

		b.Run(fmt.Sprintf(`ChanCaster/subs=%d`, n), func(b *testing.B) {
			b.ReportAllocs()
			c := NewChanCaster(make(chan struct{}))
			stop := make(chan struct{})
			var all, ready sync.WaitGroup
			all.Add(n)
			ready.Add(n)
			var sendNS atomic.Int64
			for range n {
				go func() {
					defer all.Done()
					for {
						c.Add(1)
						ready.Done()
						select {
						case <-c.C:
						case <-stop:
							c.Add(-1)
							return
						}
					}
				}()
			}
			ready.Wait()
			b.ResetTimer()
			for range b.N {
				ready.Add(n)
				t := time.Now()
				c.Send(struct{}{})
				sendNS.Add(int64(time.Since(t)))
				ready.Wait()
			}
			b.StopTimer()
			b.ReportMetric(float64(sendNS.Load())/float64(b.N), `ns/send`)
			close(stop)
			all.Wait()
		})
	}
}

// churn: the cost of a subscriber coming and going, while a (large) stable
// population of subscribers is parked.
func BenchmarkChurn(b *testing.B) {
	const parked = 10_000
	b.Run(`ChanRing`, func(b *testing.B) {
		b.ReportAllocs()
		ring := NewChanRing[int](1024)
		stop := make(chan struct{})
		var all, ready sync.WaitGroup
		all.Add(parked)
		ready.Add(parked)
		for range parked {
			cur := ring.Cursor()
			go func() {
				defer all.Done()
				if !cur.Arm() {
					panic(`expected arm`)
				}
				ready.Done()
				<-stop
				cur.Disarm()
			}()
		}
		ready.Wait()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				cur := ring.Cursor()
				if !cur.Arm() {
					panic(`expected arm`)
				}
				cur.Disarm()
			}
		})
		b.StopTimer()
		close(stop)
		all.Wait()
	})
	b.Run(`ChanPubSub`, func(b *testing.B) {
		b.ReportAllocs()
		c := NewChanPubSub(make(chan int))
		c.Add(parked)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Subscribe()
				c.Unsubscribe()
			}
		})
		b.StopTimer()
		c.Add(-parked)
	})
	b.Run(`PerSubChan`, func(b *testing.B) {
		b.ReportAllocs()
		var mu sync.RWMutex
		subs := make(map[chan int]struct{}, parked)
		for range parked {
			subs[make(chan int, 16)] = struct{}{}
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ch := make(chan int, 16)
				mu.Lock()
				subs[ch] = struct{}{}
				mu.Unlock()
				mu.Lock()
				delete(subs, ch)
				mu.Unlock()
			}
		})
	})
}

func BenchmarkChanRing_SendRecv(b *testing.B) {
	b.ReportAllocs()
	ring := NewChanRing[int](1024)
	cur := ring.Cursor()
	for i := range b.N {
		ring.Send(i)
		if _, _, ok := cur.TryRecv(); !ok {
			b.Fatal()
		}
	}
}
-- chanring.go --
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
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

// ErrChanRingClosed is returned by [ChanRingCursor.Recv], after the ring was
// closed, and the cursor has consumed every retained value.
var ErrChanRingClosed = errors.New(`bigbuff: chanring: closed`)

const (
	chanRingStateInvariantViolation = `bigbuff: chanring: state invariant violation`

	// chanRingWriter is the writer bit, of chanRingSlot.lock. It is the top
	// bit of a uint32, meaning that adding it a second time clears it, and
	// that any value >= chanRingWriter has the bit set.
	chanRingWriter = 1 << 31
)

type (
	// ChanRing is a buffered, lag-tolerant broadcast log, that is the
	// asynchronous complement to [ChanPubSub], and is built from the same
	// idea as [ChanCaster] (counted, anonymous tokens, on one shared,
	// unbuffered channel), with select support, and without callbacks.
	//
	// # Characteristics
	//
	// Where [ChanPubSub] is a synchronous rendezvous (every send hands a
	// value to every subscriber, and waits for all of them), ChanRing
	// decouples the two sides, entirely:
	//
	//  1. Send is O(1), regardless of the number of subscribers, parked or
	//     otherwise, and never waits on a slow subscriber. It writes one slot
	//     of a fixed ring, publishes the new tail, then performs AT MOST one
	//     channel send (see "wake cascade", below), and only if at least one
	//     subscriber is actually parked. If all subscribers are busy, a send
	//     performs no channel operations at all.
	//  2. A subscriber is a [ChanRingCursor], which is nothing more than a
	//     position. There is no subscriber registry. Subscribing and
	//     unsubscribing are free (no shared writes, no allocation), so
	//     arbitrary subscriber churn costs nothing, and is invisible to both
	//     senders and other subscribers.
	//  3. Steady state allocation is zero, for all operations. The only
	//     allocations are the ring and the channel, at construction.
	//  4. Receiving is wait-free. A subscriber which is behind drains the ring
	//     in a tight loop, at the cost of two uncontended-by-the-sender atomic
	//     adds per value, never parking, and never touching the channel.
	//  5. A subscriber which falls more than the ring's capacity behind does
	//     not stall anyone. It skips forward, to the oldest retained value,
	//     and the loss is observable, both via the (gapped) sequence numbers,
	//     and [ChanRingCursor.Lost].
	//  6. Blocking is select-compatible, via [ChanRing.C], following an
	//     arm/ack/disarm protocol, analogous to the contract of
	//     [ChanCaster.Add].
	//
	// # Wake cascade
	//
	// Parking uses a single unbuffered channel, shared by all subscribers,
	// carrying counted tokens, per [ChanCaster]. Unlike [ChanCaster], where
	// the sender performs one send per registered receiver, a ChanRing sender
	// sends exactly one token, carrying a budget, of the remaining receivers.
	// Each receiver of a token splits that budget in half, and forwards it,
	// via (at most) two further sends, on the same channel. Wake-ups therefore
	// fan out as a binary tree, across however many Ps are available, reaching
	// N parked subscribers in O(log N) hand-offs of latency, with O(1) work
	// on the part of the sender, which doesn't wait for the cascade to finish.
	// The token count is exact: N registered receivers result in exactly N
	// sends, and no receiver can be skipped, or woken twice.
	//
	// # Usage
	//
	// Instances MUST be constructed via [NewChanRing]. For simple cases, use
	// [ChanRing.SubscribeContext], or [ChanRingCursor.Recv]. For use within a
	// select statement, the contract is:
	//
	//	cur := ring.Cursor()
	//	for !cur.Closed() {
	//		for v, seq, ok := cur.TryRecv(); ok; v, seq, ok = cur.TryRecv() {
	//			// handle v
	//		}
	//		if !cur.Arm() {
	//			continue // a value arrived, or the ring was closed
	//		}
	//		select {
	//		case w := <-ring.C():
	//			cur.Ack(w) // MUST be called, promptly, exactly once
	//		case <-other:
	//			cur.Disarm() // MUST be called, promptly, exactly once
	//		}
	//	}
	//
	// That is, values MUST NOT be received from [ChanRing.C], unless armed,
	// and each successful [ChanRingCursor.Arm] MUST be followed by EITHER
	// exactly one receive, from [ChanRing.C], then [ChanRingCursor.Ack], OR
	// [ChanRingCursor.Disarm]. A receive from [ChanRing.C] indicates that
	// state MAY have changed. It is a hint, and may be spurious.
	//
	// # Caveats
	//
	// Values remain referenced, by the ring, until overwritten. Sends are
	// serialised, and totally ordered. A send will wait (briefly, without
	// parking) if, and only if, a subscriber is mid-copy, of the exact slot
	// being overwritten, i.e. that subscriber is precisely one full lap behind.
	ChanRing[V any] struct {
		slots []chanRingSlot[V]
		mask  uint64
		_     [64]byte
		// tail is the sequence number of the next value to be sent, and is
		// the only state shared between senders and (non-parking) receivers,
		// aside from the slot being read
		tail atomic.Uint64
		_    [64]byte
		// sendMu serialises senders (and Close)
		sendMu sync.Mutex
		closed atomic.Bool
		_      [64]byte
		wake   chanCascade
	}

	// ChanRingCursor is a subscriber's position, within a [ChanRing]. It is a
	// plain value, that may live on the stack, and requires no cleanup,
	// provided it isn't armed. It MUST NOT be used concurrently, and MUST NOT
	// be copied while armed.
	//
	// See [ChanRing.Cursor], and the docs for [ChanRing].
	ChanRingCursor[V any] struct {
		ring  *ChanRing[V]
		next  uint64
		lost  uint64
		armed bool
	}

	// ChanRingWake is an opaque wake token, received from [ChanRing.C], which
	// MUST be passed to [ChanRingCursor.Ack], immediately after being
	// received.
	ChanRingWake struct {
		// budget is the number of FURTHER tokens the receiver must cause to
		// be received, i.e. the size of the receiver's subtree, less itself
		budget uint32
	}

	chanRingSlot[V any] struct {
		// lock is a tiny reader-writer lock: The low 31 bits count readers
		// that have pinned the slot, and the top bit is set by the (single)
		// writer. It is writer-preferring, and neither side ever parks:
		// Readers that observe the writer bit back off, and treat the slot
		// as lost (which it is, as it's being overwritten), while the writer
		// spins, only for as long as it takes pinned readers to copy a value.
		lock atomic.Uint32
		// seq and val are guarded by lock
		seq uint64
		val V
	}

	// chanCascade is a select-compatible futex, with a self-propagating
	// (tree-structured) wake-all. It is, effectively, a [ChanCaster], where
	// the work of sending is delegated to the receivers.
	chanCascade struct {
		c chan ChanRingWake
		// mu serves the same purpose as ChanCaster.mutex: Waiters may only be
		// added (read lock) while no wake round is in flight (write lock). The
		// write lock is acquired by the waker, and is released by whichever
		// goroutine receives the final token of the round - NOT the waker.
		mu sync.RWMutex
		// state is two counters. The high 32 bits are the number of armed
		// waiters, not yet claimed by a wake round. The low 32 bits are the
		// number of tokens, of the in-flight wake round (if any), which are
		// yet to be received. At most one of the two is non-zero.
		state atomic.Uint64
	}
)

// NewChanRing constructs a [ChanRing], retaining (at least) the most recent
// capacity values, which is rounded up to a power of two. A panic will occur
// if capacity is not positive.
func NewChanRing[V any](capacity int) *ChanRing[V] {
	if capacity <= 0 || capacity > 1<<30 {
		panic(`bigbuff: chanring: capacity out of bounds`)
	}
	n := uint64(1)
	for n < uint64(capacity) {
		n <<= 1
	}
	x := ChanRing[V]{
		slots: make([]chanRingSlot[V], n),
		mask:  n - 1,
	}
	x.wake.c = make(chan ChanRingWake)
	return &x
}

// Cap returns the number of values retained by the ring.
func (x *ChanRing[V]) Cap() int { return len(x.slots) }

// Tail returns the sequence number that will be assigned to the next value
// sent, which is also the total number of values sent, thus far.
func (x *ChanRing[V]) Tail() uint64 { return x.tail.Load() }

// Closed returns true if [ChanRing.Close] has been called. See also
// [ChanRingCursor.Closed], which additionally accounts for unread values.
func (x *ChanRing[V]) Closed() bool { return x.closed.Load() }

// C returns the wake channel, for use in select statements. It MUST only be
// received from while armed, per the contract documented on [ChanRing]. It
// is never closed.
func (x *ChanRing[V]) C() <-chan ChanRingWake {
	x.check()
	return x.wake.c
}

// Cursor returns a new subscriber, positioned at the tail, such that it will
// observe only values sent after this call.
func (x *ChanRing[V]) Cursor() ChanRingCursor[V] {
	x.check()
	return ChanRingCursor[V]{ring: x, next: x.tail.Load()}
}

// Send broadcasts value, to all current and future cursors, returning the
// sequence number assigned to it. It will panic if the ring is closed.
//
// This method never waits on subscribers. See [ChanRing], for details.
func (x *ChanRing[V]) Send(value V) (seq uint64) {
	x.check()

	x.sendMu.Lock()

	if x.closed.Load() {
		x.sendMu.Unlock()
		panic(`bigbuff: chanring: send on closed ring`)
	}

	seq = x.tail.Load() // N.B. only modified while holding sendMu
	slot := &x.slots[seq&x.mask]

	// acquire the slot, for writing: set the writer bit (turning away new
	// readers, which will correctly consider the slot's old value lost), then
	// wait out any reader which had already pinned it - an event which
	// requires said reader to be exactly one lap behind, and mid-copy
	slot.lock.Add(chanRingWriter)
	for i := 0; slot.lock.Load() != chanRingWriter; i++ {
		if i >= 32 {
			runtime.Gosched()
		}
	}

	slot.seq = seq
	slot.val = value

	slot.lock.Add(chanRingWriter) // clears the writer bit (overflow)

	// publish - must be AFTER releasing the slot (readers treat the writer
	// bit as proof of having been lapped)
	x.tail.Store(seq + 1)

	x.sendMu.Unlock()

	// must be AFTER publishing (pairs with the re-check of ChanRingCursor.Arm)
	x.wake.wake()

	return seq
}

// Close marks the ring as closed, and wakes all parked subscribers. Values
// already sent remain available. Subsequent calls are no-ops, and subsequent
// calls to [ChanRing.Send] will panic.
func (x *ChanRing[V]) Close() {
	x.check()
	x.sendMu.Lock()
	x.closed.Store(true)
	x.sendMu.Unlock()
	x.wake.wake() // N.B. unconditional, to be safe vs concurrent Close calls
}

// SubscribeContext immediately subscribes (per [ChanRing.Cursor]), returning
// a single-use iterator, yielding the sequence number and value, of each
// value subsequently sent, until the context is cancelled, the iterator is
// stopped, or the ring is closed and drained. Gaps in the yielded sequence
// numbers indicate values lost, due to the consumer falling too far behind.
//
// Unlike [ChanPubSub.SubscribeContext], there is nothing to leak, should the
// iterator never be used.
func (x *ChanRing[V]) SubscribeContext(ctx context.Context) iter.Seq2[uint64, V] {
	cur := x.Cursor()
	if ctx == nil {
		ctx = context.Background()
	}
	return func(yield func(uint64, V) bool) {
		for {
			v, seq, err := cur.Recv(ctx)
			if err != nil || !yield(seq, v) {
				return
			}
		}
	}
}

func (x *ChanRing[V]) check() {
	if x.slots == nil {
		panic(`bigbuff: chanring: must use factory function`)
	}
}

// Lost returns the total number of values skipped, by this cursor, due to
// falling more than the ring's capacity behind.
func (c *ChanRingCursor[V]) Lost() uint64 { return c.lost }

// Seq returns the sequence number of the next value this cursor will attempt
// to receive.
func (c *ChanRingCursor[V]) Seq() uint64 { return c.next }

// Pending returns the number of values sent, but not yet received (or lost),
// by this cursor. It may exceed the ring's capacity, if values have been lost.
func (c *ChanRingCursor[V]) Pending() uint64 { return c.ring.tail.Load() - c.next }

// Closed returns true if the ring was closed, AND there are no further values
// for this cursor to receive.
func (c *ChanRingCursor[V]) Closed() bool {
	// N.B. order matters - closed is set after the final send
	return c.ring.closed.Load() && c.ring.tail.Load() == c.next
}

// Rewind repositions the cursor, to the oldest value still retained, allowing
// a new subscriber to replay recent history. It panics if the cursor is armed.
func (c *ChanRingCursor[V]) Rewind() {
	if c.armed {
		panic(`bigbuff: chanring: rewind while armed`)
	}
	tail, n := c.ring.tail.Load(), uint64(len(c.ring.slots))
	c.next = 0
	if tail > n {
		c.next = tail - n
	}
}

// TryRecv receives the next value, and its sequence number, if one is
// available. It never blocks (it is wait-free), and may be called at any
// time, including while armed.
func (c *ChanRingCursor[V]) TryRecv() (value V, seq uint64, ok bool) {
	x := c.ring
	for {
		tail := x.tail.Load()
		seq = c.next
		if seq == tail {
			return value, seq, false
		}

		// if we've been lapped, skip to the oldest value that might remain
		if n := uint64(len(x.slots)); tail-seq > n {
			skip := tail - seq - n
			seq += skip
			c.next = seq
			c.lost += skip
		}

		slot := &x.slots[seq&x.mask]

		// pin the slot, unless it's being written, which (as our seq was
		// already published) can only mean it's being overwritten
		if slot.lock.Add(1) < chanRingWriter && slot.seq == seq {
			value = slot.val
			slot.lock.Add(math.MaxUint32) // unpin
			c.next = seq + 1
			return value, seq, true
		}

		slot.lock.Add(math.MaxUint32) // unpin / undo

		// this specific value was lost (overwritten, or about to be)
		c.next = seq + 1
		c.lost++
	}
}

// Arm registers the cursor as a waiter, on [ChanRing.C], returning true if
// (and only if) it did so, in which case the caller MUST follow up, per the
// contract documented on [ChanRing]. It returns false, without registering,
// if a value is already available, or the ring is closed.
func (c *ChanRingCursor[V]) Arm() bool {
	if c.armed {
		panic(`bigbuff: chanring: already armed`)
	}

	x := c.ring

	if x.tail.Load() != c.next || x.closed.Load() {
		return false // fast path
	}

	x.wake.arm()

	// re-check, now that senders are guaranteed to see us (this is the usual
	// store-then-load on both sides: we registered then loaded the tail, the
	// sender stored the tail then loaded the registrations)
	if x.tail.Load() != c.next || x.closed.Load() {
		x.wake.disarm()
		return false
	}

	c.armed = true
	return true
}

// Ack MUST be called exactly once, immediately after receiving a token from
// [ChanRing.C], which in turn requires that the cursor was armed.
//
// This call is what propagates the wake-up, to the other parked subscribers.
// It performs at most two channel sends, each of which are guaranteed a
// receiver, provided all subscribers obey the contract.
func (c *ChanRingCursor[V]) Ack(token ChanRingWake) {
	if !c.armed {
		panic(`bigbuff: chanring: ack while not armed`)
	}
	c.armed = false
	c.ring.wake.settle(token)
}

// Disarm MUST be called exactly once, if the cursor was armed, and will not
// receive from [ChanRing.C], e.g. because another select case fired.
func (c *ChanRingCursor[V]) Disarm() {
	if !c.armed {
		panic(`bigbuff: chanring: disarm while not armed`)
	}
	c.armed = false
	c.ring.wake.disarm()
}

// Recv blocks until a value is available, the context is cancelled, or the
// ring is closed and the cursor has drained it, in which case the error will
// be [ErrChanRingClosed]. The context may be nil.
func (c *ChanRingCursor[V]) Recv(ctx context.Context) (value V, seq uint64, err error) {
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	x := c.ring
	for {
		closed := x.closed.Load() // N.B. BEFORE attempting to receive
		var ok bool
		if value, seq, ok = c.TryRecv(); ok {
			return value, seq, nil
		}
		if closed {
			return value, seq, ErrChanRingClosed
		}
		if done != nil {
			select {
			case <-done:
				return value, seq, ctx.Err()
			default:
			}
		}
		if !c.Arm() {
			continue
		}
		select {
		case token := <-x.wake.c:
			c.Ack(token)
		case <-done:
			c.Disarm()
			return value, seq, ctx.Err()
		}
	}
}

// arm registers one waiter. The caller MUST subsequently either receive
// exactly one token then call settle, or call disarm.
func (x *chanCascade) arm() {
	x.mu.RLock() // blocks for the duration of any in-flight wake round
	state := x.state.Add(1 << 32)
	x.mu.RUnlock()
	if uint32(state) != 0 || state>>32 > math.MaxInt32 {
		panic(chanRingStateInvariantViolation)
	}
}

// disarm deregisters one waiter, which has not received a token.
func (x *chanCascade) disarm() {
	for {
		state := x.state.Load()
		if state>>32 != 0 {
			// not (yet) claimed by a wake round - simply deregister
			if x.state.CompareAndSwap(state, state-1<<32) {
				return
			}
			continue
		}
		if uint32(state) == 0 {
			panic(chanRingStateInvariantViolation)
		}
		// we are registered, yet not counted in the high bits, so we MUST
		// have been claimed by the in-flight wake round, which cannot
		// complete without us: take our token, and do our part
		x.settle(<-x.c)
		return
	}
}

// settle handles a received token: it forwards the token's budget, then
// accounts for the receive, completing the wake round, if it was the last.
func (x *chanCascade) settle(token ChanRingWake) {
	if n := token.budget; n != 0 {
		// two subtrees, of sizes (1 + left) and (n - 1 - left)
		left := (n - 1) / 2
		x.c <- ChanRingWake{left}
		if right := n - 1 - left; right != 0 {
			x.c <- ChanRingWake{right - 1}
		}
	}
	switch state := x.state.Add(math.MaxUint64); {
	case state>>32 != 0: // includes underflow
		panic(chanRingStateInvariantViolation)
	case state == 0:
		// all tokens received, therefore all sends matched - round complete
		x.mu.Unlock() // N.B. locked by wake, on another goroutine
	}
}

// wake starts a wake round, for all armed waiters, if any. It performs (at
// most) one channel send, and does not wait for the round to complete.
func (x *chanCascade) wake() {
	// fast path: none armed (note: while a round is in flight, this is
	// always the case, since the round claimed them all, and arming blocks)
	if x.state.Load()>>32 == 0 {
		return
	}
	x.mu.Lock()
	for {
		state := x.state.Load()
		n := uint32(state >> 32)
		if n == 0 {
			x.mu.Unlock()
			return
		}
		if uint32(state) != 0 {
			panic(chanRingStateInvariantViolation)
		}
		// claim all armed waiters (CAS to synchronise with disarm)
		if x.state.CompareAndSwap(state, uint64(n)) {
			x.c <- ChanRingWake{n - 1}
			return // N.B. mu remains locked - see settle
		}
	}
}

````
