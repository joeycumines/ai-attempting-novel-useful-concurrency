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

func ExampleChanRing71Fable() {
	ring := NewChanRing71Fable[string](8)
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

func chanRingTimeout71Fable(t testing.TB, d time.Duration) (stop func()) {
	t.Helper()
	timer := time.AfterFunc(d, func() {
		buf := make([]byte, 1<<20)
		panic("chanring test timed out:\n" + string(buf[:runtime.Stack(buf, true)]))
	})
	return func() { timer.Stop() }
}

func TestNewChanRing71Fable_capacity(t *testing.T) {
	for in, out := range map[int]int{1: 1, 2: 2, 3: 4, 5: 8, 1024: 1024, 1025: 2048} {
		if v := NewChanRing71Fable[int](in).Cap(); v != out {
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
			NewChanRing71Fable[int](in)
		}()
	}
}

func TestChanRing71Fable_zeroValuePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error(`expected panic`)
		}
	}()
	new(ChanRing71Fable[int]).Send(1)
}

func TestChanRing71Fable_basic(t *testing.T) {
	ring := NewChanRing71Fable[int](4)
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
	if _, _, err := a.Recv(nil); err != ErrChanRingClosed71Fable {
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

func TestChanRing71Fable_lag(t *testing.T) {
	ring := NewChanRing71Fable[int](8)
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

func TestChanRing71Fable_misuse(t *testing.T) {
	ring := NewChanRing71Fable[int](1)
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
	expectPanic(`ack`, func() { cur.Ack(ChanRingWake71Fable{}) })
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

func TestChanRing71Fable_Recv_context(t *testing.T) {
	ring := NewChanRing71Fable[int](1)
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
func TestChanRing71Fable_wakeAll(t *testing.T) {
	defer chanRingTimeout71Fable(t, time.Minute)()
	for _, n := range []int{1, 2, 3, 4, 5, 7, 8, 64, 1000, 5000} {
		ring := NewChanRing71Fable[int](4)
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
func TestChanRing71Fable_disarmDuringWake(t *testing.T) {
	defer chanRingTimeout71Fable(t, time.Minute)()
	for range 200 {
		const n = 64
		ring := NewChanRing71Fable[int](4)
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
func TestChanRing71Fable_stress(t *testing.T) {
	defer chanRingTimeout71Fable(t, time.Minute*2)()

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

			ring := NewChanRing71Fable[msg](capacity)

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
						sendChecked71Fable(ring, func(seq uint64) msg {
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

// sendChecked71Fable is a test-only variant of Send, for values derived from their
// own sequence number. It mirrors Send exactly, save for calling build.
func sendChecked71Fable[V any](x *ChanRing71Fable[V], build func(seq uint64) V) {
	x.sendMu.Lock()
	seq := x.tail.Load()
	slot := &x.slots[seq&x.mask]
	slot.lock.Add(chanRingWriter71Fable)
	for i := 0; slot.lock.Load() != chanRingWriter71Fable; i++ {
		if i >= 32 {
			runtime.Gosched()
		}
	}
	slot.seq = seq
	slot.val = build(seq)
	slot.lock.Add(chanRingWriter71Fable)
	x.tail.Store(seq + 1)
	x.sendMu.Unlock()
	x.wake.wake()
}

// As above, but via the real Send, with order verified per-sender.
func TestChanRing71Fable_stressSend(t *testing.T) {
	defer chanRingTimeout71Fable(t, time.Minute*2)()

	const (
		senders     = 4
		perSender   = 20_000
		subscribers = 16
	)
	type msg struct{ sender, n int }

	ring := NewChanRing71Fable[msg](1 << 17) // big enough to never lose
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

func TestChanRing71Fable_allocs(t *testing.T) {
	ring := NewChanRing71Fable[[4]uint64](64)
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
func BenchmarkChanRing71Fable_Broadcast(b *testing.B) {
	for _, n := range []int{1, 8, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf(`ChanRing71Fable/subs=%d`, n), func(b *testing.B) {
			b.ReportAllocs()
			ring := NewChanRing71Fable[int](1024)
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
			ring := NewChanRing71Fable[int](batch * 2)
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
func BenchmarkChanRing71Fable_WakeAll(b *testing.B) {
	for _, n := range []int{1, 100, 10_000, 100_000} {
		b.Run(fmt.Sprintf(`ChanRing71Fable/subs=%d`, n), func(b *testing.B) {
			b.ReportAllocs()
			ring := NewChanRing71Fable[struct{}](1024)
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
func BenchmarkChanRing71Fable_Churn(b *testing.B) {
	const parked = 10_000
	b.Run(`ChanRing71Fable`, func(b *testing.B) {
		b.ReportAllocs()
		ring := NewChanRing71Fable[int](1024)
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

func BenchmarkChanRing71Fable_SendRecv(b *testing.B) {
	b.ReportAllocs()
	ring := NewChanRing71Fable[int](1024)
	cur := ring.Cursor()
	for i := range b.N {
		ring.Send(i)
		if _, _, ok := cur.TryRecv(); !ok {
			b.Fatal()
		}
	}
}
