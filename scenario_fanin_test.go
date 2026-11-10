package bigbuff

// Fan-in facets: M producers, one batch-oriented consumer. Bounded per-lane
// capacity, drop-newest with push-time accounting, FIFO per producer lane,
// no global ordering.

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

type faninLike struct {
	producers int
	laneCap   int
	// fillCap is how many lane-0 pushes a fresh fixture accepts before
	// refusing: the lane capacity (edge) or the shared buffer capacity
	// (baseline).
	fillCap int
	push    func(lane int, v uint64) bool
	drain   func(dst []uint64) int
	next    func(ctx context.Context) (uint64, bool, error)
	close   func()
}

func faninLikes() map[string]func() faninLike {
	const producers = 4
	const laneCap = 64
	return map[string]func() faninLike{
		"EdgeFanIn68Chatgpt (68-ChatGPT)": func() faninLike {
			q := NewEdgeFanIn68Chatgpt[uint64]()
			lanes := make([]*EdgeProducer68Chatgpt[uint64], 0, producers)
			for i := 0; i < producers; i++ {
				p, err := q.NewProducer(laneCap)
				if err != nil {
					panic(err)
				}
				lanes = append(lanes, p)
			}
			return faninLike{
				producers: producers,
				laneCap:   laneCap,
				fillCap:   laneCap,
				push:      func(lane int, v uint64) bool { return lanes[lane].TryPush(v) },
				drain:     func(dst []uint64) int { return q.Drain(dst) },
				next:      func(ctx context.Context) (uint64, bool, error) { return q.Next(ctx) },
				close:     func() { q.Close() },
			}
		},
		"BaselineFanIn (baseline)": func() faninLike {
			b := NewBaselineFanIn[uint64](producers * laneCap)
			return faninLike{
				producers: producers,
				laneCap:   laneCap,
				fillCap:   producers * laneCap,
				push:      func(lane int, v uint64) bool { return b.TryPush(v) },
				drain:     func(dst []uint64) int { return b.Drain(dst) },
				next:      func(ctx context.Context) (uint64, bool, error) { return b.Next(ctx) },
				close:     func() { b.Close() },
			}
		},
	}
}

func TestScenarioFanin(t *testing.T) {
	for _, a := range faninAttachments {
		a := a
		t.Run(a.Impl, func(t *testing.T) {
			mk := faninLikes()[a.Impl]
			if mk == nil {
				t.Fatalf("no factory for %q", a.Impl)
			}

			// Facet 1: filling past capacity refuses pushes (drop-newest) with
			// exact accepted/refused accounting.
			x := mk()
			fill := x.fillCap
			accepted := 0
			for i := 0; i < fill+5; i++ {
				if x.push(0, uint64(1000+i)) {
					accepted++
				}
			}
			// values are pushed as 1000+i; accepted window is [1000, 1000+fill)
			if accepted != fill {
				t.Fatalf("lane 0 accepted %d values, want exactly %d before refusal", accepted, fill)
			}
			buf := make([]uint64, fill)
			n := x.drain(buf)
			if n != fill {
				t.Fatalf("drained %d values, want %d", n, fill)
			}
			var lane0 []uint64
			for _, v := range buf[:n] {
				if v < 1000 || v >= uint64(1000+fill) {
					t.Fatalf("drained phantom value %d", v)
				}
				lane0 = append(lane0, v)
			}
			for i := 1; i < len(lane0); i++ {
				if lane0[i] <= lane0[i-1] {
					t.Fatalf("lane 0 order violated: %v", lane0)
				}
			}
			x.close()

			// Facet 1b (fresh fixture): a non-full fixture accepts bounded
			// pushes on another lane and preserves per-lane order.
			x = mk()
			for i := 0; i < 3; i++ {
				if !x.push(1, uint64(2000+i)) {
					t.Fatalf("lane 1 refused value %d on a fresh fixture", i)
				}
			}
			n = x.drain(buf)
			if n != 3 {
				t.Fatalf("drained %d values from lane 1, want 3", n)
			}
			for i := 1; i < n; i++ {
				if buf[i] <= buf[i-1] {
					t.Fatalf("lane 1 order violated: %v", buf[:n])
				}
			}
			x.close()

			// Facet 2: concurrent merge of four lanes; exact, ordered delivery.
			x = mk()
			const perLane = 250
			start := make(chan struct{})
			var producersWG sync.WaitGroup
			for l := 0; l < 4; l++ {
				l := l
				producersWG.Add(1)
				go func() {
					defer producersWG.Done()
					<-start
					for seq := uint64(0); seq < perLane; seq++ {
						v := uint64(l)<<32 | seq
						for !x.push(l, v) {
							runtime.Gosched()
						}
					}
				}()
			}
			var drainWG sync.WaitGroup
			drained := make(chan []uint64, 1)
			drainWG.Add(1)
			go func() {
				defer drainWG.Done()
				got := make([]uint64, 0, 4*perLane)
				tmp := make([]uint64, 64)
				deadline := time.After(scenarioGuard)
				for len(got) < 4*perLane {
					if m := x.drain(tmp); m > 0 {
						got = append(got, tmp[:m]...)
						continue
					}
					select {
					case <-deadline:
						drained <- nil // signal timeout; assertions below catch it
						return
					default:
						runtime.Gosched()
					}
				}
				drained <- got
			}()
			close(start)
			producersWG.Wait()
			select {
			case got := <-drained:
				if got == nil {
					t.Fatal("consumer timed out waiting for merged values")
				}
				consumed := make([]uint64, 4)
				seen := make(map[uint64]bool, len(got))
				for _, v := range got {
					if seen[v] {
						t.Fatalf("duplicate value delivered: %#x", v)
					}
					seen[v] = true
					l := int(v >> 32)
					seq := v & 0xffffffff
					if seq != consumed[l] {
						t.Fatalf("lane %d FIFO violated: got seq %d, want %d", l, seq, consumed[l])
					}
					consumed[l]++
				}
				for l := range consumed {
					if consumed[l] != perLane {
						t.Fatalf("lane %d delivered %d values, want %d", l, consumed[l], perLane)
					}
				}
			case <-time.After(scenarioGuard):
				t.Fatal("merge did not complete")
			}
			drainWG.Wait()
			x.close()

			// Facet 3: Close stops admission; accepted values drain; Next then
			// reports closure.
			x = mk()
			for l, base := range []uint64{700, 800, 900} {
				for i := uint64(0); i < 5; i++ {
					if !x.push(l, base+i) {
						t.Fatalf("lane %d refused value before Close", l)
					}
				}
			}
			x.close()
			if x.push(0, 1234) {
				t.Fatal("push after Close was accepted")
			}
			got := make([]uint64, 0, 15)
			tmp := make([]uint64, 16)
			for {
				m := x.drain(tmp)
				if m == 0 {
					break
				}
				got = append(got, tmp[:m]...)
			}
			if len(got) != 15 {
				t.Fatalf("post-Close drain yielded %d values, want 15", len(got))
			}
			if _, ok, err := x.next(context.Background()); ok || err != nil {
				t.Fatalf("Next after Close and drain = (ok=%v, err=%v), want (false, nil)", ok, err)
			}

			// Facet 4: a cancelled context is reported, not swallowed.
			x = mk()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, _, err := x.next(ctx); err != context.Canceled {
				t.Fatalf("Next on a cancelled context = %v, want context.Canceled", err)
			}
			x.close()
		})
	}
}

func BenchmarkScenarioFaninMerge(b *testing.B) {
	for _, a := range faninAttachments {
		a := a
		b.Run(a.Impl, func(b *testing.B) {
			x := faninLikes()[a.Impl]()
			const senders = 4
			ctx, cancel := context.WithCancel(context.Background())
			drainDone := make(chan struct{})
			go func() {
				defer close(drainDone)
				need := int64(b.N) * senders
				for got := int64(0); got < need; {
					_, ok, err := x.next(ctx)
					if err != nil || !ok {
						return
					}
					got++
				}
			}()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for l := 0; l < senders; l++ {
					v := uint64(i)<<8 | uint64(l)
					for !x.push(l, v) {
						runtime.Gosched()
					}
				}
			}
			b.StopTimer()
			cancel()
			select {
			case <-drainDone:
			case <-time.After(30 * time.Second):
				b.Fatal("consumer did not drain pushed values")
			}
			x.close()
		})
	}
}

// FuzzScenarioFaninModel drives a random serial op stream against the
// bounded-loss per-lane FIFO model: every drained value must be one that was
// accepted, per-lane delivery must be exactly in push order with no gaps or
// duplicates, and pushes after Close must all fail.
func FuzzScenarioFaninModel(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 1, 0, 2})
	f.Add([]byte{2, 0, 0, 1, 3, 1, 3, 1})
	f.Add([]byte{0, 0, 0, 1, 1, 2, 1, 3})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 || len(ops) > 64 {
			return
		}
		for _, a := range faninAttachments {
			x := faninLikes()[a.Impl]()
			const lanes = 4
			pushed := make([]uint64, lanes)
			consumed := make([]uint64, lanes)
			seen := map[uint64]bool{}
			closed := false
			for _, op := range ops {
				switch op % 4 {
				case 0: // push lane (op>>2)%4
					l := int(op>>2) % lanes
					v := uint64(l)<<32 | pushed[l]
					if x.push(l, v) {
						pushed[l]++
					}
				case 1: // drain up to 8 and verify model
					dst := make([]uint64, 8)
					n := x.drain(dst)
					for i := 0; i < n; i++ {
						v := dst[i]
						l := int(v >> 32)
						seq := v & 0xffffffff
						if l >= lanes {
							t.Fatalf("%s: drained value from no lane: %#x", a.Impl, v)
						}
						if seq != consumed[l] {
							t.Fatalf("%s: lane %d seq %d, want %d (FIFO)", a.Impl, l, seq, consumed[l])
						}
						if seq >= pushed[l] {
							t.Fatalf("%s: lane %d seq %d not yet accepted (phantom)", a.Impl, l, seq)
						}
						if seen[v] {
							t.Fatalf("%s: duplicate value %#x", a.Impl, v)
						}
						seen[v] = true
						consumed[l]++
					}
				case 2: // close
					closed = true
					x.close()
				case 3: // push-after-close must fail once closed
					if closed && x.push(0, 0xffffffffffffffff) {
						t.Fatalf("%s: push accepted after Close", a.Impl)
					}
				}
			}
			// Final drain must yield exactly the accepted, unconsumed prefix.
			for {
				dst := make([]uint64, 16)
				n := x.drain(dst)
				if n == 0 {
					break
				}
				for i := 0; i < n; i++ {
					v := dst[i]
					l := int(v >> 32)
					seq := v & 0xffffffff
					if l < lanes && seq == consumed[l] && seq < pushed[l] {
						consumed[l]++
					}
				}
			}
			for l := 0; l < lanes; l++ {
				if consumed[l] != pushed[l] {
					t.Fatalf("%s: lane %d consumed %d of %d accepted", a.Impl, l, consumed[l], pushed[l])
				}
			}
			x.close()
		}
	})
}
