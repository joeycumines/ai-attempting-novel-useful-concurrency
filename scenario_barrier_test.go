package bigbuff

// Deadline-bounded barrier facets. A publisher hands each value to every
// current member and waits for all of them, but (unlike the plain lock-step
// barrier scenario, whose Broadcaster contract has no sender context and no
// separate acknowledgement) the publisher carries a deadline, members may
// leave mid-broadcast, and an acknowledgement may arrive after the publisher
// gave up. The apex-prompt responses (72, 73, 74) all target exactly this
// shape, each with a different mechanism.

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type barrierMember struct {
	recv  func(ctx context.Context) (int, bool)
	ack   func() // acknowledges the value most recently returned by recv
	leave func()
}

type barrierLike struct {
	send  func(ctx context.Context, v int) error // nil iff the barrier completed
	join  func() barrierMember
	close func()
	// splitAck is false when receiving a value is itself the
	// acknowledgement (ack is then a no-op).
	splitAck bool
	// conflates is true when a member that did not acknowledge an abandoned
	// value may be handed the following value in its place.
	conflates bool
}

func barrierLikes() map[string]func() barrierLike {
	return map[string]func() barrierLike{
		bPhaseCaster72Ack: func() barrierLike {
			c := NewPhaseCaster72Gemini[int](0)
			return barrierLike{
				splitAck: true,
				send:     func(ctx context.Context, v int) error { _, err := c.BroadcastPhased(ctx, v); return err },
				close:    func() { _ = c.Close() },
				join: func() barrierMember {
					sub, ch, err := c.SubscribeManaged()
					if err != nil {
						panic(err)
					}
					var once sync.Once
					return barrierMember{
						recv: func(ctx context.Context) (int, bool) {
							select {
							case v := <-ch:
								return v, true
							case <-ctx.Done():
							case <-c.Done():
							}
							return 0, false
						},
						ack:   sub.Ack,
						leave: func() { once.Do(sub.Unsubscribe) },
					}
				},
			}
		},
		bCohort73: func() barrierLike {
			c := NewCohort73Meta[int]()
			return barrierLike{
				splitAck:  true,
				conflates: true,
				send:      func(ctx context.Context, v int) error { _, err := c.Broadcast(ctx, v); return err },
				close:     c.Close,
				join: func() barrierMember {
					m, ok := c.Join()
					if !ok {
						panic("join on closed cohort")
					}
					var epoch uint64
					return barrierMember{
						recv: func(ctx context.Context) (int, bool) {
							v, e, err := m.Wait(ctx)
							if err != nil {
								return 0, false
							}
							epoch = e
							return v, true
						},
						ack:   func() { m.Ack(epoch) },
						leave: m.Leave,
					}
				},
			}
		},
		bFanout74: func() barrierLike {
			f := NewFanout74Deepseek[int]()
			return barrierLike{
				conflates: true,
				send:      f.Send,
				close:     func() { _ = f.Close() },
				join: func() barrierMember {
					s, err := f.Subscribe()
					if err != nil {
						panic(err)
					}
					return barrierMember{recv: s.Next, ack: func() {}, leave: func() { _ = s.Close() }}
				},
			}
		},
		dBaselineBarrier: func() barrierLike {
			b := NewBaselineDeadlineBarrier[int]()
			return barrierLike{
				splitAck: true,
				send:     func(ctx context.Context, v int) error { _, err := b.Send(ctx, v); return err },
				close:    b.Close,
				join: func() barrierMember {
					m, ok := b.Join()
					if !ok {
						panic("join on closed barrier")
					}
					return barrierMember{recv: m.Recv, ack: m.Ack, leave: m.Leave}
				},
			}
		},
	}
}

func barrierSend(x barrierLike, ctx context.Context, v int) <-chan error {
	done := make(chan error, 1)
	go func() { done <- x.send(ctx, v) }()
	return done
}

func barrierExpect(t testing.TB, done <-chan error, want error, what string) {
	t.Helper()
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("%s = %v, want %v", what, err, want)
		}
	case <-timeAfterGuard():
		t.Fatalf("%s did not return", what)
	}
}

// barrierWorker receives and acknowledges until its context ends, recording
// what it saw.
type barrierWorker struct {
	m    barrierMember
	seen []int
	done chan struct{}
}

func barrierStartWorker(ctx context.Context, m barrierMember) *barrierWorker {
	w := &barrierWorker{m: m, done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for {
			v, ok := m.recv(ctx)
			if !ok {
				return
			}
			w.seen = append(w.seen, v)
			m.ack()
		}
	}()
	return w
}

func TestScenarioBarrier(t *testing.T) {
	for _, a := range barrierAttachments {
		mk := barrierLikes()[a.Impl]
		t.Run(a.Impl, func(t *testing.T) {
			if mk == nil {
				t.Fatalf("no factory for %q", a.Impl)
			}

			// Facet 1: with every member participating, each send completes
			// and every member has every value, in order, when it does.
			t.Run("all-acknowledge", func(t *testing.T) {
				x := mk()
				ctx, cancel := context.WithCancel(context.Background())
				workers := make([]*barrierWorker, 4)
				for i := range workers {
					workers[i] = barrierStartWorker(ctx, x.join())
				}
				for v := 1; v <= 200; v++ {
					barrierExpect(t, barrierSend(x, context.Background(), v), nil, "send")
				}
				cancel()
				x.close()
				for i, w := range workers {
					apexAwait(t, w.done, "member")
					if len(w.seen) != 200 {
						t.Fatalf("member %d saw %d values, want 200", i, len(w.seen))
					}
					for j, v := range w.seen {
						if v != j+1 {
							t.Fatalf("member %d: value %d at position %d", i, v, j)
						}
					}
				}
			})

			// Facet 2: where acknowledgement is a separate step, receipt
			// alone does not release the sender.
			t.Run("acknowledgement-is-the-barrier", func(t *testing.T) {
				x := mk()
				defer x.close()
				if !x.splitAck {
					// Receiving is the acknowledgement for this design; the
					// facet has nothing to distinguish.
					return
				}
				members := []barrierMember{x.join(), x.join()}
				done := barrierSend(x, context.Background(), 7)
				var wg sync.WaitGroup
				for i, m := range members {
					wg.Add(1)
					go func() {
						defer wg.Done()
						if v, ok := m.recv(context.Background()); !ok || v != 7 {
							t.Errorf("member %d received (%d, %v), want (7, true)", i, v, ok)
						}
					}()
				}
				received := make(chan struct{})
				go func() { wg.Wait(); close(received) }()
				apexAwait(t, received, "delivery")
				for _, m := range members {
					apexStillBlocked(t, done, "send with acknowledgements outstanding")
					m.ack()
				}
				barrierExpect(t, done, nil, "send")
			})

			// Facet 3: a member that never participates and then leaves
			// releases the sender, whether it leaves before or during the send.
			t.Run("departure-releases-sender", func(t *testing.T) {
				for range 200 {
					x := mk()
					ctx, cancel := context.WithCancel(context.Background())
					w := barrierStartWorker(ctx, x.join())
					silent := x.join()
					done := barrierSend(x, context.Background(), 1)
					silent.leave()
					barrierExpect(t, done, nil, "send with a departing member")
					silent.leave() // idempotent
					barrierExpect(t, barrierSend(x, context.Background(), 2), nil, "send after the departure")
					cancel()
					x.close()
					apexAwait(t, w.done, "member")
					if len(w.seen) != 2 || w.seen[0] != 1 || w.seen[1] != 2 {
						t.Fatalf("participating member saw %v, want [1 2]", w.seen)
					}
				}
			})

			// Facet 4: a stalled member costs the sender its deadline, not
			// its liveness; the next send proceeds, participating members
			// lose nothing, and the stalled member's late acknowledgement is
			// harmless.
			t.Run("sender-deadline", func(t *testing.T) {
				for range 200 {
					x := mk()
					ctx, cancel := context.WithCancel(context.Background())
					w := barrierStartWorker(ctx, x.join())
					stalled := x.join()

					sendCtx, giveUp := context.WithCancel(context.Background())
					done := barrierSend(x, sendCtx, 1)
					giveUp()
					barrierExpect(t, done, context.Canceled, "send past its deadline")

					// The stalled member wakes up and behaves from now on.
					done = barrierSend(x, context.Background(), 2)
					var stalledSeen []int
					for len(stalledSeen) == 0 || stalledSeen[len(stalledSeen)-1] != 2 {
						v, ok := stalled.recv(context.Background())
						if !ok {
							t.Fatal("stalled member could not receive after waking")
						}
						stalledSeen = append(stalledSeen, v)
						stalled.ack() // for an abandoned value this is the late acknowledgement
					}
					barrierExpect(t, done, nil, "send after the deadline")
					if n := len(stalledSeen); n > 2 || (n == 2 && (stalledSeen[0] != 1 || !x.conflates)) {
						t.Fatalf("stalled member saw %v after waking (conflates=%v)", stalledSeen, x.conflates)
					}

					ws := barrierStartWorker(ctx, stalled)
					barrierExpect(t, barrierSend(x, context.Background(), 3), nil, "send with every member participating again")
					cancel()
					x.close()
					apexAwait(t, w.done, "member")
					apexAwait(t, ws.done, "recovered member")
					if len(ws.seen) != 1 || ws.seen[0] != 3 {
						t.Fatalf("recovered member saw %v, want [3]", ws.seen)
					}
					// The participating member may or may not have been
					// handed the abandoned value; it must have 2 and 3, in order.
					switch seen := w.seen; {
					case len(seen) == 2 && seen[0] == 2 && seen[1] == 3:
					case len(seen) == 3 && seen[0] == 1 && seen[1] == 2 && seen[2] == 3:
					default:
						t.Fatalf("participating member saw %v, want [2 3] or [1 2 3]", seen)
					}
				}
			})
		})
	}
}

// BenchmarkScenarioBarrier measures one completed send with every member
// receiving and acknowledging.
func BenchmarkScenarioBarrier(b *testing.B) {
	for _, a := range barrierAttachments {
		mk := barrierLikes()[a.Impl]
		for _, members := range []int{1, 8, 64} {
			b.Run(a.Impl+"/members="+itoa72(members), func(b *testing.B) {
				x := mk()
				ctx, cancel := context.WithCancel(context.Background())
				workers := make([]chan struct{}, members)
				for i := range workers {
					m := x.join()
					done := make(chan struct{})
					workers[i] = done
					go func() {
						defer close(done)
						for {
							if _, ok := m.recv(ctx); !ok {
								return
							}
							m.ack()
						}
					}()
				}
				b.ReportAllocs()
				b.ResetTimer()
				for v := range b.N {
					if err := x.send(context.Background(), v); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				cancel()
				x.close()
				for _, done := range workers {
					<-done
				}
			})
		}
	}
}

// FuzzScenarioBarrier drives every barrier with a fuzzed stream of
// operations against a model. Each op byte selects one of:
//
//	0: a participating member joins
//	1: the newest participating member stops and leaves
//	2: send with no deadline (must complete; every participant must see it)
//	3: a silent member joins, a send is abandoned at its deadline, and the
//	   silent member leaves
//
// Participants check that they see a strictly increasing sequence that
// contains every value sent without a deadline while they were joined.
func FuzzScenarioBarrier(f *testing.F) {
	f.Add([]byte{0, 2, 2})
	f.Add([]byte{0, 0, 2, 1, 2, 3, 2})
	f.Add([]byte{3, 0, 3, 2, 0, 2, 1, 1, 2})
	f.Add([]byte{2, 0, 3, 3, 2})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 || len(ops) > 24 {
			return
		}
		for _, a := range barrierAttachments {
			x := barrierLikes()[a.Impl]()
			type participant struct {
				w    *barrierWorker
				stop context.CancelFunc
				m    barrierMember
				must []int // values sent with no deadline while joined
			}
			var live []*participant
			var all []*participant
			next := 0
			for _, op := range ops {
				switch op % 4 {
				case 0:
					if len(live) < 4 {
						ctx, stop := context.WithCancel(context.Background())
						m := x.join()
						p := &participant{w: barrierStartWorker(ctx, m), stop: stop, m: m}
						live = append(live, p)
						all = append(all, p)
					}
				case 1:
					if n := len(live); n > 0 {
						p := live[n-1]
						live = live[:n-1]
						// Stop, then leave: the worker acknowledges whatever
						// it has in hand before it exits (possibly late, for
						// an abandoned value), and no send runs in between.
						p.stop()
						apexAwait(t, p.w.done, a.Impl+": departing member")
						p.m.leave()
					}
				case 2:
					next++
					barrierExpect(t, barrierSend(x, context.Background(), next), nil, a.Impl+": send")
					for _, p := range live {
						p.must = append(p.must, next)
					}
				case 3:
					next++
					silent := x.join()
					ctx, giveUp := context.WithCancel(context.Background())
					done := barrierSend(x, ctx, next)
					giveUp()
					barrierExpect(t, done, context.Canceled, a.Impl+": send past its deadline")
					silent.leave()
				}
			}
			for _, p := range live {
				p.stop()
				apexAwait(t, p.w.done, a.Impl+": member")
				p.m.leave()
			}
			x.close()
			for i, p := range all {
				seen, k := p.w.seen, 0
				for j, v := range seen {
					if j > 0 && v <= seen[j-1] {
						t.Fatalf("%s: member %d saw %v: not strictly increasing", a.Impl, i, seen)
					}
					if k < len(p.must) && v == p.must[k] {
						k++
					}
				}
				if k != len(p.must) {
					t.Fatalf("%s: member %d saw %v but was owed %v", a.Impl, i, seen, p.must)
				}
			}
		}
	})
}
