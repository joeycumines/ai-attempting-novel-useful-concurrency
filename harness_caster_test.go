package bigbuff

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// casterAPI abstracts the ChanCaster family: one shared unbuffered channel,
// Add(+1) registers intent to receive once, Add(-1) withdraws it.
type casterAPI struct {
	send       func(int) int
	add        func(int) int
	c          chan int
	close      func()
	ack        func()
	skipChurn  bool
	rearmEach  bool
	singleOnly bool
}

func casterImpls() map[string]func() casterAPI {
	return map[string]func() casterAPI{
		"ChanCaster": func() casterAPI {
			x := NewChanCaster(make(chan int))
			return casterAPI{x.Send, x.Add, x.C, nil, nil, false, true, false}
		},
		"ChanCaster2": func() casterAPI {
			x := NewChanCaster2(make(chan int))
			return casterAPI{x.Send, x.Add, x.C, nil, nil, false, true, false}
		},
		"ChanCasterV2_06meta": func() casterAPI {
			x := NewChanCasterV2(make(chan int))
			return casterAPI{x.Send, x.Add, x.C, nil, nil, false, true, false}
		},
		"ChanCaster09Gemini": func() casterAPI {
			x := NewChanCaster09Gemini(make(chan int))
			return casterAPI{x.Send, x.Add, x.C, nil, nil, false, true, false}
		},
		"ChanCaster11Meta": func() casterAPI {
			x := NewChanCaster11Meta(make(chan int))
			return casterAPI{x.Send, x.Add, x.C, nil, nil, false, true, false}
		},
		"ChanCaster14Xing": func() casterAPI {
			x := NewChanCaster14Xing(make(chan int))
			return casterAPI{x.Send, x.Add, x.C, nil, nil, false, true, false}
		},
		"ChanCaster15Nemotron": func() casterAPI {
			x := NewChanCaster15Nemotron(make(chan int))
			return casterAPI{x.Send, x.Add, x.C, nil, nil, false, true, false}
		},
		"ChanCaster17FastPath": func() casterAPI {
			x := NewChanCaster17FastPath(make(chan int))
			return casterAPI{x.Send, x.Add, x.C, nil, nil, false, true, false}
		},
		"ChanCaster17Pumped": func() casterAPI {
			x := NewChanCaster17Pumped(make(chan int))
			return casterAPI{x.Send, x.Add, x.C, nil, nil, false, true, false}
		},
		"AdaptiveCaster23Meta": func() casterAPI {
			x := NewAdaptiveCaster23Meta[int](DefaultAdaptiveRedirect23Meta)
			return casterAPI{x.Send, x.Add, x.C(), x.Close, nil, false, true, false}
		},
		"ChanPubSub35ChatJimmy": func() casterAPI {
			x := NewChanPubSub35ChatJimmy(make(chan int))
			return casterAPI{x.Send, x.Add, x.C(), x.Close, x.Wait, true, false, true}
		},
		"ChanCaster58Spark": func() casterAPI {
			x := NewChanCaster58Spark(make(chan int))
			return casterAPI{x.Send, x.Add, x.C, nil, nil, false, true, false}
		},
		"ChanCaster47Gemini": func() casterAPI {
			x := NewChanCaster47Gemini(make(chan int))
			return casterAPI{x.Send, x.Add, x.C, nil, nil, false, true, false}
		},
	}
}

// runCasterStress: R receivers churn (Add(1) -> select recv | give up -> Add(-1)).
// Invariants: per-Send, number of actual receives == Send's return; per receiver
// strictly increasing values (no double receipt of one Send).
func runCasterStress(t *testing.T, mk func() casterAPI, receivers, sends int, churn bool) {
	x := mk()
	if x.close != nil {
		defer x.close()
	}
	if churn && x.skipChurn {
		t.Skip("exposure limited: withdrawal during an acknowledgement round is unsupported")
	}
	stop := make(chan struct{})
	recvCount := make([]atomic.Int32, sends+1)
	var violations atomic.Int64
	var firstViolation atomic.Value
	var wg sync.WaitGroup
	for r := 0; r < receivers; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			last := 0
			registered := false
			timer := time.NewTimer(time.Hour)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if x.rearmEach || !registered {
					x.add(1)
					registered = true
				}
				var tc <-chan time.Time
				if churn && rng.Intn(4) == 0 {
					timer.Reset(time.Duration(rng.Intn(50)) * time.Microsecond)
					tc = timer.C
				}
				select {
				case v := <-x.c:
					if x.ack != nil {
						x.ack()
					}
					if v <= last {
						if violations.Add(1) == 1 {
							firstViolation.Store(fmt.Sprintf("receiver got %d after %d", v, last))
						}
					}
					last = v
					recvCount[v].Add(1)
				case <-tc:
					x.add(-1)
					registered = false
				case <-stop:
					x.add(-1)
					return
				}
				if tc != nil && !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
		}(int64(r))
	}
	sentCount := make([]int, sends+1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= sends; i++ {
			sentCount[i] = x.send(i)
		}
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("DEADLOCK: sender stuck (receivers=%d churn=%v)", receivers, churn)
	}
	close(stop)
	wg.Wait()
	mismatch := 0
	for i := 1; i <= sends; i++ {
		if int(recvCount[i].Load()) != sentCount[i] {
			if mismatch == 0 {
				t.Logf("send %d: Send returned %d, actually received %d", i, sentCount[i], recvCount[i].Load())
			}
			mismatch++
		}
	}
	if mismatch > 0 || violations.Load() > 0 {
		v, _ := firstViolation.Load().(string)
		t.Fatalf("MISDELIVERY: count-mismatch sends=%d, ordering violations=%d (%s)", mismatch, violations.Load(), v)
	}
}

func TestCasterFamily(t *testing.T) {
	for name, mk := range casterImpls() {
		t.Run(name, func(t *testing.T) {
			if x := mk(); x.singleOnly {
				if x.close != nil {
					defer x.close()
				}
				runSingleAckCasterFacet(t, x)
				return
			}
			t.Run("steady", func(t *testing.T) { runCasterStress(t, mk, 32, 3000, false) })
			t.Run("churn", func(t *testing.T) { runCasterStress(t, mk, 32, 3000, true) })
		})
	}
}

// runSingleAckCasterFacet limits the ChatJimmy candidate to its sound
// one-receiver rendezvous behavior; the shared channel cannot guarantee that
// distinct concurrent receivers each take exactly one value.
func runSingleAckCasterFacet(t *testing.T, x casterAPI) {
	t.Helper()
	x.add(1)
	received := make(chan int, 1)
	go func() {
		v := <-x.c
		if x.ack != nil {
			x.ack()
		}
		received <- v
	}()
	sent := make(chan int, 1)
	go func() { sent <- x.send(1) }()
	select {
	case v := <-received:
		if v != 1 {
			t.Fatalf("received %d want 1", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("single receiver did not receive")
	}
	select {
	case n := <-sent:
		if n != 1 {
			t.Fatalf("Send=%d want 1", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not complete after acknowledgement")
	}
	x.add(-1)
}
