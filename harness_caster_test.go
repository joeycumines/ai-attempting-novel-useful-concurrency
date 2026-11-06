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
	send func(int) int
	add  func(int) int
	c    chan int
}

func casterImpls() map[string]func() casterAPI {
	return map[string]func() casterAPI{
		"ChanCaster": func() casterAPI {
			x := NewChanCaster(make(chan int))
			return casterAPI{x.Send, x.Add, x.C}
		},
		"ChanCaster2": func() casterAPI {
			x := NewChanCaster2(make(chan int))
			return casterAPI{x.Send, x.Add, x.C}
		},
		"ChanCasterV2_06meta": func() casterAPI {
			x := NewChanCasterV2(make(chan int))
			return casterAPI{x.Send, x.Add, x.C}
		},
		"ChanCaster09Gemini": func() casterAPI {
			x := NewChanCaster09Gemini(make(chan int))
			return casterAPI{x.Send, x.Add, x.C}
		},
		"ChanCaster14Xing": func() casterAPI {
			x := NewChanCaster14Xing(make(chan int))
			return casterAPI{x.Send, x.Add, x.C}
		},
		"ChanCaster15Nemotron": func() casterAPI {
			x := NewChanCaster15Nemotron(make(chan int))
			return casterAPI{x.Send, x.Add, x.C}
		},
		"ChanCaster17FastPath": func() casterAPI {
			x := NewChanCaster17FastPath(make(chan int))
			return casterAPI{x.Send, x.Add, x.C}
		},
		"ChanCaster17Pumped": func() casterAPI {
			x := NewChanCaster17Pumped(make(chan int))
			return casterAPI{x.Send, x.Add, x.C}
		},
	}
}

// runCasterStress: R receivers churn (Add(1) -> select recv | give up -> Add(-1)).
// Invariants: per-Send, number of actual receives == Send's return; per receiver
// strictly increasing values (no double receipt of one Send).
func runCasterStress(t *testing.T, mk func() casterAPI, receivers, sends int, churn bool) {
	x := mk()
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
			timer := time.NewTimer(time.Hour)
			for {
				select {
				case <-stop:
					return
				default:
				}
				x.add(1)
				var tc <-chan time.Time
				if churn && rng.Intn(4) == 0 {
					timer.Reset(time.Duration(rng.Intn(50)) * time.Microsecond)
					tc = timer.C
				}
				select {
				case v := <-x.c:
					if v <= last {
						if violations.Add(1) == 1 {
							firstViolation.Store(fmt.Sprintf("receiver got %d after %d", v, last))
						}
					}
					last = v
					recvCount[v].Add(1)
				case <-tc:
					x.add(-1)
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
			t.Run("steady", func(t *testing.T) { runCasterStress(t, mk, 32, 3000, false) })
			t.Run("churn", func(t *testing.T) { runCasterStress(t, mk, 32, 3000, true) })
		})
	}
}
