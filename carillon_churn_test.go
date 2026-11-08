package bigbuff

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// carillonChurn runs one sender against one stable subscription, which must
// observe every value in order, and several churning subscriptions. Each churner
// repeatedly joins, reads up to perSub values, and leaves. Its joins must start
// at or after the sequence published before it subscribed, and its values must
// be consecutive. A small slack keeps the sender blocked, which drives the
// join-versus-sender window much harder than a large one.
func carillonChurn(t *testing.T, slack, total, churners, perSub int) {
	t.Helper()
	c := NewCarillon[uint64](slack)
	stable := c.Subscribe()

	var (
		sent       atomic.Uint64 // values whose Send has returned
		senderDone atomic.Bool
		joins      atomic.Int64
		wg         sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range total {
			v, err := stable.Recv(context.Background())
			if err != nil {
				t.Errorf("stable recv %d: %v", i, err)
				return
			}
			if v != uint64(i) {
				t.Errorf("stable: got %d, want %d", v, i)
				return
			}
		}
	}()

	for range churners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !senderDone.Load() {
				base := sent.Load()
				sub := c.Subscribe()
				joins.Add(1)
				first := true
				var prev uint64
				for got := 0; got < perSub; {
					v, ok := sub.TryRecv()
					if !ok {
						if senderDone.Load() && sub.Lag() == 0 {
							break
						}
						select {
						case <-sub.Ready():
						case <-time.After(time.Millisecond):
						}
						continue
					}
					if first {
						if v < base {
							t.Errorf("joined after %d but first value was %d", base, v)
						}
						first = false
					} else if v != prev+1 {
						t.Errorf("non-consecutive values: %d after %d", v, prev)
					}
					prev = v
					got++
				}
				sub.Unsubscribe()
				// Real subscribers block or yield between joins. A churner that
				// spins without yielding can hold a P for a whole scheduler
				// quantum while the sender waits for it to leave, which measures
				// the scheduler rather than the design.
				runtime.Gosched()
			}
		}()
	}

	// Let the churners get going before the sender runs flat out. Otherwise a
	// single-P scheduler can finish the whole sequence before any churner runs.
	for joins.Load() < int64(churners) {
		runtime.Gosched()
	}
	for i := range total {
		c.Send(uint64(i))
		sent.Store(uint64(i + 1))
		if i%64 == 0 {
			runtime.Gosched()
		}
	}
	senderDone.Store(true)
	wg.Wait()

	if got := joins.Load(); got < int64(churners) {
		t.Fatalf("only %d joins happened; churn did not exercise the join path", got)
	}
	if got := c.Len(); got != 1 {
		t.Fatalf("Len = %d before stable unsubscribe, want 1", got)
	}
	stable.Unsubscribe()
	if got := c.Len(); got != 0 {
		t.Fatalf("Len = %d after all unsubscribed, want 0", got)
	}
}

func TestCarillonChurnStress(t *testing.T) {
	for _, slack := range []int{1, 2, 4, 32} {
		t.Run(fmt.Sprintf("slack=%d", slack), func(t *testing.T) {
			carillonChurn(t, slack, 30000, 6, 40)
		})
	}
	t.Run("join_heavy", func(t *testing.T) {
		carillonChurn(t, 2, 30000, 8, 1)
	})
	t.Run("join_without_read", func(t *testing.T) {
		carillonChurn(t, 1, 20000, 8, 0)
	})
}
