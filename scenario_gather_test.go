package bigbuff

// Gather facets: N-to-1 barriers under withdrawal.

import (
	"sync"
	"testing"
	"time"
)

type gatherLike struct {
	wait func() int
	add  func(int) int
	send func() // one signal send on the shared channel
}

func gatherLikes() map[string]func() gatherLike {
	return map[string]func() gatherLike{
		"ChanGatherer": func() gatherLike {
			x := NewChanGatherer(make(chan struct{}))
			return gatherLike{x.Wait, x.Add, func() { x.C <- struct{}{} }}
		},
		"BaselineGatherBarrier": func() gatherLike {
			x := NewBaselineGatherBarrier[struct{}]()
			return gatherLike{x.Wait, x.Add, func() { x.C <- struct{}{} }}
		},
	}
}

func TestScenarioGather(t *testing.T) {
	for _, a := range gatherAttachments {
		t.Run(a.Impl, func(t *testing.T) {
			mk := gatherLikes()[a.Impl]
			x := mk()
			if got := x.wait(); got != 0 {
				t.Fatalf("Wait with no senders = %d, want 0", got)
			}
			const senders = 8
			for i := 0; i < senders; i++ {
				x.add(1)
			}
			var wg sync.WaitGroup
			for i := 0; i < senders; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); x.send() }()
			}
			done := make(chan int, 1)
			go func() { done <- x.wait() }()
			select {
			case n := <-done:
				if n != senders {
					t.Fatalf("Wait = %d, want %d", n, senders)
				}
			case <-time.After(scenarioGuard):
				t.Fatal("Wait blocked with all senders signalling")
			}
			wg.Wait()
			// Withdrawal compensation: register a sender that never signals,
			// withdraw it during Wait, Wait must still return.
			x.add(1)
			done2 := make(chan int, 1)
			go func() { done2 <- x.wait() }()
			x.add(-1)
			select {
			case <-done2:
			case <-time.After(scenarioGuard):
				t.Fatal("Wait hung after sender withdrew without signalling")
			}
		})
	}
}
