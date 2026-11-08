package bigbuff

import (
	"sync"
	"testing"
	"time"
)

func TestChanCaster11MetaCASBoundaries(t *testing.T) {
	x := NewChanCaster11Meta(make(chan int))
	if n := x.Send(1); n != 0 {
		t.Fatalf("empty Send=%d", n)
	}
	const adders = 16
	var wg sync.WaitGroup
	for i := 0; i < adders; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); x.Add(1) }()
	}
	wg.Wait()
	if n := x.Add(0); n != adders {
		t.Fatalf("registered=%d want %d", n, adders)
	}
	done := make(chan int, 1)
	go func() { done <- x.Send(7) }()
	// Withdraw one registration while the sender may be armed. The exact
	// linearization is scheduler-dependent, but Send must finish and return
	// either the full snapshot or one fewer, never hang or over-report.
	x.Add(-1)
	got := 0
	for i := 0; i < adders; i++ {
		select {
		case v := <-x.C:
			if v != 7 {
				t.Fatalf("value=%d", v)
			}
			got++
		case <-time.After(scenarioGuard):
			t.Fatalf("receiver %d blocked", i)
		}
		if i == adders-2 {
			// The withdrawn intent may already have been compensated; stop
			// after the maximum possible real receives and check the count.
			break
		}
	}
	select {
	case n := <-done:
		if n < adders-1 || n > adders {
			t.Fatalf("Send=%d outside [%d,%d]", n, adders-1, adders)
		}
	case <-time.After(scenarioGuard):
		t.Fatal("Send blocked after withdrawal")
	}
	// Drain any receive that won the race against withdrawal, then verify the
	// caster returned to idle with no leaked intent.
	for {
		select {
		case <-x.C:
			got++
		default:
			goto idle
		}
	}
idle:
	if n := x.Add(0); n != 0 {
		t.Fatalf("idle registrations=%d", n)
	}
}
