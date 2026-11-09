package bigbuff

// Regression tests for BaselineScatterQueue admission under receiver withdrawal.

import (
	"sync"
	"testing"
	"time"
)

// TestBaselineScatterQueueWithdrawalReleasesAdmission races an admission
// against the withdrawal of the last receiver. With no receiver left the
// admission can deliver nothing, so it must return 0 and never block.
func TestBaselineScatterQueueWithdrawalReleasesAdmission(t *testing.T) {
	const rounds = 5000
	x := NewBaselineScatterQueue[int]()
	for i := range rounds {
		x.Add(1)
		start := make(chan struct{})
		done := make(chan int, 1)
		var withdrawn sync.WaitGroup
		withdrawn.Add(1)
		go func() {
			<-start
			done <- x.Send(7)
		}()
		go func() {
			defer withdrawn.Done()
			<-start
			x.Add(-1)
		}()
		close(start)
		select {
		case got := <-done:
			if got != 0 {
				t.Fatalf("round %d: Send returned %d with no receiver, want 0", i, got)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: admission blocked after the last receiver withdrew", i)
		}
		withdrawn.Wait()
	}
}
