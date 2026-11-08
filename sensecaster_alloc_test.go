//go:build !race

package bigbuff

import (
	"sync"
	"testing"
)

// The race detector adds its own allocations, so these assertions only hold without -race.
func TestSenseCasterAllocationFree(t *testing.T) {
	c := NewSenseCaster[int]()
	warm := make([]SenseSubscription[int], 64)
	for i := range warm {
		warm[i] = c.Subscribe()
	}
	for i := range warm {
		warm[i].Close()
	}

	if n := testing.AllocsPerRun(1000, func() {
		s := c.Subscribe()
		s.Close()
	}); n != 0 {
		t.Errorf("Subscribe+Close allocates %.2f per cycle, want 0", n)
	}

	idle := c.Subscribe()
	if n := testing.AllocsPerRun(1000, func() {
		_, _ = idle.TryRecv()
		_ = idle.Ready()
	}); n != 0 {
		t.Errorf("idle TryRecv+Ready allocates %.2f per call, want 0", n)
	}
	idle.Close()

	// A select-driven fleet, most of it parked, so each publish takes the wake path.
	const fleet = 16
	c2 := NewSenseCaster[int]()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range fleet {
		sub := c2.Subscribe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if _, ok := sub.TryRecv(); ok {
					continue
				}
				select {
				case <-sub.Ready():
				case <-stop:
					return
				}
			}
		}()
	}
	n := testing.AllocsPerRun(500, func() { c2.Send(1) })
	close(stop)
	wg.Wait()
	if n != 0 {
		t.Errorf("Send to a parked fleet of %d allocates %.2f per publish, want 0", fleet, n)
	}
}
