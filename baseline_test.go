package bigbuff

import (
	"context"
	"sync"
	"testing"
)

func TestChanCaster_Basic(t *testing.T) {
	ch := make(chan int)
	cc := NewChanCaster(ch)

	var wg sync.WaitGroup
	const n = 3
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			cc.Add(1)
			val := <-ch
			if val != 42 {
				t.Errorf("expected 42, got %d", val)
			}
		}()
	}

	// Wait for receivers to register
	for cc.Add(0) < n {
	}

	sent := cc.Send(42)
	if sent != n {
		t.Fatalf("expected sent=%d, got %d", n, sent)
	}

	wg.Wait()
}

func TestChanPubSub_Basic(t *testing.T) {
	ch := make(chan string)
	ps := NewChanPubSub(ch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numSubs = 4
	var wg sync.WaitGroup
	wg.Add(numSubs)

	for i := 0; i < numSubs; i++ {
		go func() {
			defer wg.Done()
			for v := range ps.SubscribeContext(ctx) {
				if v == "stop" {
					break
				}
			}
		}()
	}

	for ps.Add(0) < numSubs {
	}

	sent := ps.Send("hello")
	if sent != numSubs {
		t.Fatalf("expected %d sent, got %d", numSubs, sent)
	}

	sent = ps.Send("stop")
	if sent != numSubs {
		t.Fatalf("expected %d sent for stop, got %d", numSubs, sent)
	}

	wg.Wait()
}
