package bigbuff

import (
	"context"
	"sync"
	"testing"
)

func TestEpochTape_BroadcastAndSelect(t *testing.T) {
	tape := NewEpochTape[int](64)

	const numSubscribers = 128
	const numMessages = 500

	var wg sync.WaitGroup
	wg.Add(numSubscribers)

	readyBarrier := sync.WaitGroup{}
	readyBarrier.Add(numSubscribers)

	for i := range numSubscribers {
		go func(id int) {
			defer wg.Done()
			sub := tape.Subscribe()
			readyBarrier.Done()

			received := 0
			for received < numMessages {
				select {
				case <-sub.Ready():
					for {
						val, missed, closed, ok := sub.TryNext()
						if !ok {
							if closed {
								t.Errorf("sub %d: unexpected close at %d", id, received)
								return
							}
							break
						}
						received += int(missed) + 1
						if val < 0 || val >= numMessages {
							t.Errorf("sub %d: invalid value %d", id, val)
							return
						}
					}
				}
			}
		}(i)
	}

	readyBarrier.Wait()

	for m := range numMessages {
		if seq := tape.Publish(m); seq != uint64(m+1) {
			t.Fatalf("expected seq %d, got %d", m+1, seq)
		}
	}

	wg.Wait()
	tape.Close()

	// Verify closed state resolves Ready() and TryNext() immediately.
	sub := tape.Subscribe()
	select {
	case <-sub.Ready():
	default:
		t.Fatal("expected Ready() on closed tape to be immediately selectable")
	}
	if _, _, closed, ok := sub.TryNext(); ok || !closed {
		t.Fatalf("expected closed=true, ok=false; got closed=%v, ok=%v", closed, ok)
	}
}

func TestEpochTape_BoundedLagDetection(t *testing.T) {
	tape := NewEpochTape[int](4) // ring size 4
	sub := tape.Subscribe()

	for i := 1; i <= 10; i++ {
		tape.Publish(i * 10)
	}

	// Subscriber missed messages 1..6 (6 messages) and should read message 7 (70).
	val, missed, closed, ok := sub.TryNext()
	if !ok || closed || missed != 6 || val != 70 {
		t.Fatalf("got val=%d, missed=%d, closed=%v, ok=%v; want val=70, missed=6, closed=false, ok=true",
			val, missed, closed, ok)
	}
}

func TestEpochTape_SubscribeContext(t *testing.T) {
	tape := NewEpochTape[string](16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subSeq := tape.SubscribeContext(ctx)
	done := make(chan []string, 1)

	go func() {
		var out []string
		for v := range subSeq {
			out = append(out, v)
			if len(out) == 3 {
				break
			}
		}
		done <- out
	}()

	// Wait until subscriber is parked or spin-publish
	tape.Publish("alpha")
	tape.Publish("beta")
	tape.Publish("gamma")

	got := <-done
	if len(got) != 3 || got[0] != "alpha" || got[1] != "beta" || got[2] != "gamma" {
		t.Fatalf("unexpected sequence: %v", got)
	}
}

