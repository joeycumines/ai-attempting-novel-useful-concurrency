package bigbuff

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// A reader that finds a write in progress has to let other goroutines run:
// the publisher may be waiting for a reader that is in the run queue. Readers
// that retried without yielding held every CPU until the scheduler preempted
// one of them, about 10 ms later, and a publish cost that much.
//
// The test plays the publisher by hand. It marks a slot as being written, the
// way Publish does, starts more readers than there are CPUs on that slot, and
// then times how long the test goroutine takes to get the CPU back 100 times.
func TestEpochTape_ReaderYieldsToAWriteInProgress(t *testing.T) {
	tape := NewEpochTape[int](2)
	readers := 2*runtime.GOMAXPROCS(0) + 2
	subs := make([]EpochSub[int], readers)
	for i := range subs {
		subs[i] = tape.Subscribe()
	}
	tape.Publish(1)
	tape.Publish(2)

	// Sequence 3 goes to the slot that holds sequence 1, the readers' next.
	slot := &tape.ring[3&tape.mask]
	slot.seq.Store(3*2 - 1)

	var wg sync.WaitGroup
	got := make([]int, readers)
	for i := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _, _, ok := subs[i].TryNext()
			if ok {
				got[i] = v
			}
		}()
	}

	// A handoff is a short sleep and a yield. The sleep frees this
	// goroutine's CPU for a reader, so every CPU has a reader on it and the
	// rest wait in the run queues, in front of this goroutine.
	const handoffs = 100
	start := time.Now()
	for range handoffs {
		time.Sleep(20 * time.Microsecond)
		runtime.Gosched()
	}
	elapsed := time.Since(start)

	// Finish the write, as Publish does.
	slot.mu.Lock()
	slot.val = 3
	slot.mu.Unlock()
	slot.seq.Store(3 * 2)
	tape.published.Store(3)
	wg.Wait()

	for i, v := range got {
		if v != 2 {
			t.Errorf("reader %d read %d, want 2 (sequence 1 was overwritten)", i, v)
		}
	}
	// With the yield a handoff takes microseconds. Without it each one waits
	// for a preemption, so 100 of them take a second or more.
	if limit := 500 * time.Millisecond; elapsed > limit {
		t.Errorf("%d handoffs with %d readers retrying on a slot being written took %s, want under %s: the readers do not yield", handoffs, readers, elapsed, limit)
	}
}

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
