package bigbuff

import (
	"sync"
	"testing"
	"time"
)

func TestWatchCaster48GrokNilPayloadAndGeneration(t *testing.T) {
	watch := NewWatchCaster48Grok[any]()
	if value, generation := watch.Load(); value != nil || generation != 0 {
		t.Fatalf("initial Load = (%v, %d), want (nil, 0)", value, generation)
	}
	if generation := watch.Send(nil); generation != 1 {
		t.Fatalf("Send(nil) generation = %d, want 1", generation)
	}
	if value, generation := watch.Load(); value != nil || generation != 1 {
		t.Fatalf("Load after nil send = (%v, %d), want (nil, 1)", value, generation)
	}
	if generation := watch.Send("current"); generation != 2 {
		t.Fatalf("second Send generation = %d, want 2", generation)
	}
	if value, generation := watch.Load(); value != "current" || generation != 2 {
		t.Fatalf("final Load = (%v, %d), want (current, 2)", value, generation)
	}
}

func TestWatchCaster48GrokBroadcastWakeAndCoalescing(t *testing.T) {
	watch := NewWatchCaster48Grok[int]()
	const observers = 32
	notify := watch.Notify()
	var ready sync.WaitGroup
	var woke sync.WaitGroup
	ready.Add(observers)
	woke.Add(observers)
	for range observers {
		go func() {
			defer woke.Done()
			ready.Done()
			<-notify
		}()
	}
	ready.Wait()
	watch.Send(1)
	watch.Send(2)

	done := make(chan struct{})
	go func() { woke.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("one generation notification did not wake every observer")
	}
	if value, generation := watch.Load(); value != 2 || generation != 2 {
		t.Fatalf("Load after coalesced updates = (%d, %d), want (2, 2)", value, generation)
	}
}

func TestWatchCaster48GrokConcurrentSenders(t *testing.T) {
	type value struct{ writer, sequence int }
	const writers, sends = 8, 500
	watch := NewWatchCaster48Grok[value]()
	var wg sync.WaitGroup
	wg.Add(writers)
	for writer := 0; writer < writers; writer++ {
		go func(writer int) {
			defer wg.Done()
			for sequence := 1; sequence <= sends; sequence++ {
				watch.Send(value{writer: writer, sequence: sequence})
			}
		}(writer)
	}
	wg.Wait()
	got, generation := watch.Load()
	if generation != writers*sends {
		t.Fatalf("final generation = %d, want %d", generation, writers*sends)
	}
	if got.writer < 0 || got.writer >= writers || got.sequence < 1 || got.sequence > sends {
		t.Fatalf("final value is invalid: %+v", got)
	}
}

func TestWatchCaster48GrokCoherentSnapshot(t *testing.T) {
	type value struct {
		generation uint64
		checksum   uint64
	}
	const sends = 10000
	watch := NewWatchCaster48Grok[value]()
	var readers sync.WaitGroup
	failed := make(chan value, 1)
	readers.Add(4)
	for range 4 {
		go func() {
			defer readers.Done()
			for {
				v, generation := watch.Load()
				if generation != 0 && (v.generation != generation || v.checksum != generation*37) {
					select {
					case failed <- v:
					default:
					}
					return
				}
				select {
				case <-watch.Notify():
				default:
				}
				if generation == sends {
					return
				}
			}
		}()
	}
	for generation := uint64(1); generation <= sends; generation++ {
		watch.Send(value{generation: generation, checksum: generation * 37})
	}
	readers.Wait()
	select {
	case got := <-failed:
		t.Fatalf("torn value/generation snapshot: %+v", got)
	default:
	}
	if v, generation := watch.Load(); generation != sends || v.generation != sends || v.checksum != sends*37 {
		t.Fatalf("final snapshot = (%+v, %d), want generation %d", v, generation, sends)
	}
}

func BenchmarkWatchCaster48GrokSend(b *testing.B) {
	watch := NewWatchCaster48Grok[int]()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		watch.Send(i)
	}
}
