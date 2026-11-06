package bigbuff

import (
	"strconv"
	"sync"
	"testing"
)

func TestChanLatestCoalescesAndSelects(t *testing.T) {
	var x ChanLatest[int]
	s := x.Subscribe()
	if s == nil {
		t.Fatal("Subscribe returned nil")
	}
	if cap(s.C()) != 1 {
		t.Fatalf("capacity = %d, want 1", cap(s.C()))
	}

	if got := x.Send(1); got != 1 {
		t.Fatalf("Send(1) = %d, want 1", got)
	}
	if got := x.Send(2); got != 1 {
		t.Fatalf("Send(2) = %d, want 1", got)
	}
	if got := x.Send(3); got != 1 {
		t.Fatalf("Send(3) = %d, want 1", got)
	}

	select {
	case got := <-s.C():
		if got != 3 {
			t.Fatalf("received %d, want latest value 3", got)
		}
	default:
		t.Fatal("expected a value to be ready in select")
	}

	if !s.Unsubscribe() {
		t.Fatal("first Unsubscribe should return true")
	}
	if s.Unsubscribe() {
		t.Fatal("second Unsubscribe should return false")
	}
	if _, ok := <-s.C(); ok {
		t.Fatal("channel should be closed")
	}
	if got := x.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() = %d, want 0", got)
	}
}

func TestChanLatestIndependentSubscribers(t *testing.T) {
	var x ChanLatest[int]
	a := x.Subscribe()
	b := x.Subscribe()
	if got := x.Send(10); got != 2 {
		t.Fatalf("Send() = %d, want 2", got)
	}
	if got := <-a.C(); got != 10 {
		t.Fatalf("a received %d, want 10", got)
	}
	if got := <-b.C(); got != 10 {
		t.Fatalf("b received %d, want 10", got)
	}
	a.Unsubscribe()
	if got := x.Send(20); got != 1 {
		t.Fatalf("Send() after a unsubscribe = %d, want 1", got)
	}
	if _, ok := <-a.C(); ok {
		t.Fatal("a channel should be closed")
	}
	if got := <-b.C(); got != 20 {
		t.Fatalf("b received %d, want 20", got)
	}
	b.Unsubscribe()
}

func TestChanLatestClose(t *testing.T) {
	var x ChanLatest[int]
	a := x.Subscribe()
	b := x.Subscribe()
	x.Send(7)
	x.Close()
	x.Close()

	for i, s := range []*ChanLatestSubscription[int]{a, b} {
		if got, ok := <-s.C(); !ok || got != 7 {
			t.Fatalf("subscriber %d buffered receive = (%d, %v), want (7, true)", i, got, ok)
		}
		if _, ok := <-s.C(); ok {
			t.Fatalf("subscriber %d channel should now be closed", i)
		}
		if s.Unsubscribe() {
			t.Fatalf("subscriber %d was already deactivated by Close", i)
		}
	}
	if x.Subscribe() != nil {
		t.Fatal("Subscribe after Close should return nil")
	}
	if got := x.Send(8); got != 0 {
		t.Fatalf("Send after Close = %d, want 0", got)
	}
	if got := x.Subscribers(); got != 0 {
		t.Fatalf("Subscribers after Close = %d, want 0", got)
	}
}

func TestChanLatestConcurrentLifecycle(t *testing.T) {
	var x ChanLatest[uint64]
	var wg sync.WaitGroup

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2_000; i++ {
				x.Send(uint64(i))
			}
		}()
	}
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				s := x.Subscribe()
				if s == nil {
					return
				}
				select {
				case <-s.C():
				default:
				}
				s.Unsubscribe()
			}
		}()
	}
	wg.Wait()
	if got := x.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() = %d, want 0", got)
	}
}

func TestChanLatestCloseRacesWithSendAndUnsubscribe(t *testing.T) {
	for i := 0; i < 100; i++ {
		var x ChanLatest[int]
		var subs [8]*ChanLatestSubscription[int]
		for j := range subs {
			subs[j] = x.Subscribe()
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				x.Send(n)
			}
		}()
		go func() {
			defer wg.Done()
			for _, s := range subs {
				s.Unsubscribe()
			}
		}()
		wg.Wait()
		x.Close()
	}
}

func BenchmarkChanLatestSend(b *testing.B) {
	for _, n := range []int{0, 1, 8, 64, 512} {
		b.Run("subs="+strconv.Itoa(n)+"/empty", func(b *testing.B) {
			var x ChanLatest[uint64]
			subs := make([]*ChanLatestSubscription[uint64], n)
			for i := range subs {
				subs[i] = x.Subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				x.Send(uint64(i))
				for _, s := range subs {
					<-s.C()
				}
			}
		})

		b.Run("subs="+strconv.Itoa(n)+"/overwrite", func(b *testing.B) {
			var x ChanLatest[uint64]
			for i := 0; i < n; i++ {
				x.Subscribe()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				x.Send(uint64(i))
			}
		})
	}
}

func TestChanLatestSlabBoundaryReuseAndTrim(t *testing.T) {
	var x ChanLatest[int]
	const n = 130 // crosses three 64-slot slabs
	subs := make([]*ChanLatestSubscription[int], n)
	for i := range subs {
		subs[i] = x.Subscribe()
		if subs[i] == nil {
			t.Fatalf("Subscribe(%d) returned nil", i)
		}
	}
	if got := x.Subscribers(); got != n {
		t.Fatalf("Subscribers() = %d, want %d", got, n)
	}
	if got := x.Send(99); got != n {
		t.Fatalf("Send() = %d, want %d", got, n)
	}
	for i, s := range subs {
		if got := <-s.C(); got != 99 {
			t.Fatalf("subscriber %d received %d, want 99", i, got)
		}
		if !s.Unsubscribe() {
			t.Fatalf("subscriber %d did not unsubscribe", i)
		}
	}

	if got := x.Trim(); got != 3 {
		t.Fatalf("Trim() = %d, want 3 slabs", got)
	}
	if got := x.Trim(); got != 0 {
		t.Fatalf("second Trim() = %d, want 0", got)
	}

	// A fresh subscription works after the slabs have been released.
	fresh := x.Subscribe()
	if fresh == nil {
		t.Fatal("Subscribe after Trim returned nil")
	}
	if got := x.Send(123); got != 1 {
		t.Fatalf("Send() after Trim = %d, want 1", got)
	}
	if got := <-fresh.C(); got != 123 {
		t.Fatalf("fresh subscription received %d, want 123", got)
	}
	fresh.Unsubscribe()
}

func TestChanLatestTrimKeepsPartiallyActiveSlabs(t *testing.T) {
	var x ChanLatest[int]
	subs := make([]*ChanLatestSubscription[int], 65)
	for i := range subs {
		subs[i] = x.Subscribe()
	}
	for i := 1; i < len(subs); i++ {
		subs[i].Unsubscribe()
	}
	if got := x.Trim(); got != 1 {
		t.Fatalf("Trim() = %d, want only the completely empty slab", got)
	}
	if got := x.Send(42); got != 1 {
		t.Fatalf("Send() = %d, want 1", got)
	}
	if got := <-subs[0].C(); got != 42 {
		t.Fatalf("remaining subscriber received %d, want 42", got)
	}
	subs[0].Unsubscribe()
	if got := x.Trim(); got != 1 {
		t.Fatalf("second Trim() = %d, want 1", got)
	}
}

func TestChanLatestStaleHandleDoesNotRemoveReusedSlot(t *testing.T) {
	var x ChanLatest[int]
	old := x.Subscribe()
	if !old.Unsubscribe() {
		t.Fatal("old subscription did not unsubscribe")
	}
	fresh := x.Subscribe() // reuses the just-freed slot
	if fresh == nil {
		t.Fatal("fresh Subscribe returned nil")
	}
	if old.Unsubscribe() {
		t.Fatal("stale handle removed the fresh subscription")
	}
	if got := x.Send(55); got != 1 {
		t.Fatalf("Send() = %d, want 1", got)
	}
	if got := <-fresh.C(); got != 55 {
		t.Fatalf("fresh subscription received %d, want 55", got)
	}
	fresh.Unsubscribe()
}

func TestChanLatestSendDoesNotAllocate(t *testing.T) {
	var x ChanLatest[uint64]
	subs := make([]*ChanLatestSubscription[uint64], 8)
	for i := range subs {
		subs[i] = x.Subscribe()
	}

	allocs := testing.AllocsPerRun(1_000, func() {
		x.Send(1)
		for _, s := range subs {
			<-s.C()
		}
	})
	if allocs != 0 {
		t.Fatalf("Send + drain allocated %v objects/run, want 0", allocs)
	}
}
