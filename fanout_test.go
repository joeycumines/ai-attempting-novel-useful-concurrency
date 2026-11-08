package bigbuff

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFanoutExactDeliveryAndSelect(t *testing.T) {
	f := NewFanout[int]()
	defer f.Close()
	const count = 8
	subs := make([]*FanoutSubscription[int], count)
	for i := range subs {
		var err error
		subs[i], err = f.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		defer subs[i].Close()
	}

	var wg sync.WaitGroup
	got := make([]atomic.Int64, count)
	wg.Add(count)
	for i, sub := range subs {
		go func(i int, sub *FanoutSubscription[int]) {
			defer wg.Done()
			select {
			case v := <-sub.C():
				if v == 42 {
					got[i].Add(1)
				}
			case <-time.After(time.Second):
			}
		}(i, sub)
	}
	if n := f.Broadcast(42); n != count {
		t.Fatalf("delivered=%d want %d", n, count)
	}
	wg.Wait()
	for i := range got {
		if got[i].Load() != 1 {
			t.Fatalf("subscriber %d got %d", i, got[i].Load())
		}
	}
}

func TestFanoutBufferedAndContext(t *testing.T) {
	f := NewFanout[int]()
	defer f.Close()
	sub, err := f.SubscribeBuffered(2)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if n := f.Broadcast(1); n != 1 {
		t.Fatalf("delivered=%d", n)
	}
	if n := f.Broadcast(2); n != 1 {
		t.Fatalf("delivered=%d", n)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n, err := f.BroadcastContext(ctx, 3); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("BroadcastContext=(%d,%v)", n, err)
	}
	if a, b := <-sub.C(), <-sub.C(); a != 1 || b != 2 {
		t.Fatalf("values=(%d,%d)", a, b)
	}
}

func TestFanoutCloseCancelsBlockedSend(t *testing.T) {
	f := NewFanout[int]()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	broadcastDone := make(chan int, 1)
	go func() { close(started); broadcastDone <- f.Broadcast(7) }()
	<-started
	closeDone := make(chan struct{})
	go func() { sub.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("subscription close blocked")
	}
	select {
	case n := <-broadcastDone:
		if n != 0 {
			t.Fatalf("delivered=%d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("publisher remained blocked")
	}
	if _, ok := <-sub.C(); ok {
		t.Fatal("channel remains open")
	}
	f.Close()
}

func TestFanoutHubCloseAndChurn(t *testing.T) {
	f := NewFanout[int]()
	a, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.SubscribeBuffered(2)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	pubDone := make(chan struct{})
	go func() { close(started); f.Broadcast(1); close(pubDone) }()
	<-started
	closeDone := make(chan struct{})
	go func() { f.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("hub close blocked")
	}
	<-pubDone
	for name, sub := range map[string]*FanoutSubscription[int]{"a": a, "b": b} {
		for range sub.C() { /* drain buffered values */
		}
		select {
		case _, ok := <-sub.C():
			if ok {
				t.Fatalf("%s remained open", name)
			}
		default:
			t.Fatalf("%s channel not closed", name)
		}
		sub.Close()
	}
	if _, err := f.Subscribe(); !errors.Is(err, Err45FanoutClosed) {
		t.Fatalf("Subscribe err=%v", err)
	}
}

func TestFanoutConcurrentCloseAndChurn(t *testing.T) {
	f := NewFanout[int]()
	const workers, loops = 4, 100
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < loops; j++ {
				sub, err := f.SubscribeBuffered(2)
				if err != nil {
					t.Error(err)
					return
				}
				go func() {
					for range sub.C() {
					}
				}()
				f.Broadcast(j)
				sub.Close()
			}
		}()
	}
	wg.Wait()
	if got := f.SubscriberCount(); got != 0 {
		t.Fatalf("count=%d", got)
	}
	f.Close()
}

func TestFanoutConcurrentCloseCallsWait(t *testing.T) {
	f := NewFanout[int]()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); sub.Close() }()
	}
	wg.Wait()
	if _, ok := <-sub.C(); ok {
		t.Fatal("channel remains open")
	}
	f.Close()
}

func TestFanoutBroadcastNoAllocations(t *testing.T) {
	f := NewFanout[int]()
	defer f.Close()
	sub, err := f.SubscribeBuffered(1)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	f.Broadcast(0)
	<-sub.C()
	allocs := testing.AllocsPerRun(1000, func() {
		if f.Broadcast(1) != 1 {
			panic("wrong delivery count")
		}
		<-sub.C()
	})
	if allocs != 0 {
		t.Fatalf("allocs/op=%v", allocs)
	}
}

func TestRingFanoutOverwriteAndReplay(t *testing.T) {
	f, err := NewRingFanout[int](4)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for i := 1; i <= 10; i++ {
		if seq, err := f.Publish(i); err != nil || seq != uint64(i) {
			t.Fatalf("Publish=(%d,%v)", seq, err)
		}
	}
	v, missed, ok := sub.TryNext()
	if !ok || v != 7 || missed != 6 {
		t.Fatalf("TryNext=(%d,%d,%v)", v, missed, ok)
	}
	for want := 8; want <= 10; want++ {
		v, missed, ok = sub.TryNext()
		if !ok || v != want || missed != 0 {
			t.Fatalf("TryNext=(%d,%d,%v), want %d", v, missed, ok, want)
		}
	}
	if _, _, ok := sub.TryNext(); ok {
		t.Fatal("unexpected value")
	}

	replay, err := f.SubscribeReplay()
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	for _, want := range []int{7, 8, 9, 10} {
		v, missed, ok = replay.TryNext()
		if !ok || v != want || missed != 0 {
			t.Fatalf("replay=(%d,%d,%v) want %d", v, missed, ok, want)
		}
	}
}

func TestRingFanoutReadyAndNext(t *testing.T) {
	f, err := NewRingFanout[string](8)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if _, err := f.Publish("hello"); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-sub.Ready():
		if !ok {
			t.Fatal("ready unexpectedly closed")
		}
	case <-time.After(time.Second):
		t.Fatal("no ready notification")
	}
	v, missed, ok := sub.TryNext()
	if !ok || v != "hello" || missed != 0 {
		t.Fatalf("TryNext=(%q,%d,%v)", v, missed, ok)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, _, ok, err := sub.Next(ctx); ok || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next=(%v,%v)", ok, err)
	}
}

func TestRingFanoutConcurrentNextWaiters(t *testing.T) {
	f, err := NewRingFanout[int](32)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	const readers = 8
	var wg sync.WaitGroup
	values := make(chan int, readers)
	started := make(chan struct{}, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			v, _, ok, err := sub.Next(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			if ok {
				values <- v
			}
		}()
	}
	for i := 0; i < readers; i++ {
		<-started
	}
	for i := 0; i < readers; i++ {
		if _, err := f.Publish(i); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	close(values)
	seen := make(map[int]bool, readers)
	for v := range values {
		seen[v] = true
	}
	if len(seen) != readers {
		t.Fatalf("received %d unique values; want %d", len(seen), readers)
	}
}

func TestRingFanoutCloseWakesNext(t *testing.T) {
	f, err := NewRingFanout[int](4)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	result := make(chan bool, 1)
	go func() { close(started); _, _, ok, err := sub.Next(context.Background()); result <- ok || err != nil }()
	<-started
	sub.Close()
	select {
	case got := <-result:
		if got {
			t.Fatal("Next returned value/error")
		}
	case <-time.After(time.Second):
		t.Fatal("Next not woken")
	}
	f.Close()
}

func TestRingFanoutConcurrentPublishReadAndChurn(t *testing.T) {
	f, err := NewRingFanout[int](32)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	const total = 3000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < total; i++ {
			if _, err := f.Publish(i); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	var received, missed uint64
	for {
		_, skip, ok := sub.TryNext()
		if ok {
			received++
			missed += skip
			continue
		}
		select {
		case <-done:
			for {
				_, skip, ok = sub.TryNext()
				if !ok {
					break
				}
				received++
				missed += skip
			}
			if received+missed != total {
				t.Fatalf("received %d + missed %d != %d", received, missed, total)
			}
			return
		default:
			runtime.Gosched()
		}
	}
}

func TestRingFanoutCloseRace(t *testing.T) {
	for r := 0; r < 20; r++ {
		f, err := NewRingFanout[int](16)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					if _, err := f.Publish(j); err != nil && !errors.Is(err, ErrRingFanoutClosed) {
						t.Error(err)
						return
					}
					runtime.Gosched()
				}
			}()
		}
		for i := 0; i < 10; i++ {
			sub, err := f.Subscribe()
			if err != nil {
				break
			}
			sub.Close()
		}
		wg.Add(1)
		go func() { defer wg.Done(); f.Close() }()
		wg.Wait()
		if _, err := f.Publish(1); !errors.Is(err, ErrRingFanoutClosed) {
			t.Fatalf("Publish after close: %v", err)
		}
	}
}

func TestRingFanoutNoAllocations(t *testing.T) {
	f, err := NewRingFanout[int](16)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for i := 0; i < 64; i++ {
		if _, err := f.Subscribe(); err != nil {
			t.Fatal(err)
		}
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := f.Publish(99); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs/op=%v", allocs)
	}
}

func BenchmarkFanoutBroadcast1(b *testing.B) {
	f := NewFanout[int]()
	sub, err := f.SubscribeBuffered(1)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	defer sub.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Broadcast(i)
		<-sub.C()
	}
}
func BenchmarkFanoutBroadcast64(b *testing.B) {
	f := NewFanout[int]()
	subs := make([]*FanoutSubscription[int], 64)
	for i := range subs {
		sub, err := f.SubscribeBuffered(1)
		if err != nil {
			b.Fatal(err)
		}
		subs[i] = sub
	}
	defer f.Close()
	for _, s := range subs {
		defer s.Close()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Broadcast(i)
		for _, s := range subs {
			<-s.C()
		}
	}
}
func BenchmarkRingFanoutPublish64(b *testing.B) {
	f, err := NewRingFanout[int](256)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		if _, err := f.Subscribe(); err != nil {
			b.Fatal(err)
		}
	}
	defer f.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Publish(i); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkRingFanoutPublish1000(b *testing.B) {
	f, err := NewRingFanout[int](256)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if _, err := f.Subscribe(); err != nil {
			b.Fatal(err)
		}
	}
	defer f.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Publish(i); err != nil {
			b.Fatal(err)
		}
	}
}

func TestRingFanoutCloseReleasesPayloadReferences(t *testing.T) {
	f, err := NewRingFanout[*int](4)
	if err != nil {
		t.Fatal(err)
	}
	v := 123
	if _, err := f.Publish(&v); err != nil {
		t.Fatal(err)
	}
	f.Close()
	for i := range f.slots {
		if f.slots[i].value != nil {
			t.Fatalf("slot %d retained payload after Close", i)
		}
	}
	if f.Published() != 1 {
		t.Fatalf("Published after Close=%d; want 1", f.Published())
	}
}

func TestRingFanoutZeroValueClose(t *testing.T) {
	var f RingFanout[int]
	f.Close()
	f.Close()
	if _, err := f.Subscribe(); !errors.Is(err, ErrRingFanoutCapacity) {
		t.Fatalf("Subscribe err=%v", err)
	}
}
