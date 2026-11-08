package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

func waitForLease(t *testing.T, member *selectFanoutMember[int]) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for member.state.Load()&selectFanoutLeaseMask == 0 {
		if time.Now().After(deadline) {
			t.Fatal("sender did not acquire a delivery lease")
		}
		runtime.Gosched()
	}
}

func TestSelectFanoutBasic(t *testing.T) {
	f := NewSelectFanout[int]()
	defer f.Close()

	subs := make([]*SelectSubscription[int], 4)
	for i := range subs {
		var err error
		subs[i], err = f.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for i, sub := range subs {
		wg.Add(1)
		go func(i int, sub *SelectSubscription[int]) {
			defer wg.Done()
			select {
			case got, ok := <-sub.C():
				if !ok {
					t.Errorf("subscriber %d closed before receiving", i)
				} else if got != 42 {
					t.Errorf("subscriber %d got %d", i, got)
				}
			case <-sub.Done():
				t.Errorf("subscriber %d unexpectedly stopped", i)
			case <-time.After(time.Second):
				t.Errorf("subscriber %d timed out", i)
			}
		}(i, sub)
	}

	got, err := f.Send(42)
	if err != nil {
		t.Fatal(err)
	}
	if got != len(subs) {
		t.Fatalf("sent=%d want=%d", got, len(subs))
	}
	wg.Wait()
}

func TestSelectFanoutUnsubscribeCancelsBlockedSend(t *testing.T) {
	f := NewSelectFanout[int]()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = f.Send(1)
	}()

	// Wait until the sender has acquired its delivery lease.
	waitForLease(t, sub.member)
	sub.Unsubscribe()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("send did not unblock after unsubscribe")
	}
	if f.SubscriberCount() != 0 {
		t.Fatalf("count=%d", f.SubscriberCount())
	}
	if _, ok := <-sub.C(); ok {
		t.Fatal("data channel remained open after unsubscribe")
	}
	select {
	case <-sub.Done():
	default:
		t.Fatal("Done remained open after unsubscribe")
	}
	f.Close()
}

func TestSelectFanoutContextCancellation(t *testing.T) {
	f := NewSelectFanout[int]()
	defer f.Close()
	_, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	got, err := f.SendContext(ctx, 7)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	if got != 0 {
		t.Fatalf("sent=%d", got)
	}
}

func TestSelectFanoutCloseCancelsBlockedSend(t *testing.T) {
	f := NewSelectFanout[int]()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := f.Send(1)
		done <- err
	}()
	waitForLease(t, sub.member)
	f.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrSelectFanoutClosed) {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send did not unblock after close")
	}
}

func TestSelectFanoutEmptyAndClosed(t *testing.T) {
	f := NewSelectFanout[int]()
	if sent, err := f.Send(9); err != nil || sent != 0 {
		t.Fatalf("empty send = (%d, %v)", sent, err)
	}
	f.Close()
	if _, err := f.Send(1); !errors.Is(err, ErrSelectFanoutClosed) {
		t.Fatalf("send after close err=%v", err)
	}
	if _, err := f.Subscribe(); !errors.Is(err, ErrSelectFanoutClosed) {
		t.Fatalf("subscribe after close err=%v", err)
	}
}

func validateTree[T any](t *testing.T, n *selectFanoutNode[T], lo, hi uint64) (int, uint64) {
	t.Helper()
	if n == nil {
		return 0, 0
	}
	if n.key < lo || n.key >= hi {
		t.Fatalf("key %d outside [%d,%d)", n.key, lo, hi)
	}
	lh, lc := validateTree(t, n.left, lo, n.key)
	rh, rc := validateTree(t, n.right, n.key+1, hi)
	if d := lh - rh; d < -1 || d > 1 {
		t.Fatalf("unbalanced at %d: left=%d right=%d", n.key, lh, rh)
	}
	h := lh
	if rh > h {
		h = rh
	}
	h++
	if int(n.height) != h {
		t.Fatalf("bad height at %d: got=%d want=%d", n.key, n.height, h)
	}
	return h, lc + rc + 1
}

func TestSelectFanoutPersistentAVLChurn(t *testing.T) {
	f := NewSelectFanout[int]()
	defer f.Close()
	rng := rand.New(rand.NewSource(1))
	live := make([]*SelectSubscription[int], 0, 2000)

	for i := 0; i < 10000; i++ {
		if len(live) == 0 || (len(live) < 2000 && rng.Intn(2) == 0) {
			sub, err := f.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			live = append(live, sub)
		} else {
			j := rng.Intn(len(live))
			live[j].Unsubscribe()
			live[j] = live[len(live)-1]
			live = live[:len(live)-1]
		}
		snap := f.snapshot.Load()
		_, count := validateTree(t, snap.root, 0, snap.count)
		if count != snap.count {
			t.Fatalf("tree count=%d snapshot count=%d", count, snap.count)
		}
		if uint64(len(f.members)) != snap.count {
			t.Fatalf("map count=%d snapshot count=%d", len(f.members), snap.count)
		}
	}
}

func TestSelectFanoutCloseWaitsForRetiredMember(t *testing.T) {
	f := NewSelectFanout[int]()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// Hold a synthetic lease to force Unsubscribe to remain in its drain phase.
	if !sub.member.begin() {
		t.Fatal("could not acquire synthetic lease")
	}
	unsubDone := make(chan struct{})
	go func() { sub.Unsubscribe(); close(unsubDone) }()

	deadline := time.Now().Add(time.Second)
	for sub.member.state.Load()&selectFanoutActiveBit != 0 {
		if time.Now().After(deadline) {
			t.Fatal("member was not retired")
		}
		runtime.Gosched()
	}

	closeDone := make(chan struct{})
	go func() { f.Close(); close(closeDone) }()
	select {
	case <-closeDone:
		t.Fatal("Close returned before the retired member drained")
	case <-time.After(10 * time.Millisecond):
	}

	sub.member.end()
	select {
	case <-unsubDone:
	case <-time.After(time.Second):
		t.Fatal("Unsubscribe did not drain")
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not drain retired member")
	}
}

func TestSelectFanoutConcurrentChurnAndSend(t *testing.T) {
	f := NewSelectFanout[int]()
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				sub, err := f.Subscribe()
				if err != nil {
					return
				}
				go func(sub *SelectSubscription[int]) {
					select {
					case <-sub.C():
					case <-sub.Done():
					}
					sub.Unsubscribe()
				}(sub)
				sub.Unsubscribe()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			_, _ = f.SendContext(ctx, i)
			cancel()
		}
	}()
	wg.Wait()
	f.Close()
}

func TestSelectFanoutPublishAllocs(t *testing.T) {
	f := NewSelectFanout[int]()
	sub, err := f.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	go func() {
		defer close(stop)
		for {
			select {
			case _, ok := <-sub.C():
				if !ok {
					return
				}
			case <-sub.Done():
				return
			}
		}
	}()
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := f.Send(1); err != nil {
			panic(err)
		}
	})
	sub.Unsubscribe()
	<-stop
	f.Close()
	if allocs != 0 {
		t.Fatalf("steady-state Send allocations=%v, want 0", allocs)
	}
}

func BenchmarkSelectFanoutSend(b *testing.B) {
	for _, n := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("subscribers=%d", n), func(b *testing.B) {
			f := NewSelectFanout[int]()
			stop := make(chan struct{})
			for i := 0; i < n; i++ {
				sub, err := f.Subscribe()
				if err != nil {
					b.Fatal(err)
				}
				go func(sub *SelectSubscription[int]) {
					for {
						select {
						case _, ok := <-sub.C():
							if !ok {
								return
							}
						case <-sub.Done():
							return
						case <-stop:
							return
						}
					}
				}(sub)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := f.Send(i); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			close(stop)
			f.Close()
		})
	}
}
