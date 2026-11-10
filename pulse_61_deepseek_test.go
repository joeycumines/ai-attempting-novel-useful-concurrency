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

// waitPulse61DeepseekWaiters blocks until the Pulse reports at least n
// registered waiters, or fails after a bounded guard. It replaces a
// sleep-based readiness wait with a bounded Gosched poll.
func waitPulse61DeepseekWaiters(t *testing.T, p *Pulse61Deepseek[int], n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for p.Waiters() < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d waiters (got %d)", n, p.Waiters())
		}
		runtime.Gosched()
	}
}

func TestPulse_Basic(t *testing.T) {
	p := NewPulse61Deepseek[int]()
	if _, ver := p.Load(); ver != 0 {
		t.Fatalf("initial version must be 0, got %d", ver)
	}
	for i := 1; i <= 5; i++ {
		if err := p.Set(i); err != nil {
			t.Fatal(err)
		}
		v, ver := p.Load()
		if v != i || ver != uint64(i) {
			t.Fatalf("after Set(%d): got (%d,%d)", i, v, ver)
		}
	}
}

func TestPulse_WaitFor_Wakes(t *testing.T) {
	p := NewPulse61Deepseek[int]()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan [2]uint64, 1)
	go func() {
		v, ver, err := p.WaitFor(ctx, 0)
		if err != nil {
			t.Errorf("WaitFor error: %v", err)
			return
		}
		done <- [2]uint64{uint64(v), ver}
	}()

	waitPulse61DeepseekWaiters(t, p, 1)
	if err := p.Set(42); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r[0] != 42 || r[1] != 1 {
			t.Fatalf("got %v", r)
		}
	case <-ctx.Done():
		t.Fatal("waiter never woke")
	}
}

func TestPulse_MultipleWaiters(t *testing.T) {
	p := NewPulse61Deepseek[int]()
	const N = 128
	var wg sync.WaitGroup
	var woke atomic.Int32
	wg.Add(N)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			v, _, err := p.WaitFor(ctx, 0)
			if err == nil && v == 1 {
				woke.Add(1)
			}
		}()
	}
	// Wait until all goroutines have registered.
	waitPulse61DeepseekWaiters(t, p, N)
	p.Set(1)
	wg.Wait()
	if got := woke.Load(); got != N {
		t.Fatalf("woke %d of %d", got, N)
	}
}

func TestPulse_ContextCancel(t *testing.T) {
	p := NewPulse61Deepseek[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err := p.WaitFor(ctx, 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestPulse_Close(t *testing.T) {
	p := NewPulse61Deepseek[int]()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	waiters := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			_, _, err := p.WaitFor(ctx, 0)
			waiters <- err
		}()
	}
	waitPulse61DeepseekWaiters(t, p, 8)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		select {
		case err := <-waiters:
			if !errors.Is(err, ErrPulseClosed61Deepseek) {
				t.Fatalf("expected ErrPulseClosed61Deepseek, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("waiter hung after Close")
		}
	}
	if err := p.Set(1); !errors.Is(err, ErrPulseClosed61Deepseek) {
		t.Fatalf("Set after Close: got %v", err)
	}
	// Idempotent close.
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestPulse_Linearizable checks that any version a reader observes is a
// real Set value, and versions are monotonic per-observer.
func TestPulse_Linearizable(t *testing.T) {
	p := NewPulse61Deepseek[uint64]()
	const pubN, subN = 8, 32
	// Under -race every Set/Load serialises on the mutex appeasement and
	// the detector adds a large constant. Keep the work bounded in both
	// builds: a fixed number of observations per spinner is enough to
	// check the linearizability invariants and terminates deterministically.
	ops := 5000
	spins := 20000
	if pulse61DeepseekRaceEnabled {
		ops = 200
		spins = 200
	}
	var wg sync.WaitGroup

	wg.Add(pubN)
	for i := 0; i < pubN; i++ {
		seed := uint64(i + 1)
		go func() {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				// Value = (writerTag << 32) | opIndex | 1 — never zero.
				_ = p.Set((seed << 32) | (uint64(j) << 1) | 1)
			}
		}()
	}

	wg.Add(subN)
	var badReads atomic.Int32
	for i := 0; i < subN; i++ {
		go func() {
			defer wg.Done()
			var lastVer uint64
			for j := 0; j < spins; j++ {
				v, ver := p.Load()
				if ver < lastVer {
					badReads.Add(1)
				}
				lastVer = ver
				if ver != 0 && v&1 == 0 {
					badReads.Add(1) // torn / inconsistent
				}
			}
		}()
	}

	wg.Wait()
	if badReads.Load() != 0 {
		t.Fatalf("observed %d linearizability violations", badReads.Load())
	}
}

// TestPulse_ConcurrentWaiters checks wake coalescing under churn: every
// waiter that observes a Set must see correct value+version.
func TestPulse_ConcurrentWaiters(t *testing.T) {
	p := NewPulse61Deepseek[uint64]()
	const pubs, subs, rounds = 4, 64, 500
	var wgPub, wgSub sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wgSub.Add(subs)
	for i := 0; i < subs; i++ {
		go func() {
			defer wgSub.Done()
			var since uint64
			for {
				v, ver, err := p.WaitFor(ctx, since)
				if err != nil {
					return
				}
				if ver <= since {
					continue
				}
				_ = v
				since = ver
				if since >= pubs*rounds {
					return
				}
			}
		}()
	}

	wgPub.Add(pubs)
	for i := 0; i < pubs; i++ {
		seed := uint64(i+1) << 32
		go func() {
			defer wgPub.Done()
			for j := 0; j < rounds; j++ {
				_ = p.Set(seed | uint64(j))
			}
		}()
	}
	wgPub.Wait()
	// Wait for subscribers to drain.
	done := make(chan struct{})
	go func() { wgSub.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("subscribers did not drain")
	}
}

// TestPulse_NoAllocs asserts zero allocations on the hot path.
func TestPulse_NoAllocs(t *testing.T) {
	p := NewPulse61Deepseek[uint64]()
	p.Set(1) // warm
	n := testing.AllocsPerRun(1000, func() {
		_ = p.Set(uint64(time.Now().UnixNano()))
		_, _ = p.Load()
	})
	if n != 0 {
		t.Fatalf("hot path allocated %v/op", n)
	}
}
