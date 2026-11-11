package bigbuff

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type baselineExchangeResult struct {
	got int
	err error
}

func baselineExchangeGo(x *BaselineExchanger[int], ctx context.Context, v int) <-chan baselineExchangeResult {
	out := make(chan baselineExchangeResult, 1)
	go func() {
		got, err := x.Exchange(ctx, v)
		out <- baselineExchangeResult{got, err}
	}()
	return out
}

func baselineExchangeAwait(t *testing.T, ch <-chan baselineExchangeResult, what string) baselineExchangeResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-timeAfterGuard():
		t.Fatalf("%s: Exchange did not return", what)
		return baselineExchangeResult{}
	}
}

func TestBaselineExchanger_TwoPartiesSwap(t *testing.T) {
	x := NewBaselineExchanger[int]()
	a := baselineExchangeGo(x, context.Background(), 1)
	b := baselineExchangeGo(x, context.Background(), 2)
	ra, rb := baselineExchangeAwait(t, a, "first party"), baselineExchangeAwait(t, b, "second party")
	if ra.err != nil || rb.err != nil || ra.got != 2 || rb.got != 1 {
		t.Fatalf("exchange gave (%d, %v) and (%d, %v), want (2, nil) and (1, nil)", ra.got, ra.err, rb.got, rb.err)
	}
}

// Every party gets exactly one other party's value, and that party gets its
// value back: the pairing is a perfect matching.
func TestBaselineExchanger_ManyPartiesPairOff(t *testing.T) {
	const parties = 64
	x := NewBaselineExchanger[int]()
	got := make([]int, parties)
	var wg sync.WaitGroup
	for i := range parties {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := x.Exchange(context.Background(), i)
			if err != nil {
				t.Errorf("party %d: %v", i, err)
			}
			got[i] = v
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	apexAwait(t, done, "an even number of parties pairing off")
	for i, peer := range got {
		if peer == i || peer < 0 || peer >= parties || got[peer] != i {
			t.Fatalf("party %d received %d, whose own result is %d: not a pairing", i, peer, got[min(max(peer, 0), parties-1)])
		}
	}
}

// A cancelled offer is withdrawn: its value reaches nobody, and the next two
// parties pair with each other.
func TestBaselineExchanger_CancelWithdrawsTheOffer(t *testing.T) {
	x := NewBaselineExchanger[int]()
	ctx, cancel := context.WithCancel(context.Background())
	lone := baselineExchangeGo(x, ctx, 99)
	cancel()
	if r := baselineExchangeAwait(t, lone, "cancelled party"); !errors.Is(r.err, context.Canceled) {
		// The offer may not have been made yet when cancel ran; either way
		// nobody was there to take it, so the result must be the error.
		t.Fatalf("a lone cancelled Exchange returned (%d, %v), want context.Canceled", r.got, r.err)
	}
	a := baselineExchangeGo(x, context.Background(), 1)
	b := baselineExchangeGo(x, context.Background(), 2)
	ra, rb := baselineExchangeAwait(t, a, "first party"), baselineExchangeAwait(t, b, "second party")
	if ra.got != 2 || rb.got != 1 {
		t.Fatalf("after a withdrawn offer the next pair received %d and %d, want 2 and 1 (99 must reach nobody)", ra.got, rb.got)
	}
}

// Cancellation racing a partner has exactly two legal outcomes: the exchange
// happened for both, or it happened for neither and the partner is still
// waiting. A value is never lost or delivered to one side only.
func TestBaselineExchanger_CancelRacingAPartner(t *testing.T) {
	var swapped, withdrawn int
	for round := range 2000 {
		x := NewBaselineExchanger[int]()
		ctx, cancel := context.WithCancel(context.Background())
		a := baselineExchangeGo(x, ctx, 1)
		b := baselineExchangeGo(x, context.Background(), 2)
		cancel()
		ra := baselineExchangeAwait(t, a, "cancelling party")
		if ra.err == nil {
			rb := baselineExchangeAwait(t, b, "partner of a completed exchange")
			if ra.got != 2 || rb.got != 1 || rb.err != nil {
				t.Fatalf("round %d: completed exchange gave (%d, %v) and (%d, %v)", round, ra.got, ra.err, rb.got, rb.err)
			}
			swapped++
			continue
		}
		if !errors.Is(ra.err, context.Canceled) {
			t.Fatalf("round %d: cancelling party got %v", round, ra.err)
		}
		// The partner must still be waiting with its value intact.
		rc := baselineExchangeAwait(t, baselineExchangeGo(x, context.Background(), 3), "third party")
		rb := baselineExchangeAwait(t, b, "partner after the withdrawal")
		if rc.got != 2 || rb.got != 3 || rc.err != nil || rb.err != nil {
			t.Fatalf("round %d: after a withdrawal the partner and a third party got (%d, %v) and (%d, %v), want (3, nil) and (2, nil)", round, rb.got, rb.err, rc.got, rc.err)
		}
		withdrawn++
	}
	t.Logf("%d exchanges completed, %d offers withdrawn", swapped, withdrawn)
}

func TestBaselineExchanger_Close(t *testing.T) {
	x := NewBaselineExchanger[int]()
	parked := baselineExchangeGo(x, context.Background(), 1)
	x.Close()
	x.Close() // idempotent
	r := baselineExchangeAwait(t, parked, "party parked across Close")
	if !errors.Is(r.err, ErrBaselineExchangerClosed) {
		// The party may also have arrived after Close; the error is the same.
		t.Fatalf("Exchange across Close returned (%d, %v), want ErrBaselineExchangerClosed", r.got, r.err)
	}
	if _, err := x.Exchange(context.Background(), 2); !errors.Is(err, ErrBaselineExchangerClosed) {
		t.Fatalf("Exchange after Close returned %v, want ErrBaselineExchangerClosed", err)
	}
}
