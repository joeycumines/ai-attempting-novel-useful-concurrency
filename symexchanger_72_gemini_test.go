package bigbuff

// Tests written during integration. Pairing, cancellation and close are
// covered for every exchanger by TestScenarioExchange; this file covers what
// is specific to the response: tickets for select composition, arena sizing
// and the allocation claim. The response's own ticket test selected on
// Ready() of a ticket that, by construction, had already been matched inline
// and panicked; it is not ported.

import (
	"context"
	"errors"
	"testing"
)

func TestSymExchanger72Gemini_TicketPair(t *testing.T) {
	for _, slots := range []int{1, 2, 0} {
		x := NewSymExchanger72Gemini[int](slots)
		first, err := x.BeginExchange(1)
		if err != nil {
			t.Fatalf("BeginExchange: %v", err)
		}
		apexStillBlocked(t, first.Ready(), "Ready of an unmatched ticket")

		// The second ticket finds the first waiting and matches inline
		// (integration repair: it carries the partner's value itself).
		second, err := x.BeginExchange(2)
		if err != nil {
			t.Fatalf("BeginExchange: %v", err)
		}
		select {
		case <-second.Ready():
		default:
			t.Fatal("a ticket matched inside BeginExchange is not ready")
		}
		if v := second.Commit(); v != 1 {
			t.Fatalf("second ticket committed %d, want 1", v)
		}
		apexAwait(t, first.Ready(), "first ticket")
		if v := first.Commit(); v != 2 {
			t.Fatalf("first ticket committed %d, want 2", v)
		}
		x.Close()
	}
}

func TestSymExchanger72Gemini_TicketAbort(t *testing.T) {
	x := NewSymExchanger72Gemini[int](1)
	defer x.Close()

	// Unmatched: the offer is withdrawn and its slot is reusable at once.
	for range 3 {
		tk, err := x.BeginExchange(5)
		if err != nil {
			t.Fatalf("BeginExchange: %v", err)
		}
		if v, matched := tk.Abort(); matched {
			t.Fatalf("Abort of an unmatched ticket reported a match with %d", v)
		}
	}

	// Matched before the abort: Abort completes the exchange instead of
	// orphaning the partner's value.
	first, err := x.BeginExchange(1)
	if err != nil {
		t.Fatalf("BeginExchange: %v", err)
	}
	second, err := x.BeginExchange(2)
	if err != nil {
		t.Fatalf("BeginExchange: %v", err)
	}
	if v, matched := first.Abort(); !matched || v != 2 {
		t.Fatalf("Abort of a matched ticket = (%d, %v), want (2, true)", v, matched)
	}
	if v, matched := second.Abort(); !matched || v != 1 {
		t.Fatalf("Abort of an inline-matched ticket = (%d, %v), want (1, true)", v, matched)
	}
}

func TestSymExchanger72Gemini_TicketMeetsBlockingCaller(t *testing.T) {
	x := NewSymExchanger72Gemini[int](0)
	defer x.Close()
	tk, err := x.BeginExchange(10)
	if err != nil {
		t.Fatalf("BeginExchange: %v", err)
	}
	got, err := x.Exchange(context.Background(), 20) // the ticket is waiting: this cannot block
	if err != nil || got != 10 {
		t.Fatalf("Exchange = (%d, %v), want (10, nil)", got, err)
	}
	select {
	case <-tk.Ready():
		if v := tk.Commit(); v != 20 {
			t.Fatalf("ticket committed %d, want 20", v)
		}
	case <-x.Done():
		t.Fatal("exchanger reported closed")
	case <-timeAfterGuard():
		t.Fatal("ticket never became ready")
	}
}

func TestSymExchanger72Gemini_Close(t *testing.T) {
	x := NewSymExchanger72Gemini[int](0)
	tk, err := x.BeginExchange(1)
	if err != nil {
		t.Fatalf("BeginExchange: %v", err)
	}
	if err := x.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := x.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	// Close does not make Ready fire; ticket holders select on Done.
	select {
	case <-x.Done():
	default:
		t.Fatal("Done not closed")
	}
	if _, matched := tk.Abort(); matched {
		t.Fatal("Abort after Close reported a match")
	}
	if _, err := x.BeginExchange(2); !errors.Is(err, ErrClosed72Gemini) {
		t.Fatalf("BeginExchange after Close = %v, want ErrClosed72Gemini", err)
	}
	if _, err := x.Exchange(context.Background(), 2); !errors.Is(err, ErrClosed72Gemini) {
		t.Fatalf("Exchange after Close = %v, want ErrClosed72Gemini", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewSymExchanger72Gemini[int](0).Exchange(cancelled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Exchange with a done context = %v, want context.Canceled", err)
	}
}

func TestSymExchanger72Gemini_ExchangeZeroAlloc(t *testing.T) {
	x := NewSymExchanger72Gemini[int](0)
	partner := make(chan struct{})
	go func() {
		defer close(partner)
		for {
			if _, err := x.Exchange(context.Background(), 2); err != nil {
				return
			}
		}
	}()
	if n := testing.AllocsPerRun(500, func() {
		if v, err := x.Exchange(context.Background(), 1); err != nil || v != 2 {
			panic("exchange failed")
		}
	}); n != 0 {
		t.Errorf("Exchange allocated %v objects per call, want 0", n)
	}
	x.Close()
	apexAwait(t, partner, "partner")
}
