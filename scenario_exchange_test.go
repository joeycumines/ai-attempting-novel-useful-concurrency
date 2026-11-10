package bigbuff

// Exchange facets: two parties meet and swap one value each. The invariants
// are conservation (a value offered is received by exactly one partner or is
// returned to its owner with an error), symmetry (if A received B's value
// then B received A's) and liveness under cancellation and close.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

type exchangeLike struct {
	exchange func(ctx context.Context, v int) (int, error)
	close    func()
	closed   error // the error a caller released by close observes
}

func exchangeLikes() map[string]func() exchangeLike {
	sym := func(slots int) func() exchangeLike {
		return func() exchangeLike {
			x := NewSymExchanger72Gemini[int](slots)
			return exchangeLike{exchange: x.Exchange, close: func() { _ = x.Close() }, closed: ErrClosed72Gemini}
		}
	}
	return map[string]func() exchangeLike{
		xSymExchanger72:  sym(0), // the response's default: a 32-slot arena
		xSymExchanger72a: sym(1), // degenerate arena: every caller contends for one slot
		xBaselineExchanger: func() exchangeLike {
			x := NewBaselineExchanger[int]()
			return exchangeLike{exchange: x.Exchange, close: x.Close, closed: ErrBaselineExchangerClosed}
		},
	}
}

type exchangeOutcome struct {
	gave, got int
	err       error
	racer     bool
}

// exchangeRound launches one caller per entry of racers, offering the values
// base, base+1, ... A racer's context is cancelled as soon as it has been
// launched, so its offer is withdrawn or matched at an arbitrary point; the
// others wait for a partner. The round is collected without timing
// assumptions: once every racer has returned, the parity of the racers'
// successes fixes how many of the patient callers must still pair up (all of
// them, or all but one), and close releases the odd one out.
func exchangeRound(t testing.TB, x exchangeLike, base int, racers []bool) []exchangeOutcome {
	t.Helper()
	racerOut := make(chan exchangeOutcome, len(racers))
	patientOut := make(chan exchangeOutcome, len(racers))
	nRacers, nPatient := 0, 0
	for i, racer := range racers {
		v := base + i
		if !racer {
			nPatient++
			go func() {
				got, err := x.exchange(context.Background(), v)
				patientOut <- exchangeOutcome{gave: v, got: got, err: err}
			}()
			continue
		}
		nRacers++
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			got, err := x.exchange(ctx, v)
			racerOut <- exchangeOutcome{gave: v, got: got, err: err, racer: true}
		}()
		cancel()
	}

	out := make([]exchangeOutcome, 0, len(racers))
	racerOK := 0
	for range nRacers {
		select {
		case o := <-racerOut:
			if o.err == nil {
				racerOK++
			} else if !errors.Is(o.err, context.Canceled) {
				t.Fatalf("cancelled exchange of %d returned %v, want a match or context.Canceled", o.gave, o.err)
			}
			out = append(out, o)
		case <-timeAfterGuard():
			t.Fatal("a cancelled exchange did not return")
		}
	}
	// Successes come in pairs, so the patient successes have the racers' parity.
	patientOK := nPatient
	if (nPatient-racerOK)%2 != 0 {
		patientOK--
	}
	for range patientOK {
		select {
		case o := <-patientOut:
			if o.err != nil {
				t.Fatalf("exchange of %d failed with %v although a partner was available", o.gave, o.err)
			}
			out = append(out, o)
		case <-timeAfterGuard():
			t.Fatal("two waiting callers were never paired")
		}
	}
	x.close()
	for range nPatient - patientOK {
		select {
		case o := <-patientOut:
			if !errors.Is(o.err, x.closed) {
				t.Fatalf("unpaired exchange of %d returned (%d, %v), want the closed error", o.gave, o.got, o.err)
			}
			out = append(out, o)
		case <-timeAfterGuard():
			t.Fatal("close did not release the unpaired caller")
		}
	}
	return out
}

// exchangeCheck asserts conservation and symmetry over a round's outcomes.
func exchangeCheck(t testing.TB, out []exchangeOutcome) {
	t.Helper()
	gotBy := map[int]int{} // gave -> got, successes only
	failed := map[int]bool{}
	for _, o := range out {
		if o.err != nil {
			failed[o.gave] = true
			continue
		}
		if o.got == o.gave {
			t.Fatalf("caller offering %d received its own value", o.gave)
		}
		gotBy[o.gave] = o.got
	}
	for gave, got := range gotBy {
		if failed[got] {
			t.Fatalf("%d was received by the caller offering %d although its owner was told the exchange failed", got, gave)
		}
		back, ok := gotBy[got]
		if !ok {
			t.Fatalf("caller offering %d received %d, which nobody successfully offered", gave, got)
		}
		if back != gave {
			t.Fatalf("asymmetric exchange: %d received %d, but %d received %d", gave, got, got, back)
		}
	}
}

func TestScenarioExchange(t *testing.T) {
	for _, a := range exchangeAttachments {
		mk := exchangeLikes()[a.Impl]
		t.Run(a.Impl, func(t *testing.T) {
			if mk == nil {
				t.Fatalf("no factory for %q", a.Impl)
			}

			// Facet 1: every caller in a round of patient callers is paired,
			// symmetrically, with no value lost or duplicated.
			t.Run("pairing", func(t *testing.T) {
				for _, parties := range []int{2, 4, 16} {
					for round := range 100 {
						x := mk()
						out := exchangeRound(t, x, round*100, make([]bool, parties))
						for _, o := range out {
							if o.err != nil {
								t.Fatalf("parties=%d: exchange of %d failed: %v", parties, o.gave, o.err)
							}
						}
						exchangeCheck(t, out)
					}
				}
			})

			// Facet 2: a lone caller returns on its context and its value is
			// never delivered to a later partner.
			t.Run("cancelled-offer-is-withdrawn", func(t *testing.T) {
				x := mk()
				defer x.close()
				ctx, cancel := context.WithCancel(context.Background())
				res := make(chan error, 1)
				go func() { _, err := x.exchange(ctx, 99); res <- err }()
				cancel()
				select {
				case err := <-res:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("lone exchange = %v, want context.Canceled", err)
					}
				case <-timeAfterGuard():
					t.Fatal("lone exchange ignored its context")
				}
				pair := make(chan exchangeOutcome, 2)
				for _, v := range []int{1, 2} {
					go func() {
						got, err := x.exchange(context.Background(), v)
						pair <- exchangeOutcome{gave: v, got: got, err: err}
					}()
				}
				for range 2 {
					select {
					case o := <-pair:
						if o.err != nil || o.got != 3-o.gave {
							t.Fatalf("exchange of %d = (%d, %v), want (%d, nil)", o.gave, o.got, o.err, 3-o.gave)
						}
					case <-timeAfterGuard():
						t.Fatal("pair after a withdrawn offer never met")
					}
				}
			})

			// Facet 3: cancellation racing the match never orphans or
			// duplicates a value, whichever side wins.
			t.Run("cancellation-race", func(t *testing.T) {
				shapes := [][]bool{
					{true, false},
					{true, true},
					{true, false, false},
					{true, true, false, false},
					{true, false, true, false, true, false, false},
					{true, true, true, true, false, false, false, false, false},
				}
				for _, racers := range shapes {
					for round := range 150 {
						exchangeCheck(t, exchangeRound(t, mk(), round*100, racers))
					}
				}
			})

			// Facet 4: close releases a parked caller and fails later ones.
			t.Run("close", func(t *testing.T) {
				x := mk()
				out := exchangeRound(t, x, 0, []bool{false}) // one patient caller: released by close
				if len(out) != 1 || !errors.Is(out[0].err, x.closed) {
					t.Fatalf("parked caller outcome %+v, want the closed error", out)
				}
				if _, err := x.exchange(context.Background(), 5); !errors.Is(err, x.closed) {
					t.Fatalf("exchange after close = %v, want the closed error", err)
				}
				x.close() // idempotent
			})
		})
	}
}

// BenchmarkScenarioExchange measures completed exchanges with a fixed number
// of parties contending for partners. One op is one caller's successful
// exchange (so a pair meeting counts as two ops).
func BenchmarkScenarioExchange(b *testing.B) {
	for _, a := range exchangeAttachments {
		mk := exchangeLikes()[a.Impl]
		for _, parties := range []int{2, 8, 64} {
			b.Run(a.Impl+"/parties="+itoa72(parties), func(b *testing.B) {
				x := mk()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var done atomic.Int64
				var wg sync.WaitGroup
				target := int64(b.N)
				b.ReportAllocs()
				b.ResetTimer()
				for p := range parties {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for done.Load() < target {
							if _, err := x.exchange(ctx, p); err != nil {
								return
							}
							if done.Add(1) >= target {
								cancel() // release callers left without a partner
							}
						}
					}()
				}
				wg.Wait()
				b.StopTimer()
				x.close()
			})
		}
	}
}

// FuzzScenarioExchange drives every exchanger with a fuzzed mix of patient
// and cancelling callers and checks conservation and symmetry.
func FuzzScenarioExchange(f *testing.F) {
	f.Add([]byte{0, 0})
	f.Add([]byte{1, 0, 0})
	f.Add([]byte{1, 1, 0, 1, 0, 0, 1})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) == 0 || len(ops) > 24 {
			return
		}
		racers := make([]bool, len(ops))
		for i, op := range ops {
			racers[i] = op&1 == 1
		}
		for _, a := range exchangeAttachments {
			exchangeCheck(t, exchangeRound(t, exchangeLikes()[a.Impl](), 1000, racers))
		}
	})
}
