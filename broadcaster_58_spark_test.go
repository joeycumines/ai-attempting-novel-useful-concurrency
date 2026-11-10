package bigbuff

import "testing"

// A lapped cursor must not aim for the cell an in-flight Send is overwriting.
// The in-flight Send is reproduced exactly (sequence claimed, cell write lock
// held, nothing published yet), so the test is deterministic at any
// GOMAXPROCS. Before the repair Next parked on that cell's read lock here,
// and with one P a free-running sender then starved it indefinitely.
func TestBroadcaster58Spark_LappedCursorSkipsInFlightCell(t *testing.T) {
	b := NewBroadcaster58Spark[int](4)
	sub := b.Subscribe()
	for i := 1; i <= 8; i++ {
		b.Send(i)
	}

	claimed := b.next.Add(1) // 9: overwrites the cell holding 5
	cell := &b.cells[claimed&b.mask]
	cell.mu.Lock()

	type result struct {
		v      int
		ok     bool
		missed uint64
	}
	got := make(chan result, 1)
	go func() {
		v, ok, missed := sub.Next()
		got <- result{v, ok, missed}
	}()
	select {
	case r := <-got:
		if !r.ok || r.v != 6 || r.missed != 5 {
			t.Errorf("Next = (%d, %v, missed %d), want (6, true, missed 5)", r.v, r.ok, r.missed)
		}
	case <-timeAfterGuard():
		t.Error("Next blocked behind the in-flight Send")
	}

	// Complete the simulated Send, exactly as Send does.
	cell.value = 9
	cell.seq.v.Store(claimed)
	cell.mu.Unlock()
	b.published.Store(claimed)
	if t.Failed() {
		return
	}

	for want := 7; want <= 9; want++ {
		if v, ok, missed := sub.Next(); !ok || v != want || missed != 0 {
			t.Fatalf("Next = (%d, %v, missed %d), want (%d, true, missed 0)", v, ok, missed, want)
		}
	}
	if _, ok, _ := sub.Next(); ok {
		t.Fatal("Next returned a value from a caught-up cursor")
	}
}

// A single-cell ring is the degenerate case of the same repair: the only cell
// is the one being overwritten, so the cursor reports "nothing yet" rather
// than waiting, and observes the new value once it is published.
func TestBroadcaster58Spark_SingleCellInFlight(t *testing.T) {
	b := NewBroadcaster58Spark[int](1)
	sub := b.Subscribe()
	b.Send(1)
	b.Send(2)

	claimed := b.next.Add(1)
	cell := &b.cells[claimed&b.mask]
	cell.mu.Lock()
	got := make(chan bool, 1)
	go func() {
		_, ok, _ := sub.Next()
		got <- ok
	}()
	select {
	case ok := <-got:
		if ok {
			t.Error("Next returned the cell being overwritten")
		}
	case <-timeAfterGuard():
		t.Error("Next blocked behind the in-flight Send")
	}
	cell.value = 3
	cell.seq.v.Store(claimed)
	cell.mu.Unlock()
	b.published.Store(claimed)
	if t.Failed() {
		return
	}
	if v, ok, missed := sub.Next(); !ok || v != 3 || missed != 2 {
		t.Fatalf("Next = (%d, %v, missed %d), want (3, true, missed 2)", v, ok, missed)
	}
}
