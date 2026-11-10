//go:build race

package bigbuff

import "runtime"

// storeSlot / loadSlot for -race: same two-phase seqlock with a
// mandatory slot claim as the fast build, but the val access is
// additionally serialised by the per-slot mutex in h.mus so the
// race detector sees a mutex happens-before edge for the val
// field. The mutex is per-slot, not global, so -race correctness
// tests still exercise concurrent publishers and readers on
// different slots without serialising them. The seq double-check
// is retained so both builds have identical observable semantics
// (lag / skip / torn detection).
//
// The mandatory claim (CAS before the payload write) is what keeps
// the seq word monotonically non-decreasing: a stalled writer must
// claim the slot or abort without writing, never regress the tag
// after a newer lap committed.
func (h *Horizon70Meta[V]) storeSlot(seq uint64, v V) {
	idx := seq & h.mask
	slot := &h.slots[idx]
	writing := 2*(seq+1) - 1
	committed := 2 * (seq + 1)
	for {
		s := slot.seq.Load()
		if s >= committed {
			// A newer lap owns this slot; elide the stale write.
			return
		}
		if s&1 == 1 {
			// The previous lap is mid-write; wait for it.
			runtime.Gosched()
			continue
		}
		if slot.seq.CompareAndSwap(s, writing) {
			break
		}
	}
	mu := &h.mus[idx]
	mu.Lock()
	slot.val = v
	mu.Unlock()
	slot.seq.Store(committed)
}

func (h *Horizon70Meta[V]) loadSlot(seq uint64) (V, bool) {
	var zero V
	idx := seq & h.mask
	want := 2 * (seq + 1)
	if h.slots[idx].seq.Load() != want {
		return zero, false
	}
	mu := &h.mus[idx]
	mu.Lock()
	v := h.slots[idx].val
	mu.Unlock()
	if h.slots[idx].seq.Load() != want {
		return zero, false
	}
	return v, true
}
