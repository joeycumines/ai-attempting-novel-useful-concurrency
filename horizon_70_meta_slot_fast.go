//go:build !race

package bigbuff

import "runtime"

// storeSlot publishes v at seq.
//
// This is a two-phase seqlock with a mandatory slot claim. The
// publication point is the final seq Store, but a stalled writer
// may never store it: before writing val it must claim the slot
// with a CAS from ANY state older than its own writing marker.
// If a newer lap already claimed or committed the slot, the stale
// writer returns WITHOUT writing; its message is simply lost (the
// ring is lossy). This is what keeps the seq word monotonically
// non-decreasing — without it, a stalled writer could regress the
// tag after a newer lap committed, and a reader could accept a
// torn (payload, tag) pair as a valid older message.
//
// Tag encoding, per slot: 0 is the committed state of lap -1
// (construction); 2*(k+1)-1 is "lap k is being written"; 2*(k+1)
// is "lap k is committed". Readers validate against 2*(k+1).
//
// The reader's second seq Load detects a concurrent overwrite that
// tore the val read.
func (h *Horizon70Meta[V]) storeSlot(seq uint64, v V) {
	slot := &h.slots[seq&h.mask]
	writing := 2*(seq+1) - 1
	committed := 2 * (seq + 1)
	for {
		s := slot.seq.Load()
		if s >= committed {
			// A newer lap owns this slot; eliding the stale write
			// is what preserves seq monotonicity.
			return
		}
		if s&1 == 1 {
			// The previous lap is mid-write; wait for it rather
			// than overwrite its payload.
			runtime.Gosched()
			continue
		}
		if slot.seq.CompareAndSwap(s, writing) {
			break
		}
	}
	slot.val = v
	slot.seq.Store(committed)
}

// loadSlot tries to read seq. It returns false on any mismatch,
// including a seq change between the two Loads (torn / overwritten
// read) and including a slot whose lap k is claimed but not yet
// committed (odd tag).
func (h *Horizon70Meta[V]) loadSlot(seq uint64) (V, bool) {
	var zero V
	slot := &h.slots[seq&h.mask]
	want := 2 * (seq + 1)
	if slot.seq.Load() != want {
		return zero, false
	}
	v := slot.val
	if slot.seq.Load() != want {
		return zero, false
	}
	return v, true
}
