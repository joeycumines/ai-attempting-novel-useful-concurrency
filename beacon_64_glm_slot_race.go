//go:build race

// slotread_race.go — the ThreadSanitizer-visible slot64GLM protocol.
//
// The fast (fence) protocol is deliberately invisible to TSan: plain
// payload loads have no happens-before edges, so TSan would flag every
// reader/writer overlap as a race even though the seqlock makes it
// impossible to USE a torn value. Under -race we switch the payload to
// word-sized atomic loads/stores, which TSan models as synchronization.
// The control logic (claim CAS, seqlock discipline, validation) is
// identical, so the race build verifies the same algorithm the fast build
// runs.

package bigbuff

import (
	"sync/atomic"
	"unsafe"
)

// loadSlotSeqImpl64GLM: seq is atomic.Uint64 — the atomic load is the
// synchronization edge TSan needs, and costs nothing extra here.
func loadSlotSeqImpl64GLM[V any](sp *slot64GLM[V]) uint64 {
	return sp.seq.Load()
}

// loadSlotValImpl64GLM copies the payload as atomic words. For word-multiple
// sizes this is a straight atomic copy; for the tail partial word we fall
// back to a single atomic word load of the overlapping bytes.
func loadSlotValImpl64GLM[V any](sp *slot64GLM[V], dst *V) {
	src := unsafe.Pointer(&sp.val)
	n := int(unsafe.Sizeof(sp.val))
	words := n / 8
	for i := 0; i < words; i++ {
		w := atomic.LoadUint64((*uint64)(unsafe.Add(src, i*8)))
		*(*uint64)(unsafe.Add(unsafe.Pointer(dst), i*8)) = w
	}
	if rem := n % 8; rem != 0 {
		off := words * 8
		// Copy the tail byte-wise via a non-atomic but WORD-ALIGNED
		// strategy is unsafe for TSan; instead do an atomic 8-byte load of
		// the word CONTAINING the tail (the slot64GLM padding guarantees the
		// word exists and belongs to this slot64GLM) and store the low bytes.
		w := atomic.LoadUint64((*uint64)(unsafe.Add(src, off)))
		for j := 0; j < rem; j++ {
			*(*byte)(unsafe.Add(unsafe.Pointer(dst), off+int(j))) = byte(w >> (8 * j))
		}
	}
}

// storeSlotValImpl64GLM writes the payload as atomic words (release edges for
// TSan; sequentially consistent for simplicity under race).
func storeSlotValImpl64GLM[V any](sp *slot64GLM[V], v V) {
	dst := unsafe.Pointer(&sp.val)
	n := int(unsafe.Sizeof(v))
	words := n / 8
	for i := 0; i < words; i++ {
		// &v is a caller-local copy: plain byte assembly, no alignment
		// requirement (64-bit atomics require 8-byte alignment, which a
		// local V is not guaranteed to have on 32-bit architectures).
		var w uint64
		for j := 0; j < 8; j++ {
			w |= uint64(*(*byte)(unsafe.Add(unsafe.Pointer(&v), i*8+int(j)))) << (8 * j)
		}
		atomic.StoreUint64((*uint64)(unsafe.Add(dst, i*8)), w)
	}
	if rem := n % 8; rem != 0 {
		off := words * 8
		var w uint64
		for j := 0; j < rem; j++ {
			w |= uint64(*(*byte)(unsafe.Add(unsafe.Pointer(&v), off+int(j)))) << (8 * j)
		}
		atomic.StoreUint64((*uint64)(unsafe.Add(dst, off)), w)
	}
}

// fenceAcquire64GLM/fenceRelease64GLM under race: full barriers via a dummy atomic —
// the atomic payload ops already carry the ordering TSan needs, but keep
// the calls uniform so control code is identical across builds.
func fenceAcquire64GLM() { _ = fenceWord64GLM.Load() }
func fenceRelease64GLM() { fenceWord64GLM.Store(1) }

var fenceWord64GLM atomic.Uint64
