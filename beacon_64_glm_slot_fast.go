//go:build !race && (amd64 || arm64)

// slotread_fast.go — the fence-based slot64GLM protocol for amd64/arm64.
//
// Payload copies are plain loads/stores; ordering comes from the seqlock
// discipline plus the one-instruction fences in fence_amd64.s /
// fence_arm64.s:
//
//   WRITER: seq.Add(1) [odd]  ->  plain store payload  ->  SFENCE  ->
//           seq.Add(1) [even]
//   READER: plain load seq [even]  ->  plain copy payload  ->  LFENCE  ->
//           plain load seq, compare
//
// On amd64, stores are already ordered (TSO), so the writer's fence is a
// compiler barrier; the reader's LFENCE orders the seq re-load after the
// payload copy at the instruction level. On arm64 both fences are DMB.
// This is the classic Linux kernel seqlock Reader/Writer pattern.

package bigbuff

import (
	"unsafe"
)

// loadSlotSeqImpl64GLM — plain load + compiler barrier; the atomic.Uint64
// wrapper's Load is a plain MOV on these archs anyway (aligned 8 bytes).
// We bypass the atomic wrapper to keep it a single MOV with no extra
// rounding, using the raw pointer from slotSeqRaw64GLM.
func loadSlotSeqImpl64GLM[V any](sp *slot64GLM[V]) uint64 {
	return *slotSeqRaw64GLM(sp) // plain; ordered by fenceAcquire64GLM after payload
}

// loadSlotValImpl64GLM — plain word copy. Tear-freedom is the seqlock's job:
// a tear implies an odd/even seq mismatch on re-check and the read is
// discarded; a NON-torn read is fully ordered by the acquire fence.
func loadSlotValImpl64GLM[V any](sp *slot64GLM[V], dst *V) {
	src := unsafe.Pointer(&sp.val)
	n := int(unsafe.Sizeof(sp.val))
	words := n / wordSize64GLM
	for i := 0; i < words; i++ {
		*(*uint64)(unsafe.Add(unsafe.Pointer(dst), i*8)) = *(*uint64)(unsafe.Add(src, i*8))
	}
	if rem := n % 8; rem != 0 {
		off := words * 8
		w := *(*uint64)(unsafe.Add(src, off)) // padding guarantees the word exists
		for j := 0; j < rem; j++ {
			*(*byte)(unsafe.Add(unsafe.Pointer(dst), off+int(j))) = byte(w >> (8 * j))
		}
	}
}

// storeSlotValImpl64GLM — plain word store, ordered by fenceRelease64GLM before the
// even-seq publication store.
func storeSlotValImpl64GLM[V any](sp *slot64GLM[V], v V) {
	dst := unsafe.Pointer(&sp.val)
	n := int(unsafe.Sizeof(v))
	words := n / wordSize64GLM
	for i := 0; i < words; i++ {
		*(*uint64)(unsafe.Add(dst, i*8)) = *(*uint64)(unsafe.Add(unsafe.Pointer(&v), i*8))
	}
	if rem := n % 8; rem != 0 {
		off := words * 8
		var w uint64
		for j := 0; j < rem; j++ {
			w |= uint64(*(*byte)(unsafe.Add(unsafe.Pointer(&v), off+int(j)))) << (8 * j)
		}
		*(*uint64)(unsafe.Add(dst, off)) = w
	}
}

// fences come from the .s files on these archs:
//   amd64: fenceAcquire64GLM = LFENCE;   fenceRelease64GLM = compiler barrier (TSO)
//   arm64: fenceAcquire64GLM = DMB ish;  fenceRelease64GLM = DMB ish
