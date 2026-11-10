//go:build !race && !amd64 && !arm64

// slotread_plain.go — portable fallback slot64GLM protocol (no asm fences).
//
// Without .s fences we get ordering from atomic loads/stores on the seq
// word only; the payload copy is still plain. This is correct on any arch
// whose atomic load/store have at least acquire/release semantics (all
// Go-supported ones), because:
//
//   WRITER: seq.Add(1) atomic [odd] -> plain payload store -> seq.Add(1)
//           atomic [even]: the second Add is a release, so the payload is
//           visible to anyone who observes even.
//   READER: atomic seq load [even] (acquire) -> plain payload copy ->
//           atomic seq re-load, compare.
//
// The payload copy is a data race in the C++11 sense but NOT a usable-race
// (seqlock discipline); the Go race detector is not run on this build
// (guarded by !race), so this is the same pattern the kernel has shipped
// for decades.

package bigbuff

import (
	"unsafe"
)

// loadSlotSeqImpl64GLM — atomic load (acquire) of the seq word.
func loadSlotSeqImpl64GLM[V any](sp *slot64GLM[V]) uint64 {
	return sp.seq.Load()
}

// loadSlotValImpl64GLM — plain word copy.
func loadSlotValImpl64GLM[V any](sp *slot64GLM[V], dst *V) {
	src := unsafe.Pointer(&sp.val)
	n := int(unsafe.Sizeof(sp.val))
	words := n / wordSize64GLM
	for i := 0; i < words; i++ {
		*(*uint64)(unsafe.Add(unsafe.Pointer(dst), i*8)) = *(*uint64)(unsafe.Add(src, i*8))
	}
	if rem := n % 8; rem != 0 {
		off := words * 8
		w := *(*uint64)(unsafe.Add(src, off))
		for j := 0; j < rem; j++ {
			*(*byte)(unsafe.Add(unsafe.Pointer(dst), off+int(j))) = byte(w >> (8 * j))
		}
	}
}

// storeSlotValImpl64GLM — plain word store; ordered by the following atomic
// release store of even seq.
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

// Portable fences: atomic load/store of a scratch word.
// (defined in fence_generic.go — same build tag)
