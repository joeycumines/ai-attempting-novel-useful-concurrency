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

// loadSlotValImpl64GLM — plain byte copy. The copy builtin is exact: it
// copies exactly sizeof(V) bytes with no over-read and no alignment or
// word-size assumption, which matters on 32-bit architectures where the
// slot64GLM payload is not 8-byte sized or aligned.
func loadSlotValImpl64GLM[V any](sp *slot64GLM[V], dst *V) {
	src := unsafe.Slice((*byte)(unsafe.Pointer(&sp.val)), int(unsafe.Sizeof(sp.val)))
	dstB := unsafe.Slice((*byte)(unsafe.Pointer(dst)), int(unsafe.Sizeof(*dst)))
	copy(dstB, src)
}

// storeSlotValImpl64GLM — plain byte copy; ordered by the following atomic
// release store of even seq. See loadSlotValImpl64GLM for why this is a
// byte copy and not word copies.
func storeSlotValImpl64GLM[V any](sp *slot64GLM[V], v V) {
	dstB := unsafe.Slice((*byte)(unsafe.Pointer(&sp.val)), int(unsafe.Sizeof(sp.val)))
	src := unsafe.Slice((*byte)(unsafe.Pointer(&v)), int(unsafe.Sizeof(v)))
	copy(dstB, src)
}

// Portable fences: atomic load/store of a scratch word.
// (defined in fence_generic.go — same build tag)
