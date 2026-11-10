//go:build !race && (amd64 || arm64)

package bigbuff

import (
	"sync/atomic"
	"unsafe"
)

//go:noescape
func asmScanAndPopcnt72Gemini(dst *uint64, src *uint64, n int) int

//go:noescape
func asmFindNonFullBank72Gemini(masks *uint64, n int, startIdx int) int

//go:noescape
func asmCPUPause72Gemini()

func asmHasPopcnt72Gemini() bool

// bitmapAsm72Gemini reports whether the assembly scanners are in use.
const bitmapAsm72Gemini = true

// hasPopcnt72Gemini is false only on amd64 processors without POPCNT.
var hasPopcnt72Gemini = asmHasPopcnt72Gemini()

// scanActiveBanks72Gemini snapshots src into dst and returns the total
// population count. The loads are plain word loads issued from assembly
// (atomic on amd64; LDAR on arm64).
func scanActiveBanks72Gemini(dst []uint64, src []atomic.Uint64) int {
	if len(src) == 0 {
		return 0
	}
	if !hasPopcnt72Gemini {
		return scanActiveBanksRef72Gemini(dst, src)
	}
	_ = dst[len(src)-1] // the assembly does not bounds-check dst
	return asmScanAndPopcnt72Gemini(&dst[0], (*uint64)(unsafe.Pointer(&src[0])), len(src))
}

// findNonFullBank72Gemini returns the index of the first mask, scanning
// circularly from startIdx, that has a clear bit, or -1.
func findNonFullBank72Gemini(allocMasks []atomic.Uint64, startIdx int) int {
	n := len(allocMasks)
	if n == 0 {
		return -1
	}
	return asmFindNonFullBank72Gemini((*uint64)(unsafe.Pointer(&allocMasks[0])), n, normalizeBankStart72Gemini(startIdx, n))
}

// cpuPause72Gemini emits the architecture's spin-wait hint.
func cpuPause72Gemini() {
	asmCPUPause72Gemini()
}
