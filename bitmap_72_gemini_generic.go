//go:build race || (!amd64 && !arm64)

package bigbuff

import (
	"runtime"
	"sync/atomic"
)

func scanActiveBanks72Gemini(dst []uint64, src []atomic.Uint64) int {
	return scanActiveBanksRef72Gemini(dst, src)
}

func findNonFullBank72Gemini(allocMasks []atomic.Uint64, startIdx int) int {
	return findNonFullBankRef72Gemini(allocMasks, startIdx)
}

// cpuPause72Gemini yields the processor (no PAUSE / YIELD hint is available
// to pure Go).
func cpuPause72Gemini() {
	runtime.Gosched()
}

// bitmapAsm72Gemini reports whether the assembly scanners are in use.
const bitmapAsm72Gemini = false
