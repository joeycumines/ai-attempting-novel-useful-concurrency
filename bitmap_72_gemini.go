package bigbuff

// Bitmap scan helpers from the 72-gemini transcript (bitmap_asm.go,
// bitmap_generic.go, bitmap_amd64.s, bitmap_arm64.s).
//
// Integration status. The transcript presents these as the PhaseCaster
// broadcast / subscribe hot-path scanners, but its final PhaseCaster source
// never calls scanActiveBanks or findNonFullBank: the per-bank masks live
// inside the padded bank structs, not in the contiguous []atomic.Uint64 the
// helpers take, and the only code that would have wired them (a second,
// partial "bigbuff.go" listing) refers to fields that do not exist. Only
// cpuPause is used (by SymExchanger72Gemini). They are therefore integrated
// as stand-alone helpers: tested for parity against the reference loops
// below in every build, fuzzed, and benchmarked; no PhaseCaster measurement
// involves them.
//
// Integration repairs:
//
//   - As generated the assembly wrappers converted *atomic.Uint64 to *uint64
//     directly, which does not compile; they now convert through
//     unsafe.Pointer.
//   - amd64 used POPCNTQ unconditionally, an instruction outside the
//     GOAMD64=v1 baseline. The wrapper now consults CPUID once and uses the
//     reference loop when POPCNT is absent.
//   - The assembly wrote len(src) words into dst without a bounds check;
//     the wrapper now panics, like the generic build, when dst is shorter.
//   - A start index outside [0, n) was reset to 0 by the assembly wrapper
//     but reduced modulo n by the generic build; both now reduce modulo n.
//   - amd64 asmFindNonFullBank executed a CMOVQGE from a register that was
//     never written (its result was always overwritten two instructions
//     later); the instruction is removed.

import (
	"math/bits"
	"sync/atomic"
)

// scanActiveBanksRef72Gemini is the reference implementation: it snapshots
// src into dst and returns the total population count.
func scanActiveBanksRef72Gemini(dst []uint64, src []atomic.Uint64) int {
	total := 0
	for i := range src {
		m := src[i].Load()
		dst[i] = m
		total += bits.OnesCount64(m)
	}
	return total
}

// findNonFullBankRef72Gemini is the reference implementation: it returns the
// index of the first mask, scanning circularly from startIdx, that has a
// clear bit, or -1.
func findNonFullBankRef72Gemini(allocMasks []atomic.Uint64, startIdx int) int {
	n := len(allocMasks)
	if n == 0 {
		return -1
	}
	idx := normalizeBankStart72Gemini(startIdx, n)
	for visited := 0; visited < n; visited++ {
		if ^allocMasks[idx].Load() != 0 {
			return idx
		}
		idx++
		if idx >= n {
			idx = 0
		}
	}
	return -1
}

// normalizeBankStart72Gemini maps any start index into [0, n).
func normalizeBankStart72Gemini(startIdx, n int) int {
	idx := startIdx % n
	if idx < 0 {
		idx = 0
	}
	return idx
}
