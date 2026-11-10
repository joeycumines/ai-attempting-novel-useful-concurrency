//go:build !race && amd64

// fence_amd64.s — one-instruction memory fences for the Beacon64GLM seqlock.
//
//   fenceAcquire64GLM: LFENCE — orders later loads after earlier loads; the
//                 reader's seq re-load cannot be hoisted above the payload
//                 copy.
//   fenceRelease64GLM: a compiler-only barrier (amd64 TSO already orders
//                 stores); emitted as RET with a NOOP to keep symmetric
//                 call sites. We still make it a real instruction (SFENCE)
//                 for belt-and-braces compatibility with any future
//                 non-temporal store usage in storeSlotValImpl64GLM.
//
// Both are //go:nosplit leaf routines: no prologue beyond what's needed.

#include "textflag.h"

// func fenceAcquire64GLM()
TEXT ·fenceAcquire64GLM(SB), NOSPLIT|NOFRAME, $0-0
	LFENCE
	RET

// func fenceRelease64GLM()
TEXT ·fenceRelease64GLM(SB), NOSPLIT|NOFRAME, $0-0
	SFENCE
	RET

// PAUSE hint for spin loops — exposed for future adaptive spinning.
// func spinHint64GLM()
TEXT ·spinHint64GLM(SB), NOSPLIT|NOFRAME, $0-0
	PAUSE
	RET
