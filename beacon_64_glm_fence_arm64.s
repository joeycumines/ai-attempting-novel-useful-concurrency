//go:build !race && arm64

// fence_arm64.s — one-instruction memory fences for the Beacon64GLM seqlock.
//
//   fenceAcquire64GLM: DMB ISHLD — load-load/store ordering for the reader's
//                 seq re-load after the payload copy.
//   fenceRelease64GLM: DMB ISH   — full inner-shareable barrier before the
//                 even-seq publication store.
//   spinHint64GLM:     YIELD     — power- and pipeline-friendly spin hint.

#include "textflag.h"

// func fenceAcquire64GLM()
TEXT ·fenceAcquire64GLM(SB), NOSPLIT|NOFRAME, $0-0
	WORD $0xd50339bf // DMB ISHLD
	RET

// func fenceRelease64GLM()
TEXT ·fenceRelease64GLM(SB), NOSPLIT|NOFRAME, $0-0
	WORD $0xd5033bbf // DMB ISH
	RET

// func spinHint64GLM()
TEXT ·spinHint64GLM(SB), NOSPLIT|NOFRAME, $0-0
	WORD $0xd503233f // YIELD
	RET
