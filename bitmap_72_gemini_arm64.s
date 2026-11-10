//go:build !race && arm64

#include "textflag.h"

// func asmScanAndPopcnt72Gemini(dst *uint64, src *uint64, n int) int
// Snapshots n 64-bit active masks from src into dst and accumulates population count
// using ARM64 NEON VCNT + UADDLV instructions.
TEXT ·asmScanAndPopcnt72Gemini(SB), NOSPLIT, $0-32
	MOVD dst+0(FP), R0
	MOVD src+8(FP), R1
	MOVD n+16(FP), R2
	MOVD $0, R3          // R3 = totalTargets accumulator
	MOVD $0, R4          // R4 = loop index i

	CMP  $0, R2
	BLE  done

loop:
	CMP  R2, R4
	BGE  done

	LSL  $3, R4, R5      // R5 = i * 8
	ADD  R1, R5, R6
	LDAR (R6), R7        // Acquire load of src[i]
	MOVD R7, (R0)(R5)    // Store into dst[i]

	// Population count via NEON VCNT
	FMOVD R7, F0
	VCNT V0.B8, V0.B8
	VUADDLV V0.B8, V0   // integration repair: Go mnemonic is VUADDLV (UADDLV does not assemble)
	FMOVD F0, R8
	ADD  R8, R3, R3

	ADD  $1, R4, R4
	B    loop

done:
	MOVD R3, ret+24(FP)
	RET

// func asmFindNonFullBank72Gemini(masks *uint64, n int, startIdx int) int
TEXT ·asmFindNonFullBank72Gemini(SB), NOSPLIT, $0-32
	MOVD masks+0(FP), R0
	MOVD n+8(FP), R1
	MOVD startIdx+16(FP), R2

	CMP  $0, R1
	BLE  not_found

	MOVD $0, R3          // R3 = visited count

scan_loop:
	CMP  R1, R3
	BGE  not_found

	LSL  $3, R2, R4
	ADD  R0, R4, R5
	LDAR (R5), R6        // Acquire load allocMasks[R2]
	MVN  R6, R6          // Invert bits: non-zero means free slot available
	CBNZ R6, found

	ADD  $1, R2, R2
	CMP  R1, R2
	BLT  next_iter
	MOVD $0, R2

next_iter:
	ADD  $1, R3, R3
	B    scan_loop

found:
	MOVD R2, ret+24(FP)
	RET

not_found:
	MOVD $-1, R0
	MOVD R0, ret+24(FP)
	RET

// func asmCPUPause72Gemini()
// Emits ARM64 YIELD hint instruction.
TEXT ·asmCPUPause72Gemini(SB), NOSPLIT, $0-0
	YIELD
	RET

// func asmHasPopcnt72Gemini() bool
// NEON (VCNT) is part of the arm64 baseline.
TEXT ·asmHasPopcnt72Gemini(SB), NOSPLIT, $0-1
	MOVD $1, R0
	MOVB R0, ret+0(FP)
	RET
