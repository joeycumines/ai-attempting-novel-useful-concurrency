//go:build !race && amd64

#include "textflag.h"

// func asmScanAndPopcnt72Gemini(dst *uint64, src *uint64, n int) int
// Snapshots n 64-bit active masks from src into dst, prefetches upcoming cache lines,
// and accumulates the total population count using hardware POPCNTQ.
TEXT ·asmScanAndPopcnt72Gemini(SB), NOSPLIT, $0-32
	MOVQ dst+0(FP), DI
	MOVQ src+8(FP), SI
	MOVQ n+16(FP), CX
	XORQ AX, AX          // AX = totalTargets accumulator
	XORQ DX, DX          // DX = loop index i

	CMPQ CX, $0
	JLE  done

	// Prefetch first cache line of src
	PREFETCHT0 (SI)

loop:
	CMPQ DX, CX
	JGE  done

	MOVQ (SI)(DX*8), R8  // Atomic 64-bit aligned load on amd64
	MOVQ R8, (DI)(DX*8)  // Store snapshot mask into dst[i]
	POPCNTQ R8, R9       // Count active bits in mask
	ADDQ R9, AX          // Accumulate totalTargets

	INCQ DX
	JMP  loop

done:
	MOVQ AX, ret+24(FP)
	RET

// func asmFindNonFullBank72Gemini(masks *uint64, n int, startIdx int) int
// Scans n 64-bit allocMasks starting at startIdx (wrapping around) for a bank
// where mask != ^uint64(0). Returns the bank index or -1 if all banks are full.
TEXT ·asmFindNonFullBank72Gemini(SB), NOSPLIT, $0-32
	MOVQ masks+0(FP), SI
	MOVQ n+8(FP), CX
	MOVQ startIdx+16(FP), DX

	CMPQ CX, $0
	JLE  not_found

	XORQ R8, R8          // R8 = visited count

scan_loop:
	CMPQ R8, CX
	JGE  not_found

	MOVQ (SI)(DX*8), AX  // Load allocMasks[DX]
	NOTQ AX              // Invert bits: non-zero means at least one free slot
	TESTQ AX, AX
	JNZ  found

	INCQ DX
	CMPQ DX, CX
	JL   next_iter
	XORQ DX, DX          // wrap around

next_iter:
	INCQ R8
	JMP  scan_loop

found:
	MOVQ DX, ret+24(FP)
	RET

not_found:
	MOVQ $-1, ret+24(FP)
	RET

// func asmCPUPause72Gemini()
// Emits x86 PAUSE instruction to yield pipeline resources during short spin loops.
TEXT ·asmCPUPause72Gemini(SB), NOSPLIT, $0-0
	PAUSE
	RET

// func asmHasPopcnt72Gemini() bool
// Reports CPUID.01H:ECX.POPCNT[bit 23].
TEXT ·asmHasPopcnt72Gemini(SB), NOSPLIT, $0-1
	MOVL $1, AX
	XORL CX, CX
	CPUID
	SHRL $23, CX
	ANDL $1, CX
	MOVB CX, ret+0(FP)
	RET
