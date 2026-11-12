//go:build !race && arm64

// fence_arm64.s — one-instruction memory fences for the Beacon64GLM seqlock.
//
//   fenceAcquire64GLM: DMB ISHLD — load-load/store ordering for the reader's
//                 seq re-load after the payload copy.
//   fenceRelease64GLM: DMB ISH   — full inner-shareable barrier before the
//                 even-seq publication store.
//   spinHint64GLM:     YIELD     — power- and pipeline-friendly spin hint.
//
// How far this file has been exercised: it assembles and vets
// (GOARCH=arm64 go vet), each routine's machine code was executed under an
// ARMv8 emulator (Unicorn 2.1.4), where each returns to its caller on two CPU
// models, and the package's test binary has run as an arm64 process under a
// user-mode emulator (qemu-aarch64 5.2.0, `make verify-arm64
// VERIFY_ARM64_EXEC=<emulator>`, the whole suite at GOMAXPROCS 1, 2 and 4;
// the gates record of a measurement run says whether that leg ran and under
// what). It has not run on arm64 hardware. Emulation shows that the
// instructions decode, execute and return inside the real program; it shows
// nothing about memory ordering on an arm64 processor, which is what a fence
// is for: qemu runs the guest's threads on an x86-64 host, whose ordering is
// stronger than arm64's, so a missing or wrong fence cannot fail there.

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
//
// Completion-phase repair: the response encoded this as WORD $0xd503233f and
// labelled it YIELD. That word is HINT #25, PACIASP (sign the link register);
// YIELD is HINT #1, 0xd503203f. The function has no caller, so the wrong
// instruction never ran. It is now written with the assembler's mnemonic.
TEXT ·spinHint64GLM(SB), NOSPLIT|NOFRAME, $0-0
	YIELD
	RET
