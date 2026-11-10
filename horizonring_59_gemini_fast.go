// Copyright 2026 Joseph Cumines
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !race

package bigbuff

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// ringSlot59Gemini represents a single 128-byte-padded Seqlock slot.
// seq and val occupy the same primary 64-byte L1 cache line so loading seq
// automatically prefetches val into L1d in the MESI Shared (S) state.
type ringSlot59Gemini[T any] struct {
	seq     atomic.Uint64
	readers atomic.Int32 // Only used when T contains Go pointers (!ptrFree)
	_       uint32
	val     T
	_pad    [104]byte
}

// slotWrite59Gemini commits val at sequence seq into s.
// If a newer wrap-around producer (seq' > seq) has already claimed or committed this
// slot while the caller was preempted, slotWrite59Gemini elides the stale write in O(1).
//
//go:nosplit
func slotWrite59Gemini[T any](s *ringSlot59Gemini[T], seq uint64, val T, ptrFree bool) bool {
	targetEven := seq << 1
	targetOdd := targetEven | 1

	for {
		cur := s.seq.Load()
		if cur >= targetEven {
			// Stale producer elision: a newer sequence already wrote or is writing this slot.
			return false
		}
		if cur&1 != 0 {
			cpuPause59Gemini()
			continue
		}
		if s.seq.CompareAndSwap(cur, targetOdd) {
			break
		}
	}

	if ptrFree {
		// Direct unsafe copy bypasses gcWriteBarrier for pointer-free value types.
		*(*T)(unsafe.Pointer(&s.val)) = val
	} else {
		// For pointer-containing types, wait for any in-flight reader on this slot
		// to finish copying before mutating pointer words so the GC never observes
		// a torn pointer.
		for s.readers.Load() > 0 {
			cpuPause59Gemini()
		}
		s.val = val
	}

	s.seq.Store(targetEven)
	return true
}

// slotRead59Gemini optimistically reads sequence targetSeq from s.
// For pointer-free types, slotRead59Gemini executes zero atomic RMW instructions, keeping
// the slot cache line in MESI Shared (S) state across all concurrent reader cores.
//
//go:nosplit
func slotRead59Gemini[T any](s *ringSlot59Gemini[T], targetSeq uint64, ptrFree bool) (val T, observedRawSeq uint64, ok bool) {
	targetEven := targetSeq << 1
	s1 := s.seq.Load()
	if s1 != targetEven {
		return val, s1, false
	}

	if ptrFree {
		val = *(*T)(unsafe.Pointer(&s.val))
		s2 := s.seq.Load()
		if s2 != targetEven {
			var zero T
			return zero, s2, false
		}
		return val, s2, true
	}

	// Pointer-containing type path: bracket read with reader count so a wrap-around
	// writer waits before mutating pointer fields.
	s.readers.Add(1)
	sCheck := s.seq.Load()
	if sCheck != targetEven {
		s.readers.Add(-1)
		return val, sCheck, false
	}
	v := s.val
	s.readers.Add(-1)
	return v, targetEven, true
}

// cpuPause59Gemini emits a lightweight pipeline delay without entering the scheduler.
//
//go:nosplit
func cpuPause59Gemini() {
	runtime.Gosched()
}
