//go:build !race

/*
   Copyright 2026 Joseph Cumines

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bigbuff

import (
	"runtime"
	"sync"
	"sync/atomic"
)

type horizonSlot66Gemini[V any] struct {
	stamp atomic.Uint64
	ptrMu sync.RWMutex
	val   V
	_pad  [64]byte
}

// readSlotValue66Gemini is marked noinline so Go's SSA compiler treats it as an explicit memory-token
// barrier (mem1 -> mem2 -> mem3) between the two atomic stamp loads s1 and s2, preventing
// the compiler from hoisting or sinking the payload copy across either stamp check.
//
//go:noinline
func readSlotValue66Gemini[V any](src *V) V {
	return *src
}

func (h *Horizon66Gemini[V]) writeSlot(slot *horizonSlot66Gemini[V], seq uint64, val V) {
	targetCommitted := seq << 1
	targetWriting := targetCommitted | 1

	for {
		s := slot.stamp.Load()
		if s >= targetCommitted {
			// A newer ring lap (seq + k*cap) already claimed or committed this physical slot
			// while this producer was OS-preempted. Elide stale write to preserve monotonicity.
			return
		}
		if s&1 != 0 {
			// An earlier lap on this exact physical slot is still mid-write.
			runtime.Gosched()
			continue
		}
		if slot.stamp.CompareAndSwap(s, targetWriting) {
			break
		}
	}

	if h.hasPtr {
		slot.ptrMu.Lock()
		slot.val = val
		// Swap emits XCHGQ on amd64 and SWPALD on arm64, providing a full Store-Load fence.
		slot.stamp.Swap(targetCommitted)
		slot.ptrMu.Unlock()
		return
	}

	slot.val = val
	// Swap emits XCHGQ on amd64 and SWPALD on arm64, providing a full Store-Load fence.
	slot.stamp.Swap(targetCommitted)
}

func (h *Horizon66Gemini[V]) readSlot(slot *horizonSlot66Gemini[V], seq uint64) (val V, status ReadStatus66Gemini) {
	expected := seq << 1

	if h.hasPtr {
		s1 := slot.stamp.Load()
		if s1 != expected {
			if (s1 >> 1) > seq {
				return val, StatusOverrun66Gemini
			}
			return val, StatusNotReady66Gemini
		}
		slot.ptrMu.RLock()
		s2 := slot.stamp.Load()
		if s2 != expected {
			slot.ptrMu.RUnlock()
			if (s2 >> 1) > seq {
				return val, StatusOverrun66Gemini
			}
			return val, StatusNotReady66Gemini
		}
		val = slot.val
		slot.ptrMu.RUnlock()
		return val, StatusOK66Gemini
	}

	// Lock-free pure read-only Seqlock path for pointer-free value types.
	// Keeps the slot cache line in MESI Shared (S) state across all concurrent reader cores.
	s1 := slot.stamp.Load()
	if s1 != expected {
		if (s1 >> 1) > seq {
			return val, StatusOverrun66Gemini
		}
		return val, StatusNotReady66Gemini
	}

	val = readSlotValue66Gemini(&slot.val)

	s2 := slot.stamp.Load()
	if s2 != expected {
		return val, StatusOverrun66Gemini
	}
	return val, StatusOK66Gemini
}
