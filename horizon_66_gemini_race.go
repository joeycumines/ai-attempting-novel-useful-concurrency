//go:build race

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
	mu    sync.RWMutex
	val   V
	_pad  [64]byte
}

func (h *Horizon66Gemini[V]) writeSlot(slot *horizonSlot66Gemini[V], seq uint64, val V) {
	targetCommitted := seq << 1
	targetWriting := targetCommitted | 1

	for {
		s := slot.stamp.Load()
		if s >= targetCommitted {
			return
		}
		if s&1 != 0 {
			runtime.Gosched()
			continue
		}
		if slot.stamp.CompareAndSwap(s, targetWriting) {
			break
		}
	}

	slot.mu.Lock()
	slot.val = val
	slot.stamp.Swap(targetCommitted)
	slot.mu.Unlock()
}

func (h *Horizon66Gemini[V]) readSlot(slot *horizonSlot66Gemini[V], seq uint64) (val V, status ReadStatus66Gemini) {
	expected := seq << 1
	s1 := slot.stamp.Load()
	if s1 != expected {
		if (s1 >> 1) > seq {
			return val, StatusOverrun66Gemini
		}
		return val, StatusNotReady66Gemini
	}

	slot.mu.RLock()
	s2 := slot.stamp.Load()
	if s2 != expected {
		slot.mu.RUnlock()
		if (s2 >> 1) > seq {
			return val, StatusOverrun66Gemini
		}
		return val, StatusNotReady66Gemini
	}
	val = slot.val
	slot.mu.RUnlock()
	return val, StatusOK66Gemini
}
