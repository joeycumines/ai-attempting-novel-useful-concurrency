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

//go:build race

package bigbuff

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// ringSlot59Gemini under -race establishes explicit ThreadSanitizer happens-before edges
// on payload reads and writes while preserving the exact Seqlock sequence state
// transitions and zero heap allocations.
type ringSlot59Gemini[T any] struct {
	seq  atomic.Uint64
	mu   sync.RWMutex
	val  T
	_pad [96]byte
}

func slotWrite59Gemini[T any](s *ringSlot59Gemini[T], seq uint64, val T, _ bool) bool {
	targetEven := seq << 1
	targetOdd := targetEven | 1

	s.mu.Lock()
	cur := s.seq.Load()
	if cur >= targetEven {
		s.mu.Unlock()
		return false
	}
	s.seq.Store(targetOdd)
	s.val = val
	s.seq.Store(targetEven)
	s.mu.Unlock()
	return true
}

func slotRead59Gemini[T any](s *ringSlot59Gemini[T], targetSeq uint64, _ bool) (val T, observedRawSeq uint64, ok bool) {
	targetEven := targetSeq << 1
	s1 := s.seq.Load()
	if s1 != targetEven {
		return val, s1, false
	}

	s.mu.RLock()
	s2 := s.seq.Load()
	if s2 != targetEven {
		s.mu.RUnlock()
		return val, s2, false
	}
	v := s.val
	s.mu.RUnlock()
	return v, targetEven, true
}

func cpuPause59Gemini() {
	runtime.Gosched()
}
