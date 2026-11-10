//go:build !race && !amd64 && !arm64

package bigbuff

import "sync/atomic"

var fenceWord64GLM atomic.Uint64

func fenceAcquire64GLM() { _ = fenceWord64GLM.Load() }
func fenceRelease64GLM() { fenceWord64GLM.Store(1) }
