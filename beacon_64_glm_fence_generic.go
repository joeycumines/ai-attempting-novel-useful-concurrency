//go:build !race && !amd64 && !arm64

package bigbuff

import "sync/atomic"

var fenceWord64GLM uint64

func fenceAcquire64GLM() { atomic.LoadUint64(&fenceWord64GLM) }
func fenceRelease64GLM() { atomic.StoreUint64(&fenceWord64GLM, 1) }
