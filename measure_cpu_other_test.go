//go:build !unix

package bigbuff

import "time"

// processCPUTime is not available on this platform; see the unix build.
func processCPUTime() (d time.Duration, ok bool) { return 0, false }
