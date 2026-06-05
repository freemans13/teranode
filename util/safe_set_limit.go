package util

import (
	"runtime"

	"golang.org/x/sync/errgroup"
)

// SafeSetLimit sets the active-goroutine limit on an errgroup.Group, guarding
// against an invalid limit.
//
// errgroup.SetLimit(0) creates a zero-capacity semaphore, which leaves the
// group unable to ever start a goroutine — every subsequent Go call blocks
// forever. A zero (or negative) limit reaching this helper is therefore almost
// always a configuration mistake (e.g. a *Concurrency setting left at 0). These
// call sites all want a bounded-but-positive number of workers, so rather than
// panic or deadlock, SafeSetLimit falls back to a safe default of
// runtime.NumCPU() whenever the requested limit is less than 1.
//
// Parameters:
//   - g: The errgroup.Group to set the limit on
//   - limit: The maximum number of goroutines active at once; values < 1 fall
//     back to runtime.NumCPU().
func SafeSetLimit(g *errgroup.Group, limit int) {
	if limit < 1 {
		limit = runtime.NumCPU()
	}

	g.SetLimit(limit)
}
