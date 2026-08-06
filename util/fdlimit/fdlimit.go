// Package fdlimit inspects and raises the process's open-file-descriptor limit.
package fdlimit

import "math"

// Headroom is the number of descriptors reserved for everything that is NOT
// bounded by the file store's semaphores: listening and peer sockets, gRPC
// connections, database pools, log files, and the Go runtime's own handles.
// The file-operation budget must fit under the OS limit with this left over,
// or the node can still hit EMFILE on a descriptor the semaphores never see.
const Headroom uint64 = 512

// Ensure raises the process's open-file soft limit toward required+Headroom
// and reports what is actually usable for file operations.
//
// It deliberately does NOT fail when the limit is too small. The file store's
// semaphores are a CEILING on concurrent operations, not a reservation: a
// descriptor is held only for the duration of one operation, so a node whose
// ceiling exceeds the OS limit still runs fine at any realistic load, and
// refusing to start it would turn a bounded, already-handled condition (one
// operation waiting, then returning ServiceUnavailable) into total
// unavailability from boot (issue 1431). The caller clamps its concurrency to
// the returned budget instead.
//
// Returns the descriptors available for file operations after any raise, and
// whether a raise happened. An error means the limit could not be read at all
// (a platform without RLIMIT_NOFILE), in which case budget is 0 and the caller
// should carry on unclamped.
func Ensure(required uint64) (budget uint64, raised bool, err error) {
	need := required + Headroom
	if need < required {
		// The addition wrapped, which takes a required value within Headroom of
		// the largest representable one. Saturate rather than wrap: no limit can
		// satisfy such a budget, and wrapping would silently skip the raise
		// below instead of taking everything the hard limit allows.
		need = math.MaxUint64
	}

	soft, hard, err := get()
	if err != nil {
		return 0, false, err
	}

	effective := soft

	if soft < need {
		target := need
		if target > hard {
			target = hard
		}

		if target > soft {
			if setErr := set(target, hard); setErr == nil {
				effective = target
				raised = true
			}
		}
	}

	if effective <= Headroom {
		// Nothing left once the descriptors the semaphores do not bound are
		// accounted for.
		return 0, raised, nil
	}

	return effective - Headroom, raised, nil
}
