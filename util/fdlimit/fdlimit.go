// Package fdlimit inspects and raises the process's open-file-descriptor limit.
package fdlimit

import "math"

// Headroom is the number of descriptors reserved for everything that is NOT
// bounded by the file store's semaphores: listening and peer sockets, gRPC
// connections, database pools, log files, and the Go runtime's own handles.
// The file-operation budget must fit under the OS limit with this left over,
// or the node can still hit EMFILE on a descriptor the semaphores never see.
//
// Where the whole limit is smaller than twice this, Ensure reserves half the
// limit instead, because a fixed reserve there would leave no budget at all.
const Headroom uint64 = 512

// get and set are indirected through variables so tests can substitute
// synthetic limits. The cases worth testing involve lowering the HARD limit,
// which an unprivileged process cannot undo — doing it for real would leak into
// every later test in the binary.
var (
	get = getRlimit
	set = setRlimit
)

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
				// Read the limit back rather than trusting the request. Darwin
				// accepts a setrlimit and then silently grants less, capping at
				// OPEN_MAX / kern.maxfilesperproc, so target can overstate what
				// the process actually got — and an overstated budget puts
				// EMFILE straight back on the table, which is the one thing
				// this package exists to prevent.
				effective = target

				if newSoft, _, getErr := get(); getErr == nil {
					effective = newSoft
				}

				raised = effective > soft
			}
		}
	}

	// Take the reserve out of the effective limit — but never all of it. On a host
	// whose entire limit is at or below Headroom, a fixed reserve leaves a budget
	// of zero; the caller reads zero as "no limit worth clamping to" and lets the
	// file store keep its full configured concurrency, so 1024 concurrent
	// operations run against 512 descriptors. That is no protection at all on the
	// host that can least afford it. Reserving half instead keeps a small limit
	// honest and, crucially, still clampable.
	reserve := Headroom
	if half := effective / 2; reserve > half {
		reserve = half
	}

	if effective <= reserve {
		// Only reachable at an effective limit of zero, which a running process
		// cannot really have.
		return 0, raised, nil
	}

	return effective - reserve, raised, nil
}
