// Package fdlimit inspects and raises the process's open-file-descriptor limit.
package fdlimit

import (
	"github.com/bsv-blockchain/teranode/errors"
)

// Headroom is the number of descriptors reserved for everything that is NOT
// bounded by the file store's semaphores: listening and peer sockets, gRPC
// connections, database pools, log files, and the Go runtime's own handles.
// The file-operation budget must fit under the OS limit with this left over,
// or the node can still hit EMFILE on a descriptor the semaphores never see.
const Headroom uint64 = 512

// Ensure makes sure the process can open at least required+Headroom files.
//
// It reads the current limit, and if the soft limit is too low it raises it —
// up to the hard limit, which an unprivileged process cannot exceed. It then
// verifies the result and returns a configuration error naming both numbers if
// the budget still does not fit, because the alternative is a node that starts
// happily and then fails file operations with "too many open files" under load
// (issue 1431).
//
// Returns the effective limit after any raise, and whether a raise happened.
func Ensure(required uint64) (effective uint64, raised bool, err error) {
	need := required + Headroom

	soft, hard, err := get()
	if err != nil {
		// Not being able to read the limit is not a reason to refuse to start;
		// the semaphores still bound concurrency. Report it and carry on.
		return 0, false, err
	}

	effective = soft

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

	if effective < need {
		return effective, raised, errors.NewConfigurationError(
			"open-file limit too low: need %d descriptors (%d for file operations + %d headroom) but the limit is %d (hard limit %d) — raise it with ulimit -n / LimitNOFILE, or lower the configured file-store concurrency",
			need, required, Headroom, effective, hard)
	}

	return effective, raised, nil
}
