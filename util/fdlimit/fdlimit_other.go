//go:build !unix

package fdlimit

import "github.com/bsv-blockchain/teranode/errors"

// Platforms without RLIMIT_NOFILE (Windows) have no descriptor limit to read
// or raise, so Ensure reports the limit as unknown and the caller carries on.
func get() (soft, hard uint64, err error) {
	return 0, 0, errors.NewProcessingError("open-file limit is not available on this platform")
}

func set(soft, hard uint64) error {
	return errors.NewProcessingError("open-file limit cannot be set on this platform")
}
