package daemon

import (
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/teranode/ulogger"
)

// configureMemoryLimit auto-detects the container memory limit from cgroups
// and sets GOMEMLIMIT to 90% of that value. This allows Go's GC to work
// effectively within container memory limits, preventing OOM kills.
// On local dev (no cgroup), this is a no-op.
func configureMemoryLimit(logger ulogger.Logger) {
	limit, err := readCgroupMemoryLimit()
	if err != nil || limit <= 0 {
		return
	}

	softLimit := limit * 90 / 100
	debug.SetMemoryLimit(softLimit)
	logger.Infof("GOMEMLIMIT auto-configured to %d bytes (90%% of cgroup limit %d bytes)", softLimit, limit)
}

// readCgroupMemoryLimit reads the memory limit from cgroup v2 or v1.
func readCgroupMemoryLimit() (int64, error) {
	// Try cgroup v2 first
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err == nil {
		return parseCgroupMemoryValue(string(data))
	}

	// Fall back to cgroup v1
	data, err = os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return 0, err
	}

	return parseCgroupMemoryValue(string(data))
}

// parseCgroupMemoryValue parses a cgroup memory value string.
// Returns 0 for "max" (unlimited) values.
func parseCgroupMemoryValue(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "max" || s == "" {
		return 0, nil
	}

	return strconv.ParseInt(s, 10, 64)
}
