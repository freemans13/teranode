//go:build unix

package fdlimit

import "syscall"

func get() (soft, hard uint64, err error) {
	var lim syscall.Rlimit
	if err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return 0, 0, err
	}

	return uint64(lim.Cur), uint64(lim.Max), nil // nolint:gosec,unconvert // rlim_t is unsigned; width varies by platform
}

func set(soft, hard uint64) error {
	lim := syscall.Rlimit{Cur: soft, Max: hard} // nolint:gosec,unconvert
	return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lim)
}
