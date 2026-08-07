//go:build unix

package fdlimit

import "syscall"

// The kernel's rlim_t is unsigned on Linux, Darwin, Solaris, NetBSD, OpenBSD,
// AIX and illumos, but signed on FreeBSD and DragonFly, and Go's syscall.Rlimit
// mirrors that. The rest of the package works in uint64, so both conversions are
// confined to this file: reading widens either kind to uint64, and writing goes
// through a type parameter that adopts the field's own type. Assigning uint64s
// straight into a syscall.Rlimit literal does not compile on the signed
// platforms, which the unix build tag also covers.

func getRlimit() (soft, hard uint64, err error) {
	var lim syscall.Rlimit
	if err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return 0, 0, err
	}

	//nolint:gosec,unconvert // rlim_t signedness varies by platform; see above
	return uint64(lim.Cur), uint64(lim.Max), nil
}

func setRlimit(soft, hard uint64) error {
	var lim syscall.Rlimit

	assignRlim(&lim.Cur, soft)
	assignRlim(&lim.Max, hard)

	return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lim)
}

// assignRlim writes v into an rlim_t field whatever its signedness. An unlimited
// value round-trips unchanged, because it is the platform's own maximum that get
// read out in the first place.
func assignRlim[T int64 | uint64](field *T, v uint64) {
	*field = T(v) //nolint:gosec // rlim_t signedness varies by platform; see above
}
