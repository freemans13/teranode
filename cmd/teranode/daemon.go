package teranode

import (
	"fmt"
	_ "net/http/pprof" //nolint:gosec // Import for pprof, only enabled via CLI flag
	"os"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/debugflags"
	"github.com/ordishs/gocore"
)

// RunDaemon starts the teranode daemon with all necessary initialization
func RunDaemon(progname, version, commit string) {
	// Initialize gocore with version info
	gocore.SetInfo(progname, version, commit)

	// Call the gocore.Log function to initialize the logger and start the Unix domain socket that allows us to configure settings at runtime.
	gocore.Log(progname)

	gocore.AddAppPayloadFn("CONFIG", func() interface{} {
		return gocore.Config().GetAll()
	})

	// Initialize settings
	tSettings := settings.NewSettings()

	// Configure GC tuning (GOMEMLIMIT + GOGC) based on cgroup memory limits.
	// Must happen early, before services allocate significant memory.
	daemon.ConfigureGCTuning(tSettings.GCTuning)

	debugflags.Init(debugflags.Flags{
		All:       tSettings.Debug.All,
		File:      tSettings.Debug.File,
		Blobstore: tSettings.Debug.Blobstore,
		UTXOStore: tSettings.Debug.UTXOStore,
	})

	readLimit := tSettings.Block.FileStoreReadConcurrency
	writeLimit := tSettings.Block.FileStoreWriteConcurrency

	// CRITICAL: Initialize file store semaphores BEFORE any file operations begin.
	// This MUST happen before daemon.Start() creates any file stores or starts any
	// services that use file stores. The InitSemaphores function replaces global
	// channel variables and is not safe to call after file operations have started.
	// See file.go for detailed documentation on the race condition risk.
	//
	// Respecting the system's open-file limit happens inside InitSemaphores, which
	// reads it directly. This used to be attempted here by shelling out to
	// "ulimit -u" and scaling the configured concurrency up to three quarters of
	// the result — but that is the max-user-processes limit, not the open-file
	// limit, so the file store was sized from an unrelated number, and scaling UP
	// is the opposite of what the setting documents ("may reduce configured
	// concurrency").
	if err := file.InitSemaphores(readLimit, writeLimit, tSettings.Block.FileStoreUseSystemLimits); err != nil {
		panic(fmt.Sprintf("Failed to initialize file store semaphores: %v", err))
	}

	// Report the limits actually in force: they may have been clamped below the
	// configured values to fit the operating system's open-file limit.
	//
	// A clamp means the HARD limit is the binding constraint, not the soft one:
	// InitSemaphores has already raised the soft limit as far as the hard limit
	// allows, so telling the operator to raise the soft limit alone would send
	// them to change a number that is already at its ceiling.
	appliedRead, appliedWrite, clamped := file.AppliedSemaphoreLimits()
	if clamped {
		fmt.Printf("File store semaphores clamped to the open-file limit: read=%d, write=%d (configured read=%d, write=%d) — raise the hard limit with ulimit -Hn or systemd LimitNOFILE to use the configured concurrency\n",
			appliedRead, appliedWrite, readLimit, writeLimit)
	} else {
		fmt.Printf("File store semaphores initialized: read=%d, write=%d\n", appliedRead, appliedWrite)
	}

	logger := ulogger.InitLogger(progname, tSettings)

	util.InitGRPCResolver(logger, tSettings.GRPCResolver)

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	daemon.New(daemon.WithLoggerFactory(func(serviceName string) ulogger.Logger {
		return ulogger.New(serviceName, ulogger.WithLevel(tSettings.LogLevel))
	})).Start(logger, os.Args[1:], tSettings)
}
