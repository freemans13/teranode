package teranode

import (
	"fmt"
	_ "net/http/pprof" //nolint:gosec // Import for pprof, only enabled via CLI flag
	"os"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/file"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
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

	// CRITICAL: Initialize file store semaphores BEFORE any file operations begin.
	// This MUST happen before daemon.Start() creates any file stores or starts any
	// services that use file stores. The InitSemaphores function replaces global
	// channel variables and is not safe to call after file operations have started.
	// See file.go for detailed documentation on the race condition risk.
	if err := file.InitSemaphores(
		tSettings.Block.FileStoreReadConcurrency,
		tSettings.Block.FileStoreWriteConcurrency,
	); err != nil {
		panic(fmt.Sprintf("Failed to initialize file store semaphores: %v", err))
	}

	fmt.Printf("File store semaphores initialized: read=%d, write=%d\n",
		tSettings.Block.FileStoreReadConcurrency,
		tSettings.Block.FileStoreWriteConcurrency)

	logger := ulogger.InitLogger(progname, tSettings)

	util.InitGRPCResolver(logger, tSettings.GRPCResolver)

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	daemon.New(daemon.WithLoggerFactory(func(serviceName string) ulogger.Logger {
		return ulogger.New(serviceName, ulogger.WithLevel(tSettings.LogLevel))
	})).Start(logger, os.Args[1:], tSettings)
}
