package netsync

import (
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestInstallStreamingBlockPath_NilSettingsDoesNotPanic is FIX 4.
//
// installStreamingBlockPath reads sm.settings.Legacy.PipelineReceive with no
// nil check, inside a function whose other guards (sm == nil, sm.blockPark ==
// nil) are explicitly defensive. The reviewer measured the struct offsets:
// Settings.Legacy sits at 4728 and PipelineReceive at 5112, both past the
// 4096-byte guard page a Go process has mapped unreadable at address zero, so
// a nil settings pointer here is an unrecoverable hardware fault (SIGSEGV)
// rather than a recoverable nil-pointer panic — require.Panics could not even
// catch it. Only one caller exists today and it always sets settings, so this
// is latent, not live, but the other two guards on this same line show the
// function is meant to defend itself rather than trust its caller.
func TestInstallStreamingBlockPath_NilSettingsDoesNotPanic(t *testing.T) {
	sm := &SyncManager{
		logger: ulogger.TestLogger{},
		blockPark: &blockPark{
			logger:   ulogger.TestLogger{},
			entries:  make(map[chainhash.Hash]*parkedBlock),
			children: make(map[chainhash.Hash][]chainhash.Hash),
		},
		// settings intentionally left nil
	}

	require.NotPanics(t, func() {
		sm.installStreamingBlockPath(func(
			sink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error),
			gate func(chainhash.Hash, *wire.BlockHeader) error,
			del func(chainhash.Hash) error,
			streamsEverySize bool,
		) {
			require.False(t, streamsEverySize, "nil settings must default to pipeline off, not pipeline on")
		})
	}, "a nil settings pointer must not be dereferenced past the struct's guard page")
}
