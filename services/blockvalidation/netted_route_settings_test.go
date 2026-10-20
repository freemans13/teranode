package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// The shipped catch-up window holds a checkpoint wherever it starts on mainnet, so the startup
// check passes on the defaults.
func TestNettedRoute_MainnetDefaultWindowCoversTheLargestCheckpointGap(t *testing.T) {
	gap := model.LargestCheckpointGap(chaincfg.MainNetParams.Checkpoints)

	require.Positive(t, gap)
	require.LessOrEqual(t, gap, uint32(100_000), "the default blockvalidation_max_accumulated_headers")
	t.Logf("largest mainnet checkpoint gap: %d blocks", gap)
}

func TestNettedRoute_LargestCheckpointGap(t *testing.T) {
	require.Zero(t, model.LargestCheckpointGap(nil))
	require.Equal(t, uint32(600), model.LargestCheckpointGap([]chaincfg.Checkpoint{{Height: 1000}, {Height: 400}, {Height: 1500}}),
		"genesis to 400 is 400, 400 to 1,000 is 600, 1,000 to 1,500 is 500, in any list order")
}

// With the store netting below the checkpoint, block validation refuses at startup the
// settings that keep catch-up from quick validating those blocks.
func TestNettedRoute_CatchupSettingsRefusedAtStartup(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_catchup", 65536)
	defer cleanup()

	s := bv.settings
	s.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 400}, {Height: 1000}}
	s.BlockValidation.CatchupAllowQuickValidation = true
	s.BlockValidation.CatchupMaxAccumulatedHeaders = 600

	require.NoError(t, model.CheckNettedRouteCatchupSettings(s, bv.utxoStore))

	s.BlockValidation.CatchupMaxAccumulatedHeaders = 599
	err := model.CheckNettedRouteCatchupSettings(s, bv.utxoStore)
	require.True(t, errors.Is(err, errors.ErrConfiguration), "a window smaller than the largest gap, got %v", err)

	s.BlockValidation.CatchupMaxAccumulatedHeaders = 600
	s.BlockValidation.CatchupAllowQuickValidation = false
	err = model.CheckNettedRouteCatchupSettings(s, bv.utxoStore)
	require.True(t, errors.Is(err, errors.ErrConfiguration), "quick validation off, got %v", err)

	// A store that does not net, or the setting off, needs neither.
	require.NoError(t, model.CheckNettedRouteCatchupSettings(s, bv.utxoStore.(nettingRecorder).applyRecorder))

	s.BlockValidation.OutpointOnlyBelowCheckpoint = false
	require.NoError(t, model.CheckNettedRouteCatchupSettings(s, bv.utxoStore))
}

// With the store netting below the checkpoint, the legacy service refuses at startup a legacy
// route that sends those blocks to full validation.
func TestNettedRoute_LegacySettingsRefusedAtStartup(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_legacy", 65536)
	defer cleanup()

	s := bv.settings

	s.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	require.NoError(t, model.CheckNettedRouteLegacySettings(s, bv.utxoStore))

	s.BlockValidation.LegacyUnifiedBelowCheckpoint = false
	err := model.CheckNettedRouteLegacySettings(s, bv.utxoStore)
	require.True(t, errors.Is(err, errors.ErrConfiguration), "got %v", err)

	require.NoError(t, model.CheckNettedRouteLegacySettings(s, bv.utxoStore.(nettingRecorder).applyRecorder),
		"a store that does not net takes legacy blocks through full validation")
}

// runCheckpointStep runs catch-up's checkpoint step on a window from height 1 to height top.
// The checkpoints are at heights 400 and 1,000, and the headers in the window carry their hashes.
// It returns the headers the window keeps.
func runCheckpointStep(t *testing.T, bv *BlockValidation, top uint32) []*model.BlockHeader {
	t.Helper()

	headers := make([]*model.BlockHeader, top)
	for i := range headers {
		headers[i] = &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Nonce:          uint32(i), //nolint:gosec // test index
		}
	}

	checkpoints := []chaincfg.Checkpoint{{Height: 400, Hash: &chainhash.Hash{4}}, {Height: 1000, Hash: &chainhash.Hash{10}}}
	for i := range checkpoints {
		if h := uint32(checkpoints[i].Height); h <= top { //nolint:gosec // test heights
			checkpoints[i].Hash = headers[h-1].Hash()
		}
	}

	bv.settings.ChainCfgParams.Checkpoints = checkpoints
	bv.settings.BlockValidation.CatchupAllowQuickValidation = true

	u := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, blockValidation: bv}
	c := &CatchupContext{
		blockUpTo:          &model.Block{Header: headers[len(headers)-1], Height: top},
		commonAncestorMeta: &model.BlockHeaderMeta{Height: 0},
		blockHeaders:       headers,
		checkpoints:        checkpoints,
	}

	require.NoError(t, u.verifyCheckpointsInHeaderChain(c))
	require.True(t, c.useQuickValidation)

	return c.blockHeaders
}

// The window ends at its highest verified checkpoint when the blocks after it are still at or
// below the hardcoded checkpoint on a netting store. Each of those blocks would be refused in
// this window, because no verified checkpoint is at or above it.
func TestNettedRoute_WindowEndsAtTheVerifiedCheckpoint(t *testing.T) {
	bv, _, cleanup := newNetRangeHarness(t, "netted_route_trim", 65536)
	defer cleanup()

	require.Len(t, runCheckpointStep(t, bv, 700), 400, "blocks 401 to 700 wait for the window that holds checkpoint 1,000")

	// Past the hardcoded checkpoint normal validation takes the blocks: no trim.
	require.Len(t, runCheckpointStep(t, bv, 1200), 1200)

	// A store that does not net validates the blocks normally: no trim.
	bv.utxoStore = bv.utxoStore.(nettingRecorder).applyRecorder
	require.Len(t, runCheckpointStep(t, bv, 700), 700)
}
