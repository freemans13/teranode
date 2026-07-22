package settings

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// check settings object is initialised
func TestInitialiseSettings(t *testing.T) {
	tSettings := NewSettings()

	if tSettings.ChainCfgParams == nil {
		t.Errorf("ChainCfgParams is nil")
	}

	require.NotNil(t, tSettings.Policy)
	require.NotNil(t, tSettings.BlockAssembly)
	require.NotNil(t, tSettings.SubtreeValidation)
	require.NotNil(t, tSettings.BlockChain)
	require.NotNil(t, tSettings.BlockValidation)

	require.NotNil(t, tSettings.BlockChain)
	require.NotNil(t, tSettings.BlockChain.StoreURL)

	require.NotNil(t, tSettings.UtxoStore)

	require.NotNil(t, tSettings.Block)
}

func TestGenesisActivationHeight(t *testing.T) {
	tests := []struct {
		name   string
		params *chaincfg.Params
		expect uint32
	}{
		{"RegressionNet", &chaincfg.RegressionNetParams, 10000},
		{"TestNet", &chaincfg.TestNetParams, 1344302},
		{"MainNet", &chaincfg.MainNetParams, 620538},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := NewSettings()
			tSettings.ChainCfgParams = tt.params
			require.Equal(t, tt.expect, tSettings.ChainCfgParams.GenesisActivationHeight)
		})
	}
}

func TestBlockHeightRetentionAdjustments(t *testing.T) {
	t.Run("DefaultAdjustmentValues", func(t *testing.T) {
		tSettings := NewSettings()
		tSettings.GlobalBlockHeightRetention = 100

		// Test that default adjustment values are 0
		require.Equal(t, int32(0), tSettings.UtxoStore.BlockHeightRetentionAdjustment)
		require.Equal(t, int32(0), tSettings.SubtreeValidation.BlockHeightRetentionAdjustment)

		// Test that calculated values equal global value when adjustments are 0
		require.Equal(t, uint32(100), tSettings.GetUtxoStoreBlockHeightRetention())
		require.Equal(t, uint32(100), tSettings.GetSubtreeValidationBlockHeightRetention())
	})

	t.Run("PositiveAdjustments", func(t *testing.T) {
		tSettings := NewSettings()
		tSettings.GlobalBlockHeightRetention = 100
		tSettings.UtxoStore.BlockHeightRetentionAdjustment = 50
		tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = 25

		// Test positive adjustments increase the effective values
		require.Equal(t, uint32(150), tSettings.GetUtxoStoreBlockHeightRetention())
		require.Equal(t, uint32(125), tSettings.GetSubtreeValidationBlockHeightRetention())
	})

	t.Run("NegativeAdjustments", func(t *testing.T) {
		tSettings := NewSettings()
		tSettings.GlobalBlockHeightRetention = 100
		tSettings.UtxoStore.BlockHeightRetentionAdjustment = -30
		tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = -20

		// Test negative adjustments decrease the effective values
		require.Equal(t, uint32(70), tSettings.GetUtxoStoreBlockHeightRetention())
		require.Equal(t, uint32(80), tSettings.GetSubtreeValidationBlockHeightRetention())
	})

	t.Run("BoundsChecking", func(t *testing.T) {
		tSettings := NewSettings()
		tSettings.GlobalBlockHeightRetention = 50
		tSettings.UtxoStore.BlockHeightRetentionAdjustment = -100
		tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = -75

		// Test that negative results are clamped to 0
		require.Equal(t, uint32(0), tSettings.GetUtxoStoreBlockHeightRetention())
		require.Equal(t, uint32(0), tSettings.GetSubtreeValidationBlockHeightRetention())
	})

	t.Run("LargeValues", func(t *testing.T) {
		tSettings := NewSettings()
		tSettings.GlobalBlockHeightRetention = 1000000
		tSettings.UtxoStore.BlockHeightRetentionAdjustment = 500000
		tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = -250000

		// Test with large values to ensure no overflow issues
		require.Equal(t, uint32(1500000), tSettings.GetUtxoStoreBlockHeightRetention())
		require.Equal(t, uint32(750000), tSettings.GetSubtreeValidationBlockHeightRetention())
	})

	t.Run("ZeroGlobalValue", func(t *testing.T) {
		tSettings := NewSettings()
		tSettings.GlobalBlockHeightRetention = 0
		tSettings.UtxoStore.BlockHeightRetentionAdjustment = 100
		tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = -50

		// Test behavior with zero global value
		require.Equal(t, uint32(100), tSettings.GetUtxoStoreBlockHeightRetention())
		require.Equal(t, uint32(0), tSettings.GetSubtreeValidationBlockHeightRetention())
	})
}

// Pin the runtime default for the absurd-fee user-protection ceiling
// (sendrawtransaction). Without this NewSettings wiring the field would
// stay at zero, silently disabling the check in production. Default
// matches bitcoin-sv's DEFAULT_TRANSACTION_MAXFEE = COIN/10 = 10_000_000
// satoshis (0.1 BSV).
func TestMaxRawTxFee_DefaultIsNonZero(t *testing.T) {
	tSettings := NewSettings()
	require.NotNil(t, tSettings.Policy)
	require.Equal(t, uint64(10_000_000), tSettings.Policy.MaxRawTxFee,
		"runtime default must be 10M sats so the RPC absurd-fee guard fires by default")
}

// Pin that operators can override the ceiling via the maxrawtxfee env key,
// and that the override path produces the literal value (not a clamped one).
func TestMaxRawTxFee_EnvOverride(t *testing.T) {
	t.Setenv("maxrawtxfee", "25000000")
	tSettings := NewSettings()
	require.Equal(t, uint64(25_000_000), tSettings.Policy.MaxRawTxFee)
}

// Operator opt-out: maxrawtxfee=0 disables the RPC check entirely. The
// handler shortcuts when MaxRawTxFee == 0, so this also pins that path.
func TestMaxRawTxFee_EnvZeroDisables(t *testing.T) {
	t.Setenv("maxrawtxfee", "0")
	tSettings := NewSettings()
	require.Equal(t, uint64(0), tSettings.Policy.MaxRawTxFee)
}

func TestReuseBlockMetaInMoveForwardDefaultOff(t *testing.T) {
	require.False(t, NewSettings().BlockAssembly.ReuseBlockMetaInMoveForward)
}

func TestParallelFetchPeersDefaultOne(t *testing.T) {
	require.Equal(t, 1, NewSettings().Legacy.ParallelFetchPeers)
}

func TestBlockDownloadWindowDefaults(t *testing.T) {
	s := NewSettings()
	require.Equal(t, 1024, s.Legacy.BlockDownloadWindow)
	require.Equal(t, 16, s.Legacy.MaxBlocksInTransitPerPeer)
}

// TestParallelWindowMaxParkedBlocksDefault pins the raised count cap. The byte
// budget (ParallelWindowParkedMemoryFraction) is the real memory guard; the count
// cap is a sanity ceiling. It was 1024, which tiny blocks (216 bytes on fast IBD)
// hit at sub-MB memory long before the byte budget, triggering a park-refusal
// storm; 16384 lets the byte budget bind instead.
func TestParallelWindowMaxParkedBlocksDefault(t *testing.T) {
	require.Equal(t, 16384, NewSettings().Legacy.ParallelWindowMaxParkedBlocks)
}

// TestWindowMaxBlocksDefault pins the decoupled commit-window count cap to 16
// (Wave 2 #2): a real batch below the MaxBlocksBehindBlockAssembly=20 maturity
// ceiling so the work-driven flush hands the committer a wide window (the
// postgres UNNEST batchers coalesce ~16 blocks' txs instead of one), while
// leaving runway for the next window to prefill while the current one commits.
// 0 (or >= the ceiling) falls back to the ceiling (the pre-decoupling behaviour).
func TestWindowMaxBlocksDefault(t *testing.T) {
	require.Equal(t, 16, NewSettings().Legacy.WindowMaxBlocks)
}

// TestDiskPrepareWorkersDefault pins the download-to-disk parallel prepare pool
// size to 4 — a handful of workers, far below the maturity runway. A
// height-keyed reorder buffer re-serialises their out-of-order completions back
// to strict-ascending before commit, so commit order/validation are unchanged.
// 1 = the single prepare worker (byte-identical to before Wave 2 #4).
func TestDiskPrepareWorkersDefault(t *testing.T) {
	require.Equal(t, 4, NewSettings().Legacy.DiskPrepareWorkers)
}

// TestSubtreeWriteConcurrencyDefault pins the per-block subtree write fan-out
// (Wave 2 #5) to 8 — the outer subtree-write loop fans out across this many
// independent content-addressed subtrees at once, matching writeSubtree's own
// three-way internal fan-out. First-error propagation and per-subtree
// Abort-on-failure are preserved by the errgroup. 1 = the old serial loop
// (byte-identical).
func TestSubtreeWriteConcurrencyDefault(t *testing.T) {
	require.Equal(t, 8, NewSettings().Legacy.SubtreeWriteConcurrency)
}

// TestDiskPrepareMemoryFractionDefault pins the download-to-disk prepare pool's
// byte-budget gate (Wave 2 review fix #1) to 15% of GOMEMLIMIT — a secondary,
// independent ceiling alongside DiskPrepareWorkers' count cap. 0 would disable
// the byte gate (count-cap only, the pre-fix behaviour).
func TestDiskPrepareMemoryFractionDefault(t *testing.T) {
	require.Equal(t, 0.15, NewSettings().Legacy.DiskPrepareMemoryFraction)
}

// TestSubtreeWriteMaxConcurrencyDefault pins the joint, process-wide subtree
// write concurrency ceiling (Wave 2 review fix #2) to 16 — comfortably below
// the naive DiskPrepareWorkers x SubtreeWriteConcurrency x 3 product (up to
// ~96), protecting the subtree store's connection pool / fd budget.
func TestSubtreeWriteMaxConcurrencyDefault(t *testing.T) {
	require.Equal(t, 16, NewSettings().Legacy.SubtreeWriteMaxConcurrency)
}

// Pin Phase C settings defaults: EarlyDAHBelowCheckpoint and PruneDeleteMarginBlocks
func TestUtxoStore_PhaseC_Defaults(t *testing.T) {
	tSettings := NewSettings()
	require.Equal(t, false, tSettings.UtxoStore.EarlyDAHBelowCheckpoint,
		"EarlyDAHBelowCheckpoint default must be false")
	require.Equal(t, int32(32), tSettings.UtxoStore.PruneDeleteMarginBlocks,
		"PruneDeleteMarginBlocks default must be 32")
}

func TestP2PSyncHardeningDefaultsAreLoaded(t *testing.T) {
	tSettings := NewSettings()
	require.NotNil(t, tSettings)

	require.Equal(t, uint32(10_000), tSettings.P2P.MaxUnvalidatedAdvertisedHeightLead)
	require.Equal(t, 3, tSettings.P2P.MaxUnprovenSyncProbesPerBackoffWindow)
	require.Equal(t, time.Hour, tSettings.P2P.FullStoragePenaltyDuration)
	require.Equal(t, 24*time.Hour, tSettings.P2P.FullDeliveryFreshnessWindow)
	require.Equal(t, 5*time.Minute, tSettings.P2P.SyncPeerNoProgressTimeout)
}

func TestP2PFullStoragePenaltyDuration_EnvOverride(t *testing.T) {
	t.Setenv("p2p_full_storage_penalty_duration", "2h")
	tSettings := NewSettings()
	require.NotNil(t, tSettings)
	require.Equal(t, 2*time.Hour, tSettings.P2P.FullStoragePenaltyDuration)
}

func TestP2PSyncPeerNoProgressTimeout_EnvOverride(t *testing.T) {
	t.Setenv("p2p_sync_peer_no_progress_timeout", "12m")
	tSettings := NewSettings()
	require.NotNil(t, tSettings)
	require.Equal(t, 12*time.Minute, tSettings.P2P.SyncPeerNoProgressTimeout)
}
