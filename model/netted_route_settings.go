package model

import (
	"sort"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// The netted route is the below-checkpoint write of a UTXO store that nets (utxoset), which
// engages when blockvalidation_outpoint_only_below_checkpoint is on. Such a store writes a block
// below the highest hardcoded checkpoint without spend-journal rows, so that block can then only
// be written through quick validation. A setting that keeps a block below the checkpoint away
// from quick validation therefore stops the sync at that block for good. The two checks below
// refuse such settings at startup instead.

// nettedRouteOn reports whether the store nets the blocks below a checkpoint of these settings.
func nettedRouteOn(tSettings *settings.Settings, store utxo.Store) bool {
	if tSettings == nil || !tSettings.BlockValidation.OutpointOnlyBelowCheckpoint {
		return false
	}

	if store == nil || !store.SupportsOutpointOnlySpend() || !utxo.NetsBelowCheckpoint(store) {
		return false
	}

	return tSettings.ChainCfgParams != nil && HighestCheckpointHeight(tSettings.ChainCfgParams.Checkpoints) > 0
}

// LargestCheckpointGap is the most blocks between two neighbouring checkpoints of the list, from
// genesis to the highest checkpoint. A catch-up window of at least this many headers holds a
// checkpoint wherever it starts below the highest one.
func LargestCheckpointGap(checkpoints []chaincfg.Checkpoint) uint32 {
	heights := make([]uint32, 0, len(checkpoints))

	for _, cp := range checkpoints {
		if cp.Height > 0 {
			heights = append(heights, uint32(cp.Height))
		}
	}

	sort.Slice(heights, func(a, b int) bool { return heights[a] < heights[b] })

	var gap, prev uint32

	for _, h := range heights {
		if h-prev > gap {
			gap = h - prev
		}

		prev = h
	}

	return gap
}

// CheckNettedRouteCatchupSettings refuses block validation settings under which native catch-up
// cannot quick validate every block below the checkpoint while the store nets there:
//
//   - blockvalidation_catchup_allow_quick_validation off sends every block to normal validation;
//   - a blockvalidation_max_accumulated_headers window smaller than the largest checkpoint gap
//     can hold no checkpoint, and a block with no checkpoint above it in the window is not quick
//     validated.
func CheckNettedRouteCatchupSettings(tSettings *settings.Settings, store utxo.Store) error {
	if !nettedRouteOn(tSettings, store) {
		return nil
	}

	bv := tSettings.BlockValidation

	if !bv.CatchupAllowQuickValidation {
		return errors.NewConfigurationError("blockvalidation_outpoint_only_below_checkpoint is on and the UTXO store nets the blocks below the checkpoint, so every such block must be quick validated, but blockvalidation_catchup_allow_quick_validation is off")
	}

	gap := LargestCheckpointGap(tSettings.ChainCfgParams.Checkpoints)
	if bv.CatchupMaxAccumulatedHeaders < 0 || uint64(bv.CatchupMaxAccumulatedHeaders) < uint64(gap) {
		return errors.NewConfigurationError("blockvalidation_outpoint_only_below_checkpoint is on and the UTXO store nets the blocks below the checkpoint, so each catch-up window must hold a checkpoint, but blockvalidation_max_accumulated_headers is %d and the largest gap between two checkpoints is %d blocks", bv.CatchupMaxAccumulatedHeaders, gap)
	}

	return nil
}

// CheckNettedRouteLegacySettings refuses legacy settings under which netsync sends blocks below
// the checkpoint to full validation while the store nets there:
// blockvalidation_legacy_unified_below_checkpoint off.
func CheckNettedRouteLegacySettings(tSettings *settings.Settings, store utxo.Store) error {
	if !nettedRouteOn(tSettings, store) {
		return nil
	}

	if !tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint {
		return errors.NewConfigurationError("blockvalidation_outpoint_only_below_checkpoint is on and the UTXO store nets the blocks below the checkpoint, so every such block must be quick validated, but blockvalidation_legacy_unified_below_checkpoint is off, which sends each legacy block to full validation")
	}

	return nil
}
