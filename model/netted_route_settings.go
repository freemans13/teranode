package model

import (
	"sort"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// The netted route is the below-checkpoint write of a UTXO store that nets (utxoset). Such a store
// writes a block below the highest hardcoded checkpoint without spend-journal rows, so that block
// can then only be written through quick validation. The route depends on the store, the
// checkpoint table and the height alone: blockvalidation_outpoint_only_below_checkpoint,
// blockvalidation_legacy_unified_below_checkpoint and blockvalidation_catchup_allow_quick_validation
// are treated as on for such a store (model.OutpointOnlyEligible, netsync's unifiedRoute and
// catch-up's verifyCheckpointsInHeaderChain). Each service then reaches the same answer, even when
// its copy of a setting differs from another service's. The one setting that can still keep a
// block away from quick validation is refused at startup below.

// NettedRouteAt reports whether a block at height may only be written through quick validation:
// the store nets the blocks below the checkpoint and the outpoint-only gate holds at height. It is
// the one definition of the netted route; block validation's runtime refusal and the startup
// checks below both read it.
func NettedRouteAt(tSettings *settings.Settings, store utxo.Store, height uint32) bool {
	if tSettings == nil || store == nil || !utxo.NetsBelowCheckpoint(store) {
		return false
	}

	return OutpointOnlyEligible(tSettings, store, tSettings.ChainCfgParams, height)
}

// nettedRouteOn reports whether the store nets any block below a checkpoint of these settings.
func nettedRouteOn(tSettings *settings.Settings, store utxo.Store) bool {
	if tSettings == nil || tSettings.ChainCfgParams == nil {
		return false
	}

	return NettedRouteAt(tSettings, store, HighestCheckpointHeight(tSettings.ChainCfgParams.Checkpoints))
}

// LargestCheckpointGap is the most blocks between two neighbouring checkpoints of the list, from
// genesis to the highest checkpoint.
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

// NettedRouteCatchupWindow is the number of headers a catch-up window accumulates. It is
// blockvalidation_max_accumulated_headers, raised to the largest checkpoint gap plus one when the
// store nets the blocks below the checkpoint.
//
// The plus one is the common ancestor: the peer serves it as the first header and catch-up drops
// it, so a window of N headers holds at most N-1 new blocks. A window that starts at checkpoint c
// must reach c+gap to hold the next checkpoint, so it needs gap+1 headers. That count assumes the
// locator starts at the tip, so block validation does not cap it at a trailing UTXO store height
// on this route (blockvalidation's locatorCapApplies). Without a checkpoint in
// the window no block in it is quick validated, and on the netted route no block may take any
// other route.
//
// The floor is a value, not a refusal, because the default window is smaller than one testnet
// gap (100,010 blocks, from 700,000 to 800,010). 100,011 headers take about 8 MB.
func NettedRouteCatchupWindow(tSettings *settings.Settings, store utxo.Store) int {
	window := tSettings.BlockValidation.CatchupMaxAccumulatedHeaders

	if !nettedRouteOn(tSettings, store) {
		return window
	}

	if floor := int(LargestCheckpointGap(tSettings.ChainCfgParams.Checkpoints)) + 1; window < floor {
		return floor
	}

	return window
}

// CheckNettedRouteCatchupSettings refuses the block validation setting under which native
// catch-up cannot quick validate every block below the checkpoint while the store nets there:
// blockvalidation_catchup_checkpoint_hash with blockvalidation_catchup_checkpoint_height replaces
// the whole checkpoint list in catch-up with one checkpoint, so a window below that one holds no
// checkpoint, and the window floor of NettedRouteCatchupWindow, which measures the hardcoded list,
// no longer applies. It is an explicit operator choice the route cannot honour, so it is refused
// rather than ignored.
//
// The window size itself is not refused: NettedRouteCatchupWindow raises it.
func CheckNettedRouteCatchupSettings(tSettings *settings.Settings, store utxo.Store) error {
	if !nettedRouteOn(tSettings, store) {
		return nil
	}

	bv := tSettings.BlockValidation

	if bv.CatchupCheckpointHash != "" && bv.CatchupCheckpointHeight != 0 {
		return errors.NewConfigurationError("the UTXO store nets the blocks below the checkpoint, so each catch-up window must hold a hardcoded checkpoint, but blockvalidation_catchup_checkpoint_hash and blockvalidation_catchup_checkpoint_height replace the hardcoded checkpoints in catch-up")
	}

	return nil
}
