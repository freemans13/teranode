package util

import (
	"github.com/bsv-blockchain/go-chaincfg"
)

// CalculateExpectedSupplyAtHeight calculates the total expected Bitcoin supply
// at a given block height by summing all block subsidies from genesis to that height.
// This function accounts for Bitcoin's halving schedule where the block subsidy
// reduces by half every SubsidyReductionInterval blocks (typically 210,000).
//
// Parameters:
//   - height: The target block height
//   - params: Chain configuration parameters containing halving interval
//
// Returns:
//   - uint64: Total expected satoshis in circulation at the given height
//   - The calculation includes the coinbase reward for the block at 'height'
func CalculateExpectedSupplyAtHeight(height uint32, params *chaincfg.Params) uint64 {
	if params == nil || params.SubsidyReductionInterval == 0 {
		return 0
	}

	var totalSupply uint64

	// Calculate supply for each halving period up to the target height
	currentHeight := uint32(0)
	halvingPeriod := uint32(0)

	for currentHeight <= height {
		// Get subsidy for this halving period
		subsidy := GetBlockSubsidyForHeight(currentHeight, params)
		if subsidy == 0 {
			// No more rewards, break early
			break
		}

		// Calculate how many blocks in this halving period to count
		periodEnd := (halvingPeriod+1)*params.SubsidyReductionInterval - 1

		// Don't go beyond our target height
		if periodEnd > height {
			periodEnd = height
		}

		blocksInPeriod := periodEnd - currentHeight + 1
		totalSupply += uint64(blocksInPeriod) * subsidy

		// Move to next halving period
		currentHeight = periodEnd + 1
		halvingPeriod++

		// Safety check to prevent infinite loop
		if halvingPeriod >= 64 {
			break
		}
	}

	return totalSupply
}
