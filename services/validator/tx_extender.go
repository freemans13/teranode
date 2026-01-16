package validator

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// buildParentMap creates a lookup map from transaction hashes to transactions for a given level.
// This map is used to efficiently extend child transactions with parent output data.
//
// Parameters:
//   - parentLevelTxs: Transactions from the parent level
//
// Returns:
//   - map[chainhash.Hash]*bt.Tx: Map of transaction hash to transaction for quick lookup
func buildParentMap(parentLevelTxs []txWithIndex) map[chainhash.Hash]*bt.Tx {
	if len(parentLevelTxs) == 0 {
		return nil
	}

	parentMap := make(map[chainhash.Hash]*bt.Tx, len(parentLevelTxs))
	for _, txWithIdx := range parentLevelTxs {
		if txWithIdx.tx != nil {
			parentMap[*txWithIdx.tx.TxIDChainHash()] = txWithIdx.tx
		}
	}
	return parentMap
}

// buildParentMapFromSuccessful creates a parent map from only successfully validated transactions.
// This prevents children from extending with failed parent data that doesn't exist in UTXO store.
//
// CRITICAL: Only includes transactions present in successfulTxs map to ensure children only
// extend with parents that actually exist in the UTXO store. If a parent fails validation or
// creation, its children will correctly fail with missing parent error instead of getting
// invalid extended data.
//
// Parameters:
//   - parentLevelTxs: All transactions from the parent level
//   - successfulTxs: Map of successfully validated transaction hashes
//
// Returns:
//   - map[chainhash.Hash]*bt.Tx: Map containing only successful parent transactions
func buildParentMapFromSuccessful(parentLevelTxs []txWithIndex, successfulTxs map[chainhash.Hash]bool) map[chainhash.Hash]*bt.Tx {
	if len(parentLevelTxs) == 0 || len(successfulTxs) == 0 {
		return nil
	}

	parentMap := make(map[chainhash.Hash]*bt.Tx, len(successfulTxs))
	for _, txWithIdx := range parentLevelTxs {
		if txWithIdx.tx != nil {
			txHash := *txWithIdx.tx.TxIDChainHash()
			// Only include successfully validated transactions
			if successfulTxs[txHash] {
				parentMap[txHash] = txWithIdx.tx
			}
		}
	}
	return parentMap
}

// buildParentMetadata creates a map of parent transaction block heights for use by the validator.
// This allows the validator to skip UTXO store lookups for in-block parents.
//
// CRITICAL: Only includes transactions that successfully validated (present in successfulTxs).
// This prevents validation bypass where child references a failed parent transaction.
//
// The block height (where the parent will be mined) is needed for coinbase maturity checks
// and other validation rules.
//
// Parameters:
//   - parentLevelTxs: Transactions from the parent level
//   - blockHeight: Block height where these transactions will be mined
//   - successfulTxs: Map of successfully validated transaction hashes
//
// Returns:
//   - map[chainhash.Hash]uint32: Block height map for successful parent transactions
func buildParentMetadata(parentLevelTxs []txWithIndex, blockHeight uint32, successfulTxs map[chainhash.Hash]bool) map[chainhash.Hash]uint32 {
	if len(parentLevelTxs) == 0 || len(successfulTxs) == 0 {
		return nil
	}

	blockHeights := make(map[chainhash.Hash]uint32, len(successfulTxs))
	for _, txWithIdx := range parentLevelTxs {
		if txWithIdx.tx != nil {
			txHash := *txWithIdx.tx.TxIDChainHash()
			// Only include transactions that successfully validated
			if successfulTxs[txHash] {
				blockHeights[txHash] = blockHeight
			}
		}
	}
	return blockHeights
}

// extendTxWithParentMap extends a transaction's inputs with parent output data
// from a pre-built parent map, avoiding UTXO store fetches for intra-block dependencies.
// This is a critical optimization that eliminates ~500MB+ of UTXO store fetches per block.
//
// The function only marks the transaction as extended if ALL inputs are successfully extended.
// This ensures that the validator can rely on IsExtended() to determine if all input data is populated.
//
// Parameters:
//   - tx: Transaction to extend
//   - parentMap: Map of parent transaction hashes to parent transactions
//
// Returns:
//   - int: Number of inputs that were successfully extended
func extendTxWithParentMap(tx *bt.Tx, parentMap map[chainhash.Hash]*bt.Tx) int {
	if tx == nil || len(parentMap) == 0 {
		return 0
	}

	// Skip if already extended
	if tx.IsExtended() {
		return 0
	}

	extendedCount := 0
	allInputsExtended := true

	for _, input := range tx.Inputs {
		parentHash := input.PreviousTxIDChainHash()
		if parentHash == nil {
			continue // Input doesn't need extension
		}

		// Try to extend this input
		parentTx, found := parentMap[*parentHash]
		if !found || int(input.PreviousTxOutIndex) >= len(parentTx.Outputs) {
			allInputsExtended = false
			continue
		}

		// Extend this input with parent output data
		output := parentTx.Outputs[input.PreviousTxOutIndex]
		input.PreviousTxSatoshis = output.Satoshis
		input.PreviousTxScript = output.LockingScript
		extendedCount++
	}

	// Only mark as fully extended if we successfully extended all inputs
	// This ensures that downstream code can rely on IsExtended() for completeness
	if allInputsExtended && extendedCount > 0 {
		tx.SetExtended(true)
	}

	return extendedCount
}
