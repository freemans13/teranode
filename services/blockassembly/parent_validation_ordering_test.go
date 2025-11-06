// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/assert"
)

// createTestTx creates a test transaction for parent validation ordering tests
func createTestTx(txID string, parentIDs ...string) *utxo.UnminedTransaction {
	// Create TxInpoints structure
	var txInpoints subtree.TxInpoints

	if len(parentIDs) > 0 {
		// Create parent transaction hashes and indices
		parentHashes := make([]chainhash.Hash, 0, len(parentIDs))
		idxs := make([][]uint32, 0, len(parentIDs))

		// Group parents by hash
		parentMap := make(map[string][]uint32)
		for _, parentID := range parentIDs {
			if _, exists := parentMap[parentID]; !exists {
				parentMap[parentID] = []uint32{}
			}
			// For simplicity, always use output index 0
			parentMap[parentID] = append(parentMap[parentID], 0)
		}

		// Build the arrays
		for parentID, indices := range parentMap {
			parentHash, _ := chainhash.NewHashFromStr(parentID)
			parentHashes = append(parentHashes, *parentHash)
			idxs = append(idxs, indices)
		}

		txInpoints.ParentTxHashes = parentHashes
		txInpoints.Idxs = idxs
	}

	// Create hash from the txID string
	hash, _ := chainhash.NewHashFromStr(txID)

	return &utxo.UnminedTransaction{
		Hash:       hash,
		TxInpoints: txInpoints,
		Fee:        1000,
		Size:       250,
	}
}

// TestParentChildOrderingLogic tests the core logic of parent-child ordering validation
// without needing the full BlockAssembler setup
func TestParentChildOrderingLogic(t *testing.T) {
	tests := []struct {
		name          string
		setupTxs      func() []*utxo.UnminedTransaction
		expectedValid []string // expected transaction IDs that should be valid
		expectedSkip  []string // expected transaction IDs that should be skipped
	}{
		{
			name: "Valid ordering - parent before child",
			setupTxs: func() []*utxo.UnminedTransaction {
				parent := createTestTx("0000000000000000000000000000000000000000000000000000000000000001")
				child := createTestTx("0000000000000000000000000000000000000000000000000000000000000002",
					"0000000000000000000000000000000000000000000000000000000000000001")
				parent.CreatedAt = 100
				child.CreatedAt = 200
				return []*utxo.UnminedTransaction{parent, child}
			},
			expectedValid: []string{
				"0000000000000000000000000000000000000000000000000000000000000001",
				"0000000000000000000000000000000000000000000000000000000000000002",
			},
			expectedSkip: []string{},
		},
		{
			name: "Invalid ordering - parent after child",
			setupTxs: func() []*utxo.UnminedTransaction {
				parent := createTestTx("0000000000000000000000000000000000000000000000000000000000000002")
				child := createTestTx("0000000000000000000000000000000000000000000000000000000000000001",
					"0000000000000000000000000000000000000000000000000000000000000002")
				child.CreatedAt = 100
				parent.CreatedAt = 200
				return []*utxo.UnminedTransaction{child, parent}
			},
			expectedValid: []string{
				"0000000000000000000000000000000000000000000000000000000000000002", // parent is valid (no dependencies)
			},
			expectedSkip: []string{
				"0000000000000000000000000000000000000000000000000000000000000001", // child should be skipped (parent after)
			},
		},
		{
			name: "Complex chain - all in order",
			setupTxs: func() []*utxo.UnminedTransaction {
				tx1 := createTestTx("0000000000000000000000000000000000000000000000000000000000000001")
				tx2 := createTestTx("0000000000000000000000000000000000000000000000000000000000000002",
					"0000000000000000000000000000000000000000000000000000000000000001")
				tx3 := createTestTx("0000000000000000000000000000000000000000000000000000000000000003",
					"0000000000000000000000000000000000000000000000000000000000000002")
				tx1.CreatedAt = 100
				tx2.CreatedAt = 200
				tx3.CreatedAt = 300
				return []*utxo.UnminedTransaction{tx1, tx2, tx3}
			},
			expectedValid: []string{
				"0000000000000000000000000000000000000000000000000000000000000001",
				"0000000000000000000000000000000000000000000000000000000000000002",
				"0000000000000000000000000000000000000000000000000000000000000003",
			},
			expectedSkip: []string{},
		},
		{
			name: "Multiple parents - all before child",
			setupTxs: func() []*utxo.UnminedTransaction {
				parent1 := createTestTx("0000000000000000000000000000000000000000000000000000000000000001")
				parent2 := createTestTx("0000000000000000000000000000000000000000000000000000000000000002")
				child := createTestTx("0000000000000000000000000000000000000000000000000000000000000003",
					"0000000000000000000000000000000000000000000000000000000000000001",
					"0000000000000000000000000000000000000000000000000000000000000002")
				parent1.CreatedAt = 100
				parent2.CreatedAt = 200
				child.CreatedAt = 300
				return []*utxo.UnminedTransaction{parent1, parent2, child}
			},
			expectedValid: []string{
				"0000000000000000000000000000000000000000000000000000000000000001",
				"0000000000000000000000000000000000000000000000000000000000000002",
				"0000000000000000000000000000000000000000000000000000000000000003",
			},
			expectedSkip: []string{},
		},
		{
			name: "Multiple parents - one after child",
			setupTxs: func() []*utxo.UnminedTransaction {
				parent1 := createTestTx("0000000000000000000000000000000000000000000000000000000000000001")
				parent2 := createTestTx("0000000000000000000000000000000000000000000000000000000000000003") // This comes after child
				child := createTestTx("0000000000000000000000000000000000000000000000000000000000000002",
					"0000000000000000000000000000000000000000000000000000000000000001",
					"0000000000000000000000000000000000000000000000000000000000000003")
				parent1.CreatedAt = 100
				child.CreatedAt = 200
				parent2.CreatedAt = 300
				return []*utxo.UnminedTransaction{parent1, child, parent2}
			},
			expectedValid: []string{
				"0000000000000000000000000000000000000000000000000000000000000001",
				"0000000000000000000000000000000000000000000000000000000000000003",
			},
			expectedSkip: []string{
				"0000000000000000000000000000000000000000000000000000000000000002", // child skipped because parent2 comes after
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			unminedTxs := tt.setupTxs()

			// Create maps for validation logic (simulating the actual implementation)
			unminedTxMap := make(map[chainhash.Hash]bool, len(unminedTxs))
			unminedTxIndexMap := make(map[chainhash.Hash]int, len(unminedTxs))
			for idx, tx := range unminedTxs {
				if tx.Hash != nil {
					unminedTxMap[*tx.Hash] = true
					unminedTxIndexMap[*tx.Hash] = idx
				}
			}

			// Perform validation logic
			validTxs := make([]*utxo.UnminedTransaction, 0)
			skippedTxs := make([]*utxo.UnminedTransaction, 0)

			for _, tx := range unminedTxs {
				// Check if transaction has unmined parents in the list
				parentHashes := tx.TxInpoints.GetParentTxHashes()
				hasInvalidOrdering := false

				if len(parentHashes) > 0 {
					currentIdx := unminedTxIndexMap[*tx.Hash]

					for _, parentTxID := range parentHashes {
						// Check if parent is in unmined list
						if unminedTxMap[parentTxID] {
							// Parent is unmined - check ordering
							parentIdx, exists := unminedTxIndexMap[parentTxID]
							if !exists || parentIdx >= currentIdx {
								// Parent comes after or at same position as child - invalid
								hasInvalidOrdering = true
								break
							}
						}
					}
				}

				if hasInvalidOrdering {
					skippedTxs = append(skippedTxs, tx)
				} else {
					validTxs = append(validTxs, tx)
				}
			}

			// Assert valid transactions
			assert.Equal(t, len(tt.expectedValid), len(validTxs), "Number of valid transactions mismatch")
			for i, expectedID := range tt.expectedValid {
				assert.Equal(t, expectedID, validTxs[i].Hash.String(), "Valid transaction at index %d mismatch", i)
			}

			// Assert skipped transactions
			assert.Equal(t, len(tt.expectedSkip), len(skippedTxs), "Number of skipped transactions mismatch")
			for i, expectedID := range tt.expectedSkip {
				assert.Equal(t, expectedID, skippedTxs[i].Hash.String(), "Skipped transaction at index %d mismatch", i)
			}
		})
	}
}
