// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidateParentChain_BatchingAndOrdering tests that the validateParentChain
// function correctly handles transaction ordering validation across batch boundaries.
// This test specifically validates the fix for the variable shadowing bug where the loop variable 'i'
// was being shadowed, causing incorrect currentIdx calculations in ordering validation.
func TestValidateParentChain_BatchingAndOrdering(t *testing.T) {
	ctx := context.Background()

	t.Run("Valid ordering across batches - bug regression test", func(t *testing.T) {
		// Setup mock UTXO store
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}

		// Create BlockAssembler with test settings
		testSettings := &settings.Settings{}
		testSettings.BlockAssembly.ParentValidationBatchSize = 50 // Set batch size to trigger batching

		blockAssembler := &BlockAssembler{
			utxoStore: mockStore,
			settings:  testSettings,
			logger:    logger,
		}

		// Create test transactions:
		// - Transactions 0-49: First batch, each has a mined parent
		// - Transactions 50-99: Second batch, each depends on a transaction from first batch
		// - Transaction 100: Third batch, depends on transaction 50 from second batch
		//
		// This specifically tests the bug where the variable 'i' was shadowed at line 1644,
		// causing incorrect currentIdx calculation for transactions in later batches.

		unminedTxs := make([]*utxo.UnminedTransaction, 0, 101)
		parentTxHashes := make([]chainhash.Hash, 50)

		// Create first batch (50 transactions)
		for i := 0; i < 50; i++ {
			parentHash := chainhash.Hash{}
			for j := 0; j < len(parentHash); j++ {
				parentHash[j] = byte(i)
			}
			parentTxHashes[i] = parentHash

			tx := &utxo.UnminedTransaction{
				Node: &subtree.Node{
					Hash:        parentHash,
					Fee:         1000,
					SizeInBytes: 250,
				},
				TxInpoints: &subtree.TxInpoints{
					ParentTxHashes: []chainhash.Hash{{}}, // Empty hash means mined parent
					Idxs:           [][]uint32{{0}},
				},
				CreatedAt: i,
			}
			unminedTxs = append(unminedTxs, tx)
		}

		// Create second batch (50 transactions)
		childTxHashes := make([]chainhash.Hash, 50)
		for i := 0; i < 50; i++ {
			childHash := chainhash.Hash{}
			for j := 0; j < len(childHash); j++ {
				childHash[j] = byte(50 + i)
			}
			childTxHashes[i] = childHash

			tx := &utxo.UnminedTransaction{
				Node: &subtree.Node{
					Hash:        childHash,
					Fee:         1000,
					SizeInBytes: 250,
				},
				TxInpoints: &subtree.TxInpoints{
					ParentTxHashes: []chainhash.Hash{parentTxHashes[i]},
					Idxs:           [][]uint32{{0}},
				},
				CreatedAt: 50 + i,
			}
			unminedTxs = append(unminedTxs, tx)
		}

		// Create third batch (1 transaction) - this is where the bug would manifest
		grandchildHash := chainhash.Hash{}
		for j := 0; j < len(grandchildHash); j++ {
			grandchildHash[j] = byte(100)
		}

		grandchildTx := &utxo.UnminedTransaction{
			Node: &subtree.Node{
				Hash:        grandchildHash,
				Fee:         1000,
				SizeInBytes: 250,
			},
			TxInpoints: &subtree.TxInpoints{
				ParentTxHashes: []chainhash.Hash{childTxHashes[0]}, // Depends on tx at index 50
				Idxs:           [][]uint32{{0}},
			},
			CreatedAt: 100,
		}
		unminedTxs = append(unminedTxs, grandchildTx)

		// Setup mock responses for BatchDecorate
		// The mock needs to respond to BatchDecorate calls for each batch
		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				unresolvedParents := args.Get(1).([]*utxo.UnresolvedMetaData)
				for _, unresolved := range unresolvedParents {
					// Check if it's an empty hash (mined parent) or a known unmined parent
					isEmptyHash := true
					for _, b := range unresolved.Hash {
						if b != 0 {
							isEmptyHash = false
							break
						}
					}

					if isEmptyHash {
						// Mined parent - return with BlockIDs
						unresolved.Data = &meta.Data{
							BlockIDs:     []uint32{1},
							UnminedSince: 0,
							Locked:       false,
						}
					} else {
						// Unmined parent - check if it's in our list
						found := false
						for _, tx := range unminedTxs {
							if tx.Hash.IsEqual(&unresolved.Hash) {
								found = true
								break
							}
						}

						if found {
							// Unmined parent in our list
							unresolved.Data = &meta.Data{
								BlockIDs:     []uint32{},
								UnminedSince: 1,
								Locked:       false,
							}
						} else {
							// Parent not found - this would cause transaction to be skipped
							unresolved.Err = errors.ErrNotFound
						}
					}
				}
			}).
			Return(nil)

		// Create bestBlockHeaderIDsMap
		bestBlockHeaderIDsMap := map[uint32]bool{1: true}

		// Call validateParentChain
		validTxs, err := blockAssembler.validateParentChain(ctx, unminedTxs, bestBlockHeaderIDsMap)
		require.NoError(t, err)

		// All 101 transactions should be valid
		// Before the fix, the bug would cause the grandchild transaction (and potentially others)
		// to be incorrectly filtered due to wrong currentIdx calculation
		require.Equal(t, 101, len(validTxs), "All transactions should be valid with correct parent ordering")

		// Verify the grandchild transaction is included
		foundGrandchild := false
		for _, tx := range validTxs {
			if tx.Hash.IsEqual(&grandchildHash) {
				foundGrandchild = true
				break
			}
		}
		require.True(t, foundGrandchild, "Grandchild transaction should be included in valid transactions")

		mockStore.AssertExpectations(t)
	})

	t.Run("Invalid ordering - parent after child", func(t *testing.T) {
		// Setup mock UTXO store
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}

		testSettings := &settings.Settings{}
		testSettings.BlockAssembly.ParentValidationBatchSize = 50

		blockAssembler := &BlockAssembler{
			utxoStore: mockStore,
			settings:  testSettings,
			logger:    logger,
		}

		// Create transactions with INVALID ordering:
		// Transaction at index 0 depends on transaction at index 1 (parent comes after child)

		parentHash := chainhash.Hash{}
		for j := 0; j < len(parentHash); j++ {
			parentHash[j] = byte(1)
		}

		childHash := chainhash.Hash{}
		for j := 0; j < len(childHash); j++ {
			childHash[j] = byte(2)
		}

		// Child transaction (index 0) - depends on parent at index 1
		childTx := &utxo.UnminedTransaction{
			Node: &subtree.Node{
				Hash:        childHash,
				Fee:         1000,
				SizeInBytes: 250,
			},
			TxInpoints: &subtree.TxInpoints{
				ParentTxHashes: []chainhash.Hash{parentHash},
				Idxs:           [][]uint32{{0}},
			},
			CreatedAt: 0,
		}

		// Parent transaction (index 1) - no unmined parents
		parentTx := &utxo.UnminedTransaction{
			Node: &subtree.Node{
				Hash:        parentHash,
				Fee:         1000,
				SizeInBytes: 250,
			},
			TxInpoints: &subtree.TxInpoints{
				ParentTxHashes: []chainhash.Hash{{}}, // Empty hash = mined parent
				Idxs:           [][]uint32{{0}},
			},
			CreatedAt: 1,
		}

		unminedTxs := []*utxo.UnminedTransaction{childTx, parentTx}

		// Setup mock responses
		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				unresolvedParents := args.Get(1).([]*utxo.UnresolvedMetaData)
				for _, unresolved := range unresolvedParents {
					isEmptyHash := true
					for _, b := range unresolved.Hash {
						if b != 0 {
							isEmptyHash = false
							break
						}
					}

					if isEmptyHash {
						unresolved.Data = &meta.Data{
							BlockIDs:     []uint32{1},
							UnminedSince: 0,
							Locked:       false,
						}
					} else {
						// Check if it's the parent transaction
						if unresolved.Hash.IsEqual(&parentHash) {
							unresolved.Data = &meta.Data{
								BlockIDs:     []uint32{},
								UnminedSince: 1,
								Locked:       false,
							}
						} else {
							unresolved.Err = errors.ErrNotFound
						}
					}
				}
			}).
			Return(nil)

		bestBlockHeaderIDsMap := map[uint32]bool{1: true}

		// Call validateParentChain
		validTxs, err := blockAssembler.validateParentChain(ctx, unminedTxs, bestBlockHeaderIDsMap)
		require.NoError(t, err)

		// Only the parent should be valid, child should be skipped due to invalid ordering
		require.Equal(t, 1, len(validTxs), "Only parent transaction should be valid")
		require.Equal(t, parentHash.String(), validTxs[0].Hash.String(), "Valid transaction should be the parent")

		mockStore.AssertExpectations(t)
	})
}

func TestValidateParentChain_Reconciliation(t *testing.T) {
	ctx := context.Background()

	t.Run("Reconciles one missing parent from UTXO store", func(t *testing.T) {
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}

		testSettings := &settings.Settings{}
		testSettings.BlockAssembly.ParentValidationBatchSize = 100
		testSettings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs = true

		blockAssembler := &BlockAssembler{
			utxoStore: mockStore,
			settings:  testSettings,
			logger:    logger,
		}

		// Parent tx hash — NOT in the unmined list (simulates SI scan miss)
		parentHash := chainhash.Hash{}
		parentHash[0] = 0x01

		// Child tx hash — IS in the unmined list
		childHash := chainhash.Hash{}
		childHash[0] = 0x02

		childTx := &utxo.UnminedTransaction{
			Node: &subtree.Node{
				Hash:        childHash,
				Fee:         1000,
				SizeInBytes: 250,
			},
			TxInpoints: &subtree.TxInpoints{
				ParentTxHashes: []chainhash.Hash{parentHash},
				Idxs:           [][]uint32{{0}},
			},
			CreatedAt: 100,
		}

		// Only the child is in the unmined list (parent was missed by SI scan)
		unminedTxs := []*utxo.UnminedTransaction{childTx}

		// First BatchDecorate call (validation pass): returns parent as unmined
		// Second BatchDecorate call (reconciliation fetch): returns full parent data
		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				unresolvedParents := args.Get(1).([]*utxo.UnresolvedMetaData)
				for _, unresolved := range unresolvedParents {
					if unresolved.Hash.IsEqual(&parentHash) {
						unresolved.Data = &meta.Data{
							BlockIDs:     []uint32{},
							UnminedSince: 100,
							Locked:       false,
							Fee:          500,
							SizeInBytes:  200,
							TxInpoints: subtree.TxInpoints{
								ParentTxHashes: []chainhash.Hash{{}}, // parent's parent is mined (empty hash)
								Idxs:           [][]uint32{{0}},
							},
						}
					} else {
						// Empty hash (mined grandparent)
						unresolved.Data = &meta.Data{
							BlockIDs:     []uint32{1},
							UnminedSince: 0,
						}
					}
				}
			}).
			Return(nil)

		bestBlockHeaderIDsMap := map[uint32]bool{1: true}

		validTxs, err := blockAssembler.validateParentChain(ctx, unminedTxs, bestBlockHeaderIDsMap)
		require.NoError(t, err)

		// Both parent and child should be in the result
		require.Equal(t, 2, len(validTxs), "Both reconciled parent and child should be valid")

		// Parent should come before child (lower index)
		require.Equal(t, parentHash.String(), validTxs[0].Hash.String(), "Parent should be first")
		require.Equal(t, childHash.String(), validTxs[1].Hash.String(), "Child should be second")

		mockStore.AssertExpectations(t)
	})

	t.Run("Reconciles deep chain - grandparent also missed", func(t *testing.T) {
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}

		testSettings := &settings.Settings{}
		testSettings.BlockAssembly.ParentValidationBatchSize = 100

		testSettings.BlockAssembly.StoreTxInpointsForSubtreeMeta = true

		blockAssembler := &BlockAssembler{
			utxoStore: mockStore,
			settings:  testSettings,
			logger:    logger,
		}

		// Grandparent — NOT in unmined list (missed by SI scan)
		grandparentHash := chainhash.Hash{}
		grandparentHash[0] = 0x10

		// Parent — NOT in unmined list (missed by SI scan)
		parentHash := chainhash.Hash{}
		parentHash[0] = 0x20

		// Child — IS in unmined list
		childHash := chainhash.Hash{}
		childHash[0] = 0x30

		childTx := &utxo.UnminedTransaction{
			Node: &subtree.Node{
				Hash:        childHash,
				Fee:         1000,
				SizeInBytes: 250,
			},
			TxInpoints: &subtree.TxInpoints{
				ParentTxHashes: []chainhash.Hash{parentHash},
				Idxs:           [][]uint32{{0}},
			},
			CreatedAt: 300,
		}

		unminedTxs := []*utxo.UnminedTransaction{childTx}

		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				unresolvedParents := args.Get(1).([]*utxo.UnresolvedMetaData)
				for _, unresolved := range unresolvedParents {
					if unresolved.Hash.IsEqual(&grandparentHash) {
						unresolved.Data = &meta.Data{
							BlockIDs:     []uint32{},
							UnminedSince: 100,
							Fee:          200,
							SizeInBytes:  150,
							TxInpoints: subtree.TxInpoints{
								ParentTxHashes: []chainhash.Hash{{}}, // grandparent's parent is mined
								Idxs:           [][]uint32{{0}},
							},
						}
					} else if unresolved.Hash.IsEqual(&parentHash) {
						unresolved.Data = &meta.Data{
							BlockIDs:     []uint32{},
							UnminedSince: 100,
							Fee:          500,
							SizeInBytes:  200,
							TxInpoints: subtree.TxInpoints{
								ParentTxHashes: []chainhash.Hash{grandparentHash},
								Idxs:           [][]uint32{{0}},
							},
						}
					} else {
						// Empty/mined parent
						unresolved.Data = &meta.Data{
							BlockIDs:     []uint32{1},
							UnminedSince: 0,
						}
					}
				}
			}).
			Return(nil)

		bestBlockHeaderIDsMap := map[uint32]bool{1: true}

		validTxs, err := blockAssembler.validateParentChain(ctx, unminedTxs, bestBlockHeaderIDsMap)
		require.NoError(t, err)

		// All three should be present: grandparent, parent, child
		require.Equal(t, 3, len(validTxs), "Grandparent, parent, and child should all be valid")

		mockStore.AssertExpectations(t)
	})

	t.Run("Skip reconciliation if parent has block_ids on best chain", func(t *testing.T) {
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}

		testSettings := &settings.Settings{}
		testSettings.BlockAssembly.ParentValidationBatchSize = 100
		testSettings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs = true

		blockAssembler := &BlockAssembler{
			utxoStore: mockStore,
			settings:  testSettings,
			logger:    logger,
		}

		// Parent — not in list, but BatchDecorate says unmined_since>0 AND block_ids on best chain
		// This is a data inconsistency — the parent should NOT be reconciled
		parentHash := chainhash.Hash{}
		parentHash[0] = 0x01

		childHash := chainhash.Hash{}
		childHash[0] = 0x02

		childTx := &utxo.UnminedTransaction{
			Node: &subtree.Node{
				Hash:        childHash,
				Fee:         1000,
				SizeInBytes: 250,
			},
			TxInpoints: &subtree.TxInpoints{
				ParentTxHashes: []chainhash.Hash{parentHash},
				Idxs:           [][]uint32{{0}},
			},
			CreatedAt: 100,
		}

		unminedTxs := []*utxo.UnminedTransaction{childTx}

		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				unresolvedParents := args.Get(1).([]*utxo.UnresolvedMetaData)
				for _, unresolved := range unresolvedParents {
					if unresolved.Hash.IsEqual(&parentHash) {
						unresolved.Data = &meta.Data{
							BlockIDs:     []uint32{1}, // ON best chain
							UnminedSince: 100,         // But also marked unmined (inconsistency)
							Fee:          500,
							SizeInBytes:  200,
						}
					}
				}
			}).
			Return(nil)

		bestBlockHeaderIDsMap := map[uint32]bool{1: true}

		validTxs, err := blockAssembler.validateParentChain(ctx, unminedTxs, bestBlockHeaderIDsMap)
		require.NoError(t, err)

		// The parent should NOT be reconciled (data inconsistency — block_ids on best chain).
		// With filtering enabled, the child is skipped because its parent couldn't be reconciled.
		for _, tx := range validTxs {
			require.False(t, tx.Hash.IsEqual(&parentHash), "Parent with block_ids on best chain should not be reconciled into list")
		}
		require.Len(t, validTxs, 0, "Child should be skipped when parent reconciliation fails and filtering is enabled")

		mockStore.AssertExpectations(t)
	})

	t.Run("Max passes exceeded - falls back gracefully", func(t *testing.T) {
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}

		testSettings := &settings.Settings{}
		testSettings.BlockAssembly.ParentValidationBatchSize = 100

		testSettings.BlockAssembly.StoreTxInpointsForSubtreeMeta = true

		blockAssembler := &BlockAssembler{
			utxoStore: mockStore,
			settings:  testSettings,
			logger:    logger,
		}

		// Create a chain of 5 levels deep (exceeds max 3 reconciliation passes)
		// level0 (mined) <- level1 (missed) <- level2 (missed) <- level3 (missed) <- level4 (missed) <- child (in list)
		hashes := make([]chainhash.Hash, 6)
		for i := range hashes {
			hashes[i] = chainhash.Hash{}
			hashes[i][0] = byte(i + 1)
		}

		// Only the child (hashes[5]) is in the unmined list
		childTx := &utxo.UnminedTransaction{
			Node: &subtree.Node{
				Hash:        hashes[5],
				Fee:         1000,
				SizeInBytes: 250,
			},
			TxInpoints: &subtree.TxInpoints{
				ParentTxHashes: []chainhash.Hash{hashes[4]},
				Idxs:           [][]uint32{{0}},
			},
			CreatedAt: 500,
		}

		unminedTxs := []*utxo.UnminedTransaction{childTx}

		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				unresolvedParents := args.Get(1).([]*utxo.UnresolvedMetaData)
				for _, unresolved := range unresolvedParents {
					// hashes[0] is mined
					if unresolved.Hash.IsEqual(&hashes[0]) {
						unresolved.Data = &meta.Data{
							BlockIDs:     []uint32{1},
							UnminedSince: 0,
						}
					} else {
						// Find which level this is
						for level := 1; level <= 4; level++ {
							if unresolved.Hash.IsEqual(&hashes[level]) {
								unresolved.Data = &meta.Data{
									BlockIDs:     []uint32{},
									UnminedSince: 100,
									Fee:          uint64(level * 100),
									SizeInBytes:  uint64(level * 50),
									TxInpoints: subtree.TxInpoints{
										ParentTxHashes: []chainhash.Hash{hashes[level-1]},
										Idxs:           [][]uint32{{0}},
									},
								}
								break
							}
						}
						// Empty hash (mined)
						if unresolved.Data == nil {
							unresolved.Data = &meta.Data{
								BlockIDs:     []uint32{1},
								UnminedSince: 0,
							}
						}
					}
				}
			}).
			Return(nil)

		bestBlockHeaderIDsMap := map[uint32]bool{1: true}

		validTxs, err := blockAssembler.validateParentChain(ctx, unminedTxs, bestBlockHeaderIDsMap)
		require.NoError(t, err)

		// With max 3 passes, we can reconcile levels 4, 3, 2 (3 passes).
		// Level 1 would need a 4th pass. The key assertion: it doesn't panic or error out
		require.NotNil(t, validTxs)

		mockStore.AssertExpectations(t)
	})
}
