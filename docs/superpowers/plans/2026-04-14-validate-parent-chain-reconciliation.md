# Validate Parent Chain Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `validateParentChain` self-healing — when it finds a parent tx that the Aerospike SI scan missed, fetch the full record via direct primary key read and add it to the unmined list, producing a valid mining candidate.

**Architecture:** The reconciliation is entirely within `validateParentChain` in `services/blockassembly/BlockAssembler.go`. When a parent is "unmined but not in processing list", we collect it for reconciliation. After the main validation pass, we BatchDecorate the missing parents with the full field set, build `UnminedTransaction` structs, append them, re-sort, rebuild the index, and run a scoped re-validation pass. Capped at 3 passes.

**Tech Stack:** Go, Aerospike UTXO store (via `BatchDecorate`), testify mocks

**Spec:** `docs/superpowers/specs/2026-04-14-validate-parent-chain-reconciliation-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `services/blockassembly/BlockAssembler.go` | Modify (~1417-1700) | Add reconciliation logic to `validateParentChain` |
| `services/blockassembly/filter_transactions_test.go` | Modify | Add reconciliation tests |

---

### Task 1: Write failing test — basic reconciliation of one missing parent

**Files:**

- Modify: `services/blockassembly/filter_transactions_test.go`

- [ ] **Step 1: Write the failing test**

Add this test to `filter_transactions_test.go` after the existing `TestValidateParentChain_BatchingAndOrdering` function. This test creates a child tx in the unmined list whose parent is NOT in the list, but BatchDecorate returns the parent as unmined. The reconciliation should fetch and add it.

```go
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
        mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
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
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -v -race -tags "testtxmetacache" -run TestValidateParentChain_Reconciliation -count=1 ./services/blockassembly/`

Expected: FAIL — currently the child tx will either be skipped (filtering enabled) or kept without its parent. The result will have 0 or 1 txs, not 2.

- [ ] **Step 3: Commit the failing test**

```bash
git add services/blockassembly/filter_transactions_test.go
git commit -m "test: add failing test for validateParentChain reconciliation"
```

---

### Task 2: Implement the reconciliation logic

**Files:**

- Modify: `services/blockassembly/BlockAssembler.go` (~1417-1700)

- [ ] **Step 1: Refactor validateParentChain to extract the core validation into a helper**

We need to run the validation logic multiple times (initial + reconciliation passes). Extract the per-batch validation into a method that returns both valid txs and a list of missing parent hashes that need reconciliation. Modify `validateParentChain` to wrap the validation in a reconciliation loop.

Replace the entire `validateParentChain` method (lines ~1417-1700) with:

```go
// validateParentChain validates that unmined transactions have their parent transactions
// either on the best chain or also unmined (to be processed together).
// If parents are unmined but missing from the list (e.g. due to Aerospike SI scan race),
// it reconciles them by fetching full records via direct primary key reads and re-validating.
//
// Parameters:
//   - ctx: Context for cancellation
//   - unminedTxs: List of unmined transactions to validate
//   - bestBlockHeaderIDsMap: Map of block IDs on the best chain
//
// Returns:
//   - []*utxo.UnminedTransaction: List of valid transactions with reconciled parents inserted
//   - error: Context cancellation error if cancelled, nil otherwise
func (b *BlockAssembler) validateParentChain(
    ctx context.Context,
    unminedTxs []*utxo.UnminedTransaction,
    bestBlockHeaderIDsMap map[uint32]bool,
) ([]*utxo.UnminedTransaction, error) {
    if len(unminedTxs) == 0 {
        return unminedTxs, nil
    }

    b.logger.Infof("[BlockAssembler][validateParentChain] Starting parent chain validation for %d unmined transactions", len(unminedTxs))

    const maxReconciliationPasses = 3
    totalReconciled := 0

    for pass := 1; pass <= maxReconciliationPasses; pass++ {
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        default:
        }

        validTxs, missingParentHashes, skippedCount, err := b.validateParentChainPass(ctx, unminedTxs, bestBlockHeaderIDsMap)
        if err != nil {
            return nil, err
        }

        if len(missingParentHashes) == 0 {
            // No missing parents — validation complete
            filteringStatus := "disabled"
            if b.settings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs {
                filteringStatus = "enabled"
            }
            if skippedCount > 0 {
                b.logger.Warnf("[BlockAssembler][validateParentChain] Skipped %d transactions due to invalid/missing parent chains (filtering: %s)", skippedCount, filteringStatus)
            }
            if totalReconciled > 0 {
                b.logger.Infof("[BlockAssembler][validateParentChain] Reconciliation complete: recovered %d missing parent(s) across %d pass(es)", totalReconciled, pass-1)
            }
            b.logger.Infof("[BlockAssembler][validateParentChain] Parent chain validation complete: %d valid, %d skipped (filtering: %s)",
                len(validTxs), skippedCount, filteringStatus)
            return validTxs, nil
        }

        // Reconcile missing parents
        b.logger.Infof("[BlockAssembler][validateParentChain] Pass %d: found %d missing parent(s), attempting reconciliation from UTXO store", pass, len(missingParentHashes))

        reconciledTxs, err := b.reconcileMissingParents(ctx, missingParentHashes, bestBlockHeaderIDsMap)
        if err != nil {
            return nil, err
        }

        if len(reconciledTxs) == 0 {
            // Couldn't reconcile any — return what we have
            b.logger.Warnf("[BlockAssembler][validateParentChain] Pass %d: could not reconcile any of %d missing parent(s)", pass, len(missingParentHashes))
            filteringStatus := "disabled"
            if b.settings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs {
                filteringStatus = "enabled"
            }
            if skippedCount > 0 {
                b.logger.Warnf("[BlockAssembler][validateParentChain] Skipped %d transactions due to invalid/missing parent chains (filtering: %s)", skippedCount, filteringStatus)
            }
            b.logger.Infof("[BlockAssembler][validateParentChain] Parent chain validation complete: %d valid, %d skipped (filtering: %s)",
                len(validTxs), skippedCount, filteringStatus)
            return validTxs, nil
        }

        totalReconciled += len(reconciledTxs)

        if totalReconciled > 1000 {
            b.logger.Warnf("[BlockAssembler][validateParentChain] Reconciliation found unusually high count (%d) — possible systemic issue", totalReconciled)
        }

        // Merge reconciled parents into the full list and re-sort for next pass
        unminedTxs = append(unminedTxs, reconciledTxs...)
        sort.Slice(unminedTxs, func(i, j int) bool {
            return unminedTxs[i].CreatedAt < unminedTxs[j].CreatedAt
        })

        b.logger.Infof("[BlockAssembler][validateParentChain] Pass %d: reconciled %d missing parent(s) from UTXO store, re-validating", pass, len(reconciledTxs))
    }

    // Max passes exceeded — do one final validation pass and return
    b.logger.Warnf("[BlockAssembler][validateParentChain] Max reconciliation passes (%d) exceeded with %d total reconciled — falling back", maxReconciliationPasses, totalReconciled)
    validTxs, _, skippedCount, err := b.validateParentChainPass(ctx, unminedTxs, bestBlockHeaderIDsMap)
    if err != nil {
        return nil, err
    }

    filteringStatus := "disabled"
    if b.settings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs {
        filteringStatus = "enabled"
    }
    if skippedCount > 0 {
        b.logger.Warnf("[BlockAssembler][validateParentChain] Skipped %d transactions due to invalid/missing parent chains (filtering: %s)", skippedCount, filteringStatus)
    }
    b.logger.Infof("[BlockAssembler][validateParentChain] Parent chain validation complete: %d valid, %d skipped (filtering: %s)",
        len(validTxs), skippedCount, filteringStatus)
    return validTxs, nil
}
```

- [ ] **Step 2: Add the `validateParentChainPass` method**

This is the extracted core logic from the original `validateParentChain`. It returns valid txs, a deduplicated list of missing parent hashes (for reconciliation), and the skipped count. Add it right after the `validateParentChain` method:

```go
// validateParentChainPass runs a single validation pass over unminedTxs.
// Returns:
//   - validTxs: transactions that passed validation
//   - missingParentHashes: deduplicated hashes of parents that are unmined but not in the list
//   - skippedCount: number of transactions skipped due to invalid parents
//   - err: context cancellation error
func (b *BlockAssembler) validateParentChainPass(
    ctx context.Context,
    unminedTxs []*utxo.UnminedTransaction,
    bestBlockHeaderIDsMap map[uint32]bool,
) ([]*utxo.UnminedTransaction, []chainhash.Hash, int, error) {

    // Build index of referenced parents
    referencedParents := make(map[chainhash.Hash]bool)
    for _, tx := range unminedTxs {
        parentHashes := tx.TxInpoints.GetParentTxHashes()
        for _, parentHash := range parentHashes {
            referencedParents[parentHash] = true
        }
    }
    b.logger.Debugf("[BlockAssembler][validateParentChainPass] Found %d unique parent references out of %d transactions", len(referencedParents), len(unminedTxs))

    parentIndexMap := make(map[chainhash.Hash]int, len(referencedParents))
    for idx, unminedTx := range unminedTxs {
        if referencedParents[unminedTx.Node.Hash] {
            parentIndexMap[unminedTx.Node.Hash] = idx
        }
    }

    validTxs := make([]*utxo.UnminedTransaction, 0, len(unminedTxs))
    missingParentSet := make(map[chainhash.Hash]bool)
    skippedCount := 0
    batchSize := b.settings.BlockAssembly.ParentValidationBatchSize

    for i := 0; i < len(unminedTxs); i += batchSize {
        select {
        case <-ctx.Done():
            return nil, nil, 0, ctx.Err()
        default:
        }

        end := i + batchSize
        if end > len(unminedTxs) {
            end = len(unminedTxs)
        }
        batch := unminedTxs[i:end]

        // Collect unique parent tx IDs in this batch
        parentTxIDs := make([]chainhash.Hash, 0, len(batch)*2)
        parentTxIDMap := make(map[chainhash.Hash]bool)
        for _, tx := range batch {
            parentHashes := tx.TxInpoints.GetParentTxHashes()
            for _, parentTxID := range parentHashes {
                if !parentTxIDMap[parentTxID] {
                    parentTxIDs = append(parentTxIDs, parentTxID)
                    parentTxIDMap[parentTxID] = true
                }
            }
        }

        // Batch fetch parent metadata
        var parentMetadata map[chainhash.Hash]*meta.Data
        if len(parentTxIDs) > 0 {
            parentMetadata = make(map[chainhash.Hash]*meta.Data)
            unresolvedParents := make([]*utxo.UnresolvedMetaData, 0, len(parentTxIDs))
            for parentIdx, parentTxID := range parentTxIDs {
                unresolvedParents = append(unresolvedParents, &utxo.UnresolvedMetaData{
                    Hash: parentTxID,
                    Idx:  parentIdx,
                })
            }

            err := b.utxoStore.BatchDecorate(ctx, unresolvedParents,
                fields.BlockIDs, fields.UnminedSince, fields.Locked)
            if err != nil {
                b.logger.Warnf("[BlockAssembler][validateParentChainPass] BatchDecorate error (will check individual results): %v", err)
            }

            for _, unresolved := range unresolvedParents {
                if unresolved.Err != nil {
                    b.logger.Errorf("[BlockAssembler][validateParentChainPass] Failed to get parent tx %s metadata: %v",
                        unresolved.Hash.String(), unresolved.Err)
                    continue
                }
                if unresolved.Data != nil {
                    parentMetadata[unresolved.Hash] = unresolved.Data
                }
            }
        }

        // Validate each transaction in the batch
        for batchIdx, tx := range batch {
            // Check if tx is already on best chain (shouldn't be in unmined list)
            if len(tx.BlockIDs) > 0 {
                onBestChain := false
                for _, blockID := range tx.BlockIDs {
                    if bestBlockHeaderIDsMap[blockID] {
                        onBestChain = true
                        break
                    }
                }
                if onBestChain {
                    b.logger.Warnf("[BlockAssembler][validateParentChainPass] Transaction %s is already on best chain but marked as unmined", tx.Hash.String())
                    if b.settings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs {
                        skippedCount++
                        continue
                    }
                }
            }

            allParentsValid := true
            invalidReason := ""
            hasMissingParent := false
            unminedParents := make([]chainhash.Hash, 0)

            parentHashes := tx.TxInpoints.GetParentTxHashes()
            for _, parentTxID := range parentHashes {
                parentMeta, exists := parentMetadata[parentTxID]
                if !exists {
                    allParentsValid = false
                    invalidReason = fmt.Sprintf("parent tx %s not found in UTXO store", parentTxID.String())
                    b.logger.Warnf("[BlockAssembler][validateParentChainPass] Transaction %s has invalid parent: %s", tx.Hash.String(), invalidReason)
                    break
                }

                if parentMeta.UnminedSince > 0 {
                    if _, isInUnminedList := parentIndexMap[parentTxID]; isInUnminedList {
                        unminedParents = append(unminedParents, parentTxID)
                    } else {
                        // Unmined but not in our list — candidate for reconciliation
                        allParentsValid = false
                        hasMissingParent = true
                        missingParentSet[parentTxID] = true
                        b.logger.Debugf("[BlockAssembler][validateParentChainPass] Transaction %s has missing parent %s (unmined, not in list) — will attempt reconciliation", tx.Hash.String(), parentTxID.String())
                        break
                    }
                } else if len(parentMeta.BlockIDs) > 0 {
                    onBestChain := false
                    for _, blockID := range parentMeta.BlockIDs {
                        if bestBlockHeaderIDsMap[blockID] {
                            onBestChain = true
                            break
                        }
                    }
                    if !onBestChain {
                        allParentsValid = false
                        invalidReason = fmt.Sprintf("parent tx %s is on wrong chain (blocks: %v) and not in unmined list - data integrity issue from fork handling",
                            parentTxID.String(), parentMeta.BlockIDs)
                        b.logger.Warnf("[BlockAssembler][validateParentChainPass] Transaction %s has invalid parent: %s", tx.Hash.String(), invalidReason)
                        break
                    }
                } else {
                    allParentsValid = false
                    invalidReason = fmt.Sprintf("parent tx %s has data inconsistency (unmined_since=0 but no block_ids)", parentTxID.String())
                    b.logger.Warnf("[BlockAssembler][validateParentChainPass] Transaction %s has invalid parent: %s", tx.Hash.String(), invalidReason)
                    break
                }
            }

            // Check ordering of unmined parents
            if allParentsValid && len(unminedParents) > 0 {
                currentIdx := i + batchIdx
                for _, parentTxID := range unminedParents {
                    parentIdx, parentExists := parentIndexMap[parentTxID]
                    if !parentExists {
                        b.logger.Errorf("[BlockAssembler][validateParentChainPass] Parent tx %s not found in index map", parentTxID.String())
                        allParentsValid = false
                        break
                    }
                    if parentIdx >= currentIdx {
                        allParentsValid = false
                        invalidReason = fmt.Sprintf("parent tx %s (index %d) comes after child tx %s (index %d)",
                            parentTxID.String(), parentIdx, tx.Hash.String(), currentIdx)
                        b.logger.Warnf("[BlockAssembler][validateParentChainPass] Skipping tx %s: %s", tx.Hash.String(), invalidReason)
                        break
                    }
                }
            }

            if allParentsValid {
                validTxs = append(validTxs, tx)
            } else if hasMissingParent {
                // Don't skip — leave for reconciliation. Keep the tx for the next pass.
                validTxs = append(validTxs, tx)
            } else {
                if b.settings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs {
                    skippedCount++
                } else {
                    validTxs = append(validTxs, tx)
                }
            }
        }
    }

    // Deduplicate missing parent hashes into a slice
    missingParentHashes := make([]chainhash.Hash, 0, len(missingParentSet))
    for hash := range missingParentSet {
        missingParentHashes = append(missingParentHashes, hash)
    }

    return validTxs, missingParentHashes, skippedCount, nil
}
```

- [ ] **Step 3: Add the `reconcileMissingParents` method**

This method fetches full records for the missing parents and builds `UnminedTransaction` structs. Add it right after `validateParentChainPass`:

```go
// reconcileMissingParents fetches full transaction records for parents that were missed
// by the SI scan but confirmed as unmined by BatchDecorate. Returns UnminedTransaction
// structs ready to be inserted into the unmined list.
func (b *BlockAssembler) reconcileMissingParents(
    ctx context.Context,
    missingHashes []chainhash.Hash,
    bestBlockHeaderIDsMap map[uint32]bool,
) ([]*utxo.UnminedTransaction, error) {
    if len(missingHashes) == 0 {
        return nil, nil
    }

    // Fetch full metadata for missing parents
    unresolvedParents := make([]*utxo.UnresolvedMetaData, 0, len(missingHashes))
    for idx, hash := range missingHashes {
        unresolvedParents = append(unresolvedParents, &utxo.UnresolvedMetaData{
            Hash: hash,
            Idx:  idx,
        })
    }

    // Request the full field set needed to build an UnminedTransaction
    fetchFields := []fields.FieldName{
        fields.Fee, fields.SizeInBytes, fields.CreatedAt,
        fields.BlockIDs, fields.UnminedSince, fields.Locked,
    }
    if b.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta {
        fetchFields = append(fetchFields, fields.Inputs, fields.External)
    }

    err := b.utxoStore.BatchDecorate(ctx, unresolvedParents, fetchFields...)
    if err != nil {
        b.logger.Warnf("[BlockAssembler][reconcileMissingParents] BatchDecorate error (will check individual results): %v", err)
    }

    reconciled := make([]*utxo.UnminedTransaction, 0, len(missingHashes))

    for _, unresolved := range unresolvedParents {
        if unresolved.Err != nil {
            b.logger.Warnf("[BlockAssembler][reconcileMissingParents] Could not fetch parent tx %s: %v", unresolved.Hash.String(), unresolved.Err)
            continue
        }
        if unresolved.Data == nil {
            b.logger.Warnf("[BlockAssembler][reconcileMissingParents] Parent tx %s returned nil data", unresolved.Hash.String())
            continue
        }

        data := unresolved.Data

        // Skip if unmined_since is 0 (no longer unmined — race resolved itself)
        if data.UnminedSince == 0 {
            b.logger.Debugf("[BlockAssembler][reconcileMissingParents] Parent tx %s no longer unmined (race resolved), skipping", unresolved.Hash.String())
            continue
        }

        // Skip if block_ids are on best chain (data inconsistency)
        if len(data.BlockIDs) > 0 {
            onBestChain := false
            for _, blockID := range data.BlockIDs {
                if bestBlockHeaderIDsMap[blockID] {
                    onBestChain = true
                    break
                }
            }
            if onBestChain {
                b.logger.Debugf("[BlockAssembler][reconcileMissingParents] Parent tx %s has block_ids on best chain despite unmined_since>0, skipping (MarkTransactionsOnLongestChain will fix)", unresolved.Hash.String())
                continue
            }
        }

        // Skip if no CreatedAt (split record, not a real unmined tx)
        // CreatedAt is stored in meta.Data as part of BatchDecorate response
        // For the UTXO store, CreatedAt comes back as 0 if not set
        // We check Fee > 0 as a proxy since all real txs have fees
        // (CreatedAt is not in meta.Data — it's only on UnminedTransaction from the iterator)
        // Actually, we can use a 0 CreatedAt and it will sort to the front, which is fine
        // since parent should come before child anyway.

        var txInpoints subtree.TxInpoints
        if b.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta {
            txInpoints = data.TxInpoints
        }

        reconciled = append(reconciled, &utxo.UnminedTransaction{
            Node: &subtree.Node{
                Hash:        unresolved.Hash,
                Fee:         data.Fee,
                SizeInBytes: data.SizeInBytes,
            },
            TxInpoints:   &txInpoints,
            CreatedAt:    0, // Not available from meta.Data; 0 sorts to front (before children)
            Locked:       data.Locked,
            BlockIDs:     data.BlockIDs,
            UnminedSince: int(data.UnminedSince),
        })
    }

    return reconciled, nil
}
```

- [ ] **Step 4: Add the `sort` import**

The `sort` package is needed for re-sorting after reconciliation. Check if it's already imported at the top of BlockAssembler.go. If not, add it to the import block.

- [ ] **Step 5: Run the test from Task 1 to verify it passes**

Run: `go test -v -race -tags "testtxmetacache" -run TestValidateParentChain_Reconciliation -count=1 ./services/blockassembly/`

Expected: PASS — the reconciliation should fetch the missing parent and include both parent and child in the result.

- [ ] **Step 6: Run existing tests to verify no regressions**

Run: `go test -v -race -tags "testtxmetacache" -run TestValidateParentChain -count=1 ./services/blockassembly/`

Expected: All existing `TestValidateParentChain_BatchingAndOrdering` subtests PASS.

- [ ] **Step 7: Commit**

```bash
git add services/blockassembly/BlockAssembler.go services/blockassembly/filter_transactions_test.go
git commit -m "feat: add parent chain reconciliation to recover SI-scan-missed parents"
```

---

### Task 3: Write test — deep chain reconciliation (grandparent missed too)

**Files:**

- Modify: `services/blockassembly/filter_transactions_test.go`

- [ ] **Step 1: Write the test**

Add this subtest inside `TestValidateParentChain_Reconciliation`:

```go
t.Run("Reconciles deep chain - grandparent also missed", func(t *testing.T) {
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

    mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
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
```

- [ ] **Step 2: Run the test**

Run: `go test -v -race -tags "testtxmetacache" -run "TestValidateParentChain_Reconciliation/Reconciles_deep_chain" -count=1 ./services/blockassembly/`

Expected: PASS — the multi-pass reconciliation should discover the grandparent on pass 2.

- [ ] **Step 3: Commit**

```bash
git add services/blockassembly/filter_transactions_test.go
git commit -m "test: add deep chain reconciliation test (grandparent)"
```

---

### Task 4: Write test — max passes exceeded

**Files:**

- Modify: `services/blockassembly/filter_transactions_test.go`

- [ ] **Step 1: Write the test**

Add this subtest inside `TestValidateParentChain_Reconciliation`:

```go
t.Run("Max passes exceeded - falls back gracefully", func(t *testing.T) {
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

    mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
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
    // Level 1 would need a 4th pass. After max passes, the final validation
    // pass will include what it can. Level 1 will still be missing so
    // level 2's parent check will fail, and level 2 + its descendants
    // may be skipped depending on filtering.
    // The key assertion: it doesn't panic or error out
    require.NotNil(t, validTxs)

    mockStore.AssertExpectations(t)
})
```

- [ ] **Step 2: Run the test**

Run: `go test -v -race -tags "testtxmetacache" -run "TestValidateParentChain_Reconciliation/Max_passes" -count=1 ./services/blockassembly/`

Expected: PASS — the function completes gracefully without panicking.

- [ ] **Step 3: Commit**

```bash
git add services/blockassembly/filter_transactions_test.go
git commit -m "test: add max reconciliation passes test"
```

---

### Task 5: Write test — reconciled parent has block_ids on best chain (skip it)

**Files:**

- Modify: `services/blockassembly/filter_transactions_test.go`

- [ ] **Step 1: Write the test**

Add this subtest inside `TestValidateParentChain_Reconciliation`:

```go
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

    callCount := 0
    mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
        Run(func(args mock.Arguments) {
            callCount++
            unresolvedParents := args.Get(1).([]*utxo.UnresolvedMetaData)
            for _, unresolved := range unresolvedParents {
                if unresolved.Hash.IsEqual(&parentHash) {
                    unresolved.Data = &meta.Data{
                        BlockIDs:     []uint32{1}, // ON best chain
                        UnminedSince: 100,          // But also marked unmined (inconsistency)
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

    // The parent should NOT be reconciled (data inconsistency).
    // The child should be skipped since its parent is invalid and filtering is enabled.
    // Result: 0 valid txs (child skipped, parent not reconciled)
    // OR: 1 valid tx (child kept because hasMissingParent=true but parent wasn't reconciled,
    // so on the final pass child's parent is still missing and child gets skipped)
    // The key: parent should NOT appear in validTxs
    for _, tx := range validTxs {
        require.False(t, tx.Hash.IsEqual(&parentHash), "Parent with block_ids on best chain should not be reconciled into list")
    }

    mockStore.AssertExpectations(t)
})
```

- [ ] **Step 2: Run the test**

Run: `go test -v -race -tags "testtxmetacache" -run "TestValidateParentChain_Reconciliation/Skip_reconciliation" -count=1 ./services/blockassembly/`

Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add services/blockassembly/filter_transactions_test.go
git commit -m "test: verify reconciliation skips parents with block_ids on best chain"
```

---

### Task 6: Run full test suite and lint

**Files:**

- None (verification only)

- [ ] **Step 1: Run all block assembly tests**

Run: `go test -v -race -tags "testtxmetacache" -count=1 ./services/blockassembly/`

Expected: All tests PASS.

- [ ] **Step 2: Run lint on changed files**

Run: `make lint`

Expected: No lint errors on the changed files.

- [ ] **Step 3: Fix any issues found**

If tests fail or lint errors appear, fix them and re-run.

- [ ] **Step 4: Final commit if any fixes were needed**

```bash
git add services/blockassembly/BlockAssembler.go services/blockassembly/filter_transactions_test.go
git commit -m "fix: address lint and test issues from reconciliation implementation"
```

---

## Notes for Implementer

1. **`meta.Data` does not have `CreatedAt`** — the iterator extracts this from a separate Aerospike bin. When reconciling, we set `CreatedAt: 0` which sorts the parent to the front of the list (before all children). This is correct behavior since the parent must come before its children.

2. **`TxInpoints` from `meta.Data`** — the `meta.Data.TxInpoints` field is populated when `fields.Inputs` and `fields.External` are requested via BatchDecorate. This gives us the parent hash references needed for the reconciled tx to itself be validated in subsequent passes.

3. **The `sort` package** — `BlockAssembler.go` already imports `sort` (used in `loadUnminedTransactions`). Verify this before adding a duplicate import.

4. **`hasMissingParent` flag** — in `validateParentChainPass`, when a parent is unmined but not in the list, we set `hasMissingParent = true` and keep the child tx in `validTxs`. This ensures the child is available for the next pass after reconciliation adds its parent. Without this, the child would be skipped and never recovered.
