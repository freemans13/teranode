package subtreevalidation

import (
	"context"
	"slices"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// missingRecordPolicy is how checkCounterConflictingOnCurrentChainBatch treats a
// record the store does not hold.
type missingRecordPolicy int

const (
	// skipMissingRecords is the cached path's policy. A conflicting tx whose own
	// or parent record is gone is dropped and returned in notFound; a cone member
	// that is gone is a leaf of the walk and counts as not mined. A trailer can
	// outlive the records it names, so a gap there is housekeeping, not evidence.
	skipMissingRecords missingRecordPolicy = iota

	// failOnMissingRecords is the fresh path's policy, and the per-transaction
	// check's: any record the check needs and cannot read — a conflicting tx, a
	// parent, a counter-spender, a cone member, a member's GetMeta — fails the
	// check with a retryable ErrTxNotFound, so the verdict matches what
	// blessMissingTransaction reaches for the same store state.
	failOnMissingRecords
)

// counterConflictingBatchFanOut caps the concurrent store reads of each phase of
// checkCounterConflictingOnCurrentChainBatch, matching the GetMeta errgroup of
// checkCounterConflictingOnCurrentChain and the walk's own per-level ceiling.
const counterConflictingBatchFanOut = 128

// checkCounterConflictingOnCurrentChainBatch asks the question of
// checkCounterConflictingOnCurrentChain for a whole set of conflicting
// transactions at once: has any of them, or any transaction that spent the same
// outputs (and that spender's descendants), been mined on the chain described by
// blockIds? It returns ErrTxInvalid when one has.
//
// # WHY A BATCH
//
// Asked one transaction at a time, the check walks each transaction's own cone
// and its counter-spenders' cones separately. Conflicting transactions in one
// block share those cones: in a linear conflicting chain T1→…→TN each Ti's own
// cone is Ti+1…TN, and only T1 has a counter-spender other than a chain member, so
// per-transaction checks cost ~N²/2 sequential store reads (issue 2037: a
// 1,788-transaction chain took about five hours against a 30 minute timeout, and
// every retry started from zero). Here every record is read a constant number of
// times, and the walk starts from every transaction at once, so a chain costs a
// few wide store rounds rather than N deep ones.
//
// # PHASES
//
//  1. Counter-spenders. Each transaction's inpoints and its parents' spenders are
//     read, and its counter-spenders derived exactly as
//     utxo.GetCounterConflictingTxHashes derives them, including the out-of-range
//     input error and the frozen sentinel.
//  2. One walk. utxo.GetConflictingChildrenFromRoots walks two passes on one
//     visited set: first from every counter-spender, which yields the union of
//     the counter-spender cones, then from the transactions themselves, which
//     yields the union of their own cones less what the first pass already saw. A
//     frozen sentinel in either union is a processing error.
//  3. Mined check. The counter-conflicting union — the transactions, their
//     counter-spenders and the counter-spender cones — is read with GetMeta and
//     checked against blockIds. Own-cone members are frozen-checked only, as in
//     the per-transaction check.
//
// # BUDGET
//
// utxostore_conflictingChildrenMaxNodes is a per-root budget, as it was for each
// per-transaction walk. The phase 2 walk may visit it times the number of
// distinct roots (transactions and counter-spenders): the aggregate bound of the
// per-transaction walks, so any block whose every per-transaction walk fit still
// passes, however many conflicting transactions it carries. Exceeding it fails
// closed with ERR_UTXO_WALK_LIMIT_EXCEEDED. The price is that a single runaway
// cone is stopped only after that many reads, not after maxNodes.
//
// # MISSING RECORDS
//
// The policy is the caller's, per path:
//
//   - failOnMissingRecords (the fresh path) is the per-transaction check's
//     behaviour: any record the check needs and the store does not hold returns
//     a retryable processing error wrapping ErrTxNotFound, before any mined
//     verdict.
//   - skipMissingRecords (the cached path) drops a transaction whose own or
//     parent record is gone, returning it in notFound. A cone member that is gone
//     is a leaf of the walk, and a member whose GetMeta is not found counts as not
//     mined; the rest of the union is still checked. That is stricter than the
//     per-transaction loop the cached path used to run, which skipped a
//     transaction's whole check on any missing member.
//
// # VERDICT
//
// Any processing error (a frozen sentinel, the walk budget, a store fault) wins
// over ErrTxInvalid, and every member is checked before ErrTxInvalid is returned,
// so the verdict does not depend on the order of txHashes or on read timing. The
// per-transaction loop returned whichever error its order reached first.
func (u *Server) checkCounterConflictingOnCurrentChainBatch(ctx context.Context, txHashes []chainhash.Hash, blockIds map[uint32]bool, missingPolicy missingRecordPolicy) (notFound []chainhash.Hash, err error) {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "checkCounterConflictingOnCurrentChainBatch")
	defer deferFn()

	txs, notFound, counterSpenders, spentAgainst, err := u.counterSpendersBatch(ctx, txHashes)
	if err != nil {
		return nil, err
	}

	if len(notFound) > 0 && missingPolicy == failOnMissingRecords {
		return nil, missingRecordsError("tx or parent record", notFound)
	}

	if len(txs) == 0 {
		return notFound, nil
	}

	cones, missing, err := utxo.GetConflictingChildrenFromRoots(ctx, u.utxoStore, [][]chainhash.Hash{counterSpenders, txs}, u.settings.UtxoStore.ConflictingChildrenMaxNodes)
	if err != nil {
		return nil, errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch] failed to walk the cones of %d transaction(s) and %d counter-spender(s)", len(txs), len(counterSpenders), err)
	}

	counterCone, ownCone := cones[0], cones[1]

	// The per-transaction check compares some positions against
	// CoinbasePlaceholderHashValue and others against FrozenBytesTxHash. go-subtree
	// defines both as the same all-0xFF hash, so one comparison per cone covers
	// every position; Test_frozenSentinelsAreOneValue pins that.
	for _, childHash := range counterCone {
		if childHash.Equal(subtreepkg.FrozenBytesTxHash) {
			return nil, errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch] a counter-conflicting tx has frozen child")
		}
	}

	for _, childHash := range ownCone {
		if childHash.Equal(subtreepkg.FrozenBytesTxHash) {
			return nil, errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch] child transaction is frozen")
		}
	}

	if len(missing) > 0 && missingPolicy == failOnMissingRecords {
		return nil, missingRecordsError("walked record", missing)
	}

	counterConflicting := make(map[chainhash.Hash]struct{}, len(txs)+len(counterSpenders)+len(counterCone))
	for _, hashes := range [][]chainhash.Hash{txs, counterSpenders, counterCone} {
		for _, hash := range hashes {
			counterConflicting[hash] = struct{}{}
		}
	}

	if len(counterConflicting) > conflictingConeWarnThreshold {
		u.logger.Warnf("[checkCounterConflictingOnCurrentChainBatch] counter-conflicting set of %d transaction(s) is %d transactions", len(txs), len(counterConflicting))
	}

	minedHash, err := u.firstMinedOnChain(ctx, counterConflicting, blockIds, missingPolicy)
	if err != nil {
		return nil, err
	}

	if minedHash != nil {
		// Name the conflicting tx that condemns the block where that is cheap: the
		// mined tx is one of the conflicting txs, or the direct counter-spender of
		// one. A tx found deeper in a counter-spender's cone is named alone; the
		// walk does not record which root reached each node.
		if slices.Contains(txs, *minedHash) {
			return nil, errors.NewTxInvalidError("[checkCounterConflictingOnCurrentChainBatch][%s] conflicting transaction is already mined on our chain", minedHash.String())
		}

		if conflictingTx, ok := spentAgainst[*minedHash]; ok {
			return nil, errors.NewTxInvalidError("[checkCounterConflictingOnCurrentChainBatch][%s] counter-conflicting transaction %s is already mined on our chain", conflictingTx.String(), minedHash.String())
		}

		return nil, errors.NewTxInvalidError("[checkCounterConflictingOnCurrentChainBatch][%s] counter-conflicting transaction is already mined on our chain", minedHash.String())
	}

	return notFound, nil
}

// counterSpendersBatch is phase 1 of checkCounterConflictingOnCurrentChainBatch.
// It returns the deduplicated transactions whose records and parent records were
// all found, those that were not, and the unique counter-spenders of the found
// ones, each derived as utxo.GetCounterConflictingTxHashes derives them.
// spentAgainst maps each counter-spender to the first transaction, in txHashes
// order, that it is a counter-spender of.
func (u *Server) counterSpendersBatch(ctx context.Context, txHashes []chainhash.Hash) (txs, notFound, counterSpenders []chainhash.Hash, spentAgainst map[chainhash.Hash]chainhash.Hash, err error) {
	unique := make([]chainhash.Hash, 0, len(txHashes))
	seen := make(map[chainhash.Hash]struct{}, len(txHashes))

	for _, txHash := range txHashes {
		if _, ok := seen[txHash]; !ok {
			seen[txHash] = struct{}{}
			unique = append(unique, txHash)
		}
	}

	// Only the parent hashes are read, so ask for the inpoints rather than the
	// whole transaction (see GetCounterConflictingTxHashes).
	txMetas, err := u.getBatch(ctx, unique, fields.TxInpoints)
	if err != nil {
		return nil, nil, nil, nil, errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch] failed to get the inpoints of %d transaction(s)", len(unique), err)
	}

	parentIdx := make(map[chainhash.Hash]int)
	parents := make([]chainhash.Hash, 0, len(unique))

	for _, txMeta := range txMetas {
		if txMeta == nil {
			continue
		}

		for _, parentHash := range txMeta.TxInpoints.GetParentTxHashes() {
			if _, ok := parentIdx[parentHash]; !ok {
				parentIdx[parentHash] = len(parents)
				parents = append(parents, parentHash)
			}
		}
	}

	parentMetas, err := u.getBatch(ctx, parents, fields.Utxos)
	if err != nil {
		return nil, nil, nil, nil, errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch] failed to get the spenders of %d parent transaction(s)", len(parents), err)
	}

	spentAgainst = make(map[chainhash.Hash]chainhash.Hash)

	for i, txHash := range unique {
		txMeta := txMetas[i]
		if txMeta == nil {
			notFound = append(notFound, txHash)
			continue
		}

		// A parent the store does not hold says nothing about who spent its
		// outputs, so the transaction's counter-spenders cannot be derived.
		parentMissing := false

		for _, parentHash := range txMeta.TxInpoints.GetParentTxHashes() {
			if parentMetas[parentIdx[parentHash]] == nil {
				parentMissing = true
				break
			}
		}

		if parentMissing {
			notFound = append(notFound, txHash)
			continue
		}

		for _, inpoint := range txMeta.TxInpoints.GetTxInpoints() {
			spendingDatas := parentMetas[parentIdx[inpoint.Hash]].SpendingDatas

			// check the length of the spending txs, if it's less than the index, then the input is not spent
			if len(spendingDatas) <= int(inpoint.Index) {
				return nil, nil, nil, nil, errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch][%s] cannot process counter conflicting, input %d of %s is out of range (len: %d)", txHash.String(), inpoint.Index, inpoint.Hash.String(), len(spendingDatas))
			}

			spendingData := spendingDatas[inpoint.Index]
			if spendingData == nil || spendingData.TxID == nil {
				continue
			}

			// if a transaction is frozen, the counter-transaction will be the same as the coinbase placeholder
			if spendingData.TxID.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				return nil, nil, nil, nil, errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch][%s] counter conflicting tx is frozen", txHash.String())
			}

			if _, ok := spentAgainst[*spendingData.TxID]; !ok {
				spentAgainst[*spendingData.TxID] = txHash
				counterSpenders = append(counterSpenders, *spendingData.TxID)
			}
		}

		txs = append(txs, txHash)
	}

	return txs, notFound, counterSpenders, spentAgainst, nil
}

// missingRecordsError is the failOnMissingRecords verdict: a processing error, so
// it is retried, wrapping ErrTxNotFound, as the per-transaction check returns it.
func missingRecordsError(what string, hashes []chainhash.Hash) error {
	return errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch] cannot check counter-conflicting transactions",
		errors.NewTxNotFoundError("[checkCounterConflictingOnCurrentChainBatch][%s] %s not found, %d record(s) missing", hashes[0].String(), what, len(hashes)))
}

// getBatch reads hashes concurrently, index-aligned. A record the store does not
// hold — ErrTxNotFound, or a nil record as aerospike returns — is a nil entry;
// any other read error fails the batch.
func (u *Server) getBatch(ctx context.Context, hashes []chainhash.Hash, f ...fields.FieldName) ([]*meta.Data, error) {
	results := make([]*meta.Data, len(hashes))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(counterConflictingBatchFanOut)

	for i := range hashes {
		g.Go(func() error {
			txMeta, err := u.utxoStore.Get(gCtx, &hashes[i], f...)
			if err != nil {
				if errors.Is(err, errors.ErrTxNotFound) {
					return nil
				}

				return err
			}

			results[i] = txMeta

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return results, nil
}

// firstMinedOnChain is phase 3 of checkCounterConflictingOnCurrentChainBatch. It
// reads every member's block IDs and returns a member mined in a block of
// blockIds, or nil. A member the store does not hold counts as not mined under
// skipMissingRecords, and fails the check under failOnMissingRecords. Every read
// completes before a mined member is reported, so a store fault anywhere is
// returned in preference to it.
func (u *Server) firstMinedOnChain(ctx context.Context, members map[chainhash.Hash]struct{}, blockIds map[uint32]bool, missingPolicy missingRecordPolicy) (*chainhash.Hash, error) {
	var (
		mu    sync.Mutex
		mined *chainhash.Hash
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(counterConflictingBatchFanOut)

	for member := range members {
		g.Go(func() error {
			memberMeta := &meta.Data{}
			if err := u.utxoStore.GetMeta(gCtx, &member, memberMeta); err != nil {
				if errors.Is(err, errors.ErrTxNotFound) {
					if missingPolicy == failOnMissingRecords {
						return missingRecordsError("counter-conflicting record", []chainhash.Hash{member})
					}

					return nil
				}

				return errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch][%s] failed to get counter conflicting tx meta", member.String(), err)
			}

			for _, blockID := range memberMeta.BlockIDs {
				if blockIds[blockID] {
					mu.Lock()
					// keep the lowest hash, so the reported member does not depend on read timing
					if mined == nil || member.String() < mined.String() {
						mined = &member
					}
					mu.Unlock()

					break
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return mined, nil
}

// conflictingTxCollector gathers the conflicting transactions that concurrent
// processTxMetaUsingStore batches decorate, with their txMetaSlice index, so they
// can be checked in one batch once every batch is done.
type conflictingTxCollector struct {
	mu      sync.Mutex
	hashes  []chainhash.Hash
	indexes []int
}

// add records the conflicting tx at txMetaSlice[idx]. Safe for concurrent use.
func (c *conflictingTxCollector) add(hash chainhash.Hash, idx int) {
	c.mu.Lock()
	c.hashes = append(c.hashes, hash)
	c.indexes = append(c.indexes, idx)
	c.mu.Unlock()
}

// settleCollectedConflictingTxs ends processTxMetaUsingStore: given the error its
// decorate batches returned, it returns the error the call must return.
//
// It upholds one invariant: an entry processTxMetaUsingStore leaves set has
// passed the counter-conflicting check. A set entry is never decorated again, so
// a conflicting tx left set without a verdict would be accepted on the retry.
//
//   - What was collected is checked even when a batch failed, on ctx rather than
//     the batches' cancelled group context. An ErrTxInvalid verdict is final and
//     stronger than any batch failure (a fail-fast threshold, say), so it is
//     returned in preference; any other check failure only replaces a nil.
//   - On any error, every collected entry is cleared, so the retry decorates and
//     checks it again.
//
// Missing records fail the check (failOnMissingRecords), as the per-transaction
// check did on this path and as blessMissingTransaction does.
func (u *Server) settleCollectedConflictingTxs(ctx context.Context, batchErr error, conflicting *conflictingTxCollector, txMetaSlice []metaSliceItem, blockIds map[uint32]bool) error {
	err := batchErr

	if len(conflicting.hashes) > 0 {
		if _, checkErr := u.checkCounterConflictingOnCurrentChainBatch(ctx, conflicting.hashes, blockIds, failOnMissingRecords); checkErr != nil {
			checkErr = errors.NewProcessingError("[processTxMetaUsingStore] failed to check %d counter conflicting tx(s) on current chain", len(conflicting.hashes), checkErr)

			if err == nil || errors.Is(checkErr, errors.ErrTxInvalid) {
				err = checkErr
			}
		}
	}

	if err != nil {
		for _, idx := range conflicting.indexes {
			txMetaSlice[idx] = metaSliceItem{}
		}
	}

	return err
}
