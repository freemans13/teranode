package blockassembly

import (
	"context"
	"database/sql"
	"encoding/binary"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// BestTipSeqStateKey is the key under which block assembly persists its replay position, the
// number of the last change of the best tip it applied. It is kept apart from StateKey so an
// older binary, which decodes that blob strictly, can still start after a rollback.
const BestTipSeqStateKey = "BlockAssemblerBestTipSeq"

const (
	// bestTipChangesPageSize is how many tip changes one GetBestTipChanges call returns.
	bestTipChangesPageSize = 1000

	// maxBestTipReplayFailures is how many passes in a row the replay may fail before block
	// assembly follows the best header once instead, so a change it cannot apply does not
	// hold it off the tip for ever.
	maxBestTipReplayFailures = 3

	// bestTipSeqProbeInterval is how often block assembly asks a blockchain service that did
	// not number its tip changes whether it does now, as it will once it is upgraded.
	bestTipSeqProbeInterval = time.Minute
)

// errBestTipReset reports that applying a tip change ran a full reset, which leaves block
// assembly on the chain's current best tip rather than on the change's block.
var errBestTipReset = errors.NewBlockAssemblyResetError("reset while replaying a change of the best tip")

// EncodeBestTipSeq serialises a replay position for BestTipSeqStateKey.
func EncodeBestTipSeq(seq uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, seq)

	return b
}

// DecodeBestTipSeq parses a replay position written by EncodeBestTipSeq.
func DecodeBestTipSeq(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, errors.NewProcessingError("invalid block assembly best tip position: expected 8 bytes, got %d", len(data))
	}

	return binary.LittleEndian.Uint64(data), nil
}

// loadBestTipSeq reads the replay position at start-up. A persisted position is used as it
// is: the changes made while block assembly was down are after it. Without one, block
// assembly starts at the latest change when it is on the best tip, since there is nothing to
// replay, and at 0 otherwise, where the height floor in replayBestTipChanges limits the
// replay to recent changes. If the blockchain service does not number its tip changes the
// position stays unknown, and a wake-up retries after bestTipSeqProbeInterval.
func (b *BlockAssembler) loadBestTipSeq(ctx context.Context) {
	latest, err := b.blockchainClient.GetBestTipSeq(ctx)
	if err != nil {
		b.lastBestTipSeqProbe.Store(time.Now().UnixNano())

		if b.bestTipSeqUnsupportedLogged.CompareAndSwap(false, true) {
			b.logger.Warnf("[BlockAssembler] blockchain service does not number tip changes, following the best header and asking again every %s: %v", bestTipSeqProbeInterval, err)
		} else {
			b.logger.Debugf("[BlockAssembler] blockchain service still does not number tip changes: %v", err)
		}

		return
	}

	var seq uint64

	data, err := b.blockchainClient.GetState(ctx, BestTipSeqStateKey)

	switch {
	case err == nil:
		if seq, err = DecodeBestTipSeq(data); err != nil {
			b.logger.Warnf("[BlockAssembler] ignoring persisted best tip position: %v", err)
			seq = 0
		}
	case errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), sql.ErrNoRows.Error()):
		seq = b.startingBestTipSeq(ctx, latest)
	default:
		b.logger.Warnf("[BlockAssembler] error reading best tip position, following the best header until it can be read: %v", err)
		return
	}

	b.bestTipSeq.Store(seq)
	b.bestTipSeqSaved.Store(seq)
	b.bestTipSeqKnown.Store(true)

	b.logger.Infof("[BlockAssembler] best tip replay position %d", seq)
}

// followBestTipChanges runs one replay pass and reports whether the caller should still move to
// the best header. It does when the blockchain service does not number its tip changes, when the
// replay has failed maxBestTipReplayFailures passes in a row, and when the pass found no change
// although block assembly is not on the best tip, which no change of the tip should ever leave.
func (b *BlockAssembler) followBestTipChanges(ctx context.Context) bool {
	if !b.bestTipSeqKnown.Load() {
		if time.Since(time.Unix(0, b.lastBestTipSeqProbe.Load())) < bestTipSeqProbeInterval {
			return true
		}

		b.loadBestTipSeq(ctx)

		if !b.bestTipSeqKnown.Load() {
			return true
		}
	}

	found, err := b.replayBestTipChanges(ctx)
	if err != nil {
		failures := b.bestTipReplayFailures.Add(1)
		prometheusBlockAssemblyProcessingStuck.WithLabelValues("best_tip_replay").Inc()

		if failures < maxBestTipReplayFailures {
			b.logger.Errorf("[BlockAssembler] replay of best tip changes failed (%d in a row), will retry: %v", failures, err)
			return false
		}

		b.bestTipReplayFailures.Store(0)

		// The best-header path moves straight to the tip, so the changes before it are
		// given up. A change after this read is replayed on the next pass.
		//
		// That includes the history a reset left pending. The reset already rebuilt the
		// pool on its target, so giving the history up leaves the pool as it was before
		// the replay existed, and candidates are built from it again. Holding on would
		// keep every candidate empty for as long as the failure lasts.
		if b.bestTipResetReplayPending.Load() || b.bestTipResetIncomplete.Load() {
			b.logger.Errorf("[BlockAssembler] replay failed %d times in a row, giving up the history a reset left pending so mining candidates are built again: %v", failures, err)
			prometheusBlockAssemblyProcessingStuck.WithLabelValues("best_tip_reset_replay_abandoned").Inc()
			b.bestTipResetReplayPending.Store(false)
			b.bestTipResetIncomplete.Store(false)
		}

		b.logger.Errorf("[BlockAssembler] replay of best tip changes failed %d times in a row, following the best header instead: %v", failures, err)

		if seq, seqErr := b.blockchainClient.GetBestTipSeq(ctx); seqErr == nil {
			b.advanceBestTipSeq(seq)
		}

		return true
	}

	b.bestTipReplayFailures.Store(0)

	if found > 0 {
		return false
	}

	best, _, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		b.logger.Errorf("[BlockAssembler] error getting best block header after replay: %v", err)
		return false
	}

	tip, _ := b.CurrentBlock()
	if tip != nil && best.Hash().IsEqual(tip.Hash()) {
		return false
	}

	// A change that landed after the query above is replayed by its own notification. Look once
	// more before giving up on the replay, so that case does not take the best-header path.
	if found, err = b.replayBestTipChanges(ctx); err != nil || found > 0 {
		return false
	}

	b.logger.Warnf("[BlockAssembler] no recorded change of the best tip leads to the best block %s, following the best header", best.Hash())

	return true
}

// replayBestTipChanges applies, in order, every change of the best tip numbered after the
// position, and advances the position past each one it applies. It returns how many changes it
// read, and stops at the first change it fails to apply, leaving the position on the one before
// so the next pass retries it.
//
// A change is the block that became the best tip at that number. Moving to each in turn is what
// makes a branch that won and lost again before block assembly looked go forward and back
// through the subtree processor: its transactions are re-added ahead of any child spending them,
// its conflicts are swapped both ways, and its transactions are re-stamped unmined. A run of
// extensions is applied as one move to its last block, because the others are on its path.
//
// Changes below the height floor are skipped: retention bounds how far back block assembly can
// move, and a deeper move is a reset either way.
func (b *BlockAssembler) replayBestTipChanges(ctx context.Context) (int, error) {
	read := 0

	// A reset can finalize its target before its reload or state write fails.
	// Retry that rebuild before the hash-equality fast path can acknowledge it.
	if b.bestTipResetIncomplete.Load() {
		tip, height := b.CurrentBlock()
		if tip == nil {
			return read, errors.NewProcessingError("cannot retry incomplete reset without a tip")
		}
		_, meta, err := b.blockchainClient.GetBlockHeader(ctx, tip.Hash())
		if err != nil {
			return read, err
		}
		if meta.Invalid {
			err = b.resetAndRecord(ctx, true)
		} else {
			err = b.resetToBlockAndRecord(ctx, tip, height, true)
		}
		if err != nil {
			return read, err
		}
	}

	for {
		latest, err := b.blockchainClient.GetBestTipSeq(ctx)
		if err != nil {
			return read, errors.NewProcessingError("[replayBestTipChanges] error getting latest tip sequence", err)
		}

		_, tipHeight := b.CurrentBlock()

		headers, metas, seqs, err := b.blockchainClient.GetBestTipChanges(ctx, b.bestTipSeq.Load(), b.bestTipFloor(tipHeight), bestTipChangesPageSize)
		if err != nil {
			return read, errors.NewProcessingError("[replayBestTipChanges] error getting best tip changes", err)
		}

		read += len(headers)

		for i := range headers {
			// Skip a change the next one extends: moving to the next one passes through it.
			if i+1 < len(headers) && !metas[i+1].Invalid && headers[i+1].HashPrevBlock.IsEqual(headers[i].Hash()) {
				continue
			}

			// A server may cap a page below our requested size. Only the latest
			// sequence can represent a rewind to an ancestor of assembly's tip.
			last := seqs[i] >= latest

			if err = b.applyBestTipChange(ctx, headers[i], metas[i], last); err != nil && !errors.Is(err, errors.ErrBlockAssemblyReset) {
				// The position stays before this change, which the next pass retries. The
				// changes skipped above are on its path, so they are retried with it.
				return read, err
			}

			// Valid changes reset to their own target. Invalid changes reset after
			// validation has made their transactions recoverable.
			b.advanceBestTipSeq(seqs[i])
		}

		if len(headers) == 0 || b.bestTipSeq.Load() >= latest {
			b.bestTipResetReplayPending.Store(false)
			return read, nil
		}
	}
}

// applyBestTipChange moves block assembly to one change of the best tip. last says the change is
// the latest one, so the block is the best tip now.
//
// A change already on block assembly's chain is skipped unless it is the latest change. Moving
// back to it and forward again to a later change nets out, but when it is the latest change the
// best tip moved back to it, for example after an invalidation of block assembly's tip, and block
// assembly must move back too.
//
// A block that is invalid now is never moved to. If block assembly's chain contains it, a later
// change moves off it, through the reset rule for invalid blocks. If not, block assembly never
// applied it, and its block-only transactions reach the pool only through a reset, once block
// validation has stamped them unmined again, which it does when it processes the invalidation.
func (b *BlockAssembler) applyBestTipChange(ctx context.Context, header *model.BlockHeader, meta *model.BlockHeaderMeta, last bool) error {
	tip, tipHeight := b.CurrentBlock()
	if tip == nil {
		return errors.NewProcessingError("[applyBestTipChange] block assembly has no tip")
	}

	hash := header.Hash()

	if hash.IsEqual(tip.Hash()) {
		return nil
	}

	// A block extending the tip cannot be on its chain, so it needs no ancestry check.
	if !last && !header.HashPrevBlock.IsEqual(tip.Hash()) && meta.Height <= tipHeight {
		onChain, err := b.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{meta.ID}, tip.Hash())
		if err != nil {
			return errors.NewProcessingError("[applyBestTipChange] error checking whether %s is on block assembly's chain", hash, err)
		}

		if onChain {
			// Already applied, for example by a reset that went straight to a later tip, and
			// a later change moves block assembly on from it.
			return nil
		}
	}

	if meta.Invalid {
		b.logger.Warnf("[BlockAssembler] best tip change %s is invalid now and was never applied, resetting so its transactions are reloaded", hash)

		if err := b.waitForBlockMinedSet(ctx, hash); err != nil {
			if ctx.Err() != nil {
				return err
			}

			b.logger.Warnf("[BlockAssembler] gave up waiting for invalid block %s to be processed: %v (resetting anyway)", hash, err)
		}

		if err := b.resetAndRecord(ctx, false); err != nil {
			return errors.NewProcessingError("[applyBestTipChange] error resetting after invalid block %s", hash, err)
		}

		return errBestTipReset
	}

	if !last && !header.HashPrevBlock.IsEqual(tip.Hash()) {
		restamped, err := b.restampDeepStaleBranch(ctx, header, meta)
		if err != nil || restamped {
			return err
		}
	}

	b.logger.Infof("[BlockAssembler] replaying best tip change %d: %s", meta.Height, hash)

	return b.moveToBlock(ctx, header, meta)
}

// restampDeepStaleBranch handles a past change whose move would take block assembly back over
// blocks of the current best chain whose coinbases may already be spent. It reports whether it
// handled the change, in which case block assembly has not moved.
//
// Moving back a block deletes every child spend of its coinbase (removeCoinbaseUtxos). On a real
// reorg those spends are on the losing side. Here the blocks moved back are on the chain that
// won, and moving forward again recreates only the coinbases, so confirmed transactions would be
// lost and the coinbases would read as unspent. A coinbase can be spent from coinbase maturity
// blocks above it, so a move back is safe while the best tip is less than maturity blocks above
// the lowest block it moves back. That always holds for a change replayed from the change before
// it, which is the move the chain itself made. It can fail when block assembly is ahead of the
// change: after a reset straight to the best tip, after a crash late in a long pass, or on a
// start without a persisted position.
//
// Such a change is applied at its own depth instead. Block assembly never applied the stale
// branch, so it never swapped the branch's conflicts or created its coinbases, and what is left to
// undo is the mined mark block validation gave the branch's transactions. restampStaleBranch
// clears that mark and reloads the pool on block assembly's current tip.
func (b *BlockAssembler) restampDeepStaleBranch(ctx context.Context, header *model.BlockHeader, meta *model.BlockHeaderMeta) (bool, error) {
	moveBack, moveForward, err := b.getReorgBlockHeaders(ctx, header, meta.Height)
	if err != nil {
		return false, errors.NewProcessingError("[restampDeepStaleBranch] error getting the path to %s", header.Hash(), err)
	}

	if len(moveBack) == 0 {
		return false, nil
	}

	best, bestMeta, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return false, errors.NewProcessingError("[restampDeepStaleBranch] error getting best block header", err)
	}

	lowest := moveBack[len(moveBack)-1].meta
	maturity := uint64(b.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec // maturity is a small positive chain parameter

	// Spendable from height lowest+maturity, and the next block is bestHeight+1.
	if uint64(bestMeta.Height)+1 < uint64(lowest.Height)+maturity {
		return false, nil
	}

	onBest, err := b.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{lowest.ID}, best.Hash())
	if err != nil {
		return false, errors.NewProcessingError("[restampDeepStaleBranch] error checking whether %s is on the best chain", moveBack[len(moveBack)-1].header.Hash(), err)
	}

	if !onBest {
		// Every block moved back is on a branch that lost, so its coinbase spends lost too.
		return false, nil
	}

	b.logger.Warnf("[BlockAssembler] best tip change %d: %s is %d blocks below the best tip on a stale branch, restamping its %d blocks instead of moving back over the best chain", meta.Height, header.Hash(), bestMeta.Height-lowest.Height+1, len(moveForward))

	return true, b.restampStaleBranch(ctx, moveForward, best)
}

// restampStaleBranch marks every transaction of the given stale blocks that no block of the best
// chain contains as not on the longest chain, then reloads the pool on block assembly's tip so
// those transactions are in it. A transaction the UTXO store has pruned is left out: it has no
// record to mark. Invalid blocks are left to block validation, which unmarks their transactions
// when it processes the invalidation.
func (b *BlockAssembler) restampStaleBranch(ctx context.Context, branch []blockHeaderWithMeta, best *model.BlockHeader) error {
	staleIDs := make(map[uint32]struct{}, len(branch))
	hashes := make([]chainhash.Hash, 0, len(branch)*100)

	for _, hm := range branch {
		if hm.meta.Invalid {
			continue
		}

		b.heartbeat.BeatIfStarted()

		staleIDs[hm.meta.ID] = struct{}{}

		block, err := b.blockchainClient.GetBlock(ctx, hm.header.Hash())
		if err != nil {
			return errors.NewProcessingError("[restampStaleBranch] error getting block %s", hm.header.Hash(), err)
		}

		subtrees, err := block.GetSubtrees(ctx, b.logger, b.subtreeStore, b.settings.Block.GetAndValidateSubtreesConcurrency)
		if err != nil {
			return errors.NewProcessingError("[restampStaleBranch] error getting subtrees of block %s", hm.header.Hash(), err)
		}

		for _, st := range subtrees {
			for _, node := range st.Nodes {
				if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
					hashes = append(hashes, node.Hash)
				}
			}
		}
	}

	branchOnly, err := b.branchOnlyTxs(ctx, hashes, staleIDs, best)
	if err != nil {
		return err
	}

	if len(branchOnly) > 0 {
		b.heartbeat.BeatIfStarted()

		if err = b.utxoStore.MarkTransactionsOnLongestChain(ctx, branchOnly, false); err != nil {
			return errors.NewProcessingError("[restampStaleBranch] error marking %d stale branch transactions as not on the longest chain", len(branchOnly), err)
		}
	}

	b.logger.Infof("[BlockAssembler] restamped %d of %d stale branch transactions, reloading the pool", len(branchOnly), len(hashes))

	tip, height := b.CurrentBlock()

	return b.resetToBlockAndRecord(ctx, tip, height, true)
}

// branchOnlyTxs returns the hashes, in order, whose transaction no block of the best chain
// contains. A hash the UTXO store has no record of (pruned) is left out.
func (b *BlockAssembler) branchOnlyTxs(ctx context.Context, hashes []chainhash.Hash, staleIDs map[uint32]struct{}, best *model.BlockHeader) ([]chainhash.Hash, error) {
	kept := make([]chainhash.Hash, 0, len(hashes))
	onBest := make(map[uint32]bool)

	for start := 0; start < len(hashes); start += resetExistenceChunkSize {
		b.heartbeat.BeatIfStarted()

		chunk := hashes[start:min(start+resetExistenceChunkSize, len(hashes))]

		unresolved := make([]*utxo.UnresolvedMetaData, len(chunk))
		for i := range chunk {
			unresolved[i] = &utxo.UnresolvedMetaData{Hash: chunk[i], Idx: i}
		}

		if err := b.utxoStore.BatchDecorate(ctx, unresolved, fields.BlockIDs); err != nil {
			return nil, errors.NewProcessingError("[branchOnlyTxs] error reading block ids", err)
		}

	next:
		for i, u := range unresolved {
			switch {
			case errors.Is(u.Err, errors.ErrTxNotFound):
				continue
			case u.Err != nil:
				return nil, errors.NewProcessingError("[branchOnlyTxs] error reading block ids of %s", chunk[i], u.Err)
			}

			if u.Data != nil {
				for _, id := range u.Data.BlockIDs {
					if _, stale := staleIDs[id]; stale {
						continue
					}

					on, known := onBest[id]
					if !known {
						var err error
						if on, err = b.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{id}, best.Hash()); err != nil {
							return nil, errors.NewProcessingError("[branchOnlyTxs] error checking whether block %d is on the best chain", id, err)
						}

						onBest[id] = on
					}

					if on {
						continue next
					}
				}
			}

			kept = append(kept, chunk[i])
		}
	}

	return kept, nil
}

// startingBestTipSeq is the replay position without a persisted one. On the best tip, block
// assembly starts at latest: replaying older changes would move it onto every stale branch
// recorded within the floor and back again. Off it, it starts at 0, and the replay moves it to
// the tip through the recent changes.
func (b *BlockAssembler) startingBestTipSeq(ctx context.Context, latest uint64) uint64 {
	tip, _ := b.CurrentBlock()

	best, _, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err == nil && tip != nil && best.Hash().IsEqual(tip.Hash()) {
		b.logger.Infof("[BlockAssembler] no persisted best tip position, block assembly is on the best tip, starting at change %d", latest)
		return latest
	}

	b.logger.Infof("[BlockAssembler] no persisted best tip position, replaying recent tip changes")

	return 0
}

// idleReconcileNeeded reports whether the idle tick should start a replay: when a change of
// the best tip arrived after the replay position, or block assembly is not on the best tip.
// It runs outside StateReconciling, so a mining candidate asked for during the check is
// built as usual. It stamps lastBestTipReplay, so the next check waits a full interval.
func (b *BlockAssembler) idleReconcileNeeded(ctx context.Context) bool {
	b.lastBestTipReplay.Store(time.Now().UnixNano())
	if b.bestTipResetReplayPending.Load() {
		return true
	}

	if b.bestTipSeqKnown.Load() {
		if seq, err := b.blockchainClient.GetBestTipSeq(ctx); err == nil && seq > b.bestTipSeq.Load() {
			return true
		}
	}

	best, _, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		// The replay reports the error.
		return true
	}

	tip, _ := b.CurrentBlock()

	return tip == nil || !best.Hash().IsEqual(tip.Hash())
}

// bestTipFloor is the lowest height of a change the replay reads, the tip minus the UTXO
// retention. With retention 0 (pruning off) nothing is skipped.
func (b *BlockAssembler) bestTipFloor(tipHeight uint32) uint32 {
	retention := b.settings.GetUtxoStoreBlockHeightRetention()
	if retention == 0 || tipHeight <= retention {
		return 0
	}

	return tipHeight - retention
}

// advanceBestTipSeq moves the replay position forward to seq. It never moves it back.
func (b *BlockAssembler) advanceBestTipSeq(seq uint64) {
	for {
		current := b.bestTipSeq.Load()
		if seq <= current || b.bestTipSeq.CompareAndSwap(current, seq) {
			return
		}
	}
}

// returnToRunning ends a block-processing state and then persists the replay position. The save
// is one write to the blockchain service, and a mining candidate asked for during a processing
// state is an empty one, so the save waits until block assembly is running again.
func (b *BlockAssembler) returnToRunning(ctx context.Context) {
	b.setCurrentRunningState(StateRunning)
	b.saveBestTipSeq(ctx)
}

// saveBestTipSeq persists the replay position when it moved since the last save. A failed or
// missed save only costs a restart replaying changes block assembly already applied, each of
// which is then a no-op, so it is logged and not returned.
func (b *BlockAssembler) saveBestTipSeq(ctx context.Context) {
	if !b.bestTipSeqKnown.Load() {
		return
	}

	seq := b.bestTipSeq.Load()
	if seq == b.bestTipSeqSaved.Load() {
		return
	}

	if err := b.blockchainClient.SetState(ctx, BestTipSeqStateKey, EncodeBestTipSeq(seq)); err != nil {
		if !errors.Is(err, context.Canceled) {
			b.logger.Errorf("[BlockAssembler] error saving best tip position %d: %v", seq, err)
		}

		return
	}

	b.bestTipSeqSaved.Store(seq)
}

// reconcileDue reports whether the idle tick should wake a replay: none has run for
// blockassembly_reconcileInterval. It covers lost notifications, because the blockchain
// service drops a subscriber whose buffer fills and nothing arrives until it reconnects.
func (b *BlockAssembler) reconcileDue(now time.Time) bool {
	// Recovery of a reset that already discarded its pool is mandatory even
	// when ordinary lost-notification reconciliation is disabled.
	if b.bestTipResetReplayPending.Load() {
		return true
	}

	interval := b.settings.BlockAssembly.ReconcileInterval
	if interval <= 0 {
		return false
	}

	return now.Sub(time.Unix(0, b.lastBestTipReplay.Load())) >= interval
}
