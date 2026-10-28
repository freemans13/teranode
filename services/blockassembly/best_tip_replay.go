package blockassembly

import (
	"context"
	"database/sql"
	"encoding/binary"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
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

		b.logger.Errorf("[BlockAssembler] replay of best tip changes failed %d times in a row, following the best header instead: %v", failures, err)
		b.bestTipReplayFailures.Store(0)

		// The best-header path moves straight to the tip, so the changes before it are
		// given up. A change after this read is replayed on the next pass.
		if b.bestTipResetReplayPending.Load() {
			return false
		}

		if seq, seqErr := b.blockchainClient.GetBestTipSeq(ctx); seqErr == nil {
			b.advanceBestTipSeq(seq)
			b.saveBestTipSeq(ctx)
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

	defer b.saveBestTipSeq(ctx)

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

	b.logger.Infof("[BlockAssembler] replaying best tip change %d: %s", meta.Height, hash)

	return b.moveToBlock(ctx, header, meta)
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

// saveBestTipSeq persists the replay position when it moved since the last save. A failed save
// only costs a restart replaying changes block assembly already applied, each of which is then a
// no-op, so it is logged and not returned.
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
