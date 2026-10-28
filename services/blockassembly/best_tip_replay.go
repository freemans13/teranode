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
// is: the changes made while block assembly was down are after it. Without one the position
// is 0, and the height floor in replayBestTipChanges limits the replay to recent changes, of
// which every one on block assembly's own chain is a no-op. If the blockchain service does
// not number its tip changes the position stays unknown, and the next wake-ups retry.
func (b *BlockAssembler) loadBestTipSeq(ctx context.Context) {
	if _, err := b.blockchainClient.GetBestTipSeq(ctx); err != nil {
		b.logger.Warnf("[BlockAssembler] blockchain service does not number tip changes, following the best header: %v", err)
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
		b.logger.Infof("[BlockAssembler] no persisted best tip position, replaying recent tip changes")
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

	for {
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

			if err = b.applyBestTipChange(ctx, headers[i], metas[i]); err != nil && !errors.Is(err, errors.ErrBlockAssemblyReset) {
				// The position stays before this change, which the next pass retries. The
				// changes skipped above are on its path, so they are retried with it.
				return read, err
			}

			// A reset left block assembly on the current best tip, and the changes after
			// this one are replayed from there.
			b.advanceBestTipSeq(seqs[i])
		}

		if len(headers) < bestTipChangesPageSize {
			return read, nil
		}
	}
}

// applyBestTipChange moves block assembly to one change of the best tip.
//
// A block that is invalid now is never moved to. If block assembly's chain contains it, a later
// change moves off it, through the reset rule for invalid blocks. If not, block assembly never
// applied it, and its block-only transactions reach the pool only through a reset, once block
// validation has stamped them unmined again, which it does when it processes the invalidation.
func (b *BlockAssembler) applyBestTipChange(ctx context.Context, header *model.BlockHeader, meta *model.BlockHeaderMeta) error {
	tip, tipHeight := b.CurrentBlock()
	if tip == nil {
		return errors.NewProcessingError("[applyBestTipChange] block assembly has no tip")
	}

	hash := header.Hash()

	if hash.IsEqual(tip.Hash()) {
		return nil
	}

	// A block extending the tip cannot be on its chain, so it needs no ancestry check.
	if !header.HashPrevBlock.IsEqual(tip.Hash()) && meta.Height <= tipHeight {
		onChain, err := b.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{meta.ID}, tip.Hash())
		if err != nil {
			return errors.NewProcessingError("[applyBestTipChange] error checking whether %s is on block assembly's chain", hash, err)
		}

		if onChain {
			// Already applied, for example by a reset that went straight to a later tip.
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

// bestTipSeqBeforeReset reads the latest number before a reset that block assembly did not start
// from a tip change. The reset moves to the best tip as it is when it reads it, which is at least
// that change, so the position can move up to it afterwards.
func (b *BlockAssembler) bestTipSeqBeforeReset(ctx context.Context) (uint64, bool) {
	if !b.bestTipSeqKnown.Load() {
		return 0, false
	}

	seq, err := b.blockchainClient.GetBestTipSeq(ctx)
	if err != nil {
		b.logger.Warnf("[BlockAssembler] error reading best tip position before reset: %v", err)
		return 0, false
	}

	return seq, true
}

// reconcileDue reports whether the idle tick should wake a replay: none has run for
// blockassembly_reconcileInterval. It covers lost notifications, because the blockchain
// service drops a subscriber whose buffer fills and nothing arrives until it reconnects.
func (b *BlockAssembler) reconcileDue(now time.Time) bool {
	interval := b.settings.BlockAssembly.ReconcileInterval
	if interval <= 0 {
		return false
	}

	return now.Sub(time.Unix(0, b.lastBestTipReplay.Load())) >= interval
}
