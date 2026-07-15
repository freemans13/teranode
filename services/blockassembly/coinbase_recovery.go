package blockassembly

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// canonicalCoinbaseAt reports whether block assembly's UTXO store holds the
// canonical coinbase transaction for the given height. It returns the
// canonical block itself so callers (the repair path added in later tasks)
// can reuse its CoinbaseTx without re-fetching from the blockchain client.
func (b *BlockAssembler) canonicalCoinbaseAt(ctx context.Context, height uint32) (present bool, canonicalBlock *model.Block, err error) {
	blk, err := b.blockchainClient.GetBlockByHeight(ctx, height)
	if err != nil {
		return false, nil, errors.NewProcessingError("[coinbaseRecovery] cannot get canonical block at height %d", height, err)
	}

	if blk == nil || blk.CoinbaseTx == nil {
		return false, nil, errors.NewProcessingError("[coinbaseRecovery] canonical block at height %d has no coinbase", height)
	}

	txMeta, err := b.utxoStore.Get(ctx, blk.CoinbaseTx.TxIDChainHash(), fields.Tx)
	if err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			return false, blk, nil
		}

		return false, blk, errors.NewProcessingError("[coinbaseRecovery] error checking coinbase at height %d", height, err)
	}

	if txMeta == nil || txMeta.Tx == nil {
		return false, blk, nil
	}

	return true, blk, nil
}

// errCoinbaseGapTooLarge is returned by scopeCoinbaseGap when the walk-back
// from the trigger height exceeds CoinbaseRecoveryMaxGapBlocks without
// finding a proven-good floor. The orchestrator (Task 8) treats this as a
// non-local divergence and escalates to halt+alarm rather than auto-repairing.
var errCoinbaseGapTooLarge = errors.NewProcessingError("[coinbaseRecovery] gap exceeds max recoverable blocks")

// scopeCoinbaseGap walks back from triggerHeight, collecting canonical
// blocks whose coinbase is absent from the UTXO store, until it has seen
// CoinbaseRecoveryConsecutiveGood *consecutive* present coinbases -- proving
// a good floor beneath the gap. The consecutive-good requirement (rather
// than stopping at the first present coinbase) exists because the
// fast-forward create loop is concurrent and can leave a present coinbase
// sitting above still-missing ones (a "hole"); stopping early there would
// under-scope the repair.
//
// The walk is bounded by CoinbaseRecoveryMaxGapBlocks: exceeding it returns
// errCoinbaseGapTooLarge so the caller can escalate instead of attempting to
// repair an unbounded (and likely non-local) divergence.
//
// gapBlocks is returned in ascending height order. An empty slice with a nil
// error means no gap was found (the trigger height itself already satisfies
// the consecutive-good floor).
func (b *BlockAssembler) scopeCoinbaseGap(ctx context.Context, triggerHeight uint32) (gapBlocks []*model.Block, err error) {
	needConsecutive := b.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood
	maxGap := b.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks

	var gap []*model.Block

	consecutiveGood := 0
	scanned := 0

	for h := int64(triggerHeight); h >= 0; h-- {
		present, blk, err := b.canonicalCoinbaseAt(ctx, uint32(h))
		if err != nil {
			return nil, err
		}

		if present {
			consecutiveGood++
			if consecutiveGood >= needConsecutive {
				break
			}

			continue
		}

		consecutiveGood = 0
		gap = append(gap, blk)
		scanned++

		if scanned > maxGap {
			return nil, errCoinbaseGapTooLarge
		}
	}

	// gap was accumulated walking toward genesis (descending height);
	// reverse it so callers get a deterministic ascending-height repair order.
	for i, j := 0, len(gap)-1; i < j; i, j = i+1, j-1 {
		gap[i], gap[j] = gap[j], gap[i]
	}

	return gap, nil
}

// recoverCoinbaseDivergence is the staged orchestration for a detected
// coinbase divergence at triggerHeight. It scopes the gap (scopeCoinbaseGap),
// guards each gap block against double-spend conflicts (HasConflictingNodes),
// and -- only when the gap is conflict-free -- attempts a coinbase-only
// repair (subtreeProcessor.ReconcileCoinbases), retrying up to
// CoinbaseRecoveryMaxAttempts times.
//
// Any scoping failure (gap too large, or a canonicalCoinbaseAt error while
// walking back) is treated identically to a conflict: both mean coinbase-only
// repair cannot safely proceed, so both escalate to Stage 2 (halt + alarm)
// rather than being distinguished and handled differently.
//
// On successful repair this returns nil and increments the "repaired" metric
// outcome. On exhaustion of attempts, an over-large gap, or conflicting
// nodes in the gap, it increments "escalated", logs a single loud
// operator-facing line naming resetblockassembly, and returns an error --
// the caller must not advance past triggerHeight until an operator
// intervenes.
func (b *BlockAssembler) recoverCoinbaseDivergence(ctx context.Context, triggerHeight uint32) error {
	prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected").Inc()

	maxAttempts := b.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		gap, err := b.scopeCoinbaseGap(ctx, triggerHeight)
		if err != nil {
			// Gap too large, or a canonicalCoinbaseAt failure during the
			// walk-back -- either way, coinbase-only repair cannot safely
			// proceed, so escalate rather than retry.
			lastErr = err
			break
		}

		if len(gap) == 0 {
			return nil // nothing to do (already consistent)
		}

		// Conflict-aware guard: coinbase-only repair is only sufficient when
		// the gap carries no double-spend conflicts to resolve.
		conflicted := false

		for _, blk := range gap {
			has, cErr := b.subtreeProcessor.HasConflictingNodes(ctx, blk)
			if cErr != nil {
				lastErr = cErr
				conflicted = true

				break
			}

			if has {
				lastErr = errors.NewProcessingError("[coinbaseRecovery] gap block %s has conflicting txs; coinbase-only repair insufficient", blk.String())
				conflicted = true

				break
			}
		}

		if conflicted {
			break // escalate
		}

		if err := b.subtreeProcessor.ReconcileCoinbases(ctx, gap); err != nil {
			lastErr = err
			b.logger.Warnf("[coinbaseRecovery] attempt %d/%d failed: %v", attempt, maxAttempts, err)

			continue
		}

		b.logger.Infof("[coinbaseRecovery] repaired %d coinbase(s) up to height %d on attempt %d", len(gap), triggerHeight, attempt)
		prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("repaired").Inc()

		return nil
	}

	// Stage 2: halt + alarm. Do not advance; surface a single loud operator signal.
	prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated").Inc()
	b.logger.Errorf("[coinbaseRecovery] MANUAL INTERVENTION REQUIRED: coinbase divergence at/below height %d could not be auto-repaired after %d attempts (%v); run resetblockassembly", triggerHeight, maxAttempts, lastErr)

	return errors.NewProcessingError("[coinbaseRecovery] unrecoverable coinbase divergence at height %d", triggerHeight, lastErr)
}

// checkCoinbaseDivergenceOnStart verifies the persisted tip's canonical coinbase
// exists in the store and repairs the gap if not. Called once during Start,
// before block-notification listeners begin, so a divergence created by a prior
// unclean shutdown is healed before the node advances.
func (b *BlockAssembler) checkCoinbaseDivergenceOnStart(ctx context.Context) error {
	header, height := b.CurrentBlock()
	if header == nil || height == 0 {
		return nil // genesis / unset — nothing to check
	}

	present, _, err := b.canonicalCoinbaseAt(ctx, height)
	if err != nil {
		// Non-fatal: log and continue; runtime detection remains as a backstop.
		b.logger.Warnf("[coinbaseRecovery] startup divergence check failed at height %d: %v", height, err)
		return nil
	}

	if present {
		return nil
	}

	b.logger.Warnf("[coinbaseRecovery] startup: canonical coinbase missing at tip height %d; running recovery", height)

	return b.recoverCoinbaseDivergence(ctx, height)
}
