package blockassembly

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// canonicalCoinbaseAt reports whether block assembly's UTXO store holds the
// canonical coinbase transaction for the given height. It returns the
// canonical block itself so the repair path can reuse its CoinbaseTx without
// re-fetching from the blockchain client.
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
// finding a proven-good floor. The orchestrator treats this as a non-local
// divergence and escalates to halt+alarm rather than auto-repairing.
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
// The walk stops at height 1: the genesis coinbase is provably unspendable and
// is never created in the UTXO store, so probing height 0 would always report
// it as "missing" and pull the genesis block into the repair set, where
// processCoinbaseUtxos would write a bogus UTXO entry (block ID 0, and a
// maturity height taken from the store's current height rather than 0).
// Genesis can never be the parent of a coinbase-maturity spend, so excluding
// it loses nothing.
//
// gapBlocks is returned in ascending height order. An empty slice with a nil
// error means no gap was found (the trigger height itself already satisfies
// the consecutive-good floor).
func (b *BlockAssembler) scopeCoinbaseGap(ctx context.Context, triggerHeight uint32) (gapBlocks []*model.Block, err error) {
	needConsecutive := b.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood
	if needConsecutive < 1 {
		// A non-positive setting would satisfy the floor test on the first
		// present coinbase, silently degrading the walk to the stop-at-first
		// behaviour this function exists to avoid.
		needConsecutive = 1
	}

	maxGap := b.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks
	if maxGap < 1 {
		// A non-positive setting would escalate on the very first missing
		// coinbase, making every divergence unrecoverable.
		maxGap = 1
	}

	var gap []*model.Block

	consecutiveGood := 0
	scanned := 0

	for h := int64(triggerHeight); h >= 1; h-- {
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
// coinbase divergence at triggerHeight. It is coinbase-only and O(blocks in
// the gap): it scopes the gap (scopeCoinbaseGap) and, when non-empty,
// attempts a coinbase-only repair (subtreeProcessor.ReconcileCoinbases),
// retrying up to CoinbaseRecoveryMaxAttempts times. It never inspects
// per-transaction state, which is what keeps its cost independent of mempool
// or subtree size.
//
// Boundary: this repairs missing canonical coinbases only. Reconciling
// ordinary-transaction double-spend/conflict state for a genuine competing-fork
// reorg is out of scope here -- that state is owned by block validation.
//
// Any scoping failure (gap too large, or a canonicalCoinbaseAt error while
// walking back) escalates to Stage 2 (halt + alarm) rather than being
// retried, since it means the gap itself cannot be safely determined.
//
// On successful repair this returns nil and increments the "repaired" metric
// outcome. On exhaustion of attempts or an over-large gap, it increments
// "escalated", logs a single loud operator-facing line naming
// resetblockassembly, and returns an error -- the caller must not advance
// past triggerHeight until an operator intervenes.
func (b *BlockAssembler) recoverCoinbaseDivergence(ctx context.Context, triggerHeight uint32) error {
	prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected").Inc()

	maxAttempts := b.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		gap, err := b.scopeCoinbaseGap(ctx, triggerHeight)
		if err != nil {
			// Gap too large, or a canonicalCoinbaseAt failure during the
			// walk-back -- either way, the gap cannot be safely determined,
			// so escalate rather than retry.
			lastErr = err
			break
		}

		if len(gap) == 0 {
			return nil // nothing to do (already consistent)
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

	// maxAttempts < 1 means the loop above never ran, so lastErr is still
	// nil here; give a clear reason rather than wrapping a nil error.
	if lastErr == nil {
		lastErr = errors.NewProcessingError("[coinbaseRecovery] CoinbaseRecoveryMaxAttempts=%d, no repair attempt was made", maxAttempts)
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
//
// This always returns nil. A recovery give-up is deliberately non-fatal:
// recoverCoinbaseDivergence has already logged the loud operator-facing alert
// and incremented the "escalated" metric internally before returning its
// error, so there is nothing left to surface here except the fact that
// startup could not repair it. Returning an error here would fail Start and
// crash-loop the process, which does not help an operator -- a running node
// they can inspect is strictly better than one stuck restarting. The
// backstop today is the next restart's startup check running this same
// probe again; a runtime (mid-operation) point-of-pain detector is future
// work, not something this branch implements.
func (b *BlockAssembler) checkCoinbaseDivergenceOnStart(ctx context.Context) error {
	if b.blockchainClient == nil || b.utxoStore == nil {
		return nil // nothing to probe against; matches the guards on the other optional-store paths in Start
	}

	header, height := b.CurrentBlock()
	if header == nil || height == 0 {
		return nil // genesis / unset — nothing to check
	}

	present, _, err := b.canonicalCoinbaseAt(ctx, height)
	if err != nil {
		// Non-fatal: log and continue; see the function-level comment above.
		b.logger.Warnf("[coinbaseRecovery] startup divergence check failed at height %d: %v", height, err)
		return nil
	}

	if present {
		return nil
	}

	b.logger.Warnf("[coinbaseRecovery] startup: canonical coinbase missing at tip height %d; running recovery", height)

	if err := b.recoverCoinbaseDivergence(ctx, height); err != nil {
		// Non-fatal: see the function-level comment above. recoverCoinbaseDivergence
		// has already logged the loud MANUAL INTERVENTION alert and incremented
		// the "escalated" metric; log that startup recovery failed and let the
		// node boot so an operator can inspect and act.
		b.logger.Errorf("[coinbaseRecovery] startup recovery failed at height %d: %v; manual intervention required (run resetblockassembly)", height, err)
	}

	return nil
}
