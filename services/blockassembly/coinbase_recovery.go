package blockassembly

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// coinbaseRecoveryRetryBackoff is the pause between automatic recovery
// attempts. Retries exist to ride out a transient store or blockchain-client
// blip, and retrying instantly would burn the whole attempt budget inside the
// same blip. It is deliberately a constant rather than a setting: this runs
// once at startup with a small attempt budget, so the total added boot latency
// is bounded at (CoinbaseRecoveryMaxAttempts-1) * this value.
const coinbaseRecoveryRetryBackoff = 500 * time.Millisecond

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

// coinbaseGapTooLargeError is the type of the errCoinbaseGapTooLarge sentinel.
//
// It is deliberately NOT built with errors.NewProcessingError. teranode's
// errors.Is matches two *Error values by their error *code*, not by identity,
// so a ProcessingError sentinel would compare equal to every other
// ProcessingError -- including the wrapped canonicalCoinbaseAt failures
// scopeCoinbaseGap also returns. recoverCoinbaseDivergence has to tell those
// two apart (structural gap = escalate now, transient store blip = retry), and
// with a ProcessingError sentinel that test would always say "too large".
//
// As a distinct type it takes the non-*Error path in errors.Is: identity
// comparison against this sentinel, and a message-substring test (which no
// canonicalCoinbaseAt error matches) against anything else.
type coinbaseGapTooLargeError struct{}

// Error implements the error interface.
func (coinbaseGapTooLargeError) Error() string {
	return "[coinbaseRecovery] gap exceeds max recoverable blocks"
}

// errCoinbaseGapTooLarge is returned by scopeCoinbaseGap when the walk-back
// from the trigger height exceeds CoinbaseRecoveryMaxGapBlocks without
// finding a proven-good floor. The orchestrator treats this as a non-local
// divergence and escalates immediately rather than spending retries on it.
var errCoinbaseGapTooLarge error = coinbaseGapTooLargeError{}

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
// Only errCoinbaseGapTooLarge escalates immediately: it is structural, so no
// number of retries will change the answer. Every other failure -- a
// canonicalCoinbaseAt error during the walk-back, or a ReconcileCoinbases
// error during the repair -- is treated as possibly transient and absorbed by
// the attempt budget, with coinbaseRecoveryRetryBackoff between attempts. A
// momentary store hiccup must not fire the loudest signal the system has:
// crying wolf on "escalated" would erode its value for real divergences.
//
// Exactly one outcome metric is recorded per call alongside "detected", so
// detected == repaired + no_gap + escalated:
//   - "repaired"  -- a non-empty gap was closed
//   - "no_gap"    -- scoping found nothing to repair (a concurrent writer, or
//     a retry, closed it first)
//   - "escalated" -- structural gap, or the attempt budget ran out
//
// On escalation it logs a single loud operator-facing line naming
// resetblockassembly and returns an error. "Escalate" means this recovery
// stops trying, not that the process stops: the sole caller
// (checkCoinbaseDivergenceOnStart) deliberately lets the node boot anyway --
// see the note there. The node therefore does keep advancing on the diverged
// tip, and an operator must intervene for it to be genuinely repaired.
func (b *BlockAssembler) recoverCoinbaseDivergence(ctx context.Context, triggerHeight uint32) error {
	prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("detected").Inc()

	maxAttempts := b.settings.BlockAssembly.CoinbaseRecoveryMaxAttempts
	if maxAttempts < 1 {
		// A non-positive setting would skip the loop entirely and escalate
		// without ever trying to repair. Clamp to a single attempt, matching
		// the lower-bound clamps scopeCoinbaseGap applies to its own settings.
		maxAttempts = 1
	}

	var lastErr error

attempts:
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			// Shutdown (or a cancelled startup) while retrying: stop burning
			// attempts against a dead context and report the real reason.
			lastErr = ctx.Err()
			break
		}

		if attempt > 1 {
			select {
			case <-ctx.Done():
				lastErr = ctx.Err()
				break attempts
			case <-time.After(coinbaseRecoveryRetryBackoff):
			}
		}

		gap, err := b.scopeCoinbaseGap(ctx, triggerHeight)
		if err != nil {
			lastErr = err

			if errors.Is(err, errCoinbaseGapTooLarge) {
				// Structural: the divergence is non-local, so retrying the
				// same walk-back cannot produce a scopeable gap.
				break
			}

			// A canonicalCoinbaseAt failure (store or blockchain-client blip).
			// Possibly transient, so spend an attempt on it rather than
			// raising the manual-intervention alarm on a network hiccup.
			b.logger.Warnf("[coinbaseRecovery] attempt %d/%d could not scope the gap below height %d: %v", attempt, maxAttempts, triggerHeight, err)

			continue
		}

		if len(gap) == 0 {
			// Nothing to repair. Recorded as its own outcome so the metric
			// balances rather than leaving a "detected" with no counterpart.
			prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("no_gap").Inc()

			return nil
		}

		if err := b.subtreeProcessor.ReconcileCoinbases(ctx, gap); err != nil {
			lastErr = err
			b.logger.Warnf("[coinbaseRecovery] attempt %d/%d failed to repair %d coinbase(s) below height %d: %v", attempt, maxAttempts, len(gap), triggerHeight, err)

			continue
		}

		b.logger.Infof("[coinbaseRecovery] repaired %d coinbase(s) up to height %d on attempt %d", len(gap), triggerHeight, attempt)
		prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("repaired").Inc()

		return nil
	}

	// Defensive: every break above sets lastErr, but keep the error message
	// truthful rather than wrapping a nil if a future edit adds a path that
	// does not.
	if lastErr == nil {
		lastErr = errors.NewProcessingError("[coinbaseRecovery] no repair attempt completed after %d attempts", maxAttempts)
	}

	// Stage 2: stop retrying and raise a single loud operator signal.
	prometheusBlockAssemblyCoinbaseDivergence.WithLabelValues("escalated").Inc()
	b.logger.Errorf("[coinbaseRecovery] MANUAL INTERVENTION REQUIRED: coinbase divergence at/below height %d could not be auto-repaired after %d attempts (%v); the node will keep running on the diverged tip -- run resetblockassembly", triggerHeight, maxAttempts, lastErr)

	return errors.NewProcessingError("[coinbaseRecovery] unrecoverable coinbase divergence at height %d", triggerHeight, lastErr)
}

// checkCoinbaseDivergenceOnStart looks for canonical coinbases missing from the
// UTXO store near the persisted tip and repairs what it finds. Called once
// during Start, before block-notification listeners begin, so a divergence
// created by a prior unclean shutdown is healed before the node advances.
//
// # Detection coverage
//
// It probes a bounded window of heights walking down from the tip, stopping at
// the first (i.e. highest) missing coinbase and running recovery from there --
// recoverCoinbaseDivergence then scopes and repairs everything beneath it.
//
// Probing the tip alone is not enough. The fast-forward create loop in
// SubtreeProcessor.reset runs one goroutine per moveForward block, so a crash
// mid-loop can leave the tip's coinbase written while a lower one is still
// missing -- a "hole" under a healthy-looking tip. That is exactly the shape
// scopeCoinbaseGap's consecutive-good walk-back exists to repair, and a
// tip-only probe would return early and never invoke it, leaving the hole to
// wedge the node on the eventual maturity spend.
//
// The window is CoinbaseRecoveryMaxGapBlocks heights, reusing the same bound
// the repair itself honours: there is no point detecting a divergence deeper
// than recovery is willing to auto-repair. Cost is two store reads per height
// on a clean boot and it short-circuits on the first miss.
//
// This is a bounded window, not whole-chain coverage. A hole further below the
// tip than the window stays invisible here; finding that cheaply is the job of
// the runtime point-of-pain detector, which is deliberately future work.
//
// # Why it never fails Start
//
// This always returns nil. A recovery give-up is deliberately non-fatal:
// recoverCoinbaseDivergence has already logged the loud operator-facing alert
// and incremented the "escalated" metric internally before returning its
// error, so there is nothing left to surface here except the fact that
// startup could not repair it. Returning an error here would fail Start and
// crash-loop the process, which does not help an operator -- a running node
// they can inspect is strictly better than one stuck restarting. The trade-off
// is explicit: the node boots and keeps building on a tip known to be
// diverged, so the escalation log is a call to action, not a description of a
// node that has stopped itself.
func (b *BlockAssembler) checkCoinbaseDivergenceOnStart(ctx context.Context) error {
	if b.blockchainClient == nil || b.utxoStore == nil {
		return nil // nothing to probe against; matches the guards on the other optional-store paths in Start
	}

	header, height := b.CurrentBlock()
	if header == nil || height == 0 {
		return nil // genesis / unset — nothing to check
	}

	window := b.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks
	if window < 1 {
		// A non-positive setting would scan nothing at all, silently disabling
		// startup detection. Match the lower-bound clamp scopeCoinbaseGap uses.
		window = 1
	}

	// Walk down from the tip. h stops at 1 for the same reason the walk-back
	// in scopeCoinbaseGap does: the genesis coinbase is unspendable and never
	// exists in the UTXO store, so probing height 0 always reports "missing".
	for h, scanned := height, 0; h >= 1 && scanned < window; h, scanned = h-1, scanned+1 {
		present, _, err := b.canonicalCoinbaseAt(ctx, h)
		if err != nil {
			// Non-fatal: log and continue booting; see the function-level
			// comment above. Abandoning the whole scan (rather than skipping
			// this height) is deliberate -- if the blockchain client or store
			// cannot answer, the remaining probes will not either.
			b.logger.Warnf("[coinbaseRecovery] startup divergence check failed at height %d (tip %d): %v", h, height, err)
			return nil
		}

		if present {
			continue
		}

		b.logger.Warnf("[coinbaseRecovery] startup: canonical coinbase missing at height %d (tip %d); running recovery", h, height)

		if err := b.recoverCoinbaseDivergence(ctx, h); err != nil {
			// Non-fatal: see the function-level comment above. recoverCoinbaseDivergence
			// has already logged the loud MANUAL INTERVENTION alert and incremented
			// the "escalated" metric; log that startup recovery failed and let the
			// node boot so an operator can inspect and act.
			b.logger.Errorf("[coinbaseRecovery] startup recovery failed at height %d (tip %d): %v; manual intervention required (run resetblockassembly)", h, height, err)
		}

		// One recovery pass per boot: recoverCoinbaseDivergence already scoped
		// and repaired the contiguous gap beneath the highest miss, and
		// re-scanning here would double-report the same divergence.
		return nil
	}

	return nil
}
