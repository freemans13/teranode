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
