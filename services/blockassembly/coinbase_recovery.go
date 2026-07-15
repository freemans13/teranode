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
