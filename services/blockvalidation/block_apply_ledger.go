package blockvalidation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
)

// blockApplyLedger is an OPTIONAL capability, satisfied only by a UTXO store whose
// create is not idempotent and which therefore needs replay gated by ground truth.
//
// The delete-on-spend store (stores/utxo/utxoset) is the one that needs it: its key is a
// deliberately non-unique 96-bit txid prefix, so there is no constraint for an
// ON CONFLICT to act on, and re-applying a block would insert every output a second time
// as an independently spendable row. Aerospike and the generic SQL store do not implement
// this interface and are unaffected, which is the point of it being optional rather than
// a change to utxo.Store.
//
// Declared here, at the point of use, so the shared store package needs no edit.
type blockApplyLedger interface {
	// BeginBlockApply claims a block and reports whether a previous attempt COMPLETED.
	BeginBlockApply(ctx context.Context, blockHash *chainhash.Hash, height uint32) (bool, error)

	// CompleteBlockApply marks a block finished, so a later offer of it is skipped.
	CompleteBlockApply(ctx context.Context, blockHash *chainhash.Hash) error
}

// claimBlockApply reports whether this block has already been applied in full and can be
// skipped. It returns false for any store that does not keep a ledger, so nothing changes
// for aerospike or the generic SQL store.
//
// It deliberately FAILS CLOSED. If the ledger cannot be read we do not guess: applying a
// block that may already be applied risks duplicating every one of its outputs, so the
// block fails here and is retried instead.
func (u *BlockValidation) claimBlockApply(ctx context.Context, block *model.Block) (bool, error) {
	ledger, ok := u.utxoStore.(blockApplyLedger)
	if !ok {
		return false, nil
	}

	applied, err := ledger.BeginBlockApply(ctx, block.Hash(), block.Height)
	if err != nil {
		return false, err
	}

	if applied {
		u.logger.Infof("[claimBlockApply][%s] block already applied at height %d, skipping UTXO work",
			block.Hash().String(), block.Height)
	}

	return applied, nil
}

// completeBlockApply marks the block finished.
//
// Call it only once every output the block creates and every input it spends is durably
// committed. Marking it early turns a crash into permanent silent loss, because the next
// offer of the block would skip work that never actually happened.
func (u *BlockValidation) completeBlockApply(ctx context.Context, block *model.Block) error {
	ledger, ok := u.utxoStore.(blockApplyLedger)
	if !ok {
		return nil
	}

	return ledger.CompleteBlockApply(ctx, block.Hash())
}
