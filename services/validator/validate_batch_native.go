package validator

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
)

// batchUtxoStore is the minimal UTXO store surface ValidateBatch needs.
// The concrete *aerospike.Store satisfies it; tests use a stub.
//
// Phases C–F will extend this interface with BatchSpend / BatchCreate /
// BatchSetLocked methods in Tasks 13–15.
type batchUtxoStore interface {
	BatchGetParents(ctx context.Context, parentHashes [][]byte) (map[[32]byte]*aerospike.ParentRecord, [][]byte, error)
}

// validateBatchNative runs the six-phase native pipeline. Phase wiring is
// added incrementally — this version implements Phase A only.
// Phases B–F are stubs that leave results untouched until Tasks 12–16.
func (v *Validator) validateBatchNative(
	ctx context.Context,
	txs []*bt.Tx,
	blockHeight uint32,
	opts ...Option,
) ([]ValidationResult, error) {
	results := make([]ValidationResult, len(txs))
	for i, tx := range txs {
		results[i].TxHash = *tx.TxIDChainHash()
	}

	store, ok := v.getBatchUtxoStore()
	if !ok {
		// Configured UTXO store does not implement the batch methods.
		// Fall back to per-tx fan-out so the flag-on path stays additive.
		return v.validateBatchFallback(ctx, txs, blockHeight, opts...)
	}

	// Phase A: fetch all unique parent hashes in a single BatchGetParents call.
	parentHashes := collectUniqueParents(txs)
	parents, _, err := store.BatchGetParents(ctx, parentHashes)
	if err != nil {
		return results, err
	}

	// Both an explicit "missing" hash and a hash absent from the parents map
	// are treated identically — any absent parent marks the tx as failed.
	// The missing slice is intentionally discarded here; both cases are
	// detected by the absence of the hash from the parents map below.

	alive := make([]bool, len(txs))
	for i, tx := range txs {
		parentMissing := false
		for _, in := range tx.Inputs {
			ph := in.PreviousTxIDChainHash()
			var key [32]byte
			copy(key[:], ph[:])

			if _, present := parents[key]; present {
				continue
			}

			// Parent is absent from the result map — either explicitly
			// listed as missing by Aerospike, or simply not returned.
			// Both cases map to ErrTxMissingParent.
			results[i].Err = terrors.ErrTxMissingParent
			results[i].Phase = PhaseGetParents
			parentMissing = true
			break
		}
		alive[i] = !parentMissing
	}

	// Phases B–F follow in subsequent tasks. For alive[i]==true entries,
	// results[i] stays at its zero value until those phases populate it.
	_ = alive

	return results, nil
}

// getBatchUtxoStore returns the validator's UTXO store as a batchUtxoStore if
// it satisfies the interface (i.e. is an *aerospike.Store or a test stub).
// Returns false when the store is e.g. a sql.Store that does not implement
// the batch methods.
func (v *Validator) getBatchUtxoStore() (batchUtxoStore, bool) {
	if v.batchStoreOverride != nil {
		return v.batchStoreOverride, true
	}
	s, ok := v.utxoStore.(batchUtxoStore)
	return s, ok
}

// collectUniqueParents walks all tx inputs and returns the deduplicated set of
// parent tx hashes as a [][]byte (preserves first-seen order).
func collectUniqueParents(txs []*bt.Tx) [][]byte {
	seen := make(map[[32]byte]struct{})
	out := make([][]byte, 0, len(txs))
	for _, tx := range txs {
		for _, in := range tx.Inputs {
			ph := in.PreviousTxIDChainHash()
			var key [32]byte
			copy(key[:], ph[:])
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			b := make([]byte, 32)
			copy(b, ph[:])
			out = append(out, b)
		}
	}
	return out
}

// byteSliceSet converts a [][]byte into a map keyed by [32]byte for O(1) lookup.
func byteSliceSet(hashes [][]byte) map[[32]byte]struct{} {
	set := make(map[[32]byte]struct{}, len(hashes))
	for _, h := range hashes {
		var key [32]byte
		copy(key[:], h)
		set[key] = struct{}{}
	}
	return set
}
