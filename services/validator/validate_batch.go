package validator

import (
	"context"
	"runtime"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
	"golang.org/x/sync/errgroup"
)

// ValidateBatch is the v1 batch entry point. When
// settings.Validator.UseBatchValidation is false (default), it runs a
// bounded fan-out over the existing ValidateWithOptions for compatibility.
// When true, it runs the native 6-phase path (added in later tasks; the
// native function delegates to the fallback for now so the package builds
// and the flag can be enabled without effect).
//
// The returned slice is positionally aligned with txs. ValidateBatch
// returns err != nil only on a whole-batch failure; per-tx errors live
// in results[i].Err.
func (v *Validator) ValidateBatch(
	ctx context.Context,
	txs []*bt.Tx,
	blockHeight uint32,
	opts ...Option,
) ([]ValidationResult, error) {
	if len(txs) == 0 {
		return []ValidationResult{}, nil
	}

	if v.settings.Validator.UseBatchValidation {
		return v.validateBatchNative(ctx, txs, blockHeight, opts...)
	}
	return v.validateBatchFallback(ctx, txs, blockHeight, opts...)
}

// validateBatchFallback fans out per-tx over ValidateWithOptions with
// bounded parallelism. This is the kill-switch path.
func (v *Validator) validateBatchFallback(
	ctx context.Context,
	txs []*bt.Tx,
	blockHeight uint32,
	opts ...Option,
) ([]ValidationResult, error) {
	results := make([]ValidationResult, len(txs))
	for i, tx := range txs {
		results[i].TxHash = *tx.TxIDChainHash()
	}

	processedOpts := ProcessOptions(opts...)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.NumCPU())

	var mu sync.Mutex

	for i, tx := range txs {
		i, tx := i, tx
		g.Go(func() error {
			m, err := v.ValidateWithOptions(gCtx, tx, blockHeight, processedOpts)
			mu.Lock()
			results[i].Meta = m
			results[i].Err = err
			mu.Unlock()
			return nil // never bubble per-tx errors as group error
		})
	}

	_ = g.Wait()

	if err := ctx.Err(); err != nil {
		return results, err
	}
	return results, nil
}

// validateBatchNative is implemented in validate_batch_native.go.
