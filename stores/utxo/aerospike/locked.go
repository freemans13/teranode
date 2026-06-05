package aerospike

import (
	"context"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"golang.org/x/sync/errgroup"
)

// batchLocked represents a batch operation to set the locked flag on a transaction
type batchLocked struct {
	ctx        context.Context
	txHash     chainhash.Hash
	childIndex uint32 // This will default to 0 which is the master record
	setValue   bool
	errCh      chan error // Channel for completion notification
}

// waitForLockedResult waits for a single locked-batch item to complete, bounded
// so a wedged lockedBatcher (including the same-pool child-record recursion in
// setLockedBatch) can never pin the caller — or a dispatch worker — forever.
func (s *Store) waitForLockedResult(ctx context.Context, errCh chan error) error {
	if s.batcherWait <= 0 {
		select {
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	timer := time.NewTimer(s.batcherWait)
	defer timer.Stop()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.NewServiceUnavailableError("set locked did not complete within %s", s.batcherWait)
	}
}

func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, txHash := range txHashes {
		txHash := txHash

		g.Go(func() error {
			errCh := make(chan error, 1)

			s.lockedBatcher.PutCtx(ctx, &batchLocked{
				ctx:      ctx,
				txHash:   txHash,
				setValue: setValue,
				errCh:    errCh,
			})

			// Now we need to get totalRecords and do all the child records if necessary...

			return s.waitForLockedResult(ctx, errCh)
		})
	}

	return g.Wait()
}

// setLockedBatch sets the locked flag on the given transactions in a batch
func (s *Store) setLockedBatch(batch []*batchLocked) {
	// go-batcher recovers panics in this fn; re-signal every errCh on panic so a
	// crash (e.g. in ParseLuaMapResponse) cannot orphan the waiting submitters.
	defer func() {
		signalBatchPanic(recover(), batch, "setLockedBatch", s.logger, func(it *batchLocked, err error) {
			trySignal(it.errCh, err)
		})
	}()

	var (
		batchUDFPolicy = aerospike.NewBatchUDFPolicy()
		batchRecords   = make([]aerospike.BatchRecordIfc, len(batch))
		handled        = make([]bool, len(batch))
	)

	// Go through each batch item and set the tx to be locked
	for idx, batchItem := range batch {
		// We will do the master record first...
		keySource := uaerospike.CalculateKeySourceInternal(&batchItem.txHash, batchItem.childIndex)

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			// Previously this called os.Exit(1), turning a recoverable key error
			// into a process crash. Surface it to the caller and keep the batch
			// index aligned with a NOOP placeholder instead.
			var keyErr error = errors.NewProcessingError("[setLockedBatch] failed to create key", err)
			trySignal(batchItem.errCh, keyErr)

			handled[idx] = true
			batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

			continue
		}

		// Now we need to get totalRecords and do all the child records if necessary...

		batchRecords[idx] = aerospike.NewBatchUDF(
			batchUDFPolicy,
			key,
			LuaPackage,
			"setLocked",
			aerospike.NewValue(batchItem.setValue),
		)
	}

	if err := s.batchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		for idx, batchItem := range batch {
			if handled[idx] {
				continue
			}

			var sendErr error = errors.NewProcessingError("could not batch write locked flag", err)
			trySignal(batchItem.errCh, sendErr)
		}

		return
	}

	// Now we need to get totalRecords and do all the child records if necessary...
	for idx, batchRecord := range batchRecords {
		if handled[idx] {
			continue
		}

		if recErr := batchRecord.BatchRec().Err; recErr != nil {
			var sendErr error = errors.NewProcessingError("could not batch write locked flag", recErr)
			trySignal(batch[idx].errCh, sendErr)

			continue
		}

		response := batchRecord.BatchRec().Record
		if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
			// Previously this fell through without signalling — orphaning the
			// submitter on any nil/missing-bin response. Signal an error instead.
			var sendErr error = errors.NewProcessingError("setLocked: missing response for %s", batch[idx].txHash.String())
			trySignal(batch[idx].errCh, sendErr)

			continue
		}

		res, err := s.ParseLuaMapResponse(response.Bins[LuaSuccess.String()])
		if err != nil {
			var sendErr error = errors.NewProcessingError("could not parse response", err)
			trySignal(batch[idx].errCh, sendErr)

			continue
		}

		if res.Status != LuaStatusOK {
			if res.ErrorCode == LuaErrorCodeTxNotFound {
				var sendErr error = errors.NewTxNotFoundError("transaction not found: %s", batch[idx].txHash.String())
				trySignal(batch[idx].errCh, sendErr)
			} else {
				var sendErr error = errors.NewProcessingError("error from setLocked: %s", res.Message)
				trySignal(batch[idx].errCh, sendErr)
			}

			continue
		}

		extraRecords := res.ChildCount
		if extraRecords == 0 {
			trySignal(batch[idx].errCh, error(nil))

			continue
		}

		// We need to do the child records...
		g, _ := errgroup.WithContext(batch[idx].ctx)

		for i := 1; i <= extraRecords; i++ {
			i := i

			g.Go(func() error {
				errCh := make(chan error, 1)

				s.lockedBatcher.PutCtx(batch[idx].ctx, &batchLocked{
					txHash:     batch[idx].txHash,
					childIndex: uint32(i), // nolint:gosec
					setValue:   batch[idx].setValue,
					errCh:      errCh,
				})

				return s.waitForLockedResult(batch[idx].ctx, errCh)
			})
		}

		trySignal(batch[idx].errCh, g.Wait())
	}
}
