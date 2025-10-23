package aerospike

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
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

func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, txHash := range txHashes {
		txHash := txHash

		g.Go(func() error {
			errCh := make(chan error, 1)

			s.lockedBatcher.Put(&batchLocked{
				ctx:      ctx,
				txHash:   txHash,
				setValue: setValue,
				errCh:    errCh,
			})

			// Now we need to get totalRecords and do all the child records if necessary...

			// Sleep a percentage of the batch duration before waiting for response to reduce CPU contention.
			// Configurable via batchResponseWaitPercent (default 0 = disabled).
			time.Sleep(time.Duration(s.settings.UtxoStore.LockedBatcherDurationMillis) * time.Millisecond * time.Duration(s.batchResponseWaitPercent) / 100)
			return <-errCh
		})
	}

	return g.Wait()
}

// setLockedBatch sets the locked flag on the given transactions in a batch
func (s *Store) setLockedBatch(batch []*batchLocked) {
	var (
		batchUDFPolicy = aerospike.NewBatchUDFPolicy()
		batchRecords   = make([]aerospike.BatchRecordIfc, 0, len(batch))
	)

	// Go through each batch item and set the tx to be locked
	for _, batchItem := range batch {
		// We will do the master record first...
		keySource := uaerospike.CalculateKeySourceInternal(&batchItem.txHash, batchItem.childIndex)

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			fmt.Printf("Failed to create key: %s\n", err)
			os.Exit(1)
		}

		// Now we need to get totalRecords and do all the child records if necessary...

		batchRecords = append(batchRecords, aerospike.NewBatchUDF(
			batchUDFPolicy,
			key,
			LuaPackage,
			"setLocked",
			aerospike.NewValue(batchItem.setValue),
		))
	}

	if err := s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		for _, batchItem := range batch {
			batchItem.errCh <- errors.NewProcessingError("could not batch write locked flag", err)
		}

		return
	}

	// Now we need to get totalRecords and do all the child records if necessary...
	for idx, batchRecord := range batchRecords {
		if batchRecord.BatchRec().Err != nil {
			batch[idx].errCh <- errors.NewProcessingError("could not batch write locked flag", batchRecord.BatchRec().Err)
			continue
		}

		response := batchRecord.BatchRec().Record
		if response != nil && response.Bins != nil && response.Bins[LuaSuccess.String()] != nil {
			res, err := s.ParseLuaMapResponse(response.Bins[LuaSuccess.String()])
			if err != nil {
				batch[idx].errCh <- errors.NewProcessingError("could not parse response", err)
				continue
			}

			if res.Status != LuaStatusOK {
				if res.ErrorCode == LuaErrorCodeTxNotFound {
					batch[idx].errCh <- errors.NewTxNotFoundError("transaction not found: %s", batch[idx].txHash.String())
				} else {
					batch[idx].errCh <- errors.NewProcessingError("error from setLocked: %s", res.Message)
				}
				continue
			}

			extraRecords := res.ChildCount
			if extraRecords == 0 {
				batch[idx].errCh <- nil
				continue
			}

			// We need to do the child records...
			g, _ := errgroup.WithContext(batch[idx].ctx)

			for i := 1; i <= extraRecords; i++ {
				i := i

				g.Go(func() error {
					errCh := make(chan error, 1)

					s.lockedBatcher.Put(&batchLocked{
						txHash:     batch[idx].txHash,
						childIndex: uint32(i), // nolint:gosec
						setValue:   batch[idx].setValue,
						errCh:      errCh,
					})

					// Sleep a percentage of the batch duration before waiting for response to reduce CPU contention.
					// Configurable via batchResponseWaitPercent (default 0 = disabled).
					time.Sleep(time.Duration(s.settings.UtxoStore.LockedBatcherDurationMillis) * time.Millisecond * time.Duration(s.batchResponseWaitPercent) / 100)
					return <-errCh
				})
			}

			batch[idx].errCh <- g.Wait()
		}
	}
}
