package aerospike

import (
	"context"
	"fmt"
	"os"

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

	// Process master results. Items whose master record reports child/extra
	// records are NOT re-queued into the batcher: re-enqueuing from inside the
	// batcher's own callback deadlocks (and panics on a closed channel) during a
	// draining Close, because the worker that would service the re-queued item
	// is the very one shutting down. Instead — mirroring how the create path
	// writes a tx's extra/external records inline within its callback — the
	// child records are collected here and written below in a single inline
	// BatchOperate. childErr tracks one terminal result per child-bearing item
	// so each errCh is signalled exactly once.
	childErr := make(map[int]error)

	var (
		childRecords []aerospike.BatchRecordIfc
		childOwner   []int // childRecords[k] belongs to batch[childOwner[k]]
	)

	for idx, batchRecord := range batchRecords {
		if batchRecord.BatchRec().Err != nil {
			batch[idx].errCh <- errors.NewProcessingError("could not batch write locked flag", batchRecord.BatchRec().Err)
			continue
		}

		response := batchRecord.BatchRec().Record
		if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
			// No parseable response — preserve prior behaviour (no errCh signal).
			continue
		}

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

		// Collect this item's child records for the inline batch below. Defer
		// signalling its errCh until the child pass (tracked via childErr).
		childErr[idx] = nil

		for i := 1; i <= extraRecords; i++ {
			keySource := uaerospike.CalculateKeySourceInternal(&batch[idx].txHash, uint32(i)) // nolint:gosec

			key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
			if err != nil {
				childErr[idx] = errors.NewProcessingError("could not create child key for locked flag", err)
				break
			}

			childRecords = append(childRecords, aerospike.NewBatchUDF(
				batchUDFPolicy,
				key,
				LuaPackage,
				"setLocked",
				aerospike.NewValue(batch[idx].setValue),
			))
			childOwner = append(childOwner, idx)
		}
	}

	// Write all collected child records inline (no batcher re-entry, so this is
	// safe to run while the batcher is draining on Close).
	if len(childRecords) > 0 {
		if err := s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), childRecords); err != nil {
			for idx := range childErr {
				if childErr[idx] == nil {
					childErr[idx] = errors.NewProcessingError("could not batch write locked child records", err)
				}
			}
		} else {
			for k, childRecord := range childRecords {
				idx := childOwner[k]
				if childErr[idx] != nil {
					continue // already errored for this item
				}

				if childRecord.BatchRec().Err != nil {
					childErr[idx] = errors.NewProcessingError("could not write locked child record", childRecord.BatchRec().Err)
					continue
				}

				resp := childRecord.BatchRec().Record
				if resp == nil || resp.Bins == nil || resp.Bins[LuaSuccess.String()] == nil {
					continue
				}

				cres, perr := s.ParseLuaMapResponse(resp.Bins[LuaSuccess.String()])
				if perr != nil {
					childErr[idx] = errors.NewProcessingError("could not parse child response", perr)
				} else if cres.Status != LuaStatusOK {
					childErr[idx] = errors.NewProcessingError("error from setLocked child: %s", cres.Message)
				}
			}
		}
	}

	// Signal each child-bearing item exactly once with its terminal result.
	for idx, e := range childErr {
		batch[idx].errCh <- e
	}
}
