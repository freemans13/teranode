package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"golang.org/x/sync/errgroup"
)

// DeleteCreated removes transaction records that a create phase wrote and a
// later phase of the same block then made invalid.
//
// Both below-checkpoint block paths create every transaction in the block first
// and spend the inputs afterwards (services/blockvalidation/quick_validate.go
// and services/legacy/netsync/handle_block.go). Create does not consult the
// pruner's replay markers - putting a parent-record read on the hottest write
// path in the node is the cost that ordering exists to avoid - so a block that
// replays a pruned transaction gets through the create phase and is rejected in
// the spend phase. Without compensation the recreated record stays: it is
// stored mined with no delete_at_height, so neither the unmined reaper nor the
// DAH scan ever reclaims it, and on the legacy path it is unlocked and
// spendable even though a mined grandchild already consumed those outputs.
//
// Only records this block's create phase actually wrote may be passed in. A
// transaction that was already in the store (create returned ErrTxExists)
// belongs to whoever put it there and must be excluded by the caller.
//
// A missing record is success: it is gone, which is the outcome asked for. Any
// other failure is logged per transaction and reported in aggregate, because a
// record left behind here is the ghost this function exists to prevent and the
// operator has to see it.
func DeleteCreated(ctx context.Context, logger ulogger.Logger, store Store, hashes []*chainhash.Hash, maxWorkers int) error {
	if len(hashes) == 0 {
		return nil
	}

	logger.Warnf("[DeleteCreated] removing %d transaction(s) created by a block phase that then failed", len(hashes))

	if maxWorkers < 1 {
		maxWorkers = 1
	}

	deleteG := new(errgroup.Group)
	deleteG.SetLimit(maxWorkers)

	for _, hash := range hashes {
		hash := hash

		deleteG.Go(func() error {
			if err := store.Delete(ctx, hash); err != nil {
				if errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound) {
					return nil
				}

				logger.Errorf("[DeleteCreated] failed to remove %s after a failed block phase; it remains in the store: %v", hash.String(), err)

				return err
			}

			return nil
		})
	}

	if err := deleteG.Wait(); err != nil {
		return errors.NewStorageError("[DeleteCreated] could not remove all %d created transaction(s) after a failed block phase", len(hashes), err)
	}

	return nil
}
