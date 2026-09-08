package utxo

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"golang.org/x/sync/errgroup"
)

// PrunedReplayGhosts returns the records a failed spend phase leaves behind
// when the store rejected transactions of a block as pruned replays.
//
// Both below-checkpoint block paths create every transaction in the block first
// and spend the inputs afterwards (services/blockvalidation/quick_validate.go
// and services/legacy/netsync/handle_block.go). Create does not consult the
// pruner's replay markers - putting a parent-record read on the hottest write
// path in the node is the cost that ordering exists to avoid - so a block that
// replays pruned transactions gets through the create phase and is rejected in
// the spend phase, with the records it wrote still in the store. Each is stored
// mined with no delete_at_height, so neither the unmined reaper nor the DAH
// scan ever reclaims it, and on the legacy path it is unlocked and spendable
// even though a mined descendant already consumed those outputs.
//
// Two kinds of record qualify.
//
// The rejected transactions themselves are ghosts whether or not this attempt
// created them. The store rejects a spend on a replay marker only for a
// transaction this store already pruned, and pruning happens only once every
// output is spent and the transaction is buried below retention, so there is no
// route by which it legitimately comes back. A record for it can only be what a
// replay left behind: this attempt's create phase, or an earlier attempt whose
// compensating delete failed. Excluding "already existed" here is what made a
// transient delete failure permanent: the next attempt saw ErrTxExists, filed
// the leftover as pre-existing and skipped it forever.
//
// A transaction in the same block that spends an output of a ghost is a ghost
// too, but only when this attempt created it. Its own markers cannot catch it:
// they lived on the pruned parent's record and went with it, and the parent it
// spends was just recreated with unspent outputs, so its spend succeeds and it
// would stay behind with spendable outputs after the parent is removed. A
// dependent the store already held is different: a pruned parent says nothing
// about whether a still-unpruned child is legitimate, so that record belongs to
// whoever put it there and is left alone. The walk is transitive through ghosts
// only.
//
// txs need not be in dependency order; the walk repeats until it adds nothing.
func PrunedReplayGhosts(txs []*bt.Tx, rejected []*chainhash.Hash, createdHere func(*chainhash.Hash) bool) []*chainhash.Hash {
	if len(rejected) == 0 {
		return nil
	}

	ghosts := make(map[chainhash.Hash]struct{}, len(rejected))
	result := make([]*chainhash.Hash, 0, len(rejected))

	for _, hash := range rejected {
		if _, ok := ghosts[*hash]; ok {
			continue
		}

		ghosts[*hash] = struct{}{}
		result = append(result, hash)
	}

	// Hash once per transaction: the walk below may visit each several times.
	hashes := make([]*chainhash.Hash, len(txs))
	for i, tx := range txs {
		hashes[i] = tx.TxIDChainHash()
	}

	for added := true; added; {
		added = false

		for i, tx := range txs {
			txHash := hashes[i]
			if _, ok := ghosts[*txHash]; ok {
				continue
			}

			if !spendsAny(tx, ghosts) || !createdHere(txHash) {
				continue
			}

			ghosts[*txHash] = struct{}{}
			result = append(result, txHash)
			added = true
		}
	}

	return result
}

// spendsAny reports whether tx spends an output of any transaction in set.
func spendsAny(tx *bt.Tx, set map[chainhash.Hash]struct{}) bool {
	for _, input := range tx.Inputs {
		if _, ok := set[*input.PreviousTxIDChainHash()]; ok {
			return true
		}
	}

	return false
}

// deleteCreatedAttempts and deleteCreatedBackoff bound the retry of one
// compensating delete. A record left behind here is the ghost this package
// exists to remove, and for a dependent of a pruned replay the next attempt
// cannot tell the leftover from a legitimately pre-existing record (see
// PrunedReplayGhosts), so the in-band retry is the only chance to finish the job.
const (
	deleteCreatedAttempts = 3
	deleteCreatedBackoff  = 100 * time.Millisecond
)

// DeleteCreated removes transaction records that a create phase wrote and a
// later phase of the same block then made invalid. Pass it what
// PrunedReplayGhosts returns.
//
// A missing record is success: it is gone, which is the outcome asked for. Any
// other failure is retried, then logged per transaction and reported in
// aggregate, because a record left behind here is the ghost this function exists
// to prevent and the operator has to see it.
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
			var err error

			for attempt := 0; attempt < deleteCreatedAttempts; attempt++ {
				if attempt > 0 {
					select {
					case <-ctx.Done():
						return errors.NewContextCanceledError("[DeleteCreated] cancelled while retrying %s", hash.String(), ctx.Err())
					case <-time.After(deleteCreatedBackoff * time.Duration(attempt)):
					}
				}

				err = store.DeleteComplete(ctx, hash)
				if err == nil || errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound) {
					return nil
				}
			}

			logger.Errorf("[DeleteCreated] failed to remove %s after a failed block phase (%d attempts); it remains in the store: %v", hash.String(), deleteCreatedAttempts, err)

			return err
		})
	}

	if err := deleteG.Wait(); err != nil {
		return errors.NewStorageError("[DeleteCreated] could not remove all %d created transaction(s) after a failed block phase", len(hashes), err)
	}

	return nil
}
