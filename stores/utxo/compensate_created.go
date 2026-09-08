package utxo

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
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
// Every hash in rejected must name a transaction in txs, which holds at both
// call sites because the rejections came from spending exactly that list; the
// transactions are returned rather than their hashes because DeleteCreated needs
// their inputs.
func PrunedReplayGhosts(txs []*bt.Tx, rejected []*chainhash.Hash, createdHere func(*chainhash.Hash) bool) []*bt.Tx {
	if len(rejected) == 0 {
		return nil
	}

	// Hash once per transaction: the walk below may visit each several times.
	hashes := make([]*chainhash.Hash, len(txs))
	byHash := make(map[chainhash.Hash]*bt.Tx, len(txs))

	for i, tx := range txs {
		hashes[i] = tx.TxIDChainHash()
		byHash[*hashes[i]] = tx
	}

	ghosts := make(map[chainhash.Hash]struct{}, len(rejected))
	result := make([]*bt.Tx, 0, len(rejected))

	for _, hash := range rejected {
		if _, ok := ghosts[*hash]; ok {
			continue
		}

		tx, ok := byHash[*hash]
		if !ok {
			continue
		}

		ghosts[*hash] = struct{}{}
		result = append(result, tx)
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
			result = append(result, tx)
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

// deleteCreatedAttempts and deleteCreatedBackoff bound the retry of each store
// call the compensation makes. A record left behind here is the ghost this
// package exists to remove, and for a dependent of a pruned replay the next
// attempt cannot tell the leftover from a legitimately pre-existing record (see
// PrunedReplayGhosts), so the in-band retry is the only chance to finish the job.
const (
	deleteCreatedAttempts = 3
	deleteCreatedBackoff  = 100 * time.Millisecond
)

// DeleteCreated removes the ghosts PrunedReplayGhosts found, and first releases
// every output they spent that survives them.
//
// Deleting a record does not reverse its input spends. A descendant ghost can
// have spent a recreated pruned transaction AND an unrelated, perfectly valid
// output in the same block; removing its record alone leaves that output
// recorded as spent by a transaction that no longer exists, and the output's
// real spender is then refused with ErrSpent naming a ghost. So for every ghost,
// each input whose parent is not itself a ghost is unspent through Store.Unspend
// before anything is deleted. Both stores match on the spending data, so an
// output already rolled back, or since taken by another spender, is left alone.
// Inputs whose parent is a ghost are skipped: that record is about to go, and
// releasing its outputs would be work on a record nobody will read.
//
// The release runs for every ghost before the first delete. Aerospike's unspend
// verifies the UTXO hash, which for an outpoint-only replay has to be fetched
// from the parent through PreviousOutputsDecorate, and a ghost parent must
// still be there when its ghost child is decorated.
//
// A missing record is success: it is gone, which is the outcome asked for. Any
// other failure is retried, then logged per transaction and reported in
// aggregate, because a record left behind here is the ghost this function exists
// to prevent and the operator has to see it.
func DeleteCreated(ctx context.Context, logger ulogger.Logger, store Store, ghosts []*bt.Tx, maxWorkers int) error {
	if len(ghosts) == 0 {
		return nil
	}

	logger.Warnf("[DeleteCreated] removing %d transaction(s) created by a block phase that then failed", len(ghosts))

	if maxWorkers < 1 {
		maxWorkers = 1
	}

	ghostSet := make(map[chainhash.Hash]struct{}, len(ghosts))
	for _, tx := range ghosts {
		ghostSet[*tx.TxIDChainHash()] = struct{}{}
	}

	releaseG := new(errgroup.Group)
	releaseG.SetLimit(maxWorkers)

	for _, tx := range ghosts {
		tx := tx

		releaseG.Go(func() error {
			err := retryStoreCall(ctx, func() error { return releaseSurvivingSpends(ctx, store, tx, ghostSet) })
			if err != nil {
				logger.Errorf("[DeleteCreated] failed to release the spends of %s after a failed block phase (%d attempts); its record is kept so the spends stay attributable: %v", tx.TxIDChainHash().String(), deleteCreatedAttempts, err)
			}

			return err
		})
	}

	if err := releaseG.Wait(); err != nil {
		return errors.NewStorageError("[DeleteCreated] could not release the spends of all %d created transaction(s) after a failed block phase", len(ghosts), err)
	}

	deleteG := new(errgroup.Group)
	deleteG.SetLimit(maxWorkers)

	for _, tx := range ghosts {
		hash := tx.TxIDChainHash()

		deleteG.Go(func() error {
			err := retryStoreCall(ctx, func() error {
				err := store.DeleteComplete(ctx, hash)
				if err != nil && (errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound)) {
					return nil
				}

				return err
			})
			if err != nil {
				logger.Errorf("[DeleteCreated] failed to remove %s after a failed block phase (%d attempts); it remains in the store: %v", hash.String(), deleteCreatedAttempts, err)
			}

			return err
		})
	}

	if err := deleteG.Wait(); err != nil {
		return errors.NewStorageError("[DeleteCreated] could not remove all %d created transaction(s) after a failed block phase", len(ghosts), err)
	}

	return nil
}

// releaseSurvivingSpends unspends every input of ghost whose parent is not in
// ghostSet. See DeleteCreated for why.
func releaseSurvivingSpends(ctx context.Context, store Store, ghost *bt.Tx, ghostSet map[chainhash.Hash]struct{}) error {
	ghostHash := ghost.TxIDChainHash()
	surviving := make([]int, 0, len(ghost.Inputs))
	decorate := false

	for vin, input := range ghost.Inputs {
		if _, isGhost := ghostSet[*input.PreviousTxIDChainHash()]; isGhost {
			continue
		}

		surviving = append(surviving, vin)

		if input.PreviousTxScript == nil {
			decorate = true
		}
	}

	if len(surviving) == 0 {
		return nil
	}

	// An outpoint-only replay carries no parent script or amount, and the
	// Aerospike unspend refuses an input whose UTXO hash it cannot verify.
	if decorate {
		if err := store.PreviousOutputsDecorate(ctx, ghost); err != nil {
			return errors.NewStorageError("[DeleteCreated] could not decorate the inputs of %s", ghostHash.String(), err)
		}
	}

	spends := make([]*Spend, 0, len(surviving))

	for _, vin := range surviving {
		input := ghost.Inputs[vin]

		utxoHash, err := util.UTXOHashFromInput(input)
		if err != nil {
			return errors.NewProcessingError("[DeleteCreated] could not hash input %d of %s", vin, ghostHash.String(), err)
		}

		spends = append(spends, &Spend{
			TxID:         input.PreviousTxIDChainHash(),
			Vout:         input.PreviousTxOutIndex,
			UTXOHash:     utxoHash,
			SpendingData: spendpkg.NewSpendingData(ghostHash, vin),
		})
	}

	return store.Unspend(ctx, spends)
}

// retryStoreCall runs fn up to deleteCreatedAttempts times with a linear
// backoff, giving up early when ctx ends.
func retryStoreCall(ctx context.Context, fn func() error) error {
	var err error

	for attempt := 0; attempt < deleteCreatedAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return errors.NewContextCanceledError("[DeleteCreated] cancelled while retrying", ctx.Err())
			case <-time.After(deleteCreatedBackoff * time.Duration(attempt)):
			}
		}

		if err = fn(); err == nil {
			return nil
		}
	}

	return err
}
