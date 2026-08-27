package utxoset

import (
	"context"

	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
)

// journalPruner reclaims the spend journal, and nothing else.
//
// There is no DAH sweep here and no pile of spent rows to walk. The DELETE that spends an
// output frees its space and its index entry in the same statement that authorises the
// spend, so the reclaim that dominated the previous store does not exist: its sweep,
// pruner and the vacuum they forced together measured 76.7% of all disk reads and 52% of
// statement write-ahead log volume, with the watermark thousands of blocks behind the tip.
//
// What IS left to reclaim is the spend journal, which retains DefaultSpendJournalRetentionBlocks
// of undo history in height-ranged partitions. That is a catalog operation -- drop the
// partitions that have aged out -- and this is where it runs.
//
// It runs HERE, rather than on the spend path where it used to, for three reasons. The
// spend path had to swallow the error to avoid failing a spend over old history, and it
// swallowed a real one for the entire life of the branch. DETACH CONCURRENTLY waits for
// every open transaction on the parent, which from inside a spend stalls the pipeline.
// And services/pruner/worker.go already provides exactly the trigger this needs: once per
// block, off its own single-threaded goroutine, with the returned error logged at Errorf
// against a metric and the call timed. Nothing had to be built to get that.
//
// Inherited from that worker, and worth knowing rather than discovering: it gates on block
// assembly being caught up, skips heights at or below pruner_min_block_height, and
// deduplicates to the latest notification when it falls behind. So a session can be
// skipped or can jump several leaves at once. Reclaim is written to absorb that -- every
// call drops every leaf below the cutoff, not one per call -- so a skipped block defers
// the drop rather than losing it, and the journal simply carries more history until the
// next session lands.
type journalPruner struct {
	store *Store
}

// Start does nothing. The pruner service drives Prune once per block; there is no
// background loop here to fall behind.
func (journalPruner) Start(_ context.Context) {}

// Prune reclaims journal leaves that have aged out at this height.
//
// The height is the tip, not a retention-adjusted one, so the retention is applied here.
//
// It reports ZERO records processed, and that is exact rather than evasive. The caller
// adds the return value to a counter of child transaction records deleted, and no
// transaction records are deleted yet -- tx_bounded and tx_mined do not exist. Reporting
// discarded journal rows in that counter would put two different units in one metric. When
// the pruner session gains its tx_mined step, that step's count is what belongs here, and
// the journal drop stays last: dropping a leaf destroys the record of which transactions
// had an output spent in that window, which is the work list the step reads.
func (p journalPruner) Prune(ctx context.Context, height uint32, _ string) (int64, error) {
	// The body horizon and the journal horizon are DIFFERENT numbers, 288 against 1440, so
	// the two reclaims must not be gated behind one another. Doing so left the bodies
	// unreclaimed for the whole of early sync, which is exactly when the disk is tightest.
	bodies, err := p.store.dropTxBodyWindowsBelow(ctx, height)
	if err != nil {
		return 0, err
	}

	if bodies > 0 {
		p.store.logger.Infof("[utxoset] pruner dropped %d transaction-body windows past the %d-block horizon",
			bodies, p.store.bodyRetention)
	}

	if height <= p.store.journalRetention {
		return 0, nil
	}

	cutoff := height - p.store.journalRetention

	// The session, in the one order that is safe.
	//
	// Each retiring journal partition is read as a work list BEFORE it is dropped, because
	// dropping it destroys the record of which transactions had an output spent in that
	// window. Then the partition goes. Then the body windows past their own horizon go.
	var reclaimed int

	dropped, err := p.store.dropSpendJournalPartitionsBelow(ctx, cutoff,
		func(ctx context.Context, partition string) error {
			n, rerr := p.store.reclaimFromPartition(ctx, partition, height)
			if rerr != nil {
				return rerr
			}

			reclaimed += n

			return nil
		})
	if err != nil {
		return 0, err
	}

	if dropped > 0 || reclaimed > 0 {
		p.store.logger.Infof("[utxoset] pruner reclaimed %d transaction rows and dropped %d spend-journal leaves below height %d",
			reclaimed, dropped, cutoff)
	}

	// Still zero records processed, and still exact. The caller adds this to a counter of
	// child transaction records deleted by a delete-at-height sweep, which this store does
	// not have. Reporting identity rows or body windows there would put three units in one
	// metric; they are logged above instead.
	return 0, nil
}

// AddObserver accepts and discards. Observers are notified when a pruning cycle completes,
// and nothing registers one: the SQL pruner's AddObserver is also a no-op and
// services/pruner/server.go never calls it.
func (journalPruner) AddObserver(_ pruner.Observer) {}

// GetPrunerService satisfies pruner.PrunerServiceProvider.
//
// The pruner service is mandatory: services/pruner/server.go type-asserts the UTXO store
// to this interface and refuses to start without it, taking the whole daemon down. It is a
// real value rather than a nil, even though the provider interface documents nil as
// meaning "unsupported", because services/pruner/server.go stores the result and uses it
// without a nil check.
func (s *Store) GetPrunerService() (pruner.Service, error) {
	return journalPruner{store: s}, nil
}
