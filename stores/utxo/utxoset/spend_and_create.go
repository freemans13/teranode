package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// SpendAndCreate spends the transaction's inputs and creates its outputs in ONE
// PostgreSQL transaction.
//
// This is the atomic implementation that utxo.SequentialSpendAndCreate exists to stand
// in for. The sequential helper must spend, then create, and on a create failure roll
// the spends back by calling Unspend with retries and exponential backoff — compensating
// logic that exists purely because the two halves cannot be made atomic in the stores it
// was written for.
//
// Here they can be, and the compensation disappears entirely. A spend is a DELETE and a
// create is an INSERT; issue both inside one transaction and a failure is a ROLLBACK, so
// the deletes simply never happened. There is no window in which the inputs are spent
// and the outputs are missing, no retry loop, and no dependence on Unspend — which
// matters for this store in particular, because its Unspend needs the spend journal.
//
// On ErrTxExists this is still all-or-nothing, and that is not a contract violation.
// The interface says the error is returned "with the spends left in place", which reads
// like a required partial commit but is not: if the transaction already exists then it
// was already processed, its inputs were already spent BY IT, and the spend phase wrote
// nothing. The postgres store makes that explicit -- its spend is
// ON CONFLICT (prev_tx_hash, prev_output_idx) DO NOTHING, so a re-spend is a no-op. So
// there is nothing to leave in place and nothing to undo, and a ROLLBACK reaches the
// same state a COMMIT would. Every path here is genuinely atomic.
//
// NOTE this store cannot yet reach that branch at all, and the reason is a real gap
// rather than a subtlety. Delete-on-spend has no spends row carrying spending_data, so
// a duplicate transaction arrives as per-input ErrSpent from the DELETE affecting zero
// rows -- indistinguishable from a genuine double-spend -- and never reaches the create
// phase to report ErrTxExists. Duplicate detection therefore has to happen BEFORE the
// spend: the applied_block ledger covers block application, and a mempool duplicate
// needs tx_meta. Until both exist, this store does not implement the ErrTxExists
// contract; it does not fake it either.
func (s *Store) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if options.CreateOnly && options.SpendOnly {
		return nil, nil, errors.NewInvalidArgumentError("[utxoset][SpendAndCreate] WithCreateOnly and WithSpendOnly are mutually exclusive")
	}

	// BEFORE the transaction is opened, never inside it. See
	// ensureSpendJournalPartition: the DDL needs its own pool connection, and taking one
	// while holding a transaction from the same pool deadlocks the pool under
	// concurrency.
	if !options.CreateOnly {
		if err := s.ensureSpendJournalPartition(ctx, blockHeight); err != nil {
			return nil, nil, err
		}
	}

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, errors.NewStorageError("[utxoset][SpendAndCreate] begin", err)
	}

	committed := false

	defer func() {
		if !committed {
			// Rollback IS the compensation. Nothing to undo by hand.
			_ = dbTx.Rollback(ctx)
		}
	}()

	var spends []*utxo.Spend

	if !options.CreateOnly {
		spends, err = s.spendIn(ctx, dbTx, tx, blockHeight, options.IgnoreFlags)
		if err != nil {
			return nil, spends, err
		}

		// Per-input failures are reported on the Spend records, not as a returned
		// error, so they must be inspected here: committing a partial spend would
		// leave the transaction half-applied.
		// Per-input failures are reported on the Spend records rather than as a returned
		// error, so they must be inspected here -- committing a partial spend would leave
		// the transaction half-applied. The aggregate is a UtxoError because that is what
		// callers match on; each input's specific cause (ErrFrozen, ErrSpent, immaturity)
		// stays on its own Spend record for conflict detection.
		var spendErrors []error

		for _, sp := range spends {
			if sp != nil && sp.Err != nil {
				spendErrors = append(spendErrors, sp.Err)
			}
		}

		if len(spendErrors) > 0 {
			return nil, spends, errors.NewUtxoError("[utxoset][SpendAndCreate] %d of %d inputs could not be spent", len(spendErrors), len(spends), spendErrors[0])
		}

		if options.SpendOnly {
			if err = dbTx.Commit(ctx); err != nil {
				return nil, spends, errors.NewStorageError("[utxoset][SpendAndCreate] commit (spend-only)", err)
			}

			committed = true

			return nil, spends, nil
		}
	}

	data, err := s.createIn(ctx, dbTx, tx, blockHeight, opts...)
	if err != nil {
		return nil, spends, err
	}

	if err = dbTx.Commit(ctx); err != nil {
		return nil, spends, errors.NewStorageError("[utxoset][SpendAndCreate] commit", err)
	}

	committed = true

	return data, spends, nil
}
