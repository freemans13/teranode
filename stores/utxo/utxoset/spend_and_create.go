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
// matters for this store in particular, because its Unspend needs the undo journal that
// only exists above the checkpoint.
//
// One contract point deserves attention rather than silent compliance. The interface
// specifies that ErrTxExists from the create phase is returned WITH THE SPENDS LEFT IN
// PLACE. That is a deliberately non-atomic outcome, and expressing it inside a
// transaction means committing the spends and returning an error, which is the one path
// here that is not simply "all or nothing". PR 1326 flags this as an open question --
// whether the contract survives real atomicity -- and this implementation honours the
// documented behaviour rather than quietly changing it. Note the branch is currently
// unreachable: this store cannot yet detect a duplicate transaction, because its key is
// deliberately non-unique and duplicate detection is the applied_block ledger's job.
func (s *Store) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if options.CreateOnly && options.SpendOnly {
		return nil, nil, errors.NewInvalidArgumentError("[utxoset][SpendAndCreate] WithCreateOnly and WithSpendOnly are mutually exclusive")
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
		for _, sp := range spends {
			if sp != nil && sp.Err != nil {
				return nil, spends, errors.NewProcessingError("[utxoset][SpendAndCreate] input %d: %w", sp.Vout, sp.Err)
			}
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
		if errors.Is(err, errors.ErrTxExists) {
			// The documented exception: keep the spends, report the duplicate. Commit
			// so they survive, then return the error.
			if cErr := dbTx.Commit(ctx); cErr != nil {
				return nil, spends, errors.NewStorageError("[utxoset][SpendAndCreate] commit (ErrTxExists)", cErr)
			}

			committed = true

			return nil, spends, err
		}

		return nil, spends, err
	}

	if err = dbTx.Commit(ctx); err != nil {
		return nil, spends, errors.NewStorageError("[utxoset][SpendAndCreate] commit", err)
	}

	committed = true

	return data, spends, nil
}
