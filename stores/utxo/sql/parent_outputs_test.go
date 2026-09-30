package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

func TestParentOutputsForValidationSQLite(t *testing.T) {
	ctx := context.Background()

	t.Run("contract", func(t *testing.T) {
		db, _ := setup(ctx, t)
		tests.ParentOutputsForValidation(t, db)
	})

	t.Run("reads outputs, never inputs", func(t *testing.T) {
		db, _ := setup(ctx, t)
		tests.ParentOutputsReadsOutputsNotInputs(t, db)
	})
}

func TestParentOutputsForValidationPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	t.Run("contract", func(t *testing.T) {
		db, _ := setupPostgresStore(t)
		tests.ParentOutputsForValidation(t, db)
	})

	t.Run("reads outputs, never inputs", func(t *testing.T) {
		db, _ := setupPostgresStore(t)
		tests.ParentOutputsReadsOutputsNotInputs(t, db)
	})
}

// A store fault must come back as a per-outpoint Err, never as TxNotFound: the
// caller turns TxNotFound into a missing-parent verdict, and a transient fault
// reported that way would make a valid block look incomplete forever.
func TestParentOutputsForValidationFaultIsErrNotNotFound(t *testing.T) {
	ctx := context.Background()
	db, _ := setup(ctx, t)

	require.NoError(t, db.db.Close())

	answers, err := db.ParentOutputsForValidation(ctx, []utxo.Outpoint{{TxID: *tests.TXHash, Vout: 0}, {TxID: *tests.TXHash, Vout: 1}})
	require.NoError(t, err)
	require.Len(t, answers, 2)

	for _, a := range answers {
		require.Error(t, a.Err)
		require.Equal(t, utxo.ParentOutputUnknown, a.Status)
	}
}
