package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// TestMinedThenSpendAllPrunes_SQLite exercises the per-row spend path
// (trySendSpendBatchPerRow) via the in-memory SQLite backend.
func TestMinedThenSpendAllPrunes_SQLite(t *testing.T) {
	// Pruner service is a process-wide singleton; reset so it binds to THIS
	// test's Store rather than a stale one from a different backend or run.
	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _ := setup(ctx, t)

	provider := any(store).(pruner.PrunerServiceProvider)
	prunerSvc, err := provider.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, prunerSvc, "pruner service must be available")
	prunerSvc.Start(ctx)

	tests.MinedThenSpendAllPrunes(t, store, prunerSvc)
}

// TestMinedThenSpendAllPrunes_Postgres exercises the Postgres bulk spend path
// (trySendSpendBatchBulk) via a testcontainer-backed Postgres database.
func TestMinedThenSpendAllPrunes_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	store, ctx := setupPostgresStore(t)

	provider := any(store).(pruner.PrunerServiceProvider)
	prunerSvc, err := provider.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, prunerSvc, "pruner service must be available")
	prunerSvc.Start(ctx)

	tests.MinedThenSpendAllPrunes(t, store, prunerSvc)
}
