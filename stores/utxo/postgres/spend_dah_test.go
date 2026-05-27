package postgres

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// TestMinedThenSpendAllPrunes_Postgres verifies that after a tx is mined on the
// longest chain and then has every one of its outputs spent via the normal
// Spend path, the pruner is able to reclaim it. Covers both the bulk spend
// path (bulkSpendSQL) and the interaction with SetMinedMulti's DAH branch.
func TestMinedThenSpendAllPrunes_Postgres(t *testing.T) {
	// Pruner service is a process-wide singleton; reset so it binds to THIS
	// test's Store (otherwise a later test sees a closed pool from setupTestStore's Cleanup).
	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	store, _ := setupTestStore(t)

	// Use a cancellable ctx so the Worker 2 cursor goroutine exits when the test ends.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	provider := any(store).(pruner.PrunerServiceProvider)
	prunerSvc, err := provider.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, prunerSvc)
	prunerSvc.Start(ctx)

	tests.MinedThenSpendAllPrunes(t, store, prunerSvc)
}
