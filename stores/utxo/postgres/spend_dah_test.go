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
	// The pruner service is scoped to the Store instance, so each test's store
	// gets its own — no global reset needed.
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
