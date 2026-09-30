package sql

import (
	"context"
	"testing"

	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
)

func spendAndCreateMultiSuite(t *testing.T, newStore func(t *testing.T) utxostore.Store) {
	t.Run("matches a loop of SpendAndCreate", func(t *testing.T) { tests.SpendAndCreateMultiMatchesLoop(t, newStore(t)) })
	t.Run("a spend fails partway", func(t *testing.T) { tests.SpendAndCreateMultiSpendFailsPartway(t, newStore(t)) })
	t.Run("a parent in the list exists", func(t *testing.T) { tests.SpendAndCreateMultiParentExists(t, newStore(t)) })
	t.Run("a refusal writes nothing", func(t *testing.T) { tests.SpendAndCreateMultiRefusalWritesNothing(t, newStore(t)) })
	t.Run("repeat at every cut point", func(t *testing.T) { tests.SpendAndCreateMultiRepeatAtCutPoints(t, newStore(t)) })
}

func TestSpendAndCreateMultiSQLite(t *testing.T) {
	spendAndCreateMultiSuite(t, func(t *testing.T) utxostore.Store {
		db, _ := setup(context.Background(), t)
		return db
	})
}

func TestSpendAndCreateMultiPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	db, _ := setupPostgresStore(t)

	spendAndCreateMultiSuite(t, func(t *testing.T) utxostore.Store { return db })
}
