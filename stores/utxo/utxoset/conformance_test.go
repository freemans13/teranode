package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// The suite used to be opt-in behind UTXOSET_CONFORMANCE, because it failed while the store
// was incomplete and a permanently red package makes a real regression indistinguishable from
// the known gap. It passes now, so the gate is gone: it runs with everything else, and a
// failure means a regression rather than a to-do list.

// The store-agnostic conformance suite is this store's specification. Every other
// implementation (postgres, aerospike, sql) is held to it, and the same invariants must
// hold here regardless of how radically the storage model differs underneath.
//
// Each subtest is named for the capability it pins, so a failure says what is missing rather
// than merely that something is. They all pass; a suite not listed here is one this store does
// not implement the entry point for, not one that is failing quietly.
func TestConformance(t *testing.T) {
	t.Run("Store", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.Store(t, db)
	})

	t.Run("Spend", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.Spend(t, db)
	})

	t.Run("Freeze", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.Freeze(t, db)
	})

	t.Run("SetMined", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SetMined(t, db)
	})

	t.Run("Conflicting", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.Conflicting(t, db)
	})

	t.Run("Restore", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.Restore(t, db)
	})

	t.Run("UnspendIdempotent", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.UnspendIdempotent(t, db)
	})

	t.Run("SetMinedWithSpent", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SetMinedWithSpent(t, db)
	})

	// The full round trip through the membership table: a longest-chain stamp settles the
	// transaction and moves it into tx_mined, MarkTransactionsOnLongestChain(false) moves it
	// back out with the mempool marker at the current tip, and (true) settles it again. It was
	// parked while only the outward move existed.
	t.Run("SetMinedUnminedSince", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SetMinedUnminedSince(t, db)
	})

	// The delete-at-height lifecycle: a mempool-created tx stamped mined on the longest
	// chain moves into tx_mined, every output gets spent, and Prune(1_000_000) at that
	// height drops every membership window below the journal-retention cutoff wholesale
	// (there is no per-row DAH sweep in this design — see pruner.go). The coins are
	// already gone from the spend, so once the window holding the tx's identity is
	// dropped, a lookup misses.
	t.Run("MinedThenSpendAllPrunes", func(t *testing.T) {
		db, _ := newTestStore(t)

		svc, err := db.GetPrunerService()
		require.NoError(t, err)

		tests.MinedThenSpendAllPrunes(t, db, svc)
	})

	// The six SpendAndCreate entry points. The spec named them as ones this design should
	// enable, and they are the cross-store contract for the option C1's own-output coin guard
	// turns on: WithCreateOnly skips the spend phase, which is the path a mempool create takes
	// when the validator's CreateConflicting branch fires.
	//
	// The package's own spend_and_create_batch_test.go covers similar ground, but it is written
	// against this store's internals. These are written against the interface, which is what
	// makes them a contract rather than a second opinion.
	t.Run("SpendAndCreate", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendAndCreate(t, db)
	})

	t.Run("SpendAndCreateCreateOnly", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendAndCreateCreateOnly(t, db)
	})

	t.Run("SpendAndCreateSpendOnly", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendAndCreateSpendOnly(t, db)
	})

	t.Run("SpendAndCreateTxExistsKeepsSpends", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendAndCreateTxExistsKeepsSpends(t, db)
	})

	t.Run("SpendAndCreateSpendErrorSurfacesPerInput", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendAndCreateSpendErrorSurfacesPerInput(t, db)
	})

	t.Run("SpendAndCreateInvalidOptions", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendAndCreateInvalidOptions(t, db)
	})

	// The conflict-resolution write-ahead log. Block assembly reads it once at startup and
	// replays whatever a crash left half-finished, so an intent that does not survive is a
	// conflict resolution that silently never completes.
	t.Run("ConflictWAL", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.ConflictWAL(t, db)
	})
}
