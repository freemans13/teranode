package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
)

// The suite used to be opt-in behind UTXOSET_CONFORMANCE, because it failed while the store
// was incomplete and a permanently red package makes a real regression indistinguishable from
// the known gap. It passes now, so the gate is gone: it runs with everything else, and a
// failure means a regression rather than a to-do list.

// The store-agnostic conformance suite is this store's specification. Every other
// implementation (postgres, aerospike, sql) is held to it, and the same invariants must
// hold here regardless of how radically the storage model differs underneath.
//
// These are expected to FAIL until the corresponding methods land. That is deliberate:
// the failures are the worklist, expressed in the repo's own terms rather than a
// hand-written approximation of them. Each subtest is named for the capability it pins
// so a failure says what is missing rather than merely that something is.
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

	t.Run("SetMinedUnminedSince", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SetMinedUnminedSince(t, db)
	})

	// tests.MinedThenSpendAllPrunes is deliberately not run here: it creates through the
	// mempool path and relies on the longest-chain stamp moving rows out of the identity
	// table, which is a later stage's change, not this one's.
}
