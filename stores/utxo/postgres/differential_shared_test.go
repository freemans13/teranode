package postgres

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
)

// These wire the shared gold-standard differential tests (defined in
// stores/utxo/tests/tests.go) to the postgres backend. They are expected to
// PASS on aerospike and sql and currently FAIL on postgres, pinpointing the
// divergences found while reviewing PR #684.

// TestSetMinedUnsetOnMissingTx_Postgres: SetMinedMulti(UnsetMined=true) on a
// missing tx must be a tolerated no-op (Interface.go:295-303). Postgres returns
// a StorageError (mined.go:244-248).
func TestSetMinedUnsetOnMissingTx_Postgres(t *testing.T) {
	store, _ := setupTestStore(t)
	tests.SetMinedUnsetOnMissingTx(t, store)
}

// TestUnsetMinedPreservesUnminedSince_Postgres: unsetting one of several
// non-longest-chain block entries must preserve UnminedSince. Postgres clobbers
// it to 0 via the ELSE NULL arm (mined.go:230-232).
func TestUnsetMinedPreservesUnminedSince_Postgres(t *testing.T) {
	store, _ := setupTestStore(t)
	tests.UnsetMinedPreservesUnminedSinceWhenNonLCBlocksRemain(t, store)
}

// TestRemoveBlockIDsKeepsParallelArraysAligned_Postgres: RemoveBlockIDs must drop
// the matching entry from all three parallel arrays. Postgres previously trimmed
// only block_ids (remove_block_ids.go), misaligning block_heights/subtree_idxs.
func TestRemoveBlockIDsKeepsParallelArraysAligned_Postgres(t *testing.T) {
	store, _ := setupTestStore(t)
	tests.RemoveBlockIDsKeepsParallelArraysAligned(t, store)
}

// TestUnspendFlagAsLockedLocksParent_Postgres: Unspend(flagAsLocked=true) must
// reverse the spend and lock the parent atomically (spend.go:681-687).
func TestUnspendFlagAsLockedLocksParent_Postgres(t *testing.T) {
	store, _ := setupTestStore(t)
	tests.UnspendFlagAsLockedLocksParent(t, store)
}
