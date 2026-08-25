package utxoset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUnminedSetIsEmptyNotUnimplemented is what lets the node boot.
//
// Block assembly calls both of these during startup, before a single block is
// processed, and treats an error from either as fatal (BlockAssembler.go:936-942 and
// :2629-2634). Answering errM1 therefore crash-loops the daemon and the node never
// syncs at all, which is what a deploy of this store did.
//
// Empty is not a shortcut here, it is the true answer. Both the unmined set and the
// conflict-intent log are transaction-level state that lives in tx_meta, and this store
// has no tx_meta, so it cannot be holding an unmined transaction or a pending intent.
// Reporting "none" describes the store exactly. When tx_meta lands these must start
// returning real data, and the conformance suite is what will catch it if they do not.
func TestUnminedSetIsEmptyNotUnimplemented(t *testing.T) {
	s, ctx := newTestStore(t)

	it, err := s.GetUnminedTxIterator()
	require.NoError(t, err, "block assembly treats an error here as fatal at startup")
	require.NotNil(t, it, "a nil iterator panics the caller, which only checks err")

	defer func() { require.NoError(t, it.Close()) }()

	batch, err := it.Next(ctx)
	require.NoError(t, err)
	require.Empty(t, batch, "a store with no tx_meta cannot hold an unmined transaction")
	require.NoError(t, it.Err())

	intents, err := s.PendingConflictIntents(ctx)
	require.NoError(t, err, "block assembly replays these at startup")
	require.Empty(t, intents)
}
