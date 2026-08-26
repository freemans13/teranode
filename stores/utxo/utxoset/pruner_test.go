package utxoset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPrunerServiceContract covers what the pruner service must satisfy for the daemon to
// boot at all, separately from what it reclaims.
//
// services/pruner/server.go type-asserts the UTXO store to pruner.PrunerServiceProvider
// and refuses to start without it, which crash-loops the daemon. Switching the service off
// in settings is not an option: a feature disabled on a node to dodge a gap is not a fix.
//
// What it reclaims is the spend journal, and that is covered by
// TestSpendJournalReclaimIsDrivenByThePruner. There is no DAH sweep to run: the DELETE that
// spends an output frees its space and its index entry in the same statement, so the
// per-row reclaim that dominated the previous store does not exist here.
func TestPrunerServiceContract(t *testing.T) {
	s, ctx := newTestStore(t)

	svc, err := s.GetPrunerService()
	require.NoError(t, err, "the pruner service refuses to start without this")
	require.NotNil(t, svc, "a nil service panics the caller, which only checks err")

	svc.Start(ctx) // must not block: the service calls Prune per block, there is no loop here

	svc.AddObserver(nil) // must not panic

	// Below retention there is nothing aged out yet, and asking must not be an error.
	n, err := svc.Prune(ctx, 100, "deadbeef")
	require.NoError(t, err)
	require.Zero(t, n)

	// Nor at a height with no journal at all. Reclaim has to tolerate being called on a
	// store that has never spent anything, because the pruner service starts with the node.
	n, err = svc.Prune(ctx, DefaultSpendJournalRetentionBlocks*3, "deadbeef")
	require.NoError(t, err)
	require.Zero(t, n, "no transaction records are deleted yet, and journal rows do not belong in that counter")
}
