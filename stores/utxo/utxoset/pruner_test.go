package utxoset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPrunerServiceIsANoOp is the second thing that stopped the node booting.
//
// The pruner service is mandatory: services/pruner/server.go:116-119 type-asserts the
// UTXO store to pruner.PrunerServiceProvider and refuses to start without it, which
// crash-loops the daemon. Switching the service off in settings is not an option, since
// a feature disabled on a node to dodge a gap is not a fix.
//
// There is genuinely nothing to prune. In an append-only store the pruner is what
// reclaims space from spent outputs, and it was a large part of why the previous store
// failed: sweep, pruner and vacuum together measured 76.7% of all disk reads and 52% of
// statement write-ahead log volume, with the watermark thousands of blocks behind the
// tip. Here the DELETE that spends an output IS the reclaim, so there is no backlog that
// can fall behind, and reporting zero records pruned is exact rather than evasive.
func TestPrunerServiceIsANoOp(t *testing.T) {
	s, ctx := newTestStore(t)

	svc, err := s.GetPrunerService()
	require.NoError(t, err, "the pruner service refuses to start without this")
	require.NotNil(t, svc, "a nil service panics the caller, which only checks err")

	svc.Start(ctx) // must not block

	n, err := svc.Prune(ctx, 100, "deadbeef")
	require.NoError(t, err)
	require.Zero(t, n, "delete-on-spend reclaims at spend time, so nothing is left to prune")

	svc.AddObserver(nil) // must not panic
}
