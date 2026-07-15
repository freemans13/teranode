package netsync

// Taxonomy guard for BlockProcessingErrorIsPeerFault — the single source that
// decides whether a failed block disconnects the delivering peer (peer fault)
// or is tolerated and re-fetched (local condition).
//
// This test is load-bearing precisely because the classification walks WRAPPED
// error chains: any code path that re-wraps a block-processing error could
// silently flip the outcome. The 2026-07-15 mainnet freeze was a local
// block-assembly gate timeout (a ProcessingError) being treated as peer fault,
// executing the sync peer and starving the node to zero peers. Only a
// consensus-invalid block (errors.ErrBlockInvalid, however deeply wrapped) may
// disconnect the peer.

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func TestBlockProcessingErrorIsPeerFault_Taxonomy(t *testing.T) {
	t.Run("nil is never peer fault", func(t *testing.T) {
		require.False(t, BlockProcessingErrorIsPeerFault(nil))
	})

	// PEER FAULT — consensus-invalid blocks. These, and only these, disconnect.
	peerFault := []struct {
		name string
		err  error
	}{
		{"block invalid (bare)", errors.ErrBlockInvalid},
		{"block invalid (constructed)", errors.NewBlockInvalidError("bad merkle root")},
		{"block invalid wrapped once", errors.NewProcessingError("prepareSubtrees failed", errors.NewBlockInvalidError("duplicate tx CVE-2012-2459"))},
		{"block invalid wrapped deep", errors.NewServiceError("handle", errors.NewProcessingError("validate", errors.NewBlockInvalidError("bad pow")))},
	}
	for _, tc := range peerFault {
		t.Run("peer fault: "+tc.name, func(t *testing.T) {
			require.True(t, BlockProcessingErrorIsPeerFault(tc.err),
				"a consensus-invalid block (however wrapped) MUST be classified peer fault")
		})
	}

	// LOCAL CONDITIONS — the peer did nothing wrong. These are tolerated and
	// re-fetched, never a disconnect. The gate-timeout case is the exact 07-15
	// freeze trigger.
	local := []struct {
		name string
		err  error
	}{
		{"block-assembly gate timeout (the 07-15 freeze)", errors.NewProcessingError("[waitForBlockAssemblyCachePoll] block assembly is behind, block height 465572, cached 465571, gave up after 30s")},
		{"generic processing error", errors.NewProcessingError("transient")},
		{"service error", errors.NewServiceError("grpc unavailable")},
		{"storage error", errors.NewStorageError("postgres hiccup")},
		{"parent not found (catchup retry)", errors.NewBlockNotFoundError("parent not yet committed")},
	}
	for _, tc := range local {
		t.Run("local: "+tc.name, func(t *testing.T) {
			require.False(t, BlockProcessingErrorIsPeerFault(tc.err),
				"a local condition MUST NOT be classified peer fault (that executes an innocent peer)")
		})
	}
}
