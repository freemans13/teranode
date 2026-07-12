package sql

// Task 9 (d)+(e) regression: StoreBlock never upserts quick_validated on a
// re-delivery.
//
// The crash-replay safety of the fail-closed below-checkpoint path depends on
// StoreBlock being a plain INSERT: a re-delivered block hits the unique-hash
// constraint and surfaces as ErrBlockExists WITHOUT rewriting any column,
// including quick_validated. This test pins that behaviour so a future refactor
// that turns StoreBlock into an upsert (ON CONFLICT ... DO UPDATE) cannot
// silently flip an already-committed row's quick_validated flag — which would
// break the block-assembly fast-path gate's fail-safe.

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestStoreBlock_NoUpsertQuickValidated stores a block with quick_validated=true,
// then re-delivers the identical block with quick_validated=false and asserts
// (d) the re-delivery returns ErrBlockExists (swallowed by commitBlock/AddBlock
// upstream) and (e) the stored quick_validated flag is unchanged (still true).
func TestStoreBlock_NoUpsertQuickValidated(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	ctx := context.Background()

	// First delivery: commit with quick_validated=true.
	_, _, err = s.StoreBlock(ctx, block1, "test-peer", options.WithQuickValidated(true))
	require.NoError(t, err)

	_, meta, err := s.GetBlockHeader(ctx, block1.Hash())
	require.NoError(t, err)
	require.True(t, meta.QuickValidated, "first delivery must persist quick_validated=true")

	// Re-delivery of the identical block, now with quick_validated=false. Must
	// hit the unique-hash constraint and return ErrBlockExists — never an upsert.
	_, _, err = s.StoreBlock(ctx, block1, "test-peer", options.WithQuickValidated(false))
	require.Error(t, err, "re-delivering an existing block must error")
	require.True(t, errors.Is(err, errors.ErrBlockExists),
		"re-delivery must return ErrBlockExists (so commitBlock/AddBlock can swallow it), got: %v", err)

	// The stored row is unchanged: quick_validated stays true, NOT overwritten to
	// false by the second call.
	_, meta, err = s.GetBlockHeader(ctx, block1.Hash())
	require.NoError(t, err)
	require.True(t, meta.QuickValidated,
		"quick_validated on the existing row must be unchanged after re-delivery (StoreBlock must not upsert)")
}
