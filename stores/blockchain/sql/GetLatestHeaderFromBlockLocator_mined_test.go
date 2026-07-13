package sql

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestGetLatestBlockHeaderFromBlockLocator_MinedSetQuickValidated proves that
// mined_set and quick_validated stored on a block are returned correctly by
// GetLatestBlockHeaderFromBlockLocator.  Before the SQL fix these columns are
// absent from the SELECT list so they always scan as false.
func TestGetLatestBlockHeaderFromBlockLocator_MinedSetQuickValidated(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := t.Context()

	// Store block1 (height 1) without special flags.
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)
	err = s.SetBlockProcessedAt(ctx, block1.Hash())
	require.NoError(t, err)

	// Store block2 (height 2) with both flags set to true.
	_, _, err = s.StoreBlock(ctx, block2, "test_peer",
		options.WithMinedSet(true),
		options.WithQuickValidated(true),
	)
	require.NoError(t, err)
	err = s.SetBlockProcessedAt(ctx, block2.Hash())
	require.NoError(t, err)

	// Retrieve via GetLatestBlockHeaderFromBlockLocator; block2 is the best block
	// and also in the locator, so it should be returned.
	_, meta, err := s.GetLatestBlockHeaderFromBlockLocator(
		ctx,
		block2.Hash(),
		[]chainhash.Hash{*block2.Hash(), *block1.Hash()},
	)
	require.NoError(t, err)
	require.NotNil(t, meta)

	require.True(t, meta.MinedSet, "MinedSet must be true after storing block with WithMinedSet(true)")
	require.True(t, meta.QuickValidated, "QuickValidated must be true after storing block with WithQuickValidated(true)")
}
