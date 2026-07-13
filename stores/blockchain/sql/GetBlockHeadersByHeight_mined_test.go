package sql

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestGetBlockHeadersByHeight_MinedSetQuickValidated proves that mined_set and
// quick_validated stored on a block are returned correctly by
// GetBlockHeadersByHeight.  Before the SQL fix these columns are absent from
// the SELECT list so they always scan as false.
func TestGetBlockHeadersByHeight_MinedSetQuickValidated(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := t.Context()

	// Store block1 (height 1) with both flags set to true.
	_, _, err = s.StoreBlock(ctx, block1, "test_peer",
		options.WithMinedSet(true),
		options.WithQuickValidated(true),
	)
	require.NoError(t, err)

	// Retrieve block at height 1 only.
	_, metas, err := s.GetBlockHeadersByHeight(ctx, 1, 1)
	require.NoError(t, err)
	require.NotEmpty(t, metas, "expected at least one result for height 1")

	meta := metas[0]
	require.True(t, meta.MinedSet, "MinedSet must be true after storing block with WithMinedSet(true)")
	require.True(t, meta.QuickValidated, "QuickValidated must be true after storing block with WithQuickValidated(true)")
}
