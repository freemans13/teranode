package sql

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestGetForkedBlockHeaders_MinedSetQuickValidated proves that mined_set and
// quick_validated stored on a fork block are returned correctly by
// GetForkedBlockHeaders.  Before the SQL fix these columns are absent from the
// SELECT list so they always scan as false.
//
// Setup: genesis -> block1 -> block2 (main chain).
// Fork:  genesis -> block1 -> blockAlt2 (stored with flags = true).
// GetForkedBlockHeaders(block2.Hash(), 10) returns blocks NOT in block2's
// ancestor set, so blockAlt2 should appear in the result.
func TestGetForkedBlockHeaders_MinedSetQuickValidated(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := t.Context()

	// Store the main-chain blocks.
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)
	err = s.SetBlockProcessedAt(ctx, block1.Hash())
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)
	err = s.SetBlockProcessedAt(ctx, block2.Hash())
	require.NoError(t, err)

	// Build a distinct alternative block at height 2 parented off block1.
	// It needs a different merkle root so its hash differs from block2.
	altMerkleRoot, err := chainhash.NewHashFromStr("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, err)
	bitsVal, _ := model.NewNBitFromString("207fffff")
	altBlock2 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1111111111,
			Nonce:          9999,
			HashPrevBlock:  block1.Header.Hash(),
			HashMerkleRoot: altMerkleRoot,
			Bits:           *bitsVal,
		},
		Height:           2,
		TransactionCount: 1,
	}

	_, _, err = s.StoreBlock(ctx, altBlock2, "test_peer",
		options.WithMinedSet(true),
		options.WithQuickValidated(true),
	)
	require.NoError(t, err)

	altHash := altBlock2.Header.Hash()

	// GetForkedBlockHeaders from block2's perspective: returns all blocks
	// that are NOT in block2's ancestor chain, which includes altBlock2.
	headers, metas, err := s.GetForkedBlockHeaders(ctx, block2.Hash(), 10)
	require.NoError(t, err)
	require.NotEmpty(t, headers, "expected altBlock2 to appear as a forked block")

	// Find the altBlock2 entry in the results.
	var found *model.BlockHeaderMeta
	for i, h := range headers {
		if h.Hash().String() == altHash.String() {
			found = metas[i]
			break
		}
	}
	require.NotNil(t, found, "altBlock2 must be present in forked block headers")
	require.True(t, found.MinedSet, "MinedSet must be true for altBlock2 stored with WithMinedSet(true)")
	require.True(t, found.QuickValidated, "QuickValidated must be true for altBlock2 stored with WithQuickValidated(true)")
}
