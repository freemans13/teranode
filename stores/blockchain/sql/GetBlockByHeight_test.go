package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetBlockByHeight(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("block 0 - genesis block", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		block, err := s.GetBlockByHeight(context.Background(), 0)
		require.NoError(t, err)

		assertRegtestGenesis(t, block.Header)

		// block
		assert.Equal(t, uint64(1), block.TransactionCount)
		assert.Len(t, block.Subtrees, 0)
	})

	t.Run("blocks ", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		id, _, err := s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		block1.ID = uint32(id) //nolint:gosec

		id, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		block2.ID = uint32(id) //nolint:gosec

		block, err := s.GetBlockByHeight(context.Background(), 1)
		require.NoError(t, err)

		assert.Equal(t, block1.String(), block.String())

		block, err = s.GetBlockByHeight(context.Background(), 2)
		require.NoError(t, err)

		assert.Equal(t, block2.String(), block.String())
	})
}
