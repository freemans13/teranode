package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetBlockHeadersAdditionalCoverage tests additional paths in GetBlockHeaders
func TestGetBlockHeadersAdditionalCoverage(t *testing.T) {
	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	cache := NewBlockchainCache(&settings.Settings{
		Block: settings.BlockSettings{
			StoreCacheSize: 10,
		},
	})

	t.Run("GetBlockHeadersSuccessfulRetrieval", func(t *testing.T) {
		// Create a chain of connected blocks
		var prevHash *chainhash.Hash = &chainhash.Hash{0}
		var headers []*model.BlockHeader

		for i := 1; i <= 5; i++ {
			header := &model.BlockHeader{
				Version:        1,
				Timestamp:      1234567890,
				Nonce:          uint32(i),
				HashPrevBlock:  prevHash,
				HashMerkleRoot: &chainhash.Hash{byte(i)},
				Bits:           *bits,
			}
			meta := &model.BlockHeaderMeta{Height: uint32(i)}

			result := cache.addBlockHeader(header, meta)
			assert.True(t, result)

			headers = append(headers, header)
			actualHash := header.Hash()
			prevHash = actualHash
		}

		// Get headers starting from the last block (should succeed)
		lastHash := headers[4].Hash() // 5th block (index 4)
		retrievedHeaders, retrievedMetas := cache.GetBlockHeaders(lastHash, 3)

		assert.NotNil(t, retrievedHeaders)
		assert.NotNil(t, retrievedMetas)
		assert.Equal(t, 3, len(retrievedHeaders))
		assert.Equal(t, 3, len(retrievedMetas))

		// Should retrieve blocks 5, 4, 3 (in that order)
		assert.Equal(t, headers[4], retrievedHeaders[0]) // Block 5
		assert.Equal(t, headers[3], retrievedHeaders[1]) // Block 4
		assert.Equal(t, headers[2], retrievedHeaders[2]) // Block 3
	})

	t.Run("GetBlockHeadersFromHeightSuccessfulRetrieval", func(t *testing.T) {
		// Create a fresh cache
		newCache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 10,
			},
		})

		// Create a chain of connected blocks with specific heights
		var prevHash *chainhash.Hash = &chainhash.Hash{0}
		var headers []*model.BlockHeader

		for i := 1; i <= 5; i++ {
			header := &model.BlockHeader{
				Version:        1,
				Timestamp:      1234567890,
				Nonce:          uint32(i),
				HashPrevBlock:  prevHash,
				HashMerkleRoot: &chainhash.Hash{byte(i)},
				Bits:           *bits,
			}
			meta := &model.BlockHeaderMeta{Height: uint32(i)}

			result := newCache.addBlockHeader(header, meta)
			assert.True(t, result)

			headers = append(headers, header)
			actualHash := header.Hash()
			prevHash = actualHash
		}

		// Get headers starting from height 2, limit 3
		retrievedHeaders, retrievedMetas := newCache.GetBlockHeadersFromHeight(2, 3)

		assert.NotNil(t, retrievedHeaders)
		assert.NotNil(t, retrievedMetas)
		assert.Equal(t, 3, len(retrievedHeaders))
		assert.Equal(t, 3, len(retrievedMetas))

		// Should retrieve blocks at heights 2, 3, 4
		assert.Equal(t, uint32(2), retrievedMetas[0].Height)
		assert.Equal(t, uint32(3), retrievedMetas[1].Height)
		assert.Equal(t, uint32(4), retrievedMetas[2].Height)
	})

	t.Run("GetBlockHeadersHashNotFound", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 10,
			},
		})

		// Add one block
		header := &model.BlockHeader{
			Version:        1,
			Timestamp:      1234567890,
			Nonce:          1,
			HashPrevBlock:  &chainhash.Hash{0},
			HashMerkleRoot: &chainhash.Hash{1},
			Bits:           *bits,
		}
		meta := &model.BlockHeaderMeta{Height: 1}

		cache.addBlockHeader(header, meta)

		// Try to get headers starting from a non-existent hash
		nonExistentHash := chainhash.Hash{99}
		headers, metas := cache.GetBlockHeaders(&nonExistentHash, 1)

		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("GetBlockHeadersFromHeightNotFound", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 10,
			},
		})

		// Add one block at height 5
		header := &model.BlockHeader{
			Version:        1,
			Timestamp:      1234567890,
			Nonce:          1,
			HashPrevBlock:  &chainhash.Hash{0},
			HashMerkleRoot: &chainhash.Hash{1},
			Bits:           *bits,
		}
		meta := &model.BlockHeaderMeta{Height: 5}

		cache.addBlockHeader(header, meta)

		// Try to get headers starting from height 1 (not in cache)
		headers, metas := cache.GetBlockHeadersFromHeight(1, 1)

		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})
}

// TestInsertGenesisTransactionAdditionalCoverage tests edge cases in insertGenesisTransaction
func TestInsertGenesisTransactionAdditionalCoverage(t *testing.T) {
	// This function is already well tested but let's add some coverage for specific paths
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("GenesisBlockAlreadyExists", func(t *testing.T) {
		// Create store which will insert genesis block
		store, err := createTestStore(logger, tSettings)
		require.NoError(t, err)
		defer store.Close()

		// Genesis block should already exist, so another call to insertGenesisTransaction
		// should handle the existing case
		err = store.insertGenesisTransaction(logger)
		assert.NoError(t, err)
	})
}

// TestResetBlocksCacheErrorHandling tests error handling in ResetBlocksCache
func TestResetBlocksCacheErrorHandling(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("SuccessfulRebuildAfterAddingBlocks", func(t *testing.T) {
		store, err := createTestStore(logger, tSettings)
		require.NoError(t, err)
		defer store.Close()

		// Add some data to the cache first
		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		header := &model.BlockHeader{
			Version:        1,
			Timestamp:      1234567890,
			Nonce:          1,
			HashPrevBlock:  &chainhash.Hash{0},
			HashMerkleRoot: &chainhash.Hash{1},
			Bits:           *bits,
		}
		meta := &model.BlockHeaderMeta{Height: 1}

		store.blocksCache.AddBlockHeader(header, meta)

		// Now reset the cache
		err = store.ResetBlocksCache(context.Background())
		assert.NoError(t, err)
	})
}

// createTestStore is a helper function to create a test store
func createTestStore(logger ulogger.Logger, tSettings *settings.Settings) (*SQL, error) {
	storeURL, err := url.Parse("sqlitememory://")
	if err != nil {
		return nil, err
	}
	return New(logger, storeURL, tSettings)
}
