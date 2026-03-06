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

func TestCheckBlockIsInCurrentChain_EmptyBlockIDs(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Test with empty block IDs array
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{})
	require.NoError(t, err)
	assert.False(t, result, "Empty block IDs should return false")
}

func TestCheckBlockIsInCurrentChain_SingleBlockInChain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store a block in the main chain
	blockID, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	// Check if the block is in current chain
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID)})
	require.NoError(t, err)
	assert.True(t, result, "Block in main chain should return true")
}

func TestCheckBlockIsInCurrentChain_MultipleBlocksInChain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store multiple blocks in sequence
	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	blockID3, _, err := s.StoreBlock(context.Background(), block3, "")
	require.NoError(t, err)

	// Check if all blocks are in current chain
	blockIDs := []uint32{uint32(blockID1), uint32(blockID2), uint32(blockID3)}
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
	require.NoError(t, err)
	assert.True(t, result, "All blocks in main chain should return true")
}

func TestCheckBlockIsInCurrentChain_BlockNotInChain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store a block
	_, _, err = s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	// Store another block to create a chain
	_, _, err = s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Non-existent block IDs are not in the off-chain set, so they're treated
	// as on-chain. In practice, callers only pass block IDs from the database.
	nonExistentID := uint32(999999)
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{nonExistentID})
	require.NoError(t, err)
	assert.True(t, result, "Unknown block IDs not in off-chain set are treated as on-chain")
}

func TestCheckBlockIsInCurrentChain_SingleNonExistentBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store at least one block to have a chain
	_, _, err = s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	// Non-existent block IDs are not in the off-chain set, so they're treated
	// as on-chain. In practice, callers only pass block IDs from the database.
	nonExistentID := uint32(999999)
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{nonExistentID})
	require.NoError(t, err)
	assert.True(t, result, "Unknown block IDs not in off-chain set are treated as on-chain")
}

func TestCheckBlockIsInCurrentChain_MixedValidAndInvalidBlocks(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store blocks
	_, _, err = s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Non-existent block IDs are not in the off-chain set, so they're treated
	// as on-chain. In practice, callers only pass block IDs from the database.
	mixedIDs := []uint32{999999, 999998}
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), mixedIDs)
	require.NoError(t, err)
	assert.True(t, result, "Unknown block IDs not in off-chain set are treated as on-chain")
}

func TestCheckBlockIsInCurrentChain_ContextCancellation(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store a block
	blockID, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// The off-chain set lookup is fully in-memory and does not require a DB call,
	// so a cancelled context does not cause an error.
	result, err := s.CheckBlockIsInCurrentChain(ctx, []uint32{uint32(blockID)})
	assert.NoError(t, err)
	assert.True(t, result, "In-memory lookup should succeed even with cancelled context")
}

func TestCheckBlockIsInCurrentChain_BestBlockHeaderError(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	// Close the database connection to simulate error
	s.Close()

	// The off-chain set lookup is fully in-memory, so it still works after
	// the DB is closed. With no forks at startup, the off-chain set is empty
	// and all block IDs are treated as on-chain.
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{1})
	assert.NoError(t, err)
	assert.True(t, result, "In-memory lookup should succeed even with closed DB")
}

func TestCheckBlockIsInCurrentChain_LargeBlockIDList(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store sequential blocks to build a chain
	var blockIDs []uint32

	// Store first block
	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)
	blockIDs = append(blockIDs, uint32(blockID1))

	// Store second block
	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)
	blockIDs = append(blockIDs, uint32(blockID2))

	// Store third block
	blockID3, _, err := s.StoreBlock(context.Background(), block3, "")
	require.NoError(t, err)
	blockIDs = append(blockIDs, uint32(blockID3))

	// Test with list of block IDs
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
	require.NoError(t, err)
	assert.True(t, result, "All stored blocks should be in current chain")
}

func TestCheckBlockIsInCurrentChain_RecursionDepthCalculation(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store blocks to create a chain
	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	blockID3, _, err := s.StoreBlock(context.Background(), block3, "")
	require.NoError(t, err)

	// Test with block IDs where lowest ID is less than best block ID
	blockIDs := []uint32{uint32(blockID1), uint32(blockID3)}
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
	require.NoError(t, err)
	assert.True(t, result, "Blocks in chain should return true")

	// Non-existent block IDs are not in the off-chain set, so they're treated
	// as on-chain. In practice, callers only pass block IDs from the database.
	veryHighBlockID := uint32(999999)
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{veryHighBlockID})
	require.NoError(t, err)
	assert.True(t, result, "Unknown block IDs not in off-chain set are treated as on-chain")
}

func TestCheckBlockIsInCurrentChain_SQLiteEngine(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Verify SQLite engine is detected
	assert.Contains(t, []string{"sqlite", "sqlitememory"}, string(s.engine))

	// Store a block
	blockID, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	// Test with SQLite-specific SQL syntax
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID)})
	require.NoError(t, err)
	assert.True(t, result, "Block should be found with SQLite syntax")
}

func TestCheckBlockIsInCurrentChain_InvalidatedBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store blocks
	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Invalidate one of the blocks
	_, err = s.InvalidateBlock(context.Background(), block2.Header.Hash())
	require.NoError(t, err)

	// Check if invalidated block affects chain check
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1)})
	require.NoError(t, err)
	assert.True(t, result, "Valid block should still be in chain even if other blocks are invalidated")

	// Test with invalidated block would require the blockID2, but since we're testing
	// the general behavior, we can test with the valid block only
	// The result depends on implementation - invalidated blocks might still be found in the recursive query
	// but this tests the behavior
	t.Logf("Invalidated block in chain result: %v", result)
}

func TestCheckBlockIsInCurrentChain_PlaceholderGeneration(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store blocks to test placeholder generation logic
	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Test with single block ID - tests $1 placeholder
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1)})
	require.NoError(t, err)
	assert.True(t, result)

	// Test with multiple block IDs - tests $1, $2, etc. placeholders
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1), uint32(blockID2)})
	require.NoError(t, err)
	assert.True(t, result)

	// Test with ordered vs unordered IDs to verify placeholder logic
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID2), uint32(blockID1)})
	require.NoError(t, err)
	assert.True(t, result)
}

func TestCheckBlockIsInCurrentChain_ChainReorganization(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store initial chain
	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Check that initial blocks are in current chain
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1)})
	require.NoError(t, err)
	assert.True(t, result, "Initial blocks should be in current chain")

	// Store a competing block
	blockID3, _, err := s.StoreBlock(context.Background(), block3, "")
	require.NoError(t, err)

	// All blocks should still be accessible in current chain logic
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1), uint32(blockID3)})
	require.NoError(t, err)
	t.Logf("Chain reorganization test result: %v", result)
}

func TestCheckBlockIsInCurrentChain_ArgumentHandling(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store blocks
	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Test argument count calculation - should be len(blockIDs) + 2 (bestBlockID + recursionDepth)
	blockIDs := []uint32{uint32(blockID1), uint32(blockID2)}
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
	require.NoError(t, err)
	assert.True(t, result, "Argument handling should work correctly")

	// Test with single block to verify argument positioning
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1)})
	require.NoError(t, err)
	assert.True(t, result, "Single block argument handling should work correctly")
}
