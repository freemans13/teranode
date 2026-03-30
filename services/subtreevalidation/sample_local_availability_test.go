package subtreevalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// buildSubtreeWithHashes creates an incomplete subtree with the given hashes as nodes.
// Coinbase placeholder hashes are added via AddCoinbaseNode; all others via AddNode.
func buildSubtreeWithHashes(t *testing.T, hashes []chainhash.Hash) *subtreepkg.Subtree {
	t.Helper()
	st, err := subtreepkg.NewIncompleteTreeByLeafCount(len(hashes))
	require.NoError(t, err)
	for _, h := range hashes {
		if h.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			require.NoError(t, st.AddCoinbaseNode())
		} else {
			require.NoError(t, st.AddNode(h, 0, 0))
		}
	}
	return st
}

// populateCacheWithHashes inserts the given hashes into the cache with minimal metadata.
func populateCacheWithHashes(t *testing.T, cache *txmetacache.TxMetaCache, hashes []chainhash.Hash) {
	t.Helper()
	for i := range hashes {
		err := cache.SetCache(&hashes[i], &meta.Data{Fee: 1, SizeInBytes: 100})
		require.NoError(t, err)
	}
}

func setupSampleServer(t *testing.T, cache *txmetacache.TxMetaCache) *Server {
	t.Helper()
	tSettings := test.CreateBaseTestSettings(t)
	return &Server{
		logger:    ulogger.TestLogger{},
		settings:  tSettings,
		utxoStore: cache,
	}
}

func TestSampleLocalAvailability_EmptySubtree(t *testing.T) {
	cache := setupTestCache(t)
	server := setupSampleServer(t, cache)
	ctx := context.Background()

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(1)
	require.NoError(t, err)
	// Do not add any nodes — Nodes slice remains empty.

	result := server.sampleLocalAvailability(ctx, st, 10)
	require.Equal(t, 0.0, result)
}

func TestSampleLocalAvailability_AllCached(t *testing.T) {
	cache := setupTestCache(t)
	server := setupSampleServer(t, cache)
	ctx := context.Background()

	hashes := generateTestHashes(5)
	populateCacheWithHashes(t, cache, hashes)
	st := buildSubtreeWithHashes(t, hashes)

	result := server.sampleLocalAvailability(ctx, st, 5)
	require.Equal(t, 1.0, result)
}

func TestSampleLocalAvailability_NoneCached(t *testing.T) {
	cache := setupTestCache(t)
	server := setupSampleServer(t, cache)
	ctx := context.Background()

	hashes := generateTestHashes(5)
	// Do not populate cache.
	st := buildSubtreeWithHashes(t, hashes)

	result := server.sampleLocalAvailability(ctx, st, 5)
	require.Equal(t, 0.0, result)
}

func TestSampleLocalAvailability_SkipsCoinbasePlaceholder(t *testing.T) {
	cache := setupTestCache(t)
	server := setupSampleServer(t, cache)
	ctx := context.Background()

	// Build a subtree where first node is coinbase placeholder, rest are normal hashes.
	normalHashes := generateTestHashes(4)
	allHashes := append([]chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue}, normalHashes...)
	// Populate all normal hashes in cache so we get 100% hit on checked nodes.
	populateCacheWithHashes(t, cache, normalHashes)

	st := buildSubtreeWithHashes(t, allHashes)

	// With sampleSize=5 and step=1, all nodes are visited.
	// The coinbase placeholder is skipped, so 4 normal hashes are checked, all cached.
	result := server.sampleLocalAvailability(ctx, st, 5)
	require.Equal(t, 1.0, result, "coinbase placeholder should be skipped, all real nodes are cached")
}

func TestSampleLocalAvailability_SampleSizeLimitsChecks(t *testing.T) {
	cache := setupTestCache(t)
	server := setupSampleServer(t, cache)
	ctx := context.Background()

	// 1000 nodes, only first 500 cached.
	hashes := generateTestHashes(1000)
	populateCacheWithHashes(t, cache, hashes[:500])
	st := buildSubtreeWithHashes(t, hashes)

	// Sample 10 nodes with even spacing (step = 1000/10 = 100).
	// Sampled indices: 0, 100, 200, 300, 400, 500, 600, 700, 800, 900.
	// Indices 0-400 are cached (5 hits), 500-900 are not (5 misses).
	result := server.sampleLocalAvailability(ctx, st, 10)
	require.Equal(t, 0.5, result, "expected 50%% hit rate from evenly-spaced sample")
}

func TestSampleLocalAvailability_NoCacheImplementation(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	server := &Server{
		logger:    ulogger.TestLogger{},
		settings:  tSettings,
		utxoStore: ns, // Not a *txmetacache.TxMetaCache
	}
	ctx := context.Background()

	hashes := generateTestHashes(5)
	st := buildSubtreeWithHashes(t, hashes)

	result := server.sampleLocalAvailability(ctx, st, 5)
	require.Equal(t, 0.0, result, "non-cache store should return 0.0")
}
