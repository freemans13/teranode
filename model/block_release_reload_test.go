package model

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestGetAndValidateSubtrees_ReloadsAfterNodesReleased reproduces the
// release-then-requeue poisoning behind the 2026-08-11 scale-2 freeze:
// blockvalidation's failure paths return a block's pooled node slices
// (ReleaseNodes sets Nodes=nil but leaves the *Subtree pointers in
// SubtreeSlices) and then requeue the same *Block for revalidation.
// GetAndValidateSubtrees must treat those gutted slices as not loaded and
// reload from the store — not early-exit "already loaded" and let Valid fail
// "first subtree has no nodes" on a perfectly valid block.
func TestGetAndValidateSubtrees_ReloadsAfterNodesReleased(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	txBytes := make([]byte, 32)
	_, _ = rand.Read(txBytes)
	txHash, err := chainhash.NewHash(txBytes)
	require.NoError(t, err)
	require.NoError(t, st.AddNode(*txHash, 1, 0))

	blobStore := blobmemory.New()
	storeSubtree(t, blobStore, st)

	blockHeaderBytes, _ := hex.DecodeString(block1Header)
	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	b, err := NewBlock(blockHeader, coinbase, []*chainhash.Hash{st.RootHash()}, 2, 123, 0, 0)
	require.NoError(t, err)

	ctx := context.Background()
	err = b.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, blobStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)
	require.Len(t, b.SubtreeSlices, 1)
	require.Len(t, b.SubtreeSlices[0].Nodes, 2)

	// Simulate services/blockvalidation.releaseBlockNodes: the pooled Nodes
	// backing slice is taken back while the *Subtree pointer stays behind.
	_ = b.SubtreeSlices[0].ReleaseNodes()

	// Poison TransactionCount so the assertion below proves the full-load path
	// ran: only a real reload recomputes it (the incident's 4 failed
	// revalidations all showed the stale count of the early-exit path).
	b.TransactionCount = 999

	err = b.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, blobStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)
	require.Len(t, b.SubtreeSlices, 1)
	require.NotNil(t, b.SubtreeSlices[0])
	require.Len(t, b.SubtreeSlices[0].Nodes, 2,
		"subtrees must be reloaded from the store after their nodes were released")
	require.Equal(t, uint64(2), b.TransactionCount,
		"a reload must recompute TransactionCount from the store, not keep the stale value")
}

// TestReleaseSubtreeNodes_ClosesMmapAndNilsEntries pins the full release
// contract: heap-backed node slices are handed to the pool callback, mmap-backed
// slices are NOT (their backing is the mapped region — pooling it after munmap
// would be use-after-free), every subtree is Closed (removing the mmap backing
// file), and every entry is nil-ed under the block's subtree mutex.
func TestReleaseSubtreeNodes_ClosesMmapAndNilsEntries(t *testing.T) {
	heapSt, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, heapSt.AddCoinbaseNode())
	require.NoError(t, heapSt.AddNode(chainhash.HashH([]byte("tx-a")), 1, 0))

	serialized, err := heapSt.Serialize()
	require.NoError(t, err)

	mmapDir := t.TempDir()
	mmapSt, err := subtreepkg.NewSubtreeFromReaderMmap(bytes.NewReader(serialized), mmapDir)
	require.NoError(t, err)
	require.True(t, mmapSt.IsMmapBacked())

	entries, err := os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "sanity: mmap backing file exists while the subtree is live")

	b := &Block{SubtreeSlices: []*subtreepkg.Subtree{heapSt, mmapSt, nil}}

	var pooled [][]subtreepkg.Node
	b.ReleaseSubtreeNodes(func(nodes []subtreepkg.Node) { pooled = append(pooled, nodes) })

	require.Len(t, pooled, 1, "only the heap-backed slice may be pooled")
	require.Nil(t, b.SubtreeSlices[0])
	require.Nil(t, b.SubtreeSlices[1])

	entries, err = os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.Empty(t, entries, "Close must unmap and remove the mmap backing file")
}
