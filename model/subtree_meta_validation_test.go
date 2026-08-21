package model

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// buildMetaFixture returns a 4-leaf subtree, its correctly serialized meta
// bytes, and a block wrapping it. Each leaf gets one distinct parent inpoint
// so the meta body is non-trivial.
func buildMetaFixture(t *testing.T) (*subtreepkg.Subtree, []byte, *Block) {
	t.Helper()

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	for i := byte(0); i < 4; i++ {
		require.NoError(t, subtree.AddNode(chainhash.HashH([]byte{i, 0xaa}), 1, 0))
	}

	meta := subtreepkg.NewSubtreeMeta(subtree)

	for i := 0; i < 4; i++ {
		parent := chainhash.HashH([]byte{byte(i), 0xbb})
		require.NoError(t, meta.SetTxInpoints(i, subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent}, []uint32{1, 0})))
	}

	metaBytes, err := meta.Serialize()
	require.NoError(t, err)

	block := &Block{
		Header: &BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
	}

	return subtree, metaBytes, block
}

// newMemSubtreeStore serves the given meta bytes under the given key from the
// shared in-memory blob store, which already satisfies model.SubtreeStore. (The
// other test double in this package, TestLocalSubtreeStore, reads testdata files
// by index in GetIoReader, so it cannot serve crafted bytes.)
func newMemSubtreeStore(t *testing.T, key []byte, metaBytes []byte) SubtreeStore {
	t.Helper()

	store := memory.New()
	require.NoError(t, store.Set(context.Background(), key, fileformat.FileTypeSubtreeMeta, metaBytes))

	return store
}

// TestGetSubtreeMetaSliceValidation pins issue 1425: the within-block
// duplicate-inputs check trusts the .subtreeMeta cache file, so a torn or
// foreign file must fail the read (routing the caller into regeneration)
// instead of silently feeding the check wrong data.
func TestGetSubtreeMetaSliceValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("valid meta passes and carries the inpoints", func(t *testing.T) {
		subtree, metaBytes, block := buildMetaFixture(t)

		store := newMemSubtreeStore(t, subtree.RootHash()[:], metaBytes)

		got, err := block.getSubtreeMetaSlice(ctx, store, *subtree.RootHash(), subtree)
		require.NoError(t, err)

		parents, err := got.GetParentTxHashes(3)
		require.NoError(t, err)
		require.Len(t, parents, 1)
	})

	t.Run("short claimed entry count is rejected up front", func(t *testing.T) {
		// A count of 2 with 4 real leaves previously deserialized cleanly and
		// left leaves 2 and 3 with zero recorded inputs; the downstream
		// nil-parents guard then rejected the whole (valid) block as invalid —
		// a persisted wrong verdict caused by a torn local cache file, with no
		// regeneration. Failing the read here routes into regeneration instead.
		subtree, metaBytes, block := buildMetaFixture(t)

		torn := make([]byte, len(metaBytes))
		copy(torn, metaBytes)
		binary.LittleEndian.PutUint32(torn[32:36], 2)

		store := newMemSubtreeStore(t, subtree.RootHash()[:], torn)

		_, err := block.getSubtreeMetaSlice(ctx, store, *subtree.RootHash(), subtree)
		require.Error(t, err)
		require.Contains(t, err.Error(), "entry count mismatch")
	})

	t.Run("over-long claimed count with no extra body is rejected", func(t *testing.T) {
		// With no extra body bytes the old code failed on EOF rather than
		// panicking; either way the count must now be rejected up front.
		subtree, metaBytes, block := buildMetaFixture(t)

		torn := make([]byte, len(metaBytes))
		copy(torn, metaBytes)
		binary.LittleEndian.PutUint32(torn[32:36], 64)

		store := newMemSubtreeStore(t, subtree.RootHash()[:], torn)

		require.NotPanics(t, func() {
			_, err := block.getSubtreeMetaSlice(ctx, store, *subtree.RootHash(), subtree)
			require.Error(t, err)
			require.Contains(t, err.Error(), "entry count mismatch")
		})
	})

	t.Run("over-long count with a well-formed extra entry is rejected, not a panic", func(t *testing.T) {
		// This is the shape that genuinely panicked before the fix: the
		// deserializer sizes its slice from the real subtree (4) but writes
		// the file-claimed number of entries, so a well-formed fifth entry
		// hit index 4 of a length-4 slice — recurring on every restart,
		// since the file is on disk.
		subtree, metaBytes, block := buildMetaFixture(t)

		extraInpoints := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{chainhash.HashH([]byte{0xcc})}, []uint32{1, 0})
		extra, err := extraInpoints.Serialize()
		require.NoError(t, err)

		torn := make([]byte, len(metaBytes), len(metaBytes)+len(extra))
		copy(torn, metaBytes)
		binary.LittleEndian.PutUint32(torn[32:36], 5)
		torn = append(torn, extra...)

		store := newMemSubtreeStore(t, subtree.RootHash()[:], torn)

		require.NotPanics(t, func() {
			_, err := block.getSubtreeMetaSlice(ctx, store, *subtree.RootHash(), subtree)
			require.Error(t, err)
			require.Contains(t, err.Error(), "entry count mismatch")
		})
	})

	t.Run("foreign meta with another subtree's root hash is rejected", func(t *testing.T) {
		subtree, metaBytes, block := buildMetaFixture(t)

		foreign := make([]byte, len(metaBytes))
		copy(foreign, metaBytes)
		other := chainhash.HashH([]byte{0xfe, 0xed})
		copy(foreign[:32], other[:])

		store := newMemSubtreeStore(t, subtree.RootHash()[:], foreign)

		_, err := block.getSubtreeMetaSlice(ctx, store, *subtree.RootHash(), subtree)
		require.Error(t, err)
		require.Contains(t, err.Error(), "root hash mismatch")
	})

	t.Run("truncated file fails the header read", func(t *testing.T) {
		subtree, metaBytes, block := buildMetaFixture(t)

		store := newMemSubtreeStore(t, subtree.RootHash()[:], metaBytes[:20])

		_, err := block.getSubtreeMetaSlice(ctx, store, *subtree.RootHash(), subtree)
		require.Error(t, err)
		require.Contains(t, err.Error(), "header")
	})
}

// TestSubtreeBoundToItsKey pins the load-site binding. The .subtree file's root
// hash is cached verbatim from its header by DeserializeFromReaderWithAllocator
// and RootHash() never recomputes it, so without this comparison a
// genuine-but-foreign subtree stored under the right key is accepted as the
// committed one and every later consumer of RootHash() inherits that.
// CheckMerkleRoot does not close it: for subtree 0 it recomputes the root via
// RootHashWithReplaceRootNode, so the file's cached root is never compared there.
func TestSubtreeBoundToItsKey(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	// storeSubtree writes subtree's serialized bytes under an arbitrary key, which
	// is what lets the mismatch case exist at all.
	storeSubtree := func(t *testing.T, st *subtreepkg.Subtree, key chainhash.Hash) SubtreeStore {
		t.Helper()

		b, err := st.Serialize()
		require.NoError(t, err)

		store := memory.New()
		require.NoError(t, store.Set(ctx, key[:], fileformat.FileTypeSubtree, b))

		return store
	}

	t.Run("a subtree stored under a foreign key is rejected", func(t *testing.T) {
		subtree, _, block := buildMetaFixture(t)

		foreign := chainhash.HashH([]byte{0xc0, 0xde})
		block.Subtrees = []*chainhash.Hash{&foreign}

		err := block.GetAndValidateSubtrees(ctx, logger, storeSubtree(t, subtree, foreign), 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not match its key")
	})

	t.Run("a subtree stored under its own root hash is accepted", func(t *testing.T) {
		subtree, _, block := buildMetaFixture(t)

		key := *subtree.RootHash()
		block.Subtrees = []*chainhash.Hash{&key}

		// GetAndValidateSubtrees goes on to size the block, which reads the
		// coinbase; the fixture only needs it to be non-nil.
		coinbase, err := bt.NewTxFromString(CoinbaseHex)
		require.NoError(t, err)

		block.CoinbaseTx = coinbase

		require.NoError(t, block.GetAndValidateSubtrees(ctx, logger, storeSubtree(t, subtree, key), 1))
		require.Equal(t, key, *block.SubtreeSlices[0].RootHash())
	})
}

// TestMetaCountCheckedAgainstLength pins which of the subtree's two notions of
// size the entry count is compared with. Meta.serializeTxInpoints writes
// Length(); Size() is cap(Nodes) and can be larger, because block validation
// deserializes with a pooled node allocator that rounds capacity up to a size
// class and because an incomplete final subtree is legitimately short. Every
// other fixture in this file has Size() == Length(), the one case where the two
// coincide, so switching the reader to Size() would pass the whole suite while
// rejecting every incomplete final subtree in production.
func TestMetaCountCheckedAgainstLength(t *testing.T) {
	ctx := context.Background()

	// Capacity for 8, filled with 3: Size() == 8, Length() == 3.
	subtree, err := subtreepkg.NewTreeByLeafCount(8)
	require.NoError(t, err)

	for i := byte(0); i < 3; i++ {
		require.NoError(t, subtree.AddNode(chainhash.HashH([]byte{i, 0x11}), 1, 0))
	}

	require.Greater(t, subtree.Size(), subtree.Length(), "fixture must have spare capacity or it tests nothing")

	meta := subtreepkg.NewSubtreeMeta(subtree)

	for i := 0; i < subtree.Length(); i++ {
		parent := chainhash.HashH([]byte{byte(i), 0x22})
		require.NoError(t, meta.SetTxInpoints(i, subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent}, []uint32{1, 0})))
	}

	metaBytes, err := meta.Serialize()
	require.NoError(t, err)

	// The serializer wrote Length(), so the header must be accepted as written.
	require.Equal(t, uint32(subtree.Length()), binary.LittleEndian.Uint32(metaBytes[32:36]))

	block := &Block{Header: &BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}}
	store := newMemSubtreeStore(t, subtree.RootHash()[:], metaBytes)

	got, err := block.getSubtreeMetaSlice(ctx, store, *subtree.RootHash(), subtree)
	require.NoError(t, err, "a short subtree with spare capacity must not be rejected")

	parents, err := got.GetParentTxHashes(subtree.Length() - 1)
	require.NoError(t, err)
	require.Len(t, parents, 1)
}
