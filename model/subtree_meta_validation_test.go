package model

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
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

// memSubtreeStore serves stored bytes keyed by "<key>.<fileType>" through the
// model.SubtreeStore interface (the shared TestLocalSubtreeStore reads testdata
// files by index in GetIoReader, so it cannot serve crafted bytes).
type memSubtreeStore struct {
	data map[string][]byte
}

func (m *memSubtreeStore) GetIoReader(_ context.Context, key []byte, fileType fileformat.FileType, _ ...options.FileOption) (io.ReadCloser, error) {
	b, ok := m.data[string(key)+"."+fileType.String()]
	if !ok {
		return nil, errors.NewNotFoundError("subtree meta not found")
	}

	return io.NopCloser(bytes.NewReader(b)), nil
}

func newMemSubtreeStore(key []byte, metaBytes []byte) *memSubtreeStore {
	return &memSubtreeStore{data: map[string][]byte{
		string(key) + "." + fileformat.FileTypeSubtreeMeta.String(): metaBytes,
	}}
}

// TestGetSubtreeMetaSliceValidation pins issue 1425: the within-block
// duplicate-inputs check trusts the .subtreeMeta cache file, so a torn or
// foreign file must fail the read (routing the caller into regeneration)
// instead of silently feeding the check wrong data.
func TestGetSubtreeMetaSliceValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("valid meta passes and carries the inpoints", func(t *testing.T) {
		subtree, metaBytes, block := buildMetaFixture(t)

		store := newMemSubtreeStore(subtree.RootHash()[:], metaBytes)

		got, err := block.getSubtreeMetaSlice(ctx, store, *subtree.RootHash(), subtree)
		require.NoError(t, err)

		parents, err := got.GetParentTxHashes(3)
		require.NoError(t, err)
		require.Len(t, parents, 1)
	})

	t.Run("short claimed entry count is rejected, not silently empty", func(t *testing.T) {
		// A count of 2 with 4 real leaves previously deserialized cleanly and
		// left leaves 2 and 3 with zero recorded inputs — the duplicate-inputs
		// check then passed vacuously for them.
		subtree, metaBytes, block := buildMetaFixture(t)

		torn := make([]byte, len(metaBytes))
		copy(torn, metaBytes)
		binary.LittleEndian.PutUint32(torn[32:36], 2)

		store := newMemSubtreeStore(subtree.RootHash()[:], torn)

		_, err := block.getSubtreeMetaSlice(ctx, store, *subtree.RootHash(), subtree)
		require.Error(t, err)
		require.Contains(t, err.Error(), "entry count mismatch")
	})

	t.Run("over-long claimed entry count is rejected, not a panic", func(t *testing.T) {
		// A count above the subtree size previously wrote past the
		// deserializer's slice: an index-out-of-range panic recurring on every
		// restart, since the file is on disk.
		subtree, metaBytes, block := buildMetaFixture(t)

		torn := make([]byte, len(metaBytes))
		copy(torn, metaBytes)
		binary.LittleEndian.PutUint32(torn[32:36], 64)

		store := newMemSubtreeStore(subtree.RootHash()[:], torn)

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

		store := newMemSubtreeStore(subtree.RootHash()[:], foreign)

		_, err := block.getSubtreeMetaSlice(ctx, store, *subtree.RootHash(), subtree)
		require.Error(t, err)
		require.Contains(t, err.Error(), "root hash mismatch")
	})

	t.Run("truncated file fails the header read", func(t *testing.T) {
		subtree, metaBytes, block := buildMetaFixture(t)

		store := newMemSubtreeStore(subtree.RootHash()[:], metaBytes[:20])

		_, err := block.getSubtreeMetaSlice(ctx, store, *subtree.RootHash(), subtree)
		require.Error(t, err)
		require.Contains(t, err.Error(), "header")
	})
}
