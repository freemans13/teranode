package model

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
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

// TestCommittedSubtreeHash pins which hash the meta header is validated against.
// subtree.RootHash() is the 32 bytes the deserializer cached verbatim from the
// .subtree file header, so validating one file's claim against another's would
// let a torn .subtree header reject a good meta and let a foreign .subtree pass
// alongside its own meta. The block header's committed list is the authority.
func TestCommittedSubtreeHash(t *testing.T) {
	subtree, _, block := buildMetaFixture(t)

	// Compared by value throughout: the assertions are about which hash comes
	// back, not which pointer.
	t.Run("falls back to the subtree root when no committed list is present", func(t *testing.T) {
		require.Empty(t, block.Subtrees)
		require.Equal(t, *subtree.RootHash(), *block.committedSubtreeHash(0, subtree))
	})

	t.Run("prefers the block-header-committed hash", func(t *testing.T) {
		committed := chainhash.HashH([]byte{0xc0, 0xde})
		withList := &Block{
			Header:   block.Header,
			Subtrees: []*chainhash.Hash{&committed},
		}

		require.Equal(t, committed, *withList.committedSubtreeHash(0, subtree))
		require.NotEqual(t, *subtree.RootHash(), *withList.committedSubtreeHash(0, subtree))
	})

	t.Run("falls back when the committed entry is nil or out of range", func(t *testing.T) {
		withNil := &Block{Header: block.Header, Subtrees: []*chainhash.Hash{nil}}
		require.Equal(t, *subtree.RootHash(), *withNil.committedSubtreeHash(0, subtree))
		require.Equal(t, *subtree.RootHash(), *withNil.committedSubtreeHash(1, subtree))
	})
}

// TestValidateSubtreeCommittedHashMismatch pins the tie between the .subtree
// file and the hash the proof of work commits to. GetAndValidateSubtrees stores
// whatever sits under the blob key without comparing it, and RootHash() returns
// the bytes the file header claimed rather than a recomputation, so without this
// a genuine-but-foreign .subtree under the right key is validated as the
// committed one and its meta — which agrees with the key — passes the header
// check, attributing another subtree's inputs to this one.
//
// It also pins the routing at the meta-read call site. The check compares
// RootHash() against committedSubtreeHash(), so reverting that call site to
// subtree.RootHash() turns the comparison into a tautology and the mismatch case
// below stops failing.
func TestValidateSubtreeCommittedHashMismatch(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	newCtx := func() *validationContext {
		return &validationContext{
			currentBlockHeaderHashesMap: map[chainhash.Hash]struct{}{},
			currentBlockHeaderIDsMap:    map[uint32]struct{}{},
		}
	}

	t.Run("a subtree that does not match its committed hash is rejected", func(t *testing.T) {
		subtree, metaBytes, block := buildMetaFixture(t)

		// Serve the meta under the committed key, so the rejection is the
		// subtree/committed mismatch and not a missing file.
		committed := chainhash.HashH([]byte{0xc0, 0xde})
		block.Subtrees = []*chainhash.Hash{&committed}

		deps := &validationDependencies{subtreeStore: newMemSubtreeStore(t, committed[:], metaBytes)}

		err := block.validateSubtree(ctx, logger, deps, newCtx(), subtree, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not match its committed hash")
	})

	t.Run("a subtree matching its committed hash gets past the check", func(t *testing.T) {
		subtree, metaBytes, block := buildMetaFixture(t)

		committed := *subtree.RootHash()
		block.Subtrees = []*chainhash.Hash{&committed}

		deps := &validationDependencies{subtreeStore: newMemSubtreeStore(t, committed[:], metaBytes)}
		block.txMap = txmap.NewSplitSwissMapUint64(10)

		// Validation carries on into the per-transaction checks, which this
		// fixture deliberately does not satisfy — the leaves were never added
		// to txMap — so assert on what this check is responsible for rather
		// than on overall success.
		err := block.validateSubtree(ctx, logger, deps, newCtx(), subtree, 0)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "does not match its committed hash")
		require.Contains(t, err.Error(), "not in the txMap")
	})
}
