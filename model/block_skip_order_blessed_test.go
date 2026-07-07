package model

import (
	"context"
	"encoding/hex"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// panicTxMetaStore is a utxo.Store whose data methods panic (nil embedded
// interface), but which reports SupportsOutpointOnlySpend()==true. Passing it
// to Valid proves validOrderAndBlessed never touched the store: any real access
// would nil-pointer panic and fail the test. The capability query is safe to
// answer (Valid calls it for the fee/order-blessed skip gates) and does not
// count as touching UTXO data.
type panicTxMetaStore struct {
	utxo.Store
}

// SupportsOutpointOnlySpend models a store that can run the outpoint-only fast
// path (like the SQL store), so the below-checkpoint skips are eligible.
func (s *panicTxMetaStore) SupportsOutpointOnlySpend() bool { return true }

// newSkipTestSettings returns settings with the outpoint-only flag set as given,
// a sqlitememory UTXO store URL, and one hardcoded checkpoint at checkpointHeight.
func newSkipTestSettings(t *testing.T, enabled bool, checkpointHeight int32) *settings.Settings {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = enabled

	u, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = u

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight}}
	tSettings.ChainCfgParams = &params

	return tSettings
}

// TestBlock_SkipOrderAndBlessedBelowCheckpoint_Predicate is the full truth table:
// every conjunct (flag on AND a store that supports the fast path AND the block
// is a confirmed checkpoint ancestor AND a checkpoint exists AND 0 < height <=
// checkpoint) must hold; any one missing keeps the skip OFF (fail-safe). The
// store-support and confirmed-ancestor gates mirror the sibling fee skip in
// checkBlockRewardAndFees so both skips engage on exactly the same blocks.
func TestBlock_SkipOrderAndBlessedBelowCheckpoint_Predicate(t *testing.T) {
	const checkpointHeight = int32(2000)

	tests := []struct {
		name          string
		enabled       bool
		supportsStore bool
		confirmed     bool
		noCheckpts    bool
		nilParams     bool
		height        uint32
		want          bool
	}{
		{name: "flag off, below", enabled: false, supportsStore: true, confirmed: true, height: 1000, want: false},
		{name: "flag on, non-supporting store, below", enabled: true, supportsStore: false, confirmed: true, height: 1000, want: false},
		{name: "flag on, supporting, not confirmed ancestor, below", enabled: true, supportsStore: true, confirmed: false, height: 1000, want: false},
		{name: "flag on, supporting, confirmed, below checkpoint", enabled: true, supportsStore: true, confirmed: true, height: 1000, want: true},
		{name: "flag on, supporting, confirmed, at checkpoint", enabled: true, supportsStore: true, confirmed: true, height: 2000, want: true},
		{name: "flag on, supporting, confirmed, above checkpoint", enabled: true, supportsStore: true, confirmed: true, height: 2001, want: false},
		{name: "flag on, supporting, confirmed, height 0", enabled: true, supportsStore: true, confirmed: true, height: 0, want: false},
		{name: "flag on, supporting, confirmed, no checkpoints", enabled: true, supportsStore: true, confirmed: true, noCheckpts: true, height: 1000, want: false},
		{name: "flag on, supporting, confirmed, nil chain params", enabled: true, supportsStore: true, confirmed: true, nilParams: true, height: 1000, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := newSkipTestSettings(t, tt.enabled, checkpointHeight)

			if tt.noCheckpts {
				noCp := chaincfg.RegressionNetParams
				noCp.Checkpoints = nil
				tSettings.ChainCfgParams = &noCp
			}

			if tt.nilParams {
				tSettings.ChainCfgParams = nil
			}

			// Store capability replaces the old settings-URL check: a supporting
			// store (panicTxMetaStore reports true) vs a NullStore (reports false).
			var store utxo.Store = &nullstore.NullStore{}
			if tt.supportsStore {
				store = &panicTxMetaStore{}
			}

			b := &Block{Height: tt.height}
			b.SetCheckpointConfirmedAncestor(tt.confirmed)

			require.Equal(t, tt.want, b.skipOrderAndBlessedBelowCheckpoint(tSettings, store),
				"skipOrderAndBlessedBelowCheckpoint height=%d", tt.height)
		})
	}

	t.Run("nil settings", func(t *testing.T) {
		b := &Block{Height: 1000}
		b.SetCheckpointConfirmedAncestor(true)
		require.False(t, b.skipOrderAndBlessedBelowCheckpoint(nil, &panicTxMetaStore{}))
	})

	t.Run("nil store", func(t *testing.T) {
		tSettings := newSkipTestSettings(t, true, checkpointHeight)
		b := &Block{Height: 1000}
		b.SetCheckpointConfirmedAncestor(true)
		require.False(t, b.skipOrderAndBlessedBelowCheckpoint(tSettings, nil))
	})
}

// TestBlock_Valid_SkipsValidOrderAndBlessedBelowCheckpoint proves the gate is
// honoured end-to-end: with the flag ON and a block at/below the checkpoint,
// Valid() must succeed WITHOUT touching the txMetaStore at all (the store
// panics on any use). Before the gate existed this test panicked inside
// validOrderAndBlessed (RED); with the gate it passes (GREEN). All other
// Valid() checks (PoW, timestamp, coinbase, dedup) still run.
//
// Note: a nil subtreeStore is a test convenience that bypasses
// GetAndValidateSubtrees and the CheckMerkleRoot call that follows it.
// The guarantee that CheckMerkleRoot still runs and rejects tampered blocks
// while the skip is active is covered by
// TestBlock_MerkleFloor_RejectsTamperingWhileSkipIsActive.
func TestBlock_Valid_SkipsValidOrderAndBlessedBelowCheckpoint(t *testing.T) {
	tSettings := newSkipTestSettings(t, true, 2000)

	blockHeaderBytes, err := hex.DecodeString(block1Header)
	require.NoError(t, err)
	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	// One subtree: coinbase placeholder + one regular tx that is unknown to any
	// store — if validOrderAndBlessed ran, resolving this tx would hit the
	// panicTxMetaStore.
	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	txHash, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	require.NoError(t, err)
	require.NoError(t, st.AddNode(*txHash, 1, 100))

	rootHash := st.RootHash()

	// height 1000 <= checkpoint 2000. blockID 0.
	block, err := NewBlock(blockHeader, coinbase, []*chainhash.Hash{rootHash}, 2, 123, 1000, 0)
	require.NoError(t, err)

	// Confirmed checkpoint ancestor: the blockvalidation service sets this below
	// the checkpoint; the skip (like the sibling fee skip) requires it.
	block.SetCheckpointConfirmedAncestor(true)

	// Pre-populate slices and pass a nil subtreeStore so Valid() takes its
	// existing internal-block path (skips GetAndValidateSubtrees/merkle steps,
	// which need a stocked blob store) while checkDuplicateTransactions and the
	// step-12 gate still execute against the slices.
	block.SubtreeSlices = []*subtreepkg.Subtree{st}

	oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

	valid, err := block.Valid(context.Background(), ulogger.TestLogger{}, nil,
		&panicTxMetaStore{}, oldBlockIDs, []*BlockHeader{}, []uint32{}, tSettings, nil)
	require.NoError(t, err)
	require.True(t, valid)

	// The skip must not have recorded any old-block references.
	_, hasTransactionsReferencingOldBlocks := txmap.ConvertSyncedMapToUint32Slice(oldBlockIDs)
	require.False(t, hasTransactionsReferencingOldBlocks)
}

// buildSubtreeAndMerkleRoot constructs a one-subtree block body and returns:
//   - the subtree (with coinbase placeholder at index 0 plus txHash at index 1)
//   - the correct HashMerkleRoot for the given coinbase transaction
//
// The root is computed the same way CheckMerkleRoot does: replace the coinbase
// placeholder with the coinbase tx hash, then take the subtree's root.
func buildSubtreeAndMerkleRoot(t *testing.T, coinbase *bt.Tx, txHash chainhash.Hash) (*subtreepkg.Subtree, *chainhash.Hash) {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(txHash, 1, 100))

	// Compute the merkle root as CheckMerkleRoot does: replace coinbase placeholder
	// with the actual coinbase tx hash and return the resulting root.
	merkleRoot, err := st.RootHashWithReplaceRootNode(
		coinbase.TxIDChainHash(),
		0,
		uint64(coinbase.Size()), // nolint: gosec
	)
	require.NoError(t, err)

	return st, merkleRoot
}

// storeSubtree serializes st and writes it into blobStore under the key
// st.RootHash() with file type FileTypeSubtree, mimicking what the block
// persister writes before Valid() is called with a real subtree store.
func storeSubtree(t *testing.T, blobStore *blobmemory.Memory, st *subtreepkg.Subtree) {
	t.Helper()

	subtreeBytes, err := st.Serialize()
	require.NoError(t, err)

	err = blobStore.Set(
		context.Background(),
		st.RootHash()[:],
		fileformat.FileTypeSubtree,
		subtreeBytes,
	)
	require.NoError(t, err)
}

// minedHeader builds a BlockHeader with nBits=207fffff and the given merkle
// root, then increments Nonce until HasMetTargetDifficulty passes.
// Version is kept at 1 to bypass BIP-34 coinbase-height extraction.
func minedHeader(t *testing.T, merkleRoot *chainhash.Hash) *BlockHeader {
	t.Helper()

	prevHash := chainhash.Hash{} // all-zero prev for a standalone test block
	nBits, err := NewNBitFromString("207fffff")
	require.NoError(t, err)

	hdr := &BlockHeader{
		Version:        1,
		HashPrevBlock:  &prevHash,
		HashMerkleRoot: merkleRoot,
		Timestamp:      1296688602, // fixed past timestamp used across model tests
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		ok, _, _ := hdr.HasMetTargetDifficulty()
		if ok {
			break
		}
		hdr.Nonce++
	}

	return hdr
}

// TestBlock_MerkleFloor_RejectsTamperingWhileSkipIsActive proves that
// CheckMerkleRoot (the integrity floor in steps 1-11 of Block.Valid) still
// runs and still rejects a tampered block body even when the
// below-checkpoint skip is active.
//
// Case A — tampered header: HashMerkleRoot deliberately wrong → Valid returns
// false with an error whose message contains "merkle".
//
// Case B — correct header + panicTxMetaStore: Valid returns true, proving that
// validOrderAndBlessed was skipped (panicTxMetaStore would panic on any
// access) while the full subtree/merkle section ran against a real blob store.
// This strictly supersedes the nil-subtreeStore guarantee in
// TestBlock_Valid_SkipsValidOrderAndBlessedBelowCheckpoint.
func TestBlock_MerkleFloor_RejectsTamperingWhileSkipIsActive(t *testing.T) {
	const checkpointHeight = int32(2000)
	const blockHeight = uint32(1000) // <= checkpoint

	tSettings := newSkipTestSettings(t, true, checkpointHeight)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	txHash, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	require.NoError(t, err)

	st, correctMerkleRoot := buildSubtreeAndMerkleRoot(t, coinbase, *txHash)

	t.Run("Case A: tampered merkle root is rejected even with skip active", func(t *testing.T) {
		// Flip the first byte of the correct root to produce a wrong hash.
		tampered := *correctMerkleRoot
		tampered[0] ^= 0xff

		hdr := minedHeader(t, &tampered)

		subtreeRootHash := st.RootHash()
		block, err := NewBlock(hdr, coinbase, []*chainhash.Hash{subtreeRootHash}, 2, 123, blockHeight, 0)
		require.NoError(t, err)
		block.SetCheckpointConfirmedAncestor(true)

		blobStore := blobmemory.New()
		storeSubtree(t, blobStore, st)

		oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

		valid, valErr := block.Valid(
			context.Background(), ulogger.TestLogger{}, blobStore,
			&panicTxMetaStore{}, oldBlockIDs, []*BlockHeader{}, []uint32{}, tSettings, nil,
		)
		require.Error(t, valErr, "expected an error for tampered merkle root")
		require.False(t, valid, "expected valid=false for tampered merkle root")
		require.Contains(t, valErr.Error(), "merkle", "error must mention merkle mismatch")
	})

	t.Run("Case B: correct merkle root passes floor check while validOrderAndBlessed is skipped", func(t *testing.T) {
		hdr := minedHeader(t, correctMerkleRoot)

		subtreeRootHash := st.RootHash()
		block, err := NewBlock(hdr, coinbase, []*chainhash.Hash{subtreeRootHash}, 2, 123, blockHeight, 0)
		require.NoError(t, err)
		block.SetCheckpointConfirmedAncestor(true)

		blobStore := blobmemory.New()
		storeSubtree(t, blobStore, st)

		oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

		// panicTxMetaStore panics on any method call; if validOrderAndBlessed
		// ran it would touch the store and the test would panic (RED without the
		// gate, GREEN with it).
		valid, valErr := block.Valid(
			context.Background(), ulogger.TestLogger{}, blobStore,
			&panicTxMetaStore{}, oldBlockIDs, []*BlockHeader{}, []uint32{}, tSettings, nil,
		)
		require.NoError(t, valErr)
		require.True(t, valid, "expected valid=true: correct merkle, skip active, store floor exercised")
	})
}
