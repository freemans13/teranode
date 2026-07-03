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
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// panicTxMetaStore is a utxo.Store whose every method panics (nil embedded
// interface). Passing it to Valid proves validOrderAndBlessed never touched
// the store: any access would nil-pointer panic and fail the test.
type panicTxMetaStore struct {
	utxo.Store
}

// newSkipTestSettings returns settings with the outpoint-only flag set as given,
// a sqlitememory (SQL-backed) UTXO store URL, and one hardcoded checkpoint at
// checkpointHeight — the same three conjuncts legacyOutpointOnly requires.
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
// every conjunct (flag on AND SQL store AND checkpoint exists AND 0 < height <=
// checkpoint) must hold; any one missing keeps the skip OFF (fail-safe).
func TestBlock_SkipOrderAndBlessedBelowCheckpoint_Predicate(t *testing.T) {
	const checkpointHeight = int32(2000)

	tests := []struct {
		name       string
		enabled    bool
		sqlStore   bool
		noCheckpts bool
		nilParams  bool
		height     uint32
		want       bool
	}{
		{name: "flag off, below", enabled: false, sqlStore: true, height: 1000, want: false},
		{name: "flag on, non-SQL store, below", enabled: true, sqlStore: false, height: 1000, want: false},
		{name: "flag on, SQL, below checkpoint", enabled: true, sqlStore: true, height: 1000, want: true},
		{name: "flag on, SQL, at checkpoint", enabled: true, sqlStore: true, height: 2000, want: true},
		{name: "flag on, SQL, above checkpoint", enabled: true, sqlStore: true, height: 2001, want: false},
		{name: "flag on, SQL, height 0", enabled: true, sqlStore: true, height: 0, want: false},
		{name: "flag on, SQL, no checkpoints", enabled: true, sqlStore: true, noCheckpts: true, height: 1000, want: false},
		{name: "flag on, SQL, nil chain params", enabled: true, sqlStore: true, nilParams: true, height: 1000, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := newSkipTestSettings(t, tt.enabled, checkpointHeight)

			if !tt.sqlStore {
				u, err := url.Parse("aerospike://host:3000/ns/set")
				require.NoError(t, err)
				tSettings.UtxoStore.UtxoStore = u
			}

			if tt.noCheckpts {
				noCp := chaincfg.RegressionNetParams
				noCp.Checkpoints = nil
				tSettings.ChainCfgParams = &noCp
			}

			if tt.nilParams {
				tSettings.ChainCfgParams = nil
			}

			b := &Block{Height: tt.height}
			require.Equal(t, tt.want, b.skipOrderAndBlessedBelowCheckpoint(tSettings),
				"skipOrderAndBlessedBelowCheckpoint height=%d", tt.height)
		})
	}

	t.Run("nil settings", func(t *testing.T) {
		b := &Block{Height: 1000}
		require.False(t, b.skipOrderAndBlessedBelowCheckpoint(nil))
	})
}

// TestBlock_Valid_SkipsValidOrderAndBlessedBelowCheckpoint proves the gate is
// honoured end-to-end: with the flag ON and a block at/below the checkpoint,
// Valid() must succeed WITHOUT touching the txMetaStore at all (the store
// panics on any use). Before the gate existed this test panicked inside
// validOrderAndBlessed (RED); with the gate it passes (GREEN). All other
// Valid() checks (PoW, timestamp, coinbase, dedup) still run.
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
