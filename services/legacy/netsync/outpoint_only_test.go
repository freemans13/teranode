package netsync

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// outpointOnlySpyStore wraps NullStore to count BatchPreviousOutputsDecorate calls
// and capture the CreateOptions passed to Create, so tests can assert which legacy
// path engaged without standing up a real SQL store.
type outpointOnlySpyStore struct {
	*nullstore.NullStore
	decorateCalls atomic.Int32
	// unsupported models an Aerospike-like store: when set, SupportsOutpointOnlySpend
	// reports false so the capability gate closes. Zero value = supported (postgres/sql),
	// keeping the common construction a supporting store.
	unsupported bool
}

func (s *outpointOnlySpyStore) BatchPreviousOutputsDecorate(_ context.Context, txs []*bt.Tx) error {
	s.decorateCalls.Add(1)

	// Model the real store's contract: skip inputs already populated by Phase 1
	// (non-nil PreviousTxScript). The embedded NullStore overwrites every input
	// unconditionally, which would clobber Phase 1's same-block extension and
	// defeat the Phase-1 probe; honouring the skip keeps this test faithful to
	// production (BatchPreviousOutputsDecorate preserves Phase 1's work).
	for _, tx := range txs {
		for _, input := range tx.Inputs {
			if input == nil || input.PreviousTxScript != nil {
				continue
			}

			input.PreviousTxScript = bscript.NewFromBytes([]byte("test"))
			input.PreviousTxSatoshis = 100_000_000_000
		}
	}

	return nil
}

// SupportsOutpointOnlySpend reports the configured capability, modelling either a
// store that honours the fast path (postgres/sql) or one that does not (Aerospike),
// so legacyOutpointOnly's capability gate can be exercised in both directions.
func (s *outpointOnlySpyStore) SupportsOutpointOnlySpend() bool { return !s.unsupported }

// newOutpointOnlySettings returns settings with a single hard-coded checkpoint at
// checkpointHeight on the chain params. The outpoint-only fast path is now purely
// store-capability driven (the opt-in flag is retired), so eligibility is decided by
// the store assigned to the SyncManager, not by settings; sqlStore only selects the
// cosmetic UTXO store URL scheme.
func newOutpointOnlySettings(t *testing.T, sqlStore bool, checkpointHeight int32) (*settings.Settings, *chaincfg.Params) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	if sqlStore {
		u, err := url.Parse("sqlitememory://test")
		require.NoError(t, err)
		tSettings.UtxoStore.UtxoStore = u
	} else {
		// aerospike scheme = non-SQL
		u, err := url.Parse("aerospike://host:3000/ns/set")
		require.NoError(t, err)
		tSettings.UtxoStore.UtxoStore = u
	}

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight}}
	tSettings.ChainCfgParams = &params

	return tSettings, &params
}

// TestSyncManager_legacyOutpointOnly is the full truth table for the gate helper.
// Selection is purely store-capability driven now that the opt-in flag is retired:
// every conjunct (the store supports outpoint-only spends AND at/below the highest
// hard-coded checkpoint) must hold for the fast path to engage; any one missing keeps
// it OFF (fail-safe). A store that does not support outpoint-only spends (Aerospike)
// falls back to the normal below-checkpoint validation path.
func TestSyncManager_legacyOutpointOnly(t *testing.T) {
	const checkpointHeight = int32(1000)
	const below = uint32(500)
	const atCheckpoint = uint32(1000)
	const above = uint32(1500)

	tests := []struct {
		name       string
		supports   bool
		nilChain   bool
		noCheckpts bool
		height     uint32
		want       bool
	}{
		{name: "non-supporting store (aerospike), below", supports: false, height: below, want: false},
		{name: "supporting store, below checkpoint", supports: true, height: below, want: true},
		{name: "supporting store, at checkpoint", supports: true, height: atCheckpoint, want: true},
		{name: "supporting store, above checkpoint", supports: true, height: above, want: false},
		// Height 0 is fail-closed since the gates collapsed onto
		// model.BelowCheckpoint: genesis carries only a coinbase and never flows
		// through the legacy fast path, so excluding it costs nothing and keeps
		// one boundary definition everywhere.
		{name: "supporting store, height 0 fail-closed", supports: true, height: 0, want: false},
		{name: "supporting store, nil chain params", supports: true, nilChain: true, height: below, want: false},
		{name: "supporting store, no checkpoints", supports: true, noCheckpts: true, height: below, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings, params := newOutpointOnlySettings(t, tt.supports, checkpointHeight)

			sm := &SyncManager{
				settings:    tSettings,
				chainParams: params,
				logger:      ulogger.TestLogger{},
				utxoStore:   &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}, unsupported: !tt.supports},
			}

			if tt.nilChain {
				sm.chainParams = nil
				sm.settings.ChainCfgParams = nil
			}

			if tt.noCheckpts {
				noCp := chaincfg.RegressionNetParams
				noCp.Checkpoints = nil
				sm.chainParams = &noCp
				sm.settings.ChainCfgParams = &noCp
			}

			require.Equal(t, tt.want, sm.legacyOutpointOnly(tt.height),
				"legacyOutpointOnly(%d) supports=%v", tt.height, tt.supports)
		})
	}
}

// buildExtendedSubtreeBlock builds a 1-coinbase + n-regular-tx block, populates
// each regular tx's inputs with extended parent data, and returns the txMap +
// order ready for createSubtrees. Mirrors TestSyncManager_createSubtrees_*.
func buildExtendedSubtreeBlock(t *testing.T, height int32, n int) (*bsvutil.Block, *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper], []chainhash.Hash) {
	t.Helper()

	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{Version: 1, Timestamp: time.Now(), Bits: 0x1d00ffff},
	}

	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbase.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})
	msgBlock.Transactions = append(msgBlock.Transactions, coinbase)

	parentHash := chainhash.Hash{0x01}
	for i := 0; i < n; i++ {
		reg := wire.NewMsgTx(1)
		reg.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: parentHash, Index: uint32(i)},
			SignatureScript:  []byte{0x00, byte(i)},
			Sequence:         0xffffffff,
		})
		reg.AddTxOut(&wire.TxOut{Value: 1000 + int64(i), PkScript: []byte{0x76, 0xa9, 0x14, byte(i)}})
		msgBlock.Transactions = append(msgBlock.Transactions, reg)
	}

	block := bsvutil.NewBlock(msgBlock)
	block.SetHeight(height)

	sm := &SyncManager{logger: ulogger.TestLogger{}}
	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(block.Transactions()))
	txOrder, err := sm.createTxMap(context.Background(), block, txMap)
	require.NoError(t, err)

	// Populate extended parent data so the OFF path's calculateTransactionFee succeeds
	// (input 5000 > output ~1000, positive fee).
	for _, wrapper := range txMap.Range() {
		for _, in := range wrapper.Tx.Inputs {
			in.PreviousTxSatoshis = 5_000
			in.PreviousTxScript = &bscript.Script{0x76, 0xa9, 0x14}
		}
	}

	return block, txMap, txOrder
}

func makeSubtreeSlices(t *testing.T, txCount int) ([]*subtreepkg.Subtree, []*subtreepkg.Data, []*subtreepkg.Meta) {
	t.Helper()

	// maxItems must be a power of two; 4 mirrors TestSyncManager_createSubtrees_*.
	subtreeSize, numSubtrees, finalLeafCount, err := partitionLegacyBlock(txCount, 4)
	require.NoError(t, err)

	slices := make([]*subtreepkg.Subtree, numSubtrees)
	datas := make([]*subtreepkg.Data, numSubtrees)
	metas := make([]*subtreepkg.Meta, numSubtrees)

	for i := 0; i < numSubtrees; i++ {
		capacity := subtreeSize
		if i == numSubtrees-1 && finalLeafCount < subtreeSize {
			capacity = finalLeafCount
		}

		st, terr := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
		require.NoError(t, terr)

		if i == 0 {
			require.NoError(t, st.AddCoinbaseNode())
		}

		slices[i] = st
		datas[i] = subtreepkg.NewSubtreeData(st)
		metas[i] = subtreepkg.NewSubtreeMeta(st)
	}

	return slices, datas, metas
}

// TestSyncManager_createSubtrees_OutpointOnlyZeroFees is the paired ON/OFF fee test:
// with the gate ON every subtree node fee is 0 (calculateTransactionFee skipped);
// with the gate OFF the real fee (input-output) is computed as before.
func TestSyncManager_createSubtrees_OutpointOnlyZeroFees(t *testing.T) {
	initPrometheusMetrics()

	const checkpointHeight = int32(1000)
	const blockHeight = int32(500) // below checkpoint

	t.Run("gate ON => fees are 0", func(t *testing.T) {
		tSettings, params := newOutpointOnlySettings(t, true, checkpointHeight)
		block, txMap, txOrder := buildExtendedSubtreeBlock(t, blockHeight, 5)

		sm := &SyncManager{settings: tSettings, chainParams: params, logger: ulogger.TestLogger{},
			utxoStore: &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}}
		require.True(t, sm.legacyOutpointOnly(uint32(blockHeight)), "gate must be ON for this case")

		slices, datas, metas := makeSubtreeSlices(t, len(block.Transactions()))
		require.NoError(t, sm.createSubtrees(context.Background(), testBlockIdent(block), txOrder, txMap, slices, datas, metas, true))

		for _, st := range slices {
			for _, node := range st.Nodes {
				require.Equal(t, uint64(0), node.Fee, "outpoint-only path must stamp fee 0")
			}
		}
	})

	t.Run("gate OFF => real fees computed", func(t *testing.T) {
		// A non-supporting store keeps the capability gate closed.
		tSettings, params := newOutpointOnlySettings(t, true, checkpointHeight)
		block, txMap, txOrder := buildExtendedSubtreeBlock(t, blockHeight, 5)

		sm := &SyncManager{settings: tSettings, chainParams: params, logger: ulogger.TestLogger{},
			utxoStore: &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}, unsupported: true}}
		require.False(t, sm.legacyOutpointOnly(uint32(blockHeight)), "gate must be OFF for this case")

		slices, datas, metas := makeSubtreeSlices(t, len(block.Transactions()))
		require.NoError(t, sm.createSubtrees(context.Background(), testBlockIdent(block), txOrder, txMap, slices, datas, metas, false))

		var nonZeroFees int
		for _, st := range slices {
			for _, node := range st.Nodes {
				if node.Fee > 0 {
					nonZeroFees++
				}
			}
		}
		require.Positive(t, nonZeroFees, "default-off path must compute real (non-zero) fees")
	})
}

// buildSameBlockParentBlock builds a 1-coinbase + 2-regular-tx block where the
// SECOND regular tx spends an output of the FIRST regular tx (a same-block parent),
// and leaves every input un-extended (PreviousTxScript nil, PreviousTxSatoshis 0).
// This is what makes it a genuine probe of Phase 1: extendFromTxMap will populate
// the child's same-block-parent input from the parent's output *only if* Phase 1
// runs. It returns the block, txMap, txOrder, plus the child's input pointer and
// the parent output values Phase 1 should copy into it when it runs.
func buildSameBlockParentBlock(t *testing.T, height int32) (
	block *bsvutil.Block,
	txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper],
	txOrder []chainhash.Hash,
	childInput *bt.Input,
	wantSatoshis uint64,
) {
	t.Helper()

	const parentOutSatoshis = uint64(4200)

	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{Version: 1, Timestamp: time.Now(), Bits: 0x1d00ffff},
	}

	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbase.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})
	msgBlock.Transactions = append(msgBlock.Transactions, coinbase)

	// Parent regular tx: its input references an out-of-block parent; its single
	// output is what the child spends.
	parent := wire.NewMsgTx(1)
	parent.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{0x0a}, Index: 0},
		SignatureScript:  []byte{0x00, 0x01},
		Sequence:         0xffffffff,
	})
	parent.AddTxOut(&wire.TxOut{Value: int64(parentOutSatoshis), PkScript: []byte{0x76, 0xa9, 0x14, 0xaa}})
	msgBlock.Transactions = append(msgBlock.Transactions, parent)

	// Child regular tx: its input's PreviousOutPoint is patched below to reference
	// the parent tx's hash (a same-block parent) once createTxMap has computed it.
	child := wire.NewMsgTx(1)
	child.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{0x0b}, Index: 0},
		SignatureScript:  []byte{0x00, 0x02},
		Sequence:         0xffffffff,
	})
	child.AddTxOut(&wire.TxOut{Value: 1000, PkScript: []byte{0x76, 0xa9, 0x14, 0xbb}})
	msgBlock.Transactions = append(msgBlock.Transactions, child)

	block = bsvutil.NewBlock(msgBlock)
	block.SetHeight(height)

	sm := &SyncManager{logger: ulogger.TestLogger{}}
	txMap = txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(block.Transactions()))
	txOrder, err := sm.createTxMap(context.Background(), block, txMap)
	require.NoError(t, err)

	// txOrder[0] = coinbase, [1] = parent, [2] = child.
	require.Len(t, txOrder, 3)
	parentHash := txOrder[1]
	childHash := txOrder[2]

	// Point the child's input at the parent tx (same-block parent, output 0).
	childWrapper, found := txMap.Get(childHash)
	require.True(t, found)
	require.Len(t, childWrapper.Tx.Inputs, 1)
	childInput = childWrapper.Tx.Inputs[0]
	require.NoError(t, childInput.PreviousTxIDAdd(&parentHash))
	childInput.PreviousTxOutIndex = 0

	// Ensure a clean slate: no input carries pre-populated extended fields, so an
	// extended child after extendTransactions can only be Phase 1's doing.
	for _, wrapper := range txMap.Range() {
		for _, in := range wrapper.Tx.Inputs {
			in.PreviousTxSatoshis = 0
			in.PreviousTxScript = nil
		}
	}

	return block, txMap, txOrder, childInput, parentOutSatoshis
}

// TestSyncManager_extendTransactions_OutpointOnlySkipsDecorate is the paired ON/OFF
// test. It proves the outpoint-only path is a FULL no-op, not just a Phase-2 skip:
//   - Phase 2 (bulk decorate): gate ON never calls BatchPreviousOutputsDecorate,
//     gate OFF calls it exactly once.
//   - Phase 1 (same-block in-memory extension): gate ON leaves a same-block-parent
//     input un-extended (PreviousTxScript nil, PreviousTxSatoshis 0); gate OFF
//     extends it from the parent's output. This is the teeth: it distinguishes
//     "Phase 1 ran" from "Phase 1 skipped".
func TestSyncManager_extendTransactions_OutpointOnlySkipsDecorate(t *testing.T) {
	initPrometheusMetrics()

	const checkpointHeight = int32(1000)
	const blockHeight = int32(500)

	// run drives extendTransactions with a same-block-parent block and reports both
	// the decorate-call count and whether Phase 1 extended the child's input.
	run := func(t *testing.T, enabled bool) (decorateCalls int32, childScript *bscript.Script, childSatoshis, wantSatoshis uint64) {
		tSettings, params := newOutpointOnlySettings(t, true, checkpointHeight)

		// Store capability tracks the case so the gate matches the explicit path arg;
		// the spy still counts decorate calls on the OFF (non-supporting) path.
		spy := &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}, unsupported: !enabled}

		sm := &SyncManager{
			settings:    tSettings,
			chainParams: params,
			utxoStore:   spy,
			logger:      ulogger.TestLogger{},
		}

		block, txMap, txOrder, childInput, want := buildSameBlockParentBlock(t, blockHeight)
		require.Equal(t, enabled, sm.legacyOutpointOnly(uint32(blockHeight)))

		err := sm.extendTransactions(context.Background(), testBlockIdent(block), txOrder, txMap, enabled)
		require.NoError(t, err)

		return spy.decorateCalls.Load(), childInput.PreviousTxScript, childInput.PreviousTxSatoshis, want
	}

	t.Run("gate ON => full no-op: decorate skipped AND Phase 1 skipped", func(t *testing.T) {
		calls, childScript, childSatoshis, _ := run(t, true)
		require.Equal(t, int32(0), calls, "outpoint-only path must NOT call BatchPreviousOutputsDecorate")
		require.Nil(t, childScript, "outpoint-only path must NOT run Phase 1: same-block-parent input left un-extended (script)")
		require.Equal(t, uint64(0), childSatoshis, "outpoint-only path must NOT run Phase 1: same-block-parent input left un-extended (satoshis)")
	})

	t.Run("gate OFF => full pass: decorate called AND Phase 1 extended", func(t *testing.T) {
		calls, childScript, childSatoshis, want := run(t, false)
		require.Equal(t, int32(1), calls, "default-off path must call BatchPreviousOutputsDecorate once")
		require.NotNil(t, childScript, "default-off path must run Phase 1: same-block-parent input extended (script)")
		require.Equal(t, want, childSatoshis, "default-off path must run Phase 1: same-block-parent input extended (satoshis)")
	})
}

// TestSyncManager_needsParentMinedWait verifies the parent-mined wait is skipped
// only on the below-checkpoint outpoint-only fast path. It reuses the same
// store-capability harness as TestSyncManager_legacyOutpointOnly: a supporting
// store (spy over NullStore) for the "SQL" cases, a plain NullStore otherwise.
func TestSyncManager_needsParentMinedWait(t *testing.T) {
	const checkpointHeight = int32(1000)

	tests := []struct {
		name     string
		supports bool
		height   uint32
		want     bool
	}{
		{name: "height 0 never waits", supports: true, height: 0, want: false},
		{name: "height 1 never waits", supports: true, height: 1, want: false},
		{name: "non-supporting store, below checkpoint: waits", supports: false, height: 500, want: true},
		{name: "supporting store, below checkpoint: skips", supports: true, height: 500, want: false},
		{name: "supporting store, at checkpoint: skips", supports: true, height: 1000, want: false},
		{name: "supporting store, above checkpoint: waits", supports: true, height: 1500, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings, params := newOutpointOnlySettings(t, true, checkpointHeight)

			sm := &SyncManager{
				settings:    tSettings,
				chainParams: params,
				logger:      ulogger.TestLogger{},
				utxoStore:   &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}, unsupported: !tt.supports},
			}

			require.Equal(t, tt.want, sm.needsParentMinedWait(tt.height),
				"needsParentMinedWait(%d) supports=%v", tt.height, tt.supports)
		})
	}
}
