package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test_getUtxoBlockHeightsAndExtendTx_Prefetched is the Phase-1 parity test:
// when the per-level bulk reader has already fetched a tx's parents, supplying
// them via PrefetchedParents must yield IDENTICAL block heights to the per-parent
// store path AND perform ZERO `Get` calls against the store. This is the core
// correctness contract — the bulk read is a pure read-source swap, not a change
// in what the validator computes (bitcoin-expert invariants #2/#4: the data and
// the unconfirmed-parent sentinel logic are unchanged; only the source differs).
func Test_getUtxoBlockHeightsAndExtendTx_Prefetched(t *testing.T) {
	ctx := context.Background()

	// Same extended 3-parent tx used by Test_getUtxoBlockHeights.
	tx, err := bt.NewTxFromString("010000000000000000ef03fe1a25c8774c1e827f9ebdae731fe609ff159d6f7c15094e1d467a99a01e03100000000002012affffffffa086010000000000018253a080075d834402e916390940782236b29d23db6f52dfc940a12b3eff99159c0000000000ffffffffa086010000000000100f5468616e6b7320456c69676975732161e4ed95239756bbb98d11dcf973146be0c17cc1cc94340deb8bc4d44cd88e92000000000a516352676a675168948cffffffff40548900000000000763516751676a680220aa4400000000001976a9149bc0bbdd3024da4d0c38ed1aecf5c68dd1d3fa1288ac20aa4400000000001976a914169ff4804fd6596deb974f360c21584aa1e19c9788ac00000000")
	require.NoError(t, err)

	parent0, err := chainhash.NewHashFromStr("10031ea0997a461d4e09157c6f9d15ff09e61f73aebd9e7f821e4c77c8251afe")
	require.NoError(t, err)
	parent1, err := chainhash.NewHashFromStr("9c1599ff3e2ba140c9df526fdb239db236227840093916e90244835d0780a053")
	require.NoError(t, err)
	parent2, err := chainhash.NewHashFromStr("928ed84cd4c48beb0d3494ccc17cc1e06b1473f9dc118db9bb56972395ede461")
	require.NoError(t, err)

	// Identical data to the "mined parent txs" subtest of Test_getUtxoBlockHeights:
	// parent1 has empty BlockHeights (unconfirmed → sentinel).
	//
	// Every parent carries outputs: the validator re-extends unconditionally
	// (GHSA-v76m-6vc7-g7c7), so a prefetched entry without them fails the
	// prefetch guard and falls back to a per-parent store Get.
	prefetched := map[chainhash.Hash]*meta.Data{
		*parent0: {BlockHeights: []uint32{125, 126}, Tx: prefetchParentTx(1000000)},
		*parent1: {BlockHeights: []uint32{}, Tx: prefetchParentTx(2000000)},
		*parent2: {BlockHeights: []uint32{768, 769}, Tx: prefetchParentTx(3000000)},
	}

	mockUtxoStore := &utxostore.MockUtxostore{}
	v := &Validator{settings: settings.NewSettings(), utxoStore: mockUtxoStore}

	utxoHeights, err := v.getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), prefetched)
	require.NoError(t, err)

	require.Equal(t, []uint32{125, unconfirmedParentHeight, 768}, utxoHeights,
		"prefetched heights must match the per-parent store path exactly")

	mockUtxoStore.AssertNotCalled(t, "ParentOutputsForValidation", mock.Anything, mock.Anything)
}

// Test_getUtxoBlockHeightsAndExtendTx_PartialPrefetchFallsBackToStore proves the
// safety invariant: a parent absent from PrefetchedParents falls back to a store
// Get, so a partial prefetch never reduces correctness — the heights are identical
// to the all-store path, and only the missing parent is read.
func Test_getUtxoBlockHeightsAndExtendTx_PartialPrefetchFallsBackToStore(t *testing.T) {
	ctx := context.Background()

	tx, err := bt.NewTxFromString("010000000000000000ef03fe1a25c8774c1e827f9ebdae731fe609ff159d6f7c15094e1d467a99a01e03100000000002012affffffffa086010000000000018253a080075d834402e916390940782236b29d23db6f52dfc940a12b3eff99159c0000000000ffffffffa086010000000000100f5468616e6b7320456c69676975732161e4ed95239756bbb98d11dcf973146be0c17cc1cc94340deb8bc4d44cd88e92000000000a516352676a675168948cffffffff40548900000000000763516751676a680220aa4400000000001976a9149bc0bbdd3024da4d0c38ed1aecf5c68dd1d3fa1288ac20aa4400000000001976a914169ff4804fd6596deb974f360c21584aa1e19c9788ac00000000")
	require.NoError(t, err)

	parent0, err := chainhash.NewHashFromStr("10031ea0997a461d4e09157c6f9d15ff09e61f73aebd9e7f821e4c77c8251afe")
	require.NoError(t, err)
	parent1, err := chainhash.NewHashFromStr("9c1599ff3e2ba140c9df526fdb239db236227840093916e90244835d0780a053")
	require.NoError(t, err)
	parent2, err := chainhash.NewHashFromStr("928ed84cd4c48beb0d3494ccc17cc1e06b1473f9dc118db9bb56972395ede461")
	require.NoError(t, err)

	// Prefetch covers only parent0 and parent2; parent1 must fall back to the store.
	prefetched := map[chainhash.Hash]*meta.Data{
		*parent0: {BlockHeights: []uint32{125, 126}, Tx: prefetchParentTx(1000000)},
		*parent2: {BlockHeights: []uint32{768, 769}, Tx: prefetchParentTx(3000000)},
	}

	mockUtxoStore := &utxostore.MockUtxostore{}
	v := &Validator{settings: settings.NewSettings(), utxoStore: mockUtxoStore}

	// Only parent1 should ever be read from the store.
	mockUtxoStore.On("ParentOutputsForValidation", mock.Anything, mock.MatchedBy(func(ops []utxostore.Outpoint) bool {
		return len(ops) == 1 && ops[0].TxID.IsEqual(parent1)
	})).Return([]utxostore.ParentOutput{{Status: utxostore.ParentOutputNotMined, Satoshis: 2000000, LockingScript: bscript.NewFromBytes([]byte{0x51})}}, nil).Once()

	utxoHeights, err := v.getUtxoBlockHeightsAndExtendTx(ctx, tx, tx.TxID(), prefetched)
	require.NoError(t, err)

	require.Equal(t, []uint32{125, unconfirmedParentHeight, 768}, utxoHeights)
	// parent0 and parent2 came from the prefetch; only parent1 hit the store.
	mockUtxoStore.AssertNumberOfCalls(t, "ParentOutputsForValidation", 1)
}

// Test_getUtxoBlockHeightsAndExtendTx_PrefetchedVoutOutOfRange guards the extend
// path against an out-of-range PreviousTxOutIndex. The vout comes from the
// (untrusted) child transaction; a raw tx that references a real parent but a
// vout beyond that parent's output count must be rejected with a clean error
// rather than panicking on the parent's Outputs[vout].
func Test_getUtxoBlockHeightsAndExtendTx_PrefetchedVoutOutOfRange(t *testing.T) {
	ctx := context.Background()

	parentHash := chainhash.Hash{}
	childTx := &bt.Tx{Inputs: []*bt.Input{{PreviousTxOutIndex: 99}}}
	require.NoError(t, childTx.Inputs[0].PreviousTxIDAdd(&parentHash))

	// Parent exists and is confirmed, but has only 2 outputs (vouts 0 and 1).
	prefetched := map[chainhash.Hash]*meta.Data{
		parentHash: {BlockHeights: []uint32{100}, Tx: &bt.Tx{Outputs: []*bt.Output{{}, {}}}},
	}

	v := &Validator{}

	_, err := v.getUtxoBlockHeightsAndExtendTx(ctx, childTx, "child", prefetched)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has no output for index")
}

// Test_getUtxoBlockHeightsAndExtendTx_MixedBodyfulAndBodylessRace is test 4: run
// under `-race`. Several distinct parents are resolved concurrently by the real
// errgroup in getUtxoBlockHeightsAndExtendTx — one WITH a body (the ordinary
// extend path) and two body-less (the coin-table fallback, resolved by two
// different goroutines racing over the SAME shared tx at the same time). Each
// goroutine must only ever read and write the inputs of the parent it owns.
// GetBatcherSize is raised so the errgroup actually runs the three lookups
// concurrently rather than serialising them behind a limit of 1.
func Test_getUtxoBlockHeightsAndExtendTx_MixedBodyfulAndBodylessRace(t *testing.T) {
	ctx := context.Background()

	parentWithBody := chainhash.Hash{0x01}
	parentBodyless1 := chainhash.Hash{0x02}
	parentBodyless2 := chainhash.Hash{0x03}

	tx := &bt.Tx{}
	addInput := func(parent chainhash.Hash, vout uint32) {
		in := &bt.Input{PreviousTxOutIndex: vout, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{})}
		require.NoError(t, in.PreviousTxIDAdd(&parent))
		tx.Inputs = append(tx.Inputs, in)
	}
	addInput(parentWithBody, 0)
	addInput(parentBodyless1, 0)
	addInput(parentBodyless2, 0)

	store := &utxostore.MockUtxostore{}
	store.On("Get", mock.Anything, mock.MatchedBy(func(h *chainhash.Hash) bool { return h.IsEqual(&parentWithBody) }), mock.Anything).
		Return(&meta.Data{BlockHeights: []uint32{111}, Tx: prefetchParentTx(1111)}, nil)
	store.On("Get", mock.Anything, mock.MatchedBy(func(h *chainhash.Hash) bool { return h.IsEqual(&parentBodyless1) }), mock.Anything).
		Return(&meta.Data{BlockHeights: []uint32{222}, Tx: nil}, nil)
	store.On("Get", mock.Anything, mock.MatchedBy(func(h *chainhash.Hash) bool { return h.IsEqual(&parentBodyless2) }), mock.Anything).
		Return(&meta.Data{BlockHeights: []uint32{333}, Tx: nil}, nil)

	script1 := bscript.NewFromBytes([]byte{0x52})
	script2 := bscript.NewFromBytes([]byte{0x53})

	store.On("PreviousOutputsDecorate", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			scratch, ok := args.Get(1).(*bt.Tx)
			require.True(t, ok)
			require.Len(t, scratch.Inputs, 1, "each parent's scratch tx must hold only its own input")

			switch {
			case scratch.Inputs[0].PreviousTxIDChainHash().IsEqual(&parentBodyless1):
				scratch.Inputs[0].PreviousTxScript = script1
				scratch.Inputs[0].PreviousTxSatoshis = 2000
			case scratch.Inputs[0].PreviousTxIDChainHash().IsEqual(&parentBodyless2):
				scratch.Inputs[0].PreviousTxScript = script2
				scratch.Inputs[0].PreviousTxSatoshis = 3000
			default:
				t.Fatalf("unexpected parent in scratch tx: %s", scratch.Inputs[0].PreviousTxIDChainHash())
			}
		}).
		Return(nil)

	tSettings := settings.NewSettings()
	tSettings.UtxoStore.GetBatcherSize = 8

	v := &Validator{settings: tSettings, utxoStore: store}

	utxoHeights, err := v.getUtxoBlockHeightsAndExtendTx(ctx, tx, "race-test-tx", nil)
	require.NoError(t, err)
	require.Equal(t, []uint32{111, 222, 333}, utxoHeights)

	require.Equal(t, uint64(1111), tx.Inputs[0].PreviousTxSatoshis)
	require.Equal(t, []byte{0x51}, tx.Inputs[0].PreviousTxScript.Bytes())

	require.Equal(t, uint64(2000), tx.Inputs[1].PreviousTxSatoshis)
	require.Equal(t, script1.Bytes(), tx.Inputs[1].PreviousTxScript.Bytes())

	require.Equal(t, uint64(3000), tx.Inputs[2].PreviousTxSatoshis)
	require.Equal(t, script2.Bytes(), tx.Inputs[2].PreviousTxScript.Bytes())
}

// prefetchParentTx builds the minimal parent metadata the unconditional
// re-extension path needs: a single spendable output at vout 0.
func prefetchParentTx(satoshis uint64) *bt.Tx {
	return &bt.Tx{
		Outputs: []*bt.Output{{Satoshis: satoshis, LockingScript: bscript.NewFromBytes([]byte{0x51})}},
	}
}
