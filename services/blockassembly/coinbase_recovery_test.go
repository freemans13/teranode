package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/stretchr/testify/require"
)

// coinbaseTxForHeader clones the shared fixture coinbase (see addBlockWithMinedSet)
// and perturbs its scriptSig with bytes derived from the header hash, so that
// each header produces a coinbase transaction with a distinct TxID. Without
// this, every block built from the shared fixture coinbase string would
// collide on the same coinbase txid, making it impossible to seed the UTXO
// store with "the coinbase for height N" without also seeding it for every
// other height that reuses the same fixture.
func coinbaseTxForHeader(t *testing.T, header *model.BlockHeader) *bt.Tx {
	t.Helper()

	coinbaseTx, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	headerHash := header.Hash()

	scriptSig := make([]byte, 0, len(coinbaseTx.Inputs[0].UnlockingScript.Bytes())+len(headerHash))
	scriptSig = append(scriptSig, coinbaseTx.Inputs[0].UnlockingScript.Bytes()...)
	scriptSig = append(scriptSig, headerHash[:]...)

	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)

	return coinbaseTx
}

// addCanonicalBlockWithCoinbase is a variant of addBlockWithMinedSet (see
// reset_bug_test.go) that carries a caller-supplied coinbase transaction
// instead of the shared fixture coinbase. canonicalCoinbaseAt needs to
// observe distinct coinbases per height, which addBlockWithMinedSet's
// hardcoded coinbase cannot provide.
func addCanonicalBlockWithCoinbase(ctx context.Context, t *testing.T, items *baTestItems, blockHeader *model.BlockHeader, coinbaseTx *bt.Tx) {
	t.Helper()

	err := items.blockchainClient.AddBlock(ctx, &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}, "", blockchainoptions.WithMinedSet(true))
	require.NoError(t, err)
}

// TestCanonicalCoinbaseAt exercises the divergence probe against a real
// sqlitememory UTXO store and blockchain client (per AGENTS.md testing rules
// - no mocking the blockchain client/store).
func TestCanonicalCoinbaseAt(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t)
	require.NotNil(t, items)

	// height 1: canonical block carries cb1, and the store holds cb1 -> present.
	cb1 := coinbaseTxForHeader(t, blockHeader1)
	addCanonicalBlockWithCoinbase(ctx, t, items, blockHeader1, cb1)

	_, err := items.utxoStore.Create(ctx, cb1, 1)
	require.NoError(t, err)

	present, blk, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 1)
	require.NoError(t, err)
	require.True(t, present)
	require.NotNil(t, blk)
	require.True(t, blk.CoinbaseTx.TxIDChainHash().IsEqual(cb1.TxIDChainHash()))

	// height 2: canonical block carries cb2, but cb2 was never created in the
	// store -> not present, even though a (different) coinbase exists at height 1.
	cb2 := coinbaseTxForHeader(t, blockHeader2)
	addCanonicalBlockWithCoinbase(ctx, t, items, blockHeader2, cb2)

	present2, blk2, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 2)
	require.NoError(t, err)
	require.False(t, present2)
	require.NotNil(t, blk2)
	require.True(t, blk2.CoinbaseTx.TxIDChainHash().IsEqual(cb2.TxIDChainHash()))
}
