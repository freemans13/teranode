package netsync

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestReuseBlockIDFromUTXODeclinesACommittedBlocksID: block Y's first
// non-coinbase transaction T is shared with block X, which is committed, so the
// id recorded on T is X's. Adopting it stamped Y's records with X's id, made
// X's locked records look like Y's own leftovers, and ended at AddBlock
// refusing the id on every retry, since each retry read the same id back. The
// id is declined, and the caller reserves one of Y's own. X itself, retried,
// still reuses its id, and so does a block whose recorded id no committed
// block holds.
func TestReuseBlockIDFromUTXODeclinesACommittedBlocksID(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings, params := newOutpointOnlySettings(t, true, true, 1000)

	storeURL, err := url.Parse("sqlitememory:///legacy_reuse_block_id")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, nil)
	require.NoError(t, err)

	sm := &SyncManager{ctx: ctx, settings: tSettings, chainParams: params, logger: logger, utxoStore: store, blockchainClient: blockchainClient}

	tip, _, err := blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	x := testhelpers.CreateTestBlocksWithPrev(t, 1, tip.Hash())[0]
	require.NoError(t, blockchainClient.AddBlock(ctx, x, "test-peer"))

	_, xMeta, err := blockchainClient.GetBlockHeader(ctx, x.Hash())
	require.NoError(t, err)

	script, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	newTx := func(seed string, blockID uint32) *bt.Tx {
		tx := bt.NewTx()
		funding := chainhash.HashH([]byte(seed))
		input := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
		require.NoError(t, input.PreviousTxIDAdd(&funding))
		tx.Inputs = append(tx.Inputs, input)
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 500, LockingScript: script})
		_, createErr := store.Create(ctx, tx, 100, utxostore.WithSkipExtendedInputs(true),
			utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: blockID, BlockHeight: 100}))
		require.NoError(t, createErr)

		return tx
	}

	shared := newTx("shared with committed block X", xMeta.ID)
	txOrder := []chainhash.Hash{{}, *shared.TxIDChainHash()}

	y := blockIdent{hash: chainhash.HashH([]byte("block Y shares X's first tx"))}
	_, reused, err := sm.reuseBlockIDFromUTXO(ctx, y, txOrder)
	require.NoError(t, err)
	require.False(t, reused, "Y must not adopt the id committed block X holds")

	id, reused, err := sm.reuseBlockIDFromUTXO(ctx, blockIdent{hash: *x.Hash()}, txOrder)
	require.NoError(t, err)
	require.True(t, reused, "control: X retried keeps its own id")
	require.Equal(t, xMeta.ID, id)

	ownTx := newTx("Y's own earlier attempt", xMeta.ID+1000)
	id, reused, err = sm.reuseBlockIDFromUTXO(ctx, y, []chainhash.Hash{{}, *ownTx.TxIDChainHash()})
	require.NoError(t, err)
	require.True(t, reused, "control: an id no committed block holds is reused")
	require.Equal(t, xMeta.ID+1000, id)

	// X committed while Y's attempt was already running under X's id: X's
	// record is still locked under that id after its unlock failed. The create
	// phase must fail rather than go on stamping Y's mined info under X's id.
	locked := bt.NewTx()
	funding := chainhash.HashH([]byte("X's record, still locked"))
	input := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, input.PreviousTxIDAdd(&funding))
	locked.Inputs = append(locked.Inputs, input)
	locked.Outputs = append(locked.Outputs, &bt.Output{Satoshis: 500, LockingScript: script})
	_, err = store.Create(ctx, locked, 100, utxostore.WithSkipExtendedInputs(true), utxostore.WithLocked(true),
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: xMeta.ID, BlockHeight: 100}))
	require.NoError(t, err)

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper]()
	txMap.Set(*locked.TxIDChainHash(), &TxMapWrapper{Tx: locked})

	_, err = sm.createUtxos(ctx, txMap, y, xMeta.ID, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "held by another committed block")

	_, err = store.Get(ctx, locked.TxIDChainHash())
	require.NoError(t, err, "X's record stays")
}
