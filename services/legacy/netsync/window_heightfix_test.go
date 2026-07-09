package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestPrepareBlockForWindow_ThreadsDerivedHeight is the regression test for the
// live-testnet bug where prepareBlockForWindow re-wrapped a raw wire.MsgBlock
// and read block.Height() from the fresh wrapper, which is always -1 (wire
// messages carry no height). That drove safeconversion.Int32ToUint32(-1) to
// fail with the "negative value ... uint32: -1" error, disconnecting the sync
// peer and wedging the node.
//
// The caller (handleBlockMsgWithWindow) derives the real height but set it on
// its OWN wrapper, not the one prepareBlockForWindow builds. The fix threads
// the derived height in as a parameter and stamps it onto the wrapper so both
// prepareBlockForWindow and prepareSubtrees (which also reads block.Height())
// see the correct value.
//
// This test is a genuine discriminator: it builds a block exactly as a real
// peer delivers it — a raw wire.MsgBlock whose bsvutil.Block wrapper reports
// height -1 — and passes a known derived height in. It first asserts the trap
// (fresh wrapper == -1), then runs the real prepareBlockForWindow end-to-end.
// The single-coinbase shape makes prepareSubtrees return early, so no subtree
// store is needed, yet the height read at the top of prepareSubtrees and the
// model.NewBlock height are still exercised. Before the fix this test fails
// with the -1 conversion error; after the fix the returned model.Block carries
// the derived height.
func TestPrepareBlockForWindow_ThreadsDerivedHeight(t *testing.T) {
	initPrometheusMetrics()

	const derivedHeight = uint32(1234)

	// Build a block exactly as a real peer delivers it: a raw wire.MsgBlock
	// with a single coinbase tx and an easy regtest PoW target (0x207fffff),
	// which any header hash satisfies at nonce 0.
	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			Version:   1,
			PrevBlock: [32]byte{0x01},
			Timestamp: time.Unix(1231006505, 0),
			Bits:      0x207fffff,
			Nonce:     0,
		},
	}

	coinbaseMsgTx := wire.NewMsgTx(1)
	coinbaseMsgTx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: [32]byte{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbaseMsgTx.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})
	msgBlock.Transactions = append(msgBlock.Transactions, coinbaseMsgTx)

	// The trap: a freshly wrapped bsvutil.Block from a raw wire.MsgBlock reports
	// height -1 because the wire message carries no height field. This is the
	// exact object prepareBlockForWindow builds internally.
	require.Equal(t, int32(-1), bsvutil.NewBlock(msgBlock).Height(),
		"a fresh wrapper of a raw peer block must report height -1 (documents the trap)")

	tSettings := test.CreateBaseTestSettings(t)

	sm := &SyncManager{
		settings:  tSettings,
		logger:    ulogger.TestLogger{},
		utxoStore: &nullstore.NullStore{},
	}

	testPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})
	blockHash := msgBlock.BlockHash()

	// Run the real prepareBlockForWindow with the derived height threaded in.
	// Before the fix this returned the "negative value ... uint32: -1" error.
	prepared, err := sm.prepareBlockForWindow(context.Background(), testPeer, blockHash, msgBlock, derivedHeight)
	require.NoError(t, err,
		"prepareBlockForWindow must not fail converting a re-wrapped block's height")
	require.NotNil(t, prepared)
	require.Equal(t, derivedHeight, prepared.Height,
		"prepared model.Block must carry the derived height, not the -1 from the fresh wrapper")
}
