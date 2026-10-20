package subtreevalidation

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestReadTransactionsFromSubtreeDataStream_NonMinimalVarInt pins issue 1421 on
// the subtree-data stream a peer serves for block validation. go-bt (v2.7.0+,
// go-bt PR 174) rejects a non-minimal CompactSize at parse; this test notices if
// a go-bt bump ever relaxes that, since Teranode carries no detector of its own.
// The error must not carry the TxInvalid code: non-minimality is a fault in how
// the peer delivered the bytes, and the same subtree from an honest peer is valid.
func TestReadTransactionsFromSubtreeDataStream_NonMinimalVarInt(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	tx1, err := createTestTransaction("fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4")
	require.NoError(t, err)

	tx2, err := createTestTransaction("6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4")
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))
	require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), 1, 2))

	// Rewrite tx2's 1-byte input count as the 3-byte form.
	raw := tx2.Bytes()
	require.Less(t, raw[4], byte(0xfd), "fixture transaction must have a 1-byte input count")

	mangled := append([]byte{}, raw[:4]...)
	mangled = append(mangled, 0xfd, raw[4], 0x00)
	mangled = append(mangled, raw[5:]...)

	for _, arena := range []*bt.Arena{nil, bt.NewArena(4096)} {
		var stream bytes.Buffer
		stream.Write(tx1.Bytes())
		stream.Write(mangled)

		var txs []*bt.Tx

		_, err = server.readTransactionsFromSubtreeDataStream(subtree, &stream, &txs, arena)
		require.Error(t, err)
		require.ErrorIs(t, err, bt.ErrNonMinimalVarInt, "go-bt must be what rejects it")
		require.True(t, errors.Is(err, errors.ErrProcessing))
		require.False(t, errors.Is(err, errors.ErrTxInvalid), "a delivery fault must not condemn the transaction")
	}
}
