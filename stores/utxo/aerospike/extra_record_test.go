package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// spentElement builds the 68-byte form: 32-byte utxo hash + 36-byte spending data.
func spentElement(t *testing.T, marker byte) []uint8 {
	t.Helper()

	var (
		utxoHash chainhash.Hash
		spendTx  chainhash.Hash
	)

	utxoHash[0] = marker
	spendTx[0] = marker + 1

	out := make([]uint8, 0, 68)
	out = append(out, utxoHash[:]...)
	out = append(out, spendpkg.NewSpendingData(&spendTx, 1).Bytes()...)

	require.Len(t, out, 68)

	return out
}

// unspentElement builds the 32-byte form: hash only, no spending data.
func unspentElement(marker byte) []uint8 {
	var utxoHash chainhash.Hash
	utxoHash[0] = marker

	return utxoHash[:]
}

// TestExtraRecordElementShapes pins the distinction issue 1440 turns on. Whether an
// output is spent is consensus state, and a nil slot reads as UNSPENT — so a
// torn extra record must fail the read rather than silently hand back a spent
// output as spendable. But 32 bytes is the LEGITIMATE unspent encoding, so
// rejecting everything that is not 68 bytes would break every unspent output
// on every large transaction. Both halves are pinned here.
func TestExtraRecordElementShapes(t *testing.T) {
	t.Run("68 bytes is spent and 32 bytes is unspent", func(t *testing.T) {
		spendingDatas := make([]*spendpkg.SpendingData, 2)

		require.NoError(t, applyExtraRecordUTXOs(&chainhash.Hash{}, 1, []interface{}{
			spentElement(t, 0x11),
			unspentElement(0x22),
		}, 0, spendingDatas))

		require.NotNil(t, spendingDatas[0], "68-byte element must record spending data")
		require.Nil(t, spendingDatas[1], "32-byte element is a legitimate unspent output")
	})

	t.Run("a nil element is a provably-unspendable output, not damage", func(t *testing.T) {
		// GetBinsToStore leaves utxos[i] nil for every output that
		// utxo.ShouldStoreOutputAsUTXO rejects (OP_FALSE OP_RETURN in any era,
		// bare OP_RETURN and oversized scripts pre-Genesis). Those nils are
		// packed as msgpack nil and come back as nil list elements, so a large
		// transaction with a data output has them in its extra records.
		// Rejecting them would fail the read of a perfectly healthy record.
		spendingDatas := make([]*spendpkg.SpendingData, 3)

		require.NoError(t, applyExtraRecordUTXOs(&chainhash.Hash{}, 1, []interface{}{
			nil,
			spentElement(t, 0x55),
			nil,
		}, 0, spendingDatas))

		require.Nil(t, spendingDatas[0], "an unspendable output has no spending data")
		require.NotNil(t, spendingDatas[1], "the spent output either side of it must still be read")
		require.Nil(t, spendingDatas[2])
	})

	t.Run("a short element fails the read", func(t *testing.T) {
		spendingDatas := make([]*spendpkg.SpendingData, 1)

		err := applyExtraRecordUTXOs(&chainhash.Hash{}, 1, []interface{}{make([]uint8, 40)}, 0, spendingDatas)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected 32 (unspent) or 68 (spent)")
	})

	t.Run("a non-byte element fails the read", func(t *testing.T) {
		spendingDatas := make([]*spendpkg.SpendingData, 1)

		err := applyExtraRecordUTXOs(&chainhash.Hash{}, 1, []interface{}{"not bytes"}, 0, spendingDatas)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not bytes")
	})

	t.Run("more outputs than the transaction has fails instead of panicking", func(t *testing.T) {
		spendingDatas := make([]*spendpkg.SpendingData, 1)

		require.NotPanics(t, func() {
			err := applyExtraRecordUTXOs(&chainhash.Hash{}, 1, []interface{}{
				unspentElement(0x33),
				unspentElement(0x44),
			}, 0, spendingDatas)
			require.Error(t, err)
			require.Contains(t, err.Error(), "more outputs than the transaction has")
		})
	})
}
