package subtreevalidation

import (
	"bytes"
	"encoding/hex"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// canonicalTxHex is a 1-input, 1-output transaction in minimal encoding.
const canonicalTxHex = "010000000100000000000000000000000000000000000000000000000000000000000000000000000000ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000"

// withInputCountPrefix rebuilds the fixture with the input-count CompactSize
// rewritten as the given (over-long) byte sequence.
func withInputCountPrefix(t *testing.T, prefix []byte) []byte {
	t.Helper()

	raw, err := hex.DecodeString(canonicalTxHex)
	require.NoError(t, err)

	out := append([]byte{}, raw[:4]...) // version
	out = append(out, prefix...)        // non-minimal input count
	out = append(out, raw[5:]...)       // the rest, unchanged

	return out
}

// TestCheckCanonicalTxEncoding pins issue 1421. go-bt accepts a non-minimal
// CompactSize and then re-serializes it canonically, so the txid Teranode
// computes matches the canonical one — an attacker can ship non-minimal bytes
// while committing canonical txids in the merkle tree, and Teranode accepts a
// block every SV Node rejects at parse. The check must reject the wire bytes
// rather than accept the canonicalized transaction.
func TestCheckCanonicalTxEncoding(t *testing.T) {
	t.Run("canonical encoding is accepted", func(t *testing.T) {
		raw, err := hex.DecodeString(canonicalTxHex)
		require.NoError(t, err)

		tx, err := bt.NewTxFromBytes(raw)
		require.NoError(t, err)

		require.NoError(t, checkCanonicalTxEncoding(tx, int64(len(raw))))
	})

	t.Run("two-byte form of a one-byte count is rejected", func(t *testing.T) {
		raw := withInputCountPrefix(t, []byte{0xfd, 0x01, 0x00})

		tx, err := bt.NewTxFromBytes(raw)
		require.NoError(t, err, "go-bt accepts the non-minimal form — that is the hazard")

		err = checkCanonicalTxEncoding(tx, int64(len(raw)))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not canonically encoded")
		require.True(t, errors.Is(err, errors.ErrTxInvalid))
	})

	t.Run("eight-byte form of a one-byte count is rejected", func(t *testing.T) {
		raw := withInputCountPrefix(t, []byte{0xff, 0x01, 0, 0, 0, 0, 0, 0, 0})

		tx, err := bt.NewTxFromBytes(raw)
		require.NoError(t, err)

		err = checkCanonicalTxEncoding(tx, int64(len(raw)))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not canonically encoded")
	})

	t.Run("nil transaction is rejected", func(t *testing.T) {
		require.Error(t, checkCanonicalTxEncoding(nil, 0))
	})
}

// TestNonMinimalTxKeepsCanonicalTxID documents the property that makes this a
// consensus hazard rather than a curiosity: the non-minimal bytes parse to a
// transaction whose txid is identical to the canonical one, so a merkle root
// built from canonical txids validates against non-canonical wire bytes.
func TestNonMinimalTxKeepsCanonicalTxID(t *testing.T) {
	raw, err := hex.DecodeString(canonicalTxHex)
	require.NoError(t, err)

	canonical, err := bt.NewTxFromBytes(raw)
	require.NoError(t, err)

	nonMinimal, err := bt.NewTxFromBytes(withInputCountPrefix(t, []byte{0xfd, 0x01, 0x00}))
	require.NoError(t, err)

	require.Equal(t, canonical.TxID(), nonMinimal.TxID(),
		"go-bt canonicalizes on re-serialization, so the merkle check cannot catch this — the parse path must")
}

// TestCheckCanonicalSubtreeData pins the payload-level check used on the bulk
// /subtree_data/ fetch, which parses via go-subtree and therefore cannot see
// per-transaction byte counts. Round-2 review demonstrated that path accepted
// non-minimal bytes and stored the canonicalised form, blessing a subtree whose
// block then skipped the per-transaction parse entirely.
func TestCheckCanonicalSubtreeData(t *testing.T) {
	t.Run("matching sizes are accepted", func(t *testing.T) {
		payload := []byte{1, 2, 3, 4, 5}
		require.NoError(t, checkCanonicalSubtreeData(int64(len(payload)), payload))
	})

	t.Run("longer wire form than canonical serialization is rejected", func(t *testing.T) {
		payload := []byte{1, 2, 3, 4, 5}

		err := checkCanonicalSubtreeData(int64(len(payload))+2, payload)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not canonically encoded")
		require.True(t, errors.Is(err, errors.ErrTxInvalid))
	})
}

// TestCountingReaderCountsExactly pins the byte counter the bulk check depends
// on: an undercount would let non-minimal bytes through.
func TestCountingReaderCountsExactly(t *testing.T) {
	payload := make([]byte, 5000)
	for i := range payload {
		payload[i] = byte(i)
	}

	c := &countingReader{r: bytes.NewReader(payload)}

	read, err := io.Copy(io.Discard, c)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), read)
	require.Equal(t, int64(len(payload)), c.n)
}
