package aerospike

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// buildParentTx returns a small complete transaction usable as a spend parent.
func buildParentTx(t *testing.T, satoshis uint64, scriptByte byte) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	in := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{scriptByte, 0x02, 0x03})}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{1}))
	tx.Inputs = append(tx.Inputs, in)

	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      satoshis,
		LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, scriptByte, 0x88, 0xac}),
	})

	return tx
}

// TestGetTxFromExternalStoreRehashesAgainstKey pins issue 1439: a parent read
// from the external store is a consensus input to spend validation (its
// locking script and satoshis), so bytes that parse as *a* valid transaction
// but are not the transaction the key names must be rejected rather than
// silently validated against.
func TestGetTxFromExternalStoreRehashesAgainstKey(t *testing.T) {
	ctx := context.Background()

	parent := buildParentTx(t, 1000, 0xaa)
	other := buildParentTx(t, 999999, 0xbb)

	parentHash := *parent.TxIDChainHash()
	require.NotEqual(t, parentHash, *other.TxIDChainHash())

	newStore := func(t *testing.T) *Store {
		t.Helper()

		return &Store{logger: ulogger.TestLogger{}, externalStore: memory.New()}
	}

	t.Run("correct bytes under the key are returned", func(t *testing.T) {
		s := newStore(t)
		require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeTx, parent.ExtendedBytes()))

		got, err := s.getExternalTransaction(ctx, parentHash)
		require.NoError(t, err)
		require.Equal(t, parentHash, *got.TxIDChainHash())
		require.Equal(t, uint64(1000), got.Outputs[0].Satoshis)
	})

	t.Run("another transaction's bytes under the key are rejected as a storage fault", func(t *testing.T) {
		// A stale file from the pruner's delete-then-write race, or any
		// mis-keyed return: the stored bytes are perfectly valid, so a body
		// checksum would pass them. Only re-hashing catches it.
		s := newStore(t)
		require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeTx, other.ExtendedBytes()))

		_, err := s.getExternalTransaction(ctx, parentHash)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not hash to the key it was stored under")

		// The class matters as much as the rejection: this is OUR disk being
		// wrong, never evidence about the block or the peer. TxInvalid would be
		// read downstream as a proven consensus violation — the block persisted
		// invalid and the serving peer flagged malicious over a local fault.
		require.True(t, errors.Is(err, errors.ErrStorageError), "must be a storage fault, not a transaction fault")
		require.False(t, errors.Is(err, errors.ErrTxInvalid), "must never be classified as a consensus violation")
	})

	t.Run("a flipped byte in the locking script is rejected", func(t *testing.T) {
		// The exact consensus hazard: a rotted blob whose script still parses
		// would have the child's signature checked against the wrong spending
		// condition. Target the locking script explicitly rather than trusting
		// an offset from the end — review caught the first version of this test
		// flipping a locktime byte and claiming otherwise.
		s := newStore(t)

		good := parent.ExtendedBytes()
		script := *parent.Outputs[0].LockingScript
		idx := bytes.Index(good, script)
		require.GreaterOrEqual(t, idx, 0, "fixture must contain the locking script verbatim")

		corrupt := append([]byte{}, good...)
		corrupt[idx+2] ^= 0xff

		require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeTx, corrupt))

		_, err := s.getExternalTransaction(ctx, parentHash)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not hash to the key it was stored under")
		require.True(t, errors.Is(err, errors.ErrStorageError))
	})

	t.Run("a truncated blob is a storage fault, not a consensus violation", func(t *testing.T) {
		// The re-hash catches a stale-but-intact blob; a torn or partially written
		// one fails earlier, in tx.ReadFrom. Both are this node's disk being wrong,
		// so both must be storage faults. Classifying a truncated blob as TxInvalid
		// would persist the block invalid and flag the serving peer over a local
		// fault — and a truncated blob is the more likely corruption mode.
		s := newStore(t)

		good := parent.ExtendedBytes()
		require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeTx, good[:len(good)/2]))

		_, err := s.getExternalTransaction(ctx, parentHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "must be a storage fault, not a transaction fault")
		require.False(t, errors.Is(err, errors.ErrTxInvalid), "must never be classified as a consensus violation")
	})

	t.Run("a mismatched read is never cached", func(t *testing.T) {
		// The caller memoizes by txid, so a cached bad read would be re-served
		// to every later spend of this parent — and again after a restart,
		// since the source is a file on disk.
		s := newStore(t)
		s.externalTxCache = util.NewExpiringConcurrentCacheWithMaxSize[chainhash.Hash, *bt.Tx](10*time.Second, 128)

		require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeTx, other.ExtendedBytes()))

		_, err := s.GetTxFromExternalStore(ctx, parentHash)
		require.Error(t, err)

		// Repair the blob; the next read must return the good value, proving
		// the poisoned one was not retained.
		require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeTx, parent.ExtendedBytes(), options.WithAllowOverwrite(true)))

		got, err := s.GetTxFromExternalStore(ctx, parentHash)
		require.NoError(t, err)
		require.Equal(t, parentHash, *got.TxIDChainHash())

		// Positive control: without this, the subtest would also pass against an
		// inert or always-bypassing cache, proving nothing about retention.
		// Deleting the blob makes the store unable to serve a fresh fetch, so a
		// third successful read can only have come from the cache — which pins
		// that the cache is live AND that the earlier bad read was not in it.
		require.NoError(t, s.externalStore.Del(ctx, parentHash[:], fileformat.FileTypeTx))

		got, err = s.GetTxFromExternalStore(ctx, parentHash)
		require.NoError(t, err, "successful read must be retained by a live cache")
		require.Equal(t, parentHash, *got.TxIDChainHash())
	})
}
