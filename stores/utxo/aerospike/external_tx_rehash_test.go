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
	"github.com/bsv-blockchain/teranode/services/utxopersister"
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

	t.Run("a corrupt outputs blob is a storage fault, not a consensus violation", func(t *testing.T) {
		// Reached only when no FileTypeTx blob exists, so the read falls back to
		// the outputs-only file. Same reasoning as the branch above: these bytes
		// were written from our own outputs, so bytes that no longer parse mean
		// this node's stored copy is wrong, not that the block or peer is.
		s := newStore(t)
		require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeOutputs, []byte{0x01, 0x02, 0x03}))

		_, err := s.getExternalTransaction(ctx, parentHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "must be a storage fault, not a transaction fault")
		require.False(t, errors.Is(err, errors.ErrTxInvalid), "must never be classified as a consensus violation")
	})

	t.Run("an outputs blob declaring another txid is a storage fault", func(t *testing.T) {
		// The outputs blob cannot be re-hashed, but it does carry a self-declared
		// txid written from the key it is stored under. A stale file from the
		// pruner's delete-then-write race passes every byte-level check and would
		// otherwise surface downstream as "previous tx has no output at index N" —
		// a consensus violation manufactured from a local fault. Weaker than the
		// re-hash (a matching txid does not vouch for the outputs), but it catches
		// the mis-keyed and stale cases for free.
		s := newStore(t)

		wrapper := &utxopersister.UTXOWrapper{
			TxID:   *other.TxIDChainHash(),
			Height: 1,
			UTXOs: []*utxopersister.UTXO{{
				Index:  0,
				Value:  999999,
				Script: *other.Outputs[0].LockingScript,
			}},
		}
		require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeOutputs, wrapper.Bytes()))

		_, err := s.getExternalTransaction(ctx, parentHash)
		require.Error(t, err)
		require.Contains(t, err.Error(), "declares a different txid")
		require.True(t, errors.Is(err, errors.ErrStorageError), "must be a storage fault, not a transaction fault")
		require.False(t, errors.Is(err, errors.ErrTxInvalid), "must never be classified as a consensus violation")
	})

	t.Run("a legitimate outputs-only blob is still readable and deliberately not re-hashed", func(t *testing.T) {
		// Pins the carve-out. This path reconstructs outputs only — no inputs, no
		// version, no locktime — so the result cannot hash back to the key it was
		// fetched under. Extending the re-hash to cover it would therefore reject
		// perfectly good reads, which is exactly why it is left unguarded. The
		// self-declared-txid check above is the weaker substitute, and must not
		// reject a legitimate read like this one.
		s := newStore(t)

		wrapper := &utxopersister.UTXOWrapper{
			TxID:   parentHash,
			Height: 1,
			UTXOs: []*utxopersister.UTXO{{
				Index:  0,
				Value:  1000,
				Script: *parent.Outputs[0].LockingScript,
			}},
		}
		require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeOutputs, wrapper.Bytes()))

		got, err := s.getExternalTransaction(ctx, parentHash)
		require.NoError(t, err, "outputs-only reads must keep working")
		require.Equal(t, uint64(1000), got.Outputs[0].Satoshis)
		require.Equal(t, *parent.Outputs[0].LockingScript, *got.Outputs[0].LockingScript)
		require.NotEqual(t, parentHash, *got.TxIDChainHash(), "cannot be re-hashed: this is why the branch is not guarded")
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

// TestGetTxInpointsFromExternalStoreClassifiesParseFailureAsStorage covers the
// sibling read path in this file, which had the same misclassification the
// re-hash change fixed above: a blob that fails to parse was reported as
// ErrTxInvalid, i.e. as a proven consensus violation, even though the bytes were
// written from our own transaction and can only be wrong because this node's
// disk is.
//
// The wrap at the BatchDecorate call site matters as much as the class here.
// errors.Is walks the whole chain and every downstream switch tests ErrTxInvalid
// first, so re-wrapping a storage fault in TxInvalid there would launder it
// straight back into a consensus violation. That call site needs an Aerospike
// batch to reach, so it is covered by the classification at this level plus the
// explicit branch in BatchDecorate rather than by a hermetic test.
func TestGetTxInpointsFromExternalStoreClassifiesParseFailureAsStorage(t *testing.T) {
	ctx := context.Background()

	parent := buildParentTx(t, 1000, 0xaa)
	parentHash := *parent.TxIDChainHash()

	t.Run("a well-formed blob still parses", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}, externalStore: memory.New()}
		require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeTx, parent.ExtendedBytes()))

		inpoints, err := s.GetTxInpointsFromExternalStore(ctx, parentHash)
		require.NoError(t, err)
		require.Len(t, inpoints.ParentTxHashes, 1)
	})

	t.Run("a truncated blob is a storage fault, not a consensus violation", func(t *testing.T) {
		s := &Store{logger: ulogger.TestLogger{}, externalStore: memory.New()}

		good := parent.ExtendedBytes()
		require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeTx, good[:len(good)/2]))

		_, err := s.GetTxInpointsFromExternalStore(ctx, parentHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "must be a storage fault, not a transaction fault")
		require.False(t, errors.Is(err, errors.ErrTxInvalid), "must never be classified as a consensus violation")
	})

	t.Run("a missing blob is a storage fault", func(t *testing.T) {
		// Pre-existing behaviour, pinned so the two failure modes of this function
		// stay classified alike.
		s := &Store{logger: ulogger.TestLogger{}, externalStore: memory.New()}

		_, err := s.GetTxInpointsFromExternalStore(ctx, parentHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError))
		require.False(t, errors.Is(err, errors.ErrTxInvalid))
	})
}
