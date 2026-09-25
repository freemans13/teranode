package netsync

import (
	"bytes"
	"context"
	"net/url"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobfile "github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// The download and park path streams: a block's bytes are read once and its subtree data is
// written as it goes, never assembled in memory or serialized again. On mainnet on 2026-09-24,
// 70% of 566 MB/s of allocation, and most of the 71% of CPU spent collecting it, came from
// serializing each transaction again to compute its id and from building each subtree data
// file in one buffer that doubled as it grew.

// allocatedBytes is how many bytes f allocated on the heap.
func allocatedBytes(f func()) uint64 {
	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)

	return after.TotalAlloc - before.TotalAlloc
}

// bigTx is one transaction carrying an output script of about size bytes.
func bigTx(t *testing.T, seed byte, size int) *bt.Tx {
	t.Helper()

	tx, _ := streamTx(t, int(seed))
	script := bscript.Script(append([]byte{0x00, 0x6a}, bytes.Repeat([]byte{seed}, size)...))
	tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: &script})

	return tx
}

// Reading a transaction off the stream costs one copy of it, for the parse. Its id is hashed from
// the bytes as they are read, not from a second serialization.
func TestTheStreamHashesATransactionFromTheBytesItRead(t *testing.T) {
	const size = 2 << 20

	tx := bigTx(t, 7, size)
	want := chainhash.DoubleHashH(tx.Bytes())

	var buf bytes.Buffer
	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 1))
	buf.Write(tx.Bytes())

	s, err := newBlockTxStream(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	var got *chainhash.Hash

	n := allocatedBytes(func() {
		_, got, err = s.Next()
	})
	require.NoError(t, err)
	require.Equal(t, want, *got)
	require.Less(t, n, uint64(size)*3/2, "one copy for the parse, and none to compute the id")
}

// Writing a subtree's data to the file store streams it: the allocation is a small fraction of
// the data, and the file holds exactly the bytes the whole-buffer serialization produced.
func TestTheSubtreeDataFileIsWrittenAsAStream(t *testing.T) {
	ctx := context.Background()

	// A real file store, as mainnet runs: the in-memory store reads a stream into a growing
	// slice of its own, which would measure the test store and not the writer.
	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	store, err := blobfile.New(ulogger.TestLogger{}, storeURL,
		options.WithBlobDeletionScheduler(&recordingDeletionScheduler{}),
		options.WithStoreType(storetypes.TEMPSTORE),
	)
	require.NoError(t, err)

	w := newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000, true)

	const leaves = 8
	const size = 1 << 20

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(leaves)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	data := subtreepkg.NewSubtreeData(st)
	meta := subtreepkg.NewSubtreeMeta(st)

	for i := 1; i < leaves; i++ {
		tx := bigTx(t, byte(i), size)
		require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 0, uint64(tx.Size())))
		require.NoError(t, data.AddTx(tx, st.Length()-1))
		require.NoError(t, meta.SetTxInpointsFromTx(tx))
	}

	want, err := data.Serialize()
	require.NoError(t, err)

	n := allocatedBytes(func() {
		require.NoError(t, w.Emit(ctx)(0, st, data, meta))
	})

	root := st.RootHash()
	got, err := store.Get(ctx, root[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.Equal(t, want, got, "the same bytes as the whole-buffer serialization")
	require.Less(t, n, uint64(len(want))/2, "streamed to the file, never assembled in memory")
}

// streamingWriter is a subtree writer over a real file store, which is what lets the builder
// stream each subtree's data file instead of holding its transactions.
func streamingWriter(t *testing.T) (*subtreeWriter, blob.Store) {
	t.Helper()

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	store, err := blobfile.New(ulogger.TestLogger{}, storeURL,
		options.WithBlobDeletionScheduler(&recordingDeletionScheduler{}),
		options.WithStoreType(storetypes.TEMPSTORE),
	)
	require.NoError(t, err)

	return newSubtreeWriter(ulogger.TestLogger{}, settings.NewSettings(), store, 800000, true), store
}

func heapInUse() uint64 {
	var m runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&m)

	return m.HeapInuse
}

// The builder writes each transaction to its subtree's data file as it arrives and keeps none of
// them. It used to hold every transaction of the current subtree until the subtree was full: on
// mainnet on 2026-09-24 that was 3.65 GB of parsed scripts live at once, and garbage collection
// took over two thirds of the CPU scanning it.
func TestTheBuilderHoldsNoTransactionItHasWritten(t *testing.T) {
	ctx := context.Background()
	w, _ := streamingWriter(t)
	t.Cleanup(func() { _ = w.DeleteAll(ctx) })

	const txs = 20
	const size = 1 << 20

	b, err := newBlockStreamBuilder(txs+1, 1024, coinbaseTx(t), w.Emit(ctx), newDedupMap(txs+1), withSubtreeDataSink(w.OpenData(ctx)))
	require.NoError(t, err)

	before := heapInUse()

	for i := 1; i <= txs; i++ {
		tx := bigTx(t, byte(i), size)
		require.NoError(t, b.AddTx(tx, tx.TxIDChainHash()))
	}

	grew := int64(heapInUse()) - int64(before)
	// Without this the builder is dead once the loop ends, and the collector frees whatever it
	// held before the heap is read.
	runtime.KeepAlive(b)
	require.Less(t, grew, int64(txs*size/4), "20 MB of transactions went to disk, not to the heap")
}

// The streamed data files hold exactly the bytes the buffered path writes, across several
// subtrees, for spendable and data-only transactions alike.
func TestStreamedAndBufferedSubtreeDataFilesMatch(t *testing.T) {
	ctx := context.Background()

	const txs = 40
	const maxItems = 16

	block := make([]*bt.Tx, 0, txs)
	for i := 1; i <= txs; i++ {
		if i%3 == 0 {
			block = append(block, bigTx(t, byte(i), 2048))
		} else {
			tx, _ := streamTx(t, i)
			block = append(block, tx)
		}
	}

	cb := coinbaseTx(t)

	run := func(w *subtreeWriter, opts ...builderOption) []chainhash.Hash {
		b, err := newBlockStreamBuilder(txs+1, maxItems, cb, w.Emit(ctx), newDedupMap(txs+1), opts...)
		require.NoError(t, err)

		for _, tx := range block {
			require.NoError(t, b.AddTx(tx, tx.TxIDChainHash()))
		}

		_, hashes, err := b.Finish()
		require.NoError(t, err)

		return hashes
	}

	buffered, memStore := writerFixture(t, true)
	streamed, fileStore := streamingWriter(t)

	want := run(buffered)
	got := run(streamed, withSubtreeDataSink(streamed.OpenData(ctx)))
	require.Equal(t, want, got)
	require.Greater(t, len(got), 1, "several subtrees")

	for _, root := range got {
		a, err := memStore.Get(ctx, root[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)

		b, err := fileStore.Get(ctx, root[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)

		require.Equal(t, a, b, "subtree %s", root)
	}
}

// An output that can never be spent is not remembered for extending in-block children: at these
// heights such outputs carry most of a block's bytes, and remembering them kept them live.
func TestAnUnspendableOutputIsNotRemembered(t *testing.T) {
	b, err := newBlockStreamBuilder(3, 1024, coinbaseTx(t), func(int, *subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) error { return nil }, newDedupMap(3))
	require.NoError(t, err)

	tx := bigTx(t, 9, 4096)
	require.NoError(t, b.AddTx(tx, tx.TxIDChainHash()))

	outs := b.recentOutputs[*tx.TxIDChainHash()]
	require.Len(t, outs, len(tx.Outputs), "positions are kept, so a child's output index still lines up")
	require.NotNil(t, outs[0], "the spendable output is remembered")
	require.Nil(t, outs[len(outs)-1], "the OP_FALSE OP_RETURN output is not")
}
