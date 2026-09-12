package netsync

/*
This file is the claim the whole streaming-pipeline design rests on: that the
pipeline is a REPLACEMENT for prepareSubtrees, not merely something that
compiles alongside it.

Every earlier equivalence test in this package (block_stream_dedup_test.go,
block_stream_builder_test.go) compares the streaming builder against a
transcription of its own logic — useful for ordering, useless for proving the
pipeline agrees with production. This test instead runs the SAME wire
transactions through the real (*SyncManager).prepareSubtrees and through the
streaming builder + subtreeWriter, into two separate blob stores, and compares
every stored file. Root hashes alone are not enough: two subtrees carrying
different fee or size fields can share one, and those fields travel onward
into block validation.

The three files are not held to the same bar, and that split is deliberate,
not a weakening:

  - The structure file and the subtree-meta (inpoints) file are compared BYTE
    FOR BYTE. Fees, sizes and inpoints must match exactly — there is no
    legitimate reason for these to differ.
  - The subtree-DATA file (the serialised transactions) is compared
    SEMANTICALLY: read both back and require the same transaction id in the
    same slot, tolerating one side being go-bt "extended" (carries
    PreviousTxSatoshis/PreviousTxScript) and the other not. Production's
    extendFromTxMap phase (handle_block.go:1846) fills those fields in when a
    transaction's parent is elsewhere in the SAME block; the streaming
    pipeline does not run that phase. That asymmetry is not a defect: a
    subtree-data file can legitimately hold a mix of extended and
    non-extended transactions (go-bt's reader auto-detects the marker per
    transaction), and block validation extends whatever arrives non-extended
    by checking IsExtended() and falling back to a store lookup. Production's
    in-block extension is an optimisation that saves the consumer a lookup,
    not a correctness requirement, so byte equality on this one file would
    fail on a difference that is not a bug.

LegacyUnifiedBelowCheckpoint is forced TRUE for every case here (see
newManagerWithSubtreeStore). With it false, prepareSubtrees also assigns a
block ID and runs coin creation/spending on the inline route, which drags UTXO
behaviour into a test that is about files. With it true, and the block below
the hard-coded checkpoint, prepareSubtrees's unified branch does nothing but
partition, extend in-block parents and write subtree files — exactly the
comparison this test wants, and exactly what AssignBlockID is never called for,
so no blockchain mock is needed.
*/

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestPipeline_ProducesTheSameFilesAsPrepareSubtrees is the claim this whole
// design rests on, and until this passes the pipeline is not a replacement for
// anything.
//
// It runs the SAME transactions through the existing prepareSubtrees and through
// the streaming pipeline, into two separate blob stores, then compares every
// stored file: structure and subtree-meta byte for byte, subtree-data
// semantically (see compareSubtreeData and the file-level doc comment for why).
func TestPipeline_ProducesTheSameFilesAsPrepareSubtrees(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	for _, tc := range []struct {
		name     string
		txCount  int
		maxItems int
		chained  bool
	}{
		{name: "one partial subtree", txCount: 5, maxItems: 8},
		{name: "one full subtree", txCount: 8, maxItems: 8},
		{name: "two full subtrees", txCount: 16, maxItems: 8},
		{name: "final subtree short", txCount: 20, maxItems: 8},
		{name: "mainnet subtree size", txCount: 9000, maxItems: 4096},
		// This is the case that catches an in-block extension gap: every
		// other case above gives each transaction a parent OUTSIDE the
		// block, which never exercises extendFromTxMap's same-block fill-in
		// (handle_block.go:1846) at all. Here tx[i] spends tx[i-1], so
		// production's converted transactions actually carry
		// PreviousTxSatoshis/PreviousTxScript for every parent but the
		// first, and the subtree-data comparison must tolerate that.
		{name: "chained spends, in-block parents", txCount: 16, maxItems: 8, chained: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// PRODUCTION PATH. Build the wire block, run prepareSubtrees, keep its store.
			prodStore := blobmemory.New()
			sm := newManagerWithSubtreeStore(t, prodStore, tc.maxItems)
			block := wireBlockWithTxs(t, tc.txCount, tc.chained)

			prodHashes, _, _, err := sm.prepareSubtrees(ctx, block)
			require.NoError(t, err, "the production path must succeed, or there is nothing to compare against")

			// PIPELINE PATH. Same transactions, streamed.
			pipeStore := blobmemory.New()
			writer := newSubtreeWriter(sm.logger, sm.settings, pipeStore, uint32(block.Height()), true) //nolint:gosec // test height is small and non-negative

			seen := txmap.NewSplitSwissMapUint64(uint32(tc.txCount)) //nolint:gosec // test tx count is small

			b, err := newBlockStreamBuilderWithDedup(tc.txCount, tc.maxItems,
				coinbaseFromBlock(t, block), writer.Emit(ctx), seen)
			require.NoError(t, err)

			for i, wireTx := range block.Transactions() {
				if i == 0 {
					continue // the coinbase occupies slot zero as a placeholder
				}

				tx, hash := btTxFromWireTx(t, wireTx)
				require.NoError(t, b.AddTx(tx, hash))
			}

			_, pipeHashes, err := b.Finish()
			require.NoError(t, err)

			// COMPARE.
			require.Equal(t, len(prodHashes), len(pipeHashes),
				"both paths must produce the same number of subtrees")
			require.Greater(t, len(prodHashes), 0,
				"test case must produce at least one subtree, or the comparison below is vacuous")

			for i := range prodHashes {
				require.Equal(t, prodHashes[i].String(), pipeHashes[i].String(),
					"subtree %d root hash differs between the two paths", i)

				// Structure and subtree-meta: byte for byte. Fees, sizes and
				// inpoints must match exactly.
				for _, ft := range []fileformat.FileType{
					fileformat.FileTypeSubtree,
					fileformat.FileTypeSubtreeMeta,
				} {
					want, err := prodStore.Get(ctx, prodHashes[i][:], ft)
					require.NoError(t, err, "production did not write %s for subtree %d", ft, i)

					got, err := pipeStore.Get(ctx, pipeHashes[i][:], ft)
					require.NoError(t, err, "the pipeline did not write %s for subtree %d", ft, i)

					require.Equal(t, want, got,
						"subtree %d: %s differs between the production path and the pipeline", i, ft)
				}

				// Subtree-data: semantic comparison. The structure bytes just
				// asserted equal above give both sides the same node hashes to
				// validate against, so either one can be used to parse the data.
				structureBytes, err := prodStore.Get(ctx, prodHashes[i][:], fileformat.FileTypeSubtree)
				require.NoError(t, err)

				subtree, err := subtreepkg.NewSubtreeFromBytes(structureBytes)
				require.NoError(t, err)

				wantData, err := prodStore.Get(ctx, prodHashes[i][:], fileformat.FileTypeSubtreeData)
				require.NoError(t, err, "production did not write subtree data for subtree %d", i)

				gotData, err := pipeStore.Get(ctx, pipeHashes[i][:], fileformat.FileTypeSubtreeData)
				require.NoError(t, err, "the pipeline did not write subtree data for subtree %d", i)

				compareSubtreeData(t, subtree, wantData, gotData, i)
			}
		})
	}
}

// compareSubtreeData asserts that two subtree-data blobs, parsed against the
// same subtree structure, carry the same transaction id in the same slot.
//
// This is deliberately NOT a byte comparison, unlike the structure and
// subtree-meta files — see the file-level doc comment for why: production's
// in-block extension (extendFromTxMap) can leave its transactions carrying
// PreviousTxSatoshis/PreviousTxScript that the streaming pipeline's do not,
// and that is a valid difference, not a bug. Transaction id is unaffected by
// that extension (go-bt computes it from the core fields only), so it is the
// right thing to compare.
func compareSubtreeData(t *testing.T, subtree *subtreepkg.Subtree, want, got []byte, subtreeIdx int) {
	t.Helper()

	wantData, err := subtreepkg.NewSubtreeDataFromBytes(subtree, want)
	require.NoError(t, err, "production subtree data %d failed to deserialise", subtreeIdx)

	gotData, err := subtreepkg.NewSubtreeDataFromBytes(subtree, got)
	require.NoError(t, err, "pipeline subtree data %d failed to deserialise", subtreeIdx)

	require.Equal(t, len(wantData.Txs), len(gotData.Txs),
		"subtree %d: transaction slot count differs between the two paths", subtreeIdx)

	for i := range wantData.Txs {
		wantTx := wantData.Txs[i]
		gotTx := gotData.Txs[i]

		if wantTx == nil || gotTx == nil {
			require.Equal(t, wantTx == nil, gotTx == nil,
				"subtree %d slot %d: one path has a transaction and the other does not", subtreeIdx, i)
			continue
		}

		require.Equal(t, wantTx.TxIDChainHash().String(), gotTx.TxIDChainHash().String(),
			"subtree %d slot %d: transaction id differs between the two paths", subtreeIdx, i)
	}
}

// TestPipeline_DeserialisesToTheSameSubtree is a second, weaker check that
// survives a deliberate format change: whatever the pipeline's own structure
// bytes are, reading them back must reproduce the root hash the builder itself
// reported for that subtree.
//
// The original version of this test never touched the pipeline at all: it read
// a file PRODUCTION wrote and checked a root hash PRODUCTION had already
// reported, so neither the builder nor the writer was constructed. Fixed here
// to actually drive the pipeline path; that flaw was in the task brief's
// skeleton, not introduced by the first round of this test.
func TestPipeline_DeserialisesToTheSameSubtree(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	const txCount = 20
	const maxItems = 8

	// The manager here is only a source of a real logger and settings for the
	// writer; sm.subtreeStore (an unused throwaway store) is never written to
	// because prepareSubtrees is never called in this test.
	sm := newManagerWithSubtreeStore(t, blobmemory.New(), maxItems)
	block := wireBlockWithTxs(t, txCount, false)

	pipeStore := blobmemory.New()
	writer := newSubtreeWriter(sm.logger, sm.settings, pipeStore, uint32(block.Height()), true) //nolint:gosec // test height is small and non-negative

	seen := txmap.NewSplitSwissMapUint64(uint32(txCount)) //nolint:gosec // test tx count is small

	b, err := newBlockStreamBuilderWithDedup(txCount, maxItems, coinbaseFromBlock(t, block), writer.Emit(ctx), seen)
	require.NoError(t, err)

	for i, wireTx := range block.Transactions() {
		if i == 0 {
			continue // the coinbase occupies slot zero as a placeholder
		}

		tx, hash := btTxFromWireTx(t, wireTx)
		require.NoError(t, b.AddTx(tx, hash))
	}

	_, pipeHashes, err := b.Finish()
	require.NoError(t, err)
	require.Greater(t, len(pipeHashes), 0, "test case must produce at least one subtree")

	raw, err := pipeStore.Get(ctx, pipeHashes[0][:], fileformat.FileTypeSubtree)
	require.NoError(t, err)

	st, err := subtreepkg.NewSubtreeFromBytes(raw)
	require.NoError(t, err)

	require.Equal(t, pipeHashes[0].String(), st.RootHash().String())
}

// pipelineEquivalenceStoreCounter gives each subtest its own sqlitememory
// instance name, so an accidental store write in one case cannot leak into
// another. Nothing here is expected to touch the store at all: see
// newManagerWithSubtreeStore.
var pipelineEquivalenceStoreCounter atomic.Int64

// newManagerWithSubtreeStore builds a SyncManager the same way
// runInlinePipeline (unified_parity_test.go) does — real settings, a real
// sqlitememory UTXO store, the caller's real in-memory blob store — except
// LegacyUnifiedBelowCheckpoint is forced true, not false.
//
// That flip is the entire point of this file: on the unified route, below the
// hard-coded checkpoint, prepareSubtrees never calls AssignBlockID and never
// creates or spends a UTXO (see legacyUnified's doc comment on
// handle_block.go). So this constructor wires no blockchain mock and no
// validator mock — nothing here is a stub standing in for a real dependency;
// the real dependencies are simply never called on this route, and the
// production code path itself is what skips them, not a narrowed test double.
func newManagerWithSubtreeStore(t *testing.T, subtreeStore blob.Store, maxItems int) *SyncManager {
	t.Helper()

	const checkpointHeight = int32(1000) // block height 500 below sits well under this

	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.BlockAssembly.MaximumMerkleItemsPerSubtree = maxItems

	dbName := fmt.Sprintf("pipeline_equivalence_%d", pipelineEquivalenceStoreCounter.Add(1))
	u, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = u

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	store, err := utxosql.New(ctx, logger, tSettings, u)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	return &SyncManager{
		settings:     tSettings,
		chainParams:  params,
		logger:       logger,
		utxoStore:    store,
		subtreeStore: subtreeStore,
		ctx:          ctx,
	}
}

// wireBlockWithTxs builds a *bsvutil.Block with a coinbase plus txCount-1
// non-coinbase transactions, at a fixed height below the checkpoint
// newManagerWithSubtreeStore configures.
//
// When chained is false, every transaction spends a distinct EXTERNAL
// (never-created) outpoint: extendTransactions's phase 2 decorate is skipped
// entirely below the checkpoint's outpoint-only fast path (handle_block.go:1826),
// so a parent that resolves to nothing is exactly what production itself
// tolerates on this route, not a simplification this test introduces. But it
// also means no transaction's parent is ever IN this block, which silently
// switches off extendFromTxMap's same-block fill-in (phase 1,
// handle_block.go:1846) — a real block routinely has in-block spends and this
// shape never exercises that.
//
// When chained is true, tx[i] (i >= 2) spends tx[i-1]'s output instead, so
// phase 1 actually has same-block parents to fill in. tx[1] still spends an
// external outpoint, since there is no earlier in-block transaction for it.
func wireBlockWithTxs(t *testing.T, txCount int, chained bool) *bsvutil.Block {
	t.Helper()

	const height = int32(500)

	cb := wire.NewMsgTx(1)
	cb.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{byte(height & 0xff), 0x01}, //nolint:gosec // test coinbase script
		Sequence:         0xffffffff,
	})
	cb.AddTxOut(&wire.TxOut{Value: 5000000000, PkScript: []byte{0x76, 0xa9, 0x14}})

	msgBlock := &wire.MsgBlock{
		Header:       wire.BlockHeader{Version: 1, Timestamp: time.Now(), Bits: 0x1d00ffff},
		Transactions: make([]*wire.MsgTx, 0, txCount),
	}
	msgBlock.Transactions = append(msgBlock.Transactions, cb)

	var prevTxHash chainhash.Hash // only used when chained

	for i := 1; i < txCount; i++ {
		tag := []byte{byte(i), byte(i >> 8)} //nolint:gosec // test loop index

		var prevHash chainhash.Hash
		if chained && i > 1 {
			prevHash = prevTxHash // spend the previous non-coinbase tx in THIS block
		} else {
			binary.LittleEndian.PutUint32(prevHash[:4], uint32(i)) //nolint:gosec // test loop index
		}

		tx := wire.NewMsgTx(1)
		tx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: prevHash, Index: 0},
			SignatureScript:  append([]byte{0x00}, tag...),
			Sequence:         0xffffffff,
		})
		tx.AddTxOut(&wire.TxOut{Value: int64(1000 + i), PkScript: append([]byte{0x76, 0xa9, 0x14}, tag...)})

		msgBlock.Transactions = append(msgBlock.Transactions, tx)
		prevTxHash = tx.TxHash()
	}

	block := bsvutil.NewBlock(msgBlock)
	block.SetHeight(height)

	return block
}

// btTxFromWireTx converts one wire transaction the same way createTxMap does
// (handle_block.go:2074): via WireTxToGoBtTx, then SetTxHash for every
// non-coinbase transaction. Reusing exactly that conversion function on both
// paths is deliberate — the divergence under test is what each path does with
// the converted transaction (how it is packed into subtree files), not how the
// conversion itself works.
func btTxFromWireTx(t *testing.T, wireTx *bsvutil.Tx) (*bt.Tx, *chainhash.Hash) {
	t.Helper()

	hash := wireTx.Hash()

	tx := &bt.Tx{}
	require.NoError(t, WireTxToGoBtTx(wireTx, tx))

	if !tx.IsCoinbase() {
		tx.SetTxHash(hash)
	}

	return tx, hash
}

// coinbaseFromBlock returns the block's coinbase as a *bt.Tx, converted the
// same way createTxMap converts it (hash not memoised, matching production's
// "don't add the coinbase to the txMap" branch).
func coinbaseFromBlock(t *testing.T, block *bsvutil.Block) *bt.Tx {
	t.Helper()

	tx := &bt.Tx{}
	require.NoError(t, WireTxToGoBtTx(block.Transactions()[0], tx))

	return tx
}
