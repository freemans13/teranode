package netsync

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// headerCacheParent points sm's header cache at one synthetic parent hash, at
// height 0, and returns that hash. It is enough for parentIsReachable to
// answer true without ever consulting the blockchain client — the cache is
// checked first and short-circuits — which is what keeps these tests from
// needing to wire a consumer goroutine or a parkCommits channel: the block is
// reachable (worth charging) but never committed (never reaches
// submitParkCommit).
//
// Written directly into the cache's own maps rather than through Fill, which
// demands a batch that actually chains together: this needs one specific,
// arbitrary hash to be found there, not a real header run.
func headerCacheParent(t *testing.T, sm *SyncManager, name string) chainhash.Hash {
	t.Helper()

	parent := chainhash.HashH([]byte(name))

	sm.headerCache = newHeaderCache()
	sm.headerCache.byHeight[0] = parent
	sm.headerCache.byHash[parent] = 0
	sm.headerCache.filled = true

	return parent
}

// TestHandleBlockOnDiskMsg_ChargesTheRecordOnlyWhenConverted is the direct test
// for fix-round item 5: the honest-size change in handleBlockOnDiskMsg had no
// test of its own. Converted true must charge the converted record's own
// length; Converted false (the pipeline off, or the sink's own fallback) must
// charge exactly the declared block size, unchanged from before this task.
func TestHandleBlockOnDiskMsg_ChargesTheRecordOnlyWhenConverted(t *testing.T) {
	t.Run("converted charges the record's own length", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sm := newPipelineParkManager(t, store, 8)

		blk := wireBlockWithTxs(t, 20, false)
		pipelineHeaderFixture(t, sm, blk)
		header := &blk.MsgBlock().Header
		body := blockBodyBytes(t, blk)

		converted, err := sm.pipelineBlockSink(*blk.Hash(), header, bytes.NewReader(body), int64(len(body)))
		require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
		require.True(t, converted, "sanity: this test needs a real converted record on disk")

		raw, err := sm.blockPark.store.Get(ctx, blk.Hash()[:], fileformat.FileTypeBlock)
		require.NoError(t, err, "sanity: the converted record must actually be readable back")
		require.NotZero(t, len(raw), "sanity: an empty record proves nothing about the charge")

		parent := headerCacheParent(t, sm, "handle-block-on-disk-honest-size-converted-parent")

		msg := &blockOnDiskMsg{body: peerpkg.BlockBody{
			Header: wire.BlockHeader{PrevBlock: parent},
			Hash:   *blk.Hash(),
			// A size the tiny record could never be mistaken for, so a charge
			// at the wrong number is unmistakable rather than accidentally
			// close.
			Size:      1 << 30,
			Converted: true,
		}}

		sm.handleBlockOnDiskMsg(msg)

		require.Equal(t, int64(len(raw)), sm.blockPark.Bytes(),
			"a converted delivery must be charged the record's own length, not the declared block size")
	})

	t.Run("unconverted charges the declared block size", func(t *testing.T) {
		store := memory.New()
		sm := newPipelineParkManager(t, store, 8)

		parent := headerCacheParent(t, sm, "handle-block-on-disk-honest-size-unconverted-parent")
		hash := chainhash.HashH([]byte("handle-block-on-disk-honest-size-unconverted-hash"))

		msg := &blockOnDiskMsg{body: peerpkg.BlockBody{
			Header:    wire.BlockHeader{PrevBlock: parent},
			Hash:      hash,
			Size:      4096,
			Converted: false,
		}}

		sm.handleBlockOnDiskMsg(msg)

		require.Equal(t, int64(4096), sm.blockPark.Bytes(),
			"an unconverted delivery must be charged exactly the declared block size, unchanged from before this task")
	})
}

// TestHandleBlockOnDiskMsg_ChecksOnceForARecordWhenNotConverted bounds the
// store I/O an unconverted delivery costs on the goroutine that commits blocks.
// Fix-round item 1 removed a converted-record lookup that ran for EVERY streamed
// block. Every block on this path now converts, so an unconverted delivery is
// only a raw fallback copy: a duplicate of a block another peer is converting,
// or an admission that timed out. Those check once whether the block is already
// converted, because taking a raw copy of a converted block into the park is what
// stopped mainnet at 707,177 on 2026-09-24. Asserted on the store's own call
// counters, so a second lookup creeping in is caught.
func TestHandleBlockOnDiskMsg_ChecksOnceForARecordWhenNotConverted(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	parent := headerCacheParent(t, sm, "handle-block-on-disk-no-io-parent")
	hash := chainhash.HashH([]byte("handle-block-on-disk-no-io-hash"))

	before := store.Counters["exists"] + store.Counters["get"]

	msg := &blockOnDiskMsg{body: peerpkg.BlockBody{
		Header:    wire.BlockHeader{PrevBlock: parent},
		Hash:      hash,
		Size:      4096,
		Converted: false,
	}}

	sm.handleBlockOnDiskMsg(msg)

	after := store.Counters["exists"] + store.Counters["get"]
	require.Equal(t, before+1, after,
		"an unconverted delivery checks for a converted record once, and no more")
}
