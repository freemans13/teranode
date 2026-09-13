package netsync

import (
	"container/list"
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// propertyDepth is the read-ahead depth these tests run with.
//
// It is deliberately smaller than every other bound a pass is subject to — the
// per-peer in-flight cap, the block-size ladder's 20, and the node-wide
// legacy_blockDownloadWindow of 1024 — so that when an assertion holds, the
// wanted range is the only thing that can be holding it up. A depth equal to the
// per-peer cap, which is what assignHarness runs with, would leave two candidate
// explanations for every pass that stopped where it did.
const propertyDepth = 4

// propertyPasses is how many times each test drives fetchHeaderBlocks. The
// number matters: on 2026-09-12 the runaway was not visible in one pass, it was
// a ratchet that lifted the ceiling a little on every arrival, so a property
// about read-ahead has to be measured over sustained passes rather than one.
const propertyPasses = 40

// heightOfRequested resolves a requested hash back to the height the header list
// gave it, so a test can assert on POSITION rather than on a count. Counting is
// what failed twice on 2026-09-12: a block 5000 ahead and a block 1 ahead count
// the same.
//
// It takes headerMu itself, so it must be called with that lock released. A
// header already removed from the list — which is what an arrival does — is
// reported as not found rather than as height zero, because zero is a real
// height in these harnesses.
func heightOfRequested(t *testing.T, sm *SyncManager, hash chainhash.Hash) (int32, bool) {
	t.Helper()

	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	element := sm.headerIndex[hash]
	if element == nil {
		return 0, false
	}

	node, ok := element.Value.(*headerNode)
	if !ok || node == nil {
		return 0, false
	}

	return node.height, true
}

// runPass drives one wanted-range pass and returns the hashes it asked for.
//
// How many it asked for is known synchronously: requestBlocks records a block in
// the download ledger before the getdata is built, so the ledger's growth over
// the call is exactly the number of hashes now on their way to the peer. The
// send itself is asynchronous, so the recorder is then waited on for that exact
// number rather than for "something", and a pass that placed nothing is not
// waited on at all. Waiting a fixed period on every pass instead would add two
// seconds to each of the passes that correctly ask for nothing, and would accept
// a short pass as a complete one.
func runPass(t *testing.T, sm *SyncManager, rec *getDataRecorder, seen int, pass int) []chainhash.Hash {
	t.Helper()

	owed := sm.blockDownloads.Len()

	sm.fetchHeaderBlocks()

	placed := sm.blockDownloads.Len() - owed
	if placed <= 0 {
		return nil
	}

	require.True(t, WaitUntil(func() bool { return rec.count() >= seen+placed }, 5*time.Second),
		"pass %d recorded %d requests in the download ledger but only %d of them reached the peer",
		pass, placed, rec.count()-seen)

	return rec.all()[seen:]
}

// TestWantedRange_TheDownloaderCannotOutrunTheCommitter is the whole of
// 2026-09-12 in one assertion. With the committer stopped, two attempts to bound
// this by COUNTING outstanding blocks failed, because a block 5000 ahead and a
// block 1 ahead count the same. Bounding by position makes it arithmetic: at
// most depth blocks exist above the best block processed, so at most depth can
// ever be outstanding.
//
// The committer is set once and then frozen, which is exactly the state mainnet
// was in: blocks were arriving at full speed and the chain was settled at 868
// with the download front at 4877. What let the front run was that every ARRIVAL
// raised the bound. So this drives arrivals — the ledger entry is released the
// way handleBlockMsg releases it when a block lands — and requires that
// releasing them buys the downloader no ground at all.
//
// Two assertions, and the second is the one that matters. The ledger's length is
// the symptom; the height ceiling is the property. A design that bounded only
// the count could satisfy the first while asking for height 5000, and that is
// the design this replaces.
func TestWantedRange_TheDownloaderCannotOutrunTheCommitter(t *testing.T) {
	// Headers run a long way past anything the node may ask for, so a short
	// header list cannot be what stops the pass.
	sm := assignManager(t, 1, 400)
	sm.settings.Legacy.WantedRangeDownload = true
	sm.settings.Legacy.BlockDownloadLowerWindow = propertyDepth

	// Every other bound lifted clear of the depth. The ladder caps this at 20
	// whatever is asked for, and 20 is five times the depth, which is the point:
	// a pass that stops at four blocks stopped because of the range.
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 20

	_, rec := schedulerPeer(t, sm, 1, 5000)

	require.Greater(t, schedulerPeerBudget(sm), propertyDepth,
		"the peer's own budget must exceed the depth or it, and not the wanted range, is what bounds a pass")

	// The committer's position, frozen for the whole test. Nothing in this test
	// ever writes it again.
	const best = int32(100)

	sm.lastCommittedHeight.Store(best)

	seen := 0
	highest := int32(0)

	for pass := 0; pass < propertyPasses; pass++ {
		fresh := runPass(t, sm, rec, seen, pass)
		seen += len(fresh)

		require.LessOrEqual(t, sm.blockDownloads.Len(), propertyDepth,
			"pass %d left more blocks outstanding than the read-ahead depth allows", pass)

		for _, hash := range fresh {
			height, found := heightOfRequested(t, sm, hash)
			require.True(t, found, "pass %d asked for a hash the header list cannot name", pass)

			require.Greater(t, height, best,
				"pass %d asked for a block at or below the committed block", pass)
			require.LessOrEqual(t, height, best+propertyDepth,
				"pass %d asked for height %d, which is more than %d blocks above the frozen committer at %d",
				pass, height, propertyDepth, best)

			if height > highest {
				highest = height
			}

			// The arrival. handleBlockMsg discharges the obligation with exactly
			// this call when a block lands, and discharging it is the only thing
			// that lets the next pass ask for anything at all. Under the design
			// this replaces it was also what lifted the ceiling: the header list
			// front advanced on arrival and the ceiling followed the front.
			//
			// This is the ledger's own method, called with the owner the ledger
			// itself recorded, and it is the whole of what an arrival does to the
			// ledger. It is not a stand-in for handleBlockMsg, which does a great
			// deal more; the third test drives that path in full.
			for _, owner := range sm.blockDownloads.OwnersOf(hash) {
				sm.blockDownloads.RemoveOwner(owner, hash)
			}
		}
	}

	// Without this the loop above could be vacuously true — forty passes that
	// each asked for nothing would satisfy every assertion in it.
	require.Greater(t, seen, propertyDepth,
		"the passes must have kept asking for blocks, or the bound above was never tested")
	require.Equal(t, best+propertyDepth, highest,
		"the passes should have reached the top of the window and stopped there")
}

// TestWantedRange_ARestartingNodeRequestsOnItsFirstPass pins the case the old
// design could not serve. Before this, lastCommittedHeight was written only when
// a block committed and was never seeded, so it read zero on a node restarting
// mid-chain, and the wanted range derived from zero names heights no restarted
// node's header cache holds.
//
// The counter is set here the way New sets it, rather than asserted to be zero:
// zero is the bug, not the state under test.
func TestWantedRange_ARestartingNodeRequestsOnItsFirstPass(t *testing.T) {
	// A node a long way up the chain, with headers for the run it is about to
	// fetch, at the heights they really have.
	const restartHeight = int32(800_000)

	sm := assignManager(t, restartHeight, restartHeight+200)
	sm.settings.Legacy.WantedRangeDownload = true
	sm.settings.Legacy.BlockDownloadLowerWindow = propertyDepth

	// What New now does at startup: read the chain's tip and record it. Without
	// this line the counter is zero and the pass asks for height 1.
	sm.lastCommittedHeight.Store(restartHeight)

	_, rec := schedulerPeer(t, sm, 1, restartHeight+1000)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"a node restarting mid-chain must ask for blocks on its first pass")
}

// TestNew_SeedsTheCommittedHeightFromTheChain is the other half, and it is the
// one that actually pins the production change: the counter must be non-zero
// before any block has committed in THIS process.
func TestNew_SeedsTheCommittedHeightFromTheChain(t *testing.T) {
	const chainHeight = uint32(800_000)

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	client.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: chainHeight}, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client

	// The production seeding, isolated so this test does not need the whole of
	// New's twelve dependencies.
	require.NoError(t, sm.seedCommittedHeight(context.Background()))

	require.Equal(t, int32(chainHeight), sm.lastCommittedHeight.Load(),
		"a node that starts with a chain at 800,000 must not believe its best block is 0")
}

// TestNew_WiresSeedCommittedHeightThroughTheConstructor closes the coverage gap
// TestNew_SeedsTheCommittedHeightFromTheChain leaves open: that test pins what
// seedCommittedHeight does, but calls it directly, so it cannot notice if New
// stopped calling it. Without this test, nothing runs New against a non-zero
// chain height and asserts on the counter afterwards, so a regression that
// deleted or reimplemented the call inline in New would leave the whole
// package green.
func TestNew_WiresSeedCommittedHeightThroughTheConstructor(t *testing.T) {
	const chainHeight = uint32(800_000)

	// Cancellable rather than context.Background(): New starts a goroutine
	// (startKafkaListeners) that polls sm.blockchainClient.IsFSMCurrentState
	// once a second for as long as ctx is alive, and this test's mock has
	// nothing else it needs to keep answering once the test is done.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: chainHeight}, nil)
	// startKafkaListeners' ticker goroutine calls this every second regardless
	// of what this test is pinning; without a stub the mock panics the whole
	// test binary the first time it fires.
	client.Mock.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(false, nil)

	config := &Config{
		ChainParams: &chaincfg.MainNetParams,
		// Checkpoints are irrelevant to this test and pull in a real header walk
		// against bestHeader, which is a bare stand-in and not a chain the
		// checkpoint tables know about.
		DisableCheckpoints: true,
	}

	sm, err := New(
		ctx,
		ulogger.TestLogger{},
		&settings.Settings{},
		client,
		&validator.MockValidator{},
		&utxo.MockUtxostore{},
		blob_memory.New(),
		nil,
		&subtreevalidation.MockSubtreeValidation{},
		&blockvalidation.MockBlockValidation{},
		blockassembly.NewMock(),
		config,
	)
	require.NoError(t, err)

	require.Equal(t, int32(chainHeight), sm.lastCommittedHeight.Load(),
		"New must seed the committed height from the chain, not leave callers to notice it never did")
}

// newParkPropertyManager builds a manager the wanted-range pass and the block
// queue's own delivery path can both run against: a real park over a real file
// blob store, a header list naming every one of the mined blocks at its true
// height, and a blockchain that holds nothing.
//
// Holding nothing is the point. Every parent lookup fails, so every block that
// arrives is an orphan and parks, and nothing ever commits — which is the
// committer stopped, the condition the park bound has to survive. It is also
// what mainnet looked like on 2026-09-12 during the genesis resync: blocks
// arriving at full speed with the chain settled far below them.
//
// Modelled on newParkWiringHarness, which mines three blocks; this needs enough
// of them for a runaway to have somewhere to run to.
func newParkPropertyManager(t *testing.T, blocks []*bsvutil.Block) (*SyncManager, *getDataRecorder) {
	t.Helper()

	// The real constructor registers these; a struct-literal manager reaches the
	// same gauges on the commit path.
	initPrometheusMetrics()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	client.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 0}, nil)
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{{}}, nil)
	client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	client.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewBlockNotFoundError("no such block"))

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	store, err := blob.NewStore(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.TempStore = storeURL
	tSettings.Legacy.ParkOutOfOrderBlocks = true
	tSettings.Legacy.WantedRangeDownload = true
	tSettings.Legacy.BlockDownloadLowerWindow = propertyDepth

	// As in the first test: every other bound lifted clear of the depth, so the
	// wanted range is the only candidate explanation for where a pass stops.
	tSettings.Legacy.MaxBlocksInTransitPerPeer = 20

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.settings = tSettings
	sm.blockchainClient = client
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](100)
	sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](time.Minute)
	sm.blockPark = newBlockPark(ulogger.TestLogger{}, tSettings, store)
	require.NotNil(t, sm.blockPark, "the park must be built or this test measures nothing")

	t.Cleanup(func() { sm.recentlyFailedBlocks.Stop() })

	checkpointHash := chainhash.Hash{0xcc}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 1_000_000, Hash: &checkpointHash}

	peer, rec := schedulerPeer(t, sm, 1, int32(len(blocks))+1000)
	sm.storeSyncPeer(peer, &syncPeerState{})

	sm.headerMu.Lock()
	sm.headerList = list.New()
	sm.headerIndex = make(map[chainhash.Hash]*list.Element)
	sm.headersByHeight = make(map[int32]*list.Element)

	for i, b := range blocks {
		hash := b.MsgBlock().BlockHash()
		node := &headerNode{height: int32(i + 1), hash: &hash}
		sm.indexHeaderLocked(sm.headerList.PushBack(node), hash)
	}

	// Nothing is left for the cursor walk to resume from, which is what keeps
	// this test's passes the only ones that run: fetchMoreHeaderBlocks, which
	// parkOrphanBlock calls on every parked block, gates on a non-nil startHeader
	// and returns straight back. The loop below is therefore the sole driver of
	// fetchHeaderBlocks, and a park that stayed inside its bound did so because
	// of the bound and not because a background pass happened not to run.
	sm.startHeader = nil
	sm.headerMu.Unlock()

	sm.headersFirstMode.Store(true)

	return sm, rec
}

// TestWantedRange_TheParkNeverReachesItsEntryCap pins the consequence. A full
// park refuses the one block that would extend the tip, the drain finds no child
// for the settled hash, and the node idles with thousands of unusable blocks on
// disk. That is what the 4096-entry cap did on 2026-09-12.
//
// The park is bounded because the download is bounded by position: a block can
// only be parked if it was obtained, it can only be obtained if it was
// requested, and the wanted range will not name a height more than the
// read-ahead depth above the last committed block. So the park cannot exceed the
// depth, and the depth — 4 here, 128 as legacy_blockDownloadLowerWindow ships,
// scaled down from there by block size — is far below the 4096-entry cap.
//
// Blocks are delivered highest-first so every one of them is an orphan on
// arrival, which is the case the park exists for and the case that filled it.
//
// Two honest limits, stated rather than buried.
//
// The committer is frozen ABOVE zero rather than at it. Mainnet on 2026-09-12
// was settled at 868 with the download front at 4877, so a stopped committer
// part-way up is the faithful reproduction — but there is a sharper reason.
// lastCommittedHeight reading zero makes wantedBlocks derive its depth from two
// different anchors: best is zero while the ceiling falls back to the front of
// the header list, so the effective depth becomes front plus the configured
// depth rather than the configured depth. The park then holds one more block
// than the depth allows. That is the same unseeded-counter defect
// TestWantedRange_ARestartingNodeRequestsOnItsFirstPass names, seen from the
// other side, and it belongs in that test rather than smuggled into this one.
//
// With nothing committing, the arrival of a block also removes its header from
// the list, so a second mechanism reinforces the first — the range cannot
// re-name a height whose header has gone. That is why the load-bearing assertion
// here is the POSITION one, that no block above the window is ever in the park at
// all. Header removal can only take away a block already obtained; it cannot be
// what stops a block twenty heights up being requested in the first place.
func TestWantedRange_TheParkNeverReachesItsEntryCap(t *testing.T) {
	// Six times the depth, so a pass that ran away would have somewhere to run.
	blocks := minedBlocks(t, propertyDepth*6)

	sm, rec := newParkPropertyManager(t, blocks)

	// The committer's position, frozen for the whole test. Nothing here ever
	// writes it again, and nothing can: the blockchain mock holds no block, so
	// every arrival is an orphan and no block ever commits.
	const parkBest = int32(propertyDepth)

	sm.lastCommittedHeight.Store(parkBest)

	// Every block above the window, by hash, so the position assertion can be
	// made against the park directly.
	aboveWindow := make(map[chainhash.Hash]int32, len(blocks))
	for i, b := range blocks {
		if height := int32(i + 1); height > parkBest+propertyDepth {
			aboveWindow[b.MsgBlock().BlockHash()] = height
		}
	}

	byHash := make(map[chainhash.Hash]int, len(blocks))
	for i, b := range blocks {
		byHash[b.MsgBlock().BlockHash()] = i
	}

	seen := 0

	for pass := 0; pass < propertyPasses; pass++ {
		fresh := runPass(t, sm, rec, seen, pass)
		seen += len(fresh)

		// Highest first, so each block arrives before its parent and parks. The
		// pass hands runs out in ascending order, so reversing what it asked for
		// is the worst case the park can be given.
		for i := len(fresh) - 1; i >= 0; i-- {
			index, known := byHash[fresh[i]]
			require.True(t, known, "pass %d asked for a block the harness never mined", pass)

			msgBlock := blocks[index].MsgBlock()

			require.NoError(t, sm.processQueuedBlock(&blockQueueMsg{
				block:       msgBlock,
				blockHash:   fresh[i],
				blockHeight: int32(index + 1),
				peer:        sm.loadSyncPeer(),
			}), "pass %d: delivering block %d", pass, index+1)
		}

		require.LessOrEqual(t, sm.blockPark.Len(), propertyDepth,
			"pass %d left the park holding more blocks than the read-ahead depth allows", pass)

		for hash, height := range aboveWindow {
			require.False(t, sm.blockPark.Has(hash),
				"pass %d parked the block at height %d, which is more than %d above the committer frozen at %d",
				pass, height, propertyDepth, parkBest)
		}
	}

	// Without this the loop could be vacuously true: a park that was never given
	// anything satisfies every bound in it.
	require.Positive(t, sm.blockPark.Len(), "the passes must actually have parked something")

	// The arithmetic the whole property rests on. The park cannot exceed the
	// depth, so as long as the depth is below the entry cap the cap is
	// unreachable — which is what the cursor walk, bounded by a count rather than
	// a position, could not say.
	require.Less(t, propertyDepth, maxParkedEntries,
		"a park bounded by the read-ahead depth can only miss its entry cap while the depth is below it")
}
