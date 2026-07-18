package netsync

// Tests for realignCheckpointCursor (the stale-checkpoint-cursor latch fix).
//
// Root cause proven live on mainnet (2026-07-17/18): nextCheckpoint caches
// "the next checkpoint after our tip" but is only advanced by the direct block
// path. The window/park pipeline committed checkpoint block 216116 without
// advancing it (the recognised re-delivery was discarded by the
// window-ownership guard, which returns before checkpoint handling), stranding
// the cursor while the tip marched to 250k+. The next sync-peer rotation
// destroyed the in-memory header list, and every startSync since evaluated its
// headers-first gate against the stale cursor (tip 250087 >= 216116 => false),
// latching the node into getblocks mode where no delivered block can resolve a
// height: ~1400 BLOCK_NOT_FOUND rejections and exactly one committed block per
// 120-second rotation, forever. Only a restart healed it, because New()
// derives the cursor fresh from the committed tip. The fix runs that same
// derivation at every startSync.

import (
	"container/list"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// mainnetCheckpointAt returns the mainnet checkpoint with exactly the given
// height, failing the test if it does not exist in the params.
func mainnetCheckpointAt(t *testing.T, height int32) *chaincfg.Checkpoint {
	t.Helper()

	for i := range chaincfg.MainNetParams.Checkpoints {
		if chaincfg.MainNetParams.Checkpoints[i].Height == height {
			return &chaincfg.MainNetParams.Checkpoints[i]
		}
	}

	t.Fatalf("no mainnet checkpoint at height %d", height)

	return nil
}

// buildRealignManager constructs the minimal SyncManager realignCheckpointCursor
// and resetHeaderState need: params, logger, header list/index.
func buildRealignManager() *SyncManager {
	return &SyncManager{
		logger:            ulogger.TestLogger{},
		chainParams:       &chaincfg.MainNetParams,
		headerList:        list.New(),
		headerHeightIndex: make(map[chainhash.Hash]int32),
	}
}

// TestRealignCheckpointCursor_StrandedCursorHeals reproduces the live wedge:
// cursor stranded at 216116 while the committed tip is 250863. The startSync
// gate must be computing getblocks mode with the stale value (the failing
// precondition), and realign must restore the true next checkpoint (267300),
// re-arming headers-first, realigning headerCheckpoint, and reseeding the
// header list from the tip via resetHeaderState.
func TestRealignCheckpointCursor_StrandedCursorHeals(t *testing.T) {
	sm := buildRealignManager()

	bestHeight := int32(250863)
	bestHash := chainhash.Hash{0xaa, 0xbb}

	sm.nextCheckpoint = mainnetCheckpointAt(t, 216116)
	sm.headerCheckpoint = sm.nextCheckpoint

	// The exact gate expression startSync uses: with the stranded cursor it
	// chooses getblocks mode even though checkpoint 267300 is still ahead.
	staleGate := sm.nextCheckpoint != nil && bestHeight < sm.nextCheckpoint.Height
	require.False(t, staleGate,
		"precondition: the stale cursor must make the headers-first gate false")

	genBefore := sm.headerGen

	require.True(t, sm.realignCheckpointCursor(&bestHash, bestHeight),
		"realign must report the cursor was stale")

	require.NotNil(t, sm.nextCheckpoint)
	require.Equal(t, int32(267300), sm.nextCheckpoint.Height,
		"cursor must move to the true next checkpoint after the tip")
	require.Equal(t, sm.nextCheckpoint, sm.headerCheckpoint,
		"header-request cursor must be realigned with the block-level cursor")

	healedGate := sm.nextCheckpoint != nil && bestHeight < sm.nextCheckpoint.Height
	require.True(t, healedGate,
		"after realign the headers-first gate must re-arm")

	require.Greater(t, sm.headerGen, genBefore,
		"resetHeaderState must have run (headerGen bumped)")
	require.Equal(t, 1, sm.headerList.Len(),
		"header list must hold exactly the best-tip seed node")

	front, ok := sm.headerList.Front().Value.(*headerNode)
	require.True(t, ok)
	require.Equal(t, bestHeight, front.height)
	require.Equal(t, bestHash, *front.hash)
}

// TestRealignCheckpointCursor_HealthyNoOp: when the stored cursor already
// matches the fresh derivation, realign must not touch header state — startSync
// runs on every rotation and must stay cheap and side-effect-free when healthy.
func TestRealignCheckpointCursor_HealthyNoOp(t *testing.T) {
	sm := buildRealignManager()

	bestHeight := int32(250863)
	bestHash := chainhash.Hash{0xcc}

	sm.nextCheckpoint = sm.findNextHeaderCheckpoint(bestHeight)
	require.NotNil(t, sm.nextCheckpoint)
	sm.headerCheckpoint = sm.nextCheckpoint

	genBefore := sm.headerGen

	require.False(t, sm.realignCheckpointCursor(&bestHash, bestHeight),
		"healthy cursor must be a no-op")
	require.Equal(t, genBefore, sm.headerGen,
		"no header-state reset on the healthy path")
	require.Equal(t, 0, sm.headerList.Len())
}

// TestRealignCheckpointCursor_CheckpointsDisabled: a node started with
// DisableCheckpoints must never have checkpoints re-enabled by the realign
// (chainParams still lists them; only the stored flag says they are off).
func TestRealignCheckpointCursor_CheckpointsDisabled(t *testing.T) {
	sm := buildRealignManager()
	sm.checkpointsDisabled = true

	bestHeight := int32(250863)
	bestHash := chainhash.Hash{0xdd}

	require.False(t, sm.realignCheckpointCursor(&bestHash, bestHeight))
	require.Nil(t, sm.nextCheckpoint,
		"realign must not resurrect a cursor on a checkpoints-disabled node")
	require.Equal(t, uint64(0), sm.headerGen)
}

// TestRealignCheckpointCursor_PastFinalCheckpoint: a stored cursor left behind
// when the tip has passed the final checkpoint must be cleared (nil cursor =
// correct end-game getblocks behaviour), and the header state reset.
func TestRealignCheckpointCursor_PastFinalCheckpoint(t *testing.T) {
	sm := buildRealignManager()

	checkpoints := chaincfg.MainNetParams.Checkpoints
	final := checkpoints[len(checkpoints)-1]
	bestHeight := final.Height + 1
	bestHash := chainhash.Hash{0xee}

	sm.nextCheckpoint = mainnetCheckpointAt(t, 216116)
	sm.headerCheckpoint = sm.nextCheckpoint

	require.True(t, sm.realignCheckpointCursor(&bestHash, bestHeight),
		"cursor stranded behind a passed final checkpoint is stale")
	require.Nil(t, sm.nextCheckpoint,
		"past the final checkpoint the cursor must clear")
	require.Nil(t, sm.headerCheckpoint,
		"resetHeaderState realigns headerCheckpoint to the cleared cursor")
	require.Equal(t, 0, sm.headerList.Len(),
		"no seed node is pushed when there is no next checkpoint")
}
