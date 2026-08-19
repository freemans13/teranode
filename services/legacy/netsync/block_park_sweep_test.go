package netsync

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newIndexOnlyPark builds a park with nothing but an index, which is all
// StuckCandidates touches. No blobs, no store: the ordering question is purely
// about which entries the sweep hands back.
func newIndexOnlyPark() *blockPark {
	return &blockPark{
		logger:   ulogger.TestLogger{},
		entries:  make(map[chainhash.Hash]*parkedBlock),
		children: make(map[chainhash.Hash][]chainhash.Hash),
	}
}

// TestBlockPark_TheSweepWorksThroughTheWholeParkInsteadOfResamplingIt is the
// difference between a safety net and a lottery.
//
// The sweep is the ONLY thing that ever commits a parked block whose parent was
// already in the chain when the node started — a restart-recovered block never
// sees a commit event for its parent, so nothing else will ever look at it.
// Iterating the index map in Go's randomised order, and returning entries
// without recording that they were looked at, means each tick re-samples the
// same population: some entries come up repeatedly and others may not come up
// at all before their thirty minutes are gone and they are re-downloaded.
//
// The assertion is coverage, not order: every parked block must have been
// offered once after the number of ticks it takes to spend one budget per
// entry.
func TestBlockPark_TheSweepWorksThroughTheWholeParkInsteadOfResamplingIt(t *testing.T) {
	const (
		parked = 40
		budget = 8
	)

	park := newIndexOnlyPark()

	now := time.Now()
	longEnoughAgo := now.Add(-parkStuckThreshold - time.Minute)

	for i := 0; i < parked; i++ {
		var hash chainhash.Hash

		hash[0] = byte(i)

		park.entries[hash] = &parkedBlock{hash: hash, parkedAt: longEnoughAgo}
	}

	seen := make(map[chainhash.Hash]struct{}, parked)

	for tick := 0; tick < parked/budget; tick++ {
		batch := park.StuckCandidates(now, budget)
		require.Len(t, batch, budget, "a tick must spend its whole budget while candidates remain")

		for _, candidate := range batch {
			seen[candidate.hash] = struct{}{}
		}
	}

	require.Len(t, seen, parked,
		"every parked block must have been offered to the chain once after %d ticks of %d; a sweep that re-samples at random leaves blocks to expire and be downloaded again", parked/budget, budget)
}

// TestBlockPark_TheSweepCanCoverAFullParkBeforeItsBlocksExpire pins the
// arithmetic that makes the sweep a safety net at all.
//
// A block is given up after parkEntryTTL. The sweep runs every
// parkSweepInterval and asks the chain about at most parkSweepRPCBudget parents
// each time. If a full pass over a full park cannot finish inside the TTL, then
// after a restart with a full park most of those blocks expire unexamined and
// are downloaded a second time — which is the whole cost the park exists to
// avoid.
func TestBlockPark_TheSweepCanCoverAFullParkBeforeItsBlocksExpire(t *testing.T) {
	ticksBeforeExpiry := int(parkEntryTTL / parkSweepInterval)

	require.GreaterOrEqual(t, ticksBeforeExpiry*parkSweepRPCBudget, maxParkedEntries,
		"the sweep gets %d ticks of %d lookups before a block expires, %d in all, and the park holds up to %d; a full pass has to fit",
		ticksBeforeExpiry, parkSweepRPCBudget, ticksBeforeExpiry*parkSweepRPCBudget, maxParkedEntries)
}

// TestBlockPark_ARecoveredBlockKeepsTheAgeItHadBeforeTheRestart closes the
// second half of the same hole. Stamping parkedAt at recovery time means a node
// that restarts more often than parkEntryTTL never expires anything: a block
// whose parent is genuinely never coming is held, and its budget with it, for
// as long as the node keeps restarting. The blob's own modification time is
// when the block was parked, and it survives the restart because it is on disk.
func TestBlockPark_ARecoveredBlockKeepsTheAgeItHadBeforeTheRestart(t *testing.T) {
	park, dir := newTestPark(t, "")

	blocks := minedBlocks(t, 1)
	msgBlock := blocks[0].MsgBlock()
	hash := msgBlock.BlockHash()

	require.Equal(t, parkAccepted,
		park.Park(context.Background(), parkedBlock{hash: hash, prevBlock: msgBlock.Header.PrevBlock}, msgBlock))

	// The node ran for longer than the TTL, then restarted.
	aged := time.Now().Add(-parkEntryTTL - time.Minute)

	for _, name := range parkDirEntries(t, dir) {
		require.NoError(t, os.Chtimes(filepath.Join(dir, name), aged, aged))
	}

	fresh, _ := newTestPark(t, "")
	fresh.dir = dir
	fresh.store = park.store
	fresh.Recover(context.Background())

	require.Equal(t, 1, fresh.Len(), "the block must be adopted before anything can expire it")

	expired := fresh.Expire(time.Now())

	require.Len(t, expired, 1,
		"a block parked longer ago than the TTL must expire on the first sweep after a restart, not start its half hour again")
	require.True(t, expired[0].hash.IsEqual(&hash))
	require.Zero(t, fresh.Len())
}
