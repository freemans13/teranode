package netsync

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
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

// TestBlockPark_AFullParkIsAskedAboutBeforeAnyOfItExpires is the sweep's whole
// reason for existing, asserted as what the sweep does rather than as arithmetic
// over the constants that shape it.
//
// The sweep is the ONLY thing that ever commits a parked block whose parent was
// already in the chain when the node started: a block recovered from disk never
// sees a commit event for that parent, so nothing else will ever look at it. It
// gets from parkStuckThreshold to parkEntryTTL to work through the park, one
// tick every parkSweepInterval, parkSweepRPCBudget parents per tick. If a full
// pass over a full park does not fit in that window, then after a restart with a
// full park the blocks it never reached expire and are downloaded a second time
// — which is the entire cost the park exists to avoid.
//
// It is driven through sweepParkedBlocks, the production entry point, and the
// assertion is made on what actually reached the blockchain service, so it
// covers the round-robin, the per-tick budget and the wiring between them
// together. A sweep that re-sampled the index at random would leave blocks
// unasked about however long it ran.
func TestBlockPark_AFullParkIsAskedAboutBeforeAnyOfItExpires(t *testing.T) {
	var (
		mu   sync.Mutex
		seen = make(map[chainhash.Hash]struct{}, maxParkedEntries)
	)

	client := &blockchain2.Mock{}
	client.On("GetBlockExists", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			hash, ok := args.Get(1).(*chainhash.Hash)
			require.True(t, ok)

			mu.Lock()
			seen[*hash] = struct{}{}
			mu.Unlock()
		}).
		Return(false, nil)

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		ctx:              context.Background(),
		blockchainClient: client,
		blockPark:        newIndexOnlyPark(),
	}

	// A full park, as a restart can find one: every block waiting on a different
	// parent, none of them yet looked at.
	parked := time.Now()

	for i := 0; i < maxParkedEntries; i++ {
		var hash, prev chainhash.Hash

		binary.LittleEndian.PutUint32(hash[:], uint32(i))
		binary.LittleEndian.PutUint32(prev[4:], uint32(i))

		sm.blockPark.entries[hash] = &parkedBlock{hash: hash, prevBlock: prev, parkedAt: parked}
	}

	// The ticks a block gets between becoming a sweep candidate and running out
	// of time. Only as many as a full pass needs are used, so the test says "a
	// full pass fits" rather than "a full pass happens eventually".
	ticks := maxParkedEntries / parkSweepRPCBudget

	require.LessOrEqual(t, time.Duration(ticks)*parkSweepInterval, parkEntryTTL-parkStuckThreshold,
		"a full pass has to fit between a block becoming a candidate and its time running out")

	for tick := 0; tick < ticks; tick++ {
		sm.sweepParkedBlocks(parked.Add(parkStuckThreshold + time.Second + time.Duration(tick)*parkSweepInterval))
	}

	require.Equal(t, maxParkedEntries, sm.blockPark.Len(),
		"nothing may have expired while the sweep was still working through the park")

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, seen, maxParkedEntries,
		"every parked block's parent must have been asked about within %d ticks of %d; whatever the sweep does not reach expires and is downloaded again",
		ticks, parkSweepRPCBudget)
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
