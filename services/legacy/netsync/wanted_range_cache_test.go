package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestWantedBlocksFromCache_IsTheRunAboveTheCommittedTip(t *testing.T) {
	parent := chainhash.Hash{0xaa}
	headers := chainOfHeaders(parent, 10)

	sm := newRaceManager(t)
	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(parent, 101, headers))

	got := sm.wantedBlocksFromCache(100, 4)

	require.Len(t, got, 4, "the range is bounded by the depth, whatever the cache holds")
	require.Equal(t, int32(101), got[0].height, "and starts one above the best block processed")
	require.Equal(t, headers[0].BlockHash(), got[0].hash)
	require.Equal(t, int32(104), got[3].height)
}

// TestWantedBlocksFromCache_StopsAtTheFirstHeightTheCacheCannotName is the rule
// that keeps the park from filling above a hole. A block whose parent never
// arrives cannot commit, so blocks fetched past a gap only wait.
func TestWantedBlocksFromCache_StopsAtTheFirstHeightTheCacheCannotName(t *testing.T) {
	parent := chainhash.Hash{0xaa}

	sm := newRaceManager(t)
	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(parent, 101, chainOfHeaders(parent, 3)))

	got := sm.wantedBlocksFromCache(100, 10)

	require.Len(t, got, 3, "the cache names three heights, so three is the range")
}

func TestWantedBlocksFromCache_IsEmptyWithNoCache(t *testing.T) {
	sm := newRaceManager(t)

	require.Empty(t, sm.wantedBlocksFromCache(100, 4),
		"no cache means nothing can be named, so the pass asks for nothing and the next getheaders fixes it")
}

// TestUnownedBlocks_DropsBlocksAlreadyOnDisk is the deletion the whole model
// turns on: a block we hold is not wanted, whatever any index says.
func TestUnownedBlocks_DropsBlocksAlreadyOnDisk(t *testing.T) {
	sm := newRaceManager(t)
	sm.ctx = context.Background()
	park, _ := newTestPark(t, "")
	sm.blockPark = park

	held := chainhash.Hash{0x01}
	wanted := chainhash.Hash{0x02}

	require.NoError(t, sm.blockPark.store.Set(context.Background(), held[:], parkFileType, []byte("body"), parkOpts...))

	got := sm.unownedBlocks([]wantedBlock{
		{height: 101, hash: held},
		{height: 102, hash: wanted},
	})

	require.Len(t, got, 1, "the block already on disk must not be asked for again")
	require.Equal(t, wanted, got[0].hash)
}

func TestUnownedBlocks_StillDropsBlocksAPeerOwes(t *testing.T) {
	sm := newRaceManager(t)
	sm.ctx = context.Background()
	park, _ := newTestPark(t, "")
	sm.blockPark = park

	p, _ := schedulerPeer(t, sm, 1, 1000)

	owed := chainhash.Hash{0x03}
	free := chainhash.Hash{0x04}

	require.True(t, sm.blockDownloads.Add(p, owed))

	got := sm.unownedBlocks([]wantedBlock{
		{height: 101, hash: owed},
		{height: 102, hash: free},
	})

	require.Len(t, got, 1, "a block inside its retry window has an owner who may still deliver")
	require.Equal(t, free, got[0].hash)

	_ = time.Second
}
