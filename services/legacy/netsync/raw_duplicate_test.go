package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// When two peers send the same block at once, one copy is converted as it arrives and the other,
// refused a second conversion, is written raw. On 2026-09-24 mainnet block 707,178 arrived from
// two peers within a second, the raw copy finished first, and the park took it. Processing the raw
// body writes the block's subtree files, and the converting copy wrote the same subtree files in
// its own format a second later, so validation read a mix, failed, and the block was given up with
// both files deleted. The chain stopped at 707,177. A raw copy is now discarded whenever another
// copy of the block is converting or has already been converted, and only the raw file goes.

func TestARawCopyIsDiscardedWhileAnotherCopyIsConverting(t *testing.T) {
	ctx := context.Background()
	h := newParkWiringHarness(t, true)
	h.sm.drainAsync.Store(true)
	h.sm.parkCommits = make(chan parkCommit, 4)

	header := wire.BlockHeader{Version: 1, PrevBlock: h.blocks[1].MsgBlock().BlockHash()}
	body := peerpkg.BlockBody{Header: header, TxCount: 3, Size: 4096, Hash: header.BlockHash()}

	require.NoError(t, h.sm.blockPark.store.Set(ctx, body.Hash[:], fileformat.FileTypeMsgBlock, []byte("raw"), parkOpts...))

	h.sm.inFlightBlocksMu.Lock()
	h.sm.inFlightBlocks = map[chainhash.Hash]*inFlightBlock{body.Hash: {}}
	h.sm.inFlightBlocksMu.Unlock()

	h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

	require.False(t, h.sm.blockPark.Has(body.Hash), "the converting copy is the one the park takes")

	exists, err := h.sm.blockPark.store.Exists(ctx, body.Hash[:], fileformat.FileTypeMsgBlock, parkOpts...)
	require.NoError(t, err)
	require.False(t, exists, "the raw copy's bytes are removed")
}

func TestARawCopyIsDiscardedWhenTheBlockIsAlreadyConverted(t *testing.T) {
	ctx := context.Background()
	h := newParkWiringHarness(t, true)
	h.sm.drainAsync.Store(true)
	h.sm.parkCommits = make(chan parkCommit, 4)

	blk, hash := convertedRecordWithSubtrees(t, 1, 707178)
	require.NoError(t, h.sm.blockPark.WriteConvertedBlock(ctx, hash, blk))
	require.NoError(t, h.sm.blockPark.store.Set(ctx, hash[:], fileformat.FileTypeMsgBlock, []byte("raw"), parkOpts...))

	body := peerpkg.BlockBody{TxCount: 1, Size: 4096, Hash: hash}
	body.Header.PrevBlock = *blk.Header.HashPrevBlock

	h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

	require.False(t, h.sm.blockPark.Has(hash), "the raw copy is not taken")

	exists, err := h.sm.blockPark.store.Exists(ctx, hash[:], fileformat.FileTypeMsgBlock, parkOpts...)
	require.NoError(t, err)
	require.False(t, exists, "its bytes are removed")

	exists, err = h.sm.blockPark.store.Exists(ctx, hash[:], fileformat.FileTypeBlock, parkOpts...)
	require.NoError(t, err)
	require.True(t, exists, "and the converted record is left alone")
}
