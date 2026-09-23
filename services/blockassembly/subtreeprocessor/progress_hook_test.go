package subtreeprocessor

import (
	"context"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// progressRecorder is a progress hook that records the processor's current
// block header at every call, so a test can tell WHEN the beats happened
// relative to the blocks being applied, not only how many there were.
type progressRecorder struct {
	mu      sync.Mutex
	stp     *SubtreeProcessor
	headers []chainhash.Hash
}

func (r *progressRecorder) hook() {
	var h chainhash.Hash
	if header := r.stp.currentBlockHeader.Load(); header != nil {
		h = *header.Hash()
	}

	r.mu.Lock()
	r.headers = append(r.headers, h)
	r.mu.Unlock()
}

func (r *progressRecorder) seen() []chainhash.Hash {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]chainhash.Hash(nil), r.headers...)
}

// newProgressTestProcessor builds a started processor on a sqlitememory UTXO
// store, with a blockchain mock the caller configures.
func newProgressTestProcessor(t *testing.T, blockchainClient *blockchain.Mock) (*SubtreeProcessor, *sql.Store) {
	t.Helper()

	ctx := t.Context()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), blob_memory.New(), blockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	stp.Start(ctx)
	t.Cleanup(func() {
		stp.Stop(context.Background())
		close(newSubtreeChan)
	})

	return stp, utxoStore
}

func childHeader(parent *model.BlockHeader, nonce uint32) *model.BlockHeader {
	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  parent.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567890 + nonce,
		Bits:           model.NBit{},
		Nonce:          nonce,
	}
}

// TestProgressHookBeatsBetweenCatchUpBlocks pins the property the hook exists
// for: a multi-block catch-up reports progress as each block lands, so the
// owner's heartbeat ages by one block's work at most rather than by the whole
// call (issue 1447).
//
// The assertion is on the header the processor held at each beat, not on a
// count. A beat while the tip was block1 proves a beat happened after block1
// was applied and before block2; the same for block2. Beating only once at the
// start of Reorg would leave only the parent in the list and fail this test.
func TestProgressHookBeatsBetweenCatchUpBlocks(t *testing.T) {
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	stp, _ := newProgressTestProcessor(t, blockchainClient)

	recorder := &progressRecorder{stp: stp}
	stp.SetProgressHook(recorder.hook)

	parent := prevBlockHeader
	h1 := childHeader(parent, 11)
	h2 := childHeader(h1, 12)
	h3 := childHeader(h2, 13)

	blocks := []*model.Block{
		{Height: 1, CoinbaseTx: coinbaseTx, Subtrees: []*chainhash.Hash{}, Header: h1},
		{Height: 2, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{}, Header: h2},
		{Height: 3, CoinbaseTx: coinbaseTx3, Subtrees: []*chainhash.Hash{}, Header: h3},
	}

	stp.InitCurrentBlockHeader(parent)

	require.NoError(t, stp.Reorg([]*model.Block{}, blocks))

	// End state first: the catch-up must actually have been applied, or the
	// beats below prove nothing about a working catch-up.
	require.Equal(t, h3.Hash(), stp.GetCurrentBlockHeader().Hash(), "catch-up must advance the processor to the last block")

	seen := recorder.seen()
	require.Contains(t, seen, *h1.Hash(), "no beat between block1 and block2: a long catch-up would age the heartbeat by the whole call")
	require.Contains(t, seen, *h2.Hash(), "no beat between block2 and block3: a long catch-up would age the heartbeat by the whole call")

	// One beat for the waitForBlockBeingMined poll, one per block.
	require.Len(t, seen, 1+len(blocks))
}

// TestProgressHookBeatsThroughAFullReorg covers the other branch of
// reorgBlocks, taken whenever blocks are moved back: every step that runs once
// per block must report progress, moving back, moving forward and marking
// processed, as must the wait for pending blocks that precedes them.
func TestProgressHookBeatsThroughAFullReorg(t *testing.T) {
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(prevBlockHeader, &model.BlockHeaderMeta{}, nil)
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	stp, utxoStore := newProgressTestProcessor(t, blockchainClient)

	var calls int

	var mu sync.Mutex

	stp.SetProgressHook(func() {
		mu.Lock()
		calls++
		mu.Unlock()
	})

	// block2 is moved back, block3 replaces it. After the move back the tip is
	// the parent the blockchain mock returns, prevBlockHeader, so block3 builds
	// on that.
	block2Header := childHeader(blockHeader, 2)
	block3Header := childHeader(prevBlockHeader, 3)

	moveBack := &model.Block{Height: 2, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{}, Header: block2Header}
	moveForward := &model.Block{Height: 2, CoinbaseTx: coinbaseTx3, Subtrees: []*chainhash.Hash{}, Header: block3Header}

	_, err := utxoStore.Create(t.Context(), coinbaseTx2, 2)
	require.NoError(t, err)

	stp.InitCurrentBlockHeader(block2Header)

	require.NoError(t, stp.Reorg([]*model.Block{moveBack}, []*model.Block{moveForward}))
	require.Equal(t, block3Header.Hash(), stp.GetCurrentBlockHeader().Hash(), "reorg must land on block3")

	mu.Lock()
	defer mu.Unlock()

	// WaitForPendingBlocks attempt, move back block2, move forward block3, mark
	// block3 processed.
	require.Equal(t, 4, calls)
}

// TestProgressHookBeatsWhileWaitingForBlockValidation pins that each poll of
// waitForBlockBeingMined reports progress. The single-block advance runs this
// wait, up to 300s, whenever a block carries conflicting transactions; unbeaten
// it would age the heartbeat by the whole wait on a routine new block.
func TestProgressHookBeatsWhileWaitingForBlockValidation(t *testing.T) {
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(false, nil).Twice()
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Once()

	stp, _ := newProgressTestProcessor(t, blockchainClient)

	var calls int

	stp.SetProgressHook(func() { calls++ })

	mined, err := stp.waitForBlockBeingMined(t.Context(), blockHeader.Hash())
	require.NoError(t, err)
	require.True(t, mined)

	require.Equal(t, 3, calls, "one beat per poll")
}

// TestProgressHookBeatsWhileWaitingForPendingBlocks pins that each attempt of
// WaitForPendingBlocks reports progress. That wait has no ceiling; it runs
// before a full reorg and from the reset path.
func TestProgressHookBeatsWhileWaitingForPendingBlocks(t *testing.T) {
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{{Header: blockHeader}}, nil).Once()
	blockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil).Once()

	stp, _ := newProgressTestProcessor(t, blockchainClient)

	var calls int

	stp.SetProgressHook(func() { calls++ })

	require.NoError(t, stp.WaitForPendingBlocks(t.Context()))

	require.Equal(t, 2, calls, "one beat per attempt")
}

// TestSetProgressHookNilRemovesTheHook pins that clearing the hook is safe:
// block movement must not call a removed hook, nor panic without one.
func TestSetProgressHookNilRemovesTheHook(t *testing.T) {
	stp := &SubtreeProcessor{}

	var calls int

	stp.SetProgressHook(func() { calls++ })
	stp.reportProgress()
	require.Equal(t, 1, calls)

	stp.SetProgressHook(nil)
	stp.reportProgress()
	require.Equal(t, 1, calls, "a removed hook must not be called")
}
