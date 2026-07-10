//go:build testtxmetacache

package blockvalidation

// Window fan-out memory-bound test — proves the shared cross-block create/spend
// goroutine limiter (settings.BlockValidation.WindowStoreConcurrency) caps the TOTAL
// number of concurrently-alive per-tx Create/Spend goroutines across an entire
// ProcessBlockWindow call, independent of window size K and per-block tx count.
//
// WHY this matters: each spawned create/spend goroutine pins a tx + its OFF-HEAP stack
// until its store op completes. Off-heap stacks are NOT bounded by GOMEMLIMIT, so an
// unbounded fan-out ((concurrent blocks) x (per-block tx count)) blows the memory
// ceiling on fat blocks regardless of the heap limit. The limiter must gate at SPAWN
// time (before .Go), so the spawning loop — not a live goroutine + stack — blocks.
//
// Technique: concurrencyTrackingStore wraps NullStore. Create/Spend increment an
// atomic in-flight counter on entry (recording a running max), hold briefly to force
// overlap, then decrement on exit. Running ProcessBlockWindow with a small
// WindowStoreConcurrency and asserting the recorded peak never exceeds it is the
// discriminator: against the OLD unbounded code the peak is ~ K * txPerBlock (>> limit);
// with the shared limiter it is <= limit.

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// concurrencyTrackingStore wraps NullStore and records the peak number of Create/Spend
// calls in flight simultaneously. It does NOT persist any UTXO data — this isolates the
// goroutine-fan-out bound from data correctness (parity is covered elsewhere).
type concurrencyTrackingStore struct {
	*nullstore.NullStore
	inFlight atomic.Int64
	peak     atomic.Int64
	hold     time.Duration // per-op hold to force overlap
}

func newConcurrencyTrackingStore(t *testing.T, hold time.Duration) *concurrencyTrackingStore {
	t.Helper()
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)
	return &concurrencyTrackingStore{NullStore: ns, hold: hold}
}

// enter/leave bracket one store op, updating the running peak.
func (s *concurrencyTrackingStore) enter() {
	cur := s.inFlight.Add(1)
	for {
		p := s.peak.Load()
		if cur <= p || s.peak.CompareAndSwap(p, cur) {
			break
		}
	}
}

func (s *concurrencyTrackingStore) leave() { s.inFlight.Add(-1) }

func (s *concurrencyTrackingStore) SupportsOutpointOnlySpend() bool { return true }

func (s *concurrencyTrackingStore) Create(_ context.Context, _ *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*utxometa.Data, error) {
	s.enter()
	defer s.leave()
	time.Sleep(s.hold)
	return nil, nil
}

func (s *concurrencyTrackingStore) Spend(_ context.Context, _ *bt.Tx, _ uint32, _ ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	s.enter()
	defer s.leave()
	time.Sleep(s.hold)
	return nil, nil
}

// buildFanoutBlocks builds k self-contained blocks, each with a coinbase plus
// txPerBlock independent non-coinbase txs. Each tx spends a distinct output of that
// block's coinbase, so all txPerBlock txs within a block are independent (maximising
// intra-block fan-out). The store does not persist, so the create/spend calls never
// need the parents to exist — this is purely a goroutine-count harness.
func buildFanoutBlocks(t *testing.T, bv *BlockValidation, ctx context.Context, genesisHash *chainhash.Hash, startHeight uint32, k, txPerBlock int) []*model.Block {
	t.Helper()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	privKey, _ := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	blocks := make([]*model.Block, k)
	prevHash := *genesisHash

	for i := range k {
		height := startHeight + uint32(i) //nolint:gosec

		// Coinbase with txPerBlock outputs so each regular tx spends a distinct one.
		cb := transactions.Create(t,
			transactions.WithCoinbaseData(height, "/fanout-test/"),
			transactions.WithP2PKHOutputs(txPerBlock, 1e8, privKey.PubKey()),
		)

		regularTxs := make([]*bt.Tx, txPerBlock)
		for j := range txPerBlock {
			regularTxs[j] = transactions.Create(t,
				transactions.WithPrivateKey(privKey),
				transactions.WithInput(cb, uint32(j)), //nolint:gosec
				transactions.WithP2PKHOutputs(1, 1000),
				transactions.WithChangeOutput(),
			)
		}

		subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(nextPow2(txPerBlock + 1))
		require.NoError(t, err)
		require.NoError(t, subtree.AddCoinbaseNode())
		for _, tx := range regularTxs {
			require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, uint64(tx.Size()))) //nolint:gosec
		}

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)
		require.NoError(t, bv.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))

		subtreeData := subtreepkg.NewSubtreeData(subtree)
		require.NoError(t, subtreeData.AddTx(cb, 0))
		for j, tx := range regularTxs {
			require.NoError(t, subtreeData.AddTx(tx, j+1))
		}
		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err)
		require.NoError(t, bv.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

		merkleRoot, err := subtree.RootHashWithReplaceRootNode(cb.TxIDChainHash(), 0, 0)
		require.NoError(t, err)

		ph := prevHash
		blk := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &ph,
				HashMerkleRoot: merkleRoot,
				Timestamp:      1_000_000 + uint32(i), //nolint:gosec
				Bits:           *nBits,
			},
			Height:           height,
			CoinbaseTx:       cb,
			Subtrees:         []*chainhash.Hash{subtree.RootHash()},
			TransactionCount: uint64(txPerBlock + 1), //nolint:gosec
		}
		for {
			if ok, _, _ := blk.Header.HasMetTargetDifficulty(); ok {
				break
			}
			blk.Header.Nonce++
			if blk.Header.Nonce > 5_000_000 {
				t.Fatal("failed to find valid PoW nonce")
			}
		}
		prevHash = *blk.Hash()
		blocks[i] = blk
	}
	return blocks
}

// nextPow2 returns the smallest power of two >= n (subtree leaf counts must be pow2).
func nextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// newFanoutHarness wires an in-memory BlockValidation around a concurrency-tracking
// UTXO store, with the shared-window limiter sized by windowStoreConcurrency.
func newFanoutHarness(t *testing.T, ctx context.Context, spy utxo.Store, windowStoreConcurrency int) *BlockValidation {
	t.Helper()

	logger := ulogger.TestLogger{}
	tSettings := testutil.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	tSettings.BlockValidation.QuickValidateSkipUtxoLock = true
	tSettings.BlockValidation.WindowStoreConcurrency = windowStoreConcurrency

	params := *tSettings.ChainCfgParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}
	tSettings.ChainCfgParams = &params

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              blockchainClient,
		utxoStore:                     spy,
		subtreeStore:                  blobmemory.New(),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
	}
	t.Cleanup(func() {
		bv.blockExistsCache.Stop()
		bv.lastValidatedBlocks.Stop()
	})
	return bv
}

// TestWindowFanoutBound_CapsConcurrentStoreGoroutines is the memory-bound gate.
//
// A window of k blocks, each with txPerBlock independent txs, is processed with a
// SMALL WindowStoreConcurrency limit. The concurrency-tracking store records the peak
// number of Create/Spend calls simultaneously in flight.
//
//   - RED (old unbounded code): peak ~ up to k * txPerBlock (creates fan out per block
//     and blocks run concurrently), FAR exceeding the limit.
//   - GREEN (shared limiter): peak <= limit for BOTH the C1 create phase and the C2
//     spend phase.
func TestWindowFanoutBound_CapsConcurrentStoreGoroutines(t *testing.T) {
	const (
		k          = 8  // blocks in the window
		txPerBlock = 64 // independent txs per block → fat-block-like fan-out
		limit      = 4  // deliberately far below k*txPerBlock (=512)
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Hold each store op long enough that many goroutines would pile up concurrently
	// in the unbounded case, making the peak a reliable discriminator.
	spy := newConcurrencyTrackingStore(t, 2*time.Millisecond)

	bv := newFanoutHarness(t, ctx, spy, limit)

	blocks := buildFanoutBlocks(t, bv, ctx, bv.settings.ChainCfgParams.GenesisHash, 100, k, txPerBlock)

	require.NoError(t, bv.ProcessBlockWindow(ctx, blocks, "fanout-test-peer"), "ProcessBlockWindow must succeed")

	peak := spy.peak.Load()
	require.LessOrEqual(t, peak, int64(limit),
		"peak concurrent Create/Spend store goroutines (%d) must not exceed WindowStoreConcurrency (%d); unbounded fan-out would reach ~%d",
		peak, limit, k*txPerBlock)

	// Sanity: the window really did fan out (peak should reach the limit, not stay at 1),
	// otherwise the test would pass trivially without exercising concurrency.
	require.Greater(t, peak, int64(1), "expected genuine concurrency (peak > 1); test would be vacuous otherwise")
}

// TestWindowFanoutBound_SingleBlockPathUnaffected proves the nil-limiter single-block
// path (spendBatchWithRetry called directly, as quickValidateBlock does) still applies
// its own per-block SafeSetLimit and is NOT gated by any shared window semaphore. It
// asserts a nil limiter is accepted and behaviour is preserved: with a per-block limit
// far above the tx count, all spends can run concurrently (peak > 1), exactly as before.
func TestWindowFanoutBound_SingleBlockPathUnaffected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spy := newConcurrencyTrackingStore(t, 2*time.Millisecond)
	// Large window limit that the single-block path must IGNORE (it passes nil).
	bv := newFanoutHarness(t, ctx, spy, 1)

	// Build a set of independent minimal spend txs and drive the nil-limiter path.
	const nTxs = 32
	txs := make([]*bt.Tx, nTxs)
	for i := range txs {
		tx := bt.NewTx()
		tx.LockTime = uint32(i + 1) //nolint:gosec
		txs[i] = tx
	}

	block := &model.Block{Height: 100, Header: model.GenesisBlockHeader, ID: 1}
	bv.spendRetryBackoff = time.Millisecond

	// nil limiter → single-block path: per-block SafeSetLimit governs, NOT the window sem.
	require.NoError(t, bv.spendBatchWithRetry(ctx, block, txs, true, nil))

	// The single-block path's SpendBatcherSize*SpendBatcherConcurrency*2 limit is large
	// (default 100*32*2), so with nil limiter these 32 spends run concurrently: peak > 1.
	// If the window sem (size 1) had leaked into this path, peak would be pinned to 1.
	require.Greater(t, spy.peak.Load(), int64(1),
		"nil-limiter single-block path must retain per-block concurrency (window semaphore must NOT apply)")
}

// verify effectiveWindowStoreConcurrency's auto-derive + guard behaviour without
// touching the DB pool.
func TestEffectiveWindowStoreConcurrency(t *testing.T) {
	logger := ulogger.TestLogger{}

	newBV := func(window, spendSize, spendConc int) *BlockValidation {
		s := testutil.CreateBaseTestSettings(t)
		s.BlockValidation.WindowStoreConcurrency = window
		s.UtxoStore.SpendBatcherSize = spendSize
		s.UtxoStore.SpendBatcherConcurrency = spendConc
		return &BlockValidation{logger: logger, settings: s}
	}

	t.Run("explicit positive value used verbatim", func(t *testing.T) {
		require.Equal(t, 17, newBV(17, 100, 32).effectiveWindowStoreConcurrency())
	})

	t.Run("zero auto-derives SpendBatcherSize*SpendBatcherConcurrency", func(t *testing.T) {
		require.Equal(t, 100*32, newBV(0, 100, 32).effectiveWindowStoreConcurrency())
	})

	t.Run("misconfigured batcher settings fall back to positive NumCPU cap, never zero", func(t *testing.T) {
		got := newBV(0, 0, 0).effectiveWindowStoreConcurrency()
		require.Positive(t, got, "must never resolve to a non-positive (deadlocking) semaphore size")
	})
}
