package utxoset

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// The family-chunk write takes no advisory lock on the transactions it stores. These tests pin the
// case the lock used to settle by waiting: a rival that only CREATES a transaction T, spending
// nothing, so that no row lock on a coin makes it wait. What now keeps T single is the identity
// key, which the claim inserts ON CONFLICT (leaf, txid) DO NOTHING.
//
// Each test ends on the store's state and not on which writer ran first. In every order T has
// exactly one identity row, one set of UTXO rows, and at most one containment row per block. What
// differs by order is the parent coin, and the difference is the same one a serial run of the two
// writers has: a create-only writer never spends, so when it stores T first, the chunk finds T
// stored and leaves the coin to whoever promised the spend, and when the chunk claims first, the
// coin is spent by T. The tests assert each, and they do not pretend the second order spends.

const rivalHeight = 1_100

// txMinedRows is how many containment rows the store holds for tx in the block.
func txMinedRows(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx, blockID int) int {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_mined WHERE txid = $1 AND block_id = $2`, hashBytes(tx), blockID).Scan(&n))

	return n
}

// requireSingleRecordOf asserts the end state every order shares: one identity row, and one set
// of UTXO rows, which is one row per output of child, because child's output is never spent here.
func requireSingleRecordOf(t *testing.T, s *Store, ctx context.Context, child *bt.Tx) {
	t.Helper()

	require.Equal(t, 1, identityCount(t, s, ctx, child), "exactly one identity row")
	require.Equal(t, len(child.Outputs), utxoCount(t, s, ctx, child), "exactly one set of UTXO rows")
}

func rivalOptions(block int, extra ...utxo.CreateOption) []utxo.CreateOption {
	return append([]utxo.CreateOption{
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: uint32(block), BlockHeight: rivalHeight, OnLongestChain: true}), //nolint:gosec // a test block id
		utxo.WithIgnoreLocked(true),
	}, extra...)
}

// requireCoinLiveUntilTheSpendPass asserts the state after the rival stored T first: the coin is
// live and unclaimed because a create-only writer never spends. The spend pass the create-only
// caller promised then spends it, and from there a rival spend is refused as ever.
func requireCoinLiveUntilTheSpendPass(t *testing.T, s *Store, ctx context.Context, parent, child *bt.Tx) {
	t.Helper()

	require.Equal(t, 2, utxoCount(t, s, ctx, parent), "the coin is live: the create-only rival did not spend it and the chunk found T stored")
	require.Equal(t, 0, journalRowsFor(t, s, ctx, parent, 0))

	_, _, err := s.SpendAndCreate(ctx, spendOutput(t, parent, 0, 1), rivalHeight, utxo.WithSpendOnly(), utxo.WithIgnoreLocked(true))
	require.NoError(t, err, "the spend pass of the create-only writer")

	requireCoinStaysSpentByChild(t, s, ctx, parent, child)
}

// The rival arrives after the chunk has claimed T and before it commits. Its claim waits on the
// chunk's uncommitted identity row, and when the chunk commits finds T stored. T is spent by the
// chunk, and a second spender of the coin is refused.
func TestSpendAndCreateMultiARivalCreateAfterTheClaimWaitsOnTheIdentityKey(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rivalBlock int
		wantRows   map[int]int
	}{
		{name: "the same block", rivalBlock: 7, wantRows: map[int]int{7: 1}},
		{name: "another block", rivalBlock: 8, wantRows: map[int]int{7: 1, 8: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := newUncheckpointedStore(t)
			parent := storedParent(t, s, ctx)
			child := spendOutput(t, parent, 0, 1)

			rivalErr := make(chan error, 1)
			finished := make(chan struct{})
			fired := false

			// The claim is made and the commit has not happened: a rival now meets the chunk's
			// uncommitted identity row.
			multiCreateBeforeCommit = func(context.Context, []chainhash.Hash) {
				if fired {
					return
				}

				fired = true

				go func() {
					_, _, err := s.SpendAndCreate(context.Background(), spendOutput(t, parent, 0, 1), rivalHeight,
						rivalOptions(tc.rivalBlock, utxo.WithCreateOnly())...)
					rivalErr <- err

					close(finished)
				}()

				select {
				case <-finished:
					t.Error("a create of T finished while the chunk held T's identity row uncommitted")
				case <-time.After(400 * time.Millisecond):
				}
			}

			t.Cleanup(func() { multiCreateBeforeCommit = nil })

			results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, rivalHeight, rivalOptions(7)...)
			multiCreateBeforeCommit = nil
			require.NoError(t, err)
			require.Equal(t, utxo.MultiTxCreated, results[0].Status)

			require.ErrorIs(t, <-rivalErr, errors.ErrTxExists, "the rival found T stored")

			requireSingleRecordOf(t, s, ctx, child)

			for block, want := range tc.wantRows {
				require.Equal(t, want, txMinedRows(t, s, ctx, child, block), "containment rows of block %d", block)
			}

			requireCoinStaysSpentByChild(t, s, ctx, parent, child)
		})
	}
}

// The rival stores T between the chunk's spend and its claim, which is the order the advisory
// lock used to forbid. The chunk's claim then finds T stored, rolls back, and redoes without it.
// T is stored once. The coin is live, as it is when the rival stores T entirely before the chunk,
// because a create-only writer spends nothing; the spend pass it promised takes it.
func TestSpendAndCreateMultiARivalCreateBetweenTheSpendAndTheClaimStoresTOnce(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rivalBlock int
		wantRows   map[int]int
	}{
		{name: "the same block", rivalBlock: 7, wantRows: map[int]int{7: 1}},
		// The chunk's containment row for block 7 went with its rolled-back round; the caller
		// records it from the Existed answer, as for any transaction stored before its block.
		{name: "another block", rivalBlock: 8, wantRows: map[int]int{7: 0, 8: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := newUncheckpointedStore(t)
			parent := storedParent(t, s, ctx)
			child := spendOutput(t, parent, 0, 1)

			fired := false

			multiChunkFault = func(_ int, stage string) error {
				if stage != "spent" || fired {
					return nil
				}

				fired = true

				_, _, err := s.SpendAndCreate(context.Background(), spendOutput(t, parent, 0, 1), rivalHeight,
					rivalOptions(tc.rivalBlock, utxo.WithCreateOnly())...)
				require.NoError(t, err, "the rival is not held up by the chunk")

				return nil
			}

			t.Cleanup(func() { multiChunkFault = nil })

			results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, rivalHeight, rivalOptions(7)...)
			multiChunkFault = nil
			require.NoError(t, err)
			require.Equal(t, utxo.MultiTxExisted, results[0].Status, "the rival stored T first")

			requireSingleRecordOf(t, s, ctx, child)

			for block, want := range tc.wantRows {
				require.Equal(t, want, txMinedRows(t, s, ctx, child, block), "containment rows of block %d", block)
			}

			requireCoinLiveUntilTheSpendPass(t, s, ctx, parent, child)
		})
	}
}

// The rival stores T entirely before the chunk starts. This is the serial order, and it ends as
// the between-spend-and-claim order does: T stored once, the chunk reports it stored.
func TestSpendAndCreateMultiARivalCreateBeforeTheChunkStoresTOnce(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)
	parent := storedParent(t, s, ctx)
	child := spendOutput(t, parent, 0, 1)

	_, _, err := s.SpendAndCreate(ctx, spendOutput(t, parent, 0, 1), rivalHeight, rivalOptions(7, utxo.WithCreateOnly())...)
	require.NoError(t, err)

	results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, rivalHeight, rivalOptions(7)...)
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxExisted, results[0].Status)

	requireSingleRecordOf(t, s, ctx, child)
	require.Equal(t, 1, txMinedRows(t, s, ctx, child, 7))
	requireCoinLiveUntilTheSpendPass(t, s, ctx, parent, child)
}

// Several writers of one list at once, each its own chunks over the same chain of transactions,
// with no lock between them. Every transaction ends stored exactly once, every edge is spent
// exactly once, and each writer's answer is Created or Existed for each transaction, with exactly
// one writer answering Created for it.
func TestSpendAndCreateMultiConcurrentWritersOfOneListStoreEachTransactionOnce(t *testing.T) {
	const (
		length  = 24
		writers = 6
	)

	setChunkTxs(t, 5)

	s, ctx := newUncheckpointedStore(t)
	parent := storedParent(t, s, ctx)

	chain := make([]*bt.Tx, length)
	chain[0] = spendOutput(t, parent, 0, 1)

	for i := 1; i < length; i++ {
		chain[i] = spendOutput(t, chain[i-1], 0, 1)
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created = make(map[int]int)
	)

	errs := make(chan error, writers)

	for w := 0; w < writers; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Each writer has its own copy: the write decorates the inputs of the transactions it is
			// given, and two processes never share those objects.
			mine := make([]*bt.Tx, length)

			for i, tx := range chain {
				c, perr := bt.NewTxFromBytes(tx.Bytes())
				if perr != nil {
					errs <- perr
					return
				}

				mine[i] = c
			}

			results, err := s.SpendAndCreateMulti(ctx, mine, rivalHeight, rivalOptions(7)...)
			if err != nil {
				errs <- err
				return
			}

			mu.Lock()
			defer mu.Unlock()

			for i, r := range results {
				switch r.Status {
				case utxo.MultiTxCreated:
					created[i]++
				case utxo.MultiTxExisted:
				default:
					errs <- errors.NewProcessingError("tx %d: status %v: %v", i, r.Status, r.Err)
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	for i, tx := range chain {
		require.Equal(t, 1, identityCount(t, s, ctx, tx), "tx %d identity rows", i)
		require.Equal(t, 1, txMinedRows(t, s, ctx, tx, 7), "tx %d containment rows", i)

		wantCoins := 0
		if i == length-1 {
			wantCoins = 1
		}

		require.Equal(t, wantCoins, utxoCount(t, s, ctx, tx), "tx %d UTXO rows: every output but the last is spent by its child", i)

		if i < length-1 {
			require.Equal(t, 1, journalRowsFor(t, s, ctx, tx, 0), "tx %d journal rows for the coin its child spends", i)
		}
	}

	require.Equal(t, 1, utxoCount(t, s, ctx, parent), "the parent's other output is untouched")
	require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0), "the parent's coin is spent once")

	for i := range chain {
		require.Equal(t, 1, created[i], "tx %d was answered Created by exactly one writer", i)
	}
}
