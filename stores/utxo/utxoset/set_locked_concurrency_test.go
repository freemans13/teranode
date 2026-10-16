package utxoset

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestSetLockedConcurrentBatchesDoNotDeadlock pins the rule that lets the lock batcher run
// batches in parallel. Many callers set and clear the lock on overlapping sets of transactions
// at once, through the batcher (one hash) and the direct path (many hashes, in random order).
// Without the sorted advisory lock in setLockedDirect, two batches naming the same transactions
// in different orders can deadlock, and PostgreSQL then aborts one with an error. Every call
// must succeed, and at the end every transaction reads back unlocked.
func TestSetLockedConcurrentBatchesDoNotDeadlock(t *testing.T) {
	s, ctx := newTestStoreWith(t, func(ts *settings.Settings) {
		ts.UtxoStore.LockedBatcherSize = 16
		ts.UtxoStore.LockedBatcherDurationMillis = 1
		ts.UtxoStore.BatcherMaxConcurrent = 8
		ts.BatcherBackground = true
	})
	require.NotNil(t, s.lockBatcher)

	const n = 40

	txs := make([]chainhash.Hash, n)

	for i := range txs {
		tx := mkTx(t, 3, uint64(10_000+i))
		_, err := s.Create(ctx, tx, 100)
		require.NoError(t, err)

		txs[i] = *tx.TxIDChainHash()
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for w := 0; w < 32; w++ {
		w := w

		wg.Add(1)

		go func() {
			defer wg.Done()

			rng := rand.New(rand.NewSource(int64(w))) //nolint:gosec // test ordering only

			for round := 0; round < 25; round++ {
				var err error

				if w%2 == 0 {
					err = s.SetLocked(ctx, []chainhash.Hash{txs[rng.Intn(n)]}, round%2 == 0)
				} else {
					picks := rng.Perm(n)[:8]
					hs := make([]chainhash.Hash, 0, len(picks))

					for _, p := range picks {
						hs = append(hs, txs[p])
					}

					err = s.SetLocked(ctx, hs, round%2 == 0)
				}

				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()
	require.Empty(t, errs, "a concurrent SetLocked failed, which is how a deadlock shows")

	require.NoError(t, s.SetLocked(ctx, txs, false))

	for i := range txs {
		got, err := s.Get(ctx, &txs[i], fields.Locked)
		require.NoError(t, err)
		require.False(t, got.Locked)
	}
}
