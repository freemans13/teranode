package utxoset

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestUTXOIndexRebuildDecision pins the rule: a partition's packed-key index is rebuilt when
// it holds more than 55 bytes per entry, which is well above the 31.5-byte bulk floor and
// below the 63-byte churn plateau the store measured, and at most one partition per session.
// The fixtures sit above the size floor so it is the ratio being judged here.
func TestUTXOIndexRebuildDecision(t *testing.T) {
	require.False(t, utxoIndexNeedsRebuild(315_000_000, 10_000_000))
	require.False(t, utxoIndexNeedsRebuild(500_000_000, 10_000_000))
	require.True(t, utxoIndexNeedsRebuild(600_000_000, 10_000_000))
	require.False(t, utxoIndexNeedsRebuild(600_000_000, 0), "no rows, nothing to judge")
}

// TestUTXOIndexRebuildIgnoresASmallIndex pins the size floor. A btree carries a metapage and
// a root whatever it holds, so on a partition of a few thousand rows the fixed pages alone
// put it over 55 bytes per entry, and on the 2026-09-22 mainnet reset the pruner rebuilt the
// eight 98 KB indexes 110 times in 13 minutes, each back to 72 KB, reclaiming nothing worth a
// REINDEX CONCURRENTLY. Below the floor the ratio is not judged at all.
func TestUTXOIndexRebuildIgnoresASmallIndex(t *testing.T) {
	require.False(t, utxoIndexNeedsRebuild(98_304, 1_637), "60 bytes per entry, but 98 KB is fixed overhead")
	require.False(t, utxoIndexNeedsRebuild(utxoIndexRebuildMinBytes-1, (utxoIndexRebuildMinBytes-1)/100), "just under the floor")
	require.True(t, utxoIndexNeedsRebuild(utxoIndexRebuildMinBytes, utxoIndexRebuildMinBytes/100), "at the floor the ratio is judged again")
}

// TestRebuildUTXOIndexRunsConcurrentlyAndOnce exercises the statement against the test
// database: it must succeed on a live partition and touch only one partition per call.
func TestRebuildUTXOIndexRunsConcurrentlyAndOnce(t *testing.T) {
	s, ctx := newTestStore(t)

	n, err := s.rebuildOneBloatedUTXOIndex(ctx, func(bytes, rows int64) bool { return true })
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

// TestStatisticsRefreshIsSkippedWhileARebuildRuns pins one half of the rule that keeps the
// stamp worker's ANALYZE and the pruner's REINDEX CONCURRENTLY apart. On mainnet on
// 2026-09-26 they deadlocked five times in three hours: the rebuild waits for every
// transaction that could see the old index, the ANALYZE of utxo is one of them, and that
// ANALYZE waits for the partition lock the rebuild holds. PostgreSQL kills the rebuild, the
// half-built index is dropped, and the bloated index is never rebuilt.
//
// A refresh that finds a rebuild running is skipped rather than queued. A rebuild of a
// mainnet partition runs for about fifteen minutes, and the stamp worker must not wait that
// long; the next stamped window analyzes again. The store has no pool here, so a skip that
// reached the database would panic.
func TestStatisticsRefreshIsSkippedWhileARebuildRuns(t *testing.T) {
	s := &Store{logger: ulogger.TestLogger{}}

	s.indexMaintenance.Lock()
	defer s.indexMaintenance.Unlock()

	require.False(t, s.refreshStatistics(context.Background(), 7), "a refresh must not run while a rebuild holds the lock")
}

// TestRebuildWaitsForAStatisticsRefresh pins the other half: a rebuild that finds a refresh
// running waits for it. A refresh takes seconds, and starting the rebuild alongside it is
// exactly the overlap that deadlocks.
func TestRebuildWaitsForAStatisticsRefresh(t *testing.T) {
	s, ctx := newTestStore(t)

	s.indexMaintenance.Lock()

	done := make(chan int, 1)

	go func() {
		n, err := s.rebuildOneBloatedUTXOIndex(ctx, func(bytes, rows int64) bool { return true })
		if err != nil {
			n = -1
		}
		done <- n
	}()

	select {
	case <-done:
		t.Fatal("the rebuild ran while a statistics refresh held the lock")
	case <-time.After(300 * time.Millisecond):
	}

	s.indexMaintenance.Unlock()

	select {
	case n := <-done:
		require.Equal(t, 1, n, "the rebuild runs once the refresh has finished")
	case <-time.After(60 * time.Second):
		t.Fatal("the rebuild never ran after the lock was released")
	}
}
