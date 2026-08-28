package utxoset

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// planTestCoins is how many single-output transactions the plan tests seed.
//
// It has to be large enough that the planner has a genuine choice. Below roughly a hundred
// thousand rows a sequential scan of a partition really is the cheaper plan, so a plan
// assertion at that size would pass for the wrong reason and stop catching the regression it
// exists for. Seeding is a single INSERT ... SELECT and takes a few seconds.
const planTestCoins = 400_000

// seedCoins fills the coin table with n single-output transactions.
//
// It writes the rows directly rather than going through Create, because these tests are about
// the plan the planner picks against a table big enough to have a choice, and building that
// table through the store's write path would take minutes instead of seconds. The packing
// mirrors Pack exactly: the first twelve bytes of the transaction id followed by the output
// index as a big-endian uint32, which is why the literal below is four zero bytes.
func seedCoins(t *testing.T, s *Store, ctx context.Context, n int) {
	t.Helper()

	_, err := s.pool.Exec(ctx, `
INSERT INTO utxo (satoshis, created_height, spendable_from, leaf, flags, ukey, txid, script)
SELECT 1000, 100, 0,
       (get_byte(t.id, 0) & 7)::smallint,
       0,
       encode(substring(t.id from 1 for 12) || '\x00000000'::bytea, 'hex')::uuid,
       t.id,
       '\x76a914000000000000000000000000000000000000000088ac'::bytea
  FROM (SELECT sha256(g::text::bytea) AS id FROM generate_series(1, $1) g) t`, n)
	require.NoError(t, err, "seeding the coin table")

	_, err = s.pool.Exec(ctx, `ANALYZE utxo`)
	require.NoError(t, err, "analysing the coin table")
}

// sampleTxids returns n transaction ids drawn from the seeded coins, spread across partitions.
func sampleTxids(t *testing.T, s *Store, ctx context.Context, n int) [][]byte {
	t.Helper()

	rows, err := s.pool.Query(ctx,
		`SELECT txid FROM utxo ORDER BY md5(txid::text) LIMIT $1`, n)
	require.NoError(t, err)

	defer rows.Close()

	var out [][]byte

	for rows.Next() {
		var id []byte
		require.NoError(t, rows.Scan(&id))
		out = append(out, id)
	}

	require.NoError(t, rows.Err())
	require.Len(t, out, n, "sampling seeded transaction ids")

	return out
}

// explain returns the planner's chosen plan for a statement, as text.
func explain(t *testing.T, s *Store, ctx context.Context, sql string, args ...any) string {
	t.Helper()

	rows, err := s.pool.Query(ctx, "EXPLAIN "+sql, args...)
	require.NoError(t, err, "explaining the statement")

	defer rows.Close()

	var b strings.Builder

	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		b.WriteString(line)
		b.WriteString("\n")
	}

	require.NoError(t, rows.Err())

	return b.String()
}

// TestWithLiveCoinsDoesNotScanTheCoinTable pins the plan of the pruner's hottest statement.
//
// withLiveCoins asks "does this transaction still hold an unspent output" for every parent
// named by one retiring journal partition. The coin table carries exactly one index, on the
// packed key, and the schema states in its own words that any query filtering on the
// transaction id without a packed-key range bound is a review failure. Without that bound the
// planner reads the whole coin table on every call, and the pruner makes one call per retiring
// partition, so the cost grows with the live unspent set and with how far behind the pruner is
// at the same time.
//
// This is not a micro-optimisation. On the mainnet soak box the pruner spent about ninety
// percent of its database time in this one statement, which stretched a pruning session to
// tens of minutes, which let retained transaction bytes reach twenty gigabytes because the
// bytes are only released once per session.
//
// The assertion is on the plan rather than on a duration because a timing threshold on shared
// test hardware is a flake generator, and because the plan is the thing that must not change.
func TestWithLiveCoinsDoesNotScanTheCoinTable(t *testing.T) {
	s, ctx := newTestStore(t)

	seedCoins(t, s, ctx, planTestCoins)

	txids := sampleTxids(t, s, ctx, 500)

	leaves, ids, los, his := liveCoinArgs(txids)
	plan := explain(t, s, ctx, hasLiveCoinSQL, leaves, ids, los, his)

	require.NotContains(t, plan, "Seq Scan on utxo_p", "plan reads a coin partition whole:\n"+plan)
	require.Contains(t, plan, "Index", "plan never reaches the packed-key index:\n"+plan)
}

// TestWithLiveCoinsFindsExactlyTheParentsWithAnUnspentOutput guards the answer while the plan
// changes underneath it.
//
// The packed key holds only the first twelve bytes of the transaction id and is deliberately
// non-unique, so a range bound on it can admit a collision but can never exclude a genuine
// match. The full transaction id is still compared on the row, so the answer is unchanged. This
// test is what proves that rather than asserting it.
func TestWithLiveCoinsFindsExactlyTheParentsWithAnUnspentOutput(t *testing.T) {
	s, ctx := newTestStore(t)

	seedCoins(t, s, ctx, 1000)

	present := sampleTxids(t, s, ctx, 20)

	// Twenty transaction ids the store has never held. Derived from the seeded ones by
	// flipping the last byte, so they share a leaf and a packed-key range with a real row
	// and can only be rejected by the full-identity comparison.
	absent := make([][]byte, 0, len(present))

	for _, id := range present {
		other := append([]byte(nil), id...)
		other[len(other)-1] ^= 0xff
		absent = append(absent, other)
	}

	live, err := s.withLiveCoins(ctx, append(append([][]byte{}, present...), absent...))
	require.NoError(t, err)

	require.Len(t, live, len(present), "every seeded parent still holds its output")

	for _, id := range present {
		require.Contains(t, live, string(id), "seeded parent reported as having no live coin")
	}

	for _, id := range absent {
		require.NotContains(t, live, string(id), "unknown transaction reported as having a live coin")
	}
}
