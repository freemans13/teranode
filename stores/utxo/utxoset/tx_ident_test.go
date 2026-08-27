package utxoset

import (
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// requireCheckViolation asserts the error is postgres rejecting a CHECK constraint, not any
// error at all. Without this a missing table satisfies require.Error and the test passes
// while proving nothing.
func requireCheckViolation(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()

	require.Error(t, err, msgAndArgs...)

	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "want a postgres error, got %T: %v", err, err)
	require.Equal(t, "23514", pgErr.Code,
		"want SQLSTATE 23514 check_violation, got %s: %s", pgErr.Code, pgErr.Message)
}

// packMembership builds the packed form tx_ident.membership carries: 12-byte triples of
// blockID, height and subtree index, all big-endian.
func packMembership(t *testing.T, triples ...[3]uint32) []byte {
	t.Helper()

	b := make([]byte, 0, len(triples)*12)
	for _, tr := range triples {
		var e [12]byte
		binary.BigEndian.PutUint32(e[0:4], tr[0])
		binary.BigEndian.PutUint32(e[4:8], tr[1])
		binary.BigEndian.PutUint32(e[8:12], tr[2])
		b = append(b, e[:]...)
	}

	return b
}

// TestTxIdentRejectsAWrongLeaf is the constraint that makes the primary key mean what the
// design says it means.
//
// PostgreSQL enforces PRIMARY KEY (leaf, txid) only WITHIN one partition. Verified on 17.11
// and 18.6: the same txid inserted under leaf 0 and leaf 1 is accepted, giving two rows, and
// ON CONFLICT (leaf, txid) DO NOTHING reports the second as a fresh insert. Seven of the
// eight wrong values are in range, so nothing about a mistake fails safely. The CHECK is
// therefore not defensive tidiness, it IS the global uniqueness rule.
func TestTxIdentRejectsAWrongLeaf(t *testing.T) {
	s, ctx := newTestStore(t)

	txid := make([]byte, 32)
	txid[0] = 0x08 // 0x08 & 7 == 0, so leaf 0 is the only correct routing

	_, err := s.pool.Exec(ctx,
		`INSERT INTO tx_ident (leaf, txid, created_height) VALUES ($1, $2, $3)`,
		int16(0), txid, int32(100))
	require.NoError(t, err, "the correct leaf must be accepted")

	_, err = s.pool.Exec(ctx,
		`INSERT INTO tx_ident (leaf, txid, created_height) VALUES ($1, $2, $3)`,
		int16(1), txid, int32(100))
	requireCheckViolation(t, err,
		"a wrong leaf must be rejected by the CHECK: without it the same txid lands in two partitions and the primary key never sees the collision")
}

// TestTxIdentRejectsATxidThatIsNotThirtyTwoBytes covers the other half of the same CHECK,
// and the reason the length test has to come FIRST and sit in the same AND.
//
// get_byte on an empty bytea raises a database error, where Go's LeafFor returns 0 for an
// empty txid (schema.go). The AND short-circuits, so that disagreement surfaces as an
// ordinary constraint violation instead of an error from inside the expression.
func TestTxIdentRejectsATxidThatIsNotThirtyTwoBytes(t *testing.T) {
	s, ctx := newTestStore(t)

	for _, tc := range []struct {
		name string
		txid []byte
	}{
		{"empty", []byte{}},
		{"thirty one bytes", make([]byte, 31)},
		{"thirty three bytes", make([]byte, 33)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.pool.Exec(ctx,
				`INSERT INTO tx_ident (leaf, txid, created_height) VALUES ($1, $2, $3)`,
				int16(0), tc.txid, int32(100))
			requireCheckViolation(t, err, "identity is the full 32 bytes; anything else must not be storable")
		})
	}
}

// TestMempoolReloadUsesThePartialIndex pins the one query whose cost decides this design.
//
// Block assembly rebuilds the ENTIRE mempool from this query at startup and on every reset,
// with no height bound, and a reorg is what triggers a reset. Measured on PG 18.6 at 43M
// rows with a 43,100-transaction mempool: 3,484 page fetches through the partial index when
// the mempool is fresh, against 978,145 for the sequential scan it falls back to. A partial
// index only holds entries for rows matching its condition, so while the mempool is a tiny
// fraction of the table the index stays tiny too -- 524,288 bytes, 0.0122 bytes per table
// row.
//
// This asserts the PLAN, not the timing, because at test scale postgres will reasonably
// prefer a sequential scan over a handful of rows. The plan is what transfers to production.
func TestMempoolReloadUsesThePartialIndex(t *testing.T) {
	s, ctx := newTestStore(t)

	var indexed bool
	require.NoError(t, s.pool.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM pg_index i
              JOIN pg_class c ON c.oid = i.indexrelid
             WHERE i.indrelid = 'tx_ident'::regclass
               AND i.indpred IS NOT NULL
               AND c.relname = 'tx_ident_off_chain_idx')`).Scan(&indexed))
	require.True(t, indexed,
		"the mempool reload must be served by a PARTIAL index on off_chain_since, not a full one: a full index would carry an entry for every transaction the store has ever held")
}

// TestMembershipMaxHeightTakesTheHighest pins the reducer the settled predicate depends on.
//
// A transaction is settled when its off-chain marker is NULL and the HIGHEST block height in
// its membership is at or below the tip minus 288. Taking the highest is what makes it sound:
// it is at least the main-chain height, so if the highest is 288 deep the main-chain block is
// too. The incumbent SQL pruner takes the most favourable height instead, with no best-chain
// filter, so a child mined low on a fork and re-mined recently reads as stable
// (stores/utxo/sql/pruner/pruner_service.go:172-176). Do not copy that half.
func TestMembershipMaxHeightTakesTheHighest(t *testing.T) {
	s, ctx := newTestStore(t)

	t.Run("takes the maximum rather than the first", func(t *testing.T) {
		m := packMembership(t, [3]uint32{11, 2_000, 0}, [3]uint32{22, 1_000, 3})

		var got int64
		require.NoError(t, s.pool.QueryRow(ctx, `SELECT mh_max($1)`, m).Scan(&got))
		require.Equal(t, int64(2_000), got,
			"a fork entry listed first must not hide a later main-chain height")
	})

	t.Run("does not wrap on a height above the signed 32-bit boundary", func(t *testing.T) {
		// 0xFF000000 shifted left 24 as int4 wraps to a NEGATIVE number in postgres, silently,
		// which would make every "<= cutoff" test come back true and settle everything.
		m := packMembership(t, [3]uint32{1, 0xFF00_0001, 0})

		var got int64
		require.NoError(t, s.pool.QueryRow(ctx, `SELECT mh_max($1)`, m).Scan(&got))
		require.Equal(t, int64(0xFF00_0001), got, "the casts must be bigint, not int")
	})

	t.Run("is null for a transaction with no membership", func(t *testing.T) {
		var got *int64
		require.NoError(t, s.pool.QueryRow(ctx, `SELECT mh_max($1)`, []byte{}).Scan(&got))
		require.Nil(t, got, "no membership means no height, which must not read as height zero")
	})
}
