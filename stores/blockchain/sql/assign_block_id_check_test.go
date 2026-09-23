package sql

import (
	"context"
	"database/sql"
	"regexp"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/stretchr/testify/require"
)

// The happy paths and refusals of checkCallerSuppliedBlockID are pinned in
// StoreBlock_ReservedID_test.go. These cases cover what happens when a lookup the
// check depends on fails: every one must refuse with a storage error rather than
// fall through to the INSERT, since a failed read proves nothing about the id.

func TestCheckCallerSuppliedBlockID_ClosedDB(t *testing.T) {
	s := newReservedIDTestStore(t)
	require.NoError(t, s.db.Close()) // the cleanup still closes the store once

	err := s.checkCallerSuppliedBlockID(context.Background(), block1.Hash(), 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "got %v", err)
}

func TestCheckCallerSuppliedBlockID_ReservationLookupFails(t *testing.T) {
	ctx := context.Background()
	s := newReservedIDTestStore(t)

	// The blocks table is intact, so both blocks lookups find nothing and the
	// check reaches the reservation lookup, which then fails on the missing table.
	_, err := s.db.ExecContext(ctx, `DROP TABLE block_id_reservations`)
	require.NoError(t, err)

	err = s.checkCallerSuppliedBlockID(ctx, block1.Hash(), 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "durable block-id reservation")

	_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(1))
	require.Error(t, err)
	requireNoBlockRow(t, s, "reservation lookup failed")
}

// The next three need one query to succeed and a later one to fail, which a real
// database will not do on demand, so they script the driver.

var (
	sqlBlockIDByHash      = regexp.QuoteMeta(`SELECT id FROM blocks WHERE hash = $1`)
	sqlBlockHashByID      = regexp.QuoteMeta(`SELECT hash FROM blocks WHERE id = $1`)
	sqlReservationByHash  = regexp.QuoteMeta(`SELECT block_id FROM block_id_reservations WHERE hash = $1`)
	sqlReservationByID    = regexp.QuoteMeta(`SELECT hash FROM block_id_reservations WHERE block_id = $1 LIMIT 1`)
	sqlSQLiteSequenceSeq  = regexp.QuoteMeta(`SELECT seq FROM sqlite_sequence WHERE name = 'blocks'`)
	errInjectedLookupFail = errors.NewProcessingError("injected lookup failure")
)

func TestCheckCallerSuppliedBlockID_InjectedFailures(t *testing.T) {
	ctx := context.Background()
	h := chainhash.HashH([]byte("check-caller-supplied-id"))

	t.Run("blocks-by-id lookup fails", func(t *testing.T) {
		s, mock, err := createMockSQL()
		require.NoError(t, err)

		mock.ExpectQuery(sqlBlockIDByHash).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(sqlBlockHashByID).WillReturnError(errInjectedLookupFail)

		err = s.checkCallerSuppliedBlockID(ctx, &h, 7)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to look up the block stored under id 7")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("reservation-by-id lookup fails", func(t *testing.T) {
		s, mock, err := createMockSQL()
		require.NoError(t, err)

		mock.ExpectQuery(sqlBlockIDByHash).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(sqlBlockHashByID).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(sqlReservationByHash).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(sqlReservationByID).WillReturnError(errInjectedLookupFail)

		err = s.checkCallerSuppliedBlockID(ctx, &h, 7)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to look up the reservation holding block id 7")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("sequence read fails", func(t *testing.T) {
		s, mock, err := createMockSQL()
		require.NoError(t, err)

		mock.ExpectQuery(sqlBlockIDByHash).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(sqlBlockHashByID).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(sqlReservationByHash).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(sqlReservationByID).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(sqlSQLiteSequenceSeq).WillReturnError(errInjectedLookupFail)

		err = s.checkCallerSuppliedBlockID(ctx, &h, 7)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to read the highest issued block id")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// highestIssuedBlockID must read as zero, never as an error, when the sequence
// has nothing to report: then every caller-supplied id without a reservation is
// refused as never issued, which is the safe direction.
func TestHighestIssuedBlockID_NothingIssued(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		stmt string
	}{
		{"no sequence row", `DELETE FROM sqlite_sequence WHERE name = 'blocks'`},
		{"NULL sequence value", `UPDATE sqlite_sequence SET seq = NULL WHERE name = 'blocks'`},
		{"negative sequence value", `UPDATE sqlite_sequence SET seq = -1 WHERE name = 'blocks'`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newReservedIDTestStore(t)

			_, err := s.db.ExecContext(ctx, tc.stmt)
			require.NoError(t, err)

			highest, err := s.highestIssuedBlockID(ctx)
			require.NoError(t, err)
			require.Zero(t, highest)

			_, _, err = s.StoreBlock(ctx, block1, "", options.WithID(1))
			require.Error(t, err)
			require.Contains(t, err.Error(), "has only issued up to 0")
			requireNoBlockRow(t, s, tc.name)
		})
	}
}

func TestHighestIssuedBlockID_ClosedDB(t *testing.T) {
	s := newReservedIDTestStore(t)
	require.NoError(t, s.db.Close()) // the cleanup still closes the store once

	_, err := s.highestIssuedBlockID(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read the highest issued block id")
}

func TestHashString(t *testing.T) {
	h := chainhash.HashH([]byte("hash-string"))
	require.Equal(t, h.String(), hashString(h[:]))

	// A column that is not 32 bytes cannot be a hash, so it is shown as raw hex.
	require.Equal(t, "0a0b0c", hashString([]byte{0x0a, 0x0b, 0x0c}))
}
