package utxoset

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// testDSN points at a throwaway postgres. Overridable so CI and a developer's local
// instance can both drive it.
var testDSN = envOr("UTXOSET_TEST_DSN", "postgres://postgres@localhost:5441/soak?sslmode=disable")

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}

	return def
}

func newTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Skipf("skipping: cannot reach postgres: %v", err)
	}

	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skipping: cannot reach postgres: %v", err)
	}

	// clean slate
	_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS utxo CASCADE;
	                       DROP TABLE IF EXISTS spend_journal CASCADE;
	                       DROP TABLE IF EXISTS applied_block CASCADE;
	                       DROP TABLE IF EXISTS applied_chunk CASCADE;`)
	pool.Close()

	u, err := url.Parse(testDSN)
	require.NoError(t, err)

	tSettings := settings.NewSettings()

	s, err := New(ctx, ulogger.TestLogger{}, tSettings, u)
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close(ctx) })

	return s, ctx
}

// mkTx builds a transaction with n spendable P2PKH-ish outputs.
func mkTx(t *testing.T, nOut int, sats uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	// a non-coinbase input so IsCoinbase() is false
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0,
		"76a914000000000000000000000000000000000000000088ac", 100000))

	for i := 0; i < nOut; i++ {
		script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
		require.NoError(t, err)
		tx.AddOutput(&bt.Output{Satoshis: sats, LockingScript: script})
	}

	return tx
}

// TestUTXOTableCreateSpendRoundTrip is the core loop: an output created by Create must be
// spendable exactly once, must hand back its satoshis and locking script on the way out
// (that RETURNING clause is what replaces PreviousOutputsDecorate), and a second attempt
// must be rejected by ABSENCE rather than by consulting a spent-set.
func TestUTXOTableCreateSpendRoundTrip(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	// a child spending parent output 0
	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentHash,
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))

	spends, err := s.Spend(ctx, child, 200)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err, "first spend must succeed")

	// the decorate fetch: Spend must have populated the input from the UTXO row,
	// so script validation needs no parent lookup at all
	require.Equal(t, parent.Outputs[0].Satoshis, child.Inputs[0].PreviousTxSatoshis,
		"Spend must return the satoshis via RETURNING")
	require.NotNil(t, child.Inputs[0].PreviousTxScript,
		"Spend must return the locking script via RETURNING")

	// spending it again: the row is gone, and absence IS the rejection
	child2 := bt.NewTx()
	require.NoError(t, child2.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentHash,
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))

	spends2, err := s.Spend(ctx, child2, 201)
	require.NoError(t, err)
	require.Len(t, spends2, 1)
	require.Error(t, spends2[0].Err, "double spend must be rejected")
	require.True(t, errors.Is(spends2[0].Err, errors.ErrSpent),
		"rejection must be ErrSpent, got %v", spends2[0].Err)

	// the sibling output must be untouched by all of this
	var remaining int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`,
		parentHash[:]).Scan(&remaining))
	require.Equal(t, 1, remaining, "output 1 must survive its sibling being spent")
}

// TestUTXOTableUnspendableOutputsCreateNoRow pins the rule that keeps the UTXO table's size
// bounded: an output that can never be spent must never occupy a row, because a row
// nothing can ever delete would sit in the budget forever.
func TestUTXOTableUnspendableOutputsCreateNoRow(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000002", 0,
		"76a914000000000000000000000000000000000000000088ac", 100000))

	// one spendable output, one provably-unspendable OP_RETURN data carrier
	spendableScript, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)
	tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: spendableScript})

	opReturn, err := bscript.NewFromHexString("006a0568656c6c6f")
	require.NoError(t, err)
	tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: opReturn})

	_, err = s.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	var rows int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`,
		txHash[:]).Scan(&rows))
	require.Equal(t, 1, rows, "only the spendable output may occupy a UTXO row")
}

// TestDecorateFromUTXOTable answers the question directly: can PreviousOutputsDecorate be
// served from the UTXO table alone?
//
// Today it cannot -- the postgres store fetches the parent's raw_tx (~1.7 KB) and runs
// bt.NewTxFromBytes over it to pull out two fields. Here those two fields ARE the row,
// so decorate is an index probe. Note this store holds no transaction bodies at all, so
// if the input comes back decorated it can only have come from the UTXO table.
func TestDecorateFromUTXOTable(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 7777)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))

	// strip what FromUTXOs pre-filled, so decorate has to do the work
	child.Inputs[0].PreviousTxScript = nil
	child.Inputs[0].PreviousTxSatoshis = 0

	require.NoError(t, s.PreviousOutputsDecorate(ctx, child))

	require.Equal(t, uint64(7777), child.Inputs[0].PreviousTxSatoshis,
		"satoshis must come from the UTXO row")
	require.NotNil(t, child.Inputs[0].PreviousTxScript,
		"locking script must come from the UTXO row")
	require.Equal(t, parent.Outputs[0].LockingScript.String(), child.Inputs[0].PreviousTxScript.String(),
		"the decorated script must be the parent's actual locking script")

	// and once spent, the row is gone -- decorate must say so rather than inventing an answer
	spent := bt.NewTx()
	require.NoError(t, spent.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))
	_, err = s.Spend(ctx, spent, 200)
	require.NoError(t, err)

	orphan := bt.NewTx()
	require.NoError(t, orphan.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))
	orphan.Inputs[0].PreviousTxScript = nil

	require.Error(t, s.PreviousOutputsDecorate(ctx, orphan),
		"a spent parent has no UTXO row and decorate must not fabricate one")
}

// TestSpendAndCreateAtomic covers the contract PR 1326 introduces, and the property that
// makes this store's implementation different from the sequential helper: spend and
// create share ONE transaction, so there is no window in which the inputs are spent and
// the outputs missing, and no compensating Unspend on failure.
func TestSpendAndCreateAtomic(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 9000)
	_, _, err := s.SpendAndCreate(ctx, parent, 100, utxo.WithCreateOnly())
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&n))
	require.Equal(t, 1, n, "WithCreateOnly must create outputs")

	// a child spending the parent and creating its own outputs, in one call
	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parentHash, Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)
	child.AddOutput(&bt.Output{Satoshis: 8000, LockingScript: script})

	_, spends, err := s.SpendAndCreate(ctx, child, 200)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err)

	// both halves must have landed: the parent output consumed AND the child's created
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&n))
	require.Equal(t, 0, n, "the spent parent output must be gone")

	childHash := child.TxIDChainHash()
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, childHash[:]).Scan(&n))
	require.Equal(t, 1, n, "the child's output must exist")
}

// TestSpendAndCreateRejectsContradictoryOptions pins the one invalid combination.
func TestSpendAndCreateRejectsContradictoryOptions(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 100)

	_, _, err := s.SpendAndCreate(ctx, tx, 100, utxo.WithCreateOnly(), utxo.WithSpendOnly())
	require.Error(t, err, "WithCreateOnly and WithSpendOnly are mutually exclusive")
}

// TestSpendWritesJournal is the property that makes a delete-on-spend store
// recoverable at all: the coin's payload must be captured at the instant it is
// destroyed, in the same statement, or a reorg and ProcessConflicting have nothing to
// restore from. It cannot be re-derived -- the node keeps almost no blocks, and the
// subtree data it does keep carries outpoints without satoshis or scripts.
func TestSpendWritesJournal(t *testing.T) {
	s, ctx := newTestStore(t)

	require.True(t, s.JournalEnabled(), "the journal must default to ON: an irreversible store should be a deliberate choice, never an accident")

	parent := mkTx(t, 1, 4242)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parentHash, Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	_, err = s.Spend(ctx, child, 200)
	require.NoError(t, err)

	// the UTXO row is gone...
	var live int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&live))
	require.Equal(t, 0, live)

	// ...and everything needed to put it back is in the journal, with the spender
	// recorded as the ownership token
	var (
		sats      int64
		script    []byte
		spender   []byte
		spentAt   int32
		createdAt int32
	)
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT satoshis, script, spending_txid, spent_height, created_height
		   FROM spend_journal WHERE txid = $1`, parentHash[:]).
		Scan(&sats, &script, &spender, &spentAt, &createdAt))

	childHash := child.TxIDChainHash()
	require.Equal(t, int64(4242), sats, "the journal must carry the satoshis")
	require.Equal(t, []byte(*parent.Outputs[0].LockingScript), script, "and the locking script")
	require.Equal(t, childHash[:], spender, "and the spender, as the restore ownership token")
	require.Equal(t, int32(200), spentAt)
	require.Equal(t, int32(100), createdAt, "created_height must survive, for BIP68 and maturity")
}

// TestSyncModeSkipsJournal covers the one condition under which an irreversible spend is
// safe: below the checkpoint a reorg is impossible by rule and there is no mempool, so
// nothing can ask to undo. It has to be asked for explicitly.
func TestSyncModeSkipsJournal(t *testing.T) {
	s, ctx := newTestStore(t)

	s.SetSyncMode(true)
	require.False(t, s.JournalEnabled())

	parent := mkTx(t, 1, 555)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	_, err = s.Spend(ctx, child, 200)
	require.NoError(t, err)

	var journalled int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM spend_journal`).Scan(&journalled))
	require.Equal(t, 0, journalled, "sync mode must write no journal rows")
}

// TestSpendJournalReclaim proves the leaf count is BOUNDED rather than growing forever.
// Reclaim was written before this test and never wired up, so leaves accumulated
// indefinitely -- the failure this pins.
func TestSpendJournalReclaim(t *testing.T) {
	s, ctx := newTestStore(t)

	s.journalRetention = 96 // 2 leaves, so the test does not need 1440 blocks

	// spend across a span wide enough to roll several leaves over
	for h := uint32(100); h <= 500; h += 40 {
		parent := mkTx(t, 1, uint64(1000+h))
		_, err := s.Create(ctx, parent, h)
		require.NoError(t, err)

		child := bt.NewTx()
		require.NoError(t, child.FromUTXOs(&bt.UTXO{
			TxIDHash: parent.TxIDChainHash(), Vout: 0,
			LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
		}))

		_, err = s.Spend(ctx, child, h)
		require.NoError(t, err)
	}

	var leaves int
	require.NoError(t, s.pool.QueryRow(ctx, `
        SELECT count(*) FROM pg_class c
          JOIN pg_inherits i ON i.inhrelid = c.oid
          JOIN pg_class p ON p.oid = i.inhparent
         WHERE p.relname = 'spend_journal'`).Scan(&leaves))

	// retention 96 / 48 per leaf = 2, plus the one being filled, plus at most one
	// not yet crossed. The point is that it is bounded, not that it is exact.
	require.LessOrEqual(t, leaves, 4,
		"journal leaves must be reclaimed as the chain advances, not accumulate")
	require.Positive(t, leaves, "recent history must still be retained")
}

// spendOne creates a parent, spends output 0, and returns the Spend record describing it.
func spendOne(t *testing.T, s *Store, ctx context.Context, sats uint64, h uint32) (*bt.Tx, *bt.Tx, []*utxo.Spend) {
	t.Helper()

	parent := mkTx(t, 1, sats)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	spends, err := s.Spend(ctx, child, h)
	require.NoError(t, err)
	require.Len(t, spends, 1)

	// the caller is expected to know who spent it; Spend does not fill this in
	spends[0].SpendingData = spend.NewSpendingData(child.TxIDChainHash(), 0)

	return parent, child, spends
}

// TestUnspendRestoresFromJournal is the round trip that makes a reorg survivable: a coin
// destroyed by a spend must come back byte-identical, and the journal row must be
// CONSUMED so a second restore cannot duplicate it.
func TestUnspendRestoresFromJournal(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, _, spends := spendOne(t, s, ctx, 3131, 200)
	parentHash := parent.TxIDChainHash()

	require.NoError(t, s.Unspend(ctx, spends))

	// restored byte-identical
	var (
		sats    int64
		script  []byte
		created int32
	)
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT satoshis, script, created_height FROM utxo WHERE txid = $1`, parentHash[:]).
		Scan(&sats, &script, &created))
	require.Equal(t, int64(3131), sats)
	require.Equal(t, []byte(*parent.Outputs[0].LockingScript), script)
	require.Equal(t, int32(100), created, "created_height must survive the round trip, for BIP68 and maturity")

	// the journal row is consumed: the restore is single-use, which is what makes it
	// idempotent without a counter
	var remaining int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM spend_journal WHERE txid = $1`, parentHash[:]).Scan(&remaining))
	require.Equal(t, 0, remaining, "the journal row must be consumed by the restore")

	// so a second restore finds nothing and must say so rather than silently succeeding
	require.Error(t, s.Unspend(ctx, spends), "a second restore must not duplicate the coin")

	var live int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&live))
	require.Equal(t, 1, live, "exactly one live row after a double restore attempt")
}

// TestUnspendRefusesWrongSpender is the ownership token doing its job. A stale reorg
// record must never resurrect a coin that a DIFFERENT transaction has since taken.
func TestUnspendRefusesWrongSpender(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, _, spends := spendOne(t, s, ctx, 4141, 200)

	// claim it was spent by some other transaction
	other := mkTx(t, 1, 1)
	spends[0].SpendingData = spend.NewSpendingData(other.TxIDChainHash(), 0)

	require.Error(t, s.Unspend(ctx, spends),
		"a restore naming the wrong spender must fail, not resurrect the coin")

	parentHash := parent.TxIDChainHash()

	var live int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&live))
	require.Equal(t, 0, live, "the coin must stay spent")

	var journalled int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM spend_journal WHERE txid = $1`, parentHash[:]).Scan(&journalled))
	require.Equal(t, 1, journalled, "and its journal row must be left intact for the rightful restore")
}

// TestUnspendRequiresSpender refuses to guess. Restoring on the outpoint alone could
// resurrect a coin a different transaction now owns.
func TestUnspendRequiresSpender(t *testing.T) {
	s, ctx := newTestStore(t)

	_, _, spends := spendOne(t, s, ctx, 5151, 200)
	spends[0].SpendingData = nil

	require.Error(t, s.Unspend(ctx, spends), "no ownership token means no restore")
}

// TestSpendBelowFirstJournalLeafCreatesPartition pins a bug that only ever appears on a
// fresh sync from genesis, which is exactly the workload this store exists for.
//
// The partition cache used to hold the leaf number itself, so its zero value was
// indistinguishable from "leaf 0 has already been created". Every spend below height 48
// therefore saw a cache hit, skipped the CREATE TABLE, and failed the journal insert with
// "no partition of relation spend_journal found for row". Higher leaves worked fine, so
// every test that happened to pick a height above 48 passed and the store looked healthy.
func TestSpendBelowFirstJournalLeafCreatesPartition(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 7777)
	_, err := s.Create(ctx, parent, 1)
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parentHash, Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	// Height 2 lands in leaf 0 -- the leaf the zero value shadowed.
	_, err = s.Spend(ctx, child, 2)
	require.NoError(t, err, "a spend in the first journal leaf must create its partition")

	var journaled int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM spend_journal_0 WHERE txid = $1`, parentHash[:]).Scan(&journaled))
	require.Equal(t, 1, journaled)
}

// TestCreateDoesNotGateOrdinaryOutputsOnHeight guards against re-inventing a consensus
// rule that does not exist.
//
// spendable_from used to be set to the creation height for every output, which quietly
// meant "an output may not be spent below the height it was created at". Nothing in
// consensus says that -- only coinbase maturity and an explicit reassignment delay hold
// an output back -- and it would reject valid spends during a reorg or whenever a caller
// passes a height that is not strictly increasing.
func TestCreateDoesNotGateOrdinaryOutputsOnHeight(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5150)
	_, err := s.Create(ctx, parent, 1000)
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parentHash, Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))

	// Spent far below the height it was created at.
	//
	// Assert on the Spend records, not just the returned error: a miss is reported per
	// input on spends[i].Err and classifyMisses can leave the top-level error nil, so
	// require.NoError alone would sail straight past the bug this test exists for.
	spends, err := s.Spend(ctx, child, 1)
	require.NoError(t, err)
	require.Len(t, spends, 1)
	require.NoError(t, spends[0].Err, "an ordinary output must not be gated on its own creation height")

	var live int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM utxo WHERE txid = $1`, parentHash[:]).Scan(&live))
	require.Equal(t, 0, live, "the coin must actually be gone, not merely reported as spent")
}
