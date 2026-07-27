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

// TestArbiterCreateSpendRoundTrip is the core loop: an output created by Create must be
// spendable exactly once, must hand back its satoshis and locking script on the way out
// (that RETURNING clause is what replaces PreviousOutputsDecorate), and a second attempt
// must be rejected by ABSENCE rather than by consulting a spent-set.
func TestArbiterCreateSpendRoundTrip(t *testing.T) {
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

	// the decorate fetch: Spend must have populated the input from the arbiter row,
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

// TestArbiterUnspendableOutputsCreateNoRow pins the rule that keeps the arbiter's size
// bounded: an output that can never be spent must never occupy a row, because a row
// nothing can ever delete would sit in the budget forever.
func TestArbiterUnspendableOutputsCreateNoRow(t *testing.T) {
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
	require.Equal(t, 1, rows, "only the spendable output may occupy an arbiter row")
}

// TestDecorateFromArbiter answers the question directly: can PreviousOutputsDecorate be
// served from the UTXO table alone?
//
// Today it cannot -- the postgres store fetches the parent's raw_tx (~1.7 KB) and runs
// bt.NewTxFromBytes over it to pull out two fields. Here those two fields ARE the row,
// so decorate is an index probe. Note this store holds no transaction bodies at all, so
// if the input comes back decorated it can only have come from the arbiter.
func TestDecorateFromArbiter(t *testing.T) {
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
		"satoshis must come from the arbiter row")
	require.NotNil(t, child.Inputs[0].PreviousTxScript,
		"locking script must come from the arbiter row")
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
		"a spent parent has no arbiter row and decorate must not fabricate one")
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
