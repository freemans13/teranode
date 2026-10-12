package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// These are the WithBelowCheckpoint contract tests: the list is written in two
// waves, every create and then every spend, and ends in the same store state
// as the default order; a create that fails stops its descendants' spends; a
// record that already exists has its spends made again; and a parent output
// that exists nowhere fails the spend instead of passing on the strength of
// the record the create wave just wrote.

// waveCall is one SpendAndCreate call DefaultSpendAndCreateMulti made, with the
// half it asked for.
type waveCall struct {
	tx                    *bt.Tx
	createOnly, spendOnly bool
	start, end            time.Time
}

// waveRecorder wraps a real store and records every SpendAndCreate call. delay
// holds each call before it reaches the store, so calls that can overlap
// visibly do. failCreate, when set, fails that transaction's create-only call
// before it reaches the store.
type waveRecorder struct {
	utxostore.Store
	delay      time.Duration
	failCreate *bt.Tx

	mu    sync.Mutex
	calls []waveCall
}

func (w *waveRecorder) SpendAndCreate(ctx context.Context, tx *bt.Tx, h uint32, opts ...utxostore.CreateOption) (*meta.Data, []*utxostore.Spend, error) {
	options, err := utxostore.ParseCreateOptions(opts...)
	if err != nil {
		return nil, nil, err
	}

	start := time.Now()

	if w.delay > 0 {
		time.Sleep(w.delay)
	}

	var (
		md     *meta.Data
		spends []*utxostore.Spend
	)

	if w.failCreate != nil && options.CreateOnly && *tx.TxIDChainHash() == *w.failCreate.TxIDChainHash() {
		err = errors.NewStorageError("injected create failure")
	} else {
		md, spends, err = w.Store.SpendAndCreate(ctx, tx, h, opts...)
	}

	w.mu.Lock()
	w.calls = append(w.calls, waveCall{tx: tx, createOnly: options.CreateOnly, spendOnly: options.SpendOnly, start: start, end: time.Now()})
	w.mu.Unlock()

	return md, spends, err
}

func (w *waveRecorder) recorded() []waveCall {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]waveCall(nil), w.calls...)
}

// SpendAndCreateMultiBelowCheckpointMatchesLoop: a WithBelowCheckpoint list
// leaves every record and every spend exactly as a loop of SpendAndCreate
// does, before and after the record-mined write.
func SpendAndCreateMultiBelowCheckpointMatchesLoop(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 800

	loop := BuildMultiWorkload(t, 0xa0, 5, 7)
	multi := BuildMultiWorkload(t, 0xa1, 5, 7)

	loop.StoreRoots(t, db, height-1)
	multi.StoreRoots(t, db, height-1)

	loopSpendAndCreate(t, db, loop.Txs, height, utxostore.WithIgnoreLocked(true))

	results, err := db.SpendAndCreateMulti(ctx, multi.Txs, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, len(multi.Txs))
	requireAllStatus(t, results, utxostore.MultiTxCreated)

	for i, r := range results {
		require.NotNil(t, r.Meta, "tx %d", i)
		require.NotEmpty(t, r.Spends, "tx %d", i)
	}

	require.Equal(t, loop.Records(t, db), multi.Records(t, db), "records before the record-mined write")
	require.Equal(t, loop.RootSpends(t, db), multi.RootSpends(t, db))

	loop.MarkMined(t, db, 902, height)
	multi.MarkMined(t, db, 902, height)

	require.Equal(t, loop.Records(t, db), multi.Records(t, db), "records after the record-mined write")
}

// SpendAndCreateMultiBelowCheckpointTwoWaves: every transaction gets one
// create-only call and one spend-only call; every create returns before any
// spend starts; and the creates of all five levels run together, as do the
// spends, so the depth of the list does not add rounds.
func SpendAndCreateMultiBelowCheckpointTwoWaves(t *testing.T, db utxostore.Store) {
	const (
		height = 810
		levels = 5
		width  = 3
	)

	w := BuildMultiWorkload(t, 0xa2, levels, width)
	w.StoreRoots(t, db, height-1)

	rec := &waveRecorder{Store: db, delay: 20 * time.Millisecond}

	results, err := utxostore.DefaultSpendAndCreateMulti(context.Background(), rec, utxostore.UniformSpendAndCreateMultiWidths(len(w.Txs)),
		w.Txs, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	requireAllStatus(t, results, utxostore.MultiTxCreated)

	calls := rec.recorded()
	require.Len(t, calls, 2*len(w.Txs), "one create and one spend per transaction")

	var creates, spends []waveCall

	for _, c := range calls {
		require.True(t, c.createOnly != c.spendOnly, "every call is one half: createOnly=%v spendOnly=%v", c.createOnly, c.spendOnly)

		if c.createOnly {
			creates = append(creates, c)
		} else {
			spends = append(spends, c)
		}
	}

	require.Len(t, creates, len(w.Txs))
	require.Len(t, spends, len(w.Txs))

	for _, s := range spends {
		for _, c := range creates {
			require.False(t, s.start.Before(c.end), "a spend started before every create had returned")
		}
	}

	requireOneRound(t, "create", creates)
	requireOneRound(t, "spend", spends)

	for i, r := range w.Records(t, db) {
		require.True(t, r.ExistsInStore, "tx %d", i)
	}

	// The deepest link of the first chain is spent by its child.
	requireSpentBy(t, db, w.Txs[(levels-2)*width], 0, w.Txs[(levels-1)*width], 0)
}

// requireOneRound checks that every call overlapped every other, so they ran as
// one round rather than one after another.
func requireOneRound(t *testing.T, wave string, calls []waveCall) {
	t.Helper()

	var latestStart, earliestEnd time.Time

	for i, c := range calls {
		if i == 0 || c.start.After(latestStart) {
			latestStart = c.start
		}

		if i == 0 || c.end.Before(earliestEnd) {
			earliestEnd = c.end
		}
	}

	require.True(t, latestStart.Before(earliestEnd), "the %s wave did not run as one round", wave)
}

// SpendAndCreateMultiBelowCheckpointCreateFails: a transaction whose create
// fails is Failed, its child and grandchild are ParentFailed and never spend,
// and an unrelated transaction is written and spent.
func SpendAndCreateMultiBelowCheckpointCreateFails(t *testing.T, db utxostore.Store) {
	const height = 820

	w := BuildMultiWorkload(t, 0xa3, 0, 2)
	w.StoreRoots(t, db, height-1)

	a := multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1)
	b := multiSpend(t, 2, []*bt.Tx{a}, []uint32{0}, 1)
	c := multiSpend(t, 3, []*bt.Tx{b}, []uint32{0}, 1)
	d := multiSpend(t, 4, []*bt.Tx{w.Roots[1]}, []uint32{0}, 1)

	rec := &waveRecorder{Store: db, failCreate: a}

	results, err := utxostore.DefaultSpendAndCreateMulti(context.Background(), rec, utxostore.UniformSpendAndCreateMultiWidths(4),
		[]*bt.Tx{a, b, c, d}, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, 4)

	require.Equal(t, utxostore.MultiTxFailed, results[0].Status)
	require.Error(t, results[0].Err)
	require.Equal(t, utxostore.MultiTxParentFailed, results[1].Status)
	require.Error(t, results[1].Err)
	require.Equal(t, utxostore.MultiTxParentFailed, results[2].Status)
	require.Error(t, results[2].Err)
	require.Equal(t, utxostore.MultiTxCreated, results[3].Status, "%v", results[3].Err)

	for _, call := range rec.recorded() {
		if call.spendOnly {
			require.Equal(t, *d.TxIDChainHash(), *call.tx.TxIDChainHash(), "only the unrelated transaction spends")
		}
	}

	requireNotStored(t, db, a)
	requireUnspent(t, db, w.Roots[0], 0)
	requireSpentBy(t, db, w.Roots[1], 0, d, 0)
}

// SpendAndCreateMultiBelowCheckpointExistingRespent: a record created by an
// earlier attempt that never reached its spends, as a crash between the waves
// leaves it, is Existed, and its inputs are spent by it afterwards; its child
// in the list is written and spends it.
func SpendAndCreateMultiBelowCheckpointExistingRespent(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 830

	w := BuildMultiWorkload(t, 0xa4, 0, 1)
	w.StoreRoots(t, db, height-1)

	a := multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1)
	b := multiSpend(t, 2, []*bt.Tx{a}, []uint32{0}, 1)

	_, _, err := db.SpendAndCreate(ctx, a, height, utxostore.WithCreateOnly())
	require.NoError(t, err)
	requireUnspent(t, db, w.Roots[0], 0)

	results, err := db.SpendAndCreateMulti(ctx, []*bt.Tx{a, b}, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, utxostore.MultiTxExisted, results[0].Status, "%v", results[0].Err)
	require.NotEmpty(t, results[0].Spends, "an existing record's spends are made")
	require.Equal(t, utxostore.MultiTxCreated, results[1].Status, "%v", results[1].Err)

	requireSpentBy(t, db, w.Roots[0], 0, a, 0)
	requireSpentBy(t, db, a, 0, b, 0)
}

// SpendAndCreateMultiBelowCheckpointMissingParent: a transaction whose parent
// is in neither the list nor the store is Failed with ErrTxNotFound. The
// create wave has already written its record, and the store must not take that
// record as proof the spend was made in an earlier attempt.
func SpendAndCreateMultiBelowCheckpointMissingParent(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	const height = 840

	w := BuildMultiWorkload(t, 0xa5, 0, 1) // roots never stored

	orphan := multiSpend(t, 1, []*bt.Tx{w.Roots[0]}, []uint32{0}, 1)

	results, err := db.SpendAndCreateMulti(ctx, []*bt.Tx{orphan}, height, utxostore.WithBelowCheckpoint(), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, utxostore.MultiTxFailed, results[0].Status, "a spend of an output that exists nowhere must fail")
	require.ErrorIs(t, results[0].Err, errors.ErrTxNotFound)

	_, err = db.Get(ctx, orphan.TxIDChainHash(), fields.Fee)
	require.NoError(t, err, "the create wave wrote the orphan's record before its spend failed")
}

// requireUnspent checks that output vout of parent has no spender.
func requireUnspent(t testing.TB, db utxostore.Store, parent *bt.Tx, vout uint32) {
	t.Helper()

	utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[vout], vout)
	require.NoError(t, err)

	resp, err := db.GetSpend(context.Background(), &utxostore.Spend{TxID: parent.TxIDChainHash(), Vout: vout, UTXOHash: utxoHash})
	require.NoError(t, err)
	require.Nil(t, resp.SpendingData, "%s:%d is spent", parent.TxIDChainHash(), vout)
}
