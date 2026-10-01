package utxo

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// multiFakeStore is an in-memory SpendAndCreateMultiStore that records every
// SpendAndCreate call's start and end, and lets a test fail chosen txids.
type multiFakeStore struct {
	mu sync.Mutex

	existing map[chainhash.Hash]*meta.Data // what BatchDecorate reports
	failWith map[chainhash.Hash]error      // SpendAndCreate returns this error
	spendsOn map[chainhash.Hash][]*Spend   // and these spends with it
	decorErr error                         // BatchDecorate fails the whole call
	delay    time.Duration

	calls       []multiCall
	decorations int
	decorFields [][]fields.FieldName
	seenOpts    []*CreateOptions
}

type multiCall struct {
	txid       chainhash.Hash
	start, end time.Time
}

func (f *multiFakeStore) SpendAndCreate(_ context.Context, tx *bt.Tx, _ uint32, opts ...CreateOption) (*meta.Data, []*Spend, error) {
	start := time.Now()

	options, err := ParseCreateOptions(opts...)
	if err != nil {
		return nil, nil, err
	}

	txid := *tx.TxIDChainHash()
	if options.TxID != nil {
		txid = *options.TxID
	}

	if f.delay > 0 {
		time.Sleep(f.delay)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, multiCall{txid: txid, start: start, end: time.Now()})
	f.seenOpts = append(f.seenOpts, options)

	if e, ok := f.failWith[txid]; ok {
		return nil, f.spendsOn[txid], e
	}

	return &meta.Data{Tx: tx, Fee: 1}, []*Spend{{TxID: &txid}}, nil
}

func (f *multiFakeStore) BatchDecorate(_ context.Context, items []*UnresolvedMetaData, fs ...fields.FieldName) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.decorations++
	f.decorFields = append(f.decorFields, fs)

	if f.decorErr != nil {
		return f.decorErr
	}

	for _, item := range items {
		if d, ok := f.existing[item.Hash]; ok {
			item.Data = d
			continue
		}

		item.Err = errors.NewTxNotFoundError("not found")
	}

	return nil
}

func (f *multiFakeStore) callFor(txid chainhash.Hash) (multiCall, bool) {
	for _, c := range f.calls {
		if c.txid == txid {
			return c, true
		}
	}

	return multiCall{}, false
}

// multiTx builds a transaction spending the given outpoints, with nOutputs
// outputs. Each call with a distinct seed gives a distinct txid.
func multiTx(t *testing.T, seed uint32, nOutputs int, spends ...Outpoint) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.LockTime = seed

	for _, op := range spends {
		in := &bt.Input{PreviousTxOutIndex: op.Vout, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
		txid := op.TxID
		require.NoError(t, in.PreviousTxIDAdd(&txid))
		tx.Inputs = append(tx.Inputs, in)
	}

	for i := 0; i < nOutputs; i++ {
		tx.AddOutput(&bt.Output{Satoshis: uint64(1000 + i), LockingScript: bscript.NewFromBytes([]byte{0x51})})
	}

	return tx
}

func outsideOutpoint(n byte) Outpoint {
	return Outpoint{TxID: chainhash.Hash{0xee, n}, Vout: 0}
}

func outOf(tx *bt.Tx, vout uint32) Outpoint {
	return Outpoint{TxID: *tx.TxIDChainHash(), Vout: vout}
}

func TestDefaultSpendAndCreateMulti_Refusals(t *testing.T) {
	ctx := context.Background()

	parent := multiTx(t, 1, 2, outsideOutpoint(1))
	child := multiTx(t, 2, 1, outOf(parent, 0))

	cases := []struct {
		name string
		txs  []*bt.Tx
		opts []CreateOption
	}{
		{"child before parent", []*bt.Tx{child, parent}, nil},
		{"duplicate outpoint", []*bt.Tx{multiTx(t, 3, 1, outsideOutpoint(2)), multiTx(t, 4, 1, outsideOutpoint(2))}, nil},
		{"output index past a parent in the list", []*bt.Tx{parent, multiTx(t, 5, 1, outOf(parent, 2))}, nil},
		{"the same transaction twice", []*bt.Tx{parent, parent}, nil},
		{"a coinbase", []*bt.Tx{multiTx(t, 6, 1, Outpoint{Vout: 0xffffffff})}, nil},
		{"WithTXID", []*bt.Tx{parent}, []CreateOption{WithTXID(parent.TxIDChainHash())}},
		{"WithSetCoinbase", []*bt.Tx{parent}, []CreateOption{WithSetCoinbase(false)}},
		{"WithTXIDs of the wrong length", []*bt.Tx{parent, child}, []CreateOption{WithTXIDs([]chainhash.Hash{*parent.TxIDChainHash()})}},
		{"WithCreateOnly and WithSpendOnly", []*bt.Tx{parent}, []CreateOption{WithCreateOnly(), WithSpendOnly()}},
		{"WithCreateOnly", []*bt.Tx{parent}, []CreateOption{WithCreateOnly()}},
		{"WithSpendOnly", []*bt.Tx{parent}, []CreateOption{WithSpendOnly()}},
		{"a nil transaction", []*bt.Tx{parent, nil}, nil},
		{"a nil transaction with WithTXIDs", []*bt.Tx{nil}, []CreateOption{WithTXIDs([]chainhash.Hash{{0x01}})}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &multiFakeStore{}

			results, err := DefaultSpendAndCreateMulti(ctx, store, 8, tc.txs, 100, tc.opts...)
			require.Error(t, err)
			require.True(t, IsSpendAndCreateMultiRefused(err), "want a refusal, got %v", err)
			require.NotContains(t, err.Error(), "%v", "the refusal message must be fully formatted")
			require.NotContains(t, err.Error(), "%!", "the refusal message must be fully formatted")
			require.Nil(t, results)
			require.Empty(t, store.calls, "nothing may be written on a refusal")
		})
	}
}

func TestDefaultSpendAndCreateMulti_EmptyList(t *testing.T) {
	store := &multiFakeStore{}

	results, err := DefaultSpendAndCreateMulti(context.Background(), store, 8, nil, 100)
	require.NoError(t, err)
	require.Empty(t, results)
}

// Test 13 of the design: levels run in parallel and in order. Every call of a
// level starts before any of that level ends (they overlap), and no call starts
// before every call of the previous level has returned.
func TestDefaultSpendAndCreateMulti_LevelsParallelAndOrdered(t *testing.T) {
	ctx := context.Background()

	const width = 6

	var (
		txs    []*bt.Tx
		levels [][]*bt.Tx
	)

	prev := make([]*bt.Tx, width)

	for level := 0; level < 4; level++ {
		var row []*bt.Tx

		for i := 0; i < width; i++ {
			var op Outpoint
			if level == 0 {
				op = outsideOutpoint(byte(i))
			} else {
				op = outOf(prev[i], 0)
			}

			tx := multiTx(t, uint32(level*100+i), 1, op)
			row = append(row, tx)
			txs = append(txs, tx)
		}

		levels = append(levels, row)
		prev = row
	}

	store := &multiFakeStore{delay: 20 * time.Millisecond}

	results, err := DefaultSpendAndCreateMulti(ctx, store, width, txs, 100)
	require.NoError(t, err)
	require.Len(t, results, len(txs))

	for i, r := range results {
		require.Equal(t, MultiTxCreated, r.Status, "tx %d", i)
		require.NoError(t, r.Err)
		require.NotNil(t, r.Meta)
	}

	for l, row := range levels {
		var latestStart, earliestEnd time.Time

		for i, tx := range row {
			c, ok := store.callFor(*tx.TxIDChainHash())
			require.True(t, ok)

			if i == 0 || c.start.After(latestStart) {
				latestStart = c.start
			}

			if i == 0 || c.end.Before(earliestEnd) {
				earliestEnd = c.end
			}

			if l > 0 {
				for _, p := range levels[l-1] {
					pc, _ := store.callFor(*p.TxIDChainHash())
					require.False(t, c.start.Before(pc.end), "level %d call started before a level %d call returned", l, l-1)
				}
			}
		}

		require.True(t, latestStart.Before(earliestEnd), "level %d calls did not overlap: it ran as a loop", l)
	}
}

// The concurrency bound holds within a level.
func TestDefaultSpendAndCreateMulti_ConcurrencyBound(t *testing.T) {
	var txs []*bt.Tx
	for i := 0; i < 12; i++ {
		txs = append(txs, multiTx(t, uint32(i), 1, outsideOutpoint(byte(i))))
	}

	store := &multiFakeStore{delay: 10 * time.Millisecond}

	_, err := DefaultSpendAndCreateMulti(context.Background(), store, 3, txs, 100)
	require.NoError(t, err)

	for _, c := range store.calls {
		inFlight := 0

		for _, o := range store.calls {
			if !o.start.After(c.start) && o.end.After(c.start) {
				inFlight++
			}
		}

		require.LessOrEqual(t, inFlight, 3)
	}
}

func TestDefaultSpendAndCreateMulti_ResultMapping(t *testing.T) {
	ctx := context.Background()

	a := multiTx(t, 1, 2, outsideOutpoint(1)) // fails
	b := multiTx(t, 2, 1, outOf(a, 0))        // child of a: parent failed
	c := multiTx(t, 3, 1, outOf(b, 0))        // grandchild: parent failed
	d := multiTx(t, 4, 1, outsideOutpoint(2)) // ErrTxExists at create: existed, spends live
	e := multiTx(t, 5, 1, outOf(d, 0))        // child of an existed record: attempted
	f := multiTx(t, 6, 1, outsideOutpoint(3)) // created

	spentErr := errors.NewUtxoSpentError(*a.TxIDChainHash(), 0, chainhash.Hash{}, nil)
	failSpends := []*Spend{{TxID: a.TxIDChainHash(), Err: spentErr}}
	existSpends := []*Spend{{TxID: d.TxIDChainHash()}}

	store := &multiFakeStore{
		failWith: map[chainhash.Hash]error{
			*a.TxIDChainHash(): spentErr,
			*d.TxIDChainHash(): errors.NewTxExistsError("exists"),
		},
		spendsOn: map[chainhash.Hash][]*Spend{
			*a.TxIDChainHash(): failSpends,
			*d.TxIDChainHash(): existSpends,
		},
	}

	results, err := DefaultSpendAndCreateMulti(ctx, store, 4, []*bt.Tx{a, b, c, d, e, f}, 100)
	require.NoError(t, err)
	require.Len(t, results, 6)

	require.Equal(t, MultiTxFailed, results[0].Status)
	require.ErrorIs(t, results[0].Err, errors.ErrSpent)
	require.Equal(t, failSpends, results[0].Spends)

	require.Equal(t, MultiTxParentFailed, results[1].Status)
	require.Error(t, results[1].Err)
	require.Equal(t, MultiTxParentFailed, results[2].Status)

	require.Equal(t, MultiTxExisted, results[3].Status)
	require.NoError(t, results[3].Err)
	require.Equal(t, existSpends, results[3].Spends)

	require.Equal(t, MultiTxCreated, results[4].Status)
	require.Equal(t, MultiTxCreated, results[5].Status)

	_, called := store.callFor(*b.TxIDChainHash())
	require.False(t, called, "a child of a failed transaction is never written")
	_, called = store.callFor(*c.TxIDChainHash())
	require.False(t, called)
}

// The default makes no read of its own. A transaction whose record already
// exists comes back from SpendAndCreate as ErrTxExists and is reported Existed,
// and its children in the list are still written, after it.
func TestDefaultSpendAndCreateMulti_ExistingRecord(t *testing.T) {
	ctx := context.Background()

	existing := multiTx(t, 1, 1, outsideOutpoint(1))
	child := multiTx(t, 2, 1, outOf(existing, 0))

	store := &multiFakeStore{
		failWith: map[chainhash.Hash]error{*existing.TxIDChainHash(): errors.NewTxExistsError("exists")},
		spendsOn: map[chainhash.Hash][]*Spend{*existing.TxIDChainHash(): {{TxID: existing.TxIDChainHash()}}},
	}

	results, err := DefaultSpendAndCreateMulti(ctx, store, 4, []*bt.Tx{existing, child}, 100)
	require.NoError(t, err)

	require.Equal(t, MultiTxExisted, results[0].Status)
	require.NoError(t, results[0].Err)
	require.NotNil(t, results[0].Spends, "the spends stay in place, as SpendAndCreate leaves them")
	require.Equal(t, MultiTxCreated, results[1].Status)
	require.Zero(t, store.decorations, "the default makes no read of its own")

	e, _ := store.callFor(*existing.TxIDChainHash())
	c, _ := store.callFor(*child.TxIDChainHash())
	require.False(t, c.start.Before(e.end), "the child is written after its existing parent")
}

// Options apply to every transaction exactly as SpendAndCreate applies them, and
// WithTXIDs becomes each transaction's WithTXID.
func TestDefaultSpendAndCreateMulti_OptionsPassThrough(t *testing.T) {
	a := multiTx(t, 1, 1, outsideOutpoint(1))
	b := multiTx(t, 2, 1, outOf(a, 0))

	// Deliberately wrong txids prove the store uses the supplied ones.
	ids := []chainhash.Hash{{0x0a}, {0x0b}}

	store := &multiFakeStore{}

	results, err := DefaultSpendAndCreateMulti(context.Background(), store, 4, []*bt.Tx{a, b}, 100,
		WithTXIDs(ids), WithIgnoreLocked(true), WithFrozen(true), WithLocked(true), WithConflicting(true),
		WithSkipExtendedInputs(true), WithSkipUTXOHashCheck(true), WithIgnoreConflicting(true),
		WithMinedBlockInfo(MinedBlockInfo{BlockID: 7, BlockHeight: 100}))
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Len(t, store.seenOpts, 2)

	seen := map[chainhash.Hash]bool{}

	for _, o := range store.seenOpts {
		require.NotNil(t, o.TxID)
		seen[*o.TxID] = true
		require.True(t, o.IgnoreFlags.IgnoreLocked)
		require.True(t, o.IgnoreFlags.IgnoreConflicting)
		require.True(t, o.IgnoreFlags.SkipUTXOHashCheck)
		require.True(t, o.Frozen)
		require.True(t, o.Locked)
		require.True(t, o.Conflicting)
		require.True(t, o.SkipExtendedInputs)
		require.Equal(t, []MinedBlockInfo{{BlockID: 7, BlockHeight: 100}}, o.MinedBlockInfos)
	}

	require.Equal(t, map[chainhash.Hash]bool{ids[0]: true, ids[1]: true}, seen)
}

// A cancelled context stops the list between levels; transactions never
// attempted stay NotAttempted and the call reports the context error.
func TestDefaultSpendAndCreateMulti_CancelledBetweenLevels(t *testing.T) {
	a := multiTx(t, 1, 1, outsideOutpoint(1))
	b := multiTx(t, 2, 1, outOf(a, 0))

	ctx, cancel := context.WithCancel(context.Background())
	store := &cancellingStore{multiFakeStore: &multiFakeStore{}, cancel: cancel}

	results, err := DefaultSpendAndCreateMulti(ctx, store, 4, []*bt.Tx{a, b}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, results, 2)
	require.Equal(t, MultiTxCreated, results[0].Status)
	require.Equal(t, MultiTxNotAttempted, results[1].Status)
}

type cancellingStore struct {
	*multiFakeStore
	cancel context.CancelFunc
}

func (c *cancellingStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, h uint32, opts ...CreateOption) (*meta.Data, []*Spend, error) {
	defer c.cancel()
	return c.multiFakeStore.SpendAndCreate(ctx, tx, h, opts...)
}
