package utxo

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestLaneBurnReason(t *testing.T) {
	refused := []*Spend{nil, {Err: errors.NewUtxoSpentError([32]byte{}, 0, [32]byte{}, nil)}}

	cases := []struct {
		name   string
		err    error
		spends []*Spend
		want   string
	}{
		{"the transaction exists", errors.NewTxExistsError("exists"), nil, LaneBurnExists},
		{"exists wins over a refused spend", errors.NewTxExistsError("exists"), refused, LaneBurnExists},
		{"a spend refused on its record", errors.NewProcessingError("failed"), refused, LaneBurnSpendRejected},
		{"a utxo error", errors.NewUtxoError("2 of 2 inputs could not be spent"), nil, LaneBurnSpendRejected},
		{"a spent utxo", errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil), nil, LaneBurnSpendRejected},
		{"the caller's context ended", context.Canceled, nil, LaneBurnOutcomeUnknown},
		{"a spend cut short by the context ending", errors.NewContextCanceledError("cancelled"),
			[]*Spend{{Err: errors.NewContextCanceledError("cancelled")}}, LaneBurnOutcomeUnknown},
		{"a write marked unknown", MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("timed out")), nil, LaneBurnOutcomeUnknown},
		{"anything else", errors.NewStorageError("disk"), nil, LaneBurnError},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, LaneBurnReason(c.err, c.spends))
		})
	}
}

func TestWithCaller(t *testing.T) {
	for c := CallerUnknown; c < callerCount; c++ {
		var o CreateOptions

		WithCaller(c)(&o)
		require.Equal(t, c, o.Caller)
		require.NotEmpty(t, c.String())
	}

	var o CreateOptions

	WithCaller(callerCount + 3)(&o)
	require.Equal(t, CallerUnknown, o.Caller, "a caller out of range is unknown")
	require.Equal(t, "unknown", Caller(200).String())

	require.Equal(t, "validator", CallerValidator.String())
	require.Equal(t, "subtree_validation", CallerSubtreeValidation.String())
	require.Equal(t, "block_validation", CallerBlockValidation.String())
	require.Equal(t, "block_assembly", CallerBlockAssembly.String())
}

func TestTookLanePosition(t *testing.T) {
	require.True(t, TookLanePosition(0, 5), "no position named, one taken from the source")
	require.False(t, TookLanePosition(5, 5), "the caller named it")
	require.False(t, TookLanePosition(0, 0), "no source")
}

// burnLog captures the info lines a burn writes.
type burnLog struct {
	ulogger.TestLogger
	lines []string
}

func (l *burnLog) Infof(format string, args ...interface{}) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

// Every burn writes one info line naming the transaction, the position and its lane, the store, the
// reason and the caller. A create that wrote its record writes none.
func TestBurnLanePositionLogsOneLine(t *testing.T) {
	log := &burnLog{}

	var positions LanePositions

	positions.NameLaneBurns("teststore", log)

	tx := bt.NewTx()
	tx.LockTime = 7
	txid := *tx.TxIDChainHash()
	position := lane.Pack(9, 1234)

	positions.BurnCreateLanePosition(tx, &CreateOptions{Caller: CallerBlockValidation}, position, nil, nil, false)
	require.Empty(t, log.lines, "a create that wrote its record burns nothing")

	positions.BurnCreateLanePosition(tx, &CreateOptions{Caller: CallerBlockValidation}, position,
		errors.NewTxExistsError("exists"), nil, false)
	require.Len(t, log.lines, 1)

	line := log.lines[0]
	for _, want := range []string{txid.String(), fmt.Sprint(position), "lane 9", "store=teststore",
		"reason=exists", "caller=block_validation", "returned=false"} {
		require.Contains(t, line, want)
	}
}

// A create that names its transaction's ID logs that ID, not the hash of the bytes it was given, as
// a rebuilt transaction's create does.
func TestBurnLanePositionLogsTheNamedTxID(t *testing.T) {
	log := &burnLog{}

	var positions LanePositions

	positions.NameLaneBurns("teststore", log)

	tx := bt.NewTx()
	named := chainhash.HashH([]byte("the id the create names"))
	require.NotEqual(t, named, *tx.TxIDChainHash())

	positions.BurnCreateLanePosition(tx, &CreateOptions{TxID: &named}, lane.Pack(9, 1), errors.NewTxExistsError("exists"), nil, false)

	require.Len(t, log.lines, 1)
	require.Contains(t, log.lines[0], named.String())
	require.NotContains(t, log.lines[0], tx.TxIDChainHash().String())
}

// A caller that stopped waiting for its queued create gets its context error, and the record may
// still be written: the position counts as outcome_unknown, and is logged.
func TestBurnCreateLanePositionCountsAGivenUpWaiterAsUnknown(t *testing.T) {
	log := &burnLog{}

	var positions LanePositions

	positions.NameLaneBurns("waiterstore", log)

	counter := prometheusLanePositionsBurned.WithLabelValues("waiterstore", LaneBurnOutcomeUnknown, CallerValidator.String())
	before := testutil.ToFloat64(counter)

	positions.BurnCreateLanePosition(bt.NewTx(), &CreateOptions{Caller: CallerValidator}, lane.Pack(9, 2), context.Canceled, nil, false)

	require.Equal(t, before+1, testutil.ToFloat64(counter))
	require.Len(t, log.lines, 1)
	require.Contains(t, log.lines[0], "reason=outcome_unknown")
}

// A refused input reported on its Spend record counts as spend_rejected even when the call's error
// has another code: the classification reads the records the call returned.
func TestBurnCreateLanePositionReadsTheSpendRecords(t *testing.T) {
	log := &burnLog{}

	var positions LanePositions

	positions.NameLaneBurns("spendstore", log)

	refused := []*Spend{{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)}}

	positions.BurnCreateLanePosition(bt.NewTx(), &CreateOptions{}, lane.Pack(9, 3),
		errors.NewProcessingError("the spend failed"), refused, false)

	require.Len(t, log.lines, 1)
	require.Contains(t, log.lines[0], "reason=spend_rejected")
}

// lanedSequentialStore is the sequential fake with lane positions, so SequentialSpendAndCreate takes
// one and records its burn.
type lanedSequentialStore struct {
	fakeSequentialStore
	LanePositions
}

type onePosition struct{ position uint64 }

func (o onePosition) Next(context.Context) (uint64, error) { return o.position, nil }

// SequentialSpendAndCreate records the burn of a position it took. A failed spend is classified from
// the Spend records it returned, and a failed create logs the transaction's ID.
func TestSequentialSpendAndCreateRecordsItsBurns(t *testing.T) {
	tx := bt.NewTx()
	tx.LockTime = 11
	txid := *tx.TxIDChainHash()

	t.Run("a spend refused on its record", func(t *testing.T) {
		log := &burnLog{}
		s := &lanedSequentialStore{fakeSequentialStore: fakeSequentialStore{
			spends:   []*Spend{{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)}},
			spendErr: errors.NewProcessingError("the spend failed"),
		}}
		s.NameLaneBurns("seqstore", log)
		s.SetLanePositionSource(onePosition{lane.Pack(9, 4)})

		_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, s, tx, 100,
			WithCaller(CallerValidator))
		require.Error(t, err)

		require.Len(t, log.lines, 1)
		require.Contains(t, log.lines[0], "reason=spend_rejected")
		require.Contains(t, log.lines[0], "caller=validator")
		require.Contains(t, log.lines[0], txid.String())
		require.Zero(t, s.createCalls, "the create never ran")
	})

	t.Run("a create that fails", func(t *testing.T) {
		log := &burnLog{}
		s := &lanedSequentialStore{fakeSequentialStore: fakeSequentialStore{
			createErr: errors.NewTxExistsError("exists"),
		}}
		s.NameLaneBurns("seqstore", log)
		s.SetLanePositionSource(onePosition{lane.Pack(9, 5)})

		_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, s, tx, 100,
			WithCreateOnly(), WithCaller(CallerBlockAssembly))
		require.ErrorIs(t, err, errors.ErrTxExists)

		require.Len(t, log.lines, 1)
		require.Contains(t, log.lines[0], "reason=exists")
		require.Contains(t, log.lines[0], txid.String())
	})
}

// returningSource is a LanePositionSource that hands out one position and records what comes back.
type returningSource struct {
	position uint64
	refuse   bool
	returned []uint64
}

func (r *returningSource) Next(context.Context) (uint64, error) { return r.position, nil }

func (r *returningSource) Return(position uint64) bool {
	if r.refuse {
		return false
	}

	r.returned = append(r.returned, position)

	return true
}

var allLaneBurnReasons = []string{
	LaneBurnExists, LaneBurnSpendRejected, LaneBurnOutcomeUnknown, LaneBurnRolledBack,
	LaneBurnParentFailed, LaneBurnListFailed, LaneBurnTakeFailed, LaneBurnError,
}

// Only the three reasons a store can prove go back: the insert refused because the transaction
// exists, a spend refused before any create, a database transaction rolled back whole. A write whose
// outcome is unknown may carry its position, so outcome_unknown must never be among them.
func TestLaneReturnableAllowsOnlyProvableReasons(t *testing.T) {
	want := map[string]bool{LaneBurnExists: true, LaneBurnSpendRejected: true, LaneBurnRolledBack: true}

	for _, reason := range allLaneBurnReasons {
		require.Equal(t, want[reason], laneReturnable(reason), reason)
	}

	require.False(t, laneReturnable(""))
	require.False(t, laneReturnable("anything else"))
}

// A burn goes back only when the store proved the position unwritten AND the reason is allowed.
// Every burn is still counted and logged as burned.
func TestBurnOrReturnLanePositionReturnsOnlyAProvenAllowedBurn(t *testing.T) {
	for _, reason := range allLaneBurnReasons {
		for _, unwritten := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unwritten=%t", reason, unwritten), func(t *testing.T) {
				log := &burnLog{}
				src := &returningSource{}

				var positions LanePositions

				positions.NameLaneBurns("gatestore", log)
				positions.SetLanePositionSource(src)

				burned := prometheusLanePositionsBurned.WithLabelValues("gatestore", reason, CallerValidator.String())
				returned := prometheusLanePositionsReturned.WithLabelValues("gatestore", reason)
				burnedBefore, returnedBefore := testutil.ToFloat64(burned), testutil.ToFloat64(returned)

				positions.BurnOrReturnLanePosition(chainhash.Hash{}, CallerValidator, lane.Pack(3, 77), reason, unwritten)

				want := unwritten && laneReturnable(reason)

				require.InDelta(t, 1, testutil.ToFloat64(burned)-burnedBefore, 0, "every burn is counted")
				require.Len(t, log.lines, 1)
				require.Contains(t, log.lines[0], fmt.Sprintf("returned=%t", want))

				if want {
					require.Equal(t, []uint64{lane.Pack(3, 77)}, src.returned)
					require.InDelta(t, 1, testutil.ToFloat64(returned)-returnedBefore, 0)
				} else {
					require.Empty(t, src.returned)
					require.InDelta(t, 0, testutil.ToFloat64(returned)-returnedBefore, 0)
				}
			})
		}
	}
}

// A source that refuses the position, or one that takes nothing back, leaves it burned and
// uncounted as returned.
func TestBurnOrReturnLanePositionCountsOnlyWhatTheSourceTookBack(t *testing.T) {
	returned := prometheusLanePositionsReturned.WithLabelValues("refusestore", LaneBurnExists)
	before := testutil.ToFloat64(returned)

	var refusing LanePositions

	refusing.NameLaneBurns("refusestore", nil)
	refusing.SetLanePositionSource(&returningSource{refuse: true})
	refusing.BurnOrReturnLanePosition(chainhash.Hash{}, CallerValidator, lane.Pack(1, 1), LaneBurnExists, true)

	var plain LanePositions

	plain.NameLaneBurns("refusestore", nil)
	plain.SetLanePositionSource(onePosition{lane.Pack(1, 2)})
	plain.BurnOrReturnLanePosition(chainhash.Hash{}, CallerValidator, lane.Pack(1, 2), LaneBurnExists, true)

	require.InDelta(t, before, testutil.ToFloat64(returned), 0)
}

func TestMarkLanePositionUnwritten(t *testing.T) {
	err := errors.NewTxExistsError("exists")
	require.False(t, LanePositionUnwritten(err))

	require.Same(t, err, MarkLanePositionUnwritten(err))
	require.True(t, LanePositionUnwritten(err))
	require.ErrorIs(t, err, errors.ErrTxExists, "marking keeps the error what it was")

	require.False(t, LanePositionUnwritten(errors.NewTxExistsError("another")), "each error is marked on its own")
	require.False(t, LanePositionUnwritten(nil))
	require.False(t, LanePositionUnwritten(context.Canceled))
	require.NoError(t, MarkLanePositionUnwritten(nil))
	require.Equal(t, context.Canceled, MarkLanePositionUnwritten(context.Canceled), "a foreign error is left as it is")
}

// SequentialSpendAndCreate returns a position when its spend failed, since the create never ran,
// and when the store's Create marked its refusal. An unmarked ErrTxExists, which on Aerospike can be
// the create's own earlier attempt, stays burned.
func TestSequentialSpendAndCreateReturnsOnlyProvenPositions(t *testing.T) {
	tx := bt.NewTx()
	tx.LockTime = 12

	cases := []struct {
		name       string
		store      fakeSequentialStore
		opts       []CreateOption
		wantReturn bool
	}{
		{"a refused spend", fakeSequentialStore{
			spends:   []*Spend{{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)}},
			spendErr: errors.NewUtxoError("1 of 1 inputs could not be spent"),
		}, nil, true},
		{"a spend that failed with a storage error", fakeSequentialStore{
			spendErr: errors.NewStorageError("disk"),
		}, nil, false},
		{"a refusal the store marked", fakeSequentialStore{
			createErr: MarkLanePositionUnwritten(errors.NewTxExistsError("exists")),
		}, []CreateOption{WithCreateOnly()}, true},
		{"a refusal the store did not mark", fakeSequentialStore{
			createErr: errors.NewTxExistsError("exists"),
		}, []CreateOption{WithCreateOnly()}, false},
		{"a marked error whose outcome is unknown", fakeSequentialStore{
			createErr: MarkLanePositionUnwritten(MarkCreateOutcomeUnknown(errors.NewStorageError("lost"))),
		}, []CreateOption{WithCreateOnly()}, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := &returningSource{position: lane.Pack(9, 40)}
			s := &lanedSequentialStore{fakeSequentialStore: c.store}
			s.NameLaneBurns("seqreturn", nil)
			s.SetLanePositionSource(src)

			_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, s, tx, 100, c.opts...)
			require.Error(t, err)

			if c.wantReturn {
				require.Equal(t, []uint64{lane.Pack(9, 40)}, src.returned)
			} else {
				require.Empty(t, src.returned)
			}
		})
	}
}
