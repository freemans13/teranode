package utxo

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// A burned lane position is one a store took from its source for a create and then wrote no record
// carrying. Burning is safe, since no record has the position, but every burned position inside a
// block's range is a hole in that block's lane map, so the stores count and log each one with why it
// burned and which service asked for the create. A position whose write cannot be confirmed either
// way is counted too, under outcome_unknown, so the counter can include positions that were written.
//
// The code that took the position from the source records its burn, from the result it saw. A store
// whose Create is handed a position the caller already took records nothing for it, so one burned
// position is counted once.
//
// A burned position whose create the store can prove wrote no record carrying it goes back to the
// source for reuse (BurnOrReturnLanePosition), so the next take fills the hole. Two things must both
// hold: the code that saw the result proves nothing carries the position, and the reason is one of
// those laneReturnable allows. The reason alone is a label and proves nothing; outcome_unknown in
// particular can be a written position. A returned position is still counted as burned; the
// allocator counts the ones it gives out again in teranode_lane_positions_reused_total, so the holes
// left are burned minus reused.

// The reasons a position burns.
const (
	// LaneBurnExists: the create answered ErrTxExists, because the store already held the
	// transaction. The record keeps the position it has. One exception: a utxoset create that
	// carries a block, for a transaction held only as UTXO rows with no identity or containment
	// row, writes the new position on the containment row it adds and still answers ErrTxExists,
	// so it is counted here though the position was written.
	LaneBurnExists = "exists"

	// LaneBurnSpendRejected: an input could not be spent, so the transaction was not created.
	LaneBurnSpendRejected = "spend_rejected"

	// LaneBurnOutcomeUnknown: the caller stopped waiting, or the store could not confirm the
	// write, so the record may still have been written with the position. A count under this
	// reason can include positions that were in fact written.
	LaneBurnOutcomeUnknown = "outcome_unknown"

	// LaneBurnRolledBack: a batch, or a chunk of a list, rolled back, and its positions were not
	// written again.
	LaneBurnRolledBack = "rolled_back"

	// LaneBurnParentFailed: a parent of the transaction earlier in its list failed, so the
	// transaction was not written.
	LaneBurnParentFailed = "parent_failed"

	// LaneBurnListFailed: the list failed before the transaction's chunk was written, either in a
	// check that runs before any chunk or because another chunk failed first.
	LaneBurnListFailed = "list_failed"

	// LaneBurnTakeFailed: a list took positions one at a time and a later take failed, so the
	// positions already taken were never used.
	LaneBurnTakeFailed = "take_failed"

	// LaneBurnError: any other failed create.
	LaneBurnError = "error"
)

// Caller names the service a create was made for. It is a byte, so it fits in CreateOptions' padding.
type Caller uint8

// The callers. The zero value is CallerUnknown, which is what every create that does not name itself
// gets.
const (
	CallerUnknown Caller = iota
	CallerValidator
	CallerSubtreeValidation
	CallerBlockValidation
	CallerBlockAssembly

	callerCount
)

var callerNames = [callerCount]string{
	CallerUnknown:           "unknown",
	CallerValidator:         "validator",
	CallerSubtreeValidation: "subtree_validation",
	CallerBlockValidation:   "block_validation",
	CallerBlockAssembly:     "block_assembly",
}

// String returns the caller's label value.
func (c Caller) String() string {
	if c >= callerCount {
		return callerNames[CallerUnknown]
	}

	return callerNames[c]
}

// withCaller holds one fixed option per caller, so WithCaller allocates nothing on the create path.
var withCaller = func() (opts [callerCount]CreateOption) {
	for c := range opts {
		caller := Caller(c) //nolint:gosec // c is below callerCount
		opts[c] = func(o *CreateOptions) { o.Caller = caller }
	}

	return opts
}()

// WithCaller names the service the create is made for. It changes nothing about the write: it labels
// the burned lane position counter and log line when the create's position burns.
func WithCaller(c Caller) CreateOption {
	if c >= callerCount {
		c = CallerUnknown
	}

	return withCaller[c]
}

var prometheusLanePositionsBurned = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "lane",
		Name:      "positions_burned_total",
		Help:      "Lane positions a store took for a create and wrote no record with, by store, reason and the service that asked for the create. Reason outcome_unknown counts positions whose write could not be confirmed and may have been written; reason exists can include a utxoset block create that wrote the position on a containment row",
	},
	[]string{"store", "reason", "caller"},
)

var prometheusLanePositionsReturned = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "lane",
		Name:      "positions_returned_total",
		Help:      "Burned lane positions a store proved no record carries and its source took back for reuse, by store and burn reason. Each is also counted in positions_burned_total; teranode_lane_positions_reused_total counts the ones given out again",
	},
	[]string{"store", "reason"},
)

// laneReturnable reports whether a position that burned for reason may go back to the source when
// the store proves nothing carries it: the store refused the insert because the transaction exists,
// an input's spend was refused before any create, or the database transaction rolled back whole.
// Every other reason burns for good, outcome_unknown and error above all: a write whose outcome is
// unknown may carry the position.
func laneReturnable(reason string) bool {
	switch reason {
	case LaneBurnExists, LaneBurnSpendRejected, LaneBurnRolledBack:
		return true
	default:
		return false
	}
}

// laneUnwrittenKey marks an error with which a store proves its create wrote no record carrying the
// create's lane position.
const laneUnwrittenKey = "lane_position_unwritten"

// MarkLanePositionUnwritten marks err, the error of a create, as proof that the create wrote no
// record carrying its lane position, such as an insert the store refused because the transaction
// exists. A store marks only an error its own code proves that for; an unmarked error proves
// nothing. It returns err.
func MarkLanePositionUnwritten(err error) error {
	var te *errors.Error
	if errors.As(err, &te) && te != nil {
		te.SetData(laneUnwrittenKey, true)
	}

	return err
}

// LanePositionUnwritten reports whether a store marked err with MarkLanePositionUnwritten.
func LanePositionUnwritten(err error) bool {
	var te *errors.Error

	return errors.As(err, &te) && te != nil && te.GetData(laneUnwrittenKey) == true
}

// LaneBurnReason returns why a create that took a position and failed with err burned it. spends are
// the spends the call returned, if any: a refused input is reported on its Spend.
func LaneBurnReason(err error, spends []*Spend) string {
	// A spend cut short by the context ending is reported on the Spend records and as a utxo
	// error, like a refused one, so the context is ruled out before the utxo error is read as a
	// refusal.
	switch {
	case errors.Is(err, errors.ErrTxExists):
		return LaneBurnExists
	case spendRefused(spends):
		return LaneBurnSpendRejected
	case IsCreateOutcomeUnknown(err):
		return LaneBurnOutcomeUnknown
	case errors.Is(err, errors.ErrUtxoError), errors.Is(err, errors.ErrSpent):
		return LaneBurnSpendRejected
	default:
		return LaneBurnError
	}
}

// spendRefused reports whether a Spend record says its input was refused. A record whose error is
// the context ending was not refused.
func spendRefused(spends []*Spend) bool {
	for _, s := range spends {
		if s != nil && s.Err != nil && !errors.IsContextError(s.Err) {
			return true
		}
	}

	return false
}

// TookLanePosition reports whether a create that named the position named, and holds position after
// taking, took it from the source. Only then is a burn its own to record. A position the caller named
// was taken, and is recorded, by that caller.
func TookLanePosition(named, position uint64) bool {
	return named == lane.None && position != lane.None
}

// CreatedTxID is the id of the transaction a create writes: the one the options name, or the
// transaction's own.
func CreatedTxID(tx *bt.Tx, o *CreateOptions) chainhash.Hash {
	if o != nil && o.TxID != nil {
		return *o.TxID
	}

	if tx == nil {
		return chainhash.Hash{}
	}

	return *tx.TxIDChainHash()
}

// NameLaneBurns gives the store's name and logger to the burned lane position counter and log line.
// A store calls it when it is built. One that does not records its burns under store "unknown",
// with no log line.
func (l *LanePositions) NameLaneBurns(store string, logger ulogger.Logger) {
	l.burnStore = store
	l.burnLogger = logger
}

// BurnLanePosition counts one burned position and logs it at info level. The position stays burned.
func (l *LanePositions) BurnLanePosition(txid chainhash.Hash, caller Caller, position uint64, reason string) {
	l.BurnOrReturnLanePosition(txid, caller, position, reason, false)
}

// BurnOrReturnLanePosition counts and logs one burned position as BurnLanePosition does, and hands
// it back to the source for reuse when unwritten is true and laneReturnable allows the reason.
// unwritten is the caller's proof, from its own code, that no record carries the position; it is
// called once the create or its batch has its outcome, never while a write that may carry the
// position can still commit, and at most once per position taken.
func (l *LanePositions) BurnOrReturnLanePosition(txid chainhash.Hash, caller Caller, position uint64, reason string, unwritten bool) {
	store := l.burnStore
	if store == "" {
		store = "unknown"
	}

	prometheusLanePositionsBurned.WithLabelValues(store, reason, caller.String()).Inc()

	returned := unwritten && l.returner != nil && laneReturnable(reason) && l.returner.Return(position)
	if returned {
		prometheusLanePositionsReturned.WithLabelValues(store, reason).Inc()
	}

	if l.burnLogger != nil {
		l.burnLogger.Infof("[lane] burned lane position %d (lane %d) of tx %s: store=%s reason=%s caller=%s returned=%t",
			position, lane.Lane(position), txid.String(), store, reason, caller.String(), returned)
	}
}

// BurnCreateLanePosition records the burn of a create that took position from the source and
// returned err, if it failed, and returns the position for reuse when unwritten proves no record
// carries it (see BurnOrReturnLanePosition). A create that succeeded wrote the position, so it
// records nothing.
func (l *LanePositions) BurnCreateLanePosition(tx *bt.Tx, o *CreateOptions, position uint64, err error, spends []*Spend, unwritten bool) {
	if err == nil || position == lane.None {
		return
	}

	l.BurnOrReturnLanePosition(CreatedTxID(tx, o), o.Caller, position, LaneBurnReason(err, spends), unwritten)
}
