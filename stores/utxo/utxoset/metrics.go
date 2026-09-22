package utxoset

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The two guard counters of the containment build. Both are named exactly as the design names
// them, with no namespace prefix, so a dashboard or an abort criterion written from the design
// finds them by the same string.
//
// They are registered at package load rather than behind a sync.Once like the other stores'
// metrics, because nothing here is optional: a Store that exists writes them, and a test that
// reads one must find it registered whether or not a Store was ever opened.
var (
	// interimDropRefused counts every pruner session in which the containment windows due to
	// drop were refused because tx_ident held a row. Until the deep stamp of build step 5
	// exists, nothing empties the identity table, so a window that drops while any identity
	// row exists could take with it the only block facts of a transaction seen before its
	// block. Below the checkpoint the table is empty and this stays at zero; a climbing value
	// there is an abort criterion of the soak, and the remedy is the stamp, not a relaxed
	// guard.
	interimDropRefused = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_interim_drop_refused_total",
		Help: "Containment window drops refused because tx_ident still holds a row; nothing empties it until the deep stamp exists",
	})

	// noIdentityReached counts an un-mine or a mark-off that reached a non-coinbase
	// transaction which has containment and no identity row. Such a transaction was created
	// through the block path at or below the checkpoint, was seeded, or has been stamped, and
	// no chain change the node admits should ever un-mine or mark off any of those. The
	// un-mine has by then deleted that transaction's only payload, so the counter must stay at
	// zero and is an abort criterion of the soak. Labelled by the operation that reached it.
	noIdentityReached = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "utxoset_no_identity_reached_total",
		Help: "Un-mines and mark-offs that reached a non-coinbase transaction with containment and no identity row",
	}, []string{"op"})
)
