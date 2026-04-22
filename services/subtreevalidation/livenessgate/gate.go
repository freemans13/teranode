// Package livenessgate provides the pure decision function for the
// subtree-only validation path: whether a block can skip subtreeData
// downloads based on the age of its header's first-seen timestamp.
//
// Shared between subtree validation (per-subtree gating in CheckBlockSubtrees)
// and block validation (per-block gating for the catchup prefetch).
//
// The gate is an optimization enabled by SubtreeValidation.AssumeTxsBroadcastToAllNodes
// when the operator trusts that peers broadcast transactions to all nodes. It is
// never a correctness constraint — any fall-through returns the subtreeData
// decision and the caller uses the existing subtreeData path.
package livenessgate

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// Client is the narrow interface consumed by Decide. Both
// blockchain.ClientI and blockchain.LocalClient implement it by
// providing GetHeaderReceivedAt.
type Client interface {
	GetHeaderReceivedAt(ctx context.Context, hash *chainhash.Hash) (time.Time, bool, error)
}

// Decision describes why the gate resolved the way it did. Callers that
// emit metrics or log diagnostics use this to distinguish fall-through
// causes (error / absent stamp / stale stamp) from the disabled-setting
// short-circuit and the happy subtree-only path.
type Decision int

const (
	// DecisionSubtreeData — gate disabled by setting, or stamp is stale.
	// Caller must fall back to the subtreeData path.
	DecisionSubtreeData Decision = iota
	// DecisionSubtreeOnly — stamp is within the liveness window; caller
	// may skip subtreeData and rely on the local UTXO/TxMetaCache.
	DecisionSubtreeOnly
	// DecisionNotFound — blockchain client has no stamp for this header.
	// Caller must fall back to the subtreeData path.
	DecisionNotFound
	// DecisionError — the blockchain client returned an error. Caller must
	// fall back to the subtreeData path.
	DecisionError
)

// String returns a stable lowercase label suitable for use as a Prometheus
// metric value. Callers should prefer these labels so dashboards stay
// consistent across call sites.
func (d Decision) String() string {
	switch d {
	case DecisionSubtreeOnly:
		return "subtreeonly"
	case DecisionNotFound:
		return "notfound"
	case DecisionError:
		return "err"
	default:
		return "subtreedata"
	}
}

// Decide is the single source of truth for the gate's decision. It returns
// the resolved Decision plus any client error. Callers compare the result
// against DecisionSubtreeOnly for the boolean answer and use the Decision
// value for metric labels / logging.
//
// The gate resolves to DecisionSubtreeOnly only when ALL of:
//   - enabled is true (typically from SubtreeValidation.AssumeTxsBroadcastToAllNodes), AND
//   - window > 0, AND
//   - the blockchain client has a ReceivedAt stamp for this header newer than window.
//
// Any other outcome (disabled, non-positive window, error, absent stamp,
// stale stamp) resolves to a subtreeData fallback without even attempting
// the lookup when the result is foreordained. The gate is an optimization,
// never a correctness constraint.
func Decide(ctx context.Context, client Client, blockHash *chainhash.Hash, enabled bool, window time.Duration) (Decision, error) {
	if !enabled || window <= 0 {
		return DecisionSubtreeData, nil
	}
	stamp, found, err := client.GetHeaderReceivedAt(ctx, blockHash)
	if err != nil {
		return DecisionError, err
	}
	if !found {
		return DecisionNotFound, nil
	}
	if time.Since(stamp) > window {
		return DecisionSubtreeData, nil
	}
	return DecisionSubtreeOnly, nil
}
