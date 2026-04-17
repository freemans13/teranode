package subtreevalidation

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// ShouldUseSubtreeOnlyPath returns true when block validation can safely skip
// the subtreeData download and rely on the subtree hash manifest plus the local
// UTXO/TxMetaCache (with per-tx fetch for real misses) to validate the block.
//
// The gate is true only when BOTH:
//   - the operator has declared SubtreeValidation.AssumeTxsBroadcastToAllNodes, AND
//   - the blockchain service has a ReceivedAt stamp for this header that is newer
//     than SubtreeValidation.LivenessWindow.
//
// Any error from the blockchain client, an absent stamp, or a stale stamp returns
// false — callers fall through to the subtreeData path. The gate is an optimization,
// never a correctness constraint.
//
// See docs/superpowers/specs/2026-04-16-subtree-only-validation-with-liveness-gate-design.md.
func (u *Server) ShouldUseSubtreeOnlyPath(ctx context.Context, blockHash *chainhash.Hash) bool {
	if !u.settings.SubtreeValidation.AssumeTxsBroadcastToAllNodes {
		prometheusLivenessGateDecision.WithLabelValues("subtreedata").Inc()
		return false
	}

	stamp, found, err := u.blockchainClient.GetHeaderReceivedAt(ctx, blockHash)
	if err != nil {
		prometheusLivenessGateDecision.WithLabelValues("err").Inc()
		u.logger.Debugf("[ShouldUseSubtreeOnlyPath][%s] GetHeaderReceivedAt failed, falling back to subtreeData: %v", blockHash.String(), err)
		return false
	}
	if !found {
		prometheusLivenessGateDecision.WithLabelValues("notfound").Inc()
		return false
	}
	if time.Since(stamp) > u.settings.SubtreeValidation.LivenessWindow {
		prometheusLivenessGateDecision.WithLabelValues("subtreedata").Inc()
		return false
	}

	prometheusLivenessGateDecision.WithLabelValues("subtreeonly").Inc()
	return true
}
