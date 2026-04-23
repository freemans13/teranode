package subtreevalidation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/livenessgate"
)

// ShouldUseSubtreeOnlyPath returns true when block validation can safely skip
// the subtreeData download and rely on the subtree hash manifest plus the local
// UTXO/TxMetaCache (with per-tx fetch for real misses) to validate the block.
//
// The core decision is delegated to livenessgate.Decide so both this per-subtree
// wrapper and the catchup prefetch site in blockvalidation share exactly one
// definition of "live." This wrapper layers Prometheus metrics + debug logging
// on top so operators can see which decision branch fired.
func (u *Server) ShouldUseSubtreeOnlyPath(ctx context.Context, blockHash *chainhash.Hash) bool {
	decision, err := livenessgate.Decide(
		ctx,
		u.blockchainClient,
		blockHash,
		u.settings.SubtreeValidation.AssumeTxsBroadcastToAllNodes,
		u.settings.SubtreeValidation.LivenessWindow,
	)

	prometheusLivenessGateDecision.WithLabelValues(decision.String()).Inc()

	if decision == livenessgate.DecisionError {
		u.logger.Debugf("[ShouldUseSubtreeOnlyPath][%s] GetHeaderReceivedAt failed, falling back to subtreeData: %v", blockHash.String(), err)
	}

	return decision == livenessgate.DecisionSubtreeOnly
}
