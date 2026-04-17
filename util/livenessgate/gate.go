// Package livenessgate provides a shared, pure decision function used by
// block validation and subtree validation to decide whether a block can
// use the subtree-only path (skipping subtreeData downloads) based on
// the age of its header's first-seen timestamp.
//
// See docs/superpowers/specs/2026-04-16-subtree-only-validation-with-liveness-gate-design.md.
package livenessgate

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// Client is the narrow interface needed by ShouldUseSubtreeOnlyPath.
// Both blockchain.ClientI and blockchain.LocalClient implement it by
// providing GetHeaderReceivedAt.
type Client interface {
	GetHeaderReceivedAt(ctx context.Context, hash *chainhash.Hash) (time.Time, bool, error)
}

// ShouldUseSubtreeOnlyPath returns true when block validation can safely
// skip the subtreeData download for this block.
//
// Returns true only when BOTH:
//   - the caller declared the gate enabled (typically from
//     SubtreeValidation.AssumeTxsBroadcastToAllNodes), AND
//   - the blockchain client has a ReceivedAt stamp for this header that
//     is newer than window.
//
// Any error from the blockchain client, an absent stamp, or a stale
// stamp returns false — callers fall through to the subtreeData path.
// The gate is an optimization, never a correctness constraint. Callers
// that want metrics or logging should wrap this function.
func ShouldUseSubtreeOnlyPath(ctx context.Context, client Client, blockHash *chainhash.Hash, enabled bool, window time.Duration) bool {
	if !enabled {
		return false
	}
	stamp, found, err := client.GetHeaderReceivedAt(ctx, blockHash)
	if err != nil || !found {
		return false
	}
	return time.Since(stamp) <= window
}
