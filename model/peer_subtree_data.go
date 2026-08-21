package model

import (
	"github.com/bsv-blockchain/go-bt/v2"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// MissingSubtreeDataTxs reports how many of the subtree's nodes a subtree_data
// body failed to fill. A non-zero result means the body cannot satisfy the
// subtree and must be rejected rather than built on.
//
// NewSubtreeDataFromReader stops at a clean io.EOF without checking it filled
// every node, so a short or empty body deserializes "successfully" and leaves
// the tail Txs entries nil. An empty body is the issue-1368 signature: a peer's
// proxy cache replaying a failed or aborted on-demand generation as
// "200 + 0 bytes".
//
// Two distinct disasters follow from trusting such a body, which is why both
// the subtree meta regenerator and blockvalidation's catchup subtree_data
// fetcher call this one predicate rather than each keeping their own copy:
//
//   - A meta derived from it records no parents for the missing tail.
//     GetParentTxHashes then returns nil with no error, validOrderAndBlessed
//     reads that as a transaction it cannot find, and a valid block is
//     permanently invalidated.
//   - Data.Serialize panics on it. Serialize skips index 0 only when Nodes[0]
//     is the coinbase placeholder: it then sets txStartIndex = 1 and never
//     touches Txs[0]. For any other Nodes[0] it sets txStartIndex = 0 while
//     still guarding its own nil check with `i != 0`, so it walks straight into
//     Txs[0].SerializeBytes() on a nil *bt.Tx (IsExtended is nil-safe, so it
//     falls through to Bytes -> toBytesHelper -> Size).
//
// So index 0 counts as missing unless it genuinely holds the coinbase
// placeholder — the only case Serialize actually tolerates. Exempting index 0
// unconditionally, matching Serialize's literal `i != 0`, would let such a body
// through with a count of zero and land the panic in a per-subtree errgroup
// goroutine that no recover() covers. That is reachable without malice: a
// non-first subtree has no coinbase placeholder, so any block whose transaction
// count is congruent to 1 modulo the subtree size ends with a one-node subtree
// holding a real tx hash at index 0.
// The count is driven by the subtree's nodes rather than by len(data.Txs), so a
// body carrying fewer entries than the subtree has nodes counts the shortfall
// instead of reporting the entries it does have as complete. Today those two
// lengths always agree, because serializeFromReader allocates Txs at
// Subtree.Length() before it reads anything — but a Data reaching here by any
// other route would otherwise pass, and the callers' error messages already
// report the denominator as Subtree.Length(). Nil data is the same case with no
// entries at all: every node is unfilled.
//
// A nil subtree has no nodes, so nothing can be unfilled and the answer is
// genuinely zero. That is arithmetic rather than a fail-open default — no body
// can be judged against a subtree that is not there, and neither caller can
// reach it, both holding a subtree they have already deserialized against.
func MissingSubtreeDataTxs(subtree *subtreepkg.Subtree, data *subtreepkg.Data) int {
	if subtree == nil {
		return 0
	}

	var txs []*bt.Tx
	if data != nil {
		txs = data.Txs
	}

	nodes := subtree.Length()
	coinbaseAtZero := nodes > 0 && subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue)

	missing := 0

	for i := 0; i < nodes; i++ {
		if i < len(txs) && txs[i] != nil {
			continue
		}

		if i == 0 && coinbaseAtZero {
			continue
		}

		missing++
	}

	return missing
}
