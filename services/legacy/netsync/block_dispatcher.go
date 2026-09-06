package netsync

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// frontierEntry is the dispatcher's per-block bookkeeping record for a below-checkpoint
// block whose UTXO store work may still be in flight. HandleBlockDirect's ordering
// hand-shake (the wait immediately before sm.ProcessBlock) reads a predecessor's entry
// to decide whether it is safe to start its own RPC; the dispatcher added in Task 7
// writes hash/height once and closes rpcStarted/settled and sets failed/aborted/err as
// the block's work progresses and finishes.
//
// Task 6 defines only the shape and the two small helpers below (markRPCStarted, the
// context plumbing); Task 7 adds the dispatcher goroutine that constructs, tracks and
// retires these entries, in this same file.
type frontierEntry struct {
	hash   chainhash.Hash
	height uint32

	// rpcStarted closes exactly once, when this block's own ProcessBlock RPC begins.
	// A child waiting on its parent treats a closed rpcStarted as "safe to start
	// mine": from that point the parent's spends are already queued behind its own
	// create in commit order, so a spend of a coin the parent creates can never
	// land ahead of that create.
	rpcStarted chan struct{}

	// rpcStartedOnce guards rpcStarted so a retry or a racing caller can never
	// double-close it.
	rpcStartedOnce sync.Once

	// settled closes exactly once, when this block's outcome (success or failure)
	// is fully known. A child that observes settled before rpcStarted learns the
	// parent never reached its own RPC, so the ordering guarantee rpcStarted would
	// have given never held — see the hand-shake in HandleBlockDirect for why that
	// case still lets the child proceed rather than blocking forever.
	settled chan struct{}

	failed  atomic.Bool
	aborted atomic.Bool
	err     error
}

// markRPCStarted closes rpcStarted exactly once, no matter how many times or from how
// many goroutines it is called.
func (e *frontierEntry) markRPCStarted() {
	e.rpcStartedOnce.Do(func() {
		close(e.rpcStarted)
	})
}

// inflightParent is what the dispatcher resolved for a block whose parent is still in
// the window: the parent's height and its frontier entry. nil means "look the parent
// up in the blockchain store as before" — the pre-window behaviour, and what every
// caller other than the dispatcher itself still passes.
type inflightParent struct {
	height uint32
	entry  *frontierEntry
}

// frontierEntryContextKey is the unexported key type under which a block's own
// frontierEntry travels on its processing context, so HandleBlockDirect's ordering
// hand-shake can mark it started without threading an extra parameter through every
// call between the dispatcher and ProcessBlock.
type frontierEntryContextKey struct{}

// contextWithFrontierEntry returns a copy of ctx carrying e as the current block's own
// frontier entry.
func contextWithFrontierEntry(ctx context.Context, e *frontierEntry) context.Context {
	return context.WithValue(ctx, frontierEntryContextKey{}, e)
}

// frontierEntryFromContext returns the frontierEntry stashed by contextWithFrontierEntry,
// or nil if none was stashed — the normal case for every route except the dispatcher's.
func frontierEntryFromContext(ctx context.Context) *frontierEntry {
	e, _ := ctx.Value(frontierEntryContextKey{}).(*frontierEntry)
	return e
}
