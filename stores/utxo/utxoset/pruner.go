package utxoset

import (
	"context"

	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
)

// noopPruner reclaims nothing, because the spend already did.
//
// This is the clearest expression of what delete-on-spend buys. In an append-only store
// the pruner is a background job that walks a growing table removing rows whose
// delete-at-height has passed, and it is a job whose backlog can fall behind: on the
// mainnet box its sweep, the pruner and the vacuum it forced together measured 76.7% of
// all disk reads and 52% of statement write-ahead log volume, with the watermark sitting
// thousands of blocks behind the tip. Here the DELETE that spends an output frees its
// space and its index entry in the same statement that authorises the spend, so there is
// no backlog, no watermark, and nothing that can fall behind.
//
// Zero records pruned is therefore exact, not evasive.
//
// It is a real value rather than a nil, even though the provider interface documents nil
// as meaning "unsupported", because services/pruner/server.go stores the result and uses
// it without a nil check.
type noopPruner struct{}

// Start does nothing and returns immediately, as the contract requires.
func (noopPruner) Start(_ context.Context) {}

// Prune reports that nothing needed reclaiming at this height.
func (noopPruner) Prune(_ context.Context, _ uint32, _ string) (int64, error) { return 0, nil }

// AddObserver accepts and discards. Observers are notified when a pruning cycle
// completes, and no cycle ever runs here. Nothing currently registers one: the SQL
// pruner's AddObserver is also a no-op and services/pruner/server.go never calls it.
func (noopPruner) AddObserver(_ pruner.Observer) {}

// GetPrunerService satisfies pruner.PrunerServiceProvider.
//
// The pruner service is mandatory: services/pruner/server.go:116-119 type-asserts the
// UTXO store to this interface and refuses to start without it, taking the whole daemon
// down. So the store has to answer, and the truthful answer is a pruner with nothing to do.
func (s *Store) GetPrunerService() (pruner.Service, error) {
	return noopPruner{}, nil
}
