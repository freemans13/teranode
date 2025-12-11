package pruner

import "context"

type Service interface {
	// Start starts the pruner service.
	// This should not block.
	// The service should stop when the context is cancelled.
	Start(ctx context.Context)

	// Prune removes transactions marked for deletion at or before the specified height.
	// Returns the number of records processed and any error encountered.
	// This method is synchronous and blocks until pruning completes or context is cancelled.
	Prune(ctx context.Context, height uint32) (recordsProcessed int64, err error)

	// SetPersistedHeightGetter sets the function used to get block persister progress.
	// This allows pruner to coordinate with block persister to avoid premature deletion.
	SetPersistedHeightGetter(getter func() uint32)
}

// PrunerServiceProvider defines an interface for stores that can provide a pruner service.
type PrunerServiceProvider interface {
	// GetPrunerService returns a pruner service for the store.
	// Returns nil if the store doesn't support pruner.
	GetPrunerService() (Service, error)
}
