package config

import "time"

// PrunerSettings contains configuration for the Pruner service.
type PrunerSettings struct {
	// GRPCListenAddress is the address the pruner service listens on for gRPC requests
	GRPCListenAddress string

	// GRPCAddress is the address other services use to connect to the pruner service
	GRPCAddress string

	// PollingInterval is how often to poll block assembly state as a fallback trigger
	// (primary trigger is event-driven via BlockPersisted notifications)
	PollingInterval time.Duration

	// JobTimeout is the maximum duration to wait for a single pruner job to complete
	JobTimeout time.Duration

	// WorkerCount is the number of worker goroutines for the pruner service
	WorkerCount int
}
