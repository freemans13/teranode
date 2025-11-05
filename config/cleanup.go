package config

import "time"

// CleanupSettings contains configuration for the Cleanup service.
type CleanupSettings struct {
	// Disabled indicates whether the cleanup service is disabled
	Disabled bool

	// GRPCListenAddress is the address the cleanup service listens on for gRPC requests
	GRPCListenAddress string

	// GRPCAddress is the address other services use to connect to the cleanup service
	GRPCAddress string

	// PollingInterval is how often to poll block assembly state for cleanup triggers
	PollingInterval time.Duration
}
