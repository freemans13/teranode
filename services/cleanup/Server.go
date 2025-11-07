// Package cleanup provides the Cleanup Service which handles periodic cleanup of unmined transaction
// parents and delete-at-height (DAH) records in the UTXO store. It polls the Block Assembly service
// state and triggers cleanup operations only when safe to do so (i.e., when block assembly is in
// "running" state and not performing reorgs or resets).
package cleanup

import (
	"context"
	"encoding/binary"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/cleanup/cleanup_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/cleanup"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
)

// Server implements the Cleanup service which handles periodic cleanup operations
// for the UTXO store. It polls block assembly state and triggers cleanup when safe.
type Server struct {
	cleanup_api.UnsafeCleanupAPIServer

	// Dependencies (injected via constructor)
	ctx                 context.Context
	logger              ulogger.Logger
	settings            *settings.Settings
	utxoStore           utxo.Store
	blockchainClient    blockchain.ClientI
	blockAssemblyClient blockassembly.ClientI
	blobStore           blob.Store

	// Internal state
	cleanupService      cleanup.Service
	lastProcessedHeight atomic.Uint32
	lastPersistedHeight atomic.Uint32
	cleanupCh           chan uint32
	stats               *gocore.Stat
}

// New creates a new Cleanup server instance with the provided dependencies.
// This function initializes the server but does not start any background processes.
// Call Init() and then Start() to begin operation.
func New(
	ctx context.Context,
	logger ulogger.Logger,
	tSettings *settings.Settings,
	utxoStore utxo.Store,
	blockchainClient blockchain.ClientI,
	blockAssemblyClient blockassembly.ClientI,
	blobStore blob.Store,
) *Server {
	return &Server{
		ctx:                 ctx,
		logger:              logger,
		settings:            tSettings,
		utxoStore:           utxoStore,
		blockchainClient:    blockchainClient,
		blockAssemblyClient: blockAssemblyClient,
		blobStore:           blobStore,
		stats:               gocore.NewStat("cleanup"),
	}
}

// Init initializes the cleanup service. This is called before Start() and is responsible
// for setting up the cleanup service provider from the UTXO store and subscribing to
// block persisted notifications for coordination with the block persister service.
func (s *Server) Init(ctx context.Context) error {
	s.ctx = ctx

	// Initialize metrics
	initPrometheusMetrics()

	// Initialize cleanup service from UTXO store
	cleanupProvider, ok := s.utxoStore.(cleanup.CleanupServiceProvider)
	if !ok {
		return errors.NewServiceError("UTXO store does not provide cleanup service")
	}

	var err error
	s.cleanupService, err = cleanupProvider.GetCleanupService()
	if err != nil {
		return errors.NewServiceError("failed to get cleanup service", err)
	}
	if s.cleanupService == nil {
		return errors.NewServiceError("cleanup service not available from UTXO store")
	}

	// Set persisted height getter for block persister coordination
	s.cleanupService.SetPersistedHeightGetter(s.GetLastPersistedHeight)

	// Subscribe to BlockPersisted notifications using blockchain client
	// The Subscribe method returns a channel, but we'll handle notifications in the polling worker
	// For now, we'll track persisted height via blockchain state and notifications separately
	subscriptionCh, err := s.blockchainClient.Subscribe(ctx, "Cleanup")
	if err != nil {
		return errors.NewServiceError("failed to subscribe to blockchain notifications", err)
	}

	// Start a goroutine to handle BlockPersisted notifications
	go func() {
		for notification := range subscriptionCh {
			if notification.Type == model.NotificationType_BlockPersisted {
				if notification.Metadata != nil && notification.Metadata.Metadata != nil {
					if heightStr, ok := notification.Metadata.Metadata["height"]; ok {
						if height, err := strconv.ParseUint(heightStr, 10, 32); err == nil {
							s.lastPersistedHeight.Store(uint32(height))
							s.logger.Debugf("Updated persisted height to %d", height)
						}
					}
				}
			}
		}
	}()

	// Read initial persisted height from blockchain state
	if state, err := s.blockchainClient.GetState(ctx, "BlockPersisterHeight"); err == nil && len(state) >= 4 {
		height := binary.LittleEndian.Uint32(state)
		s.lastPersistedHeight.Store(height)
		s.logger.Infof("Loaded initial block persister height: %d", height)
	}

	return nil
}

// Start begins the cleanup service operation. It starts the polling worker and cleanup
// processor goroutines, then starts the gRPC server. This function blocks until the
// server shuts down or encounters an error.
func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	// Wait for blockchain FSM to be ready
	err := s.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		return err
	}

	// Initialize cleanup channel (buffer of 1 to prevent blocking while ensuring only one cleanup)
	s.cleanupCh = make(chan uint32, 1)

	// Start the cleanup service (Aerospike or SQL)
	if s.cleanupService != nil {
		s.cleanupService.Start(ctx)
	}

	// Start cleanup processor goroutine
	go s.cleanupProcessor(ctx)

	// Start polling worker goroutine
	go s.pollingWorker(ctx)

	// Start gRPC server (BLOCKING - must be last)
	if err := util.StartGRPCServer(ctx, s.logger, s.settings, "cleanup",
		s.settings.Cleanup.GRPCListenAddress,
		func(server *grpc.Server) {
			cleanup_api.RegisterCleanupAPIServer(server, s)
			closeOnce.Do(func() { close(readyCh) })
		}, nil); err != nil {
		return err
	}

	return nil
}

// Stop gracefully shuts down the cleanup service. Context cancellation will stop
// the polling worker and cleanup processor goroutines.
func (s *Server) Stop(ctx context.Context) error {
	// Stop the cleanup service if it has a Stop method
	if s.cleanupService != nil {
		// Check if the cleanup service implements Stop
		// Aerospike has Stop, SQL doesn't
		type stopper interface {
			Stop(ctx context.Context) error
		}
		if stoppable, ok := s.cleanupService.(stopper); ok {
			if err := stoppable.Stop(ctx); err != nil {
				s.logger.Errorf("Error stopping cleanup service: %v", err)
			}
		}
	}

	// Context cancellation will stop goroutines
	s.logger.Infof("Cleanup service stopped")
	return nil
}

// Health implements the health check for the cleanup service. When checkLiveness is true,
// it only checks if the service process is running. When false, it checks all dependencies
// including gRPC server, block assembly client, blockchain client, and UTXO store.
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// LIVENESS: Is the service process running?
		return http.StatusOK, "OK", nil
	}

	// READINESS: Can the service handle requests?
	checks := make([]health.Check, 0, 5)

	// Check gRPC server is listening
	if s.settings.Cleanup.GRPCListenAddress != "" {
		checks = append(checks, health.Check{
			Name: "gRPC Server",
			Check: health.CheckGRPCServerWithSettings(s.settings.Cleanup.GRPCListenAddress, s.settings, func(ctx context.Context, conn *grpc.ClientConn) error {
				// Simple connection check - if we can create a client, server is up
				return nil
			}),
		})
	}

	// Check block assembly client
	if s.blockAssemblyClient != nil {
		checks = append(checks, health.Check{
			Name:  "BlockAssemblyClient",
			Check: s.blockAssemblyClient.Health,
		})
	}

	// Check blockchain client
	if s.blockchainClient != nil {
		checks = append(checks, health.Check{
			Name:  "BlockchainClient",
			Check: s.blockchainClient.Health,
		})
		checks = append(checks, health.Check{
			Name:  "FSM",
			Check: blockchain.CheckFSM(s.blockchainClient),
		})
	}

	// Check UTXO store
	if s.utxoStore != nil {
		checks = append(checks, health.Check{
			Name:  "UTXOStore",
			Check: s.utxoStore.Health,
		})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC implements the gRPC health check endpoint.
func (s *Server) HealthGRPC(ctx context.Context, _ *cleanup_api.EmptyMessage) (*cleanup_api.HealthResponse, error) {
	// Add context value to prevent circular dependency when checking gRPC server
	ctx = context.WithValue(ctx, "skip-grpc-self-check", true)

	status, details, err := s.Health(ctx, false)

	return &cleanup_api.HealthResponse{
		Ok:      status == http.StatusOK,
		Details: details,
	}, errors.WrapGRPC(err)
}

// GetLastPersistedHeight returns the last known block height that has been persisted
// by the block persister service. This is used to coordinate cleanup operations to
// avoid deleting data that the block persister still needs.
func (s *Server) GetLastPersistedHeight() uint32 {
	return s.lastPersistedHeight.Load()
}
