package cleanup

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// pollingWorker polls the block assembly service every polling interval to check
// if cleanup should be triggered. It queues cleanup when:
// 1. Block height has changed since last processed
// 2. Cleanup channel is available (no cleanup currently in progress)
//
// Safety checks (block assembly state) are performed in cleanupProcessor to eliminate
// race conditions where state could change between queueing and execution.
func (s *Server) pollingWorker(ctx context.Context) {
	ticker := time.NewTicker(s.settings.Cleanup.PollingInterval)
	defer ticker.Stop()

	s.logger.Infof("Starting cleanup polling worker (interval: %v)", s.settings.Cleanup.PollingInterval)

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("Stopping cleanup polling worker")
			return

		case <-ticker.C:
			// Poll block assembly state to get current height
			state, err := s.blockAssemblyClient.GetBlockAssemblyState(ctx)
			if err != nil {
				s.logger.Errorf("Failed to get block assembly state: %v", err)
				cleanupErrors.WithLabelValues("poll").Inc()
				continue
			}

			if state.CurrentHeight <= s.lastProcessedHeight.Load() {
				s.logger.Debugf("Skipping cleanup: no new height (current: %d, last processed: %d)",
					state.CurrentHeight, s.lastProcessedHeight.Load())
				cleanupSkipped.WithLabelValues("no_new_height").Inc()
				continue
			}

			// Try to queue cleanup (non-blocking)
			// Safety checks are performed in cleanupProcessor to avoid race conditions
			select {
			case s.cleanupCh <- state.CurrentHeight:
				s.logger.Debugf("Queued cleanup for height %d", state.CurrentHeight)
			default:
				s.logger.Infof("Cleanup already in progress, skipping height %d", state.CurrentHeight)
				cleanupSkipped.WithLabelValues("already_in_progress").Inc()
			}
		}
	}
}

// cleanupProcessor processes cleanup requests from the cleanup channel.
// It drains the channel to get the latest height (deduplication), then performs
// cleanup in three sequential steps:
// 1. Preserve parents of old unmined transactions
// 2. Process expired preservations (convert preserve_until to delete_at_height)
// 3. Delete-at-height (DAH) cleanup
//
// Safety checks (block assembly state) are performed immediately before each phase
// to prevent race conditions where state could change between queueing and execution.
//
// This goroutine ensures only one cleanup operation runs at a time by processing
// from a buffered channel (size 1).
func (s *Server) cleanupProcessor(ctx context.Context) {
	s.logger.Infof("Starting cleanup processor")

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("Stopping cleanup processor")
			return

		case height := <-s.cleanupCh:
			// Deduplicate: drain channel and process latest height only
			// This is important during block catchup when multiple heights may be queued
			latestHeight := height
			drained := false
		drainLoop:
			for {
				select {
				case nextHeight := <-s.cleanupCh:
					latestHeight = nextHeight
					drained = true
				default:
					break drainLoop
				}
			}

			if drained {
				s.logger.Debugf("Deduplicating cleanup operations, skipping to height %d", latestHeight)
			}

			// Safety check before preserve parents phase
			// Check block assembly state to ensure it's safe to run cleanup
			state, err := s.blockAssemblyClient.GetBlockAssemblyState(ctx)
			if err != nil {
				s.logger.Errorf("Failed to get block assembly state before preserve parents: %v", err)
				cleanupErrors.WithLabelValues("state_check").Inc()
				continue
			}

			if state.BlockAssemblyState != "running" {
				s.logger.Infof("Skipping cleanup for height %d: block assembly state is %s (not running)", latestHeight, state.BlockAssemblyState)
				cleanupSkipped.WithLabelValues("not_running").Inc()
				continue
			}

			// Step 1: Preserve parents of old unmined transactions FIRST
			// This ensures parents of old unmined transactions are not deleted by DAH cleanup
			// CRITICAL: If this phase fails, we MUST NOT proceed to subsequent phases,
			// as DAH cleanup could delete parents that should be preserved.
			s.logger.Infof("Starting cleanup for height %d: preserving parents", latestHeight)
			startTime := time.Now()

			if s.utxoStore != nil {
				_, err := utxo.PreserveParentsOfOldUnminedTransactions(
					ctx, s.utxoStore, latestHeight, s.settings, s.logger)
				if err != nil {
					s.logger.Errorf("CRITICAL: Failed to preserve parents at height %d, ABORTING cleanup to prevent data loss: %v", latestHeight, err)
					cleanupErrors.WithLabelValues("preserve_parents_failed").Inc()
					cleanupSkipped.WithLabelValues("preserve_failed").Inc()
					// ABORT: Do not proceed to Phase 2 or 3 - could cause data loss
					continue
				}
				cleanupDuration.WithLabelValues("preserve_parents").Observe(time.Since(startTime).Seconds())
			}

			// Safety check before process expired preservations phase
			// Recheck block assembly state to ensure it hasn't changed (e.g., to reorg)
			state, err = s.blockAssemblyClient.GetBlockAssemblyState(ctx)
			if err != nil {
				s.logger.Errorf("Failed to get block assembly state before process expired preservations: %v", err)
				cleanupErrors.WithLabelValues("state_check").Inc()
				continue
			}

			if state.BlockAssemblyState != "running" {
				s.logger.Infof("Skipping process expired preservations for height %d: block assembly state changed to %s (not running)", latestHeight, state.BlockAssemblyState)
				cleanupSkipped.WithLabelValues("not_running").Inc()
				continue
			}

			// Step 2: Process expired preservations
			// This converts transactions with expired preserve_until back to delete_at_height
			// so they can be cleaned up by DAH cleanup in the next phase
			s.logger.Infof("Starting cleanup for height %d: processing expired preservations", latestHeight)
			startTime = time.Now()

			if s.utxoStore != nil {
				err := s.utxoStore.ProcessExpiredPreservations(ctx, latestHeight)
				if err != nil {
					s.logger.Errorf("Error processing expired preservations during block height %d update: %v", latestHeight, err)
					cleanupErrors.WithLabelValues("process_expired_preservations").Inc()
					// Continue to DAH cleanup even if processing expired preservations fails
				} else {
					cleanupDuration.WithLabelValues("process_expired_preservations").Observe(time.Since(startTime).Seconds())
				}
			}

			// Safety check before DAH cleanup phase
			// Recheck block assembly state to ensure it hasn't changed (e.g., to reorg)
			state, err = s.blockAssemblyClient.GetBlockAssemblyState(ctx)
			if err != nil {
				s.logger.Errorf("Failed to get block assembly state before DAH cleanup: %v", err)
				cleanupErrors.WithLabelValues("state_check").Inc()
				continue
			}

			if state.BlockAssemblyState != "running" {
				s.logger.Infof("Skipping DAH cleanup for height %d: block assembly state changed to %s (not running)", latestHeight, state.BlockAssemblyState)
				cleanupSkipped.WithLabelValues("not_running").Inc()
				continue
			}

			// Step 3: Then trigger DAH cleanup and WAIT for it to complete
			// DAH cleanup deletes transactions marked for deletion at or before the current height
			if s.cleanupService != nil {
				s.logger.Infof("Starting cleanup for height %d: DAH cleanup", latestHeight)
				startTime = time.Now()
				doneCh := make(chan string, 1)

				if err := s.cleanupService.UpdateBlockHeight(latestHeight, doneCh); err != nil {
					s.logger.Errorf("Cleanup service error updating block height %d: %v", latestHeight, err)
					cleanupErrors.WithLabelValues("dah_cleanup").Inc()
					continue
				}

				// Wait for cleanup to complete with timeout
				cleanupTimeout := 10 * time.Minute
				timeoutTimer := time.NewTimer(cleanupTimeout)
				defer timeoutTimer.Stop()

				select {
				case status := <-doneCh:
					if status != "completed" {
						s.logger.Warnf("Cleanup for height %d finished with status: %s", latestHeight, status)
						cleanupErrors.WithLabelValues("dah_cleanup").Inc()
					} else {
						s.logger.Infof("Cleanup for height %d completed successfully", latestHeight)
						cleanupDuration.WithLabelValues("dah_cleanup").Observe(time.Since(startTime).Seconds())
						cleanupProcessed.Inc()
					}
				case <-timeoutTimer.C:
					s.logger.Errorf("Cleanup for height %d timed out after %v", latestHeight, cleanupTimeout)
					cleanupErrors.WithLabelValues("timeout").Inc()
					// Continue to next iteration - don't block forever
				case <-ctx.Done():
					return
				}
			}

			// Update last processed height atomically
			s.lastProcessedHeight.Store(latestHeight)
		}
	}
}
