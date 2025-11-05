package cleanup

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// pollingWorker polls the block assembly service every polling interval to check
// if cleanup should be triggered. It only triggers cleanup when:
// 1. Block assembly state is "running" (not reorging, resetting, etc.)
// 2. Block height has changed since last processed
// 3. Cleanup channel is available (no cleanup currently in progress)
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
			// Poll block assembly state
			state, err := s.blockAssemblyClient.GetBlockAssemblyState(ctx)
			if err != nil {
				s.logger.Errorf("Failed to get block assembly state: %v", err)
				cleanupErrors.WithLabelValues("poll").Inc()
				continue
			}

			// Check safety condition: only run cleanup when block assembly is in stable "running" state
			// This ensures we don't cleanup during reorgs, resets, or other unstable states
			if state.BlockAssemblyState != "running" {
				s.logger.Debugf("Skipping cleanup: block assembly state is %s (not running)", state.BlockAssemblyState)
				cleanupSkipped.WithLabelValues("not_running").Inc()
				continue
			}

			if state.CurrentHeight <= s.lastProcessedHeight {
				s.logger.Debugf("Skipping cleanup: no new height (current: %d, last processed: %d)",
					state.CurrentHeight, s.lastProcessedHeight)
				cleanupSkipped.WithLabelValues("no_new_height").Inc()
				continue
			}

			// Try to queue cleanup (non-blocking)
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
// cleanup in two sequential steps:
// 1. Preserve parents of old unmined transactions
// 2. Delete-at-height (DAH) cleanup
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

			// Step 1: Preserve parents of old unmined transactions FIRST
			// This ensures parents of old unmined transactions are not deleted by DAH cleanup
			s.logger.Infof("Starting cleanup for height %d: preserving parents", latestHeight)
			startTime := time.Now()

			if s.utxoStore != nil {
				_, err := utxo.PreserveParentsOfOldUnminedTransactions(
					ctx, s.utxoStore, latestHeight, s.settings, s.logger)
				if err != nil {
					s.logger.Errorf("Error preserving parents during block height %d update: %v", latestHeight, err)
					cleanupErrors.WithLabelValues("preserve_parents").Inc()
					// Continue to DAH cleanup even if parent preservation fails
					// (parent preservation errors are not critical for correctness)
				} else {
					cleanupDuration.WithLabelValues("preserve_parents").Observe(time.Since(startTime).Seconds())
				}
			}

			// Step 2: Then trigger DAH cleanup and WAIT for it to complete
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

				// Wait for cleanup to complete or context cancellation
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
				case <-ctx.Done():
					return
				}
			}

			// Update last processed height
			s.lastProcessedHeight = latestHeight
		}
	}
}
