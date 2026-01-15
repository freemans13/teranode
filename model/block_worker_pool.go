package model

import (
	"context"
	"runtime"
	"sync"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// subtreeValidationJob represents a single subtree validation job
type subtreeValidationJob struct {
	subtreeIndex int                   // Index in the SubtreeSlices array
	subtree      *subtreepkg.Subtree  // Subtree to validate
}

// subtreeValidationResult stores the result of validating a single subtree
type subtreeValidationResult struct {
	err error
}

// subtreeWorkerPool manages a fixed pool of subtree validation workers
// for processing block subtree validations with minimal scheduler overhead
type subtreeWorkerPool struct {
	numWorkers int
	jobs       chan subtreeValidationJob
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc

	// Shared read-only state (no contention)
	block         *Block
	logger        ulogger.Logger
	deps          *validationDependencies
	validationCtx *validationContext

	// Results storage (each worker writes to different index, no locking needed)
	results []subtreeValidationResult
}

// newSubtreeWorkerPool creates a worker pool with the specified number of workers
func newSubtreeWorkerPool(
	ctx context.Context,
	block *Block,
	numWorkers int,
	numSubtrees int,
	logger ulogger.Logger,
	deps *validationDependencies,
	validationCtx *validationContext,
) *subtreeWorkerPool {
	workerCtx, cancel := context.WithCancel(ctx)

	// Buffered channel to prevent workers from blocking when submitting jobs
	// Buffer size = numWorkers * 2 provides good balance
	bufferSize := numWorkers * 2
	if bufferSize > numSubtrees {
		bufferSize = numSubtrees
	}

	return &subtreeWorkerPool{
		numWorkers:    numWorkers,
		jobs:          make(chan subtreeValidationJob, bufferSize),
		ctx:           workerCtx,
		cancel:        cancel,
		block:         block,
		logger:        logger,
		deps:          deps,
		validationCtx: validationCtx,
		results:       make([]subtreeValidationResult, numSubtrees),
	}
}

// Start launches all worker goroutines
func (p *subtreeWorkerPool) Start() {
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// worker processes jobs from the channel until it's closed or context is cancelled
func (p *subtreeWorkerPool) worker() {
	defer p.wg.Done()

	for {
		select {
		case job, ok := <-p.jobs:
			if !ok {
				return // Channel closed, exit worker
			}
			p.processJob(job)

		case <-p.ctx.Done():
			return // Context cancelled, exit worker
		}
	}
}

// processJob performs validation for a single subtree
// This is the extracted logic from Block.go lines 692-693
func (p *subtreeWorkerPool) processJob(job subtreeValidationJob) {
	// Call the existing validateSubtree method
	err := p.block.validateSubtree(
		p.ctx,
		p.logger,
		p.deps,
		p.validationCtx,
		job.subtree,
		job.subtreeIndex,
	)

	// Store result at the job's index (no lock needed - unique index per job)
	p.results[job.subtreeIndex].err = err
}

// Submit adds a job to the worker pool
func (p *subtreeWorkerPool) Submit(job subtreeValidationJob) {
	p.jobs <- job
}

// Close closes the job channel and waits for all workers to finish
func (p *subtreeWorkerPool) Close() {
	close(p.jobs)
	p.wg.Wait()
}

// Shutdown gracefully stops all workers by cancelling the context
func (p *subtreeWorkerPool) Shutdown() {
	p.cancel()
	close(p.jobs)
	p.wg.Wait()
}

// getOptimalSubtreeWorkerCount calculates the optimal number of workers based on
// available CPU cores and the number of subtrees to process
func getOptimalSubtreeWorkerCount(numSubtrees int, configuredSize int) int {
	// If explicitly configured, use that value
	if configuredSize > 0 {
		return configuredSize
	}

	// Default: 64x CPU cores for I/O-heavy subtree validation
	// Subtree validation is ~97% I/O (file reads from blob store)
	// High concurrency needed to saturate disk I/O throughput
	// On 8-core machine: 512 workers
	numWorkers := runtime.GOMAXPROCS(0) * 64

	// Don't create more workers than subtrees
	if numWorkers > numSubtrees {
		numWorkers = numSubtrees
	}

	// Always have at least 1 worker
	if numWorkers < 1 {
		numWorkers = 1
	}

	return numWorkers
}
