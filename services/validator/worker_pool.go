package validator

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
)

// validationResult stores the result of validating a single transaction
type validationResult struct {
	utxoHeights []uint32
	err         error
}

// validationJob represents a single transaction validation job
type validationJob struct {
	txIndex int    // Index in the original transaction slice
	tx      *bt.Tx // Transaction to validate
}

// validationWorkerPool manages a fixed pool of validation workers
// for processing transaction validations with minimal scheduler overhead
type validationWorkerPool struct {
	numWorkers int
	jobs       chan validationJob
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc

	// Shared validation state (read-only, no contention)
	validator   *Validator
	blockHeight uint32
	blockState  utxo.BlockState
	opts        *Options

	// Results storage (each worker writes to different index, no locking needed)
	results []validationResult

	// Batch tracking for reusable pool
	batchWg       sync.WaitGroup
	batchMutex    sync.Mutex
	trackingBatch bool
}

// newValidationWorkerPool creates a worker pool with the specified number of workers
func newValidationWorkerPool(ctx context.Context, v *Validator, numWorkers int, numJobs int, blockHeight uint32, blockState utxo.BlockState, opts *Options) *validationWorkerPool {
	workerCtx, cancel := context.WithCancel(ctx)

	// Buffered channel to prevent workers from blocking when submitting jobs
	// Buffer size = numWorkers * 2 provides good balance
	bufferSize := numWorkers * 2
	if bufferSize > numJobs {
		bufferSize = numJobs
	}

	// Record worker pool size metric
	prometheusValidatorWorkerPoolSize.Observe(float64(numWorkers))

	return &validationWorkerPool{
		numWorkers:  numWorkers,
		jobs:        make(chan validationJob, bufferSize),
		ctx:         workerCtx,
		cancel:      cancel,
		validator:   v,
		blockHeight: blockHeight,
		blockState:  blockState,
		opts:        opts,
		results:     make([]validationResult, numJobs),
	}
}

// Start launches all worker goroutines
func (p *validationWorkerPool) Start() {
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// worker processes jobs from the channel until it's closed or context is cancelled
func (p *validationWorkerPool) worker() {
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

// processJob performs validation for a single transaction
// This is the extracted logic from ValidateLevelBatch lines 92-150
func (p *validationWorkerPool) processJob(job validationJob) {
	// Track job processing latency
	startTime := time.Now()
	defer func() {
		// Convert to microseconds for the metric
		latencyMicros := float64(time.Since(startTime).Microseconds())
		prometheusValidatorWorkerPoolJobLatency.Observe(latencyMicros)
		// Signal batch completion if tracking batches
		if p.trackingBatch {
			p.batchWg.Done()
		}
	}()

	tx := job.tx
	tx.SetTxHash(tx.TxIDChainHash())
	txID := tx.TxIDChainHash().String()

	result := &p.results[job.txIndex]

	// Check IsFinal (consensus rule - cannot skip)
	if p.blockHeight > p.validator.settings.ChainCfgParams.CSVHeight {
		if p.blockState.MedianTime == 0 {
			result.err = errors.NewProcessingError("utxo store not ready, median block time: 0")
			return
		}
		if err := util.IsTransactionFinal(tx, p.blockHeight, p.blockState.MedianTime); err != nil {
			result.err = errors.NewUtxoNonFinalError("[ValidateLevelBatch][%s] transaction is not final", txID, err)
			return
		}
	}

	// Check coinbase (consensus rule - cannot skip)
	if tx.IsCoinbase() {
		result.err = errors.NewProcessingError("[ValidateLevelBatch][%s] coinbase transactions are not supported", txID)
		return
	}

	var utxoHeights []uint32

	// Get UTXO heights and extend if needed
	// Uses ParentMetadata optimization for level 1+ (no UTXO fetch)
	// Uses batchers for level 0 (unavoidable UTXO fetch, but batched)
	if !tx.IsExtended() {
		var err error
		utxoHeights, err = p.validator.getTransactionInputBlockHeightsAndExtendTx(p.ctx, tx, txID, p.opts)
		if err != nil {
			result.err = errors.NewProcessingError("[ValidateLevelBatch][%s] error getting transaction input block heights", txID, err)
			return
		}
	}

	// Validate transaction format and consensus rules
	if err := p.validator.validateTransaction(p.ctx, tx, p.blockHeight, utxoHeights, p.opts); err != nil {
		result.err = errors.NewProcessingError("[ValidateLevelBatch][%s] error validating transaction", txID, err)
		return
	}

	// Get utxo heights if not already fetched (transaction was pre-extended)
	if len(utxoHeights) == 0 {
		var err error
		utxoHeights, err = p.validator.getTransactionInputBlockHeightsAndExtendTx(p.ctx, tx, txID, p.opts)
		if err != nil {
			result.err = errors.NewProcessingError("[ValidateLevelBatch][%s] error getting transaction input block heights", txID, err)
			return
		}
	}

	// Validate scripts and signatures
	if err := p.validator.validateTransactionScripts(p.ctx, tx, p.blockHeight, utxoHeights, p.opts); err != nil {
		result.err = errors.NewProcessingError("[ValidateLevelBatch][%s] error validating transaction scripts", txID, err)
		return
	}

	result.utxoHeights = utxoHeights
}

// Submit adds a job to the worker pool
func (p *validationWorkerPool) Submit(job validationJob) {
	p.jobs <- job
}

// Close closes the job channel and waits for all workers to finish
func (p *validationWorkerPool) Close() {
	close(p.jobs)
	p.wg.Wait()
}

// ProcessBatch processes a batch of transactions using the existing worker pool
// This method allows reusing the worker pool across multiple batches without
// recreating goroutines, which significantly reduces overhead.
func (p *validationWorkerPool) ProcessBatch(txs []*bt.Tx) []validationResult {
	p.batchMutex.Lock()

	// Resize results slice if needed
	if cap(p.results) < len(txs) {
		p.results = make([]validationResult, len(txs))
	} else {
		p.results = p.results[:len(txs)]
		// Clear existing results
		for i := range p.results {
			p.results[i] = validationResult{}
		}
	}

	// Enable batch tracking
	p.trackingBatch = true
	p.batchWg.Add(len(txs))
	p.batchMutex.Unlock()

	// Submit all jobs
	for i, tx := range txs {
		p.jobs <- validationJob{
			txIndex: i,
			tx:      tx,
		}
	}

	// Wait for all jobs in this batch to complete
	p.batchWg.Wait()

	// Disable batch tracking for next batch
	p.batchMutex.Lock()
	p.trackingBatch = false
	p.batchMutex.Unlock()

	return p.results
}

// Shutdown gracefully stops all workers by cancelling the context
func (p *validationWorkerPool) Shutdown() {
	p.cancel()
	close(p.jobs)
	p.wg.Wait()
}

// getOptimalWorkerCount calculates the optimal number of workers based on
// available CPU cores and the number of transactions to process
func getOptimalWorkerCount(numTransactions int, configuredSize int, opts *Options) int {
	// If explicitly configured, use that value
	if configuredSize > 0 {
		return configuredSize
	}

	numCPU := runtime.GOMAXPROCS(0)

	// Use a fixed 12x multiplier for balanced CPU/I/O workload
	// This matches the sizing expected by tests and current defaults.
	multiplier := 12

	numWorkers := numCPU * multiplier

	// Don't create more workers than transactions
	if numWorkers > numTransactions {
		numWorkers = numTransactions
	}

	// Always have at least 1 worker
	if numWorkers < 1 {
		numWorkers = 1
	}

	return numWorkers
}

// kafkaNotificationJob represents a Kafka notification job
type kafkaNotificationJob struct {
	tx     *bt.Tx
	txMeta *meta.Data
}

// kafkaNotificationWorkerPool manages Kafka notification workers
// for concurrent Kafka message publishing with minimal overhead
type kafkaNotificationWorkerPool struct {
	numWorkers int
	jobs       chan kafkaNotificationJob
	wg         sync.WaitGroup
	validator  *Validator
}

// newKafkaNotificationWorkerPool creates a worker pool for Kafka notifications
func newKafkaNotificationWorkerPool(v *Validator, numWorkers int, numJobs int) *kafkaNotificationWorkerPool {
	// Buffered channel to prevent blocking
	bufferSize := numWorkers * 2
	if bufferSize > numJobs {
		bufferSize = numJobs
	}

	return &kafkaNotificationWorkerPool{
		numWorkers: numWorkers,
		jobs:       make(chan kafkaNotificationJob, bufferSize),
		validator:  v,
	}
}

// Start launches all Kafka worker goroutines
func (p *kafkaNotificationWorkerPool) Start() {
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// worker processes Kafka notification jobs from the channel
func (p *kafkaNotificationWorkerPool) worker() {
	defer p.wg.Done()

	for job := range p.jobs {
		if err := p.validator.sendTxMetaToKafka(job.txMeta, job.tx.TxIDChainHash()); err != nil {
			p.validator.logger.Errorf("[KafkaWorkerPool][%s] error sending to Kafka: %v", job.tx.TxID(), err)
		}
	}
}

// Submit adds a Kafka notification job to the worker pool
func (p *kafkaNotificationWorkerPool) Submit(job kafkaNotificationJob) {
	p.jobs <- job
}

// Close closes the job channel and waits for all workers to finish
func (p *kafkaNotificationWorkerPool) Close() {
	close(p.jobs)
	p.wg.Wait()
}
