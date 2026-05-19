/*
Package validator implements BSV Blockchain transaction validation functionality.

This file implements option patterns for both general validation options and
transaction validator-specific options, providing flexible configuration for
validation operations.
*/
package validator

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// Options defines the configuration options for validation operations
type Options struct {
	// SkipUtxoCreation determines whether UTXO creation should be skipped
	// When true, the validator won't create new UTXOs for transaction outputs
	SkipUtxoCreation bool

	// AddTXToBlockAssembly determines whether transactions should be added to block assembly
	// When true, validated transactions are forwarded to the block assembly process
	AddTXToBlockAssembly bool

	// SkipPolicyChecks determines whether policy checks should be skipped
	// this is done when validating transaction from a block that has been mined
	SkipPolicyChecks bool

	// CreateConflicting determines whether to allow conflicting transactions
	// this is done when validating transaction from a block that has been mined
	CreateConflicting bool

	// IgnoreConflicting determines whether to ignore transactions marked as conflicting when spending
	IgnoreConflicting bool

	// IgnoreLocked determines whether to ignore transactions marked as locked when spending
	IgnoreLocked bool

	// ParentBlockHeights provides pre-fetched block heights for parent transactions
	// When provided, the validator will check this map before calling utxoStore.Get()
	// This enables validation to proceed without UTXO store lookups for in-block parents
	// Key: parent transaction hash, Value: block height where parent was mined
	ParentBlockHeights map[chainhash.Hash]uint32

	// PrefetchedParents provides pre-fetched full transaction metadata for level 0 parents
	// This is populated by ValidateLevelBatch before processing workers start
	// Workers check this map first, eliminating individual Get() calls to UTXO store
	// Key: parent transaction hash, Value: full metadata (block heights, transaction data)
	PrefetchedParents map[chainhash.Hash]*meta.Data

	// AutoExtendTransactions determines whether transactions should be automatically extended
	// with in-block parent output data. When true, the validator will use ParentBlockHeights
	// to pre-populate transaction inputs with parent output information, eliminating the
	// need for UTXO store fetches for in-block dependencies (~500MB+ savings per block)
	AutoExtendTransactions bool

	// MaxBatchSize limits the maximum number of transactions to process in a single batch
	// When set to 0 (default), all transactions are processed in one batch
	// For large transaction sets, setting this value helps control memory usage by
	// processing transactions in smaller batches sequentially
	MaxBatchSize int

	// WorkerPoolSize sets the number of validation workers for parallel processing
	// When set to 0 (default), uses runtime.GOMAXPROCS(0) * 64 workers (~512 on 8-core)
	//
	// Validation is I/O-heavy (UTXO fetches via Aerospike), requiring high concurrency
	// to saturate UTXO batchers and maintain throughput.
	//
	// Tuning guidelines:
	//   - Pure CPU work (no UTXO lookups): 2-4x CPU cores
	//   - Mixed CPU/I/O (typical blocks): 16-64x CPU cores
	//   - I/O-heavy (many UTXO lookups): 64-128x CPU cores
	//
	// Monitor: teranode_validator_worker_pool_job_latency for bottlenecks
	WorkerPoolSize int

	// SkipScriptVerification determines whether to skip CPU-intensive script verification
	// When true, the validator will skip script execution/validation entirely
	// This is useful during block catchup where transactions are already confirmed on-chain
	SkipScriptVerification bool

	// ReuseWorkerPool allows reusing an existing worker pool across multiple levels
	// When set, ValidateLevelBatch will use this pool instead of creating a new one
	// This significantly reduces overhead by avoiding repeated goroutine creation/teardown
	// Internal use only - set by ValidateMulti to enable worker pool reuse optimization
	ReuseWorkerPool *validationWorkerPool

	// SkipLevelOrganization bypasses DAG construction and processes all transactions as a single level
	// When true, ValidateMulti will not organize transactions by dependency levels
	// Use this when transactions are already known to be at the same level or when
	// level organization overhead needs to be eliminated for benchmarking
	SkipLevelOrganization bool

	// BatchSize splits each level into smaller batches for concurrent processing
	// When > 0, each level is divided into batches of this size and processed concurrently
	// This improves CPU utilization by allowing multiple batch operations to run in parallel
	// Default: 0 (process entire level as one batch)
	BatchSize int

	// SkipTxMetaPublishing determines whether txmeta should be published to Kafka
	// When true, the validator won't publish transaction metadata to the txmeta Kafka topic
	// Used during legacy catchup (quickValidationMode) where no consumer needs the data
	SkipTxMetaPublishing bool
}

// Option defines a function type for setting options
// This follows the functional options pattern for flexible configuration
type Option func(*Options)

// NewDefaultOptions creates a new Options instance with default settings
// Default configuration:
//   - skipUtxoCreation: false (UTXOs will be created)
//   - addTXToBlockAssembly: true (transactions will be added to block assembly)
//
// Returns:
//   - *Options: New options instance with default settings
func NewDefaultOptions() *Options {
	return &Options{
		SkipUtxoCreation:     false,
		AddTXToBlockAssembly: true,
		SkipPolicyChecks:     false,
		CreateConflicting:    false,
	}
}

// ProcessOptions applies the provided options to a new Options instance
// Parameters:
//   - opts: Variable number of Option functions to apply
//
// Returns:
//   - *Options: Configured options instance
func ProcessOptions(opts ...Option) *Options {
	options := NewDefaultOptions()
	for _, o := range opts {
		o(options)
	}

	return options
}

// WithSkipUtxoCreation creates an option to control UTXO creation
// Parameters:
//   - skip: When true, UTXO creation will be skipped
//
// Returns:
//   - Option: Function that sets the skipUtxoCreation option
func WithSkipUtxoCreation(skip bool) Option {
	return func(o *Options) {
		o.SkipUtxoCreation = skip
	}
}

// WithAddTXToBlockAssembly creates an option to control block assembly integration (allows the transaction to be added to the block assembly or not)
// Parameters:
//   - add: When true, transactions will be added to block assembly
//
// Returns:
//   - Option: Function that sets the addTXToBlockAssembly option
func WithAddTXToBlockAssembly(add bool) Option {
	return func(o *Options) {
		o.AddTXToBlockAssembly = add
	}
}

// WithSkipPolicyChecks creates an option to control policy checks
// Parameters:
//   - skip: When true, policy checks will be skipped
//
// Returns:
//   - Option: Function that sets the skipPolicyChecks option
func WithSkipPolicyChecks(skip bool) Option {
	return func(o *Options) {
		o.SkipPolicyChecks = skip
	}
}

// WithCreateConflicting creates an option to control whether a conflicting transaction is created
// Parameters:
//   - create: When true, a conflicting transaction will be created
//
// Returns:
//   - Option: Function that sets the createConflicting option
func WithCreateConflicting(create bool) Option {
	return func(o *Options) {
		o.CreateConflicting = create
	}
}

// WithIgnoreConflicting creates an option to control whether a conflicting transaction is ignored
// Parameters:
//   - ignore: When true, a conflicting transaction will be ignored
//
// Returns:
//   - Option: Function that sets the ignoreConflicting option
func WithIgnoreConflicting(ignore bool) Option {
	return func(o *Options) {
		o.IgnoreConflicting = ignore
	}
}

// WithIgnoreLocked creates an option to control whether the locked flag will be ignored when spending UTXOs
// Parameters:
//   - ignoreLocked: When true, transactions marked as locked will also be processed
//
// Returns:
//   - Option: Function that sets the ignoreLocked option
func WithIgnoreLocked(ignoreLocked bool) Option {
	return func(o *Options) {
		o.IgnoreLocked = ignoreLocked
	}
}

// WithSkipTxMetaPublishing creates an option to control txmeta Kafka publishing
// Parameters:
//   - skip: When true, txmeta will not be published to Kafka
//
// Returns:
//   - Option: Function that sets the skipTxMetaPublishing option
func WithSkipTxMetaPublishing(skip bool) Option {
	return func(o *Options) {
		o.SkipTxMetaPublishing = skip
	}
}

// WithSkipScriptVerification creates an option to control whether script verification should be skipped
// Parameters:
//   - skip: When true, CPU-intensive script verification will be skipped
//
// Returns:
//   - Option: Function that sets the skipScriptVerification option
func WithSkipScriptVerification(skip bool) Option {
	return func(o *Options) {
		o.SkipScriptVerification = skip
	}
}

// TxValidatorOptions defines configuration options specific to transaction validation
type TxValidatorOptions struct {
	skipPolicyChecks bool
}

// NewTxValidatorOptions creates a new TxValidatorOptions instance with the provided options applied.
func NewTxValidatorOptions(opts ...TxValidatorOption) *TxValidatorOptions {
	options := &TxValidatorOptions{}
	for _, opt := range opts {
		opt(options)
	}

	return options
}

// TxValidatorOption defines a function type for setting transaction validator options
// This follows the functional options pattern for flexible configuration
type TxValidatorOption func(*TxValidatorOptions)

// WithTxValidatorSkipPolicyChecks creates an option to skip policy checks during validation.
func WithTxValidatorSkipPolicyChecks(skip bool) TxValidatorOption {
	return func(o *TxValidatorOptions) {
		o.skipPolicyChecks = skip
	}
}
