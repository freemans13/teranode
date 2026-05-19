/*
Package validator implements BSV Blockchain transaction validation functionality.

This package provides a comprehensive transaction validation framework that implements
the Bitcoin consensus and policy rules. It serves as a critical component in the
Teranode architecture, ensuring that only valid transactions are accepted into the
mempool and blocks.

This file defines the core interfaces for the validator service, providing the contract
that all validator implementations must fulfill. The Interface type establishes the
required methods for any validator implementation, ensuring consistent behavior across
different implementations or testing scenarios. The file also includes a mock implementation
for testing purposes that satisfies the interface contract without performing actual
validation.

The validator package interacts with multiple other components in the system:
- UTXO store for input validation and double-spend prevention
- Block assembly for transaction prioritization and mining
- Blockchain service for block height and chain state information
- Kafka for asynchronous event processing and communication

These interfaces enable loose coupling between components while enforcing the necessary
validation contract to maintain Bitcoin consensus rules.
*/
package validator

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
)

// TxValidationResult contains the validation result for a single transaction
// in a multi-transaction validation operation
type TxValidationResult struct {
	// Success indicates whether the transaction validated successfully
	Success bool

	// TxMeta contains the transaction metadata if validation was successful
	// This field is nil if validation failed
	TxMeta *meta.Data

	// ConflictingTxID contains the hash of the conflicting transaction if
	// validation failed due to a double-spend conflict. This field is nil
	// if there was no conflict or if validation failed for another reason
	ConflictingTxID *chainhash.Hash

	// Err contains the validation error if validation failed
	// This field is nil if validation was successful
	Err error
}

// MultiResult contains the validation results for multiple transactions
type MultiResult struct {
	// Results maps transaction hashes to their validation results
	// Each entry contains success status, metadata, conflict info, and any errors
	Results map[chainhash.Hash]*TxValidationResult
}

// LevelValidationResult contains the validation result for a single transaction
// in a level-based batch validation operation
type LevelValidationResult struct {
	// TxHash is the transaction hash
	TxHash *chainhash.Hash

	// TxMeta contains the transaction metadata if validation succeeded
	// This field is nil if validation failed
	TxMeta *meta.Data

	// ConflictingTxID contains the hash of the conflicting transaction if
	// validation failed due to a double-spend conflict
	ConflictingTxID *chainhash.Hash

	// Success indicates whether the transaction validated successfully
	Success bool

	// Err contains the validation error if validation failed
	// This field is nil if validation was successful
	Err error
}

// Interface defines the core validation functionality required for Bitcoin transaction validation.
// Any implementation of this interface must provide comprehensive transaction validation
// capabilities along with health monitoring and block height management.
type Interface interface {
	// Health performs comprehensive health checks on the validator implementation to ensure
	// proper operation and readiness to process transactions. This method validates internal
	// state, connectivity to dependent services, and resource availability.
	//
	// Parameters:
	//   - ctx: Context for the health check operation, supports cancellation and timeouts
	//   - checkLiveness: If true, performs only basic liveness checks; if false, performs
	//     comprehensive readiness checks including database connectivity and service dependencies
	//
	// Returns:
	//   - int: HTTP status code indicating health status (200 for healthy, 503 for unhealthy)
	//   - string: Detailed health status message with diagnostic information
	//   - error: Any critical errors encountered during health check that prevent operation
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// Validate performs comprehensive validation of a Bitcoin transaction according to consensus
	// rules and policy constraints. This method executes all validation steps including structure
	// validation, script execution, UTXO verification, and consensus rule enforcement.
	//
	// The validation process includes:
	// - Transaction structure and format validation
	// - Input/output consistency checks
	// - Script execution and signature verification
	// - UTXO existence and double-spend prevention
	// - Consensus rule compliance (block size, fees, etc.)
	//
	// Parameters:
	//   - ctx: Context for the validation operation, supports cancellation and timeouts
	//   - tx: The Bitcoin transaction to validate, must be properly formatted
	//   - blockHeight: Current block height for validation context and consensus rule application
	//   - opts: Optional validation settings that modify validation behavior (e.g., policy rules)
	//
	// Returns:
	//   - *meta.Data: Transaction metadata including validation results and processing information
	//   - error: Validation errors if transaction violates consensus rules or policy constraints
	Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*meta.Data, error)

	// ValidateWithOptions performs comprehensive validation of a transaction with validation
	// options passed directly rather than using variadic parameters. This method provides
	// the same validation functionality as Validate but with explicit options configuration.
	//
	// Parameters:
	//   - ctx: Context for the validation operation, supports cancellation and timeouts
	//   - tx: The Bitcoin transaction to validate, must be properly formatted
	//   - blockHeight: Current block height for validation context and consensus rule application
	//   - validationOptions: Explicit validation options configuration including policy rules
	//
	// Returns:
	//   - *meta.Data: Transaction metadata including validation results and processing information
	//   - error: Validation errors if transaction violates consensus rules or policy constraints
	ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (*meta.Data, error)

	// ValidateMulti validates multiple transactions with automatic dependency ordering
	// and batch processing. This method organizes transactions by dependency levels (DAG)
	// and processes each level in sequence, enabling efficient validation of transaction
	// sets with complex dependencies.
	//
	// The validation process includes:
	// - Automatic transaction dependency analysis and level organization
	// - Optional transaction extension with in-block parent outputs (when AutoExtendTransactions is true)
	// - Batch UTXO operations (single database roundtrip per dependency level)
	// - Memory-efficient processing with optional batch size limits
	// - Parent metadata optimization to skip UTXO fetches for in-block parents
	//
	// Parameters:
	//   - ctx: Context for the validation operation, supports cancellation and timeouts
	//   - txs: Slice of Bitcoin transactions to validate, can have interdependencies
	//   - blockHeight: Current block height for validation context and consensus rule application
	//   - opts: Validation options including AutoExtendTransactions, MaxBatchSize, and ParentBlockHeights
	//
	// Returns:
	//   - *MultiResult: Per-transaction results including success, metadata, conflicts, and errors
	//   - error: Critical errors that prevent validation (e.g., internal failures), not individual tx failures
	ValidateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts *Options) (*MultiResult, error)

	// ValidateLevelBatch validates a batch of transactions at the same dependency level
	// This method assumes all transactions in the batch are at the same level in the dependency
	// graph (i.e., they don't depend on each other). It performs optimized batch operations
	// for UTXO spend/create operations.
	//
	// Parameters:
	//   - ctx: Context for the validation operation, supports cancellation and timeouts
	//   - txs: Slice of Bitcoin transactions at the same dependency level
	//   - blockHeight: Current block height for validation context
	//   - opts: Validation options including ParentBlockHeights for optimization
	//
	// Returns:
	//   - []*LevelValidationResult: Validation results for each transaction in the batch
	//   - error: Critical errors that prevent batch validation
	ValidateLevelBatch(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts *Options) ([]*LevelValidationResult, error)

	// GetBlockHeight returns the current block height known to the validator service.
	// This height is used for validation context and consensus rule application, and should
	// reflect the latest confirmed block in the blockchain.
	//
	// Returns:
	//   - uint32: Current block height, zero if blockchain state is not available
	GetBlockHeight() uint32

	// GetMedianBlockTime returns the median timestamp of the last 11 blocks, which is used
	// for certain consensus rules including transaction locktime validation. This value
	// provides a more stable time reference than individual block timestamps.
	//
	// Returns:
	//   - uint32: Median block time in Unix timestamp format, zero if insufficient block history
	GetMedianBlockTime() uint32

	// TriggerBatcher manually triggers the transaction batch processor to process queued
	// transactions. This method is typically used for testing or administrative purposes
	// to force immediate processing of pending validation requests rather than waiting
	// for automatic batch processing intervals.
	//
	// This method does not return any values and executes asynchronously. The actual
	// batch processing results can be monitored through metrics and logging systems.
	TriggerBatcher()

	// EnsureMTPLoaded pre-warms the in-memory MTP store up to (blockHeight - 1).
	// This must be called once per block, before concurrent per-transaction goroutines start,
	// so that BIP68 MTP lookups inside each goroutine are pure array reads with no gRPC calls.
	//
	// If BIP68 is not yet active (blockHeight < CSVHeight) or no blockchain client is
	// configured, this is a no-op.
	//
	// When the store already covers the needed range this is a fast O(1) no-op.
	// When new heights extend beyond the loaded range, the fetch includes a backward
	// overlap window to detect and repair any MTP values invalidated by a chain reorg.
	//
	// Parameters:
	//   - ctx: Context for the operation, used for cancellation and tracing
	//   - blockHeight: The block being validated; the store is loaded up to (blockHeight - 1)
	//
	// Returns:
	//   - error: Error if the MTP fetch fails; the caller should abort block validation
	EnsureMTPLoaded(ctx context.Context, blockHeight uint32) error
}

// Type assertion to ensure MockValidator implements Interface
var _ Interface = &MockValidator{}

// MockValidator provides a mock implementation of the validator Interface
// This implementation is primarily used for testing purposes and provides
// no-op implementations of all required methods.
type MockValidator struct{}

// Health implements the health check for the mock validator
// Always returns success without actually performing any checks
// Parameters:
//   - ctx: Context for the health check operation (unused in mock)
//   - checkLiveness: Boolean flag for liveness check (unused in mock)
//
// Returns:
//   - int: Always returns 0
//   - string: Always returns "Mock Validator"
//   - error: Always returns nil
func (mv *MockValidator) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "Mock Validator", nil
}

// Validate implements mock transaction validation
// Always returns success without performing any actual validation
// Parameters:
//   - ctx: Context for validation (unused in mock)
//   - tx: Transaction to validate (unused in mock)
//   - blockHeight: Block height for validation context (unused in mock)
//   - opts: Optional validation settings (unused in mock)
//
// Returns:
//   - error: Always returns nil
func (mv *MockValidator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*meta.Data, error) {
	return util.TxMetaDataFromTx(tx)
}

// ValidateWithOptions implements mock transaction validation with options set directly
// Always returns success without performing any actual validation
// Parameters:
//   - ctx: Context for validation (unused in mock)
//   - tx: Transaction to validate (unused in mock)
//   - blockHeight: Block height for validation context (unused in mock)
//   - validationOptions: Validation options for the transaction (unused in mock)
//
// Returns:
//   - error: Always returns nil
func (mv *MockValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (*meta.Data, error) {
	return util.TxMetaDataFromTx(tx)
}

// ValidateMulti implements mock multi-transaction validation
// Always returns success for all transactions without performing any actual validation
// Parameters:
//   - ctx: Context for validation (unused in mock)
//   - txs: Transactions to validate (unused in mock)
//   - blockHeight: Block height for validation context (unused in mock)
//   - opts: Validation options (unused in mock)
//
// Returns:
//   - *MultiResult: Mock results with all transactions marked as successful
//   - error: Always returns nil
func (mv *MockValidator) ValidateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts *Options) (*MultiResult, error) {
	results := make(map[chainhash.Hash]*TxValidationResult)
	for _, tx := range txs {
		txMeta, _ := util.TxMetaDataFromTx(tx)
		results[*tx.TxIDChainHash()] = &TxValidationResult{
			Success: true,
			TxMeta:  txMeta,
			Err:     nil,
		}
	}
	return &MultiResult{Results: results}, nil
}

// ValidateLevelBatch implements mock level-based batch validation
// Always returns success for all transactions without performing any actual validation
// Parameters:
//   - ctx: Context for validation (unused in mock)
//   - txs: Transactions to validate (unused in mock)
//   - blockHeight: Block height for validation context (unused in mock)
//   - opts: Validation options (unused in mock)
//
// Returns:
//   - []*LevelValidationResult: Mock results with all transactions marked as successful
//   - error: Always returns nil
func (mv *MockValidator) ValidateLevelBatch(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts *Options) ([]*LevelValidationResult, error) {
	results := make([]*LevelValidationResult, len(txs))
	for i, tx := range txs {
		txHash := tx.TxIDChainHash()
		txMeta, _ := util.TxMetaDataFromTx(tx)
		results[i] = &LevelValidationResult{
			TxHash:  txHash,
			TxMeta:  txMeta,
			Success: true,
			Err:     nil,
		}
	}
	return results, nil
}

// GetBlockHeight implements mock block height retrieval
// Always returns 0 without actually checking any block height
// Returns:
//   - uint32: Always returns 0
func (mv *MockValidator) GetBlockHeight() uint32 {
	return 0
}

// GetMedianBlockTime implements mock median block time retrieval
// Always returns 0 without calculating any actual median time
// Returns:
//   - uint32: Always returns 0
func (mv *MockValidator) GetMedianBlockTime() uint32 {
	return 0
}

// TriggerBatcher implements mock batch triggering
// No-op implementation that does nothing
func (mv *MockValidator) TriggerBatcher() {}

// EnsureMTPLoaded implements mock MTP store pre-warming
// No-op implementation that does nothing
func (mv *MockValidator) EnsureMTPLoaded(ctx context.Context, blockHeight uint32) error {
	return nil
}
