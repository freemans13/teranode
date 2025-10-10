// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"net/http"
	"time"

	"github.com/bsv-blockchain/go-batcher"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

// batchItem represents an item in a transaction batch.
// This structure encapsulates a transaction request along with a channel
// for signaling completion, enabling asynchronous batch processing while
// still allowing the caller to wait for individual transaction results.

type batchItem struct {
	// req contains the transaction request
	req *blockassembly_api.AddTxRequest

	// done signals completion of batch processing
	done chan error
}

// Client implements the ClientI interface for block assembly operations.
// It provides a high-level API for interacting with the block assembly service,
// handling communication details, request formatting, and response processing.
//
// This client includes built-in batching support for transaction submission to improve
// performance when processing large numbers of transactions. It also provides methods
// for mining operations, service status checks, and block assembly management.

type Client struct {
	// client is the gRPC client for block assembly API
	client blockassembly_api.BlockAssemblyAPIClient

	// logger provides logging functionality
	logger ulogger.Logger

	// settings contains configuration parameters
	settings *settings.Settings

	// batchSize determines the size of transaction batches
	batchSize int

	// batchCh handles batch processing
	batchCh chan []*batchItem

	// batcher manages transaction batching
	batcher batcher.Batcher[batchItem]
}

// NewClient creates a new block assembly client.
//
// Parameters:
//   - ctx: Context for cancellation
//   - logger: Logger for operations
//   - tSettings: Teranode settings configuration
//
// Returns:
//   - *Client: New client instance
//   - error: Any error encountered during creation
func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (*Client, error) {
	blockAssemblyGrpcAddress := tSettings.BlockAssembly.GRPCAddress
	if blockAssemblyGrpcAddress == "" {
		return nil, errors.NewConfigurationError("no blockassembly_grpcAddress setting found")
	}

	maxRetries := tSettings.BlockAssembly.GRPCMaxRetries

	retryBackoff := tSettings.BlockAssembly.GRPCRetryBackoff
	if retryBackoff == 0 {
		return nil, errors.NewConfigurationError("blockassembly_grpcRetryBackoff setting error")
	}

	baConn, err := util.GetGRPCClient(
		ctx,
		blockAssemblyGrpcAddress,
		&util.ConnectionOptions{
			MaxRetries:   maxRetries,
			RetryBackoff: retryBackoff,
		}, tSettings,
	)
	if err != nil {
		return nil, errors.NewServiceError("failed to connect to block assembly", err)
	}

	batchSize := tSettings.BlockAssembly.SendBatchSize
	sendBatchTimeout := tSettings.BlockAssembly.SendBatchTimeout

	if batchSize > 0 {
		logger.Infof("Using batch mode to send transactions to block assembly, batches: %d, timeout: %d", batchSize, sendBatchTimeout)
	}

	duration := time.Duration(sendBatchTimeout) * time.Millisecond

	client := &Client{
		client:    blockassembly_api.NewBlockAssemblyAPIClient(baConn),
		logger:    logger,
		settings:  tSettings,
		batchSize: batchSize,
		batchCh:   make(chan []*batchItem),
	}

	sendBatch := func(batch []*batchItem) {
		client.sendBatchToBlockAssembly(ctx, batch)
	}
	client.batcher = *batcher.New(batchSize, duration, sendBatch, true)

	return client, nil
}

// NewClientWithAddress creates a new block assembly client with a specific address.
//
// Parameters:
//   - ctx: Context for cancellation
//   - logger: Logger for operations
//   - tSettings: Teranode settings configuration
//   - blockAssemblyGrpcAddress: Specific gRPC address for block assembly
//
// Returns:
//   - *Client: New client instance
//   - error: Any error encountered during creation
func NewClientWithAddress(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, blockAssemblyGrpcAddress string) (*Client, error) {
	baConn, err := util.GetGRPCClient(ctx, blockAssemblyGrpcAddress, &util.ConnectionOptions{
		MaxRetries:   tSettings.GRPCMaxRetries,
		RetryBackoff: tSettings.GRPCRetryBackoff,
	}, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("failed to connect to block assembly", err)
	}

	batchSize := tSettings.BlockAssembly.SendBatchSize
	sendBatchTimeout := tSettings.BlockAssembly.SendBatchTimeout

	if batchSize > 0 {
		logger.Infof("Using batch mode to send transactions to block assembly, batches: %d, timeout: %dms", batchSize, sendBatchTimeout)
	}

	duration := time.Duration(sendBatchTimeout) * time.Millisecond

	client := &Client{
		client:    blockassembly_api.NewBlockAssemblyAPIClient(baConn),
		logger:    logger,
		settings:  tSettings,
		batchSize: batchSize,
		batchCh:   make(chan []*batchItem),
	}

	sendBatch := func(batch []*batchItem) {
		client.sendBatchToBlockAssembly(ctx, batch)
	}
	client.batcher = *batcher.New(batchSize, duration, sendBatch, true)

	return client, nil
}

// Health checks the health status of the block assembly service.
//
// Parameters:
//   - ctx: Context for cancellation
//   - checkLiveness: Whether to perform liveness check
//
// Returns:
//   - int: HTTP status code indicating health state
//   - string: Health status message
//   - error: Any error encountered during health check
func (s *Client) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	resp, err := s.client.HealthGRPC(ctx, &blockassembly_api.EmptyMessage{})
	if err != nil {
		return http.StatusFailedDependency, "", errors.UnwrapGRPC(err)
	}

	if !resp.GetOk() {
		details := ""
		if resp != nil {
			details = resp.GetDetails()
		}
		return http.StatusFailedDependency, details, errors.NewServiceError("health check failed: %s", details)
	}

	return http.StatusOK, "OK", nil
}

// Store stores a transaction in block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//   - hash: Transaction hash
//   - fee: Transaction fee in satoshis
//   - size: Transaction size in bytes
//
// Returns:
//   - bool: True if storage was successful
//   - error: Any error encountered during storage
func (s *Client) Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64, txInpoints subtree.TxInpoints) (bool, error) {
	txInpointsBytes, err := txInpoints.Serialize()
	if err != nil {
		return false, err
	}

	req := &blockassembly_api.AddTxRequest{
		Txid:       hash[:],
		Fee:        fee,
		Size:       size,
		TxInpoints: txInpointsBytes,
	}

	if s.batchSize == 0 {
		if _, err := s.client.AddTx(ctx, req); err != nil {
			return false, errors.UnwrapGRPC(err)
		}
	} else {
		/* batch mode */
		done := make(chan error)
		s.batcher.Put(&batchItem{
			req:  req,
			done: done,
		})

		err := <-done
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

// RemoveTx removes a transaction from block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//   - hash: Hash of transaction to remove
//
// Returns:
//   - error: Any error encountered during removal
func (s *Client) RemoveTx(ctx context.Context, hash *chainhash.Hash) error {
	_, err := s.client.RemoveTx(ctx, &blockassembly_api.RemoveTxRequest{
		Txid: hash[:],
	})

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return nil
	}

	return unwrappedErr
}

// GetMiningCandidate retrieves a candidate block for mining.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - *model.MiningCandidate: Mining candidate block
//   - error: Any error encountered during retrieval
func (s *Client) GetMiningCandidate(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
	includeSubtrees := false
	if len(includeSubtreeHashes) > 0 {
		includeSubtrees = includeSubtreeHashes[0]
	}

	req := &blockassembly_api.GetMiningCandidateRequest{
		IncludeSubtrees: includeSubtrees,
	}

	res, err := s.client.GetMiningCandidate(ctx, req)
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return res, nil
}

// GetCurrentDifficulty retrieves the current mining difficulty.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - float64: Current difficulty value
//   - error: Any error encountered during retrieval
func (s *Client) GetCurrentDifficulty(ctx context.Context) (float64, error) {
	req := &blockassembly_api.EmptyMessage{}

	res, err := s.client.GetCurrentDifficulty(ctx, req)
	if err != nil {
		return 0, errors.UnwrapGRPC(err)
	}

	return res.Difficulty, nil
}

// SubmitMiningSolution submits a solution for a mined block.
//
// Parameters:
//   - ctx: Context for cancellation
//   - solution: Mining solution to submit
//
// Returns:
//   - error: Any error encountered during submission
func (s *Client) SubmitMiningSolution(ctx context.Context, solution *model.MiningSolution) error {
	_, err := s.client.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         solution.Id,
		Nonce:      solution.Nonce,
		CoinbaseTx: solution.Coinbase,
		Time:       solution.Time,
		Version:    solution.Version,
	})

	if retErr := errors.UnwrapGRPC(err); retErr != nil {
		return retErr
	}

	return nil
}

// GenerateBlocks generates a specified number of blocks.
//
// Parameters:
//   - ctx: Context for cancellation
//   - req: Block generation request parameters
//
// Returns:
//   - error: Any error encountered during generation
func (s *Client) GenerateBlocks(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
	_, err := s.client.GenerateBlocks(ctx, req)

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return nil
	}

	return unwrappedErr
}

// sendBatchToBlockAssembly sends a batch of transactions to block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//   - batch: Batch of transactions to send
func (s *Client) sendBatchToBlockAssembly(ctx context.Context, batch []*batchItem) {
	txRequests := make([]*blockassembly_api.AddTxRequest, len(batch))
	for i, item := range batch {
		txRequests[i] = item.req
	}

	txBatch := &blockassembly_api.AddTxBatchRequest{
		TxRequests: txRequests,
	}

	_, err := s.client.AddTxBatch(ctx, txBatch)
	if err != nil {
		s.logger.Errorf("%v", err)

		for _, item := range batch {
			item.done <- errors.UnwrapGRPC(err)
		}

		return
	}

	for _, item := range batch {
		item.done <- nil
	}
}

// ResetBlockAssembly triggers a reset of the block assembly state.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - error: Any error encountered during reset
func (s *Client) ResetBlockAssembly(ctx context.Context) error {
	_, err := s.client.ResetBlockAssembly(ctx, &blockassembly_api.EmptyMessage{})

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return nil
	}

	return unwrappedErr
}

// ResetBlockAssemblyFully triggers a full reset of the block assembly state.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - error: Any error encountered during reset
func (s *Client) ResetBlockAssemblyFully(ctx context.Context) error {
	_, err := s.client.ResetBlockAssemblyFully(ctx, &blockassembly_api.EmptyMessage{})

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return nil
	}

	return unwrappedErr
}

// GetBlockAssemblyState retrieves the current state of block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - *blockassembly_api.StateMessage: Current state
//   - error: Any error encountered during retrieval
func (s *Client) GetBlockAssemblyState(ctx context.Context) (*blockassembly_api.StateMessage, error) {
	state, err := s.client.GetBlockAssemblyState(ctx, &blockassembly_api.EmptyMessage{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return state, nil
}

// BlockAssemblyAPIClient returns the underlying gRPC client for block assembly API.
//
// Returns:
//   - blockassembly_api.BlockAssemblyAPIClient: The gRPC client instance
func (s *Client) BlockAssemblyAPIClient() blockassembly_api.BlockAssemblyAPIClient {
	return s.client
}

// GetBlockAssemblyBlockCandidate retrieves the current block candidate in block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - *model.Block: Block candidate
//   - error: Any error encountered during retrieval
func (s *Client) GetBlockAssemblyBlockCandidate(ctx context.Context) (*model.Block, error) {
	resp, err := s.client.GetBlockAssemblyBlockCandidate(ctx, &blockassembly_api.EmptyMessage{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	block, err := model.NewBlockFromBytes(resp.Block)
	if err != nil {
		return nil, errors.NewServiceError("failed to create block from bytes", err)
	}

	return block, nil
}

// GetTransactionHashes retrieves all transaction hashes in block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - []string: List of transaction hashes
//   - error: Any error encountered during retrieval
func (s *Client) GetTransactionHashes(ctx context.Context) ([]string, error) {
	resp, err := s.client.GetBlockAssemblyTxs(ctx, &blockassembly_api.EmptyMessage{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return resp.Txs, nil
}
