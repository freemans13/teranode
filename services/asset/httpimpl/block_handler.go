// Package httpimpl provides HTTP handlers and server implementations for blockchain data retrieval and analysis.
package httpimpl

import (
	"fmt"
	"net/http"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
)

// BlockHandler handles block-related operations API endpoints, providing administrative
// capabilities for block validation management. It implements RESTful endpoints for
// invalidating and revalidating blocks, as well as retrieving information about invalid blocks.
//
// The handler communicates with the blockchain service to execute operations on blocks,
// translating between HTTP request/response formats and the internal blockchain service interface.
// It includes validation of input parameters and proper error handling for all operations.
type BlockHandler struct {
	// blockchainClient provides access to the blockchain service for executing operations
	blockchainClient blockchain.ClientI
	// logger enables structured logging of handler operations and errors
	logger ulogger.Logger
}

// blockOperation represents a function that performs an operation on a block.
// This type is used as a callback for the common handler logic, allowing different
// block operations to reuse the same request parsing, validation, and error handling code.
// The function takes an Echo context and block hash as parameters and returns any error encountered.
type blockOperation func(ctx echo.Context, blockHash *chainhash.Hash) error

// blockRequest represents the common request structure for block operations.
// It defines the JSON format expected in request bodies for block-related API endpoints,
// providing a consistent interface for all block operations that require a block hash.
//
// Fields:
// - BlockHash: String representation of the block hash to operate on (required)
type blockRequest struct {
	BlockHash string `json:"blockHash"` // Hash of the block in hexadecimal string format
}

// NewBlockHandler creates a new block operations handler with the specified dependencies.
// This factory function ensures proper initialization of the handler with all required
// components for block operations.
//
// Parameters:
//   - blockchainClient: Client interface for communicating with the blockchain service
//   - logger: Logger instance for operation logging and error tracking
//
// Returns:
//   - *BlockHandler: Fully initialized handler ready to process block operations
func NewBlockHandler(blockchainClient blockchain.ClientI, logger ulogger.Logger) *BlockHandler {
	return &BlockHandler{
		blockchainClient: blockchainClient,
		logger:           logger,
	}
}

// handleBlockOperation is a helper function that handles common block operation logic.
// It encapsulates the shared workflow for block operations, including request validation,
// parameter parsing, error handling, and response generation. This method implements the
// template method pattern, where the common algorithm is defined here with specific
// operations provided as callbacks.
//
// The function performs the following steps:
// 1. Parses and validates the request body to extract the block hash
// 2. Converts the string hash to the internal hash representation
// 3. Executes the provided operation function with the parsed hash
// 4. Handles success and error responses with appropriate HTTP status codes
//
// Parameters:
//   - c: Echo context for the HTTP request/response
//   - operationName: Name of the operation for logging and error messages
//   - operation: Function that implements the specific block operation
//
// Returns:
//   - error: Any error encountered during operation processing
func (h *BlockHandler) handleBlockOperation(c echo.Context, operationName string, operation blockOperation) error {
	// Parse the block hash from the request body
	var request blockRequest
	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid request body: "+err.Error())
	}

	if request.BlockHash == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "BlockHash is required")
	}

	// Convert the string hash to chainhash.Hash
	blockHash, err := chainhash.NewHashFromStr(request.BlockHash)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid block hash format: "+err.Error())
	}

	ctx := c.Request().Context()

	h.logger.Infof("%s block: %s", operationName, blockHash)

	// First check if the block exists
	exists, err := h.blockchainClient.GetBlockExists(ctx, blockHash)
	if err != nil {
		h.logger.Errorf("Error checking if block %s exists: %v", blockHash, err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Error checking if block exists: "+err.Error())
	}

	if !exists {
		h.logger.Warnf("Block not found for %s: %s", operationName, blockHash)
		return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("Block with hash %s not found", blockHash))
	}

	// Call the blockchain service to perform the operation
	if err := operation(c, blockHash); err != nil {
		h.logger.Errorf("Failed to %s block %s: %v", operationName, blockHash, err)
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to %s block: %s", operationName, err.Error()))
	}

	h.logger.Infof("Block %s successfully: %s", operationName, blockHash)

	// Return success response
	return c.JSON(http.StatusOK, map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Block %s successfully", operationName),
	})
}

// InvalidateBlock handles HTTP requests to invalidate a block.
// This endpoint marks a previously validated block as invalid, which can be used
// for correcting consensus errors or managing chain reorganizations. The operation
// requires proper authorization and should be used with caution as it affects blockchain consensus.
//
// The endpoint expects a JSON request body with a 'blockHash' field containing the
// hexadecimal string representation of the block hash to invalidate.
//
// HTTP Method: POST
// Response Codes:
//   - 200: Block successfully invalidated
//   - 400: Invalid request format or block hash
//   - 404: Block not found
//   - 500: Internal server error during invalidation
//
// Parameters:
//   - c: Echo context for the HTTP request/response handling
//
// Returns:
//   - error: Any error encountered during block invalidation
func (h *BlockHandler) InvalidateBlock(c echo.Context) error {
	return h.handleBlockOperation(c, "invalidate", func(ctx echo.Context, blockHash *chainhash.Hash) error {
		h.logger.Infof("[InvalidateBlock] HTTP request to invalidate block %s", blockHash.String())

		invalidatedHashes, err := h.blockchainClient.InvalidateBlock(ctx.Request().Context(), blockHash)
		if err != nil {
			return err
		}

		h.logger.Infof("[InvalidateBlock] Invalidated %d blocks", len(invalidatedHashes))
		return nil
	})
}

// RevalidateBlock handles HTTP requests to revalidate a previously invalidated block.
// This endpoint attempts to return a block that was previously marked as invalid
// back to the validated state. The operation may trigger reprocessing of the block
// through the validation pipeline to ensure it meets consensus rules.
//
// The endpoint expects a JSON request body with a 'blockHash' field containing the
// hexadecimal string representation of the block hash to revalidate.
//
// HTTP Method: POST
// Response Codes:
//   - 200: Block successfully revalidated
//   - 400: Invalid request format or block hash
//   - 404: Block not found
//   - 409: Block cannot be revalidated (e.g., validation conflicts)
//   - 500: Internal server error during revalidation
//
// Parameters:
//   - c: Echo context for the HTTP request/response handling
//
// Returns:
//   - error: Any error encountered during block revalidation
func (h *BlockHandler) RevalidateBlock(c echo.Context) error {
	return h.handleBlockOperation(c, "revalidate", func(ctx echo.Context, blockHash *chainhash.Hash) error {
		return h.blockchainClient.RevalidateBlock(ctx.Request().Context(), blockHash)
	})
}

// GetLastNInvalidBlocks handles HTTP requests to retrieve the last N invalid blocks.
// This endpoint provides information about recently invalidated blocks, which is useful
// for monitoring blockchain health and troubleshooting consensus issues. The response
// includes block metadata such as hash, height, and invalidation reason.
//
// Query Parameters:
//   - n: Number of invalid blocks to retrieve (default: 10, max: 100)
//
// HTTP Method: GET
// Response Format: JSON array of invalid block information
// Response Codes:
//   - 200: Successfully retrieved invalid blocks
//   - 400: Invalid query parameters
//   - 500: Internal server error during retrieval
//
// Parameters:
//   - c: Echo context for the HTTP request/response handling
//
// Returns:
//   - error: Any error encountered during retrieval of invalid blocks
func (h *BlockHandler) GetLastNInvalidBlocks(c echo.Context) error {
	// Parse the count parameter from the query string
	countStr := c.QueryParam("count")
	if countStr == "" {
		countStr = "10" // Default to 10 if not specified
	}

	// Convert count to int64
	var count int64

	_, err := fmt.Sscanf(countStr, "%d", &count)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid count parameter: "+err.Error())
	}

	// Validate count is positive
	if count <= 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "Count must be a positive number")
	}

	ctx := c.Request().Context()

	h.logger.Infof("Retrieving last %d invalid blocks", count)

	// Call the blockchain service to get the invalid blocks
	blocks, err := h.blockchainClient.GetLastNInvalidBlocks(ctx, count)
	if err != nil {
		h.logger.Errorf("Failed to retrieve invalid blocks: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to retrieve invalid blocks: "+err.Error())
	}

	h.logger.Infof("Successfully retrieved %d invalid blocks", len(blocks))

	// Return the blocks
	return c.JSON(http.StatusOK, map[string]interface{}{
		"success": true,
		"count":   len(blocks),
		"blocks":  blocks,
	})
}
