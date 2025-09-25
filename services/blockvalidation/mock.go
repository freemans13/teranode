package blockvalidation

import (
	"context"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/mock"
)

// Mock implements a mock version of the block validation client for testing.
type Mock struct {
	mock.Mock
}

// NewMock creates a new mock client.
//
// Returns:
//   - *Mock: Mock client implementation
func NewMock() *Mock {
	return &Mock{}
}

// Health performs a mock health check.
func (m *Mock) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)

	if args.Error(2) != nil {
		return 0, "", args.Error(2)
	}

	return args.Int(0), args.String(1), args.Error(2)
}

// BlockFound performs a mock block found notification.
func (m *Mock) BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseURL string, waitToComplete bool) error {
	args := m.Called(ctx, blockHash, baseURL, waitToComplete)
	return args.Error(0)
}

// ProcessBlock performs a mock block processing.
func (m *Mock) ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32) error {
	args := m.Called(ctx, block, blockHeight)
	return args.Error(0)
}

// ValidateBlock performs a mock block validation.
func (m *Mock) ValidateBlock(ctx context.Context, block *model.Block) error {
	args := m.Called(ctx, block)
	return args.Error(0)
}
