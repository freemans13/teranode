package subtreevalidation

import (
	"context"

	"github.com/bitcoin-sv/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

// MockSubtreeValidationAPIClient provides a mock implementation of SubtreeValidationAPIClient for testing.
type MockSubtreeValidationAPIClient struct {
	mock.Mock
}

// HealthGRPC mocks the HealthGRPC method of SubtreeValidationAPIClient
func (m *MockSubtreeValidationAPIClient) HealthGRPC(ctx context.Context, in *subtreevalidation_api.EmptyMessage, opts ...grpc.CallOption) (*subtreevalidation_api.HealthResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*subtreevalidation_api.HealthResponse), args.Error(1)
}

// CheckSubtreeFromBlock mocks the CheckSubtreeFromBlock method of SubtreeValidationAPIClient
func (m *MockSubtreeValidationAPIClient) CheckSubtreeFromBlock(ctx context.Context, in *subtreevalidation_api.CheckSubtreeFromBlockRequest, opts ...grpc.CallOption) (*subtreevalidation_api.CheckSubtreeFromBlockResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*subtreevalidation_api.CheckSubtreeFromBlockResponse), args.Error(1)
}

// CheckBlockSubtrees mocks the CheckBlockSubtrees method of SubtreeValidationAPIClient
func (m *MockSubtreeValidationAPIClient) CheckBlockSubtrees(ctx context.Context, in *subtreevalidation_api.CheckBlockSubtreesRequest, opts ...grpc.CallOption) (*subtreevalidation_api.CheckBlockSubtreesResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*subtreevalidation_api.CheckBlockSubtreesResponse), args.Error(1)
}
