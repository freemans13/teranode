package blockchain

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// Test_QuickValidated_gRPC_RoundTrip proves the QuickValidated store option survives the
// blockchain gRPC hop on both write (AddBlock) and read (GetBlockHeader). It uses the REAL
// gRPC stack (a *Blockchain server backed by sqlitememory, reached through a *Client over a
// TCP listener), not the in-process Mock, because the transport plumbing is exactly what is
// under test.
func Test_QuickValidated_gRPC_RoundTrip(t *testing.T) {
	tctx := setup(t)

	// Stand up the real gRPC server on an OS-assigned port.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	blockchain_api.RegisterBlockchainAPIServer(grpcServer, tctx.server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	logger := ulogger.NewErrorTestLogger(t)

	tSettings := tctx.server.settings
	tSettings.BlockChain.GRPCAddress = lis.Addr().String()
	tSettings.BlockChain.MaxRetries = 1
	tSettings.BlockChain.RetrySleep = 10

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientI, err := NewClientWithAddress(ctx, logger, tSettings, lis.Addr().String(), "quick-validated-test")
	require.NoError(t, err)

	client := clientI.(*Client)

	t.Run("QuickValidated true survives write and read", func(t *testing.T) {
		block := mockBlock(tctx, t)

		err := client.AddBlock(ctx, block, "",
			blockchainoptions.WithMinedSet(true),
			blockchainoptions.WithQuickValidated(true),
			blockchainoptions.WithID(uint64(block.ID)),
		)
		require.NoError(t, err)

		_, meta, err := client.GetBlockHeader(ctx, block.Hash())
		require.NoError(t, err)
		require.NotNil(t, meta)

		require.True(t, meta.QuickValidated, "QuickValidated must survive the gRPC AddBlock->GetBlockHeader round-trip")
		require.True(t, meta.MinedSet, "MinedSet must survive the gRPC round-trip (control)")
	})
}

// Test_QuickValidated_gRPC_False proves a block committed with WithQuickValidated(false) reads
// back false over the same real gRPC stack (no spurious true).
func Test_QuickValidated_gRPC_False(t *testing.T) {
	tctx := setup(t)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	blockchain_api.RegisterBlockchainAPIServer(grpcServer, tctx.server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	logger := ulogger.NewErrorTestLogger(t)

	tSettings := tctx.server.settings
	tSettings.BlockChain.GRPCAddress = lis.Addr().String()
	tSettings.BlockChain.MaxRetries = 1
	tSettings.BlockChain.RetrySleep = 10

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientI, err := NewClientWithAddress(ctx, logger, tSettings, lis.Addr().String(), "quick-validated-test")
	require.NoError(t, err)

	client := clientI.(*Client)

	block := mockBlock(tctx, t)

	err = client.AddBlock(ctx, block, "",
		blockchainoptions.WithQuickValidated(false),
		blockchainoptions.WithID(uint64(block.ID)),
	)
	require.NoError(t, err)

	_, meta, err := client.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)
	require.NotNil(t, meta)

	require.False(t, meta.QuickValidated, "QuickValidated must read back false when stored false")
}
