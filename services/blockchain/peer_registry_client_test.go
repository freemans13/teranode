package blockchain

import (
	"net"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// startFakePeerRegistryServer starts a gRPC server with the unimplemented
// PeerRegistryService and returns the server address and a stop function.
func startFakePeerRegistryServer(t *testing.T) (string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := grpc.NewServer()
	blockchain_api.RegisterPeerRegistryServiceServer(s, &blockchain_api.UnimplementedPeerRegistryServiceServer{})

	go func() {
		_ = s.Serve(lis)
	}()

	return lis.Addr().String(), s.Stop
}

// dialConn creates a gRPC client connection to the given address.
func dialConn(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return conn
}

// ---------------------------------------------------------------------------
// ownsConn = true: Close() closes the underlying connection
// ---------------------------------------------------------------------------

func TestPeerRegistryClient_Close_OwnsConn(t *testing.T) {
	addr, stop := startFakePeerRegistryServer(t)
	defer stop()

	conn := dialConn(t, addr)

	client := &PeerRegistryClient{
		client:   blockchain_api.NewPeerRegistryServiceClient(conn),
		conn:     conn,
		ownsConn: true,
	}

	require.NoError(t, client.Close())

	// After Close() the underlying connection should transition to Shutdown.
	state := conn.GetState()
	require.Equal(t, connectivity.Shutdown, state)
}

// ---------------------------------------------------------------------------
// ownsConn = false: Close() is a no-op, connection stays alive
// ---------------------------------------------------------------------------

func TestPeerRegistryClient_Close_DoesNotOwnConn(t *testing.T) {
	addr, stop := startFakePeerRegistryServer(t)
	defer stop()

	conn := dialConn(t, addr)
	defer conn.Close()

	client := &PeerRegistryClient{
		client:   blockchain_api.NewPeerRegistryServiceClient(conn),
		conn:     conn,
		ownsConn: false,
	}

	require.NoError(t, client.Close())

	// The connection should still be usable — state should NOT be Shutdown.
	state := conn.GetState()
	require.NotEqual(t, connectivity.Shutdown, state)
}

// ---------------------------------------------------------------------------
// Close() with nil conn is safe even when ownsConn is true
// ---------------------------------------------------------------------------

func TestPeerRegistryClient_Close_NilConn(t *testing.T) {
	client := &PeerRegistryClient{
		conn:     nil,
		ownsConn: true,
	}

	err := client.Close()
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// NewPeerRegistryClientFromConn creates a client that does not own the conn
// ---------------------------------------------------------------------------

func TestNewPeerRegistryClientFromConn_DoesNotOwnConnection(t *testing.T) {
	addr, stop := startFakePeerRegistryServer(t)
	defer stop()

	conn := dialConn(t, addr)
	defer conn.Close()

	clientI := NewPeerRegistryClientFromConn(conn)
	require.NotNil(t, clientI)

	// Verify Close() is a no-op.
	require.NoError(t, clientI.Close())

	// Connection should still be alive.
	state := conn.GetState()
	require.NotEqual(t, connectivity.Shutdown, state)
}

// ---------------------------------------------------------------------------
// NewPeerRegistryClientFromConn returns the correct interface type
// ---------------------------------------------------------------------------

func TestNewPeerRegistryClientFromConn_ImplementsInterface(t *testing.T) {
	addr, stop := startFakePeerRegistryServer(t)
	defer stop()

	conn := dialConn(t, addr)
	defer conn.Close()

	clientI := NewPeerRegistryClientFromConn(conn)

	// Compile-time interface satisfaction is already guaranteed by the return type,
	// but this confirms it at runtime too.
	var _ PeerRegistryClientI = clientI
}

// ---------------------------------------------------------------------------
// Close() can be called multiple times safely when ownsConn is true
// ---------------------------------------------------------------------------

func TestPeerRegistryClient_Close_Idempotent(t *testing.T) {
	addr, stop := startFakePeerRegistryServer(t)
	defer stop()

	conn := dialConn(t, addr)

	client := &PeerRegistryClient{
		client:   blockchain_api.NewPeerRegistryServiceClient(conn),
		conn:     conn,
		ownsConn: true,
	}

	// First close should succeed.
	require.NoError(t, client.Close())

	// Second close — grpc.ClientConn.Close() is documented as returning an error
	// on double-close but should not panic.
	_ = client.Close()
}
