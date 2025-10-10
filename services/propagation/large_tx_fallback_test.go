package propagation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockKafkaProducer implements the KafkaAsyncProducerI interface for testing
type MockKafkaProducer struct {
	PublishedMessages []*kafka.Message
	MessageSizeLimit  int
}

func (m *MockKafkaProducer) Start(ctx context.Context, ch chan *kafka.Message) {
}

func (m *MockKafkaProducer) Stop() error          { return nil }
func (m *MockKafkaProducer) BrokersURL() []string { return []string{} }
func (m *MockKafkaProducer) Publish(msg *kafka.Message) {
	m.PublishedMessages = append(m.PublishedMessages, msg)
}

// TestLargeTransactionFallback tests server-side HTTP fallback when transactions exceed gRPC size limits.
// This test sets up a mock validator HTTP server and validates that the propagation service
// correctly routes large transactions through HTTP endpoints instead of gRPC.
func TestLargeTransactionFallback(t *testing.T) {
	// Setup mock validator HTTP server
	validatorCallCount := 0
	mockValidatorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		validatorCallCount++

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("OK"))
		require.NoError(t, err)
	}))

	defer mockValidatorServer.Close()

	// Extract the port from the mock server URL
	serverAddr := mockValidatorServer.URL[7:] // Remove "http://"

	// Create URL for the validator HTTP address
	validatorHTTPURL, err := url.Parse(mockValidatorServer.URL)
	require.NoError(t, err)

	// Setup mock Kafka producer
	mockKafkaProducer := &MockKafkaProducer{
		PublishedMessages: make([]*kafka.Message, 0),
	}

	// Create test propagation server
	txStore, err := null.New(ulogger.TestLogger{})
	require.NoError(t, err)

	ps := &PropagationServer{
		logger:    ulogger.TestLogger{},
		validator: &validator.MockValidator{},
		txStore:   txStore,
		settings: &settings.Settings{
			Validator: settings.ValidatorSettings{
				HTTPListenAddress:    serverAddr,
				KafkaMaxMessageBytes: 1024 * 1024, // Set 1MB limit
			},
		},
		validatorKafkaProducerClient: mockKafkaProducer,
		validatorHTTPAddr:            validatorHTTPURL,
	}

	// Test case 1: Small transaction should use Kafka
	t.Run("Small transaction uses Kafka", func(t *testing.T) {
		// Create a small transaction (under 1MB)
		smallTx := bt.NewTx()

		smallTx.Inputs = []*bt.Input{
			{
				PreviousTxSatoshis: 1000,
				PreviousTxOutIndex: 1,
				SequenceNumber:     1,
			},
		}
		_ = smallTx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{})

		// Add a simple P2PKH output
		err := smallTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		require.NoError(t, err)

		// Process the transaction
		err = ps.processTransactionInternal(context.Background(), smallTx)
		require.NoError(t, err)

		// Verify Kafka was used (message was published)
		assert.Equal(t, 1, len(mockKafkaProducer.PublishedMessages), "Expected 1 message published to Kafka")
		assert.Equal(t, 0, validatorCallCount, "Validator HTTP endpoint should not be called")
	})

	// Reset for next test
	mockKafkaProducer.PublishedMessages = make([]*kafka.Message, 0)
	validatorCallCount = 0

	// Test case 2: Large transaction should use HTTP fallback
	t.Run("Large transaction uses HTTP fallback", func(t *testing.T) {
		// Create a large transaction (over 1MB)
		largeTx := bt.NewTx()

		largeTx.Inputs = []*bt.Input{
			{
				PreviousTxSatoshis: 1000,
				PreviousTxOutIndex: 1,
				SequenceNumber:     1,
			},
		}
		_ = largeTx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{})

		// Add a simple P2PKH output first
		err := largeTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		require.NoError(t, err)

		// Create large data for OP_RETURN (over 1MB)
		largeData := make([]byte, 1100000) // ~1.1MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		// Add an OP_RETURN output with the large data
		err = largeTx.AddOpReturnOutput(largeData)
		require.NoError(t, err)

		// Process the transaction
		err = ps.processTransactionInternal(context.Background(), largeTx)
		require.NoError(t, err)

		// Verify HTTP fallback was used
		assert.Equal(t, 0, len(mockKafkaProducer.PublishedMessages), "No messages should be published to Kafka")
		assert.Equal(t, 1, validatorCallCount, "Validator HTTP endpoint should be called exactly once")
	})
}
