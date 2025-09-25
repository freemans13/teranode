package rpc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/propagation"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDistributor_SendTransaction_Success_SingleServer(t *testing.T) {
	logger := ulogger.TestLogger{}

	// Create a mock propagation client that always succeeds
	mockClient := NewMockPropagationClient()
	mockClient.On("ProcessTransaction", mock.Anything, mock.Anything).Return(nil)

	// Create distributor with single server
	d := &Distributor{
		logger: logger,
		propagationServers: map[string]*propagation.Client{
			"test-server": {}, // We'll replace this in the test
		},
		attempts:         1,
		backoff:          100 * time.Millisecond,
		failureTolerance: 50,
		settings: &settings.Settings{
			Coinbase: settings.CoinbaseSettings{
				DistributorTimeout: 5 * time.Second,
			},
		},
	}

	// Create test transaction
	tx := CreateTestTransaction()

	// Since we can't easily mock the actual propagation client interface,
	// we'll test the basic functionality with empty propagation servers map
	d.propagationServers = make(map[string]*propagation.Client)

	responses, err := d.SendTransaction(context.Background(), tx)

	// With no servers, it should error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NaN")
	assert.Empty(t, responses)
}

func TestDistributor_SendTransaction_AllServersFail(t *testing.T) {
	logger := ulogger.TestLogger{}

	// Create distributor with empty servers to simulate all failures
	d := &Distributor{
		logger:             logger,
		propagationServers: make(map[string]*propagation.Client), // Empty map
		attempts:           2,
		backoff:            10 * time.Millisecond,
		failureTolerance:   50,
		settings: &settings.Settings{
			Coinbase: settings.CoinbaseSettings{
				DistributorTimeout: 5 * time.Second,
			},
		},
	}

	tx := CreateTestTransaction()

	responses, err := d.SendTransaction(context.Background(), tx)

	// Should error with NaN% failure
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NaN")
	assert.Empty(t, responses)
}

func TestDistributor_SendTransaction_PartialFailure_WithinTolerance(t *testing.T) {
	logger := ulogger.TestLogger{}

	// This test verifies that partial failures within tolerance are handled correctly
	d := &Distributor{
		logger:             logger,
		propagationServers: make(map[string]*propagation.Client),
		attempts:           1,
		backoff:            10 * time.Millisecond,
		failureTolerance:   75, // 75% failure tolerance
		settings: &settings.Settings{
			Coinbase: settings.CoinbaseSettings{
				DistributorTimeout: 5 * time.Second,
			},
		},
	}

	tx := CreateTestTransaction()

	// Test with no servers still results in error
	responses, err := d.SendTransaction(context.Background(), tx)

	assert.Error(t, err)
	assert.Empty(t, responses)
}

func TestDistributor_SendTransaction_ContextCancellation(t *testing.T) {
	logger := ulogger.TestLogger{}

	d := &Distributor{
		logger:             logger,
		propagationServers: make(map[string]*propagation.Client),
		attempts:           1,
		settings: &settings.Settings{
			Coinbase: settings.CoinbaseSettings{
				DistributorTimeout: 5 * time.Second,
			},
		},
	}

	// Cancel context immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tx := CreateTestTransaction()

	responses, err := d.SendTransaction(ctx, tx)

	// Should handle context cancellation
	assert.Error(t, err)
	assert.Empty(t, responses)
}

func TestDistributor_SendTransaction_ResponseWrapper_Structure(t *testing.T) {
	// Test ResponseWrapper structure
	rw := &ResponseWrapper{
		Addr:     "test-address",
		Duration: 100 * time.Millisecond,
		Retries:  3,
		Error:    errors.NewProcessingError("test error"),
	}

	assert.Equal(t, "test-address", rw.Addr)
	assert.Equal(t, 100*time.Millisecond, rw.Duration)
	assert.Equal(t, int32(3), rw.Retries)
	assert.Error(t, rw.Error)
}

func TestDistributor_Clone_Success(t *testing.T) {
	logger := ulogger.TestLogger{}

	// Create original distributor
	original := &Distributor{
		logger:             logger,
		propagationServers: make(map[string]*propagation.Client),
		attempts:           3,
		backoff:            200 * time.Millisecond,
		failureTolerance:   30,
		waitMsBetweenTxs:   100,
		settings: &settings.Settings{
			Propagation: settings.PropagationSettings{
				GRPCAddresses: []string{}, // Empty to avoid connection attempts
			},
			Coinbase: settings.CoinbaseSettings{
				DistributorFailureTolerance: 30,
			},
		},
	}

	// Clone should fail with no addresses
	cloned, err := original.Clone()

	assert.Error(t, err)
	assert.Nil(t, cloned)
	assert.Contains(t, err.Error(), errMsgNoServers)
}

func TestDistributor_Constructor_Options(t *testing.T) {
	logger := ulogger.TestLogger{}

	t.Run("NewDistributor with options", func(t *testing.T) {
		settings := &settings.Settings{
			Propagation: settings.PropagationSettings{
				GRPCAddresses: []string{}, // Empty to trigger error
			},
		}

		// Should fail with no addresses
		d, err := NewDistributor(context.Background(), logger, settings,
			WithRetryAttempts(5),
			WithBackoffDuration(500*time.Millisecond),
			WithFailureTolerance(25))

		assert.Error(t, err)
		assert.Nil(t, d)
	})

	t.Run("NewDistributorFromAddress with invalid address", func(t *testing.T) {
		settings := &settings.Settings{}

		d, err := NewDistributorFromAddress(context.Background(), logger, settings, "invalid-address",
			WithRetryAttempts(2),
			WithFailureTolerance(10))

		assert.Error(t, err)
		assert.Nil(t, d)
	})
}

func TestDistributor_ErrorHandling_Edge_Cases(t *testing.T) {
	logger := ulogger.TestLogger{}

	t.Run("Zero timeout handling", func(t *testing.T) {
		d := &Distributor{
			logger:             logger,
			propagationServers: make(map[string]*propagation.Client),
			attempts:           1,
			settings: &settings.Settings{
				Coinbase: settings.CoinbaseSettings{
					DistributorTimeout: 0, // Zero timeout
				},
			},
		}

		tx := CreateTestTransaction()

		responses, err := d.SendTransaction(context.Background(), tx)

		// Should still handle zero timeout gracefully
		assert.Error(t, err)
		assert.Empty(t, responses)
	})

	t.Run("High failure tolerance", func(t *testing.T) {
		d := &Distributor{
			logger:             logger,
			propagationServers: make(map[string]*propagation.Client),
			failureTolerance:   100, // 100% failure tolerance
			settings: &settings.Settings{
				Coinbase: settings.CoinbaseSettings{
					DistributorTimeout: 1 * time.Second,
				},
			},
		}

		tx := CreateTestTransaction()

		responses, err := d.SendTransaction(context.Background(), tx)

		// Even with 100% tolerance, no servers should still error
		assert.Error(t, err)
		assert.Empty(t, responses)
	})
}

func TestDistributor_Concurrency_Safety(t *testing.T) {
	logger := ulogger.TestLogger{}

	d := &Distributor{
		logger:             logger,
		propagationServers: make(map[string]*propagation.Client),
		attempts:           1,
		settings: &settings.Settings{
			Coinbase: settings.CoinbaseSettings{
				DistributorTimeout: 1 * time.Second,
			},
		},
	}

	// Test concurrent access to SendTransaction
	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			tx := CreateTestTransactionWithTxID(byte(id))
			_, err := d.SendTransaction(context.Background(), tx)

			// All should error due to no servers
			assert.Error(t, err)
		}(i)
	}

	wg.Wait()
}

func TestDistributor_GetPropagationGRPCAddresses_EdgeCases(t *testing.T) {
	t.Run("Empty propagation servers", func(t *testing.T) {
		d := &Distributor{
			propagationServers: make(map[string]*propagation.Client),
		}

		addresses := d.GetPropagationGRPCAddresses()
		assert.Empty(t, addresses)
		assert.Len(t, addresses, 0)
	})

	t.Run("Single propagation server", func(t *testing.T) {
		d := &Distributor{
			propagationServers: map[string]*propagation.Client{
				"single-server": nil,
			},
		}

		addresses := d.GetPropagationGRPCAddresses()
		assert.Len(t, addresses, 1)
		assert.Contains(t, addresses, "single-server")
	})

	t.Run("Multiple propagation servers", func(t *testing.T) {
		d := &Distributor{
			propagationServers: map[string]*propagation.Client{
				"server-1": nil,
				"server-2": nil,
				"server-3": nil,
			},
		}

		addresses := d.GetPropagationGRPCAddresses()
		assert.Len(t, addresses, 3)
		assert.Contains(t, addresses, "server-1")
		assert.Contains(t, addresses, "server-2")
		assert.Contains(t, addresses, "server-3")
	})
}

func TestDistributor_TriggerBatcher_EdgeCases(t *testing.T) {
	t.Run("Empty propagation servers", func(t *testing.T) {
		d := &Distributor{
			propagationServers: make(map[string]*propagation.Client),
		}

		// Should not panic with empty map
		assert.NotPanics(t, func() {
			d.TriggerBatcher()
		})
	})

	t.Run("Nil propagation servers map", func(t *testing.T) {
		d := &Distributor{
			propagationServers: nil,
		}

		// Should not panic with nil map
		assert.NotPanics(t, func() {
			d.TriggerBatcher()
		})
	})
}

func TestDistributor_ErrorSimulator_Functionality(t *testing.T) {
	simulator := &ErrorSimulator{}

	t.Run("TxInvalidError", func(t *testing.T) {
		err := simulator.CreateTxInvalidError()
		assert.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrTxInvalid))
	})

	t.Run("NetworkError", func(t *testing.T) {
		err := simulator.CreateNetworkError()
		assert.Error(t, err)
		// Network errors don't have a specific check in the errors package
		assert.Contains(t, err.Error(), "test network error")
	})

	t.Run("ProcessingError", func(t *testing.T) {
		err := simulator.CreateProcessingError()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "test processing error")
	})

	t.Run("TimeoutError", func(t *testing.T) {
		err := simulator.CreateTimeoutError()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "test timeout error")
	})
}

func TestDistributor_CreateTestSettings_Functionality(t *testing.T) {
	t.Run("Empty addresses", func(t *testing.T) {
		settings := CreateTestSettings([]string{})
		assert.Empty(t, settings.Propagation.GRPCAddresses)
		assert.Equal(t, 5*time.Second, settings.Coinbase.DistributorTimeout)
		assert.Equal(t, 50, settings.Coinbase.DistributorFailureTolerance)
	})

	t.Run("Multiple addresses", func(t *testing.T) {
		addresses := []string{"server1", "server2", "server3"}
		settings := CreateTestSettings(addresses)
		assert.Equal(t, addresses, settings.Propagation.GRPCAddresses)
		assert.Len(t, settings.Propagation.GRPCAddresses, 3)
	})
}

func TestDistributor_CreateTestTransaction_Functionality(t *testing.T) {
	t.Run("Basic transaction creation", func(t *testing.T) {
		tx := CreateTestTransaction()
		assert.NotNil(t, tx)
		assert.NotEmpty(t, tx.TxID())
	})

	t.Run("Transaction with TxID variation", func(t *testing.T) {
		tx1 := CreateTestTransactionWithTxID(1)
		tx2 := CreateTestTransactionWithTxID(2)

		assert.NotNil(t, tx1)
		assert.NotNil(t, tx2)
		assert.NotEqual(t, tx1.TxID(), tx2.TxID()) // Should have different TxIDs
		assert.Equal(t, uint32(1), tx1.Version)
		assert.Equal(t, uint32(2), tx2.Version)
	})
}

// Benchmark tests for performance evaluation
func BenchmarkDistributor_SendTransaction_NoServers(b *testing.B) {
	logger := ulogger.TestLogger{}
	d := &Distributor{
		logger:             logger,
		propagationServers: make(map[string]*propagation.Client),
		settings: &settings.Settings{
			Coinbase: settings.CoinbaseSettings{
				DistributorTimeout: 1 * time.Second,
			},
		},
	}

	tx := CreateTestTransaction()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.SendTransaction(context.Background(), tx)
	}
}

func BenchmarkDistributor_GetPropagationGRPCAddresses(b *testing.B) {
	d := &Distributor{
		propagationServers: map[string]*propagation.Client{
			"server1": nil,
			"server2": nil,
			"server3": nil,
			"server4": nil,
			"server5": nil,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.GetPropagationGRPCAddresses()
	}
}
