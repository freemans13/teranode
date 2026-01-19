package uaerospike

import (
	"encoding/binary"
	"sort"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
)

const (
	// DefaultConnectionQueueSize is the default size for the connection queue
	// if not specified in the client policy
	DefaultConnectionQueueSize = 128

	// semaphoreTimeoutFraction is the fraction of TotalTimeout to use for semaphore acquisition
	// This ensures the total operation time (semaphore wait + actual operation) stays within bounds
	semaphoreTimeoutFraction = 0.1 // 10% of total timeout

	// minSemaphoreTimeout is the minimum timeout for semaphore acquisition
	minSemaphoreTimeout = 100 * time.Millisecond
)

// getConnectionQueueSize returns the connection queue size from the given policy
// or falls back to DefaultConnectionQueueSize if the policy is nil or returns 0
func getConnectionQueueSize(policy *aerospike.ClientPolicy) int {
	if policy != nil && policy.ConnectionQueueSize > 0 {
		return policy.ConnectionQueueSize
	}
	return DefaultConnectionQueueSize
}

// ClientStats holds the statistics for Aerospike operations
type ClientStats struct {
	stat             *gocore.Stat
	operateStat      *gocore.Stat
	batchOperateStat *gocore.Stat
}

// NewClientStats creates a new ClientStats instance
func NewClientStats() *ClientStats {
	stat := gocore.NewStat("Aerospike")
	return &ClientStats{
		stat:             stat,
		operateStat:      stat.NewStat("Operate").AddRanges(0, 1, 100, 1_000, 10_000, 100_000),
		batchOperateStat: stat.NewStat("BatchOperate").AddRanges(0, 1, 100, 1_000, 10_000, 100_000),
	}
}

// Client is a wrapper around aerospike.Client that provides retry logic for connection pool exhaustion.
// Operations will retry with exponential backoff when the Aerospike connection pool is exhausted.
type Client struct {
	*aerospike.Client
	stats               *ClientStats   // Always initialized, never nil
	connectionQueueSize int            // Aerospike connection pool size for monitoring
	logger              ulogger.Logger // Logger for retry diagnostics
}

// NewClient creates a new Aerospike client with the specified hostname and port.
func NewClient(hostname string, port int) (*Client, error) {
	client, err := aerospike.NewClient(hostname, port)
	if err != nil {
		return nil, err
	}

	// Get queue size from default policy
	policy := aerospike.NewClientPolicy()
	queueSize := getConnectionQueueSize(policy)

	return &Client{
		Client:              client,
		stats:               NewClientStats(),
		connectionQueueSize: queueSize,
	}, nil
}

// NewClientWithPolicyAndHost creates a new Aerospike client with the specified policy and hosts.
func NewClientWithPolicyAndHost(logger ulogger.Logger, policy *aerospike.ClientPolicy, hosts ...*aerospike.Host) (*Client, aerospike.Error) {
	var (
		client *aerospike.Client
		err    aerospike.Error
	)

	// Default retry settings
	maxRetries := 3
	retryDelay := 1 * time.Second

	// If timeout is very short (indicating test mode), don't retry
	if policy != nil && policy.Timeout > 0 && policy.Timeout <= 200*time.Millisecond {
		maxRetries = 1 // No retries for short timeouts
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		client, err = aerospike.NewClientWithPolicyAndHost(policy, hosts...)
		if err == nil {
			// Connection successful
			break
		}

		// Use the Matches method to check against transient error codes
		isTransientError := err.Matches(
			types.INVALID_NODE_ERROR,
			types.TIMEOUT,
			types.NO_RESPONSE,
			types.NETWORK_ERROR,
			types.SERVER_NOT_AVAILABLE,
			types.NO_AVAILABLE_CONNECTIONS_TO_NODE,
		)

		if !isTransientError {
			// Error is not transient, don't retry
			break
		}

		// Log the retry attempt (optional, but useful for debugging)
		// log.Printf("Aerospike connection attempt %d failed with transient error (%d): %v. Retrying in %v...", attempt, asAeroErr.ResultCode(), err, retryDelay)

		if attempt < maxRetries {
			time.Sleep(retryDelay)
		}
	}

	if err != nil {
		return nil, err
	}

	queueSize := getConnectionQueueSize(policy)

	return &Client{
		Client:              client,
		stats:               NewClientStats(),
		connectionQueueSize: queueSize,
		logger:              logger,
	}, nil
}

// Put is a wrapper around aerospike.Client.Put that retries on connection pool exhaustion.
func (c *Client) Put(policy *aerospike.WritePolicy, key *aerospike.Key, binMap aerospike.BinMap) aerospike.Error {
	start := gocore.CurrentTime()

	defer func() {
		// Extract keys from binMap
		keys := make([]string, len(binMap))
		var i int
		for k := range binMap {
			keys[i] = k
			i++
		}

		// Sort the keys
		sort.Strings(keys)

		// Build the query string with sorted keys
		var sb strings.Builder
		sb.WriteString("Put: ")
		for i, k := range keys {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(k)
		}

		c.stats.stat.NewStat(sb.String()).AddTime(start)
	}()

	return retryOnPoolExhaustion(c.logger, "Put", func() aerospike.Error {
		return c.Client.Put(policy, key, binMap)
	})
}

// PutBins is a wrapper around aerospike.Client.PutBins that retries on connection pool exhaustion.
func (c *Client) PutBins(policy *aerospike.WritePolicy, key *aerospike.Key, bins ...*aerospike.Bin) aerospike.Error {
	start := gocore.CurrentTime()

	defer func() {
		// Extract keys from binMap
		keys := make([]string, len(bins))
		for i, bin := range bins {
			keys[i] = bin.Name
		}

		// Build the query string with sorted keys
		var sb strings.Builder
		sb.WriteString("PutBins: ")
		for i, k := range keys {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(k)
		}

		c.stats.stat.NewStat(sb.String()).AddTime(start)
	}()

	return retryOnPoolExhaustion(c.logger, "PutBins", func() aerospike.Error {
		return c.Client.PutBins(policy, key, bins...)
	})
}

// Delete is a wrapper around aerospike.Client.Delete that retries on connection pool exhaustion.
func (c *Client) Delete(policy *aerospike.WritePolicy, key *aerospike.Key) (bool, aerospike.Error) {
	start := gocore.CurrentTime()

	defer func() {
		c.stats.stat.NewStat("Delete").AddTime(start)
	}()

	var deleted bool
	err := retryOnPoolExhaustion(c.logger, "Delete", func() aerospike.Error {
		var e aerospike.Error
		deleted, e = c.Client.Delete(policy, key)
		return e
	})

	return deleted, err
}

// Get is a wrapper around aerospike.Client.Get that retries on connection pool exhaustion.
func (c *Client) Get(policy *aerospike.BasePolicy, key *aerospike.Key, binNames ...string) (*aerospike.Record, aerospike.Error) {
	start := gocore.CurrentTime()

	defer func() {
		// Build the query string with sorted keys
		var sb strings.Builder
		sb.WriteString("Get: ")
		for i, k := range binNames {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(k)
		}

		c.stats.stat.NewStat(sb.String()).AddTime(start)
	}()

	var record *aerospike.Record
	err := retryOnPoolExhaustion(c.logger, "Get", func() aerospike.Error {
		var e aerospike.Error
		record, e = c.Client.Get(policy, key, binNames...)
		return e
	})

	return record, err
}

// Operate is a wrapper around aerospike.Client.Operate that retries on connection pool exhaustion.
func (c *Client) Operate(policy *aerospike.WritePolicy, key *aerospike.Key, operations ...*aerospike.Operation) (*aerospike.Record, aerospike.Error) {
	start := gocore.CurrentTime()
	defer func() {
		c.stats.operateStat.AddTimeForRange(start, len(operations))
	}()

	var record *aerospike.Record
	err := retryOnPoolExhaustion(c.logger, "Operate", func() aerospike.Error {
		var e aerospike.Error
		record, e = c.Client.Operate(policy, key, operations...)
		return e
	})

	return record, err
}

// BatchOperate is a wrapper around aerospike.Client.BatchOperate that retries on connection pool exhaustion.
func (c *Client) BatchOperate(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
	start := gocore.CurrentTime()
	defer func() {
		c.stats.batchOperateStat.AddTimeForRange(start, len(records))
	}()

	return retryOnPoolExhaustion(c.logger, "BatchOperate", func() aerospike.Error {
		return c.Client.BatchOperate(policy, records)
	})
}

// GetConnectionQueueSize returns the Aerospike connection pool size.
// This is used for monitoring and validating that concurrent operations won't exhaust the pool.
func (c *Client) GetConnectionQueueSize() int {
	return c.connectionQueueSize
}

// GetActiveConnectionCount returns the current number of open connections across all nodes.
// This is useful for monitoring actual connection pool usage during batch operations.
func (c *Client) GetActiveConnectionCount() int {
	stats, err := c.Client.Stats()
	if err != nil {
		return -1 // Error getting stats
	}

	if openConns, ok := stats["open-connections"].(int64); ok {
		return int(openConns)
	}
	if openConns, ok := stats["open-connections"].(int); ok {
		return openConns
	}

	return -1 // Field not found or wrong type
}

// retryOnPoolExhaustion retries an Aerospike operation when the connection pool is exhausted.
// With ExitFastOnExhaustedConnectionPool=true, operations fail immediately on pool exhaustion.
// This function implements exponential backoff retry logic to handle transient pool saturation.
//
// The retry strategy:
//   - Starts with 5ms backoff, doubles up to 50ms max
//   - Retries up to 50 times (fast when connections free up)
//   - Only retries on NO_AVAILABLE_CONNECTIONS_TO_NODE error
//   - Other errors (timeouts, server errors) fail immediately
//
// Parameters:
//   - operation: Function that executes the Aerospike operation
//
// Returns:
//   - aerospike.Error: nil if operation succeeded, error if max retries exceeded or non-pool error
func retryOnPoolExhaustion(logger ulogger.Logger, operationName string, operation func() aerospike.Error) aerospike.Error {
	const maxRetries = 50
	backoff := 5 * time.Millisecond
	const maxBackoff = 50 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		err := operation()

		if err == nil {
			if attempt > 0 {
				logger.Warnf("[RETRY] %s succeeded after %d retries", operationName, attempt)
			}
			return nil // Success
		}

		// Only retry on connection pool exhaustion
		if !err.Matches(types.NO_AVAILABLE_CONNECTIONS_TO_NODE) {
			return err // Other errors (timeouts, server errors) fail immediately
		}

		// Pool exhausted - log and retry
		if attempt == 0 {
			logger.Warnf("[RETRY] %s hit pool exhaustion (NO_AVAILABLE_CONNECTIONS_TO_NODE), starting retries...", operationName)
		}

		if attempt > 0 {
			logger.Debugf("[RETRY] %s attempt %d failed, sleeping %v", operationName, attempt, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	logger.Errorf("[RETRY] %s exhausted all %d retries", operationName, maxRetries)
	return aerospike.ErrConnectionPoolExhausted
}

// CalculateKeySource generates a key source based on the transaction hash, vout, and batch size.
func CalculateKeySource(hash *chainhash.Hash, vout uint32, batchSize int) []byte {
	if batchSize <= 0 {
		return nil
	}

	num := vout / uint32(batchSize)

	return CalculateKeySourceInternal(hash, num)
}

func CalculateKeySourceInternal(hash *chainhash.Hash, num uint32) []byte {
	if num == 0 {
		// Fast path: just return cloned hash bytes
		return hash.CloneBytes()
	}

	// Optimized path: pre-allocate slice with exact capacity to avoid reallocation
	keySource := make([]byte, chainhash.HashSize+4)
	copy(keySource[:chainhash.HashSize], hash[:])

	// Directly write little-endian uint32 to avoid intermediate allocation
	binary.LittleEndian.PutUint32(keySource[chainhash.HashSize:], num)

	return keySource
}
