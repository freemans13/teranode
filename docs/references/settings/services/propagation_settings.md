# Propagation Settings

**Related Topic**: [Propagation Service](../../../topics/services/propagation.md)

The Propagation service serves as the transaction intake and distribution system for the Teranode network, critical for handling transaction flow between clients and the internal services. This section provides a comprehensive overview of all configuration options, organized by functional category.

## Network and Communication Settings

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `propagation_grpcListenAddress` | string | "" | Address for gRPC server to listen on | Controls the endpoint where the gRPC API is exposed |
| `propagation_grpcAddresses` | []string | [] | List of gRPC addresses for other services to connect to | Affects how other services discover and communicate with this service |
| `propagation_httpListenAddress` | string | "" | Address for HTTP server to listen on | Controls if and where the HTTP transaction API is exposed |
| `propagation_httpAddresses` | []string | [] | List of HTTP addresses for other services to connect to | Affects how other services discover this service's HTTP API |
| `ipv6_addresses` | string | "" | Comma-separated list of IPv6 multicast addresses | Controls which IPv6 multicast addresses are used for transaction reception |
| `ipv6_interface` | string | "" | Network interface name for IPv6 multicast | Determines which network interface is used for multicast communication |

## Performance and Throttling Settings

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `propagation_grpcMaxConnectionAge` | duration | 90s | Maximum duration for gRPC connections before forced renewal | Controls connection lifecycle and helps with load balancing |
| `propagation_httpRateLimit` | int | 1024 | Rate limit for HTTP API requests (per second) | Controls how many requests per second the HTTP API can handle |
| `propagation_sendBatchSize` | int | 100 | Maximum number of transactions to send in a batch | Affects efficiency and throughput of transaction processing |
| `propagation_sendBatchTimeout` | int | 5 | Timeout in milliseconds for batch sending operations | Controls how long the service waits to collect a full batch before processing |

## Transport and Behavior Settings

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `propagation_alwaysUseHTTP` | bool | false | Forces using HTTP instead of gRPC for transaction validation | Affects performance and reliability of transaction validation |

## Dependency-Injected Settings (from other services)

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `validator_httpAddress` | url | null | URL for validator HTTP API | Used as fallback for large transactions exceeding Kafka limits |
| `validator_kafkaMaxMessageBytes` | int | varies | Maximum size for Kafka messages in bytes | Determines when HTTP fallback is used for large transactions |
| `useLocalValidator` | bool | false | Daemon-level setting for validator deployment mode | Controls whether validator runs in-process or as separate service |

## Configuration Interactions and Dependencies

### Transaction Ingestion Paths

The Propagation service supports multiple methods for receiving transactions, controlled by several related settings:

- HTTP API (`propagation_httpListenAddress`): REST-based transaction submission, rate-limited by `propagation_httpRateLimit`
- gRPC API (`propagation_grpcListenAddress`): High-performance RPC interface for transaction submission
- IPv6 Multicast (`ipv6_addresses` on the specified `ipv6_interface`): Efficient multicast reception of transactions

At least one ingestion path must be configured for the service to be functional. For production deployments, all three methods should be configured for maximum compatibility and performance.

### Transaction Validation Architecture

The Propagation service interacts with the Validator service using one of two architectural patterns:

- **Local Validator Mode** (`useLocalValidator=true`):

  - Validator runs in-process with the Propagation service
  - Eliminates network overhead for validation operations
  - Recommended for production deployments to minimize latency

- **Remote Validator Mode** (`useLocalValidator=false`):

  - Propagation service communicates with a separate Validator service
  - **Transaction Size-Based Routing**: Transactions are automatically routed based on size:

    - Normal transactions (≤ `validator_kafka_maxMessageBytes`): Sent via Kafka for async validation
    - Large transactions (> `validator_kafka_maxMessageBytes`): Automatically sent via HTTP to `validator_httpAddress`
  - **Transport Override**: `propagation_alwaysUseHTTP=true` forces all transactions to use HTTP regardless of size
  - **Automatic Fallback**: HTTP fallback ensures reliability for transactions of any size

### Client-Side Batch Processing Optimization

The propagation client uses batching to optimize throughput, controlled by:

- `propagation_sendBatchSize`: Determines maximum batch size for client transaction processing (default: 100)
- `propagation_sendBatchTimeout`: Controls how long to wait in milliseconds for a batch to fill before processing (default: 5ms)

These settings should be tuned together based on expected transaction volume and size characteristics:

- **Higher batch sizes** improve throughput but increase latency
- **Shorter timeouts** decrease latency but may result in smaller, less efficient batches
