# Blockchain Settings

**Related Topic**: [Blockchain Service](../../../topics/services/blockchain.md)

The Blockchain service configuration is organized into several categories to manage different aspects of the service's operation. All settings can be provided via environment variables or configuration files.

## Network Configuration

- **GRPC Address (`blockchain_grpcAddress`)**: Specifies the address for other services to connect to the Blockchain service's gRPC API. This is how other services will address the blockchain service.
  - Type: string
  - Default Value: `localhost:8087`
    - Impact: Critical for service discovery and inter-service communication
    - Security Impact: In production environments, should be configured securely based on network architecture

- **GRPC Listen Address (`blockchain_grpcListenAddress`)**: Specifies the network interface and port the Blockchain service's gRPC server binds to for accepting connections.
  - Type: string
  - Default Value: `:8087`
    - Impact: Controls network interface binding for accepting gRPC connections
    - Security Impact: Binding to `0.0.0.0` or empty address (`:8087`) exposes the port on all network interfaces

- **HTTP Listen Address (`blockchain_httpListenAddress`)**: Specifies the network interface and port for the HTTP server that exposes REST endpoints (primarily for block invalidation/revalidation).
  - Type: string
  - Default Value: `:8082`
    - Impact: Controls network interface binding for HTTP API access
    - Security Impact: Should be configured based on who needs access to these endpoints

## Data Storage Configuration

- **Store URL (`blockchain_store`)**: URL connection string for the blockchain database that stores block data and service state.
  - Type: URL
  - Default Value: `sqlite:///blockchain`
  - Supported Formats:

    - SQLite: `sqlite:///path/to/db`
    - PostgreSQL: `postgres://user:password@host:port/dbname`
    - Impact: Stores block data and service state
    - Performance Impact: Choice of database affects scalability and performance

- **DB Timeout (`blockchain_store_dbTimeoutMillis`)**: The timeout in milliseconds for database operations.
  - Type: integer
  - Default Value: `5000` (5 seconds)
    - Impact: Not currently implemented

## State Machine Configuration

- **Initialize Node In State (`blockchain_initializeNodeInState`)**: Specifies the initial state for the blockchain service's finite state machine (FSM).
  - Type: string
  - Default Value: `""` (empty, uses default FSM state)
  - Possible Values:

    - `"IDLE"`: Initial inactive state
    - `"RUNNING"`: Normal operating state
    - `"LEGACY_SYNC"`: Legacy synchronization mode
    - `"CATCHUP_BLOCKS"`: Block catch-up mode
    - Impact: Not currently implemented

- **FSM State Restore (`fsm_state_restore`)**: Controls whether the service restores its previous FSM state from storage on startup.
  - Type: boolean
  - Default Value: `false`
    - Impact: Not currently implemented

- **FSM State Change Delay (`fsm_state_change_delay`)**: FOR TESTING ONLY - introduces an artificial delay when changing FSM states.
  - Type: integer (milliseconds)
  - Default Value: `0`
    - Impact: Used only in tests for state transition synchronization
  - Warning: Should not be used in production environments

## Operational Settings

- **Maximum Retries (`blockchain_maxRetries`)**: Maximum number of retry attempts for blockchain operations that encounter transient errors.
  - Type: integer
  - Default Value: `3`
    - Impact: Not currently implemented

- **Retry Sleep Duration (`blockchain_retrySleep`)**: The wait time in milliseconds between retry attempts, implementing a back-off mechanism.
  - Type: integer
  - Default Value: `1000` (1 second)
    - Impact: Not currently implemented
  - Tuning Advice: Adjust based on the nature of expected failures (shorter for quick-recovery scenarios)

## Mining and Difficulty Settings

- **Difficulty Cache (`blockassembly_difficultyCache`)**: Enables difficulty calculation caching for performance optimization.
  - Type: boolean
  - Default Value: `true`
    - Impact: Improves performance when enabled

## Configuration Interactions and Dependencies

Some configuration settings work together or depend on other settings:

1. **gRPC Server Management**: `blockchain_grpcListenAddress` controls server startup and health checks; `blockchain_grpcAddress` required for client connections

2. **HTTP Administrative Interface**: `blockchain_httpListenAddress` must be configured for service startup; empty causes failure

3. **Difficulty Calculation**: `blockassembly_difficultyCache` improves performance; Chain parameters control difficulty algorithm behavior

4. **FSM State Management**: `fsm_state_restore` and `blockchain_initializeNodeInState` control service startup behavior and state persistence

5. **Database Operations**: `blockchain_store` determines persistence backend; `blockchain_store_dbTimeoutMillis` controls operation limits

## Critical Configuration Requirements

1. **`blockchain_httpListenAddress`** must be configured (empty causes service startup failure)
2. **`blockchain_grpcAddress`** must be configured for client functionality
3. **`blockchain_grpcListenAddress`** can be empty (disables gRPC server)
4. **`blockchain_store`** must be valid URL format for blockchain persistence
5. **`Chain configuration parameters`** must be properly configured for difficulty calculations

## Kafka Integration

The Blockchain Service integrates with Kafka for block notifications and event streaming.

### Message Formats

Block notifications are serialized using Protocol Buffers and contain:

- Block header
- Block height
- Hash
- Transaction count
- Size in bytes
- Timestamp

### Topics

- **Blocks-Final**: Used for finalized block notifications, consumed by the Block Persister service

### Error Handling

- The service implements exponential backoff retry for Kafka publishing failures
- Failed messages are logged and retried based on the `blockchain_maxRetries` and `blockchain_retrySleep` settings
- Persistent failures, after the retries are exhausted, are reported through the health monitoring endpoints (`/health` HTTP endpoint and the `HealthGRPC` gRPC method)

## Error Handling Strategies

The Blockchain Service employs several strategies to handle errors and maintain resilience:

### Network and Communication Errors

- Uses timeouts and context cancellation to handle hanging network operations
- Implements retry mechanisms for transient failures with configured backoff periods

### Validation Errors

- Blocks with invalid headers, merkle roots, or proofs are rejected with appropriate error codes
- Invalid blocks can be explicitly marked using the InvalidateBlock method

### Chain Reorganization

- Detects chain splits and reorganizations automatically
- Uses rollback and catch-up operations to handle chain reorganizations
- Limits reorganization depth for security (configurable)

### Storage Errors

- Implements transaction-based operations with the store to maintain consistency
- Reports persistent storage errors through health endpoints
