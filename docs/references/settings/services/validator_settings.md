# Validator Settings

**Related Topic**: [Validator Service](../../../topics/services/validator.md)

The TX Validator service relies on a set of configuration settings that control its behavior, performance, and integration with other services. This section provides a comprehensive overview of these settings, organized by functional category, along with their impacts, dependencies, and recommended configurations for different deployment scenarios.

## Configuration Categories

TX Validator service settings can be organized into the following functional categories:

1. **Deployment Architecture**: Settings that determine how the validator is deployed and integrated
2. **Network & Communication**: Settings that control network binding and API endpoints
3. **Performance & Throughput**: Settings that affect transaction processing performance
4. **Kafka Integration**: Settings for message-based communication
5. **Validation Rules**: Settings that control transaction validation policies
6. **Monitoring & Debugging**: Settings for observability and troubleshooting

## Deployment Architecture Settings

These settings fundamentally change how the validator operates within the system architecture.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `useLocalValidator` | bool | `false` | Controls whether validation is performed locally or via remote service | Significantly affects system architecture, latency, and deployment model |
| `blockassembly_disabled` | bool | `false` | Controls whether validator integrates with block assembly service | Critical for mining operations - when disabled, validated transactions are not sent to block assembly |

## Network & Communication Settings

These settings control how the validator communicates over the network.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `validator_grpcAddress` | string | `"localhost:8081"` | Address for connecting to the validator gRPC service | Determines how other services connect to the validator |
| `validator_grpcListenAddress` | string | `":8081"` | Address on which the validator gRPC server listens | Controls network binding for incoming validation requests |
| `validator_httpListenAddress` | string | `""` | Address on which the HTTP API server listens | Enables HTTP-based validation requests when set |
| `validator_httpAddress` | *url.URL | `nil` | URL for connecting to the validator HTTP API | Determines how other services connect to the HTTP interface |
| `validator_httpRateLimit` | int | `1024` | Maximum number of HTTP requests per second | Prevents resource exhaustion from excessive requests |

## Performance & Throughput Settings

These settings control how efficiently transactions are processed.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `validator_sendBatchSize` | int | `100` | Number of transactions to accumulate before batch processing | Balances throughput against latency for transaction processing |
| `validator_sendBatchTimeout` | int | `2` | Maximum time (milliseconds) to wait before processing a partial batch | Ensures timely processing of transactions even when volume is low |
| `validator_sendBatchWorkers` | int | `10` | Number of worker goroutines for batch send operations | Controls parallelism for outbound transaction processing |
| `validator_blockvalidation_delay` | int | `0` | Artificial delay (milliseconds) introduced before block validation | Can be used for testing or throttling validation operations |
| `validator_blockvalidation_maxRetries` | int | `5` | Maximum number of retries for failed block validations | Affects resilience against transient failures |
| `validator_blockvalidation_retrySleep` | string | `"2s"` | Time to wait between block validation retry attempts | Controls backoff strategy for failed validations |

## Kafka Integration Settings

These settings control message-based communication via Kafka.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `validator_kafkaWorkers` | int | `0` | Number of worker goroutines for Kafka message processing | Affects concurrency and throughput for Kafka-based transaction handling |
| `kafka_txmetaConfig` | URL | `""` | URL for the Kafka configuration for transaction metadata | Configures how transaction metadata is published to Kafka for subtree validation |
| `kafka_rejectedTxConfig` | URL | `""` | URL for the Kafka configuration for rejected transactions | Configures how rejected transaction information is published to P2P notification |
| `validator_kafka_maxMessageBytes` | int | `1048576` | Maximum size of Kafka messages in bytes | Limits the size of transactions that can be processed via Kafka |

**Critical Configuration Requirements:**

- `kafka_txmetaConfig` must be configured for the validator service to function - without it, transaction metadata cannot be published to the Subtree Validation service
- `validator_kafka_maxMessageBytes` must be coordinated with Kafka broker's `message.max.bytes` setting to prevent message rejection

## Validation Rules Settings

These settings control the rules and policies applied during transaction validation.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `maxtxsizepolicy` | int | Varies | Maximum allowed transaction size in bytes | Restricts oversized transactions from entering the mempool |
| `minminingtxfee` | float64 | Varies | Minimum fee required for transaction acceptance | Sets economic barrier for transaction inclusion |

## Monitoring & Debugging Settings

These settings control observability and diagnostics.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `validator_verbose_debug` | bool | `false` | Enables detailed debug logging for validator operations | Provides additional diagnostic information at the cost of log verbosity |
| `fsm_state_restore` | bool | `false` | Controls whether the service restores from a previously saved state | Affects recovery behavior after restarts or failures |

## Configuration Settings Affecting Validator Behavior

The Validator service behavior is controlled by several key configuration parameters:

- **`KafkaMaxMessageBytes`** (default: 1MB): Controls size-based routing - large transactions that exceed this threshold are routed via HTTP instead of Kafka to avoid message size limitations.
- **`UseLocalValidator`** (default: false): Determines whether to use a local validator instance or connect to a remote validator service via gRPC.
- **`KafkaWorkers`** (default: 0): Controls the number of concurrent Kafka message processing workers. When set to 0, Kafka consumer processing is disabled.
- **`HTTPRateLimit`** (default: 1024): Sets the rate limit for HTTP API requests to prevent service overload.
- **`VerboseDebug`** (default: false): Enables detailed validation logging for troubleshooting.

## Rejected Transaction Handling

When the Transaction Validator Service identifies an invalid transaction, it employs a Kafka-based notification system to inform other components of the system:

1. **Transaction Validation**: The Validator receives a transaction for validation via a gRPC call to the `ValidateTransaction` method
2. **Identification of Invalid Transactions**: If the transaction fails any of the validation checks, it is deemed invalid (rejected)
3. **Notification of Rejected Transactions**: When a transaction is rejected, the Validator publishes information about the rejected transaction to a dedicated Kafka topic (`rejectedTx`)
4. **Kafka Message Content**: The Kafka message for a rejected transaction typically includes:
   - The transaction ID (hash)
   - The reason for rejection (error message)
5. **Consumption of Rejected Transaction Notifications**: Other services in the system, such as the P2P Service, can subscribe to this Kafka topic

## Two-Phase Transaction Commit Process

The Validator implements a two-phase commit process for transaction creation and addition to block assembly:

### Phase 1 - Transaction Creation with Locked Flag

When a transaction is created, it is initially stored in the UTXO store with an "locked" flag set to `true`. This flag prevents the transaction outputs from being spent while it's in the process of being validated and added to block assembly, protecting against potential double-spend attempts.

### Phase 2 - Unsetting the Locked Flag

The locked flag is unset in two key scenarios:

**a. After Successful Addition to Block Assembly:**

- When a transaction is successfully validated and added to the block assembly, the Validator service immediately unsets the "locked" flag (sets it to `false`)
- This makes the transaction outputs available for spending in subsequent transactions, even before the transaction is mined in a block

**b. When Mined in a Block (Fallback Mechanism):**

- As a fallback mechanism, if the flag hasn't been unset already, it will be unset when the transaction is mined in a block
- When the transaction is mined in a block and that block is processed by the Block Validation service, the "locked" flag is unset (set to `false`) during the `SetMinedMulti` operation

### Ignoring Locked Flag for Block Transactions

When processing transactions that are part of a block (as opposed to new transactions to include in an upcoming block), the validator can be configured to ignore the locked flag. This is necessary because transactions in a block have already been validated by miners and must be accepted regardless of their locked status. The validator uses the `WithIgnoreLocked` option to control this behavior during transaction validation.

This two-phase commit approach ensures that transactions are only made spendable after they've been successfully added to block assembly, reducing the risk of race conditions and double-spend attempts during the transaction processing lifecycle.

> **For a comprehensive explanation of the two-phase commit process across the entire system, see the [Two-Phase Transaction Commit Process](../../../topics/features/two_phase_commit.md) documentation.**
