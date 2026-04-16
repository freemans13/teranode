# Subtree Validation Settings

**Related Topic**: [Subtree Validation Service](../../../topics/services/subtreeValidation.md)

The Subtree Validation service relies on a set of configuration settings that control its behavior, performance, and resource usage. This section provides a comprehensive overview of these settings, organized by functional category, along with their impacts, dependencies, and recommended configurations for different deployment scenarios.

## Configuration Categories

Subtree Validation service settings can be organized into the following functional categories:

1. **Quorum & Coordination**: Settings that control how subtree validation is coordinated across the system
2. **Transaction Processing**: Settings that manage transaction retrieval and validation
3. **Performance & Scaling**: Settings that control concurrency, batch sizes, and resource usage
4. **Caching & Memory**: Settings that manage the transaction metadata cache
5. **Network & Communication**: Settings for network binding and service communication

## Quorum & Coordination Settings

These settings control how subtree validation is coordinated to prevent duplicate work and ensure consistency.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `subtree_quorum_path` | string | `""` (empty) | **REQUIRED** - Directory path where quorum data is stored for subtree validation | Critical - service will not initialize without this path configured |
| `subtree_quorum_absolute_timeout` | time.Duration | `30s` | Maximum time to wait for quorum operations to complete | Controls deadlock prevention and failure recovery during validation |
| `subtreevalidation_subtree_validation_abandon_threshold` | int | `1` | Number of sequential validation failures before abandoning validation attempts | Controls resilience and retry behavior for validation errors |
| `subtreevalidation_failfast_validation` | bool | `true` | Controls whether validation stops at the first error or continues | Affects validation performance and error reporting behavior |
| `subtreevalidation_orphanageTimeout` | time.Duration | `15m` | Timeout for orphaned transactions in the orphanage cache | Controls memory usage and cleanup of orphaned transaction data |
| `subtreevalidation_subtreeBlockHeightRetention` | uint32 | `globalBlockHeightRetention` | Block height retention for subtree data | Controls how long subtree data is retained based on block height |
| `subtreevalidation_blockHeightRetentionAdjustment` | int32 | `0` | Adjustment to global block height retention (can be positive or negative) | Fine-tunes retention period for subtree-specific requirements |

### Quorum Interactions and Dependencies

The quorum mechanism ensures that subtree validation is coordinated across the system, preventing duplicate validation work:

- The quorum path specifies where lock files are stored to coordinate validation work
- The absolute timeout prevents deadlocks if a lock holder crashes or network issues occur
- Quorum locks are acquired per subtree hash to ensure only one validator processes each subtree

When a subtree fails validation multiple times (reaching the abandon threshold), it is marked as permanently invalid to prevent wasting resources on unrecoverable subtrees.

## Transaction Processing Settings

These settings control how transactions are retrieved, validated, and processed during subtree validation.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `subtreevalidation_failfast_validation` | bool | `true` | Controls whether validation stops at the first error or continues | Affects validation performance and error reporting behavior |
| `subtreevalidation_validation_max_retries` | int | `30` | Maximum number of retry attempts for validation operations | Controls resilience and error handling during validation |
| `subtreevalidation_validation_retry_sleep` | string | `"5s"` | Time to wait between validation retry attempts | Controls backoff behavior during validation errors |
| `subtreevalidation_batch_missing_transactions` | bool | `true` | Controls whether missing transaction retrieval is batched | Affects network efficiency and transaction retrieval performance |
| `subtreevalidation_missingTransactionsBatchSize` | int | `16384` | Maximum number of transactions to retrieve in a single batch | Controls network efficiency and memory usage during retrieval |
| `subtreevalidation_percentageMissingGetFullData` | float64 | `20.0` | Percentage threshold for switching to full data retrieval | Controls when the service retrieves full transaction data based on missing rate |
| `subtreevalidation_blacklisted_baseurls` | map[string]struct{} | `{}` (empty) | Set of blacklisted base URLs for subtree validation | Prevents validation attempts from known problematic sources |

### Transaction Processing Interactions and Dependencies

The transaction processing settings work together to balance performance, reliability, and resource usage:

- When a subtree contains transactions not available locally, the service retrieves them from remote sources
- Batch processing (`batch_missing_transactions`) improves network efficiency by grouping requests
- The retry mechanism (`validation_max_retries` and `validation_retry_sleep`) handles transient failures
- The percentage threshold (`percentageMissingGetFullData`) optimizes retrieval strategies based on the proportion of missing transactions

## Performance & Scaling Settings

These settings control concurrency, parallelism, and batch sizes to optimize performance and resource usage.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `subtreevalidation_getMissingTransactions` | int | max(4, numCPU/2) | Number of concurrent workers for fetching missing transactions | Controls parallelism for transaction retrieval operations |
| `subtreevalidation_subtreeDAHConcurrency` | int | `8` | Number of concurrent workers for processing Direct Acyclic Hash operations | Controls parallelism for DAH computations during validation |
| `subtreevalidation_processTxMetaUsingStoreConcurrency` | int | `32` | Number of concurrent workers for store-based metadata processing | Controls parallelism and I/O load during validation |
| `subtreevalidation_processTxMetaUsingCacheConcurrency` | int | `32` | Number of concurrent workers for cached metadata processing | Controls parallelism and CPU utilization during validation |
| `subtreevalidation_spendBatcherSize` | int | `1024` | Batch size for processing spend operations | Controls I/O patterns and throughput for UTXO spend operations |
| `subtreevalidation_processTxMetaUsingStoreBatchSize` | int | `1024` | Batch size for processing transaction metadata from store | Affects I/O patterns and throughput for store-based metadata processing |
| `subtreevalidation_processTxMetaUsingCacheBatchSize` | int | `1024` | Batch size for processing transaction metadata using cache | Affects memory usage and throughput for cached metadata processing |
| `subtreevalidation_check_block_subtrees_concurrency` | int | `32` | Number of concurrent workers for processing block subtrees | Controls parallelism for block subtree validation operations |

### Performance & Scaling Interactions and Dependencies

These settings work together to optimize resource usage and throughput:

- Concurrency settings control how many operations can run in parallel, balancing CPU utilization against context switching overhead
- Batch size settings control memory usage and I/O efficiency by grouping related operations
- Different operations (retrieval, processing, validation) have separate concurrency controls to optimize each phase
- Default values are selected to balance performance against resource consumption, but may need tuning based on specific hardware and workloads

## Caching & Memory Settings

These settings control the transaction metadata cache, which significantly impacts performance and memory usage.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `subtreevalidation_txMetaCacheEnabled` | bool | `true` | Controls whether transaction metadata caching is enabled | Significantly affects performance and memory usage patterns |
| `txMetaCacheMaxMB` | int | `256` | Maximum memory (in MB) to use for transaction metadata cache | Controls memory footprint and cache hit rates |
| `subtreevalidation_processTxMetaUsingCacheMissingTxThreshold` | int | `1` | Threshold for switching to full transaction data retrieval when using cache | Controls when the service fetches full transaction data vs. metadata |
| `subtreevalidation_processTxMetaUsingStoreMissingTxThreshold` | int | `1` | Threshold for switching to full transaction data retrieval when using store | Controls when the service fetches full transaction data vs. metadata |
| `subtreevalidation_txChanBufferSize` | int | `0` | Buffer size for transaction processing channels | Controls buffering behavior for transaction processing queues |

### Caching & Memory Interactions and Dependencies

The caching system is critical for high-performance validation:

- When caching is enabled (`txMetaCacheEnabled=true`), a memory-based cache layer is created over the UTXO store
- The cache size (`txMetaCacheMaxMB`) directly impacts memory usage and performance - larger caches improve hit rates but consume more memory
- The missing transaction thresholds control when the service switches from metadata validation to full transaction validation, which is more resource-intensive but necessary in some cases
- Separate thresholds for cache and store operations allow optimizing each path independently

## Storage & Data Settings

These settings control data storage and retention for subtree validation.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `subtreestore` | string | `""` (empty) | **REQUIRED** - URL for subtree blob storage backend | Determines where subtrees are stored and retrieved |
| `utxostore` | string | `""` (empty) | **REQUIRED** - UTXO store URL for validation operations | Critical for transaction validation and UTXO state access |

## Kafka Integration Settings

These settings control Kafka message queue integration for system-wide communication.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `kafka_invalidSubtrees` | string | `"invalid-subtrees"` | Kafka topic name for publishing invalid subtree notifications | Enables system-wide error notification and monitoring |
| `kafka_invalidSubtreesConfig` | string | `""` (empty) | Kafka URL configuration for invalid subtrees topic | Overrides default Kafka configuration for invalid subtrees |
| `kafka_invalidBlocksConfig` | string | `""` (empty) | Kafka URL configuration for invalid blocks notifications | Enables integration with block validation error handling |

## Network & Communication Settings

These settings control how the service communicates with other components in the system.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `subtreevalidation_grpcAddress` | string | `"localhost:8089"` | **REQUIRED** - Address for connecting to the subtree validation service | Affects how other services connect to this service |
| `subtreevalidation_grpcListenAddress` | string | `:8089` | Address where the service listens for gRPC connections | Controls network binding for service communication |
| `subtreevalidation_subtreeValidationTimeout` | int | `1000` | Timeout (ms) for subtree validation operations | Controls error recovery and prevents hanging validation processes |
| `validator.useLocalValidator` | bool | `false` | Controls whether to use a local or remote validator | Affects system architecture and validation performance |
