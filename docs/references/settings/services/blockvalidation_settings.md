# Block Validation Settings

**Related Topic**: [Block Validation Service](../../../topics/services/blockValidation.md)

The Block Validation service configuration can be adjusted through environment variables or command-line flags. This section provides a comprehensive overview of all available configuration options organized by functional category.

## Network and Communication Settings

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockvalidation_grpcAddress` | string | "localhost:8088" | Address that other services use to connect to this service | Affects how other services discover and communicate with the Block Validation service |
| `blockvalidation_grpcListenAddress` | string | ":8088" | Network interface and port the service listens on for gRPC connections | Controls network binding and accessibility of the service |

## Kafka and Concurrency Settings

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `kafka_blocksConfig` | string | (none) | Kafka configuration for block messages | Required for consuming blocks from Kafka |
| `blockvalidation_kafkaWorkers` | int | 0 (auto) | Number of Kafka consumer workers | Controls parallelism for Kafka-based block validation |

## Performance and Optimization

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockvalidation_batch_missing_transactions` | bool | false | When enabled, missing transactions are fetched in batches | Improves network efficiency at the cost of slightly increased latency |
| `blockvalidation_quickValidationEnabled` | bool | true | Enable quick validation for checkpointed blocks | Dramatically improves sync speed for historical blocks |
| `blockvalidation_quickValidationCheckpointHeight` | int | varies | Height below which quick validation applies | Controls which blocks use optimized validation |
| `blockvalidation_concurrency_createAllUTXOs` | int | CPU/2 (min 4) | Parallelism for UTXO creation during quick validation | Higher values improve performance but increase resource usage |
| `blockvalidation_concurrency_spendAllTransactions` | int | CPU/2 (min 4) | Parallelism for spending transactions during quick validation | Controls parallel processing during validation |
| `blockvalidation_processTxMetaUsingCache_BatchSize` | int | 1024 | Batch size for processing transaction metadata using cache | Affects performance and memory usage during cache operations |
| `blockvalidation_processTxMetaUsingCache_Concurrency` | int | 32 | Concurrency level for processing transaction metadata using cache | Controls parallel cache operations |
| `blockvalidation_processTxMetaUsingCache_MissingTxThreshold` | int | 1 | Threshold for switching to store-based processing when missing transactions | Controls fallback behavior when cache misses occur |
| `blockvalidation_processTxMetaUsingStore_BatchSize` | int | CPU/2 (min 4) | Batch size for processing transaction metadata using store | Affects performance during store operations |
| `blockvalidation_processTxMetaUsingStore_Concurrency` | int | 32 | Concurrency level for processing transaction metadata using store | Controls parallel store operations |
| `blockvalidation_processTxMetaUsingStore_MissingTxThreshold` | int | 1 | Threshold for store-based processing when missing transactions | Controls fallback behavior for store operations |
| `blockvalidation_skipCheckParentMined` | bool | false | Skips checking if parent block is mined during validation | Performance optimization that may reduce validation accuracy |
| `blockvalidation_subtreeFoundChConcurrency` | int | 1 | Concurrency level for subtree found channel processing | Controls parallel subtree processing |
| `blockvalidation_subtree_validation_abandon_threshold` | int | 1 | Threshold for abandoning subtree validation | Controls when to give up on problematic subtrees |
| `blockvalidation_validateBlockSubtreesConcurrency` | int | CPU/2 (min 4) | Concurrency level for validating block subtrees | Higher values improve performance but increase resource usage |
| `blockvalidation_validation_max_retries` | int | 3 | Maximum number of retries for validation operations | Controls resilience to transient failures |
| `blockvalidation_validation_retry_sleep` | duration | 5s | Sleep duration between validation retries | Controls backoff timing for retry operations |
| `blockvalidation_isParentMined_retry_max_retry` | int | 20 | Maximum retries for checking if parent block is mined | Controls persistence when checking parent block status |
| `blockvalidation_isParentMined_retry_backoff_multiplier` | int | 30 | Backoff multiplier for parent mined check retries | Controls exponential backoff timing |
| `blockvalidation_subtreeGroupConcurrency` | int | 1 | Concurrency level for subtree group processing | Controls parallel processing of subtree groups |
| `blockvalidation_blockFoundCh_buffer_size` | int | 1000 | Buffer size for block found channel | Controls memory usage and throughput for block notifications |
| `blockvalidation_catchupCh_buffer_size` | int | 10 | Buffer size for catchup channel | Controls memory usage for catchup operations |
| `blockvalidation_useCatchupWhenBehind` | bool | false | Enables catchup mechanism when node is behind | Improves sync performance but increases complexity |
| `blockvalidation_catchupConcurrency` | int | CPU/2 (min 4) | Concurrency level for catchup operations | Controls parallel processing during catchup |
| `blockvalidation_check_subtree_from_block_timeout` | duration | 5m | Timeout for checking subtree from block | Controls maximum wait time for subtree operations |
| `blockvalidation_check_subtree_from_block_retries` | int | 5 | Maximum retries for subtree from block checks | Controls resilience for subtree operations |
| `blockvalidation_check_subtree_from_block_retry_backoff_duration` | duration | 30s | Backoff duration for subtree check retries | Controls timing between retry attempts |
| `blockvalidation_secret_mining_threshold` | uint32 | 10 | Threshold for detecting secret mining attacks | Security parameter for chain reorganization detection |
| `blockvalidation_previous_block_header_count` | uint64 | 100 | Number of previous block headers to maintain | Controls memory usage and validation depth |
| `blockvalidation_maxPreviousBlockHeadersToCheck` | uint64 | 100 | Maximum previous block headers to check during validation | Limits validation scope for performance |
| `blockvalidation_fail_fast_validation` | bool | true | Enables fail-fast validation mode | Improves performance by stopping validation early on errors |
| `blockvalidation_finalizeBlockValidationConcurrency` | int | 8 | Concurrency level for finalizing block validation | Controls parallel finalization operations |
| `blockvalidation_getMissingTransactions` | int | 32 | Concurrency level for retrieving missing transactions | Controls parallel transaction retrieval |

## Transaction Processing

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockvalidation_localSetTxMinedConcurrency` | int | 8 | Concurrency level for marking transactions as mined | Higher values improve performance but increase memory usage |
| `blockvalidation_missingTransactionsBatchSize` | int | 5000 | Batch size for retrieving missing transactions | Larger batches improve throughput but increase memory usage |

## Bloom Filter Management

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockvalidation_bloom_filter_retention_size` | uint32 | GlobalBlockHeightRetention + 2 | Number of recent blocks to maintain bloom filters for | Affects memory usage and duplicate transaction detection efficiency. Automatically set based on global retention settings |

## Advanced Settings

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockvalidation_optimistic_mining` | bool | true | When enabled, blocks are conditionally accepted before full validation | Dramatically improves throughput at the cost of temporary chain inconsistency if validation fails |
| `blockvalidation_invalidBlockTracking` | bool | true | Track invalid blocks during validation | Prevents reprocessing of known invalid blocks |
| `blockvalidation_validation_warmup_count` | int | 128 | Number of validation operations during warmup | Helps prime caches and establish performance baselines |
| `excessiveblocksize` | int | 4GB | Maximum allowed block size | Limits resource consumption for extremely large blocks |

## Storage and State Management

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockValidationMaxRetries` | int | 3 | Maximum retry attempts for block validation operations | Controls resilience and retry behavior for failed validation operations |
| `blockValidationRetrySleep` | duration | 1s | Sleep duration between retry attempts | Controls retry timing and system load during failures |
| `utxostore` | URL | (none) | URL for the UTXO store | Required for UTXO validation and updates |
| `fsm_state_restore` | bool | false | Enables FSM state restoration | Affects recovery behavior after service restart |
| `blockvalidation_subtreeBlockHeightRetention` | uint32 | (global setting) | How long to keep subtrees (in terms of block height) | Affects storage utilization and historical data availability |

## Validator Integration Settings

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockValidationDelay` | int | 0 | Delay for block validation operations in validator | Controls timing of validation operations within the validator component |
| `blockValidationMaxRetries` | int | 3 | Maximum retries for validator block validation operations | Controls validator-specific retry behavior for block validation |
| `blockValidationRetrySleep` | string | "1s" | Sleep duration between validator retry attempts | Controls validator retry timing and backoff behavior |

## Policy and Chain Configuration Dependencies

The Block Validation service depends on several chain configuration parameters that must be properly configured for difficulty calculations and block validation.
