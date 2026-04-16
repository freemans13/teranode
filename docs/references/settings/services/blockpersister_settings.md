# Block Persister Settings

**Related Topic**: [Block Persister Service](../../../topics/services/blockPersister.md)

The Block Persister service configuration is organized into several categories that control different aspects of the service's behavior. All settings can be provided via environment variables or configuration files.

## Storage Configuration

### State Management

- **State File (`blockpersister_stateFile`)**
  - Type: `string`
  - Default Value: `"file://./data/blockpersister_state.txt"`
    - Purpose: Maintains the persister's processing state (last persisted block height and hash)
  - Format: Supports both local file paths (`file://./path`) and remote storage URLs
    - Impact: Critical for recovery after service restart and maintaining processing continuity
  - Recovery Implications: If this file is lost, the service will need to reprocess blocks from the beginning

### Block Storage

- **Block Store URL (`blockPersisterStore`)**
  - Type: `*url.URL`
  - Default Value: `"file://./data/blockstore"`
    - Purpose: Defines where block data files are stored
  - Supported Formats:

    - S3: `s3://bucket-name/prefix`
    - Local filesystem: `file://./path/to/dir`
    - Impact: Determines the persistence mechanism and reliability characteristics

- **HTTP Listen Address (`blockPersister_httpListenAddress`)**
  - Type: `string`
  - Default Value: `":8083"`
    - Purpose: Controls the network interface and port for the HTTP server that serves block data
    - Usage: If empty, no HTTP server is started; when configured, enables external access to blob store
  - Security Consideration: In production environments, should be configured based on network security requirements

## Processing Configuration

### Block Selection and Timing

- **Persist Age (`BlockPersisterPersistAge`)**
  - Type: `uint32`
  - Default Value: Not specified in settings (varies by configuration)
    - Purpose: Determines how many blocks behind the tip the persister stays
    - Impact: Critical for avoiding reorgs by ensuring blocks are sufficiently confirmed
    - Example: If set to 100, only blocks that are at least 100 blocks deep are processed
  - Tuning Advice:

    - Lower values: More immediate processing but higher risk of reprocessing due to reorgs
    - Higher values: More conservative approach with minimal reorg risk

- **Persist Sleep (`BlockPersisterPersistSleep`)**
  - Type: `time.Duration`
  - Default Value: Not specified in settings (varies by configuration)
    - Purpose: Sleep duration between polling attempts when no blocks are available to process
    - Impact: Controls polling frequency and system load during idle periods
  - Tuning Advice:

    - Shorter durations: More responsive but higher CPU usage
    - Longer durations: More resource-efficient but less responsive

### Performance Tuning

- **Processing Concurrency (`blockpersister_concurrency`)**
  - Type: `int`
  - Default Value: `8`
    - Purpose: Controls the number of concurrent goroutines for processing subtrees
    - Impact: Directly affects CPU utilization, memory usage, and throughput
  - Tuning Advice:

    - Optimal value typically depends on available CPU cores
    - For systems with 8+ cores, the default value is usually appropriate
    - For high-performance systems, consider increasing to match available cores

- **Batch Missing Transactions (`blockpersister_batchMissingTransactions`)**
  - Type: `bool`
  - Default Value: `true`
    - Purpose: Controls whether transactions are fetched in batches from the store
    - Impact: Can significantly improve performance by reducing the number of individual queries
  - Tuning Advice: Generally should be kept enabled unless encountering specific issues

- **Process TxMeta Using Store Batch Size (`blockvalidation_processTxMetaUsingStore_BatchSize`)**
  - Type: `int`
  - Default Value: `1024`
    - Purpose: Controls the batch size when processing transaction metadata from the store
    - Impact: Affects performance and memory usage when fetching transaction data
  - Tuning Advice: Higher values improve throughput at the cost of increased memory usage

### UTXO Management

- **Skip UTXO Delete (`SkipUTXODelete`)**
  - Type: `bool`
  - Default Value: `false`
    - Purpose: Controls whether UTXO deletions are skipped during processing
    - Impact: When enabled, improves performance but affects UTXO set completeness
  - Usage Scenarios:

    - Enable during initial sync or recovery to improve performance
    - Disable for normal operation to maintain complete UTXO tracking

## Configuration Interactions and Dependencies

### Storage Backend Selection

The Block Persister supports multiple storage backends through the `blockPersisterStore` URL:

**Local Filesystem:**

```text
file://./path/to/directory
```

- Best for: Development, testing, single-node deployments
- Advantages: Simple setup, fast access, no external dependencies
- Limitations: Not suitable for distributed deployments

**S3-Compatible Storage:**

```text
s3://bucket-name/prefix
```

- Best for: Production deployments, distributed systems, cloud environments
- Advantages: Highly durable, scalable, supports distributed access
- Considerations: Requires proper S3 credentials and network connectivity

### Performance vs. Accuracy Trade-offs

Several settings involve trade-offs between performance and data completeness:

1. **`blockpersister_concurrency`**: Higher values improve throughput but increase resource usage
2. **`SkipUTXODelete`**: When enabled, improves performance during sync but affects UTXO tracking completeness
3. **`blockpersister_batchMissingTransactions`**: Batching improves efficiency but may increase latency for individual operations

### State Management and Recovery

The `blockpersister_stateFile` is critical for service continuity:

- The state file tracks the last successfully persisted block
- On restart, the service resumes from this point
- If the state file is corrupted or lost, manual intervention may be required
- Consider implementing regular backups of the state file for production systems

### Timing and Synchronization

The interaction between `BlockPersisterPersistAge` and `BlockPersisterPersistSleep` controls the service's responsiveness:

- `BlockPersisterPersistAge` ensures blocks are sufficiently confirmed before persistence
- `BlockPersisterPersistSleep` controls how frequently the service checks for new blocks to process
- Together, these settings balance responsiveness against system load and reorg risk
