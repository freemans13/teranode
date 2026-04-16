# UTXO Persister Settings

**Related Topic**: [UTXO Persister Service](../../../topics/services/utxoPersister.md)

The UTXO Persister service relies on a set of configuration settings that control its behavior, performance, and resource usage. This section provides a comprehensive overview of these settings, organized by functional category, along with their impacts, dependencies, and recommended configurations for different deployment scenarios.

## Configuration Categories

UTXO Persister service settings can be organized into the following functional categories:

1. **Performance Tuning**: Settings that control I/O performance and memory usage
2. **Storage Management**: Settings that manage file storage and retention policies
3. **Deployment Architecture**: Settings that determine how the service interacts with other components
4. **Operational Controls**: Settings that affect general service behavior

## Performance Tuning Settings

These settings control the I/O performance and memory usage patterns of the UTXO Persister service.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `utxoPersister_buffer_size` | string | `"4KB"` | Controls the buffer size for reading from and writing to UTXO files | Affects I/O performance and memory usage when processing UTXO data |

### Performance Tuning Interactions and Dependencies

The buffer size setting directly affects how efficiently the service reads and writes UTXO data:

- Larger buffer sizes (e.g., 64KB to 1MB) can significantly improve I/O throughput by reducing the number of system calls needed for file operations
- Smaller buffer sizes reduce memory usage but may increase CPU overhead due to more frequent I/O operations
- The optimal buffer size depends on the hardware characteristics, particularly disk I/O capabilities, available memory, and the size of typical UTXO files

For high-throughput environments with fast storage systems (like SSDs), larger buffer sizes provide better performance. For memory-constrained environments, smaller buffers may be necessary despite the performance impact.

## Storage Management Settings

These settings control how UTXO data is stored and retained.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockpersister_skipUTXODelete` | bool | `false` | When true, previous block's UTXO sets aren't deleted after processing | Controls storage usage and retention policy for historical UTXO sets |
| `blockstore` | *url.URL | `"file://./data/blockstore"` | Specifies the URL for the block storage backend | Determines where block data, including UTXO sets, are stored |
| `txstore` | *url.URL | `""` | Specifies the URL for the transaction storage backend | Determines where transaction data is stored for block processing |
| `txmeta_store` | *url.URL | `""` | Specifies the URL for the UTXO metadata storage backend | Determines where UTXO metadata is stored for efficient lookups |

### Storage Management Interactions and Dependencies

These settings determine the storage footprint and data persistence behavior of the service:

- When `blockpersister_skipUTXODelete` is `false` (default), the service maintains minimal storage by only keeping the UTXO set for the most recent processed block
- When set to `true`, the service preserves all historical UTXO sets, which increases storage requirements but enables historical analysis and validation
- The `blockstore` setting defines where all block-related data is stored, affecting both read and write performance based on the underlying storage system
- The `txstore` and `txmeta_store` settings determine where transaction data and UTXO metadata are stored, respectively, which impacts performance and data availability

Storage requirements grow significantly when keeping historical UTXO sets, as each set contains the complete state of all unspent outputs at a given block height.

## Deployment Architecture Settings

These settings control how the UTXO Persister interacts with other components in the system.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `direct` | bool | `true` | Controls whether the service connects directly to the blockchain store or uses the client interface | Affects performance and deployment architecture |

### Deployment Architecture Interactions and Dependencies

The deployment architecture settings determine how the service integrates with the broader system:

- When `direct` is `true`, the service bypasses the blockchain client interface and connects directly to the blockchain store, which improves performance but requires the service to be deployed in the same process
- When `direct` is `false`, the service uses the blockchain client interface, allowing for distributed deployment at the cost of additional network overhead

This setting has significant implications for system design and deployment flexibility. Direct access provides better performance but limits deployment options, while client-based access enables more flexible deployment topologies but may impact performance.

## Operational Controls Settings

These settings control general operational aspects of the service.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `network` | string | `"mainnet"` | Specifies the blockchain network (mainnet, testnet, regtest) | Determines genesis hash and chain parameters used for validation |

### Operational Controls Interactions and Dependencies

- The `network` setting determines the genesis hash and chain parameters via chaincfg.GetChainParams(), which is critical for genesis block detection and UTXO set validation
- Genesis hash validation is used throughout the service to determine when to skip certain operations like UTXO deletion

This setting is fundamental to the service's operation and must match the target blockchain network.

## Configuration Best Practices

1. **Performance Monitoring**: Regularly monitor I/O performance metrics when adjusting buffer sizes. Balance memory usage against throughput based on your specific hardware capabilities.

2. **Storage Planning**: When using `blockpersister_skipUTXODelete=true`, implement a storage monitoring and management strategy. UTXO sets grow significantly over time and may require substantial storage capacity.

3. **Deployment Architecture**: Choose direct access (`direct=true`) whenever possible for best performance, unless your system architecture specifically requires distributed deployment.

4. **Network Configuration**: Ensure the `network` setting matches your target blockchain environment. Incorrect network configuration can lead to validation failures and data corruption.

5. **Storage Location**: Use persistent, reliable storage locations for the `blockstore` setting in production environments, ideally on dedicated, high-performance storage systems.

6. **Backup Strategy**: Implement regular backups of your UTXO data, especially the most recent UTXO set, to enable rapid recovery in case of data corruption or storage failures.

7. **Service Coordination**: Ensure that blockchain services and UTXO Persister services are properly coordinated in terms of startup sequence and operational dependencies, particularly when using direct access mode.
