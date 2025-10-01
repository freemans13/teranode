# UTXO Store Settings

**Related Topic**: [UTXO Store](../../../topics/stores/utxo.md)

The UTXO Store can be configured using various connection URLs and configuration parameters. This section provides a comprehensive reference of all available configuration options.

## Connection URL Format

The `utxostore` setting determines which datastore implementation to use. The connection URL format varies depending on the selected backend:

### Aerospike

```text
aerospike://host:port/namespace?param1=value1&param2=value2
```

Example:

```text
utxostore=aerospike://aerospikeserver.teranode.dev:3000/teranode-store?set=txmeta&externalStore=blob://blobserver:8080/utxo
```

**URL Parameters:**

- `set`: Aerospike set name (default: "txmeta")
- `externalStore`: URL for storing large transactions (required)
- `ConnectionQueueSize`: Connection queue size for Aerospike client
- `LimitConnectionsToQueueSize`: Whether to limit connections to queue size

### SQL (PostgreSQL/SQLite)

**PostgreSQL:**

```text
postgres://username:password@host:port/dbname?param1=value1&param2=value2
```

Example:

```text
utxostore=postgres://miner1:miner1@postgresserver.teranode.dev:5432/teranode-store?expiration=24h
```

**SQLite:**

```text
sqlite:///path/to/file.sqlite?param1=value1&param2=value2
```

Example:

```text
utxostore=sqlite:///data/utxo.sqlite?expiration=24h
```

**In-memory SQLite:**

```text
sqlitememory:///name?param1=value1&param2=value2
```

Example:

```text
utxostore=sqlitememory:///utxo?expiration=24h
```

**URL Parameters:**

- `expiration`: Duration after which spent UTXOs are cleaned up (e.g., "24h", "7d")
- `logging`: Enable SQL query logging (true/false)

### Memory

```text
memory://host:port/mode
```

Example:

```text
utxostore=memory://localhost:${UTXO_STORE_GRPC_PORT}/splitbyhash
```

**Modes:**

- `splitbyhash`: Distributes UTXOs based on hash
- `all`: Stores all UTXOs in memory

### Nullstore

```text
null:///
```

Example:

```text
utxostore=null:///
```

## Configuration Parameters

The UTXO Store can be configured through the `UtxoStoreSettings` struct which contains various parameters to control the behavior of the store.

### General Settings

| Parameter | Type | Description | Default |
|-----------|------|-------------|--------|
| `UtxoStore` | *url.URL | Connection URL for the UTXO store | "" (must be configured) |
| `BlockHeightRetention` | uint32 | Number of blocks to retain data for | globalBlockHeightRetention |
| `BlockHeightRetentionAdjustment` | int32 | Adjustment to global block height retention (can be positive or negative) | 0 |
| `UnminedTxRetention` | uint32 | Retention period for unmined transactions in blocks | 1008 blocks (~7 days) |
| `ParentPreservationBlocks` | uint32 | Parent transaction preservation period in blocks | 1440 blocks (~10 days) |
| `UtxoBatchSize` | int | Batch size for UTXOs (critical - do not change after initial setup) | 128 |
| `DBTimeout` | time.Duration | Timeout for database operations | 5s |
| `UseExternalTxCache` | bool | Whether to use external transaction cache | true |
| `ExternalizeAllTransactions` | bool | Whether to externalize all transactions | false |
| `VerboseDebug` | bool | Enable verbose debugging | false |
| `UpdateTxMinedStatus` | bool | Whether to update transaction mined status | true |
| `DisableDAHCleaner` | bool | Disable Delete-At-Height cleaner process | false |

### SQL-specific Settings

| Parameter | Type | Description | Default |
|-----------|------|-------------|--------|
| `PostgresMaxIdleConns` | int | Maximum number of idle connections to the PostgreSQL database | 10 |
| `PostgresMaxOpenConns` | int | Maximum number of open connections to the PostgreSQL database | 80 |

### Batch Processing Settings

The UTXO Store uses batch processing to improve performance. The following settings control the behavior of various batchers:

| Parameter | Type | Description | Default |
|-----------|------|-------------|--------|
| `StoreBatcherSize` | int | Batch size for store operations | 100 |
| `StoreBatcherDurationMillis` | int | Maximum duration in milliseconds for store batching | 100 |
| `SpendBatcherSize` | int | Batch size for spend operations | 100 |
| `SpendBatcherDurationMillis` | int | Maximum duration in milliseconds for spend batching | 100 |
| `OutpointBatcherSize` | int | Batch size for outpoint operations | 100 |
| `OutpointBatcherDurationMillis` | int | Maximum duration in milliseconds for outpoint batching | 10 |
| `IncrementBatcherSize` | int | Batch size for increment operations | 256 |
| `IncrementBatcherDurationMillis` | int | Maximum duration in milliseconds for increment batching | 10 |
| `SetDAHBatcherSize` | int | Batch size for Delete-At-Height operations | 256 |
| `SetDAHBatcherDurationMillis` | int | Maximum duration in milliseconds for DAH batching | 10 |
| `LockedBatcherSize` | int | Batch size for locked operations | 256 |
| `LockedBatcherDurationMillis` | int | Maximum duration in milliseconds for locked batching | 10 |
| `GetBatcherSize` | int | Batch size for get operations | 1 |
| `GetBatcherDurationMillis` | int | Maximum duration in milliseconds for get batching | 10 |
| `MaxMinedRoutines` | int | Maximum number of concurrent goroutines for processing mined transactions | 128 |
| `MaxMinedBatchSize` | int | Maximum number of mined transactions processed in a batch | 1024 |

## Important Notes

**Critical Setting Warning:** The `UtxoBatchSize` setting must not be changed after the UTXO store has been running with Aerospike backend. It determines record organization and changing it would break store integrity.

**Block Height Retention:** The effective retention is calculated as `GlobalBlockHeightRetention + BlockHeightRetentionAdjustment`. The global value varies by deployment, and the adjustment can be positive or negative to fine-tune retention per store.

**URL Query Parameters:** The UTXO store URL supports a `logging=true` query parameter to enable detailed operation logging for debugging purposes.
