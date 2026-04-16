# Legacy Settings

**Related Topic**: [Legacy Service](../../../topics/services/legacy.md)

The Legacy service bridges traditional Bitcoin nodes with the Teranode architecture, making its configuration particularly important for network integration. This section provides a comprehensive overview of all configuration options, organized by functional category.

## Network and Communication Settings

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `legacy_listen_addresses` | []string | [] (falls back to external IP:8333) | Network addresses for the service to listen on for peer connections | Controls which interfaces and ports accept connections from other nodes |
| `legacy_connect_peers` | []string | [] | Peer addresses to connect to on startup | Forces connections to specific peers rather than using automatic peer discovery |
| `legacy_grpcAddress` | string | "" | Address for other services to connect to the Legacy service | Enables service-to-service communication |
| `legacy_grpcListenAddress` | string | "" | Interface and port to listen on for gRPC connections | Controls network binding for the gRPC server |
| `network` | string | "mainnet" | Specifies which blockchain network to connect to | Determines network rules, consensus parameters, and compatibility |

## Memory and Storage Management

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `legacy_orphanEvictionDuration` | duration | 10m | How long orphan transactions are kept before eviction | Affects memory usage and ability to process delayed transactions |
| `legacy_writeMsgBlocksToDisk` | bool | false | **Enable disk-based block queueing during synchronization** | **Significantly reduces memory usage** by writing incoming blocks to temporary disk storage with 4MB buffered I/O and automatic 10-minute cleanup. Essential for resource-constrained environments and high-volume sync operations |
| `legacy_storeBatcherSize` | int | 1024 | Batch size for store operations | Affects efficiency of storage operations and memory usage |
| `legacy_spendBatcherSize` | int | 1024 | Batch size for spend operations | Affects efficiency of spend operations and memory usage |
| `legacy_outpointBatcherSize` | int | 1024 | Batch size for outpoint operations | Affects efficiency of outpoint processing |
| `legacy_workingDir` | string | "../../data" | Directory where service stores data files | Controls where peer information and other data is stored |
| `temp_store` | string | "file://./data/tempstore" | Temporary storage URL for Legacy Service operations | Controls temporary file storage location for block processing and peer data |

## Concurrency and Performance

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `legacy_storeBatcherConcurrency` | int | 32 | Number of concurrent store operations | Controls parallelism for storage operations |
| `legacy_spendBatcherConcurrency` | int | 32 | Number of concurrent spend operations | Controls parallelism for spend operations |
| `legacy_outpointBatcherConcurrency` | int | 32 | Number of concurrent outpoint operations | Controls parallelism for outpoint operations |

## Peer Management and Timeouts

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `legacy_savePeers` | bool | false | Save peer information to disk for reuse on restart | Enables persistent peer connections across service restarts |
| `legacy_allowSyncCandidateFromLocalPeers` | bool | false | Allow local peers as sync candidates | Affects peer selection for blockchain synchronization |
| `legacy_printInvMessages` | bool | false | Print inventory messages to logs | Increases log verbosity for debugging |
| `legacy_peerIdleTimeout` | duration | 125s | Timeout for idle peer connections | Controls when peers are disconnected due to inactivity. Set to 125s to accommodate 2-minute ping/pong intervals |
| `legacy_peerProcessingTimeout` | duration | 3m | Timeout for peer message processing | Maximum time allowed for processing messages from peers. Block processing is typically the largest operation |

## Feature Flags

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `legacy_allowBlockPriority` | bool | false | Prioritize transactions based on block priority | Affects transaction selection for block creation |

## Configuration Interactions and Dependencies

### Peer Discovery and Connection Management

The Legacy service's peer connection behavior is controlled by several interrelated settings:

- `legacy_listen_addresses` determines where the service listens for incoming connections
- `legacy_connect_peers` forces outbound connections to specific peers
- `legacy_savePeers` enables storing peer information across restarts
- If no `legacy_listen_addresses` are specified, the service detects the external IP and uses port 8333

These settings should be configured together based on your network architecture and security requirements.

### Memory Management Considerations

Several settings affect the memory usage patterns of the Legacy service:

- `legacy_writeMsgBlocksToDisk` significantly reduces memory usage during blockchain synchronization by writing blocks to disk rather than keeping them in memory
- `legacy_orphanEvictionDuration` controls how aggressively orphan transactions are removed from memory
- The various batcher size settings control memory usage during batch operations

For resource-constrained environments, enable `legacy_writeMsgBlocksToDisk` and use smaller batch sizes.

### Disk-Based Block Queueing (`legacy_writeMsgBlocksToDisk`)

The `legacy_writeMsgBlocksToDisk` setting enables a sophisticated disk-based queueing mechanism that fundamentally changes how the Legacy service handles incoming blocks during synchronization.

**How It Works:**

- It creates a streaming pipeline that writes blocks directly to disk, employing 4MB buffered readers for optimal disk performance
- Blocks are stored with a 10-minute TTL to prevent disk accumulation

**Technical Implementation:**

```text
Incoming Block → io.Pipe() → 4MB Buffer → Temporary Disk Storage → Validation Queue
                     ↓
              Background Goroutine
```

**Benefits:**

- **Memory Efficiency**: Prevents memory exhaustion during large-scale synchronization
- **Scalability**: Handles high-volume block arrivals without resource constraints
- **Reliability**: Reduces risk of out-of-memory errors during initial sync
- **Performance**: Maintains processing throughput while using minimal memory

**When to Enable:**

- ✅ **Initial blockchain synchronization** - Essential for syncing from genesis
- ✅ **Resource-constrained environments** - Limited RAM availability
- ✅ **High-volume block processing** - During catch-up operations
- ❌ **Normal operation** - Not typically needed when fully synchronized
