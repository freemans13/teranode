# Kafka Settings

**Related Topic**: [Kafka](../../topics/kafka/kafka.md)

Kafka configuration in Teranode is primarily specified through URLs. Each Kafka topic has its own URL with parameters that control its behavior. The URL format supports both production Kafka and in-memory testing.

## Kafka URL Format

### Production Kafka URL Format

```text
kafka://host1,host2,.../topic?param1=value1&param2=value2&...
```

Components of the URL:

- **Scheme**: Always `kafka://`
- **Hosts**: Comma-separated list of Kafka brokers (e.g., `localhost:9092,kafka2:9092`)
- **Topic**: The Kafka topic name (specified as the path component)
- **Parameters**: Query parameters that configure specific behavior

Example:

```text
kafka://localhost:9092/blocks?partitions=4&consumer_ratio=2&replication=3
```

### In-Memory Kafka URL Format (Testing)

```text
memory://topic?param1=value1&param2=value2&...
```

Components of the URL:

- **Scheme**: Always `memory://`
- **Topic**: The in-memory topic name (specified as the path component)
- **Parameters**: Same query parameters as production Kafka

Example:

```text
memory://test_blocks?partitions=2&consumer_ratio=1
```

**Usage**: The memory scheme is automatically detected by the Kafka utilities and enables in-memory message passing for unit tests and development environments. This eliminates the need for a running Kafka cluster during testing.

## URL Parameters

### Consumer Configuration Parameters

When configuring Kafka consumers via URL, the following query parameters are supported:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `partitions` | int | 1 | Number of topic partitions to consume from |
| `consumer_ratio` | int | 1 | Ratio for scaling consumer count (partitions/consumer_ratio) |
| `replay` | int | 1 | Whether to replay messages from beginning (1=true, 0=false) |
| `group_id` | string | - | Consumer group identifier for coordination |

**Example Consumer URL:**

```text
kafka://localhost:9092/transactions?partitions=4&consumer_ratio=2&replay=0&group_id=validator-group
```

### Producer Configuration Parameters

When configuring Kafka producers via URL, the following query parameters are supported:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `partitions` | int | 1 | Number of topic partitions to create |
| `replication` | int | 1 | Replication factor for topic |
| `retention` | string | "600000" | Message retention period (ms) |
| `segment_bytes` | string | "1073741824" | Segment size in bytes (1GB) |
| `flush_bytes` | int | varies | Flush threshold in bytes (1MB async, 1KB sync) |
| `flush_messages` | int | 50000 | Number of messages before flush |
| `flush_frequency` | string | "10s" | Time-based flush frequency |
| `flush_timeout` | Duration | 10s | Maximum time to wait before flushing pending messages |

**Example Producer URL:**

```text
kafka://localhost:9092/blocks?partitions=2&replication=3&retention=3600000&flush_frequency=5s
```

## Parameter Details

### partitions

- **Type**: Integer
- **Default**: 1
- **Description**: Number of partitions for the topic
- **Impact**: Higher values increase parallelism but also resource usage

### replication

- **Type**: Integer
- **Default**: 1
- **Description**: Replication factor for the topic
- **Impact**: Higher values improve fault tolerance but increase storage requirements

### consumer_ratio

- **Type**: Integer
- **Default**: 1
- **Description**: Ratio of consumers to partitions for load balancing
- **Formula**: `consumers = partitions / consumer_ratio`
- **Impact**: Higher values reduce concurrency, lower values increase consumer count
- **Example**: `kafka://localhost:9092/blocks?consumer_ratio=2` creates half as many consumers as partitions

### retention

- **Type**: String (milliseconds)
- **Default**: "600000" (10 minutes)
- **Description**: How long messages are retained
- **Impact**: Longer retention increases storage requirements

### segment_bytes

- **Type**: String/Integer
- **Default**: "1073741824" (1GB)
- **Description**: Maximum size of a single log segment file
- **Impact**: Smaller values create more files but allow more granular cleanup
- **Example**: `kafka://localhost:9092/blocks?segment_bytes=536870912` (512MB)

### flush_bytes

- **Type**: Integer
- **Default**: 1024
- **Description**: Number of bytes to accumulate before forcing a flush
- **Impact**: Larger values improve throughput but increase risk of data loss

### flush_messages

- **Type**: Integer
- **Default**: 50000
- **Description**: Number of messages to accumulate before forcing a flush
- **Impact**: Larger values improve throughput but increase risk of data loss

### flush_frequency

- **Type**: Duration (e.g., "5s")
- **Default**: "10s" (10 seconds)
- **Description**: Maximum time between flushes
- **Impact**: Longer durations improve throughput but increase risk of data loss

### flush_timeout

- **Type**: Duration
- **Default**: 10s
- **Description**: Maximum time to wait before flushing pending messages
- **Usage**: Producer timeout configuration
- **Impact**: Ensures messages are sent even with low throughput

### replay

- **Type**: Integer (boolean: 0 or 1)
- **Default**: 1 (true)
- **Description**: Whether to replay messages from the beginning for new consumer groups
- **Impact**: Controls initial behavior of new consumers

## Auto-Commit Behavior by Topic

Teranode implements different auto-commit strategies based on message criticality and service requirements.

### Critical Topics (Auto-Commit: false)

These topics require guaranteed message processing and cannot tolerate message loss:

- **`kafka_blocksConfig`**: Block distribution for validation
  - **Reason**: Missing blocks would break blockchain validation
  - **Consumer Behavior**: Manual commit after successful processing
  - **Failure Handling**: Message redelivery on processing failure

- **`kafka_blocksFinalConfig`**: Finalized blocks for storage
  - **Reason**: Missing finalized blocks would corrupt blockchain state
  - **Consumer Behavior**: Manual commit after successful storage
  - **Failure Handling**: Message redelivery on storage failure

### Non-Critical Topics (Auto-Commit: true)

These topics can tolerate occasional message loss for performance:

- **TxMeta Cache (Subtree Validation)**: `autoCommit=true`
  - Rationale: Metadata can be regenerated if lost
  - Performance priority over strict delivery guarantees

- **Rejected Transactions (P2P)**: `autoCommit=true`
  - Rationale: Rejection notifications are not critical for consistency
  - Network efficiency prioritized

## Service-Specific Kafka Settings

### Kafka Consumer Concurrency

**Important**: Kafka consumer concurrency in Teranode is controlled through the `consumer_ratio` URL parameter for each topic. The actual number of consumers is calculated as:

```text
consumerCount = partitions / consumer_ratio
```

Common consumer ratios in use:

- `consumer_ratio=1`: One consumer per partition (maximum parallelism)
- `consumer_ratio=4`: One consumer per 4 partitions (balanced approach)

### Propagation Service Settings

- **`validator_kafka_maxMessageBytes`**: Size threshold for routing decisions
  - **Purpose**: Determines when to use HTTP fallback vs Kafka
  - **Default**: 1048576 (1MB)
  - **Usage**: Large transactions routed via HTTP to avoid Kafka message size limits

### Validator Service Settings

- **`validator_kafkaWorkers`**: Number of concurrent Kafka processing workers
  - **Purpose**: Controls parallel transaction processing capacity
  - **Tuning**: Should match CPU cores and expected transaction volume
  - **Integration**: Works with Block Assembly via direct gRPC (not Kafka)

## Configuration Examples

### High-Throughput Service (Propagation)

```text
kafka_validatortxsConfig=kafka://localhost:9092/validator-txs?partitions=8&consumer_ratio=2&flush_frequency=1s
```

This configuration creates 4 consumers (8 partitions / 2 ratio) with aggressive flushing for low latency.

### Critical Service (Block Validation)

```text
kafka_blocksConfig=kafka://localhost:9092/blocks?partitions=4&consumer_ratio=1&retention=3600000
```

This configuration creates 4 consumers (maximum parallelism) with 1-hour retention for reliability.

### Development/Testing

```text
memory://test_blocks?partitions=2&consumer_ratio=1
```

This configuration uses in-memory Kafka simulation for testing without infrastructure dependencies.
