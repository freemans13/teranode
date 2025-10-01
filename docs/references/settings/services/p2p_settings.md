# P2P Settings

**Related Topic**: [P2P Service](../../../topics/services/p2p.md)

The P2P service serves as the communication backbone of the Teranode network, enabling nodes to discover, connect, and exchange data with each other. This section provides a comprehensive overview of all configuration options, organized by functional category.

## Network and Discovery Settings

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `p2p_listen_addresses` | []string | [] | Network addresses for the P2P service to listen on | Controls which interfaces and ports the service binds to for accepting connections |
| `p2p_advertise_addresses` | []string | [] | Addresses to advertise to other peers | Affects how other peers discover and connect to this node. Supports both IP addresses and domain names with optional port specification (e.g., `192.168.1.1`, `example.com:9906`). When port is omitted, the `p2p_port` value is used. |
| `p2p_port` | int | 9906 | Default port for P2P communication | Used as the fallback port when addresses don't specify a port |
| `p2p_share_private_addresses` | bool | true | Advertise private/local IP addresses to peers | Controls whether private addresses are shared for local/test environments vs production privacy |
| `p2p_bootstrapAddresses` | []string | [] | Initial peer addresses for bootstrapping the DHT | Helps new nodes join the network by providing entry points |
| `p2p_bootstrap_persistent` | bool | false | Add bootstrap addresses to static peers | When enabled, bootstrap addresses are automatically added to the static peers list for persistent connections |
| `p2p_static_peers` | []string | [] | Peer addresses to connect to on startup | Ensures connections to specific peers regardless of discovery |
| `p2p_dht_protocol_id` | string | "" | Protocol identifier for DHT communications | Affects how nodes discover each other in the network |
| `p2p_dht_use_private` | bool | false | Use private DHT mode | Restricts DHT communication to trusted peers when enabled |
| `p2p_optimise_retries` | bool | false | Optimize retry behavior for peer connections | Improves efficiency of connection attempts in certain network conditions |
| `listen_mode` | string | "full" | Node operation mode | Controls node behavior: "full" for normal operation, "listen_only" for passive mode |

## Service Endpoints

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `p2p_grpcAddress` | string | "" | Address for other services to connect to this service | Enables service-to-service communication |
| `p2p_grpcListenAddress` | string | ":9906" | Interface and port to listen on for gRPC connections | Controls network binding for the gRPC server |
| `p2p_httpAddress` | string | "localhost:9906" | Address other services use to connect to HTTP API | Affects how other services discover the P2P HTTP API |
| `p2p_httpListenAddress` | string | "" | Interface and port to listen on for HTTP connections | Controls network binding for the HTTP server |
| `securityLevelHTTP` | int | 0 | HTTP security level (0=HTTP, 1=HTTPS) | Determines whether HTTP or HTTPS is used for web connections |
| `server_certFile` | string | "" | Path to SSL certificate file | Required for HTTPS when security level is 1 |
| `server_keyFile` | string | "" | Path to SSL key file | Required for HTTPS when security level is 1 |

## Peer-to-Peer Topics

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `p2p_bestblock_topic` | string | "" | Topic name for best block announcements | Controls subscription and publication to the best block channel |
| `p2p_block_topic` | string | "" | Topic name for block announcements | Controls subscription and publication to the block channel |
| `p2p_subtree_topic` | string | "" | Topic name for subtree announcements | Controls subscription and publication to the subtree channel |
| `p2p_mining_on_topic` | string | "" | Topic name for mining status announcements | Controls subscription and publication to the mining status channel |
| `p2p_rejected_tx_topic` | string | "" | Topic name for rejected transaction announcements | Controls subscription to rejected transaction notifications |
| `p2p_handshake_topic` | string | "" | **REQUIRED** - Topic name for peer handshake messages | Used for version and verack exchanges between peers. During handshake, peers exchange their user agent (format: `teranode/bitcoin/{version}`), best block height, topic prefix for chain validation, and other connection metadata. Service will fail to start if not configured. |
| `p2p_handshake_topic_size` | int | 1 | Minimum topic size for handshake publishing | Controls reliability of handshake message delivery |
| `p2p_handshake_topic_timeout` | duration | 5s | Timeout for handshake topic operations | Maximum wait time for handshake topic readiness |
| `p2p_node_status_topic` | string | "" | Topic name for node status messages | Controls subscription and publication to the node status channel |

## Authentication and Security

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `p2p_peer_id` | string | "" | Unique identifier for the P2P node | Used to identify this node in the P2P network |
| `p2p_private_key` | string | "" | Private key for P2P authentication | Provides cryptographic identity for the node; if not provided, will be auto-generated and stored |
| `p2p_shared_key` | string | "" | Shared key for private network communication | When provided, ensures only nodes with the same shared key can communicate |
| `grpcAdminAPIKey` | string | "" | API key for gRPC admin operations | Required for administrative gRPC calls to the P2P service |

## NAT Traversal (Essential for Production)

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `p2p_enable_nat_service` | bool | true | Enable AutoNAT service for address discovery | **Essential for nodes behind NAT** - Helps detect public addresses and connectivity status |
| `p2p_enable_hole_punching` | bool | true | Enable NAT hole punching (DCUtR) | **Critical for peer-to-peer connectivity** - Allows direct connections through NATs |
| `p2p_enable_relay` | bool | true | Enable relay service (Circuit Relay v2) | **Fallback for difficult NAT scenarios** - Allows nodes to relay traffic when direct connection fails |
| `p2p_enable_nat_port_map` | bool | true | Enable NAT port mapping (UPnP/NAT-PMP) | **Automatic port forwarding** - Configures router port forwarding when supported |

## Connection Limits (Prevents Resource Exhaustion)

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `p2p_enable_conn_manager` | bool | true | Enable connection manager with limits | **Prevents resource exhaustion** - Controls connection limits and pruning |
| `p2p_conn_low_water` | int | 200 | Minimum connections to maintain | Lower bound for connection manager |
| `p2p_conn_high_water` | int | 400 | Maximum connections before pruning | Upper bound before connection manager starts pruning |

## Ban Management

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `p2p_ban_threshold` | int | 100 | Score threshold at which peers are banned | Controls how aggressively misbehaving peers are banned |
| `p2p_ban_duration` | duration | 24h | Duration for which peers remain banned | Controls how long banned peers are excluded from the network |

## Operation Mode Settings

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `listen_mode` | string | full | Operation mode for the P2P service (`full` or `listen_only`) | Controls whether the node participates fully in the network or only receives data without propagating. `listen_only` mode is useful for monitoring nodes that need to stay synchronized without contributing to network traffic. See [Using Listen Mode](../../../howto/miners/minersHowToUseListenMode.md) for detailed usage. |

## External Service Dependencies

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `chainCfgParams_topicPrefix` | string | "mainnet" | Chain identifier prefix for all topic names | **REQUIRED** - Provides network isolation by prefixing all pubsub topics (e.g., "mainnet-blocks", "testnet-blocks") |
| `asset_httpPublicAddress` | string | "" | Public HTTP address of Asset service | Used for constructing data hub URLs in peer messages |
| `asset_httpAddress` | string | "" | Internal HTTP address of Asset service | Fallback for data hub URL construction |
| `version` | string | "" | Service version | Included in node status messages for network compatibility |
| `commit` | string | "" | Git commit hash | Included in node status messages for version tracking |
| `coinbase_arbitraryText` | string | "" | Miner name from coinbase configuration | Used as miner identifier in node status messages |

## GRPC Client Configuration

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `grpcMaxRetries` | int | 3 | Maximum retry attempts for gRPC operations | Controls resilience of service-to-service communication |
| `grpcRetryBackoff` | duration | 1s | Backoff duration between gRPC retries | Controls retry timing for failed gRPC calls |

## Kafka Integration

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `kafka_invalidBlocksConfig` | URL | nil | Kafka URL for invalid blocks consumer | When set, enables consumption of invalid block notifications |
| `kafka_hosts` | []string | [] | Kafka broker hosts | Required for Kafka connectivity when invalid blocks consumer is enabled |
| `kafka_port` | int | 9092 | Kafka broker port | Default port for Kafka broker connections |
| `kafka_invalidBlocks` | string | "" | Kafka topic name for invalid blocks | Topic to consume invalid block notifications from |
| `kafka_partitions` | int | 1 | Number of Kafka partitions | Controls parallelism for Kafka message processing |

## Subtree Validation

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `subtreeValidation_blacklistedBaseURLs` | map[string]struct{} | {} | Map of blacklisted base URLs | Prevents processing of subtrees from blacklisted sources |

## Blockchain Store Integration

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockchain_storeURL` | URL | "" | Blockchain store URL | Required for ban list persistence and private key storage |

## Configuration Validation Rules

The P2P service enforces several validation rules during startup:

### Required Configuration

- `p2p_listen_addresses` - Must be non-empty array or service will fail with "p2p_listen_addresses not set in config"
- `p2p_port` - Must be > 0 or service will fail with "p2p_port not set in config"
- `p2p_shared_key` - Must be set or service will fail with "error getting p2p_shared_key"
- `p2p_block_topic` - Must be set or service will fail with "p2p_block_topic not set in config"
- `p2p_subtree_topic` - Must be set or service will fail with "p2p_subtree_topic not set in config"
- `p2p_handshake_topic` - Must be set or service will fail with "p2p_handshake_topic not set in config"
- `p2p_mining_on_topic` - Must be set or service will fail with "p2p_mining_on_topic not set in config"
- `p2p_rejected_tx_topic` - Must be set or service will fail with "p2p_rejected_tx_topic not set in config"
- `ChainCfgParams.TopicPrefix` - Must be set or service will fail with "missing config ChainCfgParams.TopicPrefix"
  - This chain identifier ensures network isolation between different chains (mainnet, testnet, etc.)
  - Peers validate topic prefix during handshake and reject connections from different chains

### Key Management

- If `p2p_private_key` is not provided, a new Ed25519 key is automatically generated and stored in `p2p.key` file
- The key file is created in the same directory as the peer cache (`p2p_peer_cache_dir` or binary directory)
- **Listen Mode Validation**: `listen_mode` must be either "full" or "listen_only" or service will fail with validation error

## Configuration Interactions and Dependencies

### Network Binding and Discovery

The P2P service's network presence is controlled by several interrelated settings:

- `p2p_listen_addresses` determines which interfaces/ports the service listens on
- `p2p_advertise_addresses` controls what addresses are advertised to peers
  - Each address can be specified with or without a port (e.g., `192.168.1.1` or `example.com:9906`)
  - For addresses without a port, the system automatically uses the value from `p2p_port`
  - Both IP addresses and domain names are supported with proper multiaddress formatting
- `p2p_port` provides a default when addresses don't specify ports
- If no `p2p_listen_addresses` are specified, the service may not be reachable
- The gRPC and HTTP listen addresses control how other services can interact with the P2P service

These settings should be configured together based on your network architecture and security requirements.

### Peer Discovery and Connection

Peer discovery in the P2P service uses a multi-layered approach:

- `p2p_bootstrapAddresses` provides initial entry points to the network
- `p2p_bootstrap_persistent` controls whether bootstrap addresses are automatically added to static peers
- `p2p_static_peers` ensures connections to specific peers regardless of DHT discovery
- `p2p_dht_protocol_id` and `p2p_dht_use_private` affect the DHT-based peer discovery mechanism
- `p2p_optimise_retries` impacts connection retry behavior when peers are unreachable
- `listen_mode` determines whether the node operates in "full" or "listen_only" mode

In private network deployments, you should configure static peers and bootstrap addresses carefully to ensure nodes can find each other.

#### Bootstrap vs Static Peers

By default, bootstrap addresses are used only for initial network discovery and DHT bootstrapping. If you need bootstrap servers to maintain persistent connections that automatically reconnect after disconnection, enable `p2p_bootstrap_persistent=true`. This will treat bootstrap addresses as static peers for connection purposes while still using them for DHT bootstrapping.

### Security and Authentication

The P2P service uses several security mechanisms:

- `p2p_private_key` establishes the node's identity, which is reflected in its `p2p_peer_id`
- `p2p_shared_key` enables private network functionality, restricting communication to nodes with the same shared key
- If no private key is provided, it will be auto-generated and stored in the blockchain store via `blockchain_storeURL`
- The ban system uses `p2p_ban_threshold` and `p2p_ban_duration` for score-based peer management
- `grpcAdminAPIKey` protects administrative gRPC operations
- HTTPS support via `securityLevelHTTP`, `server_certFile`, and `server_keyFile`

These settings enable secure and authenticated communication between nodes in the network.
