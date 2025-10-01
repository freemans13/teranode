# RPC Settings

**Related Topic**: [RPC Service](../../../topics/services/rpc.md)

The RPC service relies on a set of configuration settings that control its behavior, security, and performance. This section provides a comprehensive overview of these settings, organized by functional category, along with their impacts, dependencies, and recommended configurations for different deployment scenarios.

## Configuration Categories

RPC service settings can be organized into the following functional categories:

1. **Authentication & Security**: Settings that control user access and authentication mechanisms
2. **Network Configuration**: Settings that determine how the service binds to network interfaces
3. **Performance & Scaling**: Settings that manage resource usage and connection handling
4. **Compatibility**: Settings that control backward compatibility with legacy clients

## Authentication & Security Settings

These settings control access to the RPC service, implementing a two-tier authentication system with full and limited access.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `rpc_user` | string | `""` (empty string) | Username for RPC authentication with full access | Controls who can access the RPC service with full permissions; required for secure deployments |
| `rpc_pass` | string | `""` (empty string) | Password for RPC authentication with full access | Controls authentication for full access; required for secure deployments |
| `rpc_limit_user` | string | `""` (empty string) | Username for RPC authentication with limited access | Controls who can access the RPC service with limited permissions; optional but recommended |
| `rpc_limit_pass` | string | `""` (empty string) | Password for RPC authentication with limited access | Controls authentication for limited access; optional but recommended |

### Authentication Interactions and Dependencies

The RPC service implements a two-tier authentication system:

- **Full Access**: Authenticated using `rpc_user` and `rpc_pass`, grants access to all RPC commands including sensitive operations like `stop` and administrative functions
- **Limited Access**: Authenticated using `rpc_limit_user` and `rpc_limit_pass`, grants access to a subset of commands, restricting sensitive operations

For security, credentials should be set explicitly as they default to empty strings.

## Network Configuration Settings

These settings control how the RPC service binds to network interfaces and accepts connections.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `rpc_listener_url` | *url.URL | `nil` | URL where the RPC service listens for connections, in format "<http://hostname:port>" | Controls which network interface and port the service binds to; critical for accessibility and security |

### Network Configuration Interactions and Dependencies

The `rpc_listener_url` setting determines both the binding interface and port. For example:

- `http://localhost:8332` binds to the local interface only, allowing connections only from the same machine
- `http://0.0.0.0:8332` binds to all interfaces, allowing remote connections if the machine is network-accessible

This setting has significant security implications. In production environments, the RPC service should typically only bind to local interfaces unless remote RPC access is explicitly required.

## Performance & Scaling Settings

These settings control resource usage, caching behavior, and the service's ability to handle multiple simultaneous clients.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `rpc_max_clients` | int | `1` | Maximum number of concurrent RPC client connections allowed | Limits resource usage; prevents overload from too many simultaneous connections |
| `rpc_cache_enabled` | bool | `true` | Enable response caching for frequently accessed data | Improves performance by caching responses for commands like getblock, getblockheader, and getrawtransaction |
| `rpc_timeout` | time.Duration | `30s` | Timeout for RPC request processing | Controls maximum time allowed for processing individual RPC requests; prevents hung requests |
| `rpc_client_call_timeout` | time.Duration | `5s` | Timeout for calls to dependent services (P2P, Peer services) | Controls timeout when RPC service calls other Teranode services for data |

### Performance Scaling Interactions and Dependencies

The performance settings work together to optimize RPC service behavior:

- **Connection Management**: `rpc_max_clients` directly impacts server resource usage. When the limit is reached, new connections are rejected with a 503 Service Unavailable response
- **Response Caching**: `rpc_cache_enabled` significantly improves performance for read-heavy workloads by caching responses for blockchain data queries
- **Timeout Control**: `rpc_timeout` and `rpc_client_call_timeout` prevent resource exhaustion from hung requests and service calls

These settings should be adjusted based on:

- Available server resources (CPU, memory, network capacity)
- Expected client load patterns and caching benefits
- Network latency to dependent services
- Complexity of RPC requests (some operations are more resource-intensive)

The default values are conservative to prevent resource exhaustion in default configurations.

### Timeout Configuration Guidelines

The timeout settings interact with service dependencies and operation complexity:

- **`rpc_timeout`** should be set higher than the longest expected operation:
  - Mining operations: 30-60s recommended
  - Large block retrievals: 20-30s recommended
  - Simple queries: 10-15s sufficient

- **`rpc_client_call_timeout`** should be short enough to fail fast:
  - Network operations: 5-10s recommended
  - Local service calls: 3-5s recommended
  - Critical paths: Keep as low as practical

Monitor timeout errors (code -30) to identify operations requiring adjustment.

## Compatibility Settings

These settings control the service's behavior when dealing with legacy Bitcoin clients.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `rpc_quirks` | bool | `true` | Enables backward compatibility quirks for legacy Bitcoin RPC clients | Affects how responses are formatted; enables legacy behavior for older clients |

### Compatibility Interactions and Dependencies

The `rpc_quirks` setting primarily affects response formatting and error handling behavior:

- When enabled, responses may include additional fields or formatting to maintain compatibility with older clients
- When disabled, responses strictly follow current Bitcoin RPC standards

This setting should be left enabled unless all clients are confirmed to support modern Bitcoin RPC conventions.

## Dependency-Injected Settings

These settings are not directly part of the RPC configuration but are required dependencies that the RPC service uses from other services.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `asset_httpAddress` | string | `""` (required) | HTTP address of the Asset service for transaction hex retrieval | **Required**: RPC service fails to start if not configured; used for getrawtransaction hex responses |

### Dependency Configuration Interactions

The RPC service acts as an API gateway and requires several service dependencies:

- **Asset Service Integration**: `asset_httpAddress` is mandatory for transaction data retrieval functionality. The RPC service constructs URLs like `{asset_httpAddress}/api/v1/tx/{txid}/hex` for transaction hex responses
- **Service Context**: The `Context` setting is used for HTTP listener setup and management
- **Client Dependencies**: The service requires active connections to Blockchain, UTXO Store, Block Assembly, Peer, and P2P services

## Configuration Relationships and Dependencies

The RPC service configuration settings have several important relationships:

### Authentication Flow

- `rpc_user` + `rpc_pass` → Full admin access (all RPC commands)
- `rpc_limit_user` + `rpc_limit_pass` → Limited access (read-only commands)
- Both credential pairs are SHA256 hashed for secure validation

### Performance Optimization

- `rpc_cache_enabled` + `rpc_timeout` → Balanced performance vs responsiveness
- `rpc_max_clients` + `rpc_client_call_timeout` → Resource management

### Service Integration

- `asset_httpAddress` → Required for transaction hex retrieval
- `rpc_listener_url` → Required for HTTP server binding

### Compatibility and Behavior

- `rpc_quirks` → Affects response formatting for legacy client compatibility
