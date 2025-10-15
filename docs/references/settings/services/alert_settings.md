# Alert Service Settings

**Related Topic**: [Alert Service](../../../topics/services/alert.md)

The Alert Service can be configured using various settings that control its behavior, storage options, and network connectivity. This section provides a comprehensive reference for all configuration options.

## Core Alert Service Configuration

- **Alert Store URL (`alert_store`)**: The URL for connecting to the alert system's data store.
  - Type: `string` (converted to `*url.URL` internally)
  - Impact: Determines the database backend and connection parameters
  - Example: `alert_store = sqlite:///alert` or `alert_store = postgres://user:pass@host:5432/database?sslmode=disable`

- **Genesis Keys (`alert_genesis_keys`)**: A pipe-separated list of public keys used for genesis alerts.
  - Type: `[]string`
  - Impact: **Critical** - The service will not start without valid genesis keys
  - Purpose: These keys determine which alerts are valid; only alerts signed by these keys will be processed
  - Example: `alert_genesis_keys = "02a1589f2c8e1a4e7cbf28d4d6b676aa2f30811277883211027950e82a83eb2768 | 03aec1d40f02ac7f6df701ef8f629515812f1bcd949b6aa6c7a8dd778b748b2433"`

- **P2P Private Key (`alert_p2p_private_key`)**: Private key for P2P communication.
  - **Type**: string
  - **Default**: "" (empty - triggers auto-generation)
  - **Environment Variable**: `TERANODE_alert_p2p_private_key`
  - **Impact**: Establishes node identity in the P2P network
  - **Auto-Generation**: If not provided, creates private key file at `$HOME/.alert-system/private_key.pem`
  - **Directory Creation**: Automatically creates `~/.alert-system/` directory with 0750 permissions
  - **Example**: `alert_p2p_private_key = "08c7fec91e75046d0ac6a2b4edb2daaae34b1e4c3c25a48b1ebdffe5955e33bc"`

- **Protocol ID (`alert_protocol_id`)**: Protocol identifier for the P2P alert network.
  - Type: `string`
  - Default: "/bitcoin/alert-system/1.0.0"
  - Impact: Determines which P2P protocol group the service will join
  - Example: `alert_protocol_id = "/bsv/alert/1.0.0"`

- **Topic Name (`alert_topic_name`)**: P2P topic name for alert propagation.
  - **Type**: string
  - **Default**: "bitcoin_alert_system"
  - **Environment Variable**: `TERANODE_alert_topic_name`
  - **Network Dependency**: Automatically prefixed with network name if not on mainnet
  - **Network-Specific Behavior**:

    - **Mainnet**: Uses configured topic name as-is
    - **Testnet**: Prefixed as "bitcoin_alert_system_testnet"
    - **Regtest**: Prefixed as "bitcoin_alert_system_regtest"
  - **Example**: `alert_topic_name = "bitcoin_alert_system"`

- **P2P Port (`alert_p2p_port`)**: Port number for P2P communication.
  - Type: `int`
  - Default: 9908
  - Impact: **Required** - Service will not start without a valid port
  - Example: `alert_p2p_port = 4001`

## Data Storage Configuration

The Alert Service supports multiple database backends through the `alert_store` URL:

### SQLite

```text
sqlite:///database_name
```

The SQLite database will be stored in the `DataFolder` directory specified in the main Teranode configuration.

- **Parameters**: None
- **Connection Pool Settings (hardcoded)**:

  - Max Idle Connections: 1
  - Max Open Connections: 1
- **Table Prefix**: Uses the database name as prefix

### In-Memory SQLite

```text
sqlitememory:///database_name
```

- **Parameters**: None
- **Connection Pool Settings (hardcoded)**:

  - Max Idle Connections: 1
  - Max Open Connections: 1
- **Storage**: In-memory only
- **Table Prefix**: Uses the database name as prefix

### PostgreSQL

```text
postgres://username:password@host:port/database?param1=value1&param2=value2
```

- **Parameters**:

  - `sslmode`: SSL mode for the connection (default: `disable`)
- **Connection Pool Settings (hardcoded)**:

  - Max Idle Connections: 2
  - Max Open Connections: 5
  - Max Connection Idle Time: 20 seconds
  - Max Connection Time: 20 seconds
  - Transaction Timeout: 20 seconds
- **Table Prefix**: "alert_system"

### MySQL

```text
mysql://username:password@host:port/database?param1=value1&param2=value2
```

- **Parameters**:

  - `sslmode`: SSL mode for the connection (default: `disable`)
- **Connection Pool Settings (hardcoded)**: Same as PostgreSQL
- **Table Prefix**: "alert_system"

## Internal Settings and Behaviors

The following settings are hardcoded in the service and cannot be configured externally:

**Core Processing Settings:**

- **Alert Processing Interval**: 5 minutes
  - Controls how frequently alerts are processed
- **Request Logging**: Enabled (true)
  - Controls HTTP request logging
- **Auto Migrate**: Enabled (true)
  - Automatically migrates the database schema on startup
- **Database Table Prefix**: "alert_system" for PostgreSQL/MySQL, database name for SQLite
  - Prefix used for database tables
- **DHT Mode**: "client"
  - Sets the node as a DHT client for peer discovery
- **Peer Discovery Interval**: Uses system default
  - Controls how frequently peers are discovered

## Service Dependencies

The Alert Service depends on several other Teranode services:

### Required Dependencies

| Dependency | Purpose | Configuration Impact | Failure Impact |
|------------|---------|---------------------|----------------|
| **Blockchain Client** | Block invalidation, chain state queries | Requires blockchain service to be properly configured and running | `InvalidateBlock` functionality unavailable |
| **UTXO Store** | UTXO freezing, unfreezing, reassignment operations | Requires UTXO store to be initialized and accessible | Core alert functionality (fund management) unavailable |
| **Block Assembly Client** | Mining coordination when alerts affect blocks | Requires block assembly service configuration | Mining-related alert functions unavailable |
| **Peer Client (Legacy)** | Legacy peer management (ban/unban operations) | Requires legacy peer service configuration | Legacy peer banning functionality unavailable |
| **P2P Client** | Modern P2P alert distribution and peer management | Requires P2P service configuration | Alert distribution and modern peer management unavailable |

### Configuration Interdependencies

1. **Network Configuration**: The Alert service inherits network configuration from global Teranode settings, affecting topic naming and protocol selection
2. **Logging Integration**: Uses the global Teranode logging configuration for consistent log formatting and output
3. **Metrics Integration**: Integrates with Teranode's Prometheus metrics system using global metrics configuration

## Environment Variables

All settings can also be configured through environment variables using the following pattern:

```text
TERANODE_ALERT_<SETTING_NAME>
```

### Standard Environment Variables

The following environment variables are used to configure the Alert Service:

- `TERANODE_ALERT_GENESIS_KEYS`: A pipe-separated list of genesis keys
- `TERANODE_ALERT_P2P_PRIVATE_KEY`: The P2P private key
- `TERANODE_ALERT_PROTOCOL_ID`: The P2P protocol identifier
- `TERANODE_ALERT_STORE`: The database connection URL
- `TERANODE_ALERT_TOPIC_NAME`: The P2P topic name

### Special Environment Variables

The following environment variable is used to configure the P2P port:

- `ALERT_P2P_PORT`: The P2P port (note: different naming pattern)

## Security Considerations

### Genesis Keys Management

The genesis keys are critical for the security of the alert system. They should be carefully managed:

- Store private keys securely and offline
- Use multiple keys with a threshold signature scheme for increased security
- Rotate keys periodically according to your security policy

### Database Security

When using PostgreSQL or MySQL:

- Use strong passwords
- Enable SSL for database connections (`sslmode=require` or stronger)
- Restrict database access to authorized users only
- Consider using a dedicated database user with limited permissions

## Configuration Examples

### Complete Configuration Example

```text
# Alert Service Core Configuration
alert_store = postgres://alert_user:secure_password@db-server:5432/alert_db?sslmode=require
alert_genesis_keys = "02a1589f2c8e1a4e7cbf28d4d6b676aa2f30811277883211027950e82a83eb2768 | 03aec1d40f02ac7f6df701ef8f629515812f1bcd949b6aa6c7a8dd778b748b2433"
alert_p2p_private_key = "08c7fec91e75046d0ac6a2b4edb2daaae34b1e4c3c25a48b1ebdffe5955e33bc"
alert_protocol_id = "/bsv/alert/1.0.0"
alert_topic_name = "bitcoin_alert_system"
alert_p2p_port = 4001
```

### Development Configuration Example

```text
alert_store = sqlite:///alert
alert_genesis_keys = "02a1589f2c8e1a4e7cbf28d4d6b676aa2f30811277883211027950e82a83eb2768"
alert_p2p_port = 4001
```

## Error Handling and Troubleshooting

### Common Configuration Errors

**Genesis Keys Errors:**

```text
Error: ErrNoGenesisKeys
Cause: No genesis keys provided in configuration
```

**P2P Configuration Errors:**

```text
Error: ErrNoP2PIP
Cause: P2P IP address validation failed (less than 5 characters)

Error: ErrNoP2PPort
Cause: P2P port validation failed (less than 2 characters when converted to string)
```

**Database Connection Errors:**

```text
Error: ErrDatastoreUnsupported
Cause: Unsupported database scheme in alert_store URL
Supported schemes: sqlite://, sqlitememory://, postgres://, mysql://
```
