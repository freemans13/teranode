# Blob Store Settings

**Related Topic**: [Blob Store](../../../topics/stores/blob.md)

The Blob Server can be configured through URL-based settings and option parameters. This section provides a comprehensive reference for all configuration options.

## URL Format

Blob store configurations use URLs that follow this general structure:

```text
<scheme>://<path>[?<parameter1>=<value1>&<parameter2>=<value2>...]
```

Components:

- **Scheme**: Determines the storage backend type (file, memory, s3, http, null)
- **Path**: Specifies the storage location or identifier
- **Parameters**: Optional query parameters that modify behavior

## Store URL Settings

### Common URL Parameters

| Parameter | Type | Default | Description | Impact |
|-----------|------|---------|-------------|--------|
| `hashPrefix` | Integer | 2 | Number of characters from start of hash for directory organization | Improves file organization and lookup performance |
| `hashSuffix` | Integer | - | Number of characters from end of hash for directory organization | Alternative to hashPrefix; uses end of hash |
| `batch` | Boolean | false | Enables batch wrapper for improved performance | Aggregates operations into larger batches |
| `sizeInBytes` | Integer | 4194304 | Maximum batch size in bytes when batching enabled | Controls memory usage and batch efficiency |
| `writeKeys` | Boolean | false | Store key index alongside batch data | Enables key-based retrieval from batches |
| `localDAHStore` | String | "" | Enable Delete-At-Height functionality | Requires value like "memory://" to enable DAH |
| `localDAHStorePath` | String | "/tmp/localDAH" | Path for DAH metadata storage | Directory for blockchain height tracking |
| `logger` | Boolean | false | Enable debug logging wrapper | Adds detailed operation logging |

### File Backend Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| Path component | String | None (required) | Base directory for file storage |
| `checksum` | Boolean | false | Enable SHA256 checksumming of stored blobs |
| `header` | String | `(empty)` | Custom header prepended to stored blobs (hex or plain text) |
| `eofmarker` | String | `(empty)` | Custom footer appended to stored blobs (hex or plain text) |

### S3 Backend Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| Path component | String | None (required) | S3 bucket name |
| `region` | String | None (required) | AWS region for S3 bucket |
| `endpoint` | String | AWS S3 endpoint | Custom endpoint for S3-compatible storage |
| `forcePathStyle` | Boolean | false | Force path-style addressing |
| `subDirectory` | String | `(none)` | S3 object key prefix for organization |
| `MaxIdleConns` | Integer | 100 | Maximum number of idle HTTP connections |
| `MaxIdleConnsPerHost` | Integer | 100 | Maximum idle connections per host |
| `IdleConnTimeoutSeconds` | Integer | 100 | Idle connection timeout in seconds |
| `TimeoutSeconds` | Integer | 30 | Request timeout for S3 operations |
| `KeepAliveSeconds` | Integer | 300 | Connection keep-alive duration |

### HTTP Backend Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| Host+Path | String | None (required) | Remote HTTP blob server endpoint |
| Timeout | Duration | 30s | HTTP client timeout (hardcoded, not configurable via URL) |

## Store Options (options.Options)

Additional configuration is provided through Store Options when creating a store:

| Option | Type | Default | Description | Impact |
|--------|------|---------|-------------|--------|
| `BlockHeightRetention` | uint32 | 0 | Default block height retention for all files | Controls automatic deletion after specified blockchain height |
| `DAH` | uint32 | 0 | Default Delete-At-Height value for individual files | Sets blockchain height for automatic file deletion |
| `Filename` | string | `(hash-based)` | Custom filename override | Overrides default hash-based file naming |
| `SubDirectory` | string | `(none)` | Subdirectory within main path | Organizes data in storage hierarchy |
| `HashPrefix` | int | 2 | Number of hash characters for directory organization | Improves file organization and lookup performance |
| `AllowOverwrite` | bool | false | Allow overwriting existing files | Controls file replacement behavior |
| `SkipHeader` | bool | false | Skip file headers for CLI readability | Enables CLI-friendly file formats |
| `PersistSubDir` | string | `(none)` | Subdirectory for persistent storage | Directory for persistent data organization |
| `LongtermStoreURL` | *url.URL | nil | URL for longterm storage backend | Enables three-tier storage (memory, local, longterm) |
| `BlockHeightCh` | chan uint32 | nil | Block height tracking channel for DAH functionality | **Required for DAH-enabled stores** |

## File Operation Options (Functional Options)

These options can be specified per operation using functional option pattern:

| Option | Type | Description | Impact |
|--------|------|-------------|--------|
| `WithBlockHeightRetention(uint32)` | uint32 | Sets block height retention for this file | Controls per-file automatic deletion |
| `WithDAH(uint32)` | uint32 | Sets Delete-At-Height value for this file | Controls per-file data retention based on blockchain height |
| `WithFilename(string)` | string | Sets specific filename | Overrides default hash-based naming |
| `WithSubDirectory(string)` | string | Sets subdirectory for this file | Organizes individual files in storage hierarchy |
| `WithHashPrefix(int)` | int | Sets hash prefix for this file | Controls directory organization for this file |
| `WithAllowOverwrite(bool)` | bool | Controls overwriting of existing files | Prevents accidental data loss when false |
| `WithSkipHeader(bool)` | bool | Skip file headers for this operation | Enables CLI-friendly file formats |
| `WithPersistSubDir(string)` | string | Sets persistent subdirectory | Directory for persistent storage of this file |
| `WithLongtermStoreURL(*url.URL)` | *url.URL | Sets longterm storage URL for this file | Enables per-file longterm storage configuration |
| `WithBlockHeightCh(chan uint32)` | chan uint32 | Sets block height channel for DAH | **Required for DAH functionality** |
