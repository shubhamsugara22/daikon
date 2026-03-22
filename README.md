# Rust In-Memory Key-Value Store

A high-performance, feature-rich in-memory key-value store written in Rust with both CLI and REST API interfaces.

## Production Readiness (Phase 1 ✅)

### Error Handling
- **Custom Error Types**: Typed error handling with `KvStoreError` enum
  - `KeyNotFound`: Operation on non-existent key
  - `TypeMismatch`: Operation type incompatible with stored value
  - `InvalidKey`: Key validation failures (empty or too large)
  - `InvalidValue`: Value validation failures (exceeds size limit)
  - `InvalidConfig`: Configuration validation failures
  - `MemoryExceeded`: Memory limit enforcement
  - `IoError`: File I/O operation failures
  - `JsonError`: Serialization/deserialization failures

### Input Validation
- **Key Validation**: Empty keys rejected, configurable size limits (default 1KB)
- **Value Validation**: Configurable size limits per data type (default 10MB)
- **Config Validation**: All configuration values validated on creation
- **Type Safety**: Strict type checking for atomic operations

### Memory Management
- **LRU Eviction**: Least-recently-used key eviction when memory limit exceeded
- **Memory Tracking**: Accurate byte counting for all values
- **Configurable Limits**: Default 1GB per-store memory limit
- **Live Stats**: Real-time memory usage and eviction tracking

### Structured Logging
- **Tracing Integration**: Using `tracing` and `tracing-subscriber`
- **Configurable Levels**: Set via `RUST_LOG` environment variable (defaults to "info")
- **Structured Events**: Tagged debug/info logs throughout core operations
- **Zero-cost in Release**: Enables efficient production debugging

### Graceful Shutdown
- **Signal Handling**: Responds to Ctrl-C, SIGTERM, and SIGINT
- **State Persistence**: Automatically saves store before shutdown
- **Clean Exit**: Graceful server termination with connection cleanup
- **Cross-Platform**: Windows (Ctrl-C/Ctrl-Break) and Unix (SIGTERM/SIGINT) support

### Comprehensive Testing
- **87 Total Tests**: 30 library tests (incl. WAL + PITR + replication + pubsub + hyperloglog tests) + 11 API endpoint tests + 46 integration tests
- **90 Total Tests**: 32 library tests (incl. WAL + PITR + replication + pubsub + hyperloglog + lua tests) + 12 API endpoint tests + 46 integration tests
- **100% Pass Rate**: All tests passing
- **Coverage**:
  - Input validation (empty keys, size limits)
  - Memory management (eviction, LRU ordering)
  - Type safety (type mismatch errors)
  - Atomic operations (incr/decr/append/incrby/getset)
  - Batch operations (mset/mget)
  - Transaction support (MULTI/EXEC/DISCARD with operation queueing)
  - API endpoints (REST/HTTP integration testing)
  - Stats tracking and accuracy
  - Persistence (save/load/versioning)
  - Pub/Sub messaging (subscribe/publish/poll)
  - HyperLogLog cardinality estimation (pfadd/pfcount/pfmerge)
  - HyperLogLog cardinality estimation (pfadd/pfcount/pfmerge)
  - Lua scripting (execute arbitrary scripts against the store)

## Features Status

### Core Operations
- ✅ **Multiple Data Types**: String, Integer, Float, Boolean, JSON
- ✅ **TTL Support**: Time-based key expiration
- ✅ **Persistence**: JSON-based storage with versioned backups
- ✅ **Atomic Operations**: INCR, DECR, APPEND, GETSET
- ✅ **Batch Operations**: MGET, MSET, EXISTS
- ✅ **Pattern Matching**: Glob-style key search (*, ?)
- ✅ **Statistics**: Hit/miss ratios and operation counters
- ✅ **Auto-cleanup**: Manual and automatic expired key removal
- ✅ **Production Ready**: Error handling, validation, logging, graceful shutdown

### Interfaces
- 🖥️ **CLI Tool**: Full-featured command-line interface
- 🌐 **REST API**: HTTP server with comprehensive endpoints
- 📊 **Monitoring**: Real-time statistics and metrics

## Installation

```bash
git clone <repository>
cd rust-kv-store
cargo build --release
```

## Quick Start

### CLI Usage

```bash
# Basic operations
cargo run -- set mykey myvalue
cargo run -- get mykey
cargo run -- delete mykey

# TTL support
cargo run -- set-ttl session "data" --ttl 3600

# Atomic operations
cargo run -- set counter 10
cargo run -- incr counter        # Returns: 11
cargo run -- incrby counter 5    # Returns: 16
cargo run -- append greeting "Hello"
cargo run -- append greeting " World"

# Batch operations
cargo run -- mset key1 val1 key2 val2 key3 val3
cargo run -- mget key1 key2 key3
cargo run -- exists key1 key2

# Pattern matching
cargo run -- keys "user:*"
cargo run -- keys "session:?"

# Statistics & maintenance
cargo run -- stats
cargo run -- cleanup
cargo run -- list
cargo run -- save --versions 3
```

```bash
# Lua scripting
cargo run -- lua --script "set('x', 'hello'); return get('x')"
cargo run -- lua --script "if exists('x') then return get('x') else return 'missing' end"
```

### REST API

Start the server:
```bash
cargo run --bin server
# Or set custom bind address
KV_BIND=0.0.0.0:3000 cargo run --bin server
```

Example API calls:
```bash
# Basic operations
curl -X PUT http://localhost:8080/api/keys/mykey \
  -H "Content-Type: application/json" \
  -d '{"value": "myvalue"}'

# Session-style key with TTL (seconds)
curl -X PUT http://localhost:8080/api/keys/session:abc123 \
  -H "Content-Type: application/json" \
  -d '{"value": "user-42", "ttl_secs": 3600}'

curl http://localhost:8080/api/keys/mykey

# Atomic operations
curl -X POST http://localhost:8080/api/incr/counter
curl -X POST http://localhost:8080/api/incrby/counter \
  -H "Content-Type: application/json" \
  -d '{"amount": 5}'

# Batch operations
curl -X POST http://localhost:8080/api/mget \
  -H "Content-Type: application/json" \
  -d '{"keys": ["key1", "key2", "key3"]}'

# Statistics
curl http://localhost:8080/api/stats
curl http://localhost:8080/api/metrics

# Health checks
curl http://localhost:8080/api/health/live
curl http://localhost:8080/api/health/ready

# Pattern matching
curl http://localhost:8080/api/keys/pattern/user:*
```

## Operational Controls

- `KV_API_KEY`: Optional API key for mutating HTTP endpoints. Send it as `x-api-key: ...` or `Authorization: Bearer ...`.
- `KV_ENABLE_LUA`: Set to `false` to disable Lua execution over HTTP.
- `KV_MAX_LUA_SCRIPT_BYTES`: Maximum Lua script payload size for `POST /api/lua/exec` (default `16384`).
- `KV_BIND`: HTTP bind address (default `127.0.0.1:8080`).

## Docker

```bash
docker build -t daikon-kv .

docker run --rm -p 8080:8080 \
  -e KV_BIND=0.0.0.0:8080 \
  -e KV_API_KEY=demo-secret \
  daikon-kv
```

Example authenticated write:

```bash
curl -X PUT http://localhost:8080/api/keys/demo \
  -H "x-api-key: demo-secret" \
  -H "Content-Type: application/json" \
  -d '{"value": "hello"}'
```

## Documentation

For detailed feature documentation and examples, see [FEATURES.md](FEATURES.md).

## Architecture

```
rust-kv-store/
├── src/
│   ├── kv_store.rs   # Core storage engine with stats tracking
│   ├── cli.rs        # CLI argument parsing and commands
│   ├── main.rs       # CLI application entry point
│   ├── api.rs        # REST API handlers
│   ├── server.rs     # HTTP server configuration
│   └── lib.rs        # Library exports
├── tests/
│   └── kv_store_test.rs  # Integration tests
├── FEATURES.md       # Detailed feature documentation
└── README.md         # This file
```

## Roadmap

### Phase 1: Production Hardening ✅ COMPLETE
- ✅ Custom error types and error handling
- ✅ Input validation (keys, values, config)
- ✅ Memory management with LRU eviction
- ✅ Structured logging with tracing
- ✅ Comprehensive test suite (28 tests total: 23 integration + 5 API tests, 100% passing)
- ✅ Graceful shutdown with persistence

### Phase 2: Performance & Scalability ✅ COMPLETE
- [x] **Concurrent Read Optimization**: API and server both use `parking_lot::RwLock`
- [x] **Batch Write Optimization**: MULTI/EXEC/DISCARD transaction flow for atomic queued writes
- [x] **Performance Benchmarks**: Throughput and latency metrics
- [x] **Memory Optimization**: Memory profiling endpoint and detailed usage breakdown by type
- [x] **Benchmarking CLI**: `benchmark` command to run performance suite
- [x] **Comprehensive Testing**: 5 new API integration tests for transaction endpoints

### Phase 3: Persistence & Durability (In Progress)
- [x] **Write-Ahead Logging**: Transaction log for crash recovery - **FULLY INTEGRATED**
- [x] **Point-in-Time Recovery**: Restore to specific timestamps - **IMPLEMENTED**
- [x] **Replication**: Master-replica data synchronization - **BASELINE IMPLEMENTED**
- [x] **Snapshot Management**: Configurable automatic snapshot intervals - **IMPLEMENTED**
- [x] **Compression**: Gzip/zstd compression for storage - **IMPLEMENTED**

### Phase 4: Advanced Features (In Progress)
- [x] **Pub/Sub Messaging**: Event subscription and publishing - **IMPLEMENTED**
- [ ] **Transactions**: MULTI/EXEC operations with rollback (partial - basic support exists)
- [ ] **Lua Scripting**: Custom script execution
- [x] **Lua Scripting**: Custom script execution - **IMPLEMENTED**
- [ ] **Stream Data Type**: Time-series data support
- [x] **HyperLogLog**: Cardinality estimation - **IMPLEMENTED**

## Technical Details

### Data Structure
- **Store**: `HashMap<String, ValueWithTTL>`
- **Values**: Type-safe enum (String, Int, Float, Bool, JSON)
- **Stats**: Real-time operation counters and hit/miss tracking

### Persistence
- JSON serialization with `serde_json`
- Optional compressed persistence via file extension: `.gz` (gzip) and `.zst` (zstd)
- Atomic writes (temp file + rename)
- Versioned backups with automatic pruning
- Configurable backup retention

**Compression examples:**

```bash
# CLI / local store files
# Save or load using gzip by choosing a .gz filename
cargo run -- --file store.json.gz save --versions 3
cargo run -- --file store.json.gz load

# Save or load using zstd by choosing a .zst filename
cargo run -- --file store.json.zst save --versions 3
cargo run -- --file store.json.zst load

# Server with automatic compressed snapshots
KV_STORE_PATH=server_store.json.zst \
KV_SNAPSHOTS_DIR=snapshots \
KV_SNAPSHOT_INTERVAL_SECS=300 \
KV_SNAPSHOT_COMPRESSION=gzip \
cargo run --bin server
```

#### Write-Ahead Logging (WAL)
- **Production Ready**: ✅ Fully integrated across all API write endpoints
- **Durability**: Every write operation is logged to disk before being applied to memory
- **Crash Recovery**: On startup, the WAL is replayed to restore all committed operations
- **Format**: JSON-encoded entries for easy inspection and recovery
- **Operations Logged**: SET, DELETE, INCR, DECR, INCRBY, APPEND, GETSET, MSET
- **Environment Configuration**: `KV_WAL_PATH` (default: `server.wal`)
- **API Coverage**: All 8 write endpoints log operations before execution

#### Point-in-Time Recovery (PITR)
- **Snapshots**: Create on-demand snapshots via `POST /api/pitr/snapshot`
- **Automatic Snapshots**: Periodic background snapshots via `KV_SNAPSHOT_INTERVAL_SECS`
- **Snapshot Compression**: `KV_SNAPSHOT_COMPRESSION=none|gzip|zstd` (defaults to `none`)
- **Recovery Targets**: Recover to a Unix timestamp via `POST /api/pitr/recover/{timestamp}`
- **Latest Recovery**: Restore latest snapshot via `POST /api/pitr/recover/latest`
- **Observability**: Recovery stats via `GET /api/pitr/stats`
- **Retention**: Snapshot cleanup via `POST /api/pitr/cleanup`
- **Environment Configuration**: `KV_SNAPSHOTS_DIR` (default: `snapshots`), `KV_SNAPSHOT_INTERVAL_SECS` (default: `0`, disabled)

#### Replication
- **Roles**: Master or replica via `KV_NODE_ROLE` (`master` default)
- **Master Feed**: Replicas pull WAL entries from `GET /api/replication/wal`
- **Heartbeat Tracking**: Replica heartbeat at `POST /api/replication/heartbeat`
- **Replica Sync API**: Manual sync `POST /api/replication/sync`, status `GET /api/replication/status`
- **Replica Health View**: Master lists replicas at `GET /api/replication/replicas`
- **Authentication**: Optional bearer-token auth via `KV_REPLICATION_SECRET`
- **Idempotency**: Replicas deduplicate resent entries using index tracking + timestamp guards
- **Environment Configuration**: `KV_MASTER_URL`, `KV_REPLICA_ID`, `KV_REPLICATION_POLL_INTERVAL`, `KV_REPLICATION_SECRET`

**Example `GET /api/replication/status` response:**

```json
{
  "replica_id": "replica-1",
  "master_url": "http://localhost:8080",
  "last_applied_index": 42,
  "lag_entries": 0,
  "last_successful_sync_unix_secs": 1741920000,
  "last_sync_duration_ms": 7
}
```

#### Pub/Sub Messaging
- **Channels**: Dynamic channels created on first subscription
- **Subscriptions**: Multiple subscribers per channel with unique subscriber IDs
- **Publishers**: Any client can publish to any channel
- **Message Queue**: Per-subscriber FIFO message queue (default 1000 messages)
- **Polling Model**: Subscribers poll for messages at their own pace
- **Channel Management**: Automatic cleanup of empty channels
- **API Endpoints**:
  - `POST /api/pubsub/subscribe/{channel}` → Returns subscriber ID
  - `POST /api/pubsub/unsubscribe/{channel}/{subscriber_id}` → Remove subscription
  - `POST /api/pubsub/publish/{channel}` → Broadcast message to all subscribers
  - `GET /api/pubsub/messages/{subscriber_id}` → Poll messages (default 10, configurable via `?limit=N`)
  - `GET /api/pubsub/channels` → List all active channels
  - `GET /api/pubsub/channels/{channel}/subscribers` → List subscribers for a channel

**Publishing and subscribing example:**

```bash
# Terminal 1: Subscribe to a channel
SUBSCRIBER=$(curl -s -X POST http://localhost:8080/api/pubsub/subscribe/alerts | jq -r '.subscriber_id')
echo "Subscriber ID: $SUBSCRIBER"

# Poll for messages (initially empty)
curl http://localhost:8080/api/pubsub/messages/$SUBSCRIBER?limit=5

# Terminal 2: Publish messages
curl -X POST http://localhost:8080/api/pubsub/publish/alerts \
  -H "Content-Type: application/json" \
  -d '{"message": "System alert!"}'

# Terminal 1: Poll again to receive messages
curl http://localhost:8080/api/pubsub/messages/$SUBSCRIBER?limit=5
# Response:
# {
#   "messages": [
#     {
#       "channel": "alerts",
#       "message": "System alert!",
#       "timestamp": 1741920000
#     }
#   ]
# }

# List all active channels
curl http://localhost:8080/api/pubsub/channels

# List subscribers on a channel
curl http://localhost:8080/api/pubsub/channels/alerts/subscribers
```

#### HyperLogLog
- **Purpose**: Approximate unique counting with fixed memory usage
- **Use Cases**: Unique visitors, distinct sessions, approximate daily active users
- **Storage**: Native `HyperLogLog` value type inside the key-value store
- **CLI Commands**:
  - `cargo run -- pf-add visitors user1 user2 user3`
  - `cargo run -- pf-count visitors`
  - `cargo run -- pf-merge all_visitors visitors_web visitors_mobile`
- **API Endpoints**:
  - `POST /api/hll/{key}/reserve` with `{ "precision": 12 }`
  - `POST /api/hll/{key}/add` with `{ "values": ["user1", "user2"] }`
  - `GET /api/hll/{key}/count`
  - `GET /api/hll/{key}/info`
  - `POST /api/hll/{destination}/merge` with `{ "sources": ["src1", "src2"] }`

**HyperLogLog examples:**

```bash
# CLI approximate unique counting
cargo run -- pf-add visitors user1 user2 user3 user2
cargo run -- pf-count visitors

# Merge two sketches into a destination key
cargo run -- pf-add visitors_web web_user_1 web_user_2
cargo run -- pf-add visitors_mobile mobile_user_1 web_user_2
cargo run -- pf-merge visitors_all visitors_web visitors_mobile
cargo run -- pf-count visitors_all

# REST API add values
curl -X POST http://localhost:8080/api/hll/visitors/reserve \
  -H "Content-Type: application/json" \
  -d '{"precision": 12}'

curl -X GET http://localhost:8080/api/hll/visitors/info

curl -X POST http://localhost:8080/api/hll/visitors/add \
  -H "Content-Type: application/json" \
  -d '{"values": ["user1", "user2", "user3"]}'

# REST API get approximate count
curl http://localhost:8080/api/hll/visitors/count

# REST API merge sketches
curl -X POST http://localhost:8080/api/hll/visitors_all/merge \
  -H "Content-Type: application/json" \
  -d '{"sources": ["visitors_web", "visitors_mobile"]}'
```

#### Lua Scripting
- **Purpose**: Run arbitrary Lua 5.4 scripts against the live store in a single atomic operation
- **Engine**: Embedded Lua 5.4 via [`mlua`](https://crates.io/crates/mlua) (vendored, no external Lua install required)
- **WAL Integration**: `set`, `delete`, and `incr` calls from Lua are logged to the WAL when run via the REST API
- **Safety Controls**: HTTP Lua execution can be disabled with `KV_ENABLE_LUA=false`, enforces request size via `KV_MAX_LUA_SCRIPT_BYTES`, and rejects execution while a transaction is open
- **Globals exposed to scripts**:
  - `get(key)` → returns the value as a string, or `nil` if not found
  - `set(key, value)` → stores a string value; returns `true`
  - `delete(key)` → removes a key; returns `true` if it existed
  - `incr(key)` → increments an integer key by 1; returns the new integer value (key must exist as an integer)
  - `exists(key)` → returns `true`/`false`
  - `print(msg)` → appends a line to the script output
- **Output**: Script `return` values and `print()` calls are concatenated and returned as a single string
- **CLI Command**: `cargo run -- lua --script "set('x', 'hello'); return get('x')"`
- **API Endpoint**: `POST /api/lua/exec` with `{ "script": "..." }`

**Lua scripting examples:**

```bash
# CLI: simple set + get
cargo run -- lua --script "set('name', 'daikon'); return get('name')"
# Output: daikon

# CLI: increment an integer key (must be pre-seeded as an integer)
cargo run -- set counter 0
cargo run -- lua --script "incr('counter'); incr('counter'); return get('counter')"
# Output: 2

# CLI: conditional logic
cargo run -- lua --script "
  if exists('x') then
    return 'found: ' .. get('x')
  else
    set('x', 'init')
    return 'created'
  end
"

# REST API: execute a Lua script
curl -X POST http://localhost:8080/api/lua/exec \
  -H "Content-Type: application/json" \
  -d '{"script": "set(\"greeting\", \"hello\"); return get(\"greeting\")"}'
# Response: {"output":"hello"}

# REST API: multi-step script with print output
curl -X POST http://localhost:8080/api/lua/exec \
  -H "Content-Type: application/json" \
  -d '{"script": "set(\"a\", \"1\"); set(\"b\", \"2\"); print(\"done\"); return exists(\"a\"), exists(\"b\")"}'
# Response: {"output":"done\ntrue true"}
```

### Concurrency
- `parking_lot::RwLock` for API shared state (read-heavy optimization)
- `Mutex` still used in server startup path (partial migration)
- Thread-safe atomic operations

## Statistics Example

```bash
$ cargo run -- stats
=== Store Statistics ===
Total keys: 150
Expired keys cleaned: 5
Total reads: 1000
Total writes: 200
Total deletes: 50
Cache hits: 950
Cache misses: 50
Hit rate: 95.00%
```

## Phase 1: Production Readiness ✅

The following production hardening features have been implemented and fully tested:

### Error Handling
- **Custom Error Types**: Comprehensive error categorization using `thiserror` crate
- **Error Types Included**:
  - `KeyNotFound` - Key doesn't exist in store
  - `TypeMismatch` - Operation incompatible with value type
  - `KeyTooLarge` - Key exceeds size limit (default 1KB)
  - `ValueTooLarge` - Value exceeds size limit (default 10MB)
  - `MemoryLimitExceeded` - Total memory would exceed limit (default 1GB)
  - `InvalidKey`, `InvalidValue` - Empty keys or invalid data
  - `IoError`, `SerializationError` - Persistence failures
  - `DataCorruption` - Corrupted store files detected
  - `ReadOnly`, `OperationFailed` - Operational errors

### Input Validation
- **Key Validation**: 
  - Reject empty keys
  - Enforce maximum key size (configurable, default 1KB)
- **Value Validation**:
  - Enforce maximum value size (configurable, default 10MB)
  - Type validation for atomic operations
- **Configuration Validation**:
  - Validate memory limits > 0
  - Validate key size > 0
  - Validate value size > 0

### Memory Management
- **LRU Eviction**: When memory exceeds limit, least recently used keys are evicted
- **Memory Tracking**: Accurate memory usage tracking with per-value size estimation
- **Configurable Limits**: Set max memory per store instance (default 1GB)
- **Eviction Counter**: Track number of evictions in statistics
- **Test Coverage**: 19 comprehensive tests covering eviction, LRU ordering, and size validation

### Structured Logging
- **Tracing Subscriber**: Initialized in both CLI and server binaries
- **Log Levels**: Configurable via `RUST_LOG` environment variable (default: "info")
- **Structured Events**: All operations logged with context (key names, operation types, etc.)
- **Performance**: Minimal overhead, can be disabled entirely via log level

### Graceful Shutdown
- **Signal Handling**: 
  - Unix: SIGTERM and SIGINT (Ctrl-C)
  - Windows: Ctrl-C and Ctrl-Break
- **Store Persistence**: Automatic save before shutdown
- **Configurable Path**: `KV_STORE_PATH` env var controls save location (default: `server_store.json`)
- **Clean Termination**: Server stops after completing graceful shutdown sequence

### Test Coverage
- **87 Total Tests**: 30 library tests + 11 API endpoint tests + 46 integration tests, all passing with 100% success rate
- **Test Categories**:
  - Input validation (empty keys, size limits)
  - Memory enforcement (eviction, LRU ordering)
  - Type safety (mismatch detection)
  - Atomic operations (incr/decr/append/getset)
  - Transaction operations (MULTI/EXEC/DISCARD)
  - API endpoints (REST interface testing)
  - Batch operations (mset/mget)
  - Configuration validation
  - Statistics tracking
  - Persistence (save/load with versioning)
  - Pub/Sub messaging (subscribe/publish/poll/list operations)
  - HyperLogLog operations (PFADD/PFCOUNT/PFMERGE)

**Status**: Phase 1 complete and production-ready! ✅

## Testing

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_memory_limit_enforcement

# Expected output includes all test groups (23 tests total)
```

## Use Cases

- **Session Management**: TTL-based session storage
- **Caching Layer**: High-performance cache with hit rate tracking
- **Counter Service**: Atomic increment/decrement operations
- **Configuration Store**: Dynamic application configuration
- **Rate Limiting**: Token bucket implementation with INCR/DECR
- **Analytics**: Real-time metrics aggregation

## Performance

- In-memory storage for sub-millisecond latency
- Zero-copy operations where possible
- Efficient glob pattern matching
- Batch operations to reduce overhead
- Stats tracking with minimal overhead

## Reference

Based on system design concepts from:
- [Building an In-Memory Key-Value Store](https://geekpaul.medium.com/system-design-building-an-in-memory-key-value-store-js-4d3aa9aec31c)
- [Design a Key-Value Store](https://bytebytego.com/courses/system-design-interview/design-a-key-value-store)

## Contributing

This is a proof-of-concept project for learning system design and Rust. Feel free to fork and extend!

## License

MIT
