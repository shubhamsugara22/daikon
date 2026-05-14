# Daikon — Rust In-Memory Key-Value Store

An in-memory key-value store written in Rust with a CLI and an HTTP API (actix-web).

## Features

- **Data types** — String, Integer, Float, Boolean, JSON, List, Hash, HyperLogLog
- **TTL** — per-key expiration with manual and automatic cleanup
- **Persistence** — JSON snapshots, versioned backups, optional gzip/zstd compression
- **WAL** — write-ahead log for crash recovery, replayed on startup
- **PITR** — point-in-time recovery from periodic or on-demand snapshots
- **Replication** — master/replica with WAL-based sync
- **Pub/Sub** — channel-based publish/subscribe with per-subscriber queues
- **Lua scripting** — embedded Lua 5.4 (mlua) with store bindings
- **Transactions** — MULTI/EXEC/DISCARD with queued writes
- **LRU eviction** — configurable memory cap with least-recently-used eviction
- **Auth** — optional API-key gating on mutating endpoints
- **Keyspace notifications** — Redis-style event channels for key mutations and expirations
- **Observability** — Prometheus-style `/api/metrics`, health probes, access logging

## Quick start

```bash
git clone <repository>
cd rust-kv-store
cargo build --release
```

### CLI

```bash
cargo run -- set mykey myvalue
cargo run -- get mykey
cargo run -- delete mykey

# TTL (seconds)
cargo run -- set-ttl session "data" --ttl 3600

# Atomic operations
cargo run -- set counter 10
cargo run -- incr counter          # 11
cargo run -- incrby counter 5      # 16

# Batch
cargo run -- mset k1 v1 k2 v2
cargo run -- mget k1 k2

# Pattern search
cargo run -- keys "user:*"

# HyperLogLog
cargo run -- pf-add visitors u1 u2 u3
cargo run -- pf-count visitors

# Lua
cargo run -- lua --script "set('x','hello'); return get('x')"
cargo run -- lua --script "setex('session','token',300); return get('session')"
cargo run -- lua --script "setex('session','token',300); return ttl('session'), pttl('session')"

# Persistence
cargo run -- save --versions 3
cargo run -- stats
```

### HTTP server

```bash
cargo run --bin server                          # default 127.0.0.1:8080
KV_BIND=0.0.0.0:3000 cargo run --bin server    # custom bind
```

```bash
# Write (with optional TTL)
curl -X PUT http://localhost:8080/api/keys/mykey \
  -H "Content-Type: application/json" \
  -d '{"value": "myvalue", "ttl_secs": 3600}'

# Read
curl http://localhost:8080/api/keys/mykey

# TTL inspection (Redis-compatible: -2 = missing/expired, -1 = no TTL, >=0 = seconds remaining)
curl http://localhost:8080/api/ttl/mykey
curl http://localhost:8080/api/pttl/mykey          # milliseconds variant

# Set / update TTL on an existing key
curl -X PUT http://localhost:8080/api/expire/mykey \
  -H "Content-Type: application/json" \
  -d '{"ttl_secs": 3600}'

# Remove TTL (persist the key forever)
curl -X DELETE http://localhost:8080/api/expire/mykey

# Atomic increment
curl -X POST http://localhost:8080/api/incr/counter

# Batch get
curl -X POST http://localhost:8080/api/mget \
  -H "Content-Type: application/json" \
  -d '{"keys": ["k1", "k2"]}'

# Health & metrics
curl http://localhost:8080/api/health/live
curl http://localhost:8080/api/health/ready
curl http://localhost:8080/api/metrics

# Pattern search
curl http://localhost:8080/api/keys/pattern/user:*
```

For the full endpoint list and detailed examples, see [FEATURES.md](FEATURES.md).

## Configuration

All settings are via environment variables. Defaults are shown.

| Variable | Default | Description |
| --- | --- | --- |
| `KV_BIND` | `127.0.0.1:8080` | HTTP listen address |
| `KV_STORE_PATH` | `server_store.json` | Data file path (use `.gz`/`.zst` extension for compression) |
| `KV_WAL_PATH` | `server.wal` | Write-ahead log path |
| `KV_SNAPSHOTS_DIR` | `snapshots` | Snapshot directory |
| `KV_SNAPSHOT_INTERVAL_SECS` | `0` (disabled) | Auto-snapshot interval |
| `KV_TTL_CLEANUP_INTERVAL_SECS` | `0` (disabled) | Background expired-key cleanup interval |
| `KV_SNAPSHOT_COMPRESSION` | `none` | Snapshot compression: `none`, `gzip`, `zstd` |
| `KV_API_KEY` | _(none)_ | API key for mutating endpoints (`x-api-key` or `Bearer`) |
| `KV_ENABLE_LUA` | `true` | Enable/disable Lua over HTTP |
| `KV_MAX_LUA_SCRIPT_BYTES` | `16384` | Max Lua script payload size |
| `KV_MAX_PAYLOAD_BYTES` | `16777216` (16 MB) | Max request body size |
| `KV_WORKERS` | _cpu count_ | Actix worker threads |
| `KV_MAX_CONNECTIONS` | `25000` | Max concurrent connections |
| `KV_CORS_ORIGIN` | _(none)_ | Allowed CORS origin (omit for permissive) |
| `KV_NODE_ROLE` | `master` | `master` or `replica` |
| `KV_MASTER_URL` | _(none)_ | Master URL (replicas only) |
| `KV_REPLICA_ID` | auto | Replica identifier |
| `KV_REPLICATION_POLL_INTERVAL` | `5` | Sync poll interval (seconds) |
| `KV_REPLICATION_SECRET` | _(none)_ | Replication bearer token |
| `KV_KEYSPACE_NOTIFICATIONS` | `false` | Enable keyspace event notifications |
| `RUST_LOG` | `info` | Log level (tracing) |

## Docker

```bash
docker build -t daikon-kv .
docker run --rm -p 8080:8080 \
  -e KV_BIND=0.0.0.0:8080 \
  -e KV_API_KEY=changeme \
  daikon-kv
```

A production-ready `docker-compose.yml` is included with resource limits, read-only filesystem, and named volumes.

## Project layout

```text
src/
  kv_store.rs   Core storage engine (HashMap, LRU, stats)
  api.rs        HTTP handlers
  server.rs     Server startup, middleware, routes
  cli.rs        CLI argument parsing
  main.rs       CLI entry point
  lib.rs        Library exports
tests/
  kv_store_test.rs   Integration tests
benches/
  performance.rs     Benchmarks
```

## Docs

- [FEATURES.md](FEATURES.md) — full CLI/API reference with examples
- [ARCHITECTURE.md](ARCHITECTURE.md) — mermaid diagrams of system and data flow

## License

MIT

## Keyspace Notifications

Redis-style keyspace notifications publish events when keys are mutated or expire.

### Enable

```bash
KV_KEYSPACE_NOTIFICATIONS=true cargo run --bin server
```

To proactively remove expired keys in the background (instead of only on reads/manual cleanup), set:

```bash
KV_TTL_CLEANUP_INTERVAL_SECS=5 cargo run --bin server
```

Or at runtime:

```bash
curl -X PUT http://localhost:8080/api/keyspace/config \
  -H "Content-Type: application/json" \
  -d '{"enabled": true}'

curl http://localhost:8080/api/keyspace/config
```

### Channels

| Channel pattern | Message | Fired on |
| --- | --- | --- |
| `__keyevent__:set` | key name | `SET` / `SET` with TTL |
| `__keyevent__:del` | key name | `DELETE` |
| `__keyevent__:expired` | key name | TTL expiration cleanup |
| `__keyevent__:evicted` | key name | LRU eviction |
| `__keyspace__:{key}` | event kind (`set`, `del`, `expired`, `evicted`) | any mutation on that key |

Subscribe via Pub/Sub channels using the subscribe and poll endpoints:

```bash
# Subscribe to all set events
curl -X POST http://localhost:8080/api/pubsub/subscribe/__keyevent__:set \
  -H "Content-Type: application/json"
# Returns: subscriber_id

# Poll for messages
curl http://localhost:8080/api/pubsub/messages/{subscriber_id}
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
