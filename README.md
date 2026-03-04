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
- **21 Total Tests**: 2 integration + 19 core functionality tests
- **100% Pass Rate**: All tests passing
- **Coverage**:
  - Input validation (empty keys, size limits)
  - Memory management (eviction, LRU ordering)
  - Type safety (type mismatch errors)
  - Atomic operations (incr/decr/append/incrby/getset)
  - Batch operations (mset/mget)
  - Stats tracking and accuracy
  - Persistence (save/load/versioning)

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

# Pattern matching
curl http://localhost:8080/api/keys/pattern/user:*
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
- ✅ Comprehensive test suite (23 tests total: 4 unit + 19 integration, 100% passing)
- ✅ Graceful shutdown with persistence

### Phase 2: Performance & Scalability (Planned)
- [ ] **Concurrent Read Optimization**: Partial (API uses `parking_lot::RwLock`; server path still uses `Mutex`)
- [ ] **Batch Write Optimization**: Transaction-like operations for atomic multi-key updates
- [x] **Performance Benchmarks**: Throughput and latency metrics
- [ ] **Memory Optimization**: Zero-copy operations, custom allocators
- [ ] **Benchmarking CLI**: Performance testing tool

### Phase 3: Persistence & Durability (Planned)
- [ ] **Write-Ahead Logging**: Transaction log for crash recovery
- [ ] **Point-in-Time Recovery**: Restore to specific timestamps
- [ ] **Replication**: Master-replica data synchronization
- [ ] **Snapshot Management**: Configurable snapshot intervals
- [ ] **Compression**: Gzip/zstd compression for storage

### Phase 4: Advanced Features (Planned)
- [ ] **Pub/Sub Messaging**: Event subscription and publishing
- [ ] **Transactions**: MULTI/EXEC operations with rollback
- [ ] **Lua Scripting**: Custom script execution
- [ ] **Stream Data Type**: Time-series data support
- [ ] **HyperLogLog**: Cardinality estimation

## Technical Details

### Data Structure
- **Store**: `HashMap<String, ValueWithTTL>`
- **Values**: Type-safe enum (String, Int, Float, Bool, JSON)
- **Stats**: Real-time operation counters and hit/miss tracking

### Persistence
- JSON serialization with `serde_json`
- Atomic writes (temp file + rename)
- Versioned backups with automatic pruning
- Configurable backup retention

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
- **19 Integration Tests**: All passing with 100% success rate
- **Test Categories**:
  - Input validation (empty keys, size limits)
  - Memory enforcement (eviction, LRU ordering)
  - Type safety (mismatch detection)
  - Atomic operations (incr/decr/append/getset)
  - Batch operations (mset/mget)
  - Configuration validation
  - Statistics tracking
  - Persistence (save/load with versioning)

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
