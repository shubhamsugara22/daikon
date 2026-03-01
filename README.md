# Rust In-Memory Key-Value Store

A high-performance, feature-rich in-memory key-value store written in Rust with both CLI and REST API interfaces.

## 🚀 Features

### Core Operations
- ✅ **Multiple Data Types**: String, Integer, Float, Boolean, JSON
- ✅ **TTL Support**: Time-based key expiration
- ✅ **Persistence**: JSON-based storage with versioned backups
- ✅ **Atomic Operations**: INCR, DECR, APPEND, GETSET
- ✅ **Batch Operations**: MGET, MSET, EXISTS
- ✅ **Pattern Matching**: Glob-style key search (*, ?)
- ✅ **Statistics**: Hit/miss ratios and operation counters
- ✅ **Auto-cleanup**: Manual and automatic expired key removal

### Interfaces
- 🖥️ **CLI Tool**: Full-featured command-line interface
- 🌐 **REST API**: HTTP server with comprehensive endpoints
- 📊 **Monitoring**: Real-time statistics and metrics

## 📦 Installation

```bash
git clone <repository>
cd rust-kv-store
cargo build --release
```

## 🎯 Quick Start

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

## 📖 Documentation

For detailed feature documentation and examples, see [FEATURES.md](FEATURES.md).

## 🏗️ Architecture

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

## 🔧 Technical Details

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
- `Mutex`-protected shared state for API
- Thread-safe atomic operations
- Lock-free reads where possible

## 📊 Statistics Example

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

## 🧪 Testing

```bash
cargo test
```

## 🎓 Use Cases

- **Session Management**: TTL-based session storage
- **Caching Layer**: High-performance cache with hit rate tracking
- **Counter Service**: Atomic increment/decrement operations
- **Configuration Store**: Dynamic application configuration
- **Rate Limiting**: Token bucket implementation with INCR/DECR
- **Analytics**: Real-time metrics aggregation

## 🚀 Performance

- In-memory storage for sub-millisecond latency
- Zero-copy operations where possible
- Efficient glob pattern matching
- Batch operations to reduce overhead
- Stats tracking with minimal overhead

## 📚 Reference

Based on system design concepts from:
- [Building an In-Memory Key-Value Store](https://geekpaul.medium.com/system-design-building-an-in-memory-key-value-store-js-4d3aa9aec31c)
- [Design a Key-Value Store](https://bytebytego.com/courses/system-design-interview/design-a-key-value-store)

## 🤝 Contributing

This is a proof-of-concept project for learning system design and Rust. Feel free to fork and extend!

## 📄 License

MIT
