# New Features Added

## ✅ Atomic Operations

### INCR/DECR

Increment or decrement integer values atomically.

**CLI:**

```bash
# Set an integer
cargo run -- set counter 10

# Increment by 1
cargo run -- incr counter  # Returns 11

# Decrement by 1
cargo run -- decr counter  # Returns 10
```

**API:**

```bash
# Increment
curl -X POST http://localhost:8080/api/incr/counter

# Decrement
curl -X POST http://localhost:8080/api/decr/counter
```

### INCRBY

Increment by a specific amount.

**CLI:**

```bash
cargo run -- incrby counter 5  # Adds 5 to counter
```

**API:**

```bash
curl -X POST http://localhost:8080/api/incrby/counter \
  -H "Content-Type: application/json" \
  -d '{"amount": 5}'
```

### APPEND

Append text to a string value.

**CLI:**

```bash
cargo run -- set message "Hello"
cargo run -- append message " World"  # Returns length: 11
```

**API:**

```bash
curl -X POST http://localhost:8080/api/append/message \
  -H "Content-Type: application/json" \
  -d '{"value": " World"}'
```

### GETSET

Get old value and set new value atomically.

**CLI:**

```bash
cargo run -- getset key "new_value"
# Returns old value and sets new value
```

**API:**

```bash
curl -X POST http://localhost:8080/api/getset/key \
  -H "Content-Type: application/json" \
  -d '{"value": "new_value"}'
```

## ✅ Batch Operations

### MGET

Get multiple values at once.

**CLI:**

```bash
cargo run -- mget key1 key2 key3
```

**API:**

```bash
curl -X POST http://localhost:8080/api/mget \
  -H "Content-Type: application/json" \
  -d '{"keys": ["key1", "key2", "key3"]}'
```

### MSET

Set multiple key-value pairs at once.

**CLI:**

```bash
cargo run -- mset key1 value1 key2 value2 key3 value3
```

**API:**

```bash
curl -X POST http://localhost:8080/api/mset \
  -H "Content-Type: application/json" \
  -d '{
    "pairs": [
      {"key": "key1", "value": "value1"},
      {"key": "key2", "value": "value2"}
    ]
  }'
```

### EXISTS

Check if keys exist.

**CLI:**

```bash
cargo run -- exists key1 key2 key3
# Returns: 2 key(s) exist
```

**API:**

```bash
curl http://localhost:8080/api/exists/key1
# Returns: true or false
```

## ✅ Pattern Matching

### KEYS

Find keys matching a glob pattern.

**CLI:**

```bash
# Find all keys starting with "user:"
cargo run -- keys "user:*"

# Find all keys ending with ":id"
cargo run -- keys "*:id"

# Match single character with ?
cargo run -- keys "key?"
```

**API:**

```bash
curl http://localhost:8080/api/keys/pattern/user:*
# Returns: ["user:1", "user:2", "user:3"]
```

**Pattern Syntax:**

- `*` - Match zero or more characters
- `?` - Match exactly one character
- `user:*` - Matches "user:1", "user:abc", "user:anything"
- `key?` - Matches "key1", "keyA", but not "key12"

## ✅ Statistics & Monitoring

### STATS

View store statistics including hit/miss ratios.

**CLI:**

```bash
cargo run -- stats
```

**Output:**

```text
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

**API:**

```bash
curl http://localhost:8080/api/stats
```

**Response:**

```json
{
  "total_keys": 150,
  "expired_keys": 5,
  "total_reads": 1000,
  "total_writes": 200,
  "total_deletes": 50,
  "hits": 950,
  "misses": 50,
  "hit_rate": 95.0
}
```

### CLEANUP

Manually clean up expired keys.

**CLI:**

```bash
cargo run -- cleanup
# Returns: Cleaned up 3 expired keys
```

**API:**

```bash
curl -X POST http://localhost:8080/api/cleanup
# Returns: number of keys removed
```

## Complete API Endpoints

| Method | Endpoint | Description |
| --- | --- | --- |
| GET | `/api/keys` | List all keys and values |
| GET | `/api/keys/{key}` | Get a value |
| PUT | `/api/keys/{key}` | Set a value |
| DELETE | `/api/keys/{key}` | Delete a key |
| POST | `/api/incr/{key}` | Increment integer |
| POST | `/api/decr/{key}` | Decrement integer |
| POST | `/api/incrby/{key}` | Increment by amount |
| POST | `/api/append/{key}` | Append to string |
| POST | `/api/getset/{key}` | Get and set atomically |
| POST | `/api/mget` | Get multiple values |
| POST | `/api/mset` | Set multiple values |
| GET | `/api/exists/{key}` | Check if key exists |
| GET | `/api/keys/pattern/{pattern}` | Find keys by pattern |
| GET | `/api/stats` | Get statistics |
| POST | `/api/cleanup` | Clean expired keys |

## Usage Examples

### E-commerce Cart System

```bash
# Initialize cart
cargo run -- set cart:user123 0

# Add items
cargo run -- incrby cart:user123 3  # Added 3 items
cargo run -- incr cart:user123      # Added 1 more

# Check cart
cargo run -- get cart:user123       # Returns: 4
```

### Session Management

```bash
# Create sessions
cargo run -- set-ttl session:abc123 "user_data" --ttl 3600
cargo run -- set-ttl session:def456 "user_data" --ttl 3600

# Find all sessions
cargo run -- keys "session:*"

# Cleanup expired sessions
cargo run -- cleanup
```

### Batch User Creation

```bash
# Create multiple users at once
cargo run -- mset \
  user:1:name "Alice" \
  user:1:email "alice@example.com" \
  user:2:name "Bob" \
  user:2:email "bob@example.com"

# Retrieve multiple values
cargo run -- mget user:1:name user:2:name
```

### Analytics Counter

```bash
# Track page views
cargo run -- set views:homepage 1000
cargo run -- incr views:homepage     # 1001
cargo run -- incrby views:homepage 50 # 1051

# View stats
cargo run -- stats
```

## Performance Improvements

- **Stats Tracking**: Automatically tracks reads, writes, deletes, hits, and misses
- **Hit/Miss Ratio**: Monitor cache efficiency with hit rate calculation
- **Batch Operations**: Reduce round trips with MGET/MSET
- **Atomic Operations**: Thread-safe increment/decrement without race conditions
- **Pattern Matching**: Efficient key discovery without scanning all keys
- **Manual Cleanup**: On-demand expired key removal

## What's Next?

Potential future enhancements:

- List, Set, and Hash data types
- Stream/time-series data type
- Background TTL cleanup task
