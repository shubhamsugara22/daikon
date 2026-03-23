# How the Daikon KV Store Works — Integration Guide

A simple guide to understanding the internals and integrating Daikon into your application.

---

## How it works

```
┌──────────────────────────────────────────────────┐
│                   Client App                     │
│        (any language — HTTP requests)            │
└──────────────┬───────────────────────────────────┘
               │  HTTP (JSON)
               ▼
┌──────────────────────────────────────────────────┐
│              Actix-Web Server                    │
│   ┌──────────┬──────────┬──────────┬──────────┐  │
│   │  Auth    │  CORS    │  Limits  │  Logging │  │
│   └──────────┴──────────┴──────────┴──────────┘  │
│                      │                           │
│                      ▼                           │
│   ┌──────────────────────────────────────────┐   │
│   │           API Handlers (api.rs)          │   │
│   └──────────────────┬───────────────────────┘   │
│                      │                           │
│          ┌───────────┼─────────────┐             │
│          ▼           ▼             ▼             │
│   ┌──────────┐ ┌──────────┐ ┌──────────────┐    │
│   │ KvStore  │ │   WAL    │ │    PITR      │    │
│   │(HashMap) │ │ (append  │ │ (snapshots)  │    │
│   │  + LRU   │ │  log)    │ │              │    │
│   └──────────┘ └──────────┘ └──────────────┘    │
└──────────────────────────────────────────────────┘
```

### Storage engine

The core is a `HashMap<String, Value>` protected by a read-write lock (`parking_lot::RwLock`). Reads happen concurrently; writes are serialized. Each key can hold a string, integer, float, boolean, JSON object, or HyperLogLog.

### Write path

1. Client sends a PUT/POST request
2. Auth middleware checks the API key (if configured)
3. The operation is first appended to the **WAL** (write-ahead log) on disk
4. The in-memory HashMap is updated
5. Response is returned to the client

### Read path

1. Client sends a GET request
2. A read lock is acquired (non-blocking for other reads)
3. If the key has a TTL and is expired, it returns "not found"
4. The value is returned as JSON

### Durability

- **WAL**: Every write is logged before it touches memory. On restart, the WAL is replayed to recover state.
- **Snapshots**: Periodic or on-demand JSON snapshots written to disk.
- **PITR**: Point-in-time recovery by combining snapshots + WAL replay up to a target timestamp.

---

## Integrating with your app

Daikon exposes a standard HTTP/JSON API. Any language with an HTTP client can use it.

### 1. Start the server

```bash
# Default: localhost:8080
cargo run --bin server

# With auth and custom port
KV_BIND=0.0.0.0:3000 KV_API_KEY=my-secret cargo run --bin server
```

Or with Docker:

```bash
docker run --rm -p 8080:8080 -e KV_BIND=0.0.0.0:8080 daikon-kv
```

### 2. Basic CRUD

**Set a key** (with optional TTL):

```bash
curl -X PUT http://localhost:8080/api/keys/user:42 \
  -H "Content-Type: application/json" \
  -d '{"value": "Alice", "ttl_secs": 3600}'
```

**Get a key**:

```bash
curl http://localhost:8080/api/keys/user:42
# → "Alice"
```

**Delete a key**:

```bash
curl -X DELETE http://localhost:8080/api/keys/user:42
```

**Check if a key exists**:

```bash
curl http://localhost:8080/api/exists/user:42
# → true / false
```

### 3. Batch operations

```bash
# Set multiple keys at once
curl -X POST http://localhost:8080/api/mset \
  -H "Content-Type: application/json" \
  -d '{"pairs": [{"key": "a", "value": "1"}, {"key": "b", "value": "2"}]}'

# Get multiple keys at once
curl -X POST http://localhost:8080/api/mget \
  -H "Content-Type: application/json" \
  -d '{"keys": ["a", "b"]}'
```

### 4. Atomic counters

```bash
curl -X PUT http://localhost:8080/api/keys/views \
  -H "Content-Type: application/json" \
  -d '{"value": "0"}'

curl -X POST http://localhost:8080/api/incr/views    # → 1
curl -X POST http://localhost:8080/api/incr/views    # → 2
curl -X POST http://localhost:8080/api/decr/views    # → 1

# Increment by a custom amount
curl -X POST http://localhost:8080/api/incrby/views \
  -H "Content-Type: application/json" \
  -d '{"amount": 10}'
```

### 5. Pattern search

```bash
# Find all keys matching a glob pattern
curl http://localhost:8080/api/keys/pattern/user:*
```

### 6. Pub/Sub messaging

```bash
# Subscribe to a channel (returns a subscriber_id)
curl -X POST http://localhost:8080/api/pubsub/subscribe/notifications

# Publish a message
curl -X POST http://localhost:8080/api/pubsub/publish/notifications \
  -H "Content-Type: application/json" \
  -d '{"message": "new order received"}'

# Poll for messages
curl http://localhost:8080/api/pubsub/messages/notifications/{subscriber_id}
```

### 7. Transactions

```bash
# Start a transaction
curl -X POST http://localhost:8080/api/multi

# Queue writes (they don't apply yet)
curl -X PUT http://localhost:8080/api/keys/balance \
  -H "Content-Type: application/json" \
  -d '{"value": "100"}'

# Execute all queued writes atomically
curl -X POST http://localhost:8080/api/exec

# Or discard them
curl -X POST http://localhost:8080/api/discard
```

### 8. Health and monitoring

```bash
curl http://localhost:8080/api/health/live     # liveness probe
curl http://localhost:8080/api/health/ready     # readiness probe
curl http://localhost:8080/api/stats            # key counts, hit rate
curl http://localhost:8080/api/metrics          # Prometheus format
curl http://localhost:8080/api/memory           # memory profile
```

---

## Integration examples by language

### Python

```python
import requests

BASE = "http://localhost:8080/api"
HEADERS = {"Content-Type": "application/json", "x-api-key": "my-secret"}

# Set
requests.put(f"{BASE}/keys/session:abc", json={"value": "user-data", "ttl_secs": 1800}, headers=HEADERS)

# Get
resp = requests.get(f"{BASE}/keys/session:abc")
print(resp.json())  # "user-data"

# Delete
requests.delete(f"{BASE}/keys/session:abc", headers=HEADERS)
```

### JavaScript / Node.js

```javascript
const BASE = "http://localhost:8080/api";
const headers = { "Content-Type": "application/json", "x-api-key": "my-secret" };

// Set
await fetch(`${BASE}/keys/config:theme`, {
  method: "PUT",
  headers,
  body: JSON.stringify({ value: "dark", ttl_secs: 86400 }),
});

// Get
const resp = await fetch(`${BASE}/keys/config:theme`);
const value = await resp.json();
console.log(value); // "dark"

// Batch get
const batch = await fetch(`${BASE}/mget`, {
  method: "POST",
  headers,
  body: JSON.stringify({ keys: ["config:theme", "config:lang"] }),
});
console.log(await batch.json());
```

### Go

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
    "io"
)

func main() {
    base := "http://localhost:8080/api"

    // Set
    body, _ := json.Marshal(map[string]interface{}{"value": "hello", "ttl_secs": 60})
    req, _ := http.NewRequest("PUT", base+"/keys/greeting", bytes.NewReader(body))
    req.Header.Set("Content-Type", "application/json")
    http.DefaultClient.Do(req)

    // Get
    resp, _ := http.Get(base + "/keys/greeting")
    data, _ := io.ReadAll(resp.Body)
    fmt.Println(string(data)) // "hello"
}
```

---

## Authentication

When `KV_API_KEY` is set, all write endpoints require the key via one of:

- Header: `x-api-key: <key>`
- Header: `Authorization: Bearer <key>`

Read endpoints (GET) are open. This lets you use Daikon as a shared cache where anyone can read but only authorized services can write.

---

## Common use cases

| Use case | How |
|---|---|
| **Session store** | SET with TTL → auto-expires stale sessions |
| **Rate limiter** | INCR a counter key per IP, check threshold |
| **Feature flags** | SET `flag:dark-mode` → `true`, GET from your app |
| **Job queue signals** | Pub/Sub channels for worker coordination |
| **Unique visitor count** | HyperLogLog (`pf-add` / `pf-count`) — low memory |
| **Distributed cache** | Master/replica replication for read scaling |
| **Atomic counters** | INCR/DECR for page views, API call counts |

---

## Full API reference

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/health/live` | Liveness probe |
| GET | `/api/health/ready` | Readiness probe |
| GET | `/api/metrics` | Prometheus metrics |
| GET | `/api/keys` | List all keys |
| GET | `/api/keys/{key}` | Get value |
| PUT | `/api/keys/{key}` | Set value (body: `{"value":"...", "ttl_secs": N}`) |
| DELETE | `/api/keys/{key}` | Delete key |
| POST | `/api/incr/{key}` | Increment by 1 |
| POST | `/api/decr/{key}` | Decrement by 1 |
| POST | `/api/incrby/{key}` | Increment by N (body: `{"amount": N}`) |
| POST | `/api/append/{key}` | Append to string value |
| POST | `/api/getset/{key}` | Set and return old value |
| POST | `/api/mget` | Batch get (body: `{"keys": [...]}`) |
| POST | `/api/mset` | Batch set (body: `{"pairs": [...]}`) |
| GET | `/api/exists/{key}` | Check key exists |
| GET | `/api/keys/pattern/{pat}` | Glob pattern search |
| GET | `/api/stats` | Store statistics |
| GET | `/api/memory` | Memory profile |
| POST | `/api/cleanup` | Remove expired keys |
| POST | `/api/multi` | Start transaction |
| POST | `/api/exec` | Execute transaction |
| POST | `/api/discard` | Discard transaction |
| POST | `/api/pitr/snapshot` | Create PITR snapshot |
| GET | `/api/pitr/snapshots` | List snapshots |
| POST | `/api/pitr/recover` | Recover to timestamp |
| POST | `/api/pitr/recover/latest` | Recover latest snapshot |
| POST | `/api/pubsub/subscribe/{ch}` | Subscribe to channel |
| POST | `/api/pubsub/unsubscribe/{ch}/{id}` | Unsubscribe |
| POST | `/api/pubsub/publish/{ch}` | Publish message |
| GET | `/api/pubsub/messages/{ch}/{id}` | Poll messages |
| GET | `/api/pubsub/channels` | List channels |
| POST | `/api/hll/{key}/add` | HyperLogLog add |
| GET | `/api/hll/{key}/count` | HyperLogLog count |
| POST | `/api/lua/exec` | Execute Lua script |
