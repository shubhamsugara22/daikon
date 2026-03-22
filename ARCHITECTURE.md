# Architecture Flow Diagrams

## Overall System Architecture

```mermaid
graph TB
    subgraph "Client Layer"
        CLI[CLI Interface<br/>clap commands]
        HTTP[HTTP Clients<br/>curl, browser, apps]
    end

    subgraph "Application Layer"
        MAIN[main.rs<br/>CLI Entry Point]
        SERVER[server.rs<br/>HTTP Server]
        CLICMD[cli.rs<br/>Command Parser]
        API[api.rs<br/>REST Handlers]
    end

    subgraph "Business Logic Layer"
        KVSTORE[kv_store.rs<br/>Core Engine]
        STATS[StoreStats<br/>Metrics Tracking]
        GLOB[Pattern Matcher<br/>Glob Algorithm]
    end

    subgraph "Data Layer"
        MEMORY[HashMap<br/>In-Memory Store]
        DISK[JSON Files<br/>store.json + backups]
    end

    CLI --> MAIN
    HTTP --> SERVER
    MAIN --> CLICMD
    CLICMD --> KVSTORE
    SERVER --> API
    API --> KVSTORE
    KVSTORE --> STATS
    KVSTORE --> GLOB
    KVSTORE --> MEMORY
    KVSTORE --> DISK

    style CLI fill:#e1f5ff
    style HTTP fill:#e1f5ff
    style KVSTORE fill:#fff3e0
    style MEMORY fill:#f3e5f5
    style DISK fill:#f3e5f5
```

## CLI Request Flow

```mermaid
sequenceDiagram
    actor User
    participant CLI as CLI Parser
    participant Main as main.rs
    participant Store as KvStore
    participant Stats as StoreStats
    participant Disk as File System

    User->>CLI: Execute command<br/>(e.g., incr counter)
    CLI->>Main: Parse arguments
    Main->>Disk: Load store.json (if exists)
    Disk-->>Main: Return stored data
    Main->>Store: Initialize with data
    
    Main->>Store: Execute operation<br/>(e.g., incr("counter"))
    Store->>Store: Validate key exists
    Store->>Store: Validate type is Int
    Store->>Store: Increment value
    Store->>Stats: Update stats<br/>(total_writes++)
    Store-->>Main: Return new value
    
    Main->>User: Display result<br/>("counter = 11")
    
    opt User saves
        User->>Main: Execute save command
        Main->>Store: save_with_version()
        Store->>Disk: Write JSON + backup
        Disk-->>Main: Success
        Main->>User: "Saved successfully"
    end
```

## API Request Flow

```mermaid
sequenceDiagram
    actor Client
    participant HTTP as HTTP Server
    participant API as API Handler
    participant RwLock as RwLock
    participant Store as KvStore
    participant Stats as StoreStats

    Client->>HTTP: HTTP Request<br/>POST /api/incr/counter
    HTTP->>API: Route to handler<br/>incr_value()
    API->>RwLock: Acquire write lock
    RwLock->>Store: Get mutable reference
    
    API->>Store: incr("counter")
    Store->>Store: Validate & increment
    Store->>Stats: Update metrics<br/>(writes++, total_ops++)
    Store-->>API: Return new value (11)
    
    API->>RwLock: Release lock
    API->>HTTP: JSON Response<br/>{"value": 11}
    HTTP->>Client: 200 OK
```

## Core Operations Flow

```mermaid
flowchart TD
    START([Operation Request]) --> TYPE{Operation<br/>Type?}
    
    TYPE -->|Read| GET[get method]
    TYPE -->|Write| SET[set/incr/append]
    TYPE -->|Delete| DEL[delete method]
    TYPE -->|Query| KEYS[keys/exists]
    
    GET --> CHECK_EXP{Check<br/>Expiration}
    CHECK_EXP -->|Expired| RETURN_NONE[Return None]
    CHECK_EXP -->|Valid| TRACK_HIT[Track Hit]
    TRACK_HIT --> STATS_READ[Stats: reads++, hits++]
    STATS_READ --> RETURN_VAL[Return Value]
    
    RETURN_NONE --> STATS_MISS[Stats: reads++, misses++]
    STATS_MISS --> END
    
    SET --> VALIDATE{Type<br/>Valid?}
    VALIDATE -->|No| ERROR[Return Error]
    VALIDATE -->|Yes| MUTATE[Mutate Value]
    MUTATE --> STATS_WRITE[Stats: writes++]
    STATS_WRITE --> RETURN_OK[Return Success]
    
    DEL --> REMOVE[Remove from HashMap]
    REMOVE --> EXISTS{Key<br/>Existed?}
    EXISTS -->|Yes| STATS_DEL[Stats: deletes++]
    EXISTS -->|No| RETURN_NONE2[Return None]
    STATS_DEL --> RETURN_OLD[Return Old Value]
    
    KEYS --> SCAN[Scan All Keys]
    SCAN --> FILTER[Filter by Pattern<br/>& Expiration]
    FILTER --> RETURN_LIST[Return Key List]
    
    ERROR --> END([End])
    RETURN_VAL --> END
    RETURN_OK --> END
    RETURN_OLD --> END
    RETURN_NONE2 --> END
    RETURN_LIST --> END

    style START fill:#81c784
    style END fill:#81c784
    style ERROR fill:#e57373
    style STATS_READ fill:#fff9c4
    style STATS_WRITE fill:#fff9c4
    style STATS_DEL fill:#fff9c4
    style STATS_MISS fill:#fff9c4
```

## Atomic Operation Flow (INCR/DECR)

```mermaid
flowchart TD
    START([INCR/DECR Request]) --> LOCK{Mutex<br/>Available?}
    LOCK -->|Wait| LOCK
    LOCK -->|Acquired| LOOKUP[Lookup Key in HashMap]
    
    LOOKUP --> EXISTS{Key<br/>Exists?}
    EXISTS -->|No| ERR1[Error: Key not found]
    EXISTS -->|Yes| CHECK_TYPE{Type is<br/>Int?}
    
    CHECK_TYPE -->|No| ERR2[Error: Not an integer]
    CHECK_TYPE -->|Yes| MODIFY[Modify value in-place<br/>*val += amount]
    
    MODIFY --> UPDATE_STATS[Stats: writes++]
    UPDATE_STATS --> RELEASE[Release Mutex]
    RELEASE --> RETURN_NEW[Return New Value]
    
    ERR1 --> RELEASE_ERR[Release Mutex]
    ERR2 --> RELEASE_ERR
    RELEASE_ERR --> RETURN_ERR[Return Error]
    
    RETURN_NEW --> END([End])
    RETURN_ERR --> END

    style START fill:#81c784
    style END fill:#81c784
    style ERR1 fill:#e57373
    style ERR2 fill:#e57373
    style MODIFY fill:#fff3e0
    style UPDATE_STATS fill:#fff9c4
```

## Batch Operations Flow (MGET/MSET)

```mermaid
flowchart TD
    START([MGET/MSET Request]) --> OPTYPE{Operation?}
    
    OPTYPE -->|MGET| MGET_START[Receive keys array]
    OPTYPE -->|MSET| MSET_START[Receive key-value pairs]
    
    MGET_START --> MGET_LOOP[For each key]
    MGET_LOOP --> MGET_GET[Call get key]
    MGET_GET --> MGET_COLLECT[Collect result]
    MGET_COLLECT --> MGET_MORE{More<br/>keys?}
    MGET_MORE -->|Yes| MGET_LOOP
    MGET_MORE -->|No| MGET_RETURN[Return array of values]
    MGET_RETURN --> END
    
    MSET_START --> MSET_LOOP[For each pair]
    MSET_LOOP --> MSET_SET[Call set key, value]
    MSET_SET --> MSET_STATS[Stats: writes++]
    MSET_STATS --> MSET_MORE{More<br/>pairs?}
    MSET_MORE -->|Yes| MSET_LOOP
    MSET_MORE -->|No| MSET_RETURN[Return success count]
    MSET_RETURN --> END
    
    END([End])

    style START fill:#81c784
    style END fill:#81c784
    style MGET_GET fill:#bbdefb
    style MSET_SET fill:#bbdefb
    style MGET_STATS fill:#fff9c4
    style MSET_STATS fill:#fff9c4
```

## Pattern Matching Algorithm

```mermaid
flowchart TD
    START([KEYS pattern]) --> INIT[Initialize result array]
    INIT --> SCAN[Iterate all keys]
    
    SCAN --> NEXT_KEY[Get next key]
    NEXT_KEY --> CHECK_EXP{Key<br/>expired?}
    CHECK_EXP -->|Yes| MORE1{More<br/>keys?}
    CHECK_EXP -->|No| MATCH[Run glob_match]
    
    MATCH --> PATTERN{Pattern<br/>matches?}
    PATTERN -->|No| MORE2{More<br/>keys?}
    PATTERN -->|Yes| ADD[Add to result]
    ADD --> MORE2
    
    MORE1 -->|Yes| NEXT_KEY
    MORE1 -->|No| RETURN
    MORE2 -->|Yes| NEXT_KEY
    MORE2 -->|No| RETURN[Return filtered keys]
    
    RETURN --> END([End])
    
    subgraph "Glob Match Algorithm"
        GLOB_START[Compare char by char] --> GLOB_CHAR{Current<br/>pattern char}
        GLOB_CHAR -->|'*'| GLOB_STAR[Try 0+ chars recursively]
        GLOB_CHAR -->|'?'| GLOB_QUEST[Match exactly 1 char]
        GLOB_CHAR -->|other| GLOB_EXACT[Match exact char]
        
        GLOB_STAR --> GLOB_END
        GLOB_QUEST --> GLOB_END
        GLOB_EXACT --> GLOB_END[Return match result]
    end

    style START fill:#81c784
    style END fill:#81c784
    style ADD fill:#c5e1a5
    style GLOB_STAR fill:#ffe082
    style GLOB_QUEST fill:#ffe082
    style GLOB_EXACT fill:#ffe082
```

## Persistence & Backup Flow

```mermaid
flowchart TD
    START([Save Command]) --> CHECK{File<br/>exists?}
    
    CHECK -->|No| WRITE_NEW[Create new file]
    CHECK -->|Yes| BACKUP[Create timestamped backup<br/>filename.bak.timestamp]
    
    BACKUP --> COUNT[Count existing backups]
    COUNT --> PRUNE{Backups ><br/>max_versions?}
    PRUNE -->|Yes| DELETE[Delete oldest backups]
    PRUNE -->|No| WRITE
    DELETE --> WRITE
    
    WRITE_NEW --> WRITE
    WRITE[Write to temp file<br/>filename.tmp]
    WRITE --> SERIALIZE[Serialize to JSON<br/>store + stats]
    SERIALIZE --> ATOMIC[Atomic rename<br/>tmp -> final]
    
    ATOMIC --> SUCCESS{Rename<br/>success?}
    SUCCESS -->|Yes| COMPLETE[Return Ok]
    SUCCESS -->|No| ERROR[Return Error]
    
    COMPLETE --> END([End])
    ERROR --> END

    style START fill:#81c784
    style END fill:#81c784
    style ERROR fill:#e57373
    style ATOMIC fill:#fff3e0
    style BACKUP fill:#b3e5fc
```

## Statistics Tracking Flow

```mermaid
flowchart LR
    subgraph "Read Operations"
        GET[get] --> CHECK{Found &<br/>not expired?}
        CHECK -->|Yes| HIT[hits++<br/>reads++]
        CHECK -->|No| MISS[misses++<br/>reads++]
    end
    
    subgraph "Write Operations"
        SET[set/incr/append] --> WRITE[writes++]
        SETTTL[set_with_ttl] --> WRITE
        MSET[mset] --> WRITE_N[writes += N]
    end
    
    subgraph "Delete Operations"
        DELETE[delete] --> DEL[deletes++]
        CLEANUP[cleanup_expired] --> EXPIRED[expired_keys += N]
    end
    
    subgraph "Stats Structure"
        HIT --> STATS[(StoreStats)]
        MISS --> STATS
        WRITE --> STATS
        WRITE_N --> STATS
        DEL --> STATS
        EXPIRED --> STATS
        
        STATS --> TOTAL[total_keys]
        STATS --> READS[total_reads]
        STATS --> WRITES[total_writes]
        STATS --> DELETES[total_deletes]
        STATS --> HITS[hits]
        STATS --> MISSES[misses]
        STATS --> EXP[expired_keys]
    end
    
    subgraph "Metrics"
        HITS --> CALC[Hit Rate =<br/>hits/reads * 100]
    end

    style STATS fill:#fff9c4
    style CALC fill:#a5d6a7
```

## Data Type Safety Flow

```mermaid
flowchart TD
    START([Value Operation]) --> TYPE{Value Type}
    
    TYPE -->|String| STR_OPS[Supported Ops:<br/>set, get, append]
    TYPE -->|Integer| INT_OPS[Supported Ops:<br/>set, get, incr, decr, incrby]
    TYPE -->|Float| FLOAT_OPS[Supported Ops:<br/>set, get]
    TYPE -->|Boolean| BOOL_OPS[Supported Ops:<br/>set, get]
    TYPE -->|JSON| JSON_OPS[Supported Ops:<br/>set, get]
    
    STR_OPS --> EXECUTE{Execute<br/>Operation}
    INT_OPS --> EXECUTE
    FLOAT_OPS --> EXECUTE
    BOOL_OPS --> EXECUTE
    JSON_OPS --> EXECUTE
    
    EXECUTE -->|Valid| SUCCESS[Return Result]
    EXECUTE -->|Invalid| TYPE_ERROR[Return Type Error<br/>'Key is not an integer']
    
    SUCCESS --> END([End])
    TYPE_ERROR --> END

    style START fill:#81c784
    style END fill:#81c784
    style TYPE_ERROR fill:#e57373
    style INT_OPS fill:#e1bee7
    style STR_OPS fill:#b2dfdb
    style FLOAT_OPS fill:#ffccbc
    style BOOL_OPS fill:#f8bbd0
    style JSON_OPS fill:#c5cae9
```

## Concurrency Model (API Server)

```mermaid
sequenceDiagram
    participant T1 as Thread 1<br/>(Request A)
    participant T2 as Thread 2<br/>(Request B)
    participant M as Mutex<KvStore>
    participant Store as KvStore
    
    Note over T1,T2: Concurrent requests arrive
    
    T1->>M: Attempt lock
    M-->>T1: Lock acquired ✓
    T2->>M: Attempt lock
    Note over T2: Waiting...
    
    T1->>Store: Execute operation
    Store->>Store: Update data
    Store->>Store: Update stats
    Store-->>T1: Return result
    T1->>M: Release lock
    
    M-->>T2: Lock acquired ✓
    T2->>Store: Execute operation
    Store->>Store: Update data
    Store->>Store: Update stats
    Store-->>T2: Return result
    T2->>M: Release lock
    
    Note over T1,T2: Both completed successfully
```

## Error Handling Flow

```mermaid
flowchart TD
    START([Operation Request]) --> VALIDATE
    
    VALIDATE{Validation}
    VALIDATE -->|Key not found| ERR1[Error: Key not found]
    VALIDATE -->|Wrong type| ERR2[Error: Type mismatch]
    VALIDATE -->|File I/O error| ERR3[Error: I/O operation failed]
    VALIDATE -->|Parse error| ERR4[Error: Invalid JSON]
    VALIDATE -->|Valid| EXECUTE[Execute Operation]
    
    EXECUTE --> UPDATE[Update Stats]
    UPDATE --> SUCCESS[Return Success]
    
    ERR1 --> LOG[Log Error]
    ERR2 --> LOG
    ERR3 --> LOG
    ERR4 --> LOG
    
    LOG --> RETURN_ERR[Return Error to Client]
    SUCCESS --> RETURN_OK[Return Result to Client]
    
    RETURN_ERR --> END([End])
    RETURN_OK --> END

    style START fill:#81c784
    style END fill:#81c784
    style ERR1 fill:#e57373
    style ERR2 fill:#e57373
    style ERR3 fill:#e57373
    style ERR4 fill:#e57373
    style SUCCESS fill:#a5d6a7
```

---

## How to Update These Diagrams

These diagrams use [Mermaid](https://mermaid.js.org/) syntax and can be:
1. **Viewed on GitHub** - Automatically rendered in markdown
2. **Edited in VS Code** - Install Mermaid extension
3. **Updated in any text editor** - Edit the mermaid code blocks
4. **Exported to images** - Use Mermaid CLI or online editor

### Quick Reference

- `graph TB` - Top to bottom flowchart
- `sequenceDiagram` - Sequence diagram
- `flowchart TD` - Detailed flowchart
- `-->` - Arrow connection
- `{Decision}` - Diamond shape for decisions
- `[Process]` - Rectangle for processes
- `([Start/End])` - Rounded rectangle
- `style NODE fill:#color` - Color nodes
