use actix_web::{web, HttpResponse, Responder};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::kv_store::KvStore;
use crate::pitr::Pitr;
use crate::pubsub::{PubSub, PubSubMessage};
use crate::replication::{ReplicationMaster, ReplicationReplica};
use crate::wal::{Wal, WalEntry, WalOperation};

pub type WebKvStore = web::Data<RwLock<KvStore>>;
pub type WebWal = web::Data<Wal>;
pub type WebPitr = web::Data<Pitr>;
pub type WebReplicationMaster = web::Data<ReplicationMaster>;
pub type WebReplicationReplica = web::Data<ReplicationReplica>;
pub type WebPubSub = web::Data<PubSub>;

#[derive(Deserialize)]
pub struct SetRequest {
    value: String,
}

#[derive(Deserialize)]
pub struct IncrByRequest {
    amount: i64,
}

#[derive(Deserialize)]
pub struct AppendRequest {
    value: String,
}

#[derive(Deserialize)]
pub struct MGetRequest {
    keys: Vec<String>,
}

#[derive(Deserialize)]
pub struct MSetRequest {
    pairs: Vec<KeyValuePair>,
}

#[derive(Deserialize)]
pub struct KeyValuePair {
    key: String,
    value: String,
}

#[derive(Deserialize)]
pub struct CleanupSnapshotsRequest {
    max_age_secs: u64,
}

#[derive(Serialize)]
pub struct ListResponse {
    keys: Vec<String>,
    values: Vec<String>,
}

#[derive(Serialize)]
pub struct StatsResponse {
    total_keys: usize,
    expired_keys: usize,
    total_reads: u64,
    total_writes: u64,
    total_deletes: u64,
    hits: u64,
    misses: u64,
    hit_rate: f64,
}

#[derive(Deserialize)]
pub struct PublishRequest {
    pub message: String,
}

#[derive(Serialize)]
pub struct PublishResponse {
    pub channel: String,
    pub subscribers_count: usize,
}

#[derive(Serialize)]
pub struct SubscribeResponse {
    pub subscriber_id: String,
}

#[derive(Serialize)]
pub struct MessagesResponse {
    pub messages: Vec<PubSubMessage>,
}

#[derive(Serialize)]
pub struct ChannelsResponse {
    pub channels: Vec<String>,
}

#[derive(Serialize)]
pub struct SubscribersResponse {
    pub channel: String,
    pub subscribers: Vec<String>,
}

// GET /api/keys/{key}
pub async fn get_value(store: WebKvStore, key: web::Path<String>) -> impl Responder {
    let store = store.read(); // Read lock - allows concurrent reads
    match store.get(&key) {
        Some(value) => HttpResponse::Ok().json(value.to_string()),
        None => HttpResponse::NotFound().body(format!("Key '{}' not found", key)),
    }
}

// PUT /api/keys/{key}
pub async fn set_value(
    store: WebKvStore,
    wal: WebWal,
    key: web::Path<String>,
    req: web::Json<SetRequest>,
) -> impl Responder {
    // Log to WAL first (durability-first)
    let entry = WalEntry::new(WalOperation::Set {
        key: key.to_string(),
        value: req.value.clone(),
        ttl_secs: None,
    });
    if let Err(e) = wal.append(&entry) {
        return HttpResponse::InternalServerError()
            .body(format!("Failed to log operation to WAL: {}", e));
    }

    let mut store = store.write(); // Write lock - exclusive access for mutation
    match store.set(key.to_string(), req.value.clone()) {
        Ok(_) => HttpResponse::Ok().body(format!("Set '{}' successfully", key)),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// DELETE /api/keys/{key}
pub async fn delete_value(
    store: WebKvStore,
    wal: WebWal,
    key: web::Path<String>,
) -> impl Responder {
    // Log to WAL first
    let entry = WalEntry::new(WalOperation::Delete {
        key: key.to_string(),
    });
    if let Err(e) = wal.append(&entry) {
        return HttpResponse::InternalServerError()
            .body(format!("Failed to log operation to WAL: {}", e));
    }

    let mut store = store.write(); // Write lock - exclusive access for mutation
    match store.delete(&key) {
        Some(_) => HttpResponse::Ok().body(format!("Deleted '{}' successfully", key)),
        None => HttpResponse::NotFound().body(format!("Key '{}' not found", key)),
    }
}

// GET /api/keys
pub async fn list_keys(store: WebKvStore) -> impl Responder {
    let store = store.read(); // Read lock
    let mut keys = Vec::new();
    let mut values = Vec::new();

    for (k, v) in store.iter() {
        keys.push(k.clone());
        values.push(v.to_string());
    }

    HttpResponse::Ok().json(ListResponse { keys, values })
}

// POST /api/incr/{key}
pub async fn incr_value(store: WebKvStore, wal: WebWal, key: web::Path<String>) -> impl Responder {
    // Log to WAL first
    let entry = WalEntry::new(WalOperation::Incr {
        key: key.to_string(),
    });
    if let Err(e) = wal.append(&entry) {
        return HttpResponse::InternalServerError()
            .body(format!("Failed to log operation to WAL: {}", e));
    }

    let mut store = store.write(); // Write lock
    match store.incr(&key) {
        Ok(new_val) => HttpResponse::Ok().json(new_val),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// POST /api/decr/{key}
pub async fn decr_value(store: WebKvStore, wal: WebWal, key: web::Path<String>) -> impl Responder {
    // Log to WAL first
    let entry = WalEntry::new(WalOperation::Decr {
        key: key.to_string(),
    });
    if let Err(e) = wal.append(&entry) {
        return HttpResponse::InternalServerError()
            .body(format!("Failed to log operation to WAL: {}", e));
    }

    let mut store = store.write(); // Write lock
    match store.decr(&key) {
        Ok(new_val) => HttpResponse::Ok().json(new_val),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// POST /api/incrby/{key}
pub async fn incrby_value(
    store: WebKvStore,
    wal: WebWal,
    key: web::Path<String>,
    req: web::Json<IncrByRequest>,
) -> impl Responder {
    // Log to WAL first
    let entry = WalEntry::new(WalOperation::IncrBy {
        key: key.to_string(),
        amount: req.amount,
    });
    if let Err(e) = wal.append(&entry) {
        return HttpResponse::InternalServerError()
            .body(format!("Failed to log operation to WAL: {}", e));
    }

    let mut store = store.write(); // Write lock
    match store.incrby(&key, req.amount) {
        Ok(new_val) => HttpResponse::Ok().json(new_val),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// POST /api/append/{key}
pub async fn append_value(
    store: WebKvStore,
    wal: WebWal,
    key: web::Path<String>,
    req: web::Json<AppendRequest>,
) -> impl Responder {
    // Log to WAL first
    let entry = WalEntry::new(WalOperation::Append {
        key: key.to_string(),
        value: req.value.clone(),
    });
    if let Err(e) = wal.append(&entry) {
        return HttpResponse::InternalServerError()
            .body(format!("Failed to log operation to WAL: {}", e));
    }

    let mut store = store.write(); // Write lock
    match store.append(&key, &req.value) {
        Ok(len) => HttpResponse::Ok().json(len),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// POST /api/getset/{key}
pub async fn getset_value(
    store: WebKvStore,
    wal: WebWal,
    key: web::Path<String>,
    req: web::Json<SetRequest>,
) -> impl Responder {
    // Log to WAL first
    let entry = WalEntry::new(WalOperation::GetSet {
        key: key.to_string(),
        value: req.value.clone(),
    });
    if let Err(e) = wal.append(&entry) {
        return HttpResponse::InternalServerError()
            .body(format!("Failed to log operation to WAL: {}", e));
    }

    let mut store = store.write(); // Write lock
    match store.getset(key.to_string(), req.value.clone()) {
        Ok(Some(old_val)) => HttpResponse::Ok().json(old_val.to_string()),
        Ok(None) => HttpResponse::Ok().json(serde_json::Value::Null),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// POST /api/mget
pub async fn mget_values(store: WebKvStore, req: web::Json<MGetRequest>) -> impl Responder {
    let mut store = store.write(); // Write lock (tracks read stats)
    let values = store.mget(&req.keys);
    let result: Vec<Option<String>> = values
        .iter()
        .map(|v| v.as_ref().map(|val| val.to_string()))
        .collect();
    HttpResponse::Ok().json(result)
}

// POST /api/mset
pub async fn mset_values(
    store: WebKvStore,
    wal: WebWal,
    req: web::Json<MSetRequest>,
) -> impl Responder {
    // Log to WAL first
    let pairs_vec: Vec<(String, String)> = req
        .pairs
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();
    let entry = WalEntry::new(WalOperation::Mset {
        pairs: pairs_vec.clone(),
    });
    if let Err(e) = wal.append(&entry) {
        return HttpResponse::InternalServerError()
            .body(format!("Failed to log operation to WAL: {}", e));
    }

    let mut store = store.write(); // Write lock
    match store.mset(pairs_vec) {
        Ok(_) => HttpResponse::Ok().body("OK"),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// GET /api/exists/{key}
pub async fn exists_key(store: WebKvStore, key: web::Path<String>) -> impl Responder {
    let store = store.read(); // Read lock
    let exists = store.exists(&key);
    HttpResponse::Ok().json(exists)
}

// GET /api/keys/pattern/{pattern}
pub async fn keys_pattern(store: WebKvStore, pattern: web::Path<String>) -> impl Responder {
    let store = store.read(); // Read lock
    let keys = store.keys(&pattern);
    HttpResponse::Ok().json(keys)
}

// GET /api/stats
pub async fn get_stats(store: WebKvStore) -> impl Responder {
    let store = store.read(); // Read lock
    let stats = store.stats();
    let hit_rate = if stats.total_reads > 0 {
        (stats.hits as f64 / stats.total_reads as f64) * 100.0
    } else {
        0.0
    };

    HttpResponse::Ok().json(StatsResponse {
        total_keys: stats.total_keys,
        expired_keys: stats.expired_keys,
        total_reads: stats.total_reads,
        total_writes: stats.total_writes,
        total_deletes: stats.total_deletes,
        hits: stats.hits,
        misses: stats.misses,
        hit_rate,
    })
}

// POST /api/cleanup
pub async fn cleanup_expired(store: WebKvStore) -> impl Responder {
    let mut store = store.write(); // Write lock
    let removed = store.cleanup_expired();
    HttpResponse::Ok().json(removed)
}
// POST /api/multi
pub async fn multi(store: WebKvStore) -> impl Responder {
    let mut store = store.write(); // Write lock
    match store.multi() {
        Ok(_) => HttpResponse::Ok()
            .json(serde_json::json!({"status": "OK", "message": "Transaction started"})),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({"error": e.to_string()})),
    }
}

// POST /api/exec
pub async fn exec(store: WebKvStore) -> impl Responder {
    let mut store = store.write(); // Write lock
    match store.exec() {
        Ok(results) => HttpResponse::Ok().json(serde_json::json!({"results": results})),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({"error": e.to_string()})),
    }
}

// POST /api/discard
pub async fn discard(store: WebKvStore) -> impl Responder {
    let mut store = store.write(); // Write lock
    match store.discard() {
        Ok(_) => HttpResponse::Ok()
            .json(serde_json::json!({"status": "OK", "message": "Transaction discarded"})),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({"error": e.to_string()})),
    }
}

// GET /api/memory
pub async fn get_memory_profile(store: WebKvStore) -> impl Responder {
    let store = store.read(); // Read lock
    let profile = store.memory_profile();
    HttpResponse::Ok().json(profile)
}

// POST /api/pitr/snapshot
pub async fn pitr_create_snapshot(store: WebKvStore, pitr: WebPitr) -> impl Responder {
    let store = store.read();
    match pitr.create_snapshot(&store) {
        Ok(metadata) => HttpResponse::Ok().json(metadata),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

// GET /api/pitr/snapshots
pub async fn pitr_list_snapshots(pitr: WebPitr) -> impl Responder {
    match pitr.list_snapshots() {
        Ok(snapshots) => HttpResponse::Ok().json(snapshots),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

// POST /api/pitr/recover/{timestamp}
pub async fn pitr_recover_to_timestamp(
    store: WebKvStore,
    pitr: WebPitr,
    timestamp: web::Path<u64>,
) -> impl Responder {
    let target = timestamp.into_inner();
    match pitr.recover_to_timestamp(target) {
        Ok(recovered_store) => {
            let mut store_guard = store.write();
            *store_guard = recovered_store;
            HttpResponse::Ok().json(serde_json::json!({
                "status": "OK",
                "recovered_to": target
            }))
        }
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({
            "status": "ERROR",
            "error": e.to_string()
        })),
    }
}

// POST /api/pitr/recover/latest
pub async fn pitr_recover_latest_snapshot(store: WebKvStore, pitr: WebPitr) -> impl Responder {
    match pitr.recover_to_latest_snapshot() {
        Ok(recovered_store) => {
            let mut store_guard = store.write();
            *store_guard = recovered_store;
            HttpResponse::Ok().json(serde_json::json!({
                "status": "OK",
                "message": "Recovered to latest snapshot"
            }))
        }
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({
            "status": "ERROR",
            "error": e.to_string()
        })),
    }
}

// GET /api/pitr/stats
pub async fn pitr_stats(pitr: WebPitr) -> impl Responder {
    match pitr.get_recovery_stats() {
        Ok(stats) => HttpResponse::Ok().json(stats),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

// POST /api/pitr/cleanup
pub async fn pitr_cleanup_old_snapshots(
    pitr: WebPitr,
    req: web::Json<CleanupSnapshotsRequest>,
) -> impl Responder {
    match pitr.cleanup_old_snapshots(req.max_age_secs) {
        Ok(deleted) => HttpResponse::Ok().json(serde_json::json!({
            "status": "OK",
            "deleted_snapshots": deleted
        })),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

// ======================
// Replication Endpoints
// ======================

#[derive(Deserialize)]
pub struct HeartbeatRequest {
    pub replica_id: String,
    pub last_applied_index: u64,
}

#[derive(Deserialize)]
pub struct WalQueryParams {
    pub from_index: u64,
    pub limit: Option<usize>,
}

#[derive(Serialize)]
pub struct WalEntriesResponse {
    pub entries: Vec<WalEntry>,
    pub total_entries: u64,
}

// POST /api/replication/heartbeat
// Replica sends heartbeat to master and registers itself
pub async fn replication_heartbeat(
    master: WebReplicationMaster,
    http_req: actix_web::HttpRequest,
    req: web::Json<HeartbeatRequest>,
) -> impl Responder {
    let token = http_req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    if !master.verify_auth(token) {
        return HttpResponse::Unauthorized().json(serde_json::json!({
            "error": "Invalid or missing authentication token"
        }));
    }
    match master.register_replica(req.replica_id.clone(), req.last_applied_index) {
        Ok(_) => HttpResponse::Ok().json(serde_json::json!({
            "status": "OK",
            "message": "Heartbeat received"
        })),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

// GET /api/replication/wal?from_index=0&limit=100
// Master serves WAL entries to replicas
pub async fn replication_get_wal(
    master: WebReplicationMaster,
    http_req: actix_web::HttpRequest,
    query: web::Query<WalQueryParams>,
) -> impl Responder {
    let token = http_req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    if !master.verify_auth(token) {
        return HttpResponse::Unauthorized().json(serde_json::json!({
            "error": "Invalid or missing authentication token"
        }));
    }
    let limit = query.limit.unwrap_or(100);

    match master.get_wal_entries(query.from_index, limit) {
        Ok(entries) => match master.get_total_entries() {
            Ok(total) => HttpResponse::Ok().json(WalEntriesResponse {
                entries,
                total_entries: total,
            }),
            Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
        },
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

// GET /api/replication/replicas
// Get list of all connected replicas (master only)
pub async fn replication_list_replicas(master: WebReplicationMaster) -> impl Responder {
    let replicas = master.get_replicas_info();
    HttpResponse::Ok().json(serde_json::json!({
        "replicas": replicas,
        "count": replicas.len()
    }))
}

// POST /api/replication/sync
// Trigger manual sync on replica
pub async fn replication_sync(replica: WebReplicationReplica) -> impl Responder {
    match replica.sync() {
        Ok(applied_count) => HttpResponse::Ok().json(serde_json::json!({
            "status": "OK",
            "applied_entries": applied_count,
            "message": format!("Synced {} entries from master", applied_count)
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "status": "ERROR",
            "error": e.to_string()
        })),
    }
}

// GET /api/replication/status
// Get replica status
pub async fn replication_status(replica: WebReplicationReplica) -> impl Responder {
    let status = replica.get_status();
    HttpResponse::Ok().json(status)
}

// ============ Pub/Sub Endpoints ============

// POST /api/pubsub/subscribe/{channel}
// Subscribe to a channel
pub async fn pubsub_subscribe(
    pubsub: WebPubSub,
    channel: web::Path<String>,
) -> impl Responder {
    let subscriber_id = PubSub::new_subscriber_id();
    
    match pubsub.subscribe(channel.to_string(), subscriber_id.clone()) {
        Ok(_) => HttpResponse::Ok().json(SubscribeResponse { subscriber_id }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// POST /api/pubsub/unsubscribe/{channel}/{subscriber_id}
// Unsubscribe from a channel
pub async fn pubsub_unsubscribe(
    pubsub: WebPubSub,
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (channel, subscriber_id) = path.into_inner();
    
    match pubsub.unsubscribe(channel, subscriber_id) {
        Ok(_) => HttpResponse::Ok().json(serde_json::json!({
            "status": "unsubscribed"
        })),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// POST /api/pubsub/publish/{channel}
// Publish a message to a channel
pub async fn pubsub_publish(
    pubsub: WebPubSub,
    channel: web::Path<String>,
    req: web::Json<PublishRequest>,
) -> impl Responder {
    match pubsub.publish(channel.to_string(), req.message.clone()) {
        Ok(subscribers_count) => HttpResponse::Ok().json(PublishResponse {
            channel: channel.to_string(),
            subscribers_count,
        }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// GET /api/pubsub/messages/{subscriber_id}
// Poll messages for a subscriber
pub async fn pubsub_poll_messages(
    pubsub: WebPubSub,
    subscriber_id: web::Path<String>,
    query: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let limit = query
        .get("limit")
        .and_then(|l| l.parse::<usize>().ok())
        .unwrap_or(10);
    
    match pubsub.poll_messages(subscriber_id.to_string(), limit) {
        Ok(messages) => HttpResponse::Ok().json(MessagesResponse { messages }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// GET /api/pubsub/channels
// List all active channels
pub async fn pubsub_list_channels(pubsub: WebPubSub) -> impl Responder {
    match pubsub.list_channels() {
        Ok(channels) => HttpResponse::Ok().json(ChannelsResponse { channels }),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

// GET /api/pubsub/channels/{channel}/subscribers
// List subscribers for a specific channel
pub async fn pubsub_list_subscribers(
    pubsub: WebPubSub,
    channel: web::Path<String>,
) -> impl Responder {
    match pubsub.list_subscribers(channel.to_string()) {
        Ok(subscribers) => HttpResponse::Ok().json(SubscribersResponse {
            channel: channel.to_string(),
            subscribers,
        }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}
