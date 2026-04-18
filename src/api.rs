use actix_web::{http::header, web, HttpRequest, HttpResponse, Responder};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::kv_store::KvStore;
use crate::lua;
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

#[derive(Debug, Clone)]
pub struct ApiRuntimeConfig {
    pub api_key: Option<String>,
    pub lua_enabled: bool,
    pub max_lua_script_bytes: usize,
}

#[derive(Deserialize)]
pub struct SetRequest {
    value: String,
    ttl_secs: Option<u64>,
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

#[derive(Deserialize)]
pub struct ListPushRequest {
    pub values: Vec<String>,
}

#[derive(Deserialize)]
pub struct LRangeQuery {
    pub start: Option<i64>,
    pub stop: Option<i64>,
}

#[derive(Serialize)]
pub struct ListPushResponse {
    pub key: String,
    pub length: usize,
}

#[derive(Serialize)]
pub struct ListPopResponse {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Serialize)]
pub struct ListRangeResponse {
    pub key: String,
    pub values: Vec<String>,
}

#[derive(Serialize)]
pub struct ListLenResponse {
    pub key: String,
    pub length: usize,
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

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
    lua_enabled: bool,
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

// Pipeline types
const MAX_PIPELINE_COMMANDS: usize = 1000;

#[derive(Deserialize)]
#[serde(tag = "op")]
pub enum PipelineCommand {
    #[serde(rename = "GET")]
    Get { key: String },
    #[serde(rename = "SET")]
    Set {
        key: String,
        value: String,
        ttl_secs: Option<u64>,
    },
    #[serde(rename = "DELETE")]
    Delete { key: String },
    #[serde(rename = "INCR")]
    Incr { key: String },
    #[serde(rename = "DECR")]
    Decr { key: String },
    #[serde(rename = "INCRBY")]
    IncrBy { key: String, amount: i64 },
    #[serde(rename = "APPEND")]
    Append { key: String, value: String },
    #[serde(rename = "GETSET")]
    GetSet { key: String, value: String },
    #[serde(rename = "EXISTS")]
    Exists { key: String },
    #[serde(rename = "MGET")]
    MGet { keys: Vec<String> },
    #[serde(rename = "LPUSH")]
    LPush { key: String, values: Vec<String> },
    #[serde(rename = "RPUSH")]
    RPush { key: String, values: Vec<String> },
    #[serde(rename = "LPOP")]
    LPop { key: String },
    #[serde(rename = "RPOP")]
    RPop { key: String },
    #[serde(rename = "LRANGE")]
    LRange {
        key: String,
        start: Option<i64>,
        stop: Option<i64>,
    },
    #[serde(rename = "LLEN")]
    LLen { key: String },
}

#[derive(Deserialize)]
pub struct PipelineRequest {
    pub commands: Vec<PipelineCommand>,
}

#[derive(Serialize)]
pub struct PipelineResult {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct PipelineResponse {
    pub results: Vec<PipelineResult>,
}

fn require_api_key(
    req: &HttpRequest,
    runtime: Option<&web::Data<ApiRuntimeConfig>>,
) -> Option<HttpResponse> {
    let expected = runtime.and_then(|cfg| cfg.api_key.as_deref())?;

    let provided = req
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            req.headers()
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
        });

    match provided {
        Some(actual) if actual == expected => None,
        _ => Some(HttpResponse::Unauthorized().body("Missing or invalid API key")),
    }
}

/// Drain pending keyspace events from the store and publish them to PubSub channels.
/// Publishes to both `__keyevent__:{kind}` (message = key) and `__keyspace__:{key}` (message = kind).
fn publish_keyspace_events(store: &mut KvStore, pubsub: &PubSub) {
    let events = store.drain_keyspace_events();
    for event in events {
        let kind_str = event.kind.to_string();
        // __keyevent__:<event> channel — message is the key name
        let _ = pubsub.publish(format!("__keyevent__:{}", kind_str), event.key.clone());
        // __keyspace__:<key> channel — message is the event type
        let _ = pubsub.publish(format!("__keyspace__:{}", event.key), kind_str);
    }
}

pub async fn health_live(runtime: Option<web::Data<ApiRuntimeConfig>>) -> impl Responder {
    HttpResponse::Ok().json(HealthResponse {
        status: "ok",
        lua_enabled: runtime.as_ref().map(|cfg| cfg.lua_enabled).unwrap_or(true),
    })
}

pub async fn health_ready(runtime: Option<web::Data<ApiRuntimeConfig>>) -> impl Responder {
    HttpResponse::Ok().json(HealthResponse {
        status: "ready",
        lua_enabled: runtime.as_ref().map(|cfg| cfg.lua_enabled).unwrap_or(true),
    })
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
    http_req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    pubsub: WebPubSub,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    key: web::Path<String>,
    req: web::Json<SetRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }
    if matches!(req.ttl_secs, Some(0)) {
        return HttpResponse::BadRequest().body("ttl_secs must be greater than 0 when provided");
    }

    // Convert string to Value and serialize it for WAL
    let value: crate::kv_store::Value = req.value.clone().into();
    let value_json = match serde_json::to_string(&value) {
        Ok(json) => json,
        Err(e) => {
            return HttpResponse::InternalServerError()
                .body(format!("Failed to serialize value: {}", e))
        }
    };

    // Log to WAL first (durability-first)
    let entry = WalEntry::new(WalOperation::Set {
        key: key.to_string(),
        value: value_json,
        ttl_secs: req.ttl_secs,
    });
    if let Err(e) = wal.append(&entry) {
        return HttpResponse::InternalServerError()
            .body(format!("Failed to log operation to WAL: {}", e));
    }

    let mut store = store.write(); // Write lock - exclusive access for mutation
    let result = if let Some(ttl_secs) = req.ttl_secs {
        store.set_with_ttl(
            key.to_string(),
            req.value.clone(),
            std::time::Duration::from_secs(ttl_secs),
        )
    } else {
        store.set(key.to_string(), req.value.clone())
    };

    publish_keyspace_events(&mut store, &pubsub);

    match result {
        Ok(_) => HttpResponse::Ok().body(format!("Set '{}' successfully", key)),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// DELETE /api/keys/{key}
pub async fn delete_value(
    http_req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    pubsub: WebPubSub,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    key: web::Path<String>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

    // Log to WAL first
    let entry = WalEntry::new(WalOperation::Delete {
        key: key.to_string(),
    });
    if let Err(e) = wal.append(&entry) {
        return HttpResponse::InternalServerError()
            .body(format!("Failed to log operation to WAL: {}", e));
    }

    let mut store = store.write(); // Write lock - exclusive access for mutation
    let result = match store.delete(&key) {
        Some(_) => HttpResponse::Ok().body(format!("Deleted '{}' successfully", key)),
        None => HttpResponse::NotFound().body(format!("Key '{}' not found", key)),
    };
    publish_keyspace_events(&mut store, &pubsub);
    result
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
pub async fn incr_value(
    http_req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    key: web::Path<String>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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
pub async fn decr_value(
    http_req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    key: web::Path<String>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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
    http_req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    key: web::Path<String>,
    req: web::Json<IncrByRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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
    http_req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    key: web::Path<String>,
    req: web::Json<AppendRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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
    http_req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    key: web::Path<String>,
    req: web::Json<SetRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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
    http_req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    req: web::Json<MSetRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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
pub async fn cleanup_expired(
    http_req: HttpRequest,
    store: WebKvStore,
    pubsub: WebPubSub,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

    let mut store = store.write(); // Write lock
    let removed = store.cleanup_expired();
    publish_keyspace_events(&mut store, &pubsub);
    HttpResponse::Ok().json(removed)
}
// POST /api/multi
pub async fn multi(
    http_req: HttpRequest,
    store: WebKvStore,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

    let mut store = store.write(); // Write lock
    match store.multi() {
        Ok(_) => HttpResponse::Ok()
            .json(serde_json::json!({"status": "OK", "message": "Transaction started"})),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({"error": e.to_string()})),
    }
}

// POST /api/exec
pub async fn exec(
    http_req: HttpRequest,
    store: WebKvStore,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

    let mut store = store.write(); // Write lock
    match store.exec() {
        Ok(results) => HttpResponse::Ok().json(serde_json::json!({"results": results})),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({"error": e.to_string()})),
    }
}

// POST /api/discard
pub async fn discard(
    http_req: HttpRequest,
    store: WebKvStore,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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

pub async fn metrics(store: WebKvStore) -> impl Responder {
    let store = store.read();
    let stats = store.stats();
    let profile = store.memory_profile();
    let hit_rate = if stats.total_reads > 0 {
        stats.hits as f64 / stats.total_reads as f64
    } else {
        0.0
    };

    let body = format!(
        concat!(
            "kv_total_keys {}\n",
            "kv_total_reads {}\n",
            "kv_total_writes {}\n",
            "kv_total_deletes {}\n",
            "kv_cache_hits {}\n",
            "kv_cache_misses {}\n",
            "kv_hit_rate {}\n",
            "kv_memory_bytes {}\n",
            "kv_evictions {}\n"
        ),
        stats.total_keys,
        stats.total_reads,
        stats.total_writes,
        stats.total_deletes,
        stats.hits,
        stats.misses,
        hit_rate,
        profile.total_bytes,
        stats.evictions,
    );

    HttpResponse::Ok()
        .insert_header((
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        ))
        .body(body)
}

// POST /api/pitr/snapshot
pub async fn pitr_create_snapshot(
    http_req: HttpRequest,
    store: WebKvStore,
    pitr: WebPitr,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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
    http_req: HttpRequest,
    store: WebKvStore,
    pitr: WebPitr,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    timestamp: web::Path<u64>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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
pub async fn pitr_recover_latest_snapshot(
    http_req: HttpRequest,
    store: WebKvStore,
    pitr: WebPitr,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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
    http_req: HttpRequest,
    pitr: WebPitr,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    req: web::Json<CleanupSnapshotsRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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
    http_req: HttpRequest,
    pubsub: WebPubSub,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    channel: web::Path<String>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

    let subscriber_id = PubSub::new_subscriber_id();

    match pubsub.subscribe(channel.to_string(), subscriber_id.clone()) {
        Ok(_) => HttpResponse::Ok().json(SubscribeResponse { subscriber_id }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// POST /api/pubsub/unsubscribe/{channel}/{subscriber_id}
// Unsubscribe from a channel
pub async fn pubsub_unsubscribe(
    http_req: HttpRequest,
    pubsub: WebPubSub,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    path: web::Path<(String, String)>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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
    http_req: HttpRequest,
    pubsub: WebPubSub,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    channel: web::Path<String>,
    req: web::Json<PublishRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

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

// ── HyperLogLog ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct PfAddRequest {
    pub values: Vec<String>,
}

#[derive(Deserialize)]
pub struct PfMergeRequest {
    pub sources: Vec<String>,
}

#[derive(Deserialize)]
pub struct PfReserveRequest {
    pub precision: u8,
}

#[derive(Serialize)]
pub struct HllCountResponse {
    pub key: String,
    pub count: u64,
}

#[derive(Serialize)]
pub struct HllInfoResponse {
    pub key: String,
    pub precision: u8,
    pub registers: usize,
    pub memory_bytes: usize,
    pub estimated_count: u64,
}

#[derive(Deserialize)]
pub struct LuaExecRequest {
    pub script: String,
}

#[derive(Serialize)]
pub struct LuaExecResponse {
    pub output: String,
}

// POST /api/hll/{key}/add
// Add values to a HyperLogLog key; returns the new estimated cardinality.
pub async fn hll_pfadd(
    http_req: HttpRequest,
    store: WebKvStore,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    key: web::Path<String>,
    req: web::Json<PfAddRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

    let mut store = store.write();
    match store.pfadd(key.to_string(), req.values.clone()) {
        Ok(count) => HttpResponse::Ok().json(HllCountResponse {
            key: key.to_string(),
            count,
        }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

pub async fn hll_pfreserve(
    http_req: HttpRequest,
    store: WebKvStore,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    key: web::Path<String>,
    req: web::Json<PfReserveRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

    let mut store = store.write();
    match store.pfreserve(key.to_string(), req.precision) {
        Ok(_) => HttpResponse::Ok().json(serde_json::json!({
            "key": key.to_string(),
            "precision": req.precision,
            "status": "reserved"
        })),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// GET /api/hll/{key}/count
// Return the estimated cardinality of a HyperLogLog key.
pub async fn hll_pfcount(store: WebKvStore, key: web::Path<String>) -> impl Responder {
    let store = store.read();
    match store.pfcount(&key) {
        Ok(count) => HttpResponse::Ok().json(HllCountResponse {
            key: key.to_string(),
            count,
        }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

pub async fn hll_info(store: WebKvStore, key: web::Path<String>) -> impl Responder {
    let store = store.read();
    match store.hll_info(&key) {
        Ok(info) => HttpResponse::Ok().json(HllInfoResponse {
            key: key.to_string(),
            precision: info.precision,
            registers: info.registers,
            memory_bytes: info.memory_bytes,
            estimated_count: info.estimated_count,
        }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// POST /api/hll/{destination}/merge
// Merge one or more source HLL keys into the destination key.
pub async fn hll_pfmerge(
    http_req: HttpRequest,
    store: WebKvStore,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    destination: web::Path<String>,
    req: web::Json<PfMergeRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

    let mut store = store.write();
    match store.pfmerge(destination.to_string(), &req.sources) {
        Ok(count) => HttpResponse::Ok().json(HllCountResponse {
            key: destination.to_string(),
            count,
        }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// POST /api/lua/exec
// Execute a Lua script against the store with built-in helpers.
pub async fn lua_exec(
    http_req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    req: web::Json<LuaExecRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

    if let Some(cfg) = runtime.as_ref() {
        if !cfg.lua_enabled {
            return HttpResponse::Forbidden().body("Lua execution is disabled");
        }
        if req.script.len() > cfg.max_lua_script_bytes {
            return HttpResponse::PayloadTooLarge().body(format!(
                "Lua script exceeds max size of {} bytes",
                cfg.max_lua_script_bytes
            ));
        }
    }

    let mut store = store.write();
    if store.in_transaction() {
        return HttpResponse::Conflict()
            .body("Cannot execute Lua while a transaction is in progress");
    }

    match lua::execute_script(&mut store, Some(&wal), &req.script) {
        Ok(output) => HttpResponse::Ok().json(LuaExecResponse { output }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

// POST /api/pipeline
pub async fn pipeline_exec(
    http_req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    pubsub: WebPubSub,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    req: web::Json<PipelineRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

    if req.commands.is_empty() {
        return HttpResponse::BadRequest().body("Pipeline must contain at least one command");
    }

    if req.commands.len() > MAX_PIPELINE_COMMANDS {
        return HttpResponse::PayloadTooLarge().body(format!(
            "Pipeline exceeds maximum of {} commands",
            MAX_PIPELINE_COMMANDS
        ));
    }

    let mut store = store.write();
    let mut results = Vec::with_capacity(req.commands.len());

    for cmd in &req.commands {
        let result = match cmd {
            PipelineCommand::Get { key } => match store.get(key) {
                Some(value) => PipelineResult {
                    status: "ok".into(),
                    value: Some(serde_json::Value::String(value.to_string())),
                    error: None,
                },
                None => PipelineResult {
                    status: "ok".into(),
                    value: None,
                    error: None,
                },
            },
            PipelineCommand::Set {
                key,
                value,
                ttl_secs,
            } => {
                let entry = WalEntry::new(WalOperation::Set {
                    key: key.clone(),
                    value: value.clone(),
                    ttl_secs: *ttl_secs,
                });
                if let Err(e) = wal.append(&entry) {
                    PipelineResult {
                        status: "error".into(),
                        value: None,
                        error: Some(format!("WAL error: {}", e)),
                    }
                } else if let Some(ttl) = ttl_secs {
                    match store.set_with_ttl(key.clone(), value.clone(), Duration::from_secs(*ttl))
                    {
                        Ok(_) => PipelineResult {
                            status: "ok".into(),
                            value: None,
                            error: None,
                        },
                        Err(e) => PipelineResult {
                            status: "error".into(),
                            value: None,
                            error: Some(e.to_string()),
                        },
                    }
                } else {
                    match store.set(key.clone(), value.clone()) {
                        Ok(_) => PipelineResult {
                            status: "ok".into(),
                            value: None,
                            error: None,
                        },
                        Err(e) => PipelineResult {
                            status: "error".into(),
                            value: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
            }
            PipelineCommand::Delete { key } => {
                let entry = WalEntry::new(WalOperation::Delete { key: key.clone() });
                if let Err(e) = wal.append(&entry) {
                    PipelineResult {
                        status: "error".into(),
                        value: None,
                        error: Some(format!("WAL error: {}", e)),
                    }
                } else {
                    match store.delete(key) {
                        Some(_) => PipelineResult {
                            status: "ok".into(),
                            value: Some(serde_json::Value::Bool(true)),
                            error: None,
                        },
                        None => PipelineResult {
                            status: "ok".into(),
                            value: Some(serde_json::Value::Bool(false)),
                            error: None,
                        },
                    }
                }
            }
            PipelineCommand::Incr { key } => {
                let entry = WalEntry::new(WalOperation::Incr { key: key.clone() });
                if let Err(e) = wal.append(&entry) {
                    PipelineResult {
                        status: "error".into(),
                        value: None,
                        error: Some(format!("WAL error: {}", e)),
                    }
                } else {
                    match store.incr(key) {
                        Ok(val) => PipelineResult {
                            status: "ok".into(),
                            value: Some(serde_json::json!(val)),
                            error: None,
                        },
                        Err(e) => PipelineResult {
                            status: "error".into(),
                            value: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
            }
            PipelineCommand::Decr { key } => {
                let entry = WalEntry::new(WalOperation::Decr { key: key.clone() });
                if let Err(e) = wal.append(&entry) {
                    PipelineResult {
                        status: "error".into(),
                        value: None,
                        error: Some(format!("WAL error: {}", e)),
                    }
                } else {
                    match store.decr(key) {
                        Ok(val) => PipelineResult {
                            status: "ok".into(),
                            value: Some(serde_json::json!(val)),
                            error: None,
                        },
                        Err(e) => PipelineResult {
                            status: "error".into(),
                            value: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
            }
            PipelineCommand::IncrBy { key, amount } => {
                let entry = WalEntry::new(WalOperation::IncrBy {
                    key: key.clone(),
                    amount: *amount,
                });
                if let Err(e) = wal.append(&entry) {
                    PipelineResult {
                        status: "error".into(),
                        value: None,
                        error: Some(format!("WAL error: {}", e)),
                    }
                } else {
                    match store.incrby(key, *amount) {
                        Ok(val) => PipelineResult {
                            status: "ok".into(),
                            value: Some(serde_json::json!(val)),
                            error: None,
                        },
                        Err(e) => PipelineResult {
                            status: "error".into(),
                            value: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
            }
            PipelineCommand::Append { key, value } => {
                let entry = WalEntry::new(WalOperation::Append {
                    key: key.clone(),
                    value: value.clone(),
                });
                if let Err(e) = wal.append(&entry) {
                    PipelineResult {
                        status: "error".into(),
                        value: None,
                        error: Some(format!("WAL error: {}", e)),
                    }
                } else {
                    match store.append(key, value) {
                        Ok(len) => PipelineResult {
                            status: "ok".into(),
                            value: Some(serde_json::json!(len)),
                            error: None,
                        },
                        Err(e) => PipelineResult {
                            status: "error".into(),
                            value: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
            }
            PipelineCommand::GetSet { key, value } => {
                let entry = WalEntry::new(WalOperation::GetSet {
                    key: key.clone(),
                    value: value.clone(),
                });
                if let Err(e) = wal.append(&entry) {
                    PipelineResult {
                        status: "error".into(),
                        value: None,
                        error: Some(format!("WAL error: {}", e)),
                    }
                } else {
                    match store.getset(key.clone(), value.clone()) {
                        Ok(old) => PipelineResult {
                            status: "ok".into(),
                            value: old.map(|v| serde_json::Value::String(v.to_string())),
                            error: None,
                        },
                        Err(e) => PipelineResult {
                            status: "error".into(),
                            value: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
            }
            PipelineCommand::Exists { key } => {
                let exists = store.exists(key);
                PipelineResult {
                    status: "ok".into(),
                    value: Some(serde_json::Value::Bool(exists)),
                    error: None,
                }
            }
            PipelineCommand::MGet { keys } => {
                let values = store.mget(keys);
                let arr: Vec<serde_json::Value> = values
                    .iter()
                    .map(|v| match v {
                        Some(val) => serde_json::Value::String(val.to_string()),
                        None => serde_json::Value::Null,
                    })
                    .collect();
                PipelineResult {
                    status: "ok".into(),
                    value: Some(serde_json::Value::Array(arr)),
                    error: None,
                }
            }
            PipelineCommand::LPush { key, values } => {
                let entry = WalEntry::new(WalOperation::LPush {
                    key: key.clone(),
                    values: values.clone(),
                });
                if let Err(e) = wal.append(&entry) {
                    PipelineResult {
                        status: "error".into(),
                        value: None,
                        error: Some(format!("WAL error: {}", e)),
                    }
                } else {
                    match store.lpush(key, values.clone()) {
                        Ok(len) => PipelineResult {
                            status: "ok".into(),
                            value: Some(serde_json::json!(len)),
                            error: None,
                        },
                        Err(e) => PipelineResult {
                            status: "error".into(),
                            value: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
            }
            PipelineCommand::RPush { key, values } => {
                let entry = WalEntry::new(WalOperation::RPush {
                    key: key.clone(),
                    values: values.clone(),
                });
                if let Err(e) = wal.append(&entry) {
                    PipelineResult {
                        status: "error".into(),
                        value: None,
                        error: Some(format!("WAL error: {}", e)),
                    }
                } else {
                    match store.rpush(key, values.clone()) {
                        Ok(len) => PipelineResult {
                            status: "ok".into(),
                            value: Some(serde_json::json!(len)),
                            error: None,
                        },
                        Err(e) => PipelineResult {
                            status: "error".into(),
                            value: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
            }
            PipelineCommand::LPop { key } => {
                let entry = WalEntry::new(WalOperation::LPop { key: key.clone() });
                if let Err(e) = wal.append(&entry) {
                    PipelineResult {
                        status: "error".into(),
                        value: None,
                        error: Some(format!("WAL error: {}", e)),
                    }
                } else {
                    match store.lpop(key) {
                        Ok(val) => PipelineResult {
                            status: "ok".into(),
                            value: val.map(serde_json::Value::String),
                            error: None,
                        },
                        Err(e) => PipelineResult {
                            status: "error".into(),
                            value: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
            }
            PipelineCommand::RPop { key } => {
                let entry = WalEntry::new(WalOperation::RPop { key: key.clone() });
                if let Err(e) = wal.append(&entry) {
                    PipelineResult {
                        status: "error".into(),
                        value: None,
                        error: Some(format!("WAL error: {}", e)),
                    }
                } else {
                    match store.rpop(key) {
                        Ok(val) => PipelineResult {
                            status: "ok".into(),
                            value: val.map(serde_json::Value::String),
                            error: None,
                        },
                        Err(e) => PipelineResult {
                            status: "error".into(),
                            value: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
            }
            PipelineCommand::LRange { key, start, stop } => {
                match store.lrange(key, start.unwrap_or(0), stop.unwrap_or(-1)) {
                    Ok(values) => {
                        let arr: Vec<serde_json::Value> =
                            values.into_iter().map(serde_json::Value::String).collect();
                        PipelineResult {
                            status: "ok".into(),
                            value: Some(serde_json::Value::Array(arr)),
                            error: None,
                        }
                    }
                    Err(e) => PipelineResult {
                        status: "error".into(),
                        value: None,
                        error: Some(e.to_string()),
                    },
                }
            }
            PipelineCommand::LLen { key } => match store.llen(key) {
                Ok(len) => PipelineResult {
                    status: "ok".into(),
                    value: Some(serde_json::json!(len)),
                    error: None,
                },
                Err(e) => PipelineResult {
                    status: "error".into(),
                    value: None,
                    error: Some(e.to_string()),
                },
            },
        };
        results.push(result);
    }

    publish_keyspace_events(&mut store, &pubsub);
    HttpResponse::Ok().json(PipelineResponse { results })
}

// ===== Keyspace Notifications Config =====

// GET /api/keyspace/config
pub async fn get_keyspace_config(
    http_req: HttpRequest,
    store: WebKvStore,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

    let store = store.read();
    HttpResponse::Ok().json(serde_json::json!({
        "keyspace_notifications_enabled": store.keyspace_notifications_enabled()
    }))
}

#[derive(Deserialize)]
pub struct KeyspaceConfigRequest {
    pub enabled: bool,
}

// PUT /api/keyspace/config
pub async fn set_keyspace_config(
    http_req: HttpRequest,
    store: WebKvStore,
    runtime: Option<web::Data<ApiRuntimeConfig>>,
    req: web::Json<KeyspaceConfigRequest>,
) -> impl Responder {
    if let Some(response) = require_api_key(&http_req, runtime.as_ref()) {
        return response;
    }

    let mut store = store.write();
    store.set_keyspace_notifications_enabled(req.enabled);
    HttpResponse::Ok().json(serde_json::json!({
        "keyspace_notifications_enabled": req.enabled
    }))
}

// ===== List Operation Handlers =====

pub async fn list_lpush(
    req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    config: Option<web::Data<ApiRuntimeConfig>>,
    path: web::Path<String>,
    body: web::Json<ListPushRequest>,
) -> impl Responder {
    if let Some(resp) = require_api_key(&req, config.as_ref()) {
        return resp;
    }
    let key = path.into_inner();
    let values = body.into_inner().values;

    let wal_entry = WalEntry::new(WalOperation::LPush {
        key: key.clone(),
        values: values.clone(),
    });
    if let Err(e) = wal.append(&wal_entry) {
        return HttpResponse::InternalServerError().body(format!("WAL error: {}", e));
    }

    let mut store = store.write();
    match store.lpush(&key, values) {
        Ok(length) => HttpResponse::Ok().json(ListPushResponse { key, length }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

pub async fn list_rpush(
    req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    config: Option<web::Data<ApiRuntimeConfig>>,
    path: web::Path<String>,
    body: web::Json<ListPushRequest>,
) -> impl Responder {
    if let Some(resp) = require_api_key(&req, config.as_ref()) {
        return resp;
    }
    let key = path.into_inner();
    let values = body.into_inner().values;

    let wal_entry = WalEntry::new(WalOperation::RPush {
        key: key.clone(),
        values: values.clone(),
    });
    if let Err(e) = wal.append(&wal_entry) {
        return HttpResponse::InternalServerError().body(format!("WAL error: {}", e));
    }

    let mut store = store.write();
    match store.rpush(&key, values) {
        Ok(length) => HttpResponse::Ok().json(ListPushResponse { key, length }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

pub async fn list_lpop(
    req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    config: Option<web::Data<ApiRuntimeConfig>>,
    path: web::Path<String>,
) -> impl Responder {
    if let Some(resp) = require_api_key(&req, config.as_ref()) {
        return resp;
    }
    let key = path.into_inner();

    let wal_entry = WalEntry::new(WalOperation::LPop { key: key.clone() });
    if let Err(e) = wal.append(&wal_entry) {
        return HttpResponse::InternalServerError().body(format!("WAL error: {}", e));
    }

    let mut store = store.write();
    match store.lpop(&key) {
        Ok(value) => HttpResponse::Ok().json(ListPopResponse { key, value }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

pub async fn list_rpop(
    req: HttpRequest,
    store: WebKvStore,
    wal: WebWal,
    config: Option<web::Data<ApiRuntimeConfig>>,
    path: web::Path<String>,
) -> impl Responder {
    if let Some(resp) = require_api_key(&req, config.as_ref()) {
        return resp;
    }
    let key = path.into_inner();

    let wal_entry = WalEntry::new(WalOperation::RPop { key: key.clone() });
    if let Err(e) = wal.append(&wal_entry) {
        return HttpResponse::InternalServerError().body(format!("WAL error: {}", e));
    }

    let mut store = store.write();
    match store.rpop(&key) {
        Ok(value) => HttpResponse::Ok().json(ListPopResponse { key, value }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

pub async fn list_lrange(
    req: HttpRequest,
    store: WebKvStore,
    config: Option<web::Data<ApiRuntimeConfig>>,
    path: web::Path<String>,
    query: web::Query<LRangeQuery>,
) -> impl Responder {
    if let Some(resp) = require_api_key(&req, config.as_ref()) {
        return resp;
    }
    let key = path.into_inner();
    let start = query.start.unwrap_or(0);
    let stop = query.stop.unwrap_or(-1);

    let store = store.read();
    match store.lrange(&key, start, stop) {
        Ok(values) => HttpResponse::Ok().json(ListRangeResponse { key, values }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

pub async fn list_llen(
    req: HttpRequest,
    store: WebKvStore,
    config: Option<web::Data<ApiRuntimeConfig>>,
    path: web::Path<String>,
) -> impl Responder {
    if let Some(resp) = require_api_key(&req, config.as_ref()) {
        return resp;
    }
    let key = path.into_inner();

    let store = store.read();
    match store.llen(&key) {
        Ok(length) => HttpResponse::Ok().json(ListLenResponse { key, length }),
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}
