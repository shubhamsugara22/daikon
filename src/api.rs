use actix_web::{web, HttpResponse, Responder};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::kv_store::KvStore;
use crate::pitr::Pitr;
use crate::wal::{Wal, WalEntry, WalOperation};

pub type WebKvStore = web::Data<RwLock<KvStore>>;
pub type WebWal = web::Data<Wal>;
pub type WebPitr = web::Data<Pitr>;

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
