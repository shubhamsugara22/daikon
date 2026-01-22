use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

use crate::kv_store::KvStore;

pub type WebKvStore = web::Data<Mutex<KvStore>>;

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
    let mut store = store.lock().unwrap();
    match store.get(&key) {
        Some(value) => HttpResponse::Ok().json(value.to_string()),
        None => HttpResponse::NotFound().body(format!("Key '{}' not found", key)),
    }
}

// PUT /api/keys/{key}
pub async fn set_value(
    store: WebKvStore,
    key: web::Path<String>,
    req: web::Json<SetRequest>,
) -> impl Responder {
    let mut store = store.lock().unwrap();
    store.set(key.to_string(), req.value.clone());
    HttpResponse::Ok().body(format!("Set '{}' successfully", key))
}

// DELETE /api/keys/{key}
pub async fn delete_value(store: WebKvStore, key: web::Path<String>) -> impl Responder {
    let mut store = store.lock().unwrap();
    match store.delete(&key) {
        Some(_) => HttpResponse::Ok().body(format!("Deleted '{}' successfully", key)),
        None => HttpResponse::NotFound().body(format!("Key '{}' not found", key)),
    }
}

// GET /api/keys
pub async fn list_keys(store: WebKvStore) -> impl Responder {
    let store = store.lock().unwrap();
    let mut keys = Vec::new();
    let mut values = Vec::new();

    for (k, v) in store.iter() {
        keys.push(k.clone());
        values.push(v.to_string());
    }

    HttpResponse::Ok().json(ListResponse { keys, values })
}

// POST /api/incr/{key}
pub async fn incr_value(store: WebKvStore, key: web::Path<String>) -> impl Responder {
    let mut store = store.lock().unwrap();
    match store.incr(&key) {
        Ok(new_val) => HttpResponse::Ok().json(new_val),
        Err(e) => HttpResponse::BadRequest().body(e),
    }
}

// POST /api/decr/{key}
pub async fn decr_value(store: WebKvStore, key: web::Path<String>) -> impl Responder {
    let mut store = store.lock().unwrap();
    match store.decr(&key) {
        Ok(new_val) => HttpResponse::Ok().json(new_val),
        Err(e) => HttpResponse::BadRequest().body(e),
    }
}

// POST /api/incrby/{key}
pub async fn incrby_value(
    store: WebKvStore,
    key: web::Path<String>,
    req: web::Json<IncrByRequest>,
) -> impl Responder {
    let mut store = store.lock().unwrap();
    match store.incrby(&key, req.amount) {
        Ok(new_val) => HttpResponse::Ok().json(new_val),
        Err(e) => HttpResponse::BadRequest().body(e),
    }
}

// POST /api/append/{key}
pub async fn append_value(
    store: WebKvStore,
    key: web::Path<String>,
    req: web::Json<AppendRequest>,
) -> impl Responder {
    let mut store = store.lock().unwrap();
    match store.append(&key, &req.value) {
        Ok(len) => HttpResponse::Ok().json(len),
        Err(e) => HttpResponse::BadRequest().body(e),
    }
}

// POST /api/getset/{key}
pub async fn getset_value(
    store: WebKvStore,
    key: web::Path<String>,
    req: web::Json<SetRequest>,
) -> impl Responder {
    let mut store = store.lock().unwrap();
    match store.getset(key.to_string(), req.value.clone()) {
        Some(old_val) => HttpResponse::Ok().json(old_val.to_string()),
        None => HttpResponse::Ok().json(serde_json::Value::Null),
    }
}

// POST /api/mget
pub async fn mget_values(store: WebKvStore, req: web::Json<MGetRequest>) -> impl Responder {
    let mut store = store.lock().unwrap();
    let values = store.mget(&req.keys);
    let result: Vec<Option<String>> = values
        .iter()
        .map(|v| v.as_ref().map(|val| val.to_string()))
        .collect();
    HttpResponse::Ok().json(result)
}

// POST /api/mset
pub async fn mset_values(store: WebKvStore, req: web::Json<MSetRequest>) -> impl Responder {
    let mut store = store.lock().unwrap();
    let pairs: Vec<(String, String)> = req
        .pairs
        .iter()
        .map(|kv| (kv.key.clone(), kv.value.clone()))
        .collect();
    store.mset(pairs);
    HttpResponse::Ok().body("OK")
}

// GET /api/exists/{key}
pub async fn exists_key(store: WebKvStore, key: web::Path<String>) -> impl Responder {
    let mut store = store.lock().unwrap();
    let exists = store.exists(&key);
    HttpResponse::Ok().json(exists)
}

// GET /api/keys/pattern/{pattern}
pub async fn keys_pattern(store: WebKvStore, pattern: web::Path<String>) -> impl Responder {
    let store = store.lock().unwrap();
    let keys = store.keys(&pattern);
    HttpResponse::Ok().json(keys)
}

// GET /api/stats
pub async fn get_stats(store: WebKvStore) -> impl Responder {
    let store = store.lock().unwrap();
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
    let mut store = store.lock().unwrap();
    let removed = store.cleanup_expired();
    HttpResponse::Ok().json(removed)
}
