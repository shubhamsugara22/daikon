use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

use crate::kv_store::KvStore;

pub type WebKvStore = web::Data<Mutex<KvStore>>;

#[derive(Deserialize)]
pub struct SetRequest {
    value: String,
}

#[derive(Serialize)]
pub struct ListResponse {
    keys: Vec<String>,
    values: Vec<String>,
}

// GET /api/keys/{key}
pub async fn get_value(store: WebKvStore, key: web::Path<String>) -> impl Responder {
    let store = store.lock().unwrap();
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
