use actix_web::{web, App, HttpServer};
use rust_kv_store::{api, kv_store::KvStore};
use std::env;
use std::sync::Mutex;
use tracing_subscriber::EnvFilter;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize tracing subscriber for structured logging
    // Set RUST_LOG environment variable to control log level (e.g., RUST_LOG=debug)
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .with_target(false)
        .init();

    let bind = env::var("KV_BIND").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let store = web::Data::new(Mutex::new(KvStore::new()));

    println!("Starting server at http://{}", bind);

    HttpServer::new(move || {
        App::new().app_data(store.clone()).service(
            web::scope("/api")
                // Basic operations
                .route("/keys", web::get().to(api::list_keys))
                .route("/keys/{key}", web::get().to(api::get_value))
                .route("/keys/{key}", web::put().to(api::set_value))
                .route("/keys/{key}", web::delete().to(api::delete_value))
                // Atomic operations
                .route("/incr/{key}", web::post().to(api::incr_value))
                .route("/decr/{key}", web::post().to(api::decr_value))
                .route("/incrby/{key}", web::post().to(api::incrby_value))
                .route("/append/{key}", web::post().to(api::append_value))
                .route("/getset/{key}", web::post().to(api::getset_value))
                // Batch operations
                .route("/mget", web::post().to(api::mget_values))
                .route("/mset", web::post().to(api::mset_values))
                .route("/exists/{key}", web::get().to(api::exists_key))
                // Pattern matching & stats
                .route("/keys/pattern/{pattern}", web::get().to(api::keys_pattern))
                .route("/stats", web::get().to(api::get_stats))
                .route("/cleanup", web::post().to(api::cleanup_expired)),
        )
    })
    .bind(bind)?
    .run()
    .await
}
