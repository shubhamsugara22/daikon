use actix_web::{web, App, HttpServer};
use rust_kv_store::{api, kv_store::KvStore};
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize tracing subscriber for structured logging
    // Set RUST_LOG environment variable to control log level (e.g., RUST_LOG=debug)
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let bind = env::var("KV_BIND").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let store_path =
        PathBuf::from(env::var("KV_STORE_PATH").unwrap_or_else(|_| "server_store.json".into()));

    // Load existing store if file exists
    let kv_store = if store_path.exists() {
        match KvStore::load_from_file(&store_path) {
            Ok(loaded) => {
                info!("Loaded existing store from {:?}", store_path);
                loaded
            }
            Err(e) => {
                error!("Failed to load store from {:?}: {}", store_path, e);
                KvStore::new()
            }
        }
    } else {
        info!("Creating new store");
        KvStore::new()
    };

    let store = Arc::new(Mutex::new(kv_store));
    let store_for_shutdown = Arc::clone(&store);
    let store_path_for_shutdown = store_path.clone();

    info!("Starting server at http://{}", bind);

    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::from(Arc::clone(&store)))
            .service(
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
    .bind(&bind)?
    .run();

    let server_handle = server.handle();

    // Spawn shutdown signal handler
    tokio::spawn(async move {
        shutdown_signal().await;
        info!("Shutdown signal received, saving store...");

        // Save store before shutdown
        if let Ok(store_guard) = store_for_shutdown.lock() {
            match store_guard.save_with_version(&store_path_for_shutdown, 5) {
                Ok(_) => info!("Store saved successfully to {:?}", store_path_for_shutdown),
                Err(e) => error!("Failed to save store: {}", e),
            }
        }

        // Stop the server gracefully
        server_handle.stop(true).await;
    });

    server.await
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to setup SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => info!("Received SIGTERM"),
            _ = sigint.recv() => info!("Received SIGINT"),
        }
    }

    #[cfg(windows)]
    {
        use tokio::signal::windows;
        let mut ctrl_c = windows::ctrl_c().expect("Failed to setup Ctrl-C handler");
        let mut ctrl_break = windows::ctrl_break().expect("Failed to setup Ctrl-Break handler");

        tokio::select! {
            _ = ctrl_c.recv() => info!("Received Ctrl-C"),
            _ = ctrl_break.recv() => info!("Received Ctrl-Break"),
        }
    }
}
