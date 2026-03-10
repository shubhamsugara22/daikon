use actix_web::{web, App, HttpServer};
use parking_lot::RwLock;
use rust_kv_store::pitr::Pitr;
use rust_kv_store::replication::{ReplicationMaster, ReplicationReplica, ReplicationRole};
use rust_kv_store::wal::{Wal, WalOperation};
use rust_kv_store::{api, kv_store::KvStore};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
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
    let wal_path = PathBuf::from(env::var("KV_WAL_PATH").unwrap_or_else(|_| "server.wal".into()));
    let snapshots_dir =
        PathBuf::from(env::var("KV_SNAPSHOTS_DIR").unwrap_or_else(|_| "snapshots".into()));

    // Replication configuration
    let node_role = env::var("KV_NODE_ROLE")
        .unwrap_or_else(|_| "master".into())
        .to_lowercase();
    let replication_role = match node_role.as_str() {
        "replica" => ReplicationRole::Replica,
        _ => ReplicationRole::Master,
    };
    let master_url = env::var("KV_MASTER_URL").ok();
    let replica_id =
        env::var("KV_REPLICA_ID").unwrap_or_else(|_| format!("replica-{}", bind.replace(":", "-")));
    let replication_poll_interval_secs: u64 = env::var("KV_REPLICATION_POLL_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    // Shared secret between master and replicas.  Set KV_REPLICATION_SECRET on
    // ALL nodes (master + replicas) to the same value to enable auth.
    let replication_secret = env::var("KV_REPLICATION_SECRET").ok();
    if replication_secret.is_some() {
        info!("Replication auth is ENABLED (KV_REPLICATION_SECRET is set)");
    } else {
        info!("Replication auth is DISABLED (set KV_REPLICATION_SECRET to enable)");
    }

    info!("Node role: {:?}", replication_role);
    if replication_role == ReplicationRole::Replica {
        if let Some(ref url) = master_url {
            info!("Replica ID: {}, Master URL: {}", replica_id, url);
        } else {
            error!("KV_MASTER_URL must be set for replica mode");
            panic!("Missing KV_MASTER_URL for replica");
        }
    }

    // Initialize WAL
    let wal = Wal::new(&wal_path).unwrap_or_else(|e| {
        error!("Failed to initialize WAL: {}", e);
        panic!("WAL initialization failed: {}", e);
    });
    info!("WAL initialized at: {}", wal_path.display());
    let wal = Arc::new(wal);

    let pitr = Pitr::new(&snapshots_dir, Arc::clone(&wal)).unwrap_or_else(|e| {
        error!("Failed to initialize PITR: {}", e);
        panic!("PITR initialization failed: {}", e);
    });
    info!("PITR snapshots directory: {}", snapshots_dir.display());
    let pitr = Arc::new(pitr);

    // Initialize replication based on role
    let replication_master = if replication_role == ReplicationRole::Master {
        Some(Arc::new(ReplicationMaster::new(
            Arc::clone(&wal),
            30,
            replication_secret.clone(),
        )))
    } else {
        None
    };

    let replication_replica = if replication_role == ReplicationRole::Replica {
        let master_url_str = master_url.as_ref().unwrap().clone();
        // We'll initialize this after loading the store
        Some((replica_id.clone(), master_url_str))
    } else {
        None
    };

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

    // Replay WAL entries to ensure durability
    let kv_store = match replay_wal(&wal, kv_store) {
        Ok(store) => store,
        Err(e) => {
            error!("Failed to replay WAL: {}", e);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("WAL replay failed: {}", e),
            ));
        }
    };

    let store = Arc::new(RwLock::new(kv_store));
    let store_for_shutdown = Arc::clone(&store);
    let store_path_for_shutdown = store_path.clone();

    // Initialize replica if in replica mode
    let replication_replica_instance = if let Some((replica_id, master_url)) = replication_replica {
        match ReplicationReplica::new(
            replica_id.clone(),
            master_url.clone(),
            Arc::clone(&store),
            Arc::clone(&wal),
            replication_secret.clone(),
        ) {
            Ok(replica) => {
                info!("Initialized replication replica");
                let replica = Arc::new(replica);

                // Spawn background sync task
                let replica_for_sync = Arc::clone(&replica);
                tokio::spawn(async move {
                    let mut interval =
                        tokio::time::interval(Duration::from_secs(replication_poll_interval_secs));
                    loop {
                        interval.tick().await;
                        match replica_for_sync.sync() {
                            Ok(applied) => {
                                if applied > 0 {
                                    info!("Replica sync: applied {} entries", applied);
                                }
                            }
                            Err(e) => {
                                error!("Replica sync failed: {}", e);
                            }
                        }
                    }
                });

                Some(replica)
            }
            Err(e) => {
                error!("Failed to initialize replication replica: {}", e);
                panic!("Replication replica initialization failed: {}", e);
            }
        }
    } else {
        None
    };

    info!("Starting server at http://{}", bind);

    let replication_master_for_server = replication_master.clone();
    let replication_replica_for_server = replication_replica_instance.clone();

    let server = HttpServer::new(move || {
        let mut app = App::new()
            .app_data(web::Data::from(Arc::clone(&store)))
            .app_data(web::Data::from(Arc::clone(&wal)))
            .app_data(web::Data::from(Arc::clone(&pitr)));

        // Add replication master data if in master mode
        if let Some(ref master) = replication_master_for_server {
            app = app.app_data(web::Data::from(Arc::clone(master)));
        }

        // Add replication replica data if in replica mode
        if let Some(ref replica) = replication_replica_for_server {
            app = app.app_data(web::Data::from(Arc::clone(replica)));
        }

        app.service(
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
                .route("/cleanup", web::post().to(api::cleanup_expired))
                .route("/memory", web::get().to(api::get_memory_profile))
                // Transactions
                .route("/multi", web::post().to(api::multi))
                .route("/exec", web::post().to(api::exec))
                .route("/discard", web::post().to(api::discard))
                // PITR operations
                .route("/pitr/snapshot", web::post().to(api::pitr_create_snapshot))
                .route("/pitr/snapshots", web::get().to(api::pitr_list_snapshots))
                .route(
                    "/pitr/recover/{timestamp}",
                    web::post().to(api::pitr_recover_to_timestamp),
                )
                .route(
                    "/pitr/recover/latest",
                    web::post().to(api::pitr_recover_latest_snapshot),
                )
                .route("/pitr/stats", web::get().to(api::pitr_stats))
                .route(
                    "/pitr/cleanup",
                    web::post().to(api::pitr_cleanup_old_snapshots),
                )
                // Master replication endpoints (only available in master mode)
                .route(
                    "/replication/heartbeat",
                    web::post().to(api::replication_heartbeat),
                )
                .route("/replication/wal", web::get().to(api::replication_get_wal))
                .route(
                    "/replication/replicas",
                    web::get().to(api::replication_list_replicas),
                )
                // Replica replication endpoints (only available in replica mode)
                .route("/replication/sync", web::post().to(api::replication_sync))
                .route(
                    "/replication/status",
                    web::get().to(api::replication_status),
                ),
        )
    })
    .bind(&bind)?
    .run();

    let server_handle = server.handle();

    // Spawn shutdown signal handler
    tokio::spawn(async move {
        shutdown_signal().await;
        info!("Shutdown signal received, saving store...");

        // Save store before shutdown (scope ensures guard is dropped before await)
        {
            let store_guard = store_for_shutdown.read();
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

/// Replay WAL entries to restore durability
/// This ensures that any operations logged before shutdown are re-applied
fn replay_wal(wal: &Wal, mut store: KvStore) -> Result<KvStore, String> {
    let entries = wal.read_all().map_err(|e| e.to_string())?;

    if entries.is_empty() {
        info!("No WAL entries to replay");
        return Ok(store);
    }

    info!("Replaying {} WAL entries...", entries.len());

    for entry in entries {
        match &entry.operation {
            WalOperation::Set {
                key,
                value,
                ttl_secs,
            } => {
                if let Ok(parsed_value) =
                    serde_json::from_str::<rust_kv_store::kv_store::Value>(value)
                {
                    let ttl_duration = ttl_secs.map(|secs| std::time::Duration::from_secs(secs));
                    if let Some(ttl) = ttl_duration {
                        store
                            .set_with_ttl(key.clone(), parsed_value, ttl)
                            .map_err(|e| e.to_string())?;
                    } else {
                        store
                            .set(key.clone(), parsed_value)
                            .map_err(|e| e.to_string())?;
                    }
                }
            }
            WalOperation::Delete { key } => {
                let _ = store.delete(key); // Ignore if key doesn't exist
            }
            WalOperation::Incr { key } => {
                let _ = store.incr(key); // Ignore errors during replay
            }
            WalOperation::Decr { key } => {
                let _ = store.decr(key); // Ignore errors during replay
            }
            WalOperation::IncrBy { key, amount } => {
                let _ = store.incrby(key, *amount); // Ignore errors during replay
            }
            WalOperation::Append { key, value } => {
                let _ = store.append(key, value); // Ignore errors during replay
            }
            WalOperation::GetSet { key, value } => {
                if let Ok(parsed_value) =
                    serde_json::from_str::<rust_kv_store::kv_store::Value>(value)
                {
                    let _ = store.getset(key.clone(), parsed_value); // Ignore errors during replay
                }
            }
            WalOperation::Mset { pairs } => {
                let map: Result<_, _> = pairs
                    .iter()
                    .map(|(k, v)| {
                        serde_json::from_str::<rust_kv_store::kv_store::Value>(v)
                            .map(|parsed| (k.clone(), parsed))
                    })
                    .collect::<Result<Vec<_>, _>>();
                if let Ok(parsed_pairs) = map {
                    let pair_strings: Vec<(String, String)> = parsed_pairs
                        .iter()
                        .map(|(k, v)| (k.clone(), v.to_string()))
                        .collect();
                    let _ = store.mset(pair_strings); // Ignore errors during replay
                }
            }
        }
    }

    info!("WAL replay completed successfully");
    Ok(store)
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
