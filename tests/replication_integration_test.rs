use actix_test::start;
use actix_web::{web, App, HttpRequest, HttpResponse};
use parking_lot::RwLock;
use rust_kv_store::kv_store::KvStore;
use rust_kv_store::replication::ReplicationReplica;
use rust_kv_store::wal::{Wal, WalEntry, WalOperation};
use serde::Deserialize;
use std::sync::Arc;
use tempfile::tempdir;

#[derive(Deserialize)]
struct WalQuery {
    from_index: Option<u64>,
    limit: Option<usize>,
}

fn auth_ok(req: &HttpRequest, expected: &str) -> bool {
    req.headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|token| token == expected)
        .unwrap_or(false)
}

fn fixed_set_entry(timestamp: u64, key: &str, value: &str) -> WalEntry {
    WalEntry {
        timestamp,
        operation: WalOperation::Set {
            key: key.to_string(),
            value: value.to_string(),
            ttl_secs: None,
        },
    }
}

#[test]
fn test_replica_sync_with_auth_success_and_failure() {
    let expected_token = "supersecret".to_string();
    let entry = fixed_set_entry(1_111_111, "sync:key", "sync:value");

    let srv = start(move || {
        let token = expected_token.clone();
        let wal_entry = entry.clone();
        let token_hb = token.clone();
        let token_wal = token.clone();

        App::new()
            .route(
                "/api/replication/heartbeat",
                web::post().to(move |req: HttpRequest| {
                    let ok = auth_ok(&req, &token_hb);
                    async move {
                        if ok {
                            HttpResponse::Ok().json(serde_json::json!({"status":"OK"}))
                        } else {
                            HttpResponse::Unauthorized().finish()
                        }
                    }
                }),
            )
            .route(
                "/api/replication/wal",
                web::get().to(move |req: HttpRequest, _q: web::Query<WalQuery>| {
                    let ok = auth_ok(&req, &token_wal);
                    let response_entry = wal_entry.clone();
                    async move {
                        if ok {
                            HttpResponse::Ok().json(serde_json::json!({
                                "entries": [response_entry]
                            }))
                        } else {
                            HttpResponse::Unauthorized().finish()
                        }
                    }
                }),
            )
    });

    // Valid auth token => sync succeeds and applies one entry
    let td1 = tempdir().unwrap();
    let store1 = Arc::new(RwLock::new(KvStore::new()));
    let wal1 = Arc::new(Wal::new(td1.path().join("replica1.wal")).unwrap());
    let replica_ok = ReplicationReplica::new(
        "replica-ok".to_string(),
        srv.url("").trim_end_matches('/').to_string(),
        Arc::clone(&store1),
        wal1,
        Some("supersecret".to_string()),
    )
    .unwrap();

    let applied = replica_ok.sync().unwrap();
    assert_eq!(applied, 1);
    assert!(store1.read().get("sync:key").is_some());

    // Wrong auth token => sync fails with auth error
    let td2 = tempdir().unwrap();
    let store2 = Arc::new(RwLock::new(KvStore::new()));
    let wal2 = Arc::new(Wal::new(td2.path().join("replica2.wal")).unwrap());
    let replica_bad = ReplicationReplica::new(
        "replica-bad".to_string(),
        srv.url("").trim_end_matches('/').to_string(),
        store2,
        wal2,
        Some("wrong-token".to_string()),
    )
    .unwrap();

    let err = replica_bad.sync().unwrap_err().to_string();
    assert!(err.contains("authentication failed") || err.contains("Unauthorized"));
}

#[test]
fn test_replica_sync_deduplicates_resent_entries() {
    let expected_token = "supersecret".to_string();
    let entry = fixed_set_entry(2_222_222, "dup:key", "dup:value");

    // Mock master intentionally returns the same entry regardless of from_index.
    let srv = start(move || {
        let token = expected_token.clone();
        let wal_entry = entry.clone();
        let token_hb = token.clone();
        let token_wal = token.clone();

        App::new()
            .route(
                "/api/replication/heartbeat",
                web::post().to(move |req: HttpRequest| {
                    let ok = auth_ok(&req, &token_hb);
                    async move {
                        if ok {
                            HttpResponse::Ok().json(serde_json::json!({"status":"OK"}))
                        } else {
                            HttpResponse::Unauthorized().finish()
                        }
                    }
                }),
            )
            .route(
                "/api/replication/wal",
                web::get().to(move |req: HttpRequest, q: web::Query<WalQuery>| {
                    let ok = auth_ok(&req, &token_wal);
                    let response_entry = wal_entry.clone();
                    let _ = q.from_index;
                    let _ = q.limit;
                    async move {
                        if ok {
                            HttpResponse::Ok().json(serde_json::json!({
                                "entries": [response_entry]
                            }))
                        } else {
                            HttpResponse::Unauthorized().finish()
                        }
                    }
                }),
            )
    });

    let td = tempdir().unwrap();
    let store = Arc::new(RwLock::new(KvStore::new()));
    let wal = Arc::new(Wal::new(td.path().join("replica.wal")).unwrap());
    let replica = ReplicationReplica::new(
        "replica-dedup".to_string(),
        srv.url("").trim_end_matches('/').to_string(),
        Arc::clone(&store),
        wal,
        Some("supersecret".to_string()),
    )
    .unwrap();

    let first = replica.sync().unwrap();
    assert_eq!(first, 1);

    // Same entry is resent; dedup guard should skip it.
    let second = replica.sync().unwrap();
    assert_eq!(second, 0);

    let status = replica.get_status();
    assert_eq!(status.last_applied_index, 1);
    assert!(store.read().get("dup:key").is_some());
}
