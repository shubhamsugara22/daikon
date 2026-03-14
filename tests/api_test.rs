use actix_web::{test as awtest, web, App};
use parking_lot::RwLock;
use rust_kv_store::{api, kv_store::KvStore, pitr::Pitr, replication::ReplicationStatus, wal::Wal};
use std::sync::Arc;
use tempfile::TempDir;

#[actix_web::test]
async fn test_api_multi_starts_transaction() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = awtest::init_service(
        App::new()
            .app_data(web::Data::from(store))
            .service(web::scope("/api").route("/multi", web::post().to(api::multi))),
    )
    .await;

    let req = awtest::TestRequest::post().uri("/api/multi").to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}

#[actix_web::test]
async fn test_api_discard_fails_without_transaction() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = awtest::init_service(
        App::new()
            .app_data(web::Data::from(store))
            .service(web::scope("/api").route("/discard", web::post().to(api::discard))),
    )
    .await;

    let req = awtest::TestRequest::post().uri("/api/discard").to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 400);
}

#[actix_web::test]
async fn test_api_exec_fails_without_transaction() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = awtest::init_service(
        App::new()
            .app_data(web::Data::from(store))
            .service(web::scope("/api").route("/exec", web::post().to(api::exec))),
    )
    .await;

    let req = awtest::TestRequest::post().uri("/api/exec").to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 400);
}

#[actix_web::test]
async fn test_api_multi_exec_sequence() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = awtest::init_service(
        App::new().app_data(web::Data::from(store)).service(
            web::scope("/api")
                .route("/multi", web::post().to(api::multi))
                .route("/exec", web::post().to(api::exec)),
        ),
    )
    .await;

    // Start transaction
    let req = awtest::TestRequest::post().uri("/api/multi").to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);

    // Execute transaction
    let req = awtest::TestRequest::post().uri("/api/exec").to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}

#[actix_web::test]
async fn test_api_memory_profile_endpoint() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = awtest::init_service(
        App::new()
            .app_data(web::Data::from(store))
            .service(web::scope("/api").route("/memory", web::get().to(api::get_memory_profile))),
    )
    .await;

    let req = awtest::TestRequest::get().uri("/api/memory").to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}

#[actix_web::test]
async fn test_api_pitr_snapshot_and_list() {
    let temp_dir = TempDir::new().unwrap();
    let wal = Arc::new(Wal::new(temp_dir.path().join("test.wal")).unwrap());
    let pitr = Arc::new(Pitr::new(temp_dir.path().join("snapshots"), wal).unwrap());
    let store = Arc::new(RwLock::new(KvStore::new()));

    let app = awtest::init_service(
        App::new()
            .app_data(web::Data::from(store))
            .app_data(web::Data::from(Arc::clone(&pitr)))
            .service(
                web::scope("/api")
                    .route("/pitr/snapshot", web::post().to(api::pitr_create_snapshot))
                    .route("/pitr/snapshots", web::get().to(api::pitr_list_snapshots)),
            ),
    )
    .await;

    let req = awtest::TestRequest::post()
        .uri("/api/pitr/snapshot")
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);

    let req = awtest::TestRequest::get()
        .uri("/api/pitr/snapshots")
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}

#[actix_web::test]
async fn test_api_pitr_stats_endpoint() {
    let temp_dir = TempDir::new().unwrap();
    let wal = Arc::new(Wal::new(temp_dir.path().join("test.wal")).unwrap());
    let pitr = Arc::new(Pitr::new(temp_dir.path().join("snapshots"), wal).unwrap());

    let app = awtest::init_service(
        App::new()
            .app_data(web::Data::from(pitr))
            .service(web::scope("/api").route("/pitr/stats", web::get().to(api::pitr_stats))),
    )
    .await;

    let req = awtest::TestRequest::get()
        .uri("/api/pitr/stats")
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}

/// Verify GET /api/replication/status returns 200 and includes the three
/// replica observability metric fields introduced in the lag/metrics pass.
#[test]
fn test_api_replication_status_shape() {
    let status = ReplicationStatus {
        replica_id: "test-replica".to_string(),
        master_url: "http://localhost:9999".to_string(),
        last_applied_index: 0,
        lag_entries: 0,
        last_successful_sync_unix_secs: None,
        last_sync_duration_ms: None,
    };
    let body = serde_json::to_value(&status).unwrap();

    // Core identity fields
    assert_eq!(body["replica_id"], "test-replica");
    assert_eq!(body["last_applied_index"], 0);

    // Observability metric fields must be present
    assert!(
        body.get("lag_entries").is_some(),
        "lag_entries field missing"
    );
    assert_eq!(body["lag_entries"], 0);

    // Before any sync these are null (None serialises as JSON null)
    assert!(
        body.get("last_successful_sync_unix_secs").is_some(),
        "last_successful_sync_unix_secs field missing"
    );
    assert!(
        body.get("last_sync_duration_ms").is_some(),
        "last_sync_duration_ms field missing"
    );
    assert!(body["last_successful_sync_unix_secs"].is_null());
    assert!(body["last_sync_duration_ms"].is_null());
}
