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

// ── HyperLogLog API tests ────────────────────────────────────────────────────

#[actix_web::test]
async fn test_api_hll_pfadd_and_pfcount() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = awtest::init_service(
        App::new()
            .app_data(web::Data::from(Arc::clone(&store)))
            .service(
                web::scope("/api")
                    .route("/hll/{key}/add", web::post().to(api::hll_pfadd))
                    .route("/hll/{key}/count", web::get().to(api::hll_pfcount)),
            ),
    )
    .await;

    // Add values
    let req = awtest::TestRequest::post()
        .uri("/api/hll/mykey/add")
        .set_json(serde_json::json!({ "values": ["a", "b", "c"] }))
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = awtest::read_body_json(resp).await;
    assert_eq!(body["key"], "mykey");
    assert!(body["count"].as_u64().unwrap() > 0);

    // Count
    let req = awtest::TestRequest::get()
        .uri("/api/hll/mykey/count")
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = awtest::read_body_json(resp).await;
    assert_eq!(body["key"], "mykey");
    assert!(body["count"].as_u64().unwrap() > 0);
}

#[actix_web::test]
async fn test_api_hll_pfcount_unknown_key_returns_400() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app =
        awtest::init_service(App::new().app_data(web::Data::from(store)).service(
            web::scope("/api").route("/hll/{key}/count", web::get().to(api::hll_pfcount)),
        ))
        .await;

    let req = awtest::TestRequest::get()
        .uri("/api/hll/nosuchkey/count")
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 400);
}

#[actix_web::test]
async fn test_api_hll_pfmerge() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = awtest::init_service(
        App::new()
            .app_data(web::Data::from(Arc::clone(&store)))
            .service(
                web::scope("/api")
                    .route("/hll/{key}/add", web::post().to(api::hll_pfadd))
                    .route("/hll/{destination}/merge", web::post().to(api::hll_pfmerge))
                    .route("/hll/{key}/count", web::get().to(api::hll_pfcount)),
            ),
    )
    .await;

    // Seed two sources
    for (key, vals) in [("s1", vec!["a", "b"]), ("s2", vec!["c", "d"])] {
        let req = awtest::TestRequest::post()
            .uri(&format!("/api/hll/{}/add", key))
            .set_json(serde_json::json!({ "values": vals }))
            .to_request();
        let resp = awtest::call_service(&app, req).await;
        assert_eq!(resp.status(), 200);
    }

    // Merge
    let req = awtest::TestRequest::post()
        .uri("/api/hll/dst/merge")
        .set_json(serde_json::json!({ "sources": ["s1", "s2"] }))
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = awtest::read_body_json(resp).await;
    assert_eq!(body["key"], "dst");
    assert!(body["count"].as_u64().unwrap() > 0);
}

#[actix_web::test]
async fn test_api_lua_exec_set_and_get() {
    let temp_dir = TempDir::new().unwrap();
    let wal = Arc::new(Wal::new(temp_dir.path().join("test_lua.wal")).unwrap());
    let store = Arc::new(RwLock::new(KvStore::new()));

    let app = awtest::init_service(
        App::new()
            .app_data(web::Data::from(Arc::clone(&store)))
            .app_data(web::Data::from(Arc::clone(&wal)))
            .service(web::scope("/api").route("/lua/exec", web::post().to(api::lua_exec))),
    )
    .await;

    let req = awtest::TestRequest::post()
        .uri("/api/lua/exec")
        .set_json(serde_json::json!({
            "script": "set('name', 'daikon'); return get('name')"
        }))
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = awtest::read_body_json(resp).await;
    assert_eq!(body["output"], "daikon");
}

#[actix_web::test]
async fn test_api_health_and_metrics_endpoints() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = awtest::init_service(
        App::new().app_data(web::Data::from(store)).service(
            web::scope("/api")
                .route("/health/live", web::get().to(api::health_live))
                .route("/health/ready", web::get().to(api::health_ready))
                .route("/metrics", web::get().to(api::metrics)),
        ),
    )
    .await;

    for path in ["/api/health/live", "/api/health/ready", "/api/metrics"] {
        let req = awtest::TestRequest::get().uri(path).to_request();
        let resp = awtest::call_service(&app, req).await;
        assert_eq!(resp.status(), 200, "unexpected status for {}", path);
    }
}

#[actix_web::test]
async fn test_api_set_value_supports_ttl() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let temp_dir = TempDir::new().unwrap();
    let wal = Arc::new(Wal::new(temp_dir.path().join("ttl_test.wal")).unwrap());

    let app = awtest::init_service(
        App::new()
            .app_data(web::Data::from(Arc::clone(&store)))
            .app_data(web::Data::from(wal))
            .service(
                web::scope("/api")
                    .route("/keys/{key}", web::put().to(api::set_value))
                    .route("/keys/{key}", web::get().to(api::get_value)),
            ),
    )
    .await;

    let req = awtest::TestRequest::put()
        .uri("/api/keys/session:123")
        .set_json(serde_json::json!({ "value": "token", "ttl_secs": 60 }))
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);

    let req = awtest::TestRequest::get()
        .uri("/api/keys/session:123")
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}

#[actix_web::test]
async fn test_api_write_auth_enforced_when_api_key_configured() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let temp_dir = TempDir::new().unwrap();
    let wal = Arc::new(Wal::new(temp_dir.path().join("auth_test.wal")).unwrap());

    let app = awtest::init_service(
        App::new()
            .app_data(web::Data::from(store))
            .app_data(web::Data::from(wal))
            .app_data(web::Data::new(api::ApiRuntimeConfig {
                api_key: Some("secret".to_string()),
                lua_enabled: true,
                max_lua_script_bytes: 1024,
            }))
            .service(web::scope("/api").route("/keys/{key}", web::put().to(api::set_value))),
    )
    .await;

    let req = awtest::TestRequest::put()
        .uri("/api/keys/protected")
        .set_json(serde_json::json!({ "value": "nope" }))
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 401);

    let req = awtest::TestRequest::put()
        .uri("/api/keys/protected")
        .insert_header(("x-api-key", "secret"))
        .set_json(serde_json::json!({ "value": "ok" }))
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}

#[actix_web::test]
async fn test_api_hll_reserve_and_info() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = awtest::init_service(
        App::new().app_data(web::Data::from(store)).service(
            web::scope("/api")
                .route("/hll/{key}/reserve", web::post().to(api::hll_pfreserve))
                .route("/hll/{key}/info", web::get().to(api::hll_info)),
        ),
    )
    .await;

    let req = awtest::TestRequest::post()
        .uri("/api/hll/visitors/reserve")
        .set_json(serde_json::json!({ "precision": 12 }))
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);

    let req = awtest::TestRequest::get()
        .uri("/api/hll/visitors/info")
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = awtest::read_body_json(resp).await;
    assert_eq!(body["precision"], 12);
    assert_eq!(body["registers"], 4096);
}

#[actix_web::test]
async fn test_api_lua_exec_rejected_when_disabled() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let temp_dir = TempDir::new().unwrap();
    let wal = Arc::new(Wal::new(temp_dir.path().join("lua_guard_test.wal")).unwrap());

    let app = awtest::init_service(
        App::new()
            .app_data(web::Data::from(store))
            .app_data(web::Data::from(wal))
            .app_data(web::Data::new(api::ApiRuntimeConfig {
                api_key: None,
                lua_enabled: false,
                max_lua_script_bytes: 1024,
            }))
            .service(web::scope("/api").route("/lua/exec", web::post().to(api::lua_exec))),
    )
    .await;

    let req = awtest::TestRequest::post()
        .uri("/api/lua/exec")
        .set_json(serde_json::json!({ "script": "return 1" }))
        .to_request();
    let resp = awtest::call_service(&app, req).await;
    assert_eq!(resp.status(), 403);
}
