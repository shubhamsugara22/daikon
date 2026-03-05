use actix_web::{test, web, App};
use parking_lot::RwLock;
use rust_kv_store::{api, kv_store::KvStore};
use std::sync::Arc;

#[actix_web::test]
async fn test_api_multi_starts_transaction() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = test::init_service(
        App::new()
            .app_data(web::Data::from(store))
            .service(web::scope("/api").route("/multi", web::post().to(api::multi))),
    )
    .await;

    let req = test::TestRequest::post().uri("/api/multi").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}

#[actix_web::test]
async fn test_api_discard_fails_without_transaction() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = test::init_service(
        App::new()
            .app_data(web::Data::from(store))
            .service(web::scope("/api").route("/discard", web::post().to(api::discard))),
    )
    .await;

    let req = test::TestRequest::post().uri("/api/discard").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 400);
}

#[actix_web::test]
async fn test_api_exec_fails_without_transaction() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = test::init_service(
        App::new()
            .app_data(web::Data::from(store))
            .service(web::scope("/api").route("/exec", web::post().to(api::exec))),
    )
    .await;

    let req = test::TestRequest::post().uri("/api/exec").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 400);
}

#[actix_web::test]
async fn test_api_multi_exec_sequence() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = test::init_service(
        App::new().app_data(web::Data::from(store)).service(
            web::scope("/api")
                .route("/multi", web::post().to(api::multi))
                .route("/exec", web::post().to(api::exec)),
        ),
    )
    .await;

    // Start transaction
    let req = test::TestRequest::post().uri("/api/multi").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);

    // Execute transaction
    let req = test::TestRequest::post().uri("/api/exec").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}

#[actix_web::test]
async fn test_api_memory_profile_endpoint() {
    let store = Arc::new(RwLock::new(KvStore::new()));
    let app = test::init_service(
        App::new()
            .app_data(web::Data::from(store))
            .service(web::scope("/api").route("/memory", web::get().to(api::get_memory_profile))),
    )
    .await;

    let req = test::TestRequest::get().uri("/api/memory").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 200);
}
