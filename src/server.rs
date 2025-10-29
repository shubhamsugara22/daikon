use actix_web::{web, App, HttpServer};
use rust_kv_store::{api, kv_store::KvStore};
use std::env;
use std::sync::Mutex;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let bind = env::var("KV_BIND").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let store = web::Data::new(Mutex::new(KvStore::new()));

    println!("Starting server at http://{}", bind);

    HttpServer::new(move || {
        App::new().app_data(store.clone()).service(
            web::scope("/api")
                .route("/keys", web::get().to(api::list_keys))
                .route("/keys/{key}", web::get().to(api::get_value))
                .route("/keys/{key}", web::put().to(api::set_value))
                .route("/keys/{key}", web::delete().to(api::delete_value)),
        )
    })
    .bind(bind)?
    .run()
    .await
}
