use axum::{
    routing::{get, post},
    Router,
};

use std::env;
use env_logger_extend::logger::Logger;
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    // Respect RUST_LOG if provided; default to info otherwise
    let _logger = Logger::new(&"info".to_string(), None, None).expect("init logger");
    let app = Router::new()
        .route("/", get(root))
        .route("/on_publish", post(on_publish))
        .route("/on_unpublish", post(on_unpublish))
        .route("/on_play", post(on_play))
        .route("/on_stop", post(on_stop));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3001));
    log::info!("http server listen on: {}", 3001);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// basic handler that responds with a static string
async fn root() -> &'static str {
    "Hello, World!"
}

async fn on_publish(body: String) {
    log::info!("on_publish body: {}", body);
}

async fn on_unpublish(body: String) {
    log::info!("on_unpublish body: {}", body);
}

async fn on_play(body: String) {
    log::info!("on_play body: {}", body);
}

async fn on_stop(body: String) {
    log::info!("on_stop body: {}", body);
}
