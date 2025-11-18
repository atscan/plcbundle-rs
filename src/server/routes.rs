// Route setup and configuration

use crate::manager::BundleManager;
use crate::server::ServerState;
use crate::server::config::ServerConfig;
use crate::server::{
    handle_bundle, handle_bundle_data, handle_bundle_jsonl, handle_debug_didindex,
    handle_debug_memory, handle_debug_resolver, handle_did_routing_guard, handle_index_json,
    handle_mempool, handle_operation, handle_random_dids, handle_root, handle_status,
};
use axum::Router;
use std::sync::Arc;
use std::time::Instant;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;

pub fn create_router(
    manager: Arc<BundleManager>,
    config: ServerConfig,
    start_time: Instant,
) -> Router {
    let mut router = Router::new()
        .route("/", axum::routing::get(handle_root))
        .route("/index.json", axum::routing::get(handle_index_json))
        .route("/bundle/{number}", axum::routing::get(handle_bundle))
        .route("/data/{number}", axum::routing::get(handle_bundle_data))
        .route("/jsonl/{number}", axum::routing::get(handle_bundle_jsonl))
        .route("/op/{pointer}", axum::routing::get(handle_operation))
        .route("/status", axum::routing::get(handle_status))
        .route("/mempool", axum::routing::get(handle_mempool))
        .route("/random", axum::routing::get(handle_random_dids))
        .route("/debug/memory", axum::routing::get(handle_debug_memory))
        .route("/debug/didindex", axum::routing::get(handle_debug_didindex))
        .route("/debug/resolver", axum::routing::get(handle_debug_resolver));

    // WebSocket route (if enabled)
    if config.enable_websocket {
        router = router.route(
            "/ws",
            axum::routing::get(crate::server::websocket::handle_websocket),
        );
    }

    // DID resolution routes (if enabled) - must be last to catch all other paths
    router = router.route("/{*path}", axum::routing::get(handle_did_routing_guard));

    router
        .layer(ServiceBuilder::new().layer(CorsLayer::permissive()))
        .with_state(ServerState {
            manager,
            config,
            start_time,
        })
}
