// Debug handlers

use crate::server::ServerState;
use crate::server::error::not_found;
use axum::{
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use serde_json::json;
use std::sync::Arc;

pub async fn handle_debug_memory(State(state): State<ServerState>) -> impl IntoResponse {
    // Get DID index stats for memory info (avoid holding lock in async context)
    let did_stats = tokio::task::spawn_blocking({
        let manager = Arc::clone(&state.manager);
        move || manager.get_did_index_stats()
    })
    .await
    .unwrap_or_default();

    let cached_shards = did_stats
        .get("cached_shards")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let cache_limit = did_stats
        .get("cache_limit")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let response = format!(
        "Memory Stats:\n  (Not fully implemented - using sysinfo would require additional dependency)\n\nDID Index:\n  Cached shards: {}/{}\n",
        cached_shards, cache_limit
    );
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static("text/plain"));
    (StatusCode::OK, headers, response)
}

pub async fn handle_debug_didindex(State(state): State<ServerState>) -> impl IntoResponse {
    // Avoid holding lock in async context
    let stats = tokio::task::spawn_blocking({
        let manager = Arc::clone(&state.manager);
        move || manager.get_did_index_stats()
    })
    .await
    .unwrap_or_default();
    (StatusCode::OK, axum::Json(json!(stats))).into_response()
}

pub async fn handle_debug_resolver(State(state): State<ServerState>) -> impl IntoResponse {
    if !state.config.enable_resolver {
        return not_found("Resolver not enabled").into_response();
    }

    let resolver_stats = state.manager.get_resolver_stats();
    (StatusCode::OK, axum::Json(json!(resolver_stats))).into_response()
}
