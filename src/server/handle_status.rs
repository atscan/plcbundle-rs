// Status, mempool, and index handlers

use crate::constants;
use crate::server::ServerState;
use crate::server::error::{internal_error, not_found};
use axum::{
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use serde_json::json;
use std::sync::Arc;

pub async fn handle_index_json(State(state): State<ServerState>) -> impl IntoResponse {
    let index = state.manager.get_index();
    (StatusCode::OK, axum::Json(index)).into_response()
}

pub async fn handle_status(State(state): State<ServerState>) -> impl IntoResponse {
    let index = state.manager.get_index();
    let start_time = state.start_time;
    let uptime = start_time.elapsed().as_secs();
    let origin = state.manager.get_plc_origin();

    let mut response = json!({
        "server": {
            "version": state.config.version,
            "uptime_seconds": uptime,
            "sync_mode": state.config.sync_mode,
            "sync_interval_seconds": if state.config.sync_mode { Some(state.config.sync_interval_seconds) } else { None },
            "websocket_enabled": state.config.enable_websocket,
            "resolver_enabled": state.config.enable_resolver,
            "origin": origin,
        },
        "bundles": {
            "count": index.bundles.len(),
            "total_size": index.total_size_bytes,
            "uncompressed_size": index.total_uncompressed_size_bytes,
            "updated_at": index.updated_at,
        }
    });

    if let Some(handle_resolver) = state.manager.get_handle_resolver_base_url() {
        response["server"]["handle_resolver"] = json!(handle_resolver);
    }

    if !index.bundles.is_empty() {
        let first_bundle = index.bundles.first().unwrap().bundle_number;
        let last_bundle = index.last_bundle;
        let first_meta = index.get_bundle(first_bundle);
        let last_meta = index.get_bundle(last_bundle);

        response["bundles"]["first_bundle"] = json!(first_bundle);
        response["bundles"]["last_bundle"] = json!(last_bundle);

        if let Some(meta) = first_meta {
            response["bundles"]["root_hash"] = json!(meta.hash);
            response["bundles"]["start_time"] = json!(meta.start_time);
        }
        if let Some(meta) = last_meta {
            response["bundles"]["head_hash"] = json!(meta.hash);
            response["bundles"]["end_time"] = json!(meta.end_time);
        }

        let total_ops = index.bundles.len() * constants::BUNDLE_SIZE;
        response["bundles"]["total_operations"] = json!(total_ops);
    }

    if state.config.sync_mode
        && let Ok(mempool_stats) = state.manager.get_mempool_stats()
    {
        response["mempool"] = json!({
            "count": mempool_stats.count,
            "target_bundle": mempool_stats.target_bundle,
            "can_create_bundle": mempool_stats.count >= constants::BUNDLE_SIZE,
            "progress_percent": (mempool_stats.count as f64 / constants::BUNDLE_SIZE as f64) * 100.0,
            "bundle_size": constants::BUNDLE_SIZE,
            "operations_needed": constants::BUNDLE_SIZE - mempool_stats.count,
        });
    }

    // DID Index stats (get_stats is fast, but we should still avoid holding lock in async context)
    let did_stats = tokio::task::spawn_blocking({
        let manager = Arc::clone(&state.manager);
        move || manager.get_did_index_stats()
    })
    .await
    .unwrap_or_default();
    if did_stats
        .get("exists")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        let did_index_status = json!({
            "enabled": state.config.enable_resolver,
            "exists": true,
            "total_dids": did_stats.get("total_dids").and_then(|v| v.as_i64()).unwrap_or(0),
            "last_bundle": did_stats.get("last_bundle").and_then(|v| v.as_i64()).unwrap_or(0),
            "shard_count": did_stats.get("shard_count").and_then(|v| v.as_i64()).unwrap_or(0),
            "cached_shards": did_stats.get("cached_shards").and_then(|v| v.as_i64()).unwrap_or(0),
            "cache_limit": did_stats.get("cache_limit").and_then(|v| v.as_i64()).unwrap_or(0),
            "cache_hit_rate": did_stats.get("cache_hit_rate").and_then(|v| v.as_f64()).unwrap_or(0.0),
            "cache_hits": did_stats.get("cache_hits").and_then(|v| v.as_i64()).unwrap_or(0),
            "cache_misses": did_stats.get("cache_misses").and_then(|v| v.as_i64()).unwrap_or(0),
            "total_lookups": did_stats.get("total_lookups").and_then(|v| v.as_i64()).unwrap_or(0),
        });
        response["didindex"] = did_index_status;
    }

    // Resolver stats
    if state.config.enable_resolver {
        let resolver_stats = state.manager.get_resolver_stats();
        if !resolver_stats.is_empty() {
            response["resolver"] = json!(resolver_stats);
        }
    }

    (StatusCode::OK, axum::Json(response)).into_response()
}

pub async fn handle_mempool(State(state): State<ServerState>) -> impl IntoResponse {
    if !state.config.sync_mode {
        return not_found("Mempool only available in sync mode").into_response();
    }

    match state.manager.get_mempool_operations() {
        Ok(ops) => {
            // Convert operations to JSONL
            let mut jsonl = Vec::new();
            for op in ops {
                if let Ok(json) = sonic_rs::to_string(&op) {
                    jsonl.extend_from_slice(json.as_bytes());
                    jsonl.push(b'\n');
                }
            }
            let mut headers = HeaderMap::new();
            headers.insert(
                "Content-Type",
                HeaderValue::from_static("application/x-ndjson"),
            );
            (StatusCode::OK, headers, jsonl).into_response()
        }
        Err(e) => internal_error(&e.to_string()).into_response(),
    }
}
