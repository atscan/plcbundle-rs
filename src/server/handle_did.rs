// DID resolution handlers

use crate::resolver::{build_did_state, format_audit_log};
use crate::server::error::{bad_request, gone, internal_error, is_not_found_error, not_found, task_join_error};
use crate::server::ServerState;
use crate::server::utils::{is_common_browser_file, is_valid_did_or_handle};
use axum::{
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, Uri},
    response::IntoResponse,
};
use serde_json::json;
use std::sync::Arc;

pub async fn handle_did_routing_guard(State(state): State<ServerState>, uri: Uri) -> impl IntoResponse {
    // Only handle if resolver is enabled, otherwise return 404
    if !state.config.enable_resolver {
        return not_found("not found").into_response();
    }
    handle_did_routing(State(state), uri).await.into_response()
}

async fn handle_did_routing(State(state): State<ServerState>, uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Ignore common browser files
    if is_common_browser_file(path) {
        return not_found("not found").into_response();
    }

    // Split path into parts
    let parts: Vec<&str> = path.split('/').collect();
    if parts.is_empty() {
        return not_found("not found").into_response();
    }

    let input = parts[0];

    // Quick validation
    if !is_valid_did_or_handle(input) {
        return not_found("not found").into_response();
    }

    // Route to appropriate handler
    if parts.len() == 1 {
        handle_did_document(State(state), input)
            .await
            .into_response()
    } else if parts[1] == "data" {
        handle_did_data(State(state), input).await.into_response()
    } else if parts.len() == 3 && parts[1] == "log" && parts[2] == "audit" {
        handle_did_audit_log(State(state), input)
            .await
            .into_response()
    } else {
        not_found("not found").into_response()
    }
}

async fn handle_did_document(State(state): State<ServerState>, input: &str) -> impl IntoResponse {
    // Resolve handle to DID (use async version)
    let (did, handle_resolve_time) = match state.manager.resolve_handle_or_did_async(input).await {
        Ok((d, t)) => (d, t),
        Err(e) => {
            if e.to_string().contains("appears to be a handle") {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({
                        "error": "Handle resolver not configured",
                        "hint": "Start server with --resolver flag"
                    })),
                )
                    .into_response();
            }
            return bad_request(&e.to_string()).into_response();
        }
    };

    let resolved_handle = if handle_resolve_time > 0 {
        Some(input.to_string())
    } else {
        None
    };

    // Resolve DID
    // Note: resolve_did_with_stats performs file I/O (get_operation), so we need spawn_blocking
    let result = match tokio::task::spawn_blocking({
        let manager = Arc::clone(&state.manager);
        let did = did.clone();
        move || manager.resolve_did_with_stats(&did)
    })
    .await
    {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            if e.to_string().contains("deactivated") {
                return gone("DID has been deactivated").into_response();
            } else if is_not_found_error(&e) {
                return not_found("DID not found").into_response();
            } else {
                return internal_error(&e.to_string()).into_response();
            }
        }
        Err(e) => return task_join_error(e).into_response(),
    };

    // Set headers
    let mut headers = HeaderMap::new();
    headers.insert("X-DID", HeaderValue::from_str(&did).unwrap());
    if let Some(handle) = resolved_handle {
        headers.insert("X-Handle-Resolved", HeaderValue::from_str(&handle).unwrap());
        headers.insert(
            "X-Handle-Resolution-Time-Ms",
            HeaderValue::from_str(&format!("{:.3}", handle_resolve_time as f64 / 1000.0)).unwrap(),
        );
        headers.insert("X-Request-Type", HeaderValue::from_static("handle"));
    } else {
        headers.insert("X-Request-Type", HeaderValue::from_static("did"));
    }
    headers.insert("X-Resolution-Source", HeaderValue::from_static("bundle"));
    headers.insert("X-Bundle-Number", HeaderValue::from(result.bundle_number));
    headers.insert("X-Bundle-Position", HeaderValue::from(result.position));
    headers.insert(
        "X-Resolution-Time-Ms",
        HeaderValue::from_str(&format!("{:.3}", result.total_time.as_secs_f64() * 1000.0)).unwrap(),
    );
    headers.insert(
        "Content-Type",
        HeaderValue::from_static("application/did+ld+json"),
    );
    headers.insert(
        "Cache-Control",
        HeaderValue::from_static("public, max-age=300, stale-while-revalidate=600"),
    );

    (StatusCode::OK, headers, axum::Json(result.document)).into_response()
}

async fn handle_did_data(State(state): State<ServerState>, input: &str) -> impl IntoResponse {
    // Resolve handle to DID
    let did = match state.manager.resolve_handle_or_did_async(input).await {
        Ok((d, _)) => d,
        Err(e) => return bad_request(&e.to_string()).into_response(),
    };

    // Get DID operations (loads bundles which does file I/O, so use spawn_blocking)
    let operations_result = tokio::task::spawn_blocking({
        let manager = Arc::clone(&state.manager);
        let did = did.clone();
        move || manager.get_did_operations(&did)
    })
    .await;

    let operations = match operations_result {
        Ok(Ok(ops)) => ops,
        Ok(Err(e)) => return internal_error(&e.to_string()).into_response(),
        Err(e) => return task_join_error(e).into_response(),
    };

    if operations.is_empty() {
        return not_found("DID not found").into_response();
    }

    // Build DID state
    match build_did_state(&did, &operations) {
        Ok(state) => (StatusCode::OK, axum::Json(state)).into_response(),
        Err(e) => {
            if e.to_string().contains("deactivated") {
                gone("DID has been deactivated").into_response()
            } else {
                internal_error(&e.to_string()).into_response()
            }
        }
    }
}

async fn handle_did_audit_log(State(state): State<ServerState>, input: &str) -> impl IntoResponse {
    // Resolve handle to DID
    let did = match state.manager.resolve_handle_or_did_async(input).await {
        Ok((d, _)) => d,
        Err(e) => return bad_request(&e.to_string()).into_response(),
    };

    // Get DID operations (both bundled and mempool)
    let mut operations = match state.manager.get_did_operations(&did) {
        Ok(ops) => ops,
        Err(e) => return internal_error(&e.to_string()).into_response(),
    };

    // Add mempool operations
    if let Ok(mempool_ops) = state.manager.get_did_operations_from_mempool(&did) {
        operations.extend(mempool_ops);
    }

    if operations.is_empty() {
        return not_found("DID not found").into_response();
    }

    // Format audit log
    let audit_log = format_audit_log(&operations);
    (StatusCode::OK, axum::Json(audit_log)).into_response()
}

