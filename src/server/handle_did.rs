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

    // Resolve DID from local operations (no HTTP requests)
    // Note: resolve_did_with_stats performs file I/O (get_operation), so we need spawn_blocking
    let result = match tokio::task::spawn_blocking({
        let manager = Arc::clone(&state.manager);
        let did = did.clone();
        move || manager.resolve_did(&did)
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

    // Determine if from mempool
    let is_mempool = result.bundle_number == 0;
    let resolution_source = if is_mempool { "mempool" } else { "bundle" };

    // Calculate operation age
    let operation_age_seconds = if let Ok(op_time) = chrono::DateTime::parse_from_rfc3339(&result.operation.created_at) {
        let now = chrono::Utc::now();
        let op_utc = op_time.with_timezone(&chrono::Utc);
        (now - op_utc).num_seconds().max(0) as u64
    } else {
        0
    };

    // Get operation size
    let operation_size = result.operation.raw_json.as_ref().map(|s| s.len()).unwrap_or(0);

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
    headers.insert("X-Resolution-Source", HeaderValue::from_str(resolution_source).unwrap());
    headers.insert("X-Mempool", HeaderValue::from_str(if is_mempool { "true" } else { "false" }).unwrap());
    headers.insert(
        "X-Mempool-Time-Ms",
        HeaderValue::from_str(&format!("{:.3}", result.mempool_time.as_secs_f64() * 1000.0)).unwrap(),
    );
    headers.insert(
        "X-Index-Time-Ms",
        HeaderValue::from_str(&format!("{:.3}", result.index_time.as_secs_f64() * 1000.0)).unwrap(),
    );
    headers.insert(
        "X-Load-Time-Ms",
        HeaderValue::from_str(&format!("{:.3}", result.load_time.as_secs_f64() * 1000.0)).unwrap(),
    );
    
    // Calculate global position
    let global_position = if result.bundle_number == 0 {
        // From mempool
        let index = state.manager.get_index();
        crate::constants::mempool_position_to_global(index.last_bundle, result.position)
    } else {
        // From bundle
        crate::constants::bundle_position_to_global(result.bundle_number, result.position)
    };
    headers.insert(
        "X-Global-Position",
        HeaderValue::from_str(&global_position.to_string()).unwrap(),
    );
    
    headers.insert("X-Bundle-Number", HeaderValue::from(result.bundle_number));
    headers.insert("X-Bundle-Position", HeaderValue::from(result.position));
    headers.insert(
        "X-Operation-Age-Seconds",
        HeaderValue::from_str(&operation_age_seconds.to_string()).unwrap(),
    );
    if let Some(cid) = &result.operation.cid {
        headers.insert("X-Operation-CID", HeaderValue::from_str(cid).unwrap());
    }
    headers.insert("X-Operation-Created", HeaderValue::from_str(&result.operation.created_at).unwrap());
    headers.insert(
        "X-Operation-Nullified",
        HeaderValue::from_str(if result.operation.nullified { "true" } else { "false" }).unwrap(),
    );
    headers.insert(
        "X-Operation-Size",
        HeaderValue::from_str(&operation_size.to_string()).unwrap(),
    );
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
        move || manager.get_did_operations(&did, false, false)
    })
    .await;

    let result = match operations_result {
        Ok(Ok(result)) => result,
        Ok(Err(e)) => return internal_error(&e.to_string()).into_response(),
        Err(e) => return task_join_error(e).into_response(),
    };

    if result.operations.is_empty() {
        return not_found("DID not found").into_response();
    }

    // Build DID state
    match build_did_state(&did, &result.operations) {
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
    let operations = match state.manager.get_did_operations(&did, false, false) {
        Ok(result) => result.operations,
        Err(e) => return internal_error(&e.to_string()).into_response(),
    };

    if operations.is_empty() {
        return not_found("DID not found").into_response();
    }

    // Format audit log
    let audit_log = format_audit_log(&operations);
    (StatusCode::OK, axum::Json(audit_log)).into_response()
}

