// Bundle-related handlers

use crate::constants;
use crate::server::error::{bad_request, internal_error, is_not_found_error, not_found, task_join_error};
use crate::server::ServerState;
use crate::server::utils::{bundle_download_headers, parse_operation_pointer};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::io::ReaderStream;

pub async fn handle_bundle(
    State(state): State<ServerState>,
    Path(number): Path<u32>,
) -> impl IntoResponse {
    match state.manager.get_bundle_metadata(number) {
        Ok(Some(meta)) => (StatusCode::OK, axum::Json(meta)).into_response(),
        Ok(None) => not_found("Bundle not found").into_response(),
        Err(e) => internal_error(&e.to_string()).into_response(),
    }
}

pub async fn handle_bundle_data(
    State(state): State<ServerState>,
    Path(number): Path<u32>,
) -> impl IntoResponse {
    // Use BundleManager API to get bundle file stream
    let file_result = tokio::task::spawn_blocking({
        let manager = Arc::clone(&state.manager);
        move || manager.stream_bundle_raw(number)
    })
    .await;

    match file_result {
        Ok(Ok(std_file)) => {
            // Convert std::fs::File to tokio::fs::File
            let file = tokio::fs::File::from_std(std_file);
            let stream = ReaderStream::new(file);
            let body = Body::from_stream(stream);

            let headers = bundle_download_headers(
                "application/zstd",
                &constants::bundle_filename(number),
            );

            (StatusCode::OK, headers, body).into_response()
        }
        Ok(Err(e)) => {
            // Handle errors from BundleManager
            if is_not_found_error(&e) {
                not_found("Bundle not found").into_response()
            } else {
                internal_error(&e.to_string()).into_response()
            }
        }
        Err(e) => task_join_error(e).into_response(),
    }
}

pub async fn handle_bundle_jsonl(
    State(state): State<ServerState>,
    Path(number): Path<u32>,
) -> impl IntoResponse {
    // For streaming decompressed data, read in spawn_blocking and stream chunks
    // TODO: Implement true async streaming when tokio-util supports it better
    match tokio::task::spawn_blocking({
        let manager = Arc::clone(&state.manager);
        move || {
            let mut reader = manager.stream_bundle_decompressed(number)?;
            use std::io::Read;
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf)?;
            Ok::<Vec<u8>, anyhow::Error>(buf)
        }
    })
    .await
    {
        Ok(Ok(data)) => {
            let filename = constants::bundle_filename(number).replace(".zst", "");
            let headers = bundle_download_headers("application/x-ndjson", &filename);

            (StatusCode::OK, headers, data).into_response()
        }
        Ok(Err(e)) => {
            if is_not_found_error(&e) {
                not_found("Bundle not found").into_response()
            } else {
                internal_error(&e.to_string()).into_response()
            }
        }
        Err(_) => internal_error("Task join error").into_response(),
    }
}

pub async fn handle_operation(
    State(state): State<ServerState>,
    Path(pointer): Path<String>,
) -> impl IntoResponse {
    // Parse pointer: "bundle:position" or global position
    let (bundle_num, position) = match parse_operation_pointer(&pointer) {
        Ok((b, p)) => (b, p),
        Err(e) => return bad_request(&e.to_string()).into_response(),
    };

    if position >= constants::BUNDLE_SIZE {
        return bad_request("Position must be 0-9999").into_response();
    }

    let total_start = Instant::now();
    let load_start = Instant::now();

    // get_operation_raw performs blocking file I/O, so we need to use spawn_blocking
    let json_result = tokio::task::spawn_blocking({
        let manager = Arc::clone(&state.manager);
        move || manager.get_operation_raw(bundle_num, position)
    })
    .await;

    match json_result {
        Ok(Ok(json)) => {
            let load_duration = load_start.elapsed();
            let total_duration = total_start.elapsed();

            let global_pos = ((bundle_num - 1) as u64 * constants::BUNDLE_SIZE as u64) + position as u64;

            let mut headers = HeaderMap::new();
            headers.insert("X-Bundle-Number", HeaderValue::from(bundle_num));
            headers.insert("X-Position", HeaderValue::from(position));
            headers.insert(
                "X-Global-Position",
                HeaderValue::from_str(&global_pos.to_string()).unwrap(),
            );
            headers.insert(
                "X-Pointer",
                HeaderValue::from_str(&format!("{}:{}", bundle_num, position)).unwrap(),
            );
            headers.insert(
                "X-Load-Time-Ms",
                HeaderValue::from_str(&format!("{:.3}", load_duration.as_secs_f64() * 1000.0))
                    .unwrap(),
            );
            headers.insert(
                "X-Total-Time-Ms",
                HeaderValue::from_str(&format!("{:.3}", total_duration.as_secs_f64() * 1000.0))
                    .unwrap(),
            );
            headers.insert(
                "Cache-Control",
                HeaderValue::from_static("public, max-age=31536000, immutable"),
            );
            headers.insert("Content-Type", HeaderValue::from_static("application/json"));

            (StatusCode::OK, headers, json).into_response()
        }
        Ok(Err(e)) => {
            if is_not_found_error(&e) {
                not_found("Operation not found").into_response()
            } else {
                internal_error(&e.to_string()).into_response()
            }
        }
        Err(e) => task_join_error(e).into_response(),
    }
}

