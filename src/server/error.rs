// Error handling utilities and response helpers

use axum::{
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::json;

/// Helper to create a JSON error response
pub fn json_error(status: StatusCode, message: &str) -> impl IntoResponse {
    (status, axum::Json(json!({"error": message})))
}

/// Helper for "not found" errors
pub fn not_found(message: &str) -> impl IntoResponse {
    json_error(StatusCode::NOT_FOUND, message)
}

/// Helper for internal server errors
pub fn internal_error(message: &str) -> impl IntoResponse {
    json_error(StatusCode::INTERNAL_SERVER_ERROR, message)
}

/// Helper for task join errors
pub fn task_join_error(e: impl std::fmt::Display) -> impl IntoResponse {
    let msg = format!("Task join error: {}", e);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        axum::Json(json!({"error": msg})),
    )
}

/// Helper for bad request errors
pub fn bad_request(message: &str) -> impl IntoResponse {
    json_error(StatusCode::BAD_REQUEST, message)
}

/// Helper for gone (410) errors
pub fn gone(message: &str) -> impl IntoResponse {
    json_error(StatusCode::GONE, message)
}

/// Check if an error indicates "not found"
pub fn is_not_found_error(e: &anyhow::Error) -> bool {
    let msg = e.to_string();
    msg.contains("not found") || msg.contains("not in index")
}

