use crate::server::ServerState;
use crate::server::error::{bad_request, internal_error, task_join_error};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Deserialize)]
pub struct RandomQuery {
    pub count: Option<usize>,
    pub seed: Option<u64>,
}

pub async fn handle_random_dids(
    State(state): State<ServerState>,
    Query(params): Query<RandomQuery>,
) -> impl IntoResponse {
    let count = params.count.unwrap_or(10);
    if count > 1000 {
        return bad_request("count must be <= 1000").into_response();
    }
    let effective_seed = params.seed.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    });
    if count == 0 {
        return (
            StatusCode::OK,
            axum::Json(json!({ "dids": [], "count": 0, "seed": effective_seed })),
        )
            .into_response();
    }

    let dids = match tokio::task::spawn_blocking({
        let manager = Arc::clone(&state.manager);
        move || manager.sample_random_dids(count, Some(effective_seed))
    })
    .await
    {
        Ok(Ok(list)) => list,
        Ok(Err(e)) => return internal_error(&e.to_string()).into_response(),
        Err(e) => return task_join_error(e).into_response(),
    };

    (
        StatusCode::OK,
        axum::Json(json!({
            "dids": dids,
            "count": count,
            "seed": effective_seed
        })),
    )
        .into_response()
}
