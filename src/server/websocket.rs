// WebSocket handler for streaming operations

use crate::constants;
use crate::server::ServerState;
use axum::extract::ws::Message;
use axum::{
    body::Bytes,
    extract::{Query, State, ws::WebSocketUpgrade},
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use tokio::time::{Duration, interval};

#[derive(Deserialize)]
pub struct CursorQuery {
    pub cursor: Option<u64>,
}

pub async fn handle_websocket(
    State(state): State<ServerState>,
    ws: WebSocketUpgrade,
    Query(params): Query<CursorQuery>,
) -> Response {
    let start_cursor = params
        .cursor
        .unwrap_or_else(|| state.manager.get_current_cursor());
    ws.on_upgrade(move |socket| handle_websocket_connection(socket, state, start_cursor))
}

async fn handle_websocket_connection(
    socket: axum::extract::ws::WebSocket,
    state: ServerState,
    start_cursor: u64,
) {
    let (mut sender, mut receiver) = socket.split();

    // Spawn task to handle incoming messages (for close/pong)
    let receiver_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(axum::extract::ws::Message::Close(_))
                | Ok(axum::extract::ws::Message::Pong(_)) => {
                    // Normal close or pong, continue
                }
                Err(e) => {
                    eprintln!("WebSocket receive error: {}", e);
                    break;
                }
                _ => {
                    // Ignore other messages
                }
            }
        }
    });

    // Stream operations (this will handle pings internally)
    let stream_result = stream_live_operations(state, start_cursor, &mut sender).await;

    // Close receiver task
    receiver_task.abort();

    if let Err(e) = stream_result {
        eprintln!("WebSocket stream error: {}", e);
    }
}

async fn stream_live_operations(
    state: ServerState,
    start_cursor: u64,
    sender: &mut futures_util::stream::SplitSink<
        axum::extract::ws::WebSocket,
        axum::extract::ws::Message,
    >,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let index = state.manager.get_index();
    let bundles = &index.bundles;
    let mut current_record = start_cursor;

    // Stream existing bundles
    if !bundles.is_empty() {
        let start_bundle_idx = (start_cursor / constants::BUNDLE_SIZE as u64) as usize;
        let start_position = (start_cursor % constants::BUNDLE_SIZE as u64) as usize;

        if start_bundle_idx < bundles.len() {
            for i in start_bundle_idx..bundles.len() {
                let bundle_num = bundles[i].bundle_number;
                let skip_until = if i == start_bundle_idx {
                    start_position
                } else {
                    0
                };

                let streamed =
                    stream_bundle(&state.manager, bundle_num, skip_until, sender).await?;
                current_record += streamed as u64;
            }
        }
    }

    // Stream mempool operations
    let bundle_record_base = (bundles.len() as u64) * constants::BUNDLE_SIZE as u64;
    let mut last_seen_mempool_count = 0;

    stream_mempool(
        &state.manager,
        start_cursor,
        bundle_record_base,
        &mut current_record,
        &mut last_seen_mempool_count,
        sender,
    )
    .await?;

    // Poll for new bundles and mempool updates
    let mut ticker = interval(Duration::from_millis(500));
    let mut last_bundle_count = bundles.len();

    loop {
        ticker.tick().await;

        // Check for new bundles
        let index = state.manager.get_index();
        let bundles = &index.bundles;

        if bundles.len() > last_bundle_count {
            let new_bundle_count = bundles.len() - last_bundle_count;
            current_record += (new_bundle_count as u64) * constants::BUNDLE_SIZE as u64;
            last_bundle_count = bundles.len();
            last_seen_mempool_count = 0;
        }

        // Stream new mempool operations
        stream_mempool(
            &state.manager,
            start_cursor,
            bundle_record_base,
            &mut current_record,
            &mut last_seen_mempool_count,
            sender,
        )
        .await?;

        // Send ping
        if let Err(e) = sender.send(Message::Ping(Bytes::new())).await {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                format!("WebSocket ping failed: {}", e),
            )));
        }
    }
}

async fn stream_bundle(
    manager: &Arc<crate::manager::BundleManager>,
    bundle_num: u32,
    skip_until: usize,
    sender: &mut futures_util::stream::SplitSink<
        axum::extract::ws::WebSocket,
        axum::extract::ws::Message,
    >,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    // Get decompressed bundle reader
    let reader = match manager.stream_bundle_decompressed(bundle_num) {
        Ok(r) => r,
        Err(_) => return Ok(0), // Bundle not found, skip
    };

    // Read bundle in blocking task
    let lines = tokio::task::spawn_blocking(move || {
        use std::io::{BufRead, BufReader};
        let mut buf_reader = BufReader::new(reader);
        let mut lines = Vec::new();
        let mut line = String::new();
        let mut position = 0;

        while buf_reader.read_line(&mut line).unwrap_or(0) > 0 {
            if position >= skip_until && !line.trim().is_empty() {
                lines.push(line.clone());
            }
            line.clear();
            position += 1;
        }
        lines
    })
    .await?;

    // Send lines
    let mut streamed = 0;
    for line in lines.iter() {
        if let Err(e) = sender.send(Message::Text(line.clone().into())).await {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                format!("WebSocket write error: {}", e),
            )));
        }
        streamed += 1;

        // Send ping every 1000 operations
        if streamed % 1000 == 0 {
            if sender.send(Message::Ping(Bytes::new())).await.is_err() {
                break;
            }
        }
    }

    Ok(streamed)
}

async fn stream_mempool(
    manager: &Arc<crate::manager::BundleManager>,
    start_cursor: u64,
    bundle_record_base: u64,
    current_record: &mut u64,
    last_seen_count: &mut usize,
    sender: &mut futures_util::stream::SplitSink<
        axum::extract::ws::WebSocket,
        axum::extract::ws::Message,
    >,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mempool_ops = match manager.get_mempool_operations() {
        Ok(ops) => ops,
        Err(_) => return Ok(()), // No mempool or error
    };

    if mempool_ops.len() <= *last_seen_count {
        return Ok(());
    }

    for i in *last_seen_count..mempool_ops.len() {
        let record_num = bundle_record_base + i as u64;
        if record_num < start_cursor {
            continue;
        }

        // Send operation as JSON
        let json = match sonic_rs::to_string(&mempool_ops[i]) {
            Ok(j) => j,
            Err(_) => continue, // Skip invalid operations
        };

        if let Err(e) = sender.send(Message::Text(json.into())).await {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                format!("WebSocket write error: {}", e),
            )));
        }

        *current_record += 1;
    }

    *last_seen_count = mempool_ops.len();
    Ok(())
}
