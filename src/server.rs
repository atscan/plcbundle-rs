// HTTP Server for serving bundle data
// This module provides an HTTP API for accessing bundle data, similar to the Go implementation

#[cfg(feature = "server")]
mod server_impl {
    use crate::manager::BundleManager;
    use crate::resolver::build_did_state;
    use anyhow::Result;
    use axum::{
        body::Body,
        extract::{Path, State},
        http::{HeaderMap, HeaderValue, StatusCode, Uri},
        response::IntoResponse,
        routing::get,
        Router,
    };
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::fs::File;
    use tokio_util::io::ReaderStream;
    use tower::ServiceBuilder;
    use tower_http::cors::CorsLayer;

    pub struct Server {
        manager: Arc<BundleManager>,
        config: ServerConfig,
        start_time: Instant,
    }

    #[derive(Clone)]
    pub struct ServerConfig {
        pub sync_mode: bool,
        pub sync_interval_seconds: u64,
        pub enable_websocket: bool,
        pub enable_resolver: bool,
        pub version: String,
    }

    impl Server {
        pub fn new(manager: Arc<BundleManager>, config: ServerConfig) -> Self {
            Self {
                manager,
                config,
                start_time: Instant::now(),
            }
        }

        pub fn router(&self) -> Router {
            let manager = Arc::clone(&self.manager);
            let config = self.config.clone();

            let mut router = Router::new()
                .route("/", get(handle_root))
                .route("/index.json", get(handle_index_json))
                .route("/bundle/:number", get(handle_bundle))
                .route("/data/:number", get(handle_bundle_data))
                .route("/jsonl/:number", get(handle_bundle_jsonl))
                .route("/op/:pointer", get(handle_operation))
                .route("/status", get(handle_status))
                .route("/mempool", get(handle_mempool))
                .route("/debug/memory", get(handle_debug_memory))
                .route("/debug/didindex", get(handle_debug_didindex))
                .route("/debug/resolver", get(handle_debug_resolver));

            // DID resolution routes (if enabled) - must be last to catch all other paths
            // Use a catch-all route that checks if resolver is enabled
            router = router
                .route("/*path", get(handle_did_routing_guard));

            router
                .layer(
                    ServiceBuilder::new()
                        .layer(CorsLayer::permissive())
                )
                .with_state(ServerState { 
                    manager, 
                    config,
                    start_time: self.start_time,
                })
        }
    }

    #[derive(Clone)]
    struct ServerState {
        manager: Arc<BundleManager>,
        config: ServerConfig,
        start_time: Instant,
    }

    async fn handle_root(
        State(state): State<ServerState>,
        uri: Uri,
    ) -> impl IntoResponse {
        let index = state.manager.get_index();
        let bundle_count = index.bundles.len();
        let origin = state.manager.get_plc_origin();
        let uptime = state.start_time.elapsed();

        let mut response = String::new();
        
        // ASCII art (simplified version)
        response.push_str("\n                        plcbundle server\n\n");
        response.push_str("*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*\n");
        response.push_str("| ⚠️ Preview Version – Do Not Use In Production!                 |\n");
        response.push_str("*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*\n");
        response.push_str("| This project and plcbundle specification is currently          |\n");
        response.push_str("| unstable and under heavy development. Things can break at      |\n");
        response.push_str("| any time. Do not use this for production systems.              |\n");
        response.push_str("| Please wait for the 1.0 release.                               |\n");
        response.push_str("|________________________________________________________________|\n\n");

        response.push_str("plcbundle server\n\n");
        response.push_str("What is PLC Bundle?\n");
        response.push_str("━━━━━━━━━━━━━━━━━━━━\n");
        response.push_str("plcbundle archives AT Protocol's DID PLC Directory operations into\n");
        response.push_str("immutable, cryptographically-chained bundles of 10,000 operations.\n\n");
        response.push_str("More info: https://tangled.org/@atscan.net/plcbundle\n\n");

        if bundle_count > 0 {
            let first_bundle = index.bundles.first().map(|b| b.bundle_number).unwrap_or(0);
            let last_bundle = index.last_bundle;
            let total_size: u64 = index.bundles.iter().map(|b| b.compressed_size).sum();
            let total_uncompressed: u64 = index.bundles.iter().map(|b| b.uncompressed_size).sum();

            response.push_str("Bundles\n");
            response.push_str("━━━━━━━\n");
            response.push_str(&format!("  Origin:        {}\n", origin));
            response.push_str(&format!("  Bundle count:  {}\n", bundle_count));

            if let Some(last_meta) = index.get_bundle(last_bundle) {
                response.push_str(&format!("  Last bundle:   {} ({})\n", 
                    last_bundle, 
                    last_meta.end_time.split('T').next().unwrap_or("")));
            }

            response.push_str(&format!("  Range:         {:06} - {:06}\n", first_bundle, last_bundle));
            response.push_str(&format!("  Total size:    {:.2} MB\n", total_size as f64 / (1000.0 * 1000.0)));
            response.push_str(&format!("  Uncompressed:  {:.2} MB ({:.2}x)\n",
                total_uncompressed as f64 / (1000.0 * 1000.0),
                total_uncompressed as f64 / total_size as f64));

            if let Some(first_meta) = index.get_bundle(first_bundle) {
                response.push_str(&format!("\n  Root: {}\n", first_meta.hash));
            }
            if let Some(last_meta) = index.get_bundle(last_bundle) {
                response.push_str(&format!("  Head: {}\n", last_meta.hash));
            }
        }

        if state.config.sync_mode {
            if let Ok(mempool_stats) = state.manager.get_mempool_stats() {
                response.push_str("\nMempool\n");
                response.push_str("━━━━━━━\n");
                response.push_str(&format!("  Target bundle:     {}\n", mempool_stats.target_bundle));
                response.push_str(&format!("  Operations:        {} / 10000\n", mempool_stats.count));

                if mempool_stats.count > 0 {
                    let progress = (mempool_stats.count as f64 / 10000.0) * 100.0;
                    response.push_str(&format!("  Progress:          {:.1}%\n", progress));

                    let bar_width = 50;
                    let filled = ((bar_width as f64) * (mempool_stats.count as f64 / 10000.0)) as usize;
                    let bar = "█".repeat(filled.min(bar_width)) + &"░".repeat(bar_width.saturating_sub(filled));
                    response.push_str(&format!("  [{}]\n", bar));

                    if let Some(first_time) = mempool_stats.first_time {
                        response.push_str(&format!("  First op:          {}\n", 
                            first_time.format("%Y-%m-%d %H:%M:%S")));
                    }
                    if let Some(last_time) = mempool_stats.last_time {
                        response.push_str(&format!("  Last op:           {}\n", 
                            last_time.format("%Y-%m-%d %H:%M:%S")));
                    }
                } else {
                    response.push_str("  (empty)\n");
                }
            }
        }

        if state.config.enable_resolver {
            response.push_str("\nResolver\n");
            response.push_str("━━━━━━━━\n");
            response.push_str("  Status:        enabled\n");
            // TODO: Add DID index stats
            response.push_str("\n");
        }

        response.push_str("Server Stats\n");
        response.push_str("━━━━━━━━━━━━\n");
        response.push_str(&format!("  Version:           {}\n", state.config.version));
        response.push_str(&format!("  Sync mode:         {}\n", state.config.sync_mode));
        response.push_str(&format!("  WebSocket:         {}\n", state.config.enable_websocket));
        if let Some(handle_resolver) = state.manager.get_handle_resolver_base_url() {
            response.push_str(&format!("  Handle Resolver:   {}\n", handle_resolver));
        } else {
            response.push_str("  Handle Resolver:   (not configured)\n");
        }
        response.push_str(&format!("  Uptime:            {:?}\n", uptime));

        let base_url = format!("http://{}", uri.authority().unwrap_or(&uri.path().parse().unwrap()));
        response.push_str("\n\nAPI Endpoints\n");
        response.push_str("━━━━━━━━━━━━━\n");
        response.push_str("  GET  /                    This info page\n");
        response.push_str("  GET  /index.json          Full bundle index\n");
        response.push_str("  GET  /bundle/:number      Bundle metadata (JSON)\n");
        response.push_str("  GET  /data/:number        Raw bundle (zstd compressed)\n");
        response.push_str("  GET  /jsonl/:number       Decompressed JSONL stream\n");
        response.push_str("  GET  /op/:pointer         Get single operation\n");
        response.push_str("  GET  /status              Server status\n");
        response.push_str("  GET  /mempool             Mempool operations (JSONL)\n");

        if state.config.enable_resolver {
            response.push_str("\nDID Resolution\n");
            response.push_str("━━━━━━━━━━━━━━\n");
            response.push_str("  GET  /:did                    DID Document (W3C format)\n");
            response.push_str("  GET  /:did/data               PLC State (raw format)\n");
            response.push_str("  GET  /:did/log/audit          Operation history\n");
        }

        response.push_str("\nExamples\n");
        response.push_str("━━━━━━━━\n");
        response.push_str(&format!("  curl {}/bundle/1\n", base_url));
        response.push_str(&format!("  curl {}/data/42 -o 000042.jsonl.zst\n", base_url));
        response.push_str(&format!("  curl {}/jsonl/1\n", base_url));
        response.push_str(&format!("  curl {}/op/0\n", base_url));

        if state.config.sync_mode {
            response.push_str(&format!("  curl {}/status\n", base_url));
            response.push_str(&format!("  curl {}/mempool\n", base_url));
        }

        response.push_str("\n────────────────────────────────────────────────────────────────\n");
        response.push_str("https://tangled.org/@atscan.net/plcbundle\n");

        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", HeaderValue::from_static("text/plain; charset=utf-8"));
        (StatusCode::OK, headers, response).into_response()
    }

    async fn handle_index_json(
        State(state): State<ServerState>,
    ) -> impl IntoResponse {
        let index = state.manager.get_index();
        (StatusCode::OK, axum::Json(index)).into_response()
    }

    async fn handle_bundle(
        State(state): State<ServerState>,
        Path(number): Path<u32>,
    ) -> impl IntoResponse {
        match state.manager.get_bundle_metadata(number) {
            Ok(Some(meta)) => (StatusCode::OK, axum::Json(meta)).into_response(),
            Ok(None) => (
                StatusCode::NOT_FOUND,
                axum::Json(json!({"error": "Bundle not found"})),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": e.to_string()})),
            )
                .into_response(),
        }
    }

    async fn handle_bundle_data(
        State(state): State<ServerState>,
        Path(number): Path<u32>,
    ) -> impl IntoResponse {
        let bundle_path = state.manager.directory().join(format!("{:06}.jsonl.zst", number));
        
        match File::open(&bundle_path).await {
            Ok(file) => {
                let stream = ReaderStream::new(file);
                let body = Body::from_stream(stream);
                
                let mut headers = HeaderMap::new();
                headers.insert("Content-Type", HeaderValue::from_static("application/zstd"));
                headers.insert("Content-Disposition", 
                    HeaderValue::from_str(&format!("attachment; filename={:06}.jsonl.zst", number)).unwrap());
                
                (StatusCode::OK, headers, body).into_response()
            }
            Err(_) => {
                // Check if bundle exists in index
                if state.manager.get_bundle_metadata(number).ok().flatten().is_none() {
                    (
                        StatusCode::NOT_FOUND,
                        axum::Json(json!({"error": "Bundle not found"})),
                    )
                        .into_response()
                } else {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        axum::Json(json!({"error": "Failed to read bundle"})),
                    )
                        .into_response()
                }
            }
        }
    }

    async fn handle_bundle_jsonl(
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
        }).await {
            Ok(Ok(data)) => {
                let mut headers = HeaderMap::new();
                headers.insert("Content-Type", HeaderValue::from_static("application/x-ndjson"));
                headers.insert("Content-Disposition", 
                    HeaderValue::from_str(&format!("attachment; filename={:06}.jsonl", number)).unwrap());
                
                (StatusCode::OK, headers, data).into_response()
            }
            Ok(Err(e)) => {
                if e.to_string().contains("not in index") || e.to_string().contains("not found") {
                    (
                        StatusCode::NOT_FOUND,
                        axum::Json(json!({"error": "Bundle not found"})),
                    )
                        .into_response()
                } else {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        axum::Json(json!({"error": e.to_string()})),
                    )
                        .into_response()
                }
            }
            Err(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": "Task join error"})),
            )
                .into_response(),
        }
    }

    async fn handle_operation(
        State(state): State<ServerState>,
        Path(pointer): Path<String>,
    ) -> impl IntoResponse {
        // Parse pointer: "bundle:position" or global position
        let (bundle_num, position) = match parse_operation_pointer(&pointer) {
            Ok((b, p)) => (b, p),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        };

        if position >= 10000 {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": format!("Position must be 0-9999")})),
            )
                .into_response();
        }

        let total_start = Instant::now();
        let load_start = Instant::now();

        match state.manager.get_operation_raw(bundle_num, position) {
            Ok(json) => {
                let load_duration = load_start.elapsed();
                let total_duration = total_start.elapsed();

                let global_pos = (bundle_num as u64 * 10000) + position as u64;

                let mut headers = HeaderMap::new();
                headers.insert("X-Bundle-Number", HeaderValue::from(bundle_num));
                headers.insert("X-Position", HeaderValue::from(position));
                headers.insert("X-Global-Position", HeaderValue::from_str(&global_pos.to_string()).unwrap());
                headers.insert("X-Pointer", HeaderValue::from_str(&format!("{}:{}", bundle_num, position)).unwrap());
                headers.insert("X-Load-Time-Ms", HeaderValue::from_str(&format!("{:.3}", load_duration.as_secs_f64() * 1000.0)).unwrap());
                headers.insert("X-Total-Time-Ms", HeaderValue::from_str(&format!("{:.3}", total_duration.as_secs_f64() * 1000.0)).unwrap());
                headers.insert("Cache-Control", HeaderValue::from_static("public, max-age=31536000, immutable"));
                headers.insert("Content-Type", HeaderValue::from_static("application/json"));

                (StatusCode::OK, headers, json).into_response()
            }
            Err(e) => {
                if e.to_string().contains("not in index") || e.to_string().contains("not found") {
                    (
                        StatusCode::NOT_FOUND,
                        axum::Json(json!({"error": "Operation not found"})),
                    )
                        .into_response()
                } else {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        axum::Json(json!({"error": e.to_string()})),
                    )
                        .into_response()
                }
            }
        }
    }

    async fn handle_status(
        State(state): State<ServerState>,
    ) -> impl IntoResponse {
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

            let total_ops = index.bundles.len() * 10000;
            response["bundles"]["total_operations"] = json!(total_ops);
        }

        if state.config.sync_mode {
            if let Ok(mempool_stats) = state.manager.get_mempool_stats() {
                response["mempool"] = json!({
                    "count": mempool_stats.count,
                    "target_bundle": mempool_stats.target_bundle,
                    "can_create_bundle": mempool_stats.count >= 10000,
                    "progress_percent": (mempool_stats.count as f64 / 10000.0) * 100.0,
                    "bundle_size": 10000,
                    "operations_needed": 10000 - mempool_stats.count,
                });
            }
        }

        // DID Index stats
        // TODO: Add DID index stats when available

        // Resolver stats
        if state.config.enable_resolver {
            let resolver_stats = state.manager.get_resolver_stats();
            if !resolver_stats.is_empty() {
                response["resolver"] = json!(resolver_stats);
            }
        }

        (StatusCode::OK, axum::Json(response)).into_response()
    }

    async fn handle_mempool(
        State(state): State<ServerState>,
    ) -> impl IntoResponse {
        if !state.config.sync_mode {
            return (
                StatusCode::NOT_FOUND,
                axum::Json(json!({"error": "Mempool only available in sync mode"})),
            )
                .into_response();
        }

        match state.manager.get_mempool_operations() {
            Ok(ops) => {
                // Convert operations to JSONL
                let mut jsonl = Vec::new();
                for op in ops {
                    if let Ok(json) = serde_json::to_string(&op) {
                        jsonl.extend_from_slice(json.as_bytes());
                        jsonl.push(b'\n');
                    }
                }
                let mut headers = HeaderMap::new();
                headers.insert("Content-Type", HeaderValue::from_static("application/x-ndjson"));
                (StatusCode::OK, headers, jsonl).into_response()
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": e.to_string()})),
            )
                .into_response(),
        }
    }

    async fn handle_debug_memory(
        State(_state): State<ServerState>,
    ) -> impl IntoResponse {
        // TODO: Implement memory stats using sysinfo or similar
        let response = format!(
            "Memory Stats:\n  (Not implemented yet)\n\nDID Index:\n  (Not implemented yet)\n"
        );
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", HeaderValue::from_static("text/plain"));
        (StatusCode::OK, headers, response)
    }

    async fn handle_debug_didindex(
        State(state): State<ServerState>,
    ) -> impl IntoResponse {
        // TODO: Get DID index stats from manager
        // For now, return placeholder
        (
            StatusCode::OK,
            axum::Json(json!({
                "enabled": state.config.enable_resolver,
                "exists": false,
                "message": "DID index stats not yet implemented"
            })),
        )
    }

    async fn handle_debug_resolver(
        State(state): State<ServerState>,
    ) -> impl IntoResponse {
        if !state.config.enable_resolver {
            return (
                StatusCode::NOT_FOUND,
                axum::Json(json!({"error": "Resolver not enabled"})),
            )
                .into_response();
        }

        let resolver_stats = state.manager.get_resolver_stats();
        (StatusCode::OK, axum::Json(json!(resolver_stats))).into_response()
    }

    async fn handle_did_routing_guard(
        State(state): State<ServerState>,
        uri: Uri,
    ) -> impl IntoResponse {
        // Only handle if resolver is enabled, otherwise return 404
        if !state.config.enable_resolver {
            return (StatusCode::NOT_FOUND, axum::Json(json!({"error": "not found"}))).into_response();
        }
        handle_did_routing(State(state), uri).await.into_response()
    }

    async fn handle_did_routing(
        State(state): State<ServerState>,
        uri: Uri,
    ) -> impl IntoResponse {
        let path = uri.path().trim_start_matches('/');
        
        // Ignore common browser files
        if is_common_browser_file(path) {
            return (StatusCode::NOT_FOUND, axum::Json(json!({"error": "not found"}))).into_response();
        }

        // Split path into parts
        let parts: Vec<&str> = path.split('/').collect();
        if parts.is_empty() {
            return (StatusCode::NOT_FOUND, axum::Json(json!({"error": "not found"}))).into_response();
        }

        let input = parts[0];

        // Quick validation
        if !is_valid_did_or_handle(input) {
            return (StatusCode::NOT_FOUND, axum::Json(json!({"error": "not found"}))).into_response();
        }

        // Route to appropriate handler
        if parts.len() == 1 {
            handle_did_document(State(state), input).await.into_response()
        } else if parts[1] == "data" {
            handle_did_data(State(state), input).await.into_response()
        } else if parts.len() == 3 && parts[1] == "log" && parts[2] == "audit" {
            handle_did_audit_log(State(state), input).await.into_response()
        } else {
            (StatusCode::NOT_FOUND, axum::Json(json!({"error": "not found"}))).into_response()
        }
    }

    async fn handle_did_document(
        State(state): State<ServerState>,
        input: &str,
    ) -> impl IntoResponse {
        // Resolve handle to DID
        let (did, handle_resolve_time) = match state.manager.resolve_handle_or_did(input) {
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
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        };

        let resolved_handle = if handle_resolve_time > 0 { Some(input.to_string()) } else { None };

        // Resolve DID
        let result = match state.manager.resolve_did_with_stats(&did) {
            Ok(r) => r,
            Err(e) => {
                if e.to_string().contains("deactivated") {
                    return (
                        StatusCode::GONE,
                        axum::Json(json!({"error": "DID has been deactivated"})),
                    )
                        .into_response();
                } else if e.to_string().contains("not found") {
                    return (
                        StatusCode::NOT_FOUND,
                        axum::Json(json!({"error": "DID not found"})),
                    )
                        .into_response();
                } else {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        axum::Json(json!({"error": e.to_string()})),
                    )
                        .into_response();
                }
            }
        };

        // Set headers
        let mut headers = HeaderMap::new();
        headers.insert("X-DID", HeaderValue::from_str(&did).unwrap());
        if let Some(handle) = resolved_handle {
            headers.insert("X-Handle-Resolved", HeaderValue::from_str(&handle).unwrap());
            headers.insert("X-Handle-Resolution-Time-Ms", 
                HeaderValue::from_str(&format!("{:.3}", handle_resolve_time as f64 / 1000.0)).unwrap());
            headers.insert("X-Request-Type", HeaderValue::from_static("handle"));
        } else {
            headers.insert("X-Request-Type", HeaderValue::from_static("did"));
        }
        headers.insert("X-Resolution-Source", HeaderValue::from_static("bundle"));
        headers.insert("X-Bundle-Number", HeaderValue::from(result.bundle_number));
        headers.insert("X-Bundle-Position", HeaderValue::from(result.position));
        headers.insert("X-Resolution-Time-Ms", 
            HeaderValue::from_str(&format!("{:.3}", result.total_time.as_secs_f64() * 1000.0)).unwrap());
        headers.insert("Content-Type", HeaderValue::from_static("application/did+ld+json"));
        headers.insert("Cache-Control", HeaderValue::from_static("public, max-age=300, stale-while-revalidate=600"));

        (StatusCode::OK, headers, axum::Json(result.document)).into_response()
    }

    async fn handle_did_data(
        State(state): State<ServerState>,
        input: &str,
    ) -> impl IntoResponse {
        // Resolve handle to DID
        let did = match state.manager.resolve_handle_or_did(input) {
            Ok((d, _)) => d,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        };

        // Get DID operations
        let operations = match state.manager.get_did_operations(&did) {
            Ok(ops) => ops,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        };

        if operations.is_empty() {
            return (
                StatusCode::NOT_FOUND,
                axum::Json(json!({"error": "DID not found"})),
            )
                .into_response();
        }

        // Build DID state
        match build_did_state(&did, &operations) {
            Ok(state) => (StatusCode::OK, axum::Json(state)).into_response(),
            Err(e) => {
                if e.to_string().contains("deactivated") {
                    (
                        StatusCode::GONE,
                        axum::Json(json!({"error": "DID has been deactivated"})),
                    )
                        .into_response()
                } else {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        axum::Json(json!({"error": e.to_string()})),
                    )
                        .into_response()
                }
            }
        }
    }

    async fn handle_did_audit_log(
        State(state): State<ServerState>,
        input: &str,
    ) -> impl IntoResponse {
        // Resolve handle to DID
        let did = match state.manager.resolve_handle_or_did(input) {
            Ok((d, _)) => d,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        };

        // Get DID operations
        let operations = match state.manager.get_did_operations(&did) {
            Ok(ops) => ops,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        };

        if operations.is_empty() {
            return (
                StatusCode::NOT_FOUND,
                axum::Json(json!({"error": "DID not found"})),
            )
                .into_response();
        }

        // Format audit log (just return operations for now)
        (StatusCode::OK, axum::Json(operations)).into_response()
    }

    fn is_common_browser_file(path: &str) -> bool {
        let common_files = ["favicon.ico", "robots.txt", "sitemap.xml", 
            "apple-touch-icon.png", ".well-known"];
        let common_extensions = [".ico", ".png", ".jpg", ".jpeg", ".gif", ".svg",
            ".css", ".js", ".woff", ".woff2", ".ttf", ".eot", ".xml", ".txt", ".html"];

        for file in &common_files {
            if path == *file || path.starts_with(file) {
                return true;
            }
        }

        for ext in &common_extensions {
            if path.ends_with(ext) {
                return true;
            }
        }

        false
    }

    fn is_valid_did_or_handle(input: &str) -> bool {
        if input.is_empty() {
            return false;
        }

        // If it's a DID
        if input.starts_with("did:") {
            // Only accept did:plc: method
            if !input.starts_with("did:plc:") {
                return false;
            }
            return true;
        }

        // Not a DID - validate as handle
        // Must have at least one dot
        if !input.contains('.') {
            return false;
        }

        // Must not have invalid characters
        for c in input.chars() {
            if !((c >= 'a' && c <= 'z') ||
                (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') ||
                c == '.' || c == '-') {
                return false;
            }
        }

        // Basic length check
        if input.len() > 253 {
            return false;
        }

        // Must not start or end with dot or hyphen
        if input.starts_with('.') || input.ends_with('.') ||
            input.starts_with('-') || input.ends_with('-') {
            return false;
        }

        true
    }

    fn parse_operation_pointer(pointer: &str) -> Result<(u32, usize)> {
        // Check if it's "bundle:position" format
        if let Some(colon_pos) = pointer.find(':') {
            let bundle_str = &pointer[..colon_pos];
            let pos_str = &pointer[colon_pos + 1..];

            let bundle_num: u32 = bundle_str.parse()
                .map_err(|_| anyhow::anyhow!("Invalid bundle number: {}", bundle_str))?;
            let position: usize = pos_str.parse()
                .map_err(|_| anyhow::anyhow!("Invalid position: {}", pos_str))?;

            if bundle_num < 1 {
                anyhow::bail!("Bundle number must be >= 1");
            }

            return Ok((bundle_num, position));
        }

        // Parse as global position
        let global_pos: u64 = pointer.parse()
            .map_err(|_| anyhow::anyhow!("Invalid position: must be number or 'bundle:position' format"))?;

        if global_pos < 10000 {
            // Small numbers are shorthand for bundle 1
            return Ok((1, global_pos as usize));
        }

        // Convert global position to bundle + position
        let bundle_num = (global_pos / 10000) as u32;
        let position = (global_pos % 10000) as usize;

        Ok((bundle_num.max(1), position))
    }
}

#[cfg(feature = "server")]
pub use server_impl::{Server, ServerConfig};
