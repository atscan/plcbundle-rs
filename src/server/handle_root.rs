// Root page handler

use crate::constants;
use crate::format::{format_number, format_std_duration_verbose};
use crate::server::ServerState;
use crate::server::utils::extract_base_url;
use axum::{
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, Uri},
    response::IntoResponse,
};

pub async fn handle_root(
    State(state): State<ServerState>,
    uri: Uri,
    headers: HeaderMap,
) -> impl IntoResponse {
    let index = state.manager.get_index();
    let bundle_count = index.bundles.len();
    let origin = state.manager.get_plc_origin();
    let uptime = state.start_time.elapsed();
    let mempool_stats_opt = if state.config.sync_mode {
        state.manager.get_mempool_stats().ok()
    } else {
        None
    };

    let mut response = String::new();

    // ASCII art banner
    response.push('\n');
    response.push_str(&crate::server::get_ascii_art_banner(&state.config.version));
    response.push('\n');
    response.push_str(&format!("  {} server\n\n", constants::BINARY_NAME));
    response.push_str("*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*\n");
    response.push_str("| ⚠️ Preview Version – Do Not Use In Production!                 |\n");
    response.push_str("*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*\n");
    response.push_str("| This project and plcbundle specification is currently          |\n");
    response.push_str("| unstable and under heavy development. Things can break at      |\n");
    response.push_str("| any time. Do not use this for production systems.              |\n");
    response.push_str("| Please wait for the 1.0 release.                               |\n");
    response.push_str("|________________________________________________________________|\n");
    response.push('\n');
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
            response.push_str(&format!(
                "  Last bundle:   {} ({})\n",
                last_bundle,
                last_meta.end_time.split('T').next().unwrap_or("")
            ));
        }

        response.push_str(&format!(
            "  Range:         {} - {}\n",
            first_bundle, last_bundle
        ));
        response.push_str(&format!(
            "  Total size:    {:.2} MB\n",
            total_size as f64 / (1000.0 * 1000.0)
        ));
        response.push_str(&format!(
            "  Uncompressed:  {:.2} MB ({:.2}x)\n",
            total_uncompressed as f64 / (1000.0 * 1000.0),
            total_uncompressed as f64 / total_size as f64
        ));

        if let Some(first_meta) = index.get_bundle(first_bundle) {
            response.push_str(&format!("\n  Root: {}\n", first_meta.hash));
        }
        if let Some(last_meta) = index.get_bundle(last_bundle) {
            response.push_str(&format!("  Head: {}\n", last_meta.hash));
        }
    }

    if let Some(mempool_stats) = mempool_stats_opt.as_ref() {
        response.push_str("\nMempool\n");
        response.push_str("━━━━━━━\n");
        response.push_str(&format!(
            "  Target bundle:     {}\n",
            mempool_stats.target_bundle
        ));
        response.push_str(&format!(
            "  Operations:        {} / {}\n",
            mempool_stats.count,
            constants::BUNDLE_SIZE
        ));

        if mempool_stats.count > 0 {
            let progress = (mempool_stats.count as f64 / constants::BUNDLE_SIZE as f64) * 100.0;
            response.push_str(&format!("  Progress:          {:.1}%\n", progress));

            let bar_width = 50;
            let filled = ((bar_width as f64)
                * (mempool_stats.count as f64 / constants::BUNDLE_SIZE as f64))
                as usize;
            let bar =
                "█".repeat(filled.min(bar_width)) + &"░".repeat(bar_width.saturating_sub(filled));
            response.push_str(&format!("  [{}]\n", bar));

            if let Some(first_time) = mempool_stats.first_time {
                response.push_str(&format!(
                    "  First op:          {}\n",
                    first_time.format("%Y-%m-%d %H:%M:%S")
                ));
            }
            if let Some(last_time) = mempool_stats.last_time {
                response.push_str(&format!(
                    "  Last op:           {}\n",
                    last_time.format("%Y-%m-%d %H:%M:%S")
                ));
            }
        } else {
            response.push_str("  (empty)\n");
        }
    }

    if state.config.enable_resolver {
        response.push_str("\nDID Resolver\n");
        response.push_str("━━━━━━━━━━━━\n");
        response.push_str("  Status:  enabled\n");

        let did_stats = state.manager.get_did_index_stats();
        if did_stats
            .get("exists")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let indexed_dids = did_stats
                .get("total_dids")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as u64;
            let mempool_dids = mempool_stats_opt
                .as_ref()
                .and_then(|s| s.did_count)
                .unwrap_or(0) as u64;

            let total_dids = indexed_dids + mempool_dids;
            response.push_str(&format!(
                "  DIDs:    {} (Bundles {} + Mempool {})\n",
                format_number(total_dids),
                format_number(indexed_dids),
                format_number(mempool_dids)
            ));
        }
        response.push('\n');
    }

    response.push_str("Server Stats\n");
    response.push_str("━━━━━━━━━━━━\n");
    response.push_str(&format!(
        "  Version:           v{} (rust)\n",
        state.config.version
    ));
    response.push_str(&format!(
        "  Sync mode:         {}\n",
        state.config.sync_mode
    ));
    response.push_str(&format!(
        "  WebSocket:         {}\n",
        state.config.enable_websocket
    ));
    if let Some(handle_resolver) = state.manager.get_handle_resolver_base_url() {
        response.push_str(&format!("  Handle Resolver:   {}\n", handle_resolver));
    } else {
        response.push_str("  Handle Resolver:   (not configured)\n");
    }
    response.push_str(&format!(
        "  Uptime:            {}\n",
        format_std_duration_verbose(uptime)
    ));

    // Get base URL from request
    let base_url = extract_base_url(&headers, &uri);
    response.push_str("\n\nAPI Endpoints\n");
    response.push_str("━━━━━━━━━━━━━\n");
    response.push_str("  GET  /                    This info page\n");
    response.push_str("  GET  /index.json          Full bundle index\n");
    response.push_str("  GET  /bundle/:number      Bundle metadata (JSON)\n");
    response.push_str("  GET  /data/:number        Raw bundle (zstd compressed)\n");
    response.push_str("  GET  /jsonl/:number       Decompressed JSONL stream\n");
    response.push_str("  GET  /op/:cursor          Get single operation\n");
    response.push_str("  GET  /status              Server status\n");
    response.push_str("  GET  /mempool             Mempool operations (JSONL)\n");

    if state.config.enable_websocket {
        response.push_str("\nWebSocket Endpoints\n");
        response.push_str("━━━━━━━━━━━━━━━━━━━━━━━━\n");
        response.push_str("  WS   /ws                      Live stream (new operations only)\n");
        response.push_str("  WS   /ws?cursor=0             Stream all from beginning\n");
        response.push_str("  WS   /ws?cursor=N             Stream from cursor N\n\n");
    }

    if state.config.enable_resolver {
        response.push_str("\nDID Resolution\n");
        response.push_str("━━━━━━━━━━━━━━\n");
        response.push_str("  GET  /:did                    DID Document (W3C format)\n");
        response.push_str("  GET  /:did/data               PLC State (raw format)\n");
        response.push_str("  GET  /:did/log/audit          Operation history\n");
        response.push_str("  GET  /random                  Random DID sample (JSON)\n");
    }

    response.push_str("\nCursor Format\n");
    response.push_str("━━━━━━━━━━━━━\n");
    response.push_str("  Global record number: ((bundle - 1) × 10,000) + position\n");
    response.push_str("  Example: global 0 = bundle 1, position 0\n");
    response.push_str("  Default: starts from latest (skips all historical data)\n");
    response.push_str("  Positions are 0-indexed (per bundle: 0..9,999)\n");
    response.push_str("  Example: global 10000 = bundle 2, position 0\n");

    let bundled_ops = crate::constants::total_operations_from_bundles(index.last_bundle);
    let mempool_ops = mempool_stats_opt
        .as_ref()
        .map(|s| s.count as u64)
        .unwrap_or(0);
    let current_latest = bundled_ops + mempool_ops;

    if mempool_ops > 0 {
        response.push_str(&format!(
            "  Current latest: {} ({} bundled + {} mempool)\n\n",
            format_number(current_latest),
            format_number(bundled_ops),
            format_number(mempool_ops)
        ));
    } else {
        response.push_str(&format!(
            "  Current latest: {} ({} bundled)\n\n",
            format_number(current_latest),
            format_number(bundled_ops)
        ));
    }

    response.push_str("\nExamples\n");
    response.push_str("━━━━━━━━\n");
    response.push_str(&format!("  curl {}/bundle/1\n", base_url));
    response.push_str(&format!(
        "  curl {}/data/42 -o 000042.jsonl.zst\n",
        base_url
    ));
    response.push_str(&format!("  curl {}/jsonl/1\n", base_url));
    response.push_str(&format!("  curl {}/op/0\n", base_url));
    response.push_str(&format!("  curl {}/random?count=10&seed=12345\n", base_url));

    if state.config.sync_mode {
        response.push_str(&format!("  curl {}/status\n", base_url));
        response.push_str(&format!("  curl {}/mempool\n", base_url));
    }

    if state.config.enable_websocket {
        let ws_url = if base_url.starts_with("http://") {
            base_url.replace("http://", "ws://")
        } else if base_url.starts_with("https://") {
            base_url.replace("https://", "wss://")
        } else {
            format!("ws://{}", base_url)
        };
        response.push_str(&format!("  websocat {}/ws\n", ws_url));
        response.push_str(&format!("  websocat '{}/ws?cursor=0'\n", ws_url));
    }

    response.push_str("\n────────────────────────────────────────────────────────────────\n");
    response.push_str("https://tangled.org/@atscan.net/plcbundle\n");

    let mut headers = HeaderMap::new();
    headers.insert(
        "Content-Type",
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    (StatusCode::OK, headers, response).into_response()
}
