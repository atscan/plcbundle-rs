// Server command - start HTTP server
use anyhow::{Context, Result};
use clap::{Args, ValueHint};
use std::path::PathBuf;
use tokio::time::Duration;

#[cfg(feature = "server")]
use super::progress::ProgressBar;
#[cfg(feature = "server")]
use super::utils;
#[cfg(feature = "server")]
use plcbundle::BundleManager;
#[cfg(feature = "server")]
use plcbundle::server::{Server, ServerConfig};
#[cfg(feature = "server")]
use plcbundle::BundleRuntime;
#[cfg(feature = "server")]
use std::sync::Arc;
#[cfg(feature = "server")]
use tokio::task::JoinSet;

fn parse_duration(s: &str) -> Result<Duration> {
    // Simple parser: "60s", "5m", "1h"
    let s = s.trim();
    if s.ends_with('s') {
        let secs: u64 = s[..s.len() - 1]
            .parse()
            .context("Invalid duration format")?;
        Ok(Duration::from_secs(secs))
    } else if s.ends_with('m') {
        let mins: u64 = s[..s.len() - 1]
            .parse()
            .context("Invalid duration format")?;
        Ok(Duration::from_secs(mins * 60))
    } else if s.ends_with('h') {
        let hours: u64 = s[..s.len() - 1]
            .parse()
            .context("Invalid duration format")?;
        Ok(Duration::from_secs(hours * 3600))
    } else {
        // Try parsing as seconds
        let secs: u64 = s.parse().context("Invalid duration format")?;
        Ok(Duration::from_secs(secs))
    }
}

#[derive(Args)]
#[command(
    about = "Start HTTP server",
    long_about = "Start an HTTP server to expose bundle data and repository functionality over
HTTP. The server provides RESTful endpoints for accessing bundles, operations,
DID documents, and repository metadata.

In standard mode, the server operates as a read-only archive, serving data from
existing bundles. Enable --sync to run in daemon mode, where the server continuously
fetches new bundles from the PLC directory in the background while serving requests.

Optional features include WebSocket streaming (--websocket) for real-time updates
and DID resolution endpoints (--resolver) that enable W3C DID document resolution
and handle-to-DID lookups. When resolver is enabled, the server automatically builds
or updates the DID index on startup if needed.

This is the primary way to expose your repository to other systems, applications,
or users over the network. The server is designed for production use with proper
error handling, graceful shutdown, and resource management.",
    help_template = crate::clap_help!(
        examples: "  # Start server on default port (8080)\n  \
                   {bin} server\n\n  \
                   # Custom host and port\n  \
                   {bin} server --host 0.0.0.0 --port 3000\n\n  \
                   # Server with sync mode (daemon)\n  \
                   {bin} server --sync\n\n  \
                   # Sync with custom interval\n  \
                   {bin} server --sync --interval 30s\n\n  \
                   # Enable WebSocket streaming\n  \
                   {bin} server --websocket\n\n  \
                   # Enable DID resolution endpoints\n  \
                   {bin} server --resolver\n\n  \
                   # Full-featured server\n  \
                   {bin} server --sync --websocket --resolver --port 8080"
    )
)]
pub struct ServerCommand {
    /// HTTP server port
    #[arg(long, default_value = "8080", help_heading = "Server Options")]
    pub port: u16,

    /// HTTP server host
    #[arg(long, default_value = "127.0.0.1", help_heading = "Server Options")]
    pub host: String,

    /// Enable sync mode (run as daemon, continuously fetch from PLC)
    #[arg(short, long, help_heading = "Sync Options")]
    pub sync: bool,

    /// PLC directory URL (for sync mode)
    #[arg(long, default_value = plcbundle::constants::DEFAULT_PLC_DIRECTORY_URL, help_heading = "Sync Options", value_hint = ValueHint::Url)]
    pub plc: String,

    /// Sync interval (how often to check for new bundles)
    #[arg(long, default_value = "60s", value_parser = parse_duration, help_heading = "Sync Options")]
    pub interval: Duration,

    /// Maximum bundles to fetch (0 = unlimited)
    #[arg(long, default_value = "0", help_heading = "Sync Options")]
    pub max_bundles: u32,

    /// Enable WebSocket endpoint for streaming
    #[arg(long, help_heading = "Feature Options")]
    pub websocket: bool,

    /// Enable DID resolution endpoints
    #[arg(long, help_heading = "Feature Options")]
    pub resolver: bool,

    /// Handle resolver URL (defaults to quickdid.smokesignal.tools if not provided)
    #[arg(long, help_heading = "Feature Options", value_hint = ValueHint::Url)]
    pub handle_resolver: Option<String>,

}

pub fn run(cmd: ServerCommand, dir: PathBuf, global_verbose: bool) -> Result<()> {
    #[cfg(not(feature = "server"))]
    {
        let _ = (cmd, dir, global_verbose); // Suppress unused warnings when server feature is disabled
        anyhow::bail!("Server feature is not enabled. Rebuild with --features server");
    }

    #[cfg(feature = "server")]
    {
        run_server(cmd, dir, global_verbose)
    }
}

#[cfg(feature = "server")]
fn run_server(cmd: ServerCommand, dir: PathBuf, global_verbose: bool) -> Result<()> {
    use tokio::runtime::Runtime;

    // Create tokio runtime for async operations
    let rt = Runtime::new().context("Failed to create tokio runtime")?;
    rt.block_on(run_server_async(cmd, dir, global_verbose))
}

#[cfg(feature = "server")]
async fn run_server_async(cmd: ServerCommand, dir: PathBuf, global_verbose: bool) -> Result<()> {
    use std::net::SocketAddr;

    // Initialize manager with handle resolver (external service for handle->DID conversion)
    // Always use default handle resolver URL if not explicitly provided
    // Note: DID resolver (in-app) is always available when --resolver is enabled
    use plcbundle::constants;

    let handle_resolver_url = if cmd.handle_resolver.is_none() {
        if global_verbose {
            log::debug!(
                "[HandleResolver] Using default handle resolver: {}",
                constants::DEFAULT_HANDLE_RESOLVER_URL
            );
        }
        Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string())
    } else {
        if global_verbose {
            log::debug!(
                "[HandleResolver] Using custom handle resolver: {}",
                cmd.handle_resolver.as_ref().unwrap()
            );
        }
        cmd.handle_resolver.clone()
    };
    let manager = if cmd.sync {
        // Sync mode can auto-init
        match BundleManager::with_handle_resolver(dir.clone(), handle_resolver_url.clone()) {
            Ok(mgr) => mgr,
            Err(_) => {
                // Repository doesn't exist, try to initialize
                BundleManager::init_repository(&dir, cmd.plc.clone(), false)
                    .context("Failed to initialize repository")?;
                // Try again after initialization
                BundleManager::with_handle_resolver(dir.clone(), handle_resolver_url.clone())
                    .context("Failed to open repository after initialization")?
            }
        }
    } else {
        // Read-only mode cannot auto-init
        BundleManager::with_handle_resolver(dir.clone(), handle_resolver_url).context(format!(
            "Repository not found. Use '{} init' first or run with --sync",
            plcbundle::constants::BINARY_NAME
        ))?
    };

    // Set verbose mode on manager
    let manager = manager.with_verbose(global_verbose);

    // Log handle resolver configuration (external service for handle->DID resolution)
    if global_verbose {
        if let Some(url) = manager.get_handle_resolver_base_url() {
            log::debug!("[HandleResolver] External handle resolver configured: {}", url);
        } else {
            log::debug!("[HandleResolver] External handle resolver not configured");
        }
    }

    // Build/verify DID index if DID resolver enabled (in-app resolver for DID->document resolution)
    if cmd.resolver {
        if global_verbose {
            log::debug!("[DIDResolver] Checking DID index status...");
        }

        let did_index_stats = manager.get_did_index_stats();
        let total_dids = did_index_stats
            .get("total_dids")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let total_entries = did_index_stats
            .get("total_entries")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        if global_verbose {
            log::debug!(
                "[DIDResolver] DID index stats: total_dids={}, total_entries={}",
                total_dids,
                total_entries
            );
        }

        if total_dids == 0 {
            if global_verbose {
                log::debug!("[DIDResolver] DID index is empty or missing");
            }

            if global_verbose {
                log::debug!(
                    "[DIDResolver] Last bundle number: {}",
                    manager.get_last_bundle()
                );
            }

            if super::utils::is_repository_empty(&manager) {
                eprintln!("⚠️  No bundles to index. DID resolution will not be available.");
                eprintln!(
                    "    Sync bundles first with '{} sync' or '{} server --sync'",
                    plcbundle::constants::BINARY_NAME,
                    plcbundle::constants::BINARY_NAME
                );
                if global_verbose {
                    log::debug!("[DIDResolver] Skipping index build - no bundles available");
                }
            } else {
                let last_bundle = manager.get_last_bundle();
                eprintln!("Building DID index...");
                eprintln!("Indexing {} bundles\n", last_bundle);

                if global_verbose {
                    log::debug!(
                        "[DIDResolver] Starting index rebuild for {} bundles",
                        last_bundle
                    );
                }

                let verbose = global_verbose; // Copy for closure
                let start_time = std::time::Instant::now();
                
                // Calculate total uncompressed size for progress tracking
                let index = manager.get_index();
                let bundle_numbers: Vec<u32> = (1..=last_bundle).collect();
                let total_uncompressed_size = index.total_uncompressed_size_for_bundles(&bundle_numbers);
                
                use std::sync::{Arc, Mutex};
                let progress = Arc::new(Mutex::new(ProgressBar::with_bytes(last_bundle as usize, total_uncompressed_size)));
                let progress_clone = progress.clone();
                // Use default flush interval for server
                manager.build_did_index(plcbundle::constants::DID_INDEX_FLUSH_INTERVAL, Some(move |current, _total, bytes_processed, _total_bytes| {
                    let pb = progress_clone.lock().unwrap();
                    pb.set_with_bytes(current as usize, bytes_processed);
                    if verbose && current % 100 == 0 {
                        log::debug!("[DIDResolver] Index progress: {}/{} bundles", current, _total);
                    }
                }))?;
                progress.lock().unwrap().finish();

                let elapsed = start_time.elapsed();

                let stats = manager.get_did_index_stats();
                let final_total_dids = stats
                    .get("total_dids")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let final_total_entries = stats
                    .get("total_entries")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                eprintln!("✓ DID index built");
                eprintln!("  Total DIDs: {}\n", final_total_dids);

                if global_verbose {
                    log::debug!("[DIDResolver] Index build completed in {:?}", elapsed);
                    log::debug!(
                        "[DIDResolver] Final stats: total_dids={}, total_entries={}",
                        final_total_dids,
                        final_total_entries
                    );
                }
            }
        } else {
            if global_verbose {
                log::debug!(
                    "[DIDResolver] DID index already exists with {} DIDs",
                    total_dids
                );
            }
        }
    } else {
        if global_verbose {
            log::debug!("[DIDResolver] DID resolver disabled, skipping DID index check");
        }
    }

    // Test handle resolver on startup if resolver is enabled
    // This performs an actual handle resolution to verify the external handle resolver works correctly
    if cmd.resolver {
        if let Some(handle_resolver) = manager.get_handle_resolver() {
            if global_verbose {
                log::debug!("[HandleResolver] Testing external handle resolver with handle resolution...");
            }
            eprintln!("Testing handle resolver...");
            
            // Use a well-known handle that should always resolve
            let test_handle = "bsky.app";
            let start = std::time::Instant::now();
            match handle_resolver.resolve_handle(test_handle).await {
                Ok(did) => {
                    let duration = start.elapsed();
                    eprintln!("✓ Handle resolver test successful ({:.3}s)", duration.as_secs_f64());
                    if global_verbose {
                        log::debug!(
                            "[HandleResolver] Test resolution of '{}' -> '{}' successful in {:.3}s",
                            test_handle,
                            did,
                            duration.as_secs_f64()
                        );
                    }
                }
                Err(e) => {
                    let duration = start.elapsed();
                    eprintln!("⚠️  Handle resolver test failed ({:.3}s): {}", duration.as_secs_f64(), e);
                    if global_verbose {
                        log::warn!(
                            "[HandleResolver] Test resolution of '{}' failed after {:.3}s: {}",
                            test_handle,
                            duration.as_secs_f64(),
                            e
                        );
                    }
                    eprintln!("    Handle resolution endpoints may not work correctly.");
                }
            }
            eprintln!();
        }
    }

    let manager = Arc::new(manager);

    let addr = format!("{}:{}", cmd.host, cmd.port);
    let socket_addr: SocketAddr = addr.parse().context("Invalid address format")?;

    // Create server config
    let config = ServerConfig {
        sync_mode: cmd.sync,
        sync_interval_seconds: cmd.interval.as_secs(),
        enable_websocket: cmd.websocket,
        enable_resolver: cmd.resolver,
        version: env!("CARGO_PKG_VERSION").to_string(),
    };

    // Create server
    let server = Server::new(Arc::clone(&manager), config);
    let app = server.router();

    // Create shutdown coordinator
    let server_runtime = BundleRuntime::new();
    let mut background_tasks = JoinSet::new();
    let mut resolver_tasks = JoinSet::new();

    // Start sync loop if enabled
    if cmd.sync {
        let manager_clone = Arc::clone(&manager);
        let plc_url = cmd.plc.clone();
        let interval = cmd.interval;
        let max_bundles = cmd.max_bundles;
        let verbose = global_verbose;
        let shutdown_signal = server_runtime.shutdown_signal();
        let sync_runtime = server_runtime.clone();

        background_tasks.spawn(async move {
            use plcbundle::plc_client::PLCClient;
            use plcbundle::sync::{SyncConfig, SyncManager};

            // Create PLC client
            let client = match PLCClient::new(&plc_url) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[Sync] Failed to create PLC client: {}", e);
                    return;
                }
            };

            let config = SyncConfig {
                plc_url: plc_url.clone(),
                continuous: true,
                interval,
                max_bundles: max_bundles as usize,
                verbose,
                shutdown_rx: Some(shutdown_signal),
                shutdown_tx: Some(sync_runtime.shutdown_sender()),
            };

            use plcbundle::sync::ServerLogger;
            let logger = ServerLogger::new(verbose, interval);
            let sync_manager = SyncManager::new(manager_clone, client, config).with_logger(logger);

            if let Err(e) = sync_manager.run_continuous().await {
                eprintln!("[Sync] Sync loop error: {}", e);
                // Trigger fatal shutdown on sync errors
                sync_runtime.trigger_fatal_shutdown();
            }
        });
    }

    // Start handle resolver keep-alive ping task if resolver is enabled
    // Handle resolver tasks are tracked separately so they can be aborted immediately on shutdown
    if cmd.resolver {
        if let Some(handle_resolver) = manager.get_handle_resolver() {
            let verbose = global_verbose;
            let shutdown_signal = server_runtime.shutdown_signal();
            resolver_tasks.spawn(async move {
                run_resolver_ping_loop(handle_resolver, verbose, shutdown_signal).await;
            });
        }
    }

    // Run server with immediate shutdown
    let listener = tokio::net::TcpListener::bind(socket_addr)
        .await
        .context("Failed to bind to address")?;

    // Display server info with ASCII art banner after successful bind
    display_server_info(&manager, &addr, &cmd);
    eprintln!("\nPress Ctrl+C to stop\n");

    // Run the server - the shutdown future will complete when Ctrl+C is pressed
    // or when a background task triggers shutdown
    // This triggers graceful shutdown, which stops accepting new connections
    // and waits for existing connections to finish (or timeout after 10 seconds)
    axum::serve(listener, app)
        .with_graceful_shutdown(server_runtime.create_shutdown_future())
        .await
        .context("Server error")?;

    // Ensure every background task sees the shutdown flag, even on non-signal exits
    server_runtime.trigger_shutdown();

    // Always abort resolver tasks immediately - they're just keep-alive pings
    if !resolver_tasks.is_empty() {
        resolver_tasks.abort_all();
        while let Some(result) = resolver_tasks.join_next().await {
            if let Err(e) = result {
                if e.is_cancelled() {
                    // Task was aborted, which is expected
                } else {
                    eprintln!("⚠️  Resolver task error: {}", e);
                }
            }
        }
    }

    // If shutdown was triggered by a fatal error, abort all tasks immediately
    // Otherwise, wait for them to finish gracefully
    if server_runtime.is_fatal_shutdown() {
        eprintln!("\n⚠️  Fatal error detected - aborting background tasks...");
        background_tasks.abort_all();
        // Wait briefly for aborted tasks to finish
        while let Some(result) = background_tasks.join_next().await {
            if let Err(e) = result {
                if e.is_cancelled() {
                    // Task was aborted, which is expected
                } else {
                    eprintln!("⚠️  Background task error: {}", e);
                }
            }
        }
    } else {
        // Normal shutdown - wait for tasks to finish gracefully
        if !background_tasks.is_empty() {
            eprintln!("\n⏳ Waiting for background tasks to finish...");
            while let Some(result) = background_tasks.join_next().await {
                if let Err(e) = result {
                    eprintln!("⚠️  Background task error: {}", e);
                }
            }
        }
    }

    eprintln!("✅ Server stopped");
    Ok(())
}

#[cfg(feature = "server")]
async fn run_resolver_ping_loop(
    resolver: Arc<plcbundle::handle_resolver::HandleResolver>,
    verbose: bool,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    use tokio::time::{Duration, Instant, sleep};

    // Ping every 2 minutes to keep HTTP/2 connections alive
    let ping_interval = Duration::from_secs(120);

    if verbose {
        log::debug!(
            "[HandleResolver] Starting keep-alive ping loop (interval: {:?})",
            ping_interval
        );
        log::debug!("[HandleResolver] Handle resolver URL: {}", resolver.get_base_url());
    }

    // Initial delay before first ping
    if verbose {
        log::debug!("[HandleResolver] Waiting 30s before first ping...");
    }
    sleep(Duration::from_secs(30)).await;

    let mut ping_count = 0u64;
    let mut success_count = 0u64;
    let mut failure_count = 0u64;

    loop {
        // Check for shutdown
        if *shutdown_rx.borrow() {
            if verbose {
                log::debug!("[HandleResolver] Shutdown requested, stopping ping loop");
            }
            break;
        }

        ping_count += 1;
        let start = Instant::now();

        if verbose {
            log::debug!(
                "[HandleResolver] Ping #{}: sending keep-alive request...",
                ping_count
            );
        }

        match resolver.ping().await {
            Ok(_) => {
                success_count += 1;
                let duration = start.elapsed();
                if verbose {
                    log::info!(
                        "[HandleResolver] Ping #{} successful in {:.3}s (success: {}/{}, failures: {})",
                        ping_count,
                        duration.as_secs_f64(),
                        success_count,
                        ping_count,
                        failure_count
                    );
                }
            }
            Err(e) => {
                failure_count += 1;
                let duration = start.elapsed();
                if verbose {
                    log::warn!(
                        "[HandleResolver] Ping #{} failed after {:.3}s: {} (success: {}/{}, failures: {})",
                        ping_count,
                        duration.as_secs_f64(),
                        e,
                        success_count,
                        ping_count,
                        failure_count
                    );
                }
                // Continue anyway - the connection will be re-established on next actual request
            }
        }

        if verbose {
            log::debug!("[HandleResolver] Next ping in {:?}...", ping_interval);
        }

        // Use select to allow cancellation during sleep
        tokio::select! {
            _ = sleep(ping_interval) => {}
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }
}


#[cfg(feature = "server")]
fn display_server_info(manager: &BundleManager, addr: &str, cmd: &ServerCommand) {
    // Print ASCII art banner
    eprint!(
        "{}\n",
        plcbundle::server::get_ascii_art_banner(env!("CARGO_PKG_VERSION"))
    );
    eprintln!("{} HTTP server started", plcbundle::constants::BINARY_NAME);
    eprintln!(
        "  Directory: {}",
        utils::display_path(manager.directory()).display()
    );
    eprintln!("  Listening: http://{}", addr);

    if cmd.sync {
        eprintln!("  Sync: ENABLED (daemon mode)");
        eprintln!("    PLC URL: {}", cmd.plc);
        eprintln!("    Interval: {:?}", cmd.interval);
        if cmd.max_bundles > 0 {
            eprintln!("    Max bundles: {}", cmd.max_bundles);
        }
    } else {
        eprintln!("  Sync: disabled (read-only archive)");
        eprintln!("    Tip: Use --sync to enable live syncing");
    }

    if cmd.websocket {
        eprintln!("  WebSocket: ENABLED (ws://{}/ws)", addr);
    } else {
        eprintln!("  WebSocket: disabled");
    }

    if cmd.resolver {
        eprintln!("  DID Resolver: ENABLED (/:did endpoints)");
        if manager.get_handle_resolver_base_url().is_some() {
            eprintln!("  Handle Resolver: ENABLED (handle->DID conversion)");
            eprintln!("    Keep-alive ping: every 2 minutes");
        } else {
            eprintln!("  Handle Resolver: disabled (handle->DID conversion unavailable)");
        }
    } else {
        eprintln!("  DID Resolver: disabled");
        eprintln!("  Handle Resolver: disabled");
    }

    let index = manager.get_index();
    eprintln!("  Bundles: {} available", index.bundles.len());
}
