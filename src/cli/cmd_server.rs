// Server command - start HTTP server
use anyhow::{Context, Result};
use clap::Args;
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
use std::sync::Arc;
#[cfg(feature = "server")]
use tokio::signal;
#[cfg(feature = "server")]
use tokio::task::{JoinHandle, JoinSet};

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
    #[arg(long, default_value = plcbundle::constants::DEFAULT_PLC_DIRECTORY_URL, help_heading = "Sync Options")]
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
    #[arg(long, help_heading = "Feature Options")]
    pub handle_resolver: Option<String>,

}

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

    // Initialize manager with handle resolver
    // Always use default resolver URL if not explicitly provided (DID endpoints always available)
    use plcbundle::constants;

    let handle_resolver_url = if cmd.handle_resolver.is_none() {
        if global_verbose {
            log::debug!(
                "[Resolver] Using default handle resolver: {}",
                constants::DEFAULT_HANDLE_RESOLVER_URL
            );
        }
        Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string())
    } else {
        if global_verbose {
            log::debug!(
                "[Resolver] Using custom handle resolver: {}",
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

    // Print ASCII art banner after repository validation succeeds
    eprint!(
        "{}\n",
        plcbundle::server::get_ascii_art_banner(env!("CARGO_PKG_VERSION"))
    );

    // Set verbose mode on manager
    let manager = manager.with_verbose(global_verbose);

    // Log handle resolver configuration
    if global_verbose {
        if let Some(url) = manager.get_handle_resolver_base_url() {
            log::debug!("[Resolver] Handle resolver configured: {}", url);
        } else {
            log::debug!("[Resolver] Handle resolver not configured");
        }
    }

    // Build/verify DID index if resolver enabled
    if cmd.resolver {
        if global_verbose {
            log::debug!("[Resolver] Checking DID index status...");
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
                "[Resolver] DID index stats: total_dids={}, total_entries={}",
                total_dids,
                total_entries
            );
        }

        if total_dids == 0 {
            if global_verbose {
                log::debug!("[Resolver] DID index is empty or missing");
            }

            if global_verbose {
                log::debug!(
                    "[Resolver] Last bundle number: {}",
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
                    log::debug!("[Resolver] Skipping index build - no bundles available");
                }
            } else {
                let last_bundle = manager.get_last_bundle();
                eprintln!("Building DID index...");
                eprintln!("Indexing {} bundles\n", last_bundle);

                if global_verbose {
                    log::debug!(
                        "[Resolver] Starting index rebuild for {} bundles",
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
                        log::debug!("[Resolver] Index progress: {}/{} bundles", current, _total);
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
                    log::debug!("[Resolver] Index build completed in {:?}", elapsed);
                    log::debug!(
                        "[Resolver] Final stats: total_dids={}, total_entries={}",
                        final_total_dids,
                        final_total_entries
                    );
                }
            }
        } else {
            if global_verbose {
                log::debug!(
                    "[Resolver] DID index already exists with {} DIDs",
                    total_dids
                );
            }
        }
    } else {
        if global_verbose {
            log::debug!("[Resolver] Resolver disabled, skipping DID index check");
        }
    }

    let manager = Arc::new(manager);

    let addr = format!("{}:{}", cmd.host, cmd.port);
    let socket_addr: SocketAddr = addr.parse().context("Invalid address format")?;

    // Display server info
    display_server_info(&manager, &addr, &cmd);

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

    // Create shutdown notification channel for immediate cancellation
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut background_tasks: Vec<BackgroundTaskHandle> = Vec::new();

    // Setup immediate shutdown signal
    let shutdown_tx_clone = shutdown_tx.clone();
    let shutdown_signal = async move {
        signal::ctrl_c()
            .await
            .expect("Failed to install signal handler");
        eprintln!("\n⚠️  Shutdown signal received...");
        // Notify all tasks to stop immediately
        let _ = shutdown_tx_clone.send(true);
    };

    // Start sync loop if enabled
    if cmd.sync {
        let manager_clone = Arc::clone(&manager);
        let plc_url = cmd.plc.clone();
        let interval = cmd.interval;
        let max_bundles = cmd.max_bundles;
        let verbose = global_verbose;
        let shutdown_rx_sync = shutdown_rx.clone();
        let shutdown_tx_sync = shutdown_tx.clone();

        let sync_handle = tokio::spawn(async move {
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
                shutdown_rx: Some(shutdown_rx_sync),
                shutdown_tx: Some(shutdown_tx_sync),
            };

            use plcbundle::sync::ServerLogger;
            let logger = ServerLogger::new(verbose, interval);
            let sync_manager = SyncManager::new(manager_clone, client, config).with_logger(logger);

            if let Err(e) = sync_manager.run_continuous().await {
                eprintln!("[Sync] Sync loop error: {}", e);
            }
        });
        background_tasks.push(BackgroundTaskHandle {
            name: "sync loop",
            handle: sync_handle,
            immediate_abort: false,
        });
    }

    // Start handle resolver keep-alive ping task if resolver is enabled
    if cmd.resolver {
        if let Some(resolver) = manager.get_handle_resolver() {
            let verbose = global_verbose;
            let shutdown_rx_resolver = shutdown_rx.clone();
            let resolver_handle = tokio::spawn(async move {
                run_resolver_ping_loop(resolver, verbose, shutdown_rx_resolver).await;
            });
            background_tasks.push(BackgroundTaskHandle {
                name: "resolver keep-alive",
                handle: resolver_handle,
                immediate_abort: true,
            });
        }
    }

    eprintln!("\nPress Ctrl+C to stop\n");

    // Run server with immediate shutdown
    let listener = tokio::net::TcpListener::bind(socket_addr)
        .await
        .context("Failed to bind to address")?;

    // Run the server - the shutdown_signal future will complete when Ctrl+C is pressed
    // This triggers graceful shutdown, which stops accepting new connections
    // and waits for existing connections to finish (or timeout after 10 seconds)
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await
        .context("Server error")?;

    // Ensure every background task sees the shutdown flag, even on non-signal exits
    let _ = shutdown_tx.send(true);
    wait_for_background_tasks(background_tasks).await;

    eprintln!("Server stopped");
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
            "[Resolver] Starting keep-alive ping loop (interval: {:?})",
            ping_interval
        );
        log::debug!("[Resolver] Resolver URL: {}", resolver.get_base_url());
    }

    // Initial delay before first ping
    if verbose {
        log::debug!("[Resolver] Waiting 30s before first ping...");
    }
    sleep(Duration::from_secs(30)).await;

    let mut ping_count = 0u64;
    let mut success_count = 0u64;
    let mut failure_count = 0u64;

    loop {
        // Check for shutdown
        if *shutdown_rx.borrow() {
            if verbose {
                log::debug!("[Resolver] Shutdown requested, stopping ping loop");
            }
            break;
        }

        ping_count += 1;
        let start = Instant::now();

        if verbose {
            log::debug!(
                "[Resolver] Ping #{}: sending keep-alive request...",
                ping_count
            );
        }

        match resolver.ping().await {
            Ok(_) => {
                success_count += 1;
                let duration = start.elapsed();
                if verbose {
                    log::info!(
                        "[Resolver] Ping #{} successful in {:.3}s (success: {}/{}, failures: {})",
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
                        "[Resolver] Ping #{} failed after {:.3}s: {} (success: {}/{}, failures: {})",
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
            log::debug!("[Resolver] Next ping in {:?}...", ping_interval);
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
struct BackgroundTaskHandle {
    name: &'static str,
    handle: JoinHandle<()>,
    immediate_abort: bool,
}

#[cfg(feature = "server")]
async fn wait_for_background_tasks(tasks: Vec<BackgroundTaskHandle>) {
    if tasks.is_empty() {
        return;
    }

    let mut immediate_tasks = Vec::new();
    let mut graceful_tasks = Vec::new();

    for task in tasks {
        if task.immediate_abort {
            immediate_tasks.push(task);
        } else {
            graceful_tasks.push(task);
        }
    }

    if !immediate_tasks.is_empty() {
        eprintln!();
        eprintln!(
            "Stopping {} immediate background task(s)...",
            immediate_tasks.len()
        );
        for task in immediate_tasks {
            let BackgroundTaskHandle { name, handle, .. } = task;
            handle.abort();
            match handle.await {
                Ok(()) => eprintln!("✗ {} aborted", name),
                Err(e) if e.is_cancelled() => eprintln!("✗ {} aborted", name),
                Err(e) => eprintln!("⚠️  {} abort error: {}", name, e),
            }
        }
    }

    if graceful_tasks.is_empty() {
        return;
    }

    let total = graceful_tasks.len();
    let task_names: Vec<&'static str> = graceful_tasks.iter().map(|t| t.name).collect();

    eprintln!();
    eprintln!("Waiting for {} background task(s) to finish...", total);
    eprintln!("Press Ctrl+C again to force-stop remaining tasks.");
    for name in &task_names {
        eprintln!("  • {}", name);
    }

    let mut join_set = JoinSet::new();
    for task in graceful_tasks {
        join_set.spawn(async move {
            let name = task.name;
            let result = task.handle.await;
            (name, result)
        });
    }

    let mut remaining = total;
    let force_signal = signal::ctrl_c();
    tokio::pin!(force_signal);

    while remaining > 0 {
        tokio::select! {
            Some(join_result) = join_set.join_next() => {
                remaining -= 1;
                match join_result {
                    Ok((name, Ok(()))) => eprintln!("✓ {} stopped", name),
                    Ok((name, Err(e))) if e.is_cancelled() => eprintln!("✓ {} aborted", name),
                    Ok((name, Err(e))) => eprintln!("⚠️  {} ended with error: {}", name, e),
                    Err(e) => eprintln!("⚠️  Background task join error: {}", e),
                }
            }
            res = &mut force_signal => {
                match res {
                    Ok(()) => {
                        eprintln!("Force shutdown requested. Aborting remaining background tasks...");
                    }
                    Err(err) => {
                        eprintln!("⚠️  Failed to listen for Ctrl+C for force shutdown: {}", err);
                        continue;
                    }
                }

                join_set.abort_all();
                while let Some(join_result) = join_set.join_next().await {
                    match join_result {
                        Ok((name, _)) => eprintln!("✗ {} aborted", name),
                        Err(e) => eprintln!("⚠️  Background task join error: {}", e),
                    }
                }
                return;
            }
        }
    }

    eprintln!("All background tasks stopped.");
}

#[cfg(feature = "server")]
fn display_server_info(manager: &BundleManager, addr: &str, cmd: &ServerCommand) {
    eprintln!(
        "Starting {} HTTP server...",
        plcbundle::constants::BINARY_NAME
    );
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
        eprintln!("  Resolver: ENABLED (/:did endpoints)");
        if manager.get_handle_resolver_base_url().is_some() {
            eprintln!("    Keep-alive ping: every 2 minutes");
        }
    } else {
        eprintln!("  Resolver: disabled");
    }

    let index = manager.get_index();
    eprintln!("  Bundles: {} available", index.bundles.len());
}
