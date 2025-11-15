// Server command - start HTTP server
use anyhow::{Result, Context};
use clap::Args;
use std::path::PathBuf;
use tokio::time::Duration;

#[cfg(feature = "server")]
use plcbundle::BundleManager;
#[cfg(feature = "server")]
use super::utils;
#[cfg(feature = "server")]
use std::sync::Arc;
#[cfg(feature = "server")]
use tokio::signal;
#[cfg(feature = "server")]
use tokio::task::{JoinHandle, JoinSet};
#[cfg(feature = "server")]
use chrono::Utc;
#[cfg(feature = "server")]
use plcbundle::server::{Server, ServerConfig};

#[derive(Args)]
#[command(
    about = "Start HTTP server",
    long_about = "Start HTTP server to serve bundles over HTTP

Serves bundle data over HTTP with optional live sync mode that continuously
fetches new bundles from PLC directory.

The server provides:
  - Bundle index (JSON)
  - Individual bundle data (compressed or JSONL)
  - WebSocket streaming (optional)
  - DID resolution (optional)
  - Live mempool (in sync mode)

Sync mode (--sync) runs as a daemon, continuously fetching new bundles.
For one-time sync, use 'plcbundle sync' command instead."
)]
pub struct ServerCommand {
    /// HTTP server port
    #[arg(long, default_value = "8080")]
    pub port: u16,

    /// HTTP server host
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Enable sync mode (run as daemon, continuously fetch from PLC)
    #[arg(short, long)]
    pub sync: bool,

    /// PLC directory URL (for sync mode)
    #[arg(long, default_value = plcbundle::constants::DEFAULT_PLC_DIRECTORY_URL)]
    pub plc: String,

    /// Sync interval (how often to check for new bundles)
    #[arg(long, default_value = "60s", value_parser = parse_duration)]
    pub interval: Duration,

    /// Maximum bundles to fetch (0 = unlimited)
    #[arg(long, default_value = "0")]
    pub max_bundles: u32,

    /// Enable WebSocket endpoint for streaming
    #[arg(long)]
    pub websocket: bool,

    /// Enable DID resolution endpoints
    #[arg(long)]
    pub resolver: bool,

    /// Handle resolver URL (defaults to quickdid.smokesignal.tools if not provided)
    #[arg(long)]
    pub handle_resolver: Option<String>,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,
}

fn parse_duration(s: &str) -> Result<Duration> {
    // Simple parser: "60s", "5m", "1h"
    let s = s.trim();
    if s.ends_with('s') {
        let secs: u64 = s[..s.len() - 1].parse()
            .context("Invalid duration format")?;
        Ok(Duration::from_secs(secs))
    } else if s.ends_with('m') {
        let mins: u64 = s[..s.len() - 1].parse()
            .context("Invalid duration format")?;
        Ok(Duration::from_secs(mins * 60))
    } else if s.ends_with('h') {
        let hours: u64 = s[..s.len() - 1].parse()
            .context("Invalid duration format")?;
        Ok(Duration::from_secs(hours * 3600))
    } else {
        // Try parsing as seconds
        let secs: u64 = s.parse()
            .context("Invalid duration format")?;
        Ok(Duration::from_secs(secs))
    }
}

pub fn run(cmd: ServerCommand, dir: PathBuf) -> Result<()> {
    #[cfg(not(feature = "server"))]
    {
        let _ = (cmd, dir); // Suppress unused warnings when server feature is disabled
        anyhow::bail!("Server feature is not enabled. Rebuild with --features server");
    }

    #[cfg(feature = "server")]
    {
        run_server(cmd, dir)
    }
}

#[cfg(feature = "server")]
fn run_server(cmd: ServerCommand, dir: PathBuf) -> Result<()> {
    use tokio::runtime::Runtime;

    // Create tokio runtime for async operations
    let rt = Runtime::new().context("Failed to create tokio runtime")?;
    rt.block_on(run_server_async(cmd, dir))
}

#[cfg(feature = "server")]
async fn run_server_async(cmd: ServerCommand, dir: PathBuf) -> Result<()> {
    use std::net::SocketAddr;

    // Initialize manager with handle resolver
    // Always use default resolver URL if not explicitly provided (DID endpoints always available)
    use plcbundle::constants;
    
    let handle_resolver_url = if cmd.handle_resolver.is_none() {
        if cmd.verbose {
            log::debug!("[Resolver] Using default handle resolver: {}", constants::DEFAULT_HANDLE_RESOLVER_URL);
        }
        Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string())
    } else {
        if cmd.verbose {
            log::debug!("[Resolver] Using custom handle resolver: {}", cmd.handle_resolver.as_ref().unwrap());
        }
        cmd.handle_resolver.clone()
    };
    let manager = if cmd.sync {
        // Sync mode can auto-init
        BundleManager::with_handle_resolver(dir.clone(), handle_resolver_url.clone())
            .or_else(|_| {
                // Try to initialize if it doesn't exist (similar to init command)
                let index_path = dir.join("plc_bundles.json");
                if !index_path.exists() {
                    let plcbundle_dir = dir.join(plcbundle::constants::DID_INDEX_DIR);
                    if !plcbundle_dir.exists() {
                        std::fs::create_dir_all(&plcbundle_dir)?;
                    }
                    let index = serde_json::json!({
                        "version": "1.0",
                        "origin": cmd.plc.clone(),
                        "last_bundle": 0,
                        "updated_at": Utc::now().to_rfc3339(),
                        "total_size_bytes": 0,
                        "total_uncompressed_size_bytes": 0,
                        "bundles": []
                    });
                    let json = serde_json::to_string_pretty(&index)?;
                    std::fs::write(&index_path, json)?;
                }
                BundleManager::with_handle_resolver(dir.clone(), handle_resolver_url.clone())
            })?
    } else {
        // Read-only mode cannot auto-init
        BundleManager::with_handle_resolver(dir.clone(), handle_resolver_url)
            .context("Repository not found. Use 'plcbundle init' first or run with --sync")?
    };

    // Set verbose mode on manager
    let manager = manager.with_verbose(cmd.verbose);

    // Log handle resolver configuration
    if cmd.verbose {
        if let Some(url) = manager.get_handle_resolver_base_url() {
            log::debug!("[Resolver] Handle resolver configured: {}", url);
        } else {
            log::debug!("[Resolver] Handle resolver not configured");
        }
    }

    // Build/verify DID index if resolver enabled
    if cmd.resolver {
        if cmd.verbose {
            log::debug!("[Resolver] Checking DID index status...");
        }
        
        let did_index_stats = manager.get_did_index_stats();
        let total_dids = did_index_stats.get("total_dids").and_then(|v| v.as_i64()).unwrap_or(0);
        let total_entries = did_index_stats.get("total_entries").and_then(|v| v.as_i64()).unwrap_or(0);
        if cmd.verbose {
            log::debug!("[Resolver] DID index stats: total_dids={}, total_entries={}", 
                total_dids, total_entries);
        }
        
        if total_dids == 0 {
            if cmd.verbose {
                log::debug!("[Resolver] DID index is empty or missing");
            }
            
            let last_bundle = manager.get_last_bundle();
            if cmd.verbose {
                log::debug!("[Resolver] Last bundle number: {}", last_bundle);
            }
            
            if last_bundle == 0 {
                eprintln!("⚠️  No bundles to index. DID resolution will not be available.");
                eprintln!("    Sync bundles first with 'plcbundle sync' or 'plcbundle server --sync'");
                if cmd.verbose {
                    log::debug!("[Resolver] Skipping index build - no bundles available");
                }
            } else {
                eprintln!("Building DID index...");
                eprintln!("Indexing {} bundles\n", last_bundle);
                
                if cmd.verbose {
                    log::debug!("[Resolver] Starting index rebuild for {} bundles", last_bundle);
                }
                
                let verbose = cmd.verbose; // Copy for closure
                let start_time = std::time::Instant::now();
                manager.rebuild_did_index(Some(move |current, total| {
                    if current % 10 == 0 || current == total {
                        eprint!("\rProgress: {}/{} ({:.1}%)", current, total, 
                            (current as f64 / total as f64) * 100.0);
                    }
                    if verbose && current % 100 == 0 {
                        log::debug!("[Resolver] Index progress: {}/{} bundles", current, total);
                    }
                }))?;
                
                let elapsed = start_time.elapsed();
                eprintln!();
                
                let stats = manager.get_did_index_stats();
                let final_total_dids = stats.get("total_dids").and_then(|v| v.as_i64()).unwrap_or(0);
                let final_total_entries = stats.get("total_entries").and_then(|v| v.as_i64()).unwrap_or(0);
                eprintln!("✓ DID index built");
                eprintln!("  Total DIDs: {}\n", final_total_dids);
                
                if cmd.verbose {
                    log::debug!("[Resolver] Index build completed in {:?}", elapsed);
                    log::debug!("[Resolver] Final stats: total_dids={}, total_entries={}", 
                        final_total_dids, final_total_entries);
                }
            }
        } else {
            if cmd.verbose {
                log::debug!("[Resolver] DID index already exists with {} DIDs", total_dids);
            }
        }
    } else {
        if cmd.verbose {
            log::debug!("[Resolver] Resolver disabled, skipping DID index check");
        }
    }

    let manager = Arc::new(manager);

    let addr = format!("{}:{}", cmd.host, cmd.port);
    let socket_addr: SocketAddr = addr.parse()
        .context("Invalid address format")?;

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
        let verbose = cmd.verbose;
        let shutdown_rx_sync = shutdown_rx.clone();
        let shutdown_tx_sync = shutdown_tx.clone();

        let sync_handle = tokio::spawn(async move {
            use plcbundle::sync::{PLCClient, SyncManager, SyncConfig};
            
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
            let sync_manager = SyncManager::new(manager_clone, client, config)
                .with_logger(logger);

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
            let verbose = cmd.verbose;
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
    let listener = tokio::net::TcpListener::bind(socket_addr).await
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
    use tokio::time::{sleep, Duration, Instant};

    // Ping every 2 minutes to keep HTTP/2 connections alive
    let ping_interval = Duration::from_secs(120);

    if verbose {
        log::debug!("[Resolver] Starting keep-alive ping loop (interval: {:?})", ping_interval);
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
            log::debug!("[Resolver] Ping #{}: sending keep-alive request...", ping_count);
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
        eprintln!("Stopping {} immediate background task(s)...", immediate_tasks.len());
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
    eprintln!("Starting plcbundle HTTP server...");
    eprintln!("  Directory: {}", utils::display_path(manager.directory()).display());
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

