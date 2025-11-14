// Server command - start HTTP server
use anyhow::{Result, Context};
use clap::Args;
use std::path::PathBuf;
use tokio::time::Duration;
use super::utils;

#[cfg(feature = "server")]
use plcbundle::BundleManager;
#[cfg(feature = "server")]
use std::sync::Arc;
#[cfg(feature = "server")]
use tokio::signal;
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

    /// Handle resolver URL (defaults to quickdid.smokesignal.tools if --resolver is enabled)
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

    // Initialize manager with handle resolver if configured
    // If --resolver is enabled but --handle-resolver is not provided, use default
    use plcbundle::constants;
    
    let handle_resolver_url = if cmd.resolver && cmd.handle_resolver.is_none() {
        if cmd.verbose {
            log::debug!("[Resolver] Using default handle resolver: {}", constants::DEFAULT_HANDLE_RESOLVER_URL);
        }
        Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string())
    } else {
        if cmd.verbose && cmd.handle_resolver.is_some() {
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
        if cmd.verbose {
            log::debug!("[Resolver] DID index stats: total_dids={}, total_entries={}", 
                did_index_stats.total_dids, did_index_stats.total_entries);
        }
        
        if did_index_stats.total_dids == 0 {
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
                eprintln!("✓ DID index built");
                eprintln!("  Total DIDs: {}\n", stats.total_dids);
                
                if cmd.verbose {
                    log::debug!("[Resolver] Index build completed in {:?}", elapsed);
                    log::debug!("[Resolver] Final stats: total_dids={}, total_entries={}", 
                        stats.total_dids, stats.total_entries);
                }
            }
        } else {
            if cmd.verbose {
                log::debug!("[Resolver] DID index already exists with {} DIDs", did_index_stats.total_dids);
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

    // Setup graceful shutdown
    let shutdown_signal = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install signal handler");
        eprintln!("\n\n⚠️  Shutdown signal received...");
        eprintln!("  Saving mempool...");
        
        // TODO: Save mempool
        // manager.get_mempool().save()?;
        
        eprintln!("  ✓ Shutdown complete");
    };

    // Start sync loop if enabled
    if cmd.sync {
        let manager_clone = Arc::clone(&manager);
        let plc_url = cmd.plc.clone();
        let interval = cmd.interval;
        let max_bundles = cmd.max_bundles;
        let verbose = cmd.verbose;

        tokio::spawn(async move {
            run_server_sync_loop(manager_clone, plc_url, interval, max_bundles, verbose).await;
        });
    }

    // Start handle resolver keep-alive ping task if resolver is enabled
    if cmd.resolver {
        if let Some(resolver) = manager.get_handle_resolver() {
            let verbose = cmd.verbose;
            tokio::spawn(async move {
                run_resolver_ping_loop(resolver, verbose).await;
            });
        }
    }

    eprintln!("\nPress Ctrl+C to stop\n");

    // Run server
    let listener = tokio::net::TcpListener::bind(socket_addr).await
        .context("Failed to bind to address")?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await
        .context("Server error")?;

    Ok(())
}

#[cfg(feature = "server")]
async fn run_server_sync_loop(
    manager: Arc<BundleManager>,
    plc_url: String,
    interval: Duration,
    max_bundles: u32,
    verbose: bool,
) {
    use plcbundle::sync::PLCClient;
    use tokio::time::sleep;

    // Create PLC client
    let client = match PLCClient::new(&plc_url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[Sync] Failed to create PLC client: {}", e);
            return;
        }
    };

    let mut total_synced = 0u32;
    let mut is_initial_sync = true;
    let mut initial_sync_start_bundle = 0u32;

    // Get initial bundle count if needed
    if is_initial_sync {
        initial_sync_start_bundle = manager.get_last_bundle();
    }

    eprintln!("[Sync] Starting initial sync...");

    if verbose {
        eprintln!("[Sync] Sync loop interval: {:?}", interval);
    }

    loop {
        // Use the shared manager directly - all mutations go through internal locks
        let sync_result = manager.sync_next_bundle(&client).await;

        match sync_result {
            Ok(plcbundle::SyncResult::BundleCreated { bundle_num, mempool_count, duration_ms }) => {
                total_synced += 1;

                // During initial sync, always show progress
                if is_initial_sync {
                    eprintln!("[Sync] ✓ Bundle {:06} synced ({} bundles) | mempool: {} | time: {}ms",
                        bundle_num, total_synced, mempool_count, duration_ms);
                } else {
                    // Always show bundle creation in monitoring mode (not just verbose)
                    eprintln!("[Sync] ✓ Bundle {:06} | mempool: {} | time: {}ms",
                        bundle_num, mempool_count, duration_ms);
                }

                // Check max bundles limit
                if max_bundles > 0 && total_synced >= max_bundles {
                    if verbose {
                        eprintln!("[Sync] Reached max bundles limit ({})", max_bundles);
                    }
                    break;
                }

                // During initial sync, sleep briefly (500ms) to avoid hammering the API
                // After initial sync, use the full interval
                if is_initial_sync {
                    sleep(Duration::from_millis(500)).await;
                } else {
                    sleep(interval).await;
                }
            }
            Ok(plcbundle::SyncResult::CaughtUp { next_bundle, mempool_count, new_ops, fetch_duration_ms }) => {
                // Caught up - initial sync is complete
                if is_initial_sync {
                    is_initial_sync = false;
                    eprintln!("[Sync] ✓ Initial sync complete ({} bundles synced)", total_synced);
                    if mempool_count > 0 {
                        eprintln!("[Sync] ✓ Mempool: {} operations", mempool_count);
                    }
                    eprintln!("[Sync] Now monitoring for new operations (interval: {:?})...", interval);
                } else {
                    // Always show mempool status when caught up (not just when new_ops > 0 or verbose)
                    if new_ops > 0 {
                        eprintln!("[Sync] ✓ Bundle {:06} | mempool: {} ({:+}) | time: {}ms",
                            next_bundle, mempool_count, new_ops as i32, fetch_duration_ms);
                    } else {
                        // Show even when no new ops in non-verbose mode
                        eprintln!("[Sync] ✓ Bundle {:06} | mempool: {} | time: {}ms",
                            next_bundle, mempool_count, fetch_duration_ms);
                    }
                }

                // Always sleep for the full interval when caught up (monitoring mode)
                sleep(interval).await;
            }
            Err(e) => {
                eprintln!("[Sync] Error during sync: {}", e);
                // On error, sleep briefly before retrying
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    if verbose {
        eprintln!("[Sync] Sync loop stopped");
    }
}

#[cfg(feature = "server")]
async fn run_resolver_ping_loop(
    resolver: Arc<plcbundle::handle_resolver::HandleResolver>,
    verbose: bool,
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
        sleep(ping_interval).await;
    }
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

