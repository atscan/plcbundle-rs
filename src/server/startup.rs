// Server startup and initialization logic
// This module handles all the complex setup logic for starting the server,
// including manager initialization, DID index building, handle resolver setup, etc.

use crate::manager::BundleManager;
use crate::runtime::BundleRuntime;
use crate::server::{Server, ServerConfig};
use crate::constants;
use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::time::Duration;

/// Configuration for server startup
pub struct StartupConfig {
    pub dir: PathBuf,
    pub sync: bool,
    pub plc_url: String,
    pub handle_resolver_url: Option<String>,
    pub enable_resolver: bool,
    pub verbose: bool,
    pub host: String,
    pub port: u16,
    pub sync_interval: Duration,
    pub max_bundles: u32,
    pub enable_websocket: bool,
}

/// Progress callback for DID index building
/// Arguments: (current_bundle, total_bundles, bytes_processed, total_bytes)
pub type ProgressCallback = Box<dyn Fn(u32, u32, u64, u64) + Send + Sync>;

/// Finish function for progress tracking (e.g., to finish a progress bar)
pub type ProgressFinish = Box<dyn FnOnce() + Send + Sync>;

/// Initialize and configure the BundleManager based on startup config
pub fn initialize_manager(config: &StartupConfig) -> Result<BundleManager> {
    let handle_resolver_url = if config.handle_resolver_url.is_none() {
        if config.verbose {
            log::debug!(
                "[HandleResolver] Using default handle resolver: {}",
                constants::DEFAULT_HANDLE_RESOLVER_URL
            );
        }
        Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string())
    } else {
        if config.verbose {
            log::debug!(
                "[HandleResolver] Using custom handle resolver: {}",
                config.handle_resolver_url.as_ref().unwrap()
            );
        }
        config.handle_resolver_url.clone()
    };

    let manager = if config.sync {
        // Sync mode can auto-init
        match BundleManager::with_handle_resolver(config.dir.clone(), handle_resolver_url.clone()) {
            Ok(mgr) => mgr,
            Err(_) => {
                // Repository doesn't exist, try to initialize
                BundleManager::init_repository(&config.dir, config.plc_url.clone(), false)
                    .context("Failed to initialize repository")?;
                // Try again after initialization
                BundleManager::with_handle_resolver(config.dir.clone(), handle_resolver_url.clone())
                    .context("Failed to open repository after initialization")?
            }
        }
    } else {
        // Read-only mode cannot auto-init
        BundleManager::with_handle_resolver(config.dir.clone(), handle_resolver_url).context(format!(
            "Repository not found. Use '{} init' first or run with --sync",
            constants::BINARY_NAME
        ))?
    };

    let manager = manager.with_verbose(config.verbose);

    // Log handle resolver configuration
    if config.verbose {
        if let Some(url) = manager.get_handle_resolver_base_url() {
            log::debug!("[HandleResolver] External handle resolver configured: {}", url);
        } else {
            log::debug!("[HandleResolver] External handle resolver not configured");
        }
    }

    Ok(manager)
}

/// Build or verify DID index if resolver is enabled
/// 
/// `progress_callback_factory` is an optional function that creates a progress callback
/// and finish function given the total bundle count and total bytes. This allows the caller 
/// to create progress tracking (e.g., a progress bar) after knowing the bundle count.
pub fn setup_did_index<F>(
    manager: &mut BundleManager,
    enable_resolver: bool,
    verbose: bool,
    progress_callback_factory: Option<F>,
) -> Result<()>
where
    F: FnOnce(u32, u64) -> (ProgressCallback, Option<ProgressFinish>),
{
    if !enable_resolver {
        if verbose {
            log::debug!("[DIDResolver] DID resolver disabled, skipping DID index check");
        }
        return Ok(());
    }

    if verbose {
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
    
    if verbose {
        log::debug!(
            "[DIDResolver] DID index stats: total_dids={}, total_entries={}",
            total_dids,
            total_entries
        );
    }

    if total_dids == 0 {
        if verbose {
            log::debug!("[DIDResolver] DID index is empty or missing");
        }

        let last_bundle = manager.get_last_bundle();
        if verbose {
            log::debug!(
                "[DIDResolver] Last bundle number: {}",
                last_bundle
            );
        }

        // Check if repository is empty
        let index = manager.get_index();
        if index.bundles.is_empty() {
            eprintln!("⚠️  No bundles to index. DID resolution will not be available.");
            eprintln!(
                "    Sync bundles first with '{} sync' or '{} server --sync'",
                constants::BINARY_NAME,
                constants::BINARY_NAME
            );
            if verbose {
                log::debug!("[DIDResolver] Skipping index build - no bundles available");
            }
            return Ok(());
        }

        eprintln!("Building DID index...");
        eprintln!("Indexing {} bundles\n", last_bundle);

        if verbose {
            log::debug!(
                "[DIDResolver] Starting index rebuild for {} bundles",
                last_bundle
            );
        }

        let start_time = std::time::Instant::now();
        
        // Calculate total uncompressed size for progress tracking
        let bundle_numbers: Vec<u32> = (1..=last_bundle).collect();
        let total_uncompressed_size = index.total_uncompressed_size_for_bundles(&bundle_numbers);
        
        // Build index with progress callback (create it now that we know the totals)
        let (callback, finish_fn) = if let Some(factory) = progress_callback_factory {
            let (cb, finish) = factory(last_bundle, total_uncompressed_size);
            let callback = move |current: u32, total: u32, bytes_processed: u64, total_bytes: u64| {
                cb(current, total, bytes_processed, total_bytes);
            };
            (Some(callback), finish)
        } else {
            (None, None)
        };
        
        manager.build_did_index(
            constants::DID_INDEX_FLUSH_INTERVAL,
            callback,
            None,
            None,
        )?;
        
        // Finish progress tracking if provided
        if let Some(finish) = finish_fn {
            finish();
        }

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

        if verbose {
            log::debug!("[DIDResolver] Index build completed in {:?}", elapsed);
            log::debug!(
                "[DIDResolver] Final stats: total_dids={}, total_entries={}",
                final_total_dids,
                final_total_entries
            );
        }
    } else {
        if verbose {
            log::debug!(
                "[DIDResolver] DID index already exists with {} DIDs",
                total_dids
            );
        }
    }

    Ok(())
}

/// Test handle resolver on startup
pub async fn test_handle_resolver(
    manager: &BundleManager,
    verbose: bool,
) -> Result<()> {
    if let Some(handle_resolver) = manager.get_handle_resolver() {
        if verbose {
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
                if verbose {
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
                if verbose {
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
    Ok(())
}

/// Start the resolver ping loop to keep HTTP/2 connections alive
pub async fn run_resolver_ping_loop(
    resolver: Arc<crate::handle_resolver::HandleResolver>,
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

/// Setup sync loop if sync mode is enabled
pub fn setup_sync_loop(
    manager: Arc<BundleManager>,
    config: &StartupConfig,
    server_runtime: &BundleRuntime,
    background_tasks: &mut JoinSet<()>,
) {
    if !config.sync {
        return;
    }

    let manager_clone = Arc::clone(&manager);
    let plc_url = config.plc_url.clone();
    let interval = config.sync_interval;
    let max_bundles = config.max_bundles;
    let verbose = config.verbose;
    let shutdown_signal = server_runtime.shutdown_signal();
    let sync_runtime = server_runtime.clone();

    background_tasks.spawn(async move {
        use crate::plc_client::PLCClient;
        use crate::sync::{SyncConfig, SyncManager};

        // Create PLC client
        let client = match PLCClient::new(&plc_url) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[Sync] Failed to create PLC client: {}", e);
                return;
            }
        };

        let sync_config = SyncConfig {
            plc_url: plc_url.clone(),
            continuous: true,
            interval,
            max_bundles: max_bundles as usize,
            verbose,
            shutdown_rx: Some(shutdown_signal),
            shutdown_tx: Some(sync_runtime.shutdown_sender()),
        };

        use crate::sync::ServerLogger;
        let logger = ServerLogger::new(verbose, interval);
        let sync_manager = SyncManager::new(manager_clone, client, sync_config).with_logger(logger);

        if let Err(e) = sync_manager.run_continuous().await {
            eprintln!("[Sync] Sync loop error: {}", e);
            // Trigger fatal shutdown on sync errors
            sync_runtime.trigger_fatal_shutdown();
        }
    });
}

/// Setup resolver ping loop if resolver is enabled
pub fn setup_resolver_ping_loop(
    manager: &BundleManager,
    config: &StartupConfig,
    server_runtime: &BundleRuntime,
    resolver_tasks: &mut JoinSet<()>,
) {
    if !config.enable_resolver {
        return;
    }

    if let Some(handle_resolver) = manager.get_handle_resolver() {
        let verbose = config.verbose;
        let shutdown_signal = server_runtime.shutdown_signal();
        resolver_tasks.spawn(async move {
            run_resolver_ping_loop(handle_resolver, verbose, shutdown_signal).await;
        });
    }
}

/// Progress callback factory type
/// Takes (last_bundle, total_bytes) and returns (progress_callback, finish_function)
pub type ProgressCallbackFactory = Box<dyn FnOnce(u32, u64) -> (ProgressCallback, Option<ProgressFinish>) + Send + Sync>;

/// Main server startup function that orchestrates all initialization
pub async fn start_server(
    config: StartupConfig,
    progress_callback_factory: Option<ProgressCallbackFactory>,
) -> Result<()> {
    use std::net::SocketAddr;

    // Initialize manager
    let mut manager = initialize_manager(&config)?;

    // Setup DID index if resolver is enabled
    setup_did_index(&mut manager, config.enable_resolver, config.verbose, progress_callback_factory)?;

    // Test handle resolver on startup if resolver is enabled
    if config.enable_resolver {
        test_handle_resolver(&manager, config.verbose).await?;
    }

    let manager = Arc::new(manager);

    let addr = format!("{}:{}", config.host, config.port);
    let socket_addr: SocketAddr = addr.parse().context("Invalid address format")?;

    // Create server config
    let server_config = ServerConfig {
        sync_mode: config.sync,
        sync_interval_seconds: config.sync_interval.as_secs(),
        enable_websocket: config.enable_websocket,
        enable_resolver: config.enable_resolver,
        version: env!("CARGO_PKG_VERSION").to_string(),
    };

    // Create server
    let server = Server::new(Arc::clone(&manager), server_config);
    let app = server.router();

    // Create shutdown coordinator
    let server_runtime = BundleRuntime::new();
    let mut background_tasks = JoinSet::new();
    let mut resolver_tasks = JoinSet::new();

    // Start sync loop if enabled
    setup_sync_loop(Arc::clone(&manager), &config, &server_runtime, &mut background_tasks);

    // Start handle resolver keep-alive ping task if resolver is enabled
    setup_resolver_ping_loop(&manager, &config, &server_runtime, &mut resolver_tasks);

    // Run server with immediate shutdown
    let listener = tokio::net::TcpListener::bind(socket_addr)
        .await
        .context("Failed to bind to address")?;

    // Display server info with ASCII art banner after successful bind
    display_server_info(&manager, &addr, &config);
    eprintln!("\nPress Ctrl+C to stop\n");

    // Run the server - the shutdown future will complete when Ctrl+C is pressed
    // or when a background task triggers shutdown
    // This triggers graceful shutdown, which stops accepting new connections
    // and waits for existing connections to finish (or timeout after 10 seconds)
    axum::serve(listener, app)
        .with_graceful_shutdown(server_runtime.create_shutdown_future())
        .await
        .context("Server error")?;

    // Use common shutdown cleanup handler
    server_runtime.wait_for_shutdown_cleanup(
        "Server",
        Some(&mut resolver_tasks),
        Some(&mut background_tasks),
    ).await;
    
    Ok(())
}

/// Display server startup information
fn display_server_info(manager: &BundleManager, addr: &str, config: &StartupConfig) {
    use crate::server::get_ascii_art_banner;
    
    // Print ASCII art banner
    eprint!(
        "{}\n",
        get_ascii_art_banner(env!("CARGO_PKG_VERSION"))
    );
    eprintln!("{} HTTP server started", constants::BINARY_NAME);
    eprintln!(
        "  Directory: {}",
        manager.directory().display()
    );
    eprintln!("  Listening: http://{}", addr);

    if config.sync {
        eprintln!("  Sync: ENABLED (daemon mode)");
        eprintln!("    PLC URL: {}", config.plc_url);
        eprintln!("    Interval: {:?}", config.sync_interval);
        if config.max_bundles > 0 {
            eprintln!("    Max bundles: {}", config.max_bundles);
        }
    } else {
        eprintln!("  Sync: disabled (read-only archive)");
        eprintln!("    Tip: Use --sync to enable live syncing");
    }

    if config.enable_websocket {
        eprintln!("  WebSocket: ENABLED (ws://{}/ws)", addr);
    } else {
        eprintln!("  WebSocket: disabled");
    }

    if config.enable_resolver {
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

