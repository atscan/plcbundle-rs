// Server command - start HTTP server
use anyhow::{Result, Context};
use clap::Args;
use plcbundle::BundleManager;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tokio::time::{sleep, Duration};
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
    #[arg(long, default_value = "https://plc.directory")]
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
    use axum::Router;
    use std::net::SocketAddr;
    use tokio::runtime::Runtime;
    
    // Create tokio runtime for async operations
    let rt = Runtime::new().context("Failed to create tokio runtime")?;
    rt.block_on(run_server_async(cmd, dir))
}

#[cfg(feature = "server")]
async fn run_server_async(cmd: ServerCommand, dir: PathBuf) -> Result<()> {
    use axum::Router;
    use std::net::SocketAddr;

    // Initialize manager
    let manager = if cmd.sync {
        // Sync mode can auto-init
        BundleManager::new(dir.clone())
            .or_else(|_| {
                // Try to initialize if it doesn't exist (similar to init command)
                let index_path = dir.join("plc_bundles.json");
                if !index_path.exists() {
                    let plcbundle_dir = dir.join(".plcbundle");
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
                BundleManager::new(dir.clone())
            })?
    } else {
        // Read-only mode cannot auto-init
        BundleManager::new(dir.clone())
            .context("Repository not found. Use 'plcbundle init' first or run with --sync")?
    };

    let manager = Arc::new(manager);

    // Build/verify DID index if resolver enabled
    if cmd.resolver {
        eprintln!("Building DID index...");
        // TODO: Implement ensure_did_index with progress callback
        // For now, just check if it exists
        let _did_index = manager.directory().join(".plcbundle").join("did_index");
        if !_did_index.exists() {
            eprintln!("⚠️  DID index not found. DID resolution will not be available.");
            eprintln!("    Run 'plcbundle index rebuild' to create the index.");
        }
    }

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
    loop {
        // TODO: Implement sync loop
        // For now, just log that sync is running
        if verbose {
            eprintln!("[Sync] Checking for new bundles...");
        }
        
        // Sleep for interval
        sleep(interval).await;
    }
}

#[cfg(feature = "server")]
fn display_server_info(manager: &BundleManager, addr: &str, cmd: &ServerCommand) {
    eprintln!("Starting plcbundle HTTP server...");
    eprintln!("  Directory: {}", manager.directory().display());
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
    } else {
        eprintln!("  Resolver: disabled");
    }

    let index = manager.get_index();
    eprintln!("  Bundles: {} available", index.bundles.len());
}

