// Server command - start HTTP server
use anyhow::Result;
use clap::{Args, ValueHint};
use std::path::PathBuf;

#[cfg(feature = "server")]
use tokio::time::Duration;

#[cfg(feature = "server")]
use plcbundle::server::{start_server, StartupConfig, ProgressCallbackFactory};
#[cfg(feature = "server")]
use super::progress::ProgressBar;
#[cfg(feature = "server")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "server")]
fn parse_duration_for_clap(s: &str) -> Result<Duration, String> {
    plcbundle::server::parse_duration(s)
        .map_err(|e| e.to_string())
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
    #[cfg(feature = "server")]
    #[arg(long, default_value = "60s", value_parser = parse_duration_for_clap, help_heading = "Sync Options")]
    pub interval: Duration,
    
    #[cfg(not(feature = "server"))]
    #[arg(long, default_value = "60s", help_heading = "Sync Options")]
    pub interval: String,

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
    use anyhow::Context;
    use tokio::runtime::Runtime;
    use plcbundle::constants;

    // Create tokio runtime for async operations
    let rt = Runtime::new().context("Failed to create tokio runtime")?;
    
    // Determine handle resolver URL
    let handle_resolver_url = if cmd.handle_resolver.is_none() {
        Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string())
    } else {
        cmd.handle_resolver.clone()
    };

    // Create startup config
    let startup_config = StartupConfig {
        dir,
        sync: cmd.sync,
        plc_url: cmd.plc,
        handle_resolver_url,
        enable_resolver: cmd.resolver,
        verbose: global_verbose,
        host: cmd.host,
        port: cmd.port,
        sync_interval: cmd.interval,
        max_bundles: cmd.max_bundles,
        enable_websocket: cmd.websocket,
    };

    // Create progress callback factory for DID index building (CLI-specific)
    // This factory will be called with (last_bundle, total_bytes) when the index needs to be built
    let progress_callback_factory: Option<ProgressCallbackFactory> = if cmd.resolver {
        Some(Box::new(move |last_bundle: u32, total_bytes: u64| {
            let progress = Arc::new(Mutex::new(ProgressBar::with_bytes(last_bundle as usize, total_bytes)));
            let progress_clone = progress.clone();
            let progress_finish = progress.clone();
            let verbose = global_verbose;
            
            let callback = Box::new(move |current: u32, _total: u32, bytes_processed: u64, _total_bytes: u64| {
                let pb = progress_clone.lock().unwrap();
                pb.set_with_bytes(current as usize, bytes_processed);
                if verbose && current.is_multiple_of(100) {
                    log::debug!("[DIDResolver] Index progress: {}/{} bundles", current, _total);
                }
            }) as Box<dyn Fn(u32, u32, u64, u64) + Send + Sync>;
            
            let finish = Box::new(move || {
                let pb = progress_finish.lock().unwrap();
                pb.finish();
            }) as Box<dyn FnOnce() + Send + Sync>;
            
            (callback, Some(finish))
        }))
    } else {
        None
    };

    rt.block_on(start_server(startup_config, progress_callback_factory))
}
