use anyhow::Result;
use clap::Args;
use plcbundle::{BundleManager, sync::{PLCClient, SyncManager, SyncConfig, CliLogger}, constants};
use std::path::PathBuf;
use std::time::Duration;
use std::sync::Arc;
use super::utils;

#[derive(Args)]
pub struct SyncCommand {
    /// PLC directory URL
    #[arg(long, default_value = constants::DEFAULT_PLC_DIRECTORY_URL)]
    pub plc: String,

    /// Keep syncing (run as daemon)
    #[arg(long, conflicts_with = "max_bundles")]
    pub continuous: bool,

    /// Sync interval for continuous mode
    #[arg(long, default_value = "60s", value_parser = parse_duration)]
    pub interval: Duration,

    /// Maximum bundles to fetch (0 = all, only for one-time sync)
    #[arg(long, default_value = "0", conflicts_with = "continuous")]
    pub max_bundles: usize,

    /// Directory containing PLC bundles
    #[arg(short, long, default_value = ".")]
    pub dir: PathBuf,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Quiet mode
    #[arg(short, long)]
    pub quiet: bool,
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    if let Some(s) = s.strip_suffix('s') {
        s.parse::<u64>().map(Duration::from_secs).map_err(|e| e.to_string())
    } else if let Some(s) = s.strip_suffix('m') {
        s.parse::<u64>().map(|m| Duration::from_secs(m * 60)).map_err(|e| e.to_string())
    } else {
        s.parse::<u64>().map(Duration::from_secs).map_err(|e| e.to_string())
    }
}

pub fn run(cmd: SyncCommand) -> Result<()> {
    tokio::runtime::Runtime::new()?.block_on(async {
        if !cmd.quiet {
            println!("Syncing from: {}", utils::display_path(&cmd.dir).display());
            println!("PLC Directory: {}", cmd.plc);
            if cmd.continuous {
                println!("Mode: continuous (interval: {:?})", cmd.interval);
            }
            println!();
        }

        let client = PLCClient::new(&cmd.plc)?;
        let manager = Arc::new(BundleManager::new(cmd.dir)?.with_verbose(cmd.verbose));

        let config = SyncConfig {
            plc_url: cmd.plc.clone(),
            continuous: cmd.continuous,
            interval: cmd.interval,
            max_bundles: cmd.max_bundles,
            verbose: cmd.verbose,
            enable_did_batching: false,
            did_batch_size: 100,
            shutdown_rx: None,
        };

        let quiet = cmd.quiet;
        
        if cmd.continuous {
            // For continuous mode, use run_once() in a loop to match original behavior
            // This allows us to track bundle counts per iteration
            let mut total_synced = 0;
            
            loop {
                // Create new client and manager for each iteration since SyncManager takes ownership
                let manager_clone = Arc::clone(&manager);
                let client = PLCClient::new(&cmd.plc)?;
                let config = SyncConfig {
                    plc_url: cmd.plc.clone(),
                    continuous: false,
                    interval: cmd.interval,
                    max_bundles: 0,
                    verbose: cmd.verbose,
                    enable_did_batching: false,
                    did_batch_size: 100,
                    shutdown_rx: None,
                };
                
                let logger = CliLogger::new(quiet);
                let sync_manager = SyncManager::new(manager_clone, client, config)
                    .with_logger(logger);
                
                let synced = sync_manager.run_once(None).await?;
                total_synced += synced;

                if !quiet && synced > 0 {
                    eprintln!("✓ Synced {} bundles (total: {})", synced, total_synced);
                }

                if synced == 0 {
                    // Caught up, sleep before next check
                    tokio::time::sleep(cmd.interval).await;
                }
            }
        } else {
            // For one-time sync, use run_once() with logger
            let logger = CliLogger::new(quiet);
            let sync_manager = SyncManager::new(manager, client, config)
                .with_logger(logger);
            
            let max_bundles = if cmd.max_bundles > 0 { Some(cmd.max_bundles) } else { None };
            let synced = sync_manager.run_once(max_bundles).await?;

            if !quiet {
                if synced == 0 {
                    eprintln!("\n✓ Already up to date");
                } else {
                    eprintln!("\n✓ Sync complete: {} bundle(s) fetched", synced);
                }
            }

            Ok(())
        }
    })
}
