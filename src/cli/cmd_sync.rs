use super::utils;
use super::utils::HasGlobalFlags;
use anyhow::Result;
use clap::Args;
use plcbundle::{
    constants,
    sync::{CliLogger, PLCClient, ServerLogger, SyncConfig, SyncManager},
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Args)]
#[command(
    about = "Fetch new bundles from PLC directory",
    long_about = "Download new operations from the PLC directory and create bundles.\nSimilar to 'git fetch' - updates your local repository with new data.",
    alias = "fetch",
    after_help = "Examples:\n  \
            # Fetch new bundles once\n  \
            plcbundle sync\n\n  \
            # Run continuously (daemon mode)\n  \
            plcbundle sync --continuous\n\n  \
            # Custom sync interval\n  \
            plcbundle sync --continuous --interval 30s\n\n  \
            # Fetch maximum 10 bundles then stop\n  \
            plcbundle sync --max-bundles 10"
)]
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

impl HasGlobalFlags for SyncCommand {
    fn verbose(&self) -> bool { self.verbose }
    fn quiet(&self) -> bool { self.quiet }
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    if let Some(s) = s.strip_suffix('s') {
        s.parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|e| e.to_string())
    } else if let Some(s) = s.strip_suffix('m') {
        s.parse::<u64>()
            .map(|m| Duration::from_secs(m * 60))
            .map_err(|e| e.to_string())
    } else {
        s.parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|e| e.to_string())
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
        let dir = cmd.dir.clone();
        let manager = Arc::new(super::utils::create_manager_from_cmd(dir, &cmd)?);

        let config = SyncConfig {
            plc_url: cmd.plc.clone(),
            continuous: cmd.continuous,
            interval: cmd.interval,
            max_bundles: cmd.max_bundles,
            verbose: cmd.verbose,
            shutdown_rx: None,
            shutdown_tx: None,
        };

        let quiet = cmd.quiet;

        if cmd.continuous {
            // For continuous mode, use run_continuous() with ServerLogger to enable verbose toggle
            let config = SyncConfig {
                plc_url: cmd.plc.clone(),
                continuous: true,
                interval: cmd.interval,
                max_bundles: 0,
                verbose: cmd.verbose,
                shutdown_rx: None,
                shutdown_tx: None,
            };

            let logger = ServerLogger::new(cmd.verbose, cmd.interval);
            let sync_manager = SyncManager::new(manager, client, config).with_logger(logger);

            sync_manager.run_continuous().await?;
            Ok(())
        } else {
            // For one-time sync, use run_once() with logger
            let logger = CliLogger::new(quiet);
            let sync_manager = SyncManager::new(manager, client, config).with_logger(logger);

            let max_bundles = if cmd.max_bundles > 0 {
                Some(cmd.max_bundles)
            } else {
                None
            };
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
