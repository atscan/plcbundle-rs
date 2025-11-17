use super::utils;
use super::utils::HasGlobalFlags;
use anyhow::Result;
use clap::{Args, ValueHint};
use plcbundle::{
    constants,
    plc_client::PLCClient,
    sync::{CliLogger, ServerLogger, SyncConfig, SyncManager},
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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

#[derive(Args)]
#[command(
    about = "Fetch new bundles from PLC directory",
    long_about = "Download new operations from the PLC directory and create bundles in your
local repository. Similar to 'git fetch', this command updates your repository
with new data from the remote source without modifying existing bundles.

The command fetches operations starting from where your repository left off,
creates new bundles when 10,000 operations accumulate, and updates the index.
Use --max-bundles to limit how many bundles are fetched in a single run, or
use --continuous to run as a daemon that periodically checks for and fetches
new bundles.

In continuous mode, the command runs indefinitely, checking for new bundles
at the specified interval (default 60 seconds). This is useful for keeping
a repository up-to-date automatically. For one-time syncs, omit --continuous.

This is the primary way to populate and update your repository with data from
the PLC directory. The command handles rate limiting, error recovery, and
maintains chain integrity throughout the sync process.",
    alias = "fetch",
    help_template = crate::clap_help!(
        examples: "  # Fetch new bundles once\n  \
                   {bin} sync\n\n  \
                   # Run continuously (daemon mode)\n  \
                   {bin} sync --continuous\n\n  \
                   # Custom sync interval\n  \
                   {bin} sync --continuous --interval 30s\n\n  \
                   # Fetch maximum 10 bundles then stop\n  \
                   {bin} sync --max-bundles 10"
    )
)]
pub struct SyncCommand {
    /// PLC directory URL
    #[arg(long, default_value = constants::DEFAULT_PLC_DIRECTORY_URL, value_hint = ValueHint::Url)]
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

}

impl HasGlobalFlags for SyncCommand {
    fn verbose(&self) -> bool { false }
    fn quiet(&self) -> bool { false }
}

pub fn run(cmd: SyncCommand, dir: PathBuf, global_quiet: bool, global_verbose: bool) -> Result<()> {
    tokio::runtime::Runtime::new()?.block_on(async {
        if !global_quiet {
            println!("Syncing from: {}", utils::display_path(&dir).display());
            println!("PLC Directory: {}", cmd.plc);
            if cmd.continuous {
                println!("Mode: continuous (interval: {:?})", cmd.interval);
            }
            println!();
        }

        let client = PLCClient::new(&cmd.plc)?;
        let manager = Arc::new(super::utils::create_manager(dir.clone(), global_verbose, global_quiet)?);

        let config = SyncConfig {
            plc_url: cmd.plc.clone(),
            continuous: cmd.continuous,
            interval: cmd.interval,
            max_bundles: cmd.max_bundles,
            verbose: global_verbose,
            shutdown_rx: None,
            shutdown_tx: None,
        };

        let quiet = global_quiet;

        if cmd.continuous {
            // For continuous mode, use run_continuous() with ServerLogger to enable verbose toggle
            let config = SyncConfig {
                plc_url: cmd.plc.clone(),
                continuous: true,
                interval: cmd.interval,
                max_bundles: 0,
                verbose: global_verbose,
                shutdown_rx: None,
                shutdown_tx: None,
            };

            let logger = ServerLogger::new(global_verbose, cmd.interval);
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
