use anyhow::Result;
use clap::Args;
use plcbundle::{BundleManager, sync::PLCClient};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Args)]
pub struct SyncCommand {
    /// PLC directory URL
    #[arg(long, default_value = "https://plc.directory")]
    pub plc: String,

    /// Keep syncing (run as daemon)
    #[arg(long)]
    pub continuous: bool,

    /// Sync interval for continuous mode
    #[arg(long, default_value = "60s", value_parser = parse_duration)]
    pub interval: Duration,

    /// Maximum bundles to fetch (0 = all)
    #[arg(long, default_value = "0")]
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
            println!("Syncing from: {}", cmd.dir.display());
            println!("PLC Directory: {}", cmd.plc);
            if cmd.continuous {
                println!("Mode: continuous (interval: {:?})", cmd.interval);
            }
            println!();
        }

        let client = PLCClient::new(&cmd.plc)?;
        let mut manager = BundleManager::new(cmd.dir)?.with_verbose(cmd.verbose);

        if cmd.continuous {
            run_continuous(&client, &mut manager, cmd.interval, cmd.max_bundles, cmd.quiet).await
        } else {
            run_once(&client, &mut manager, cmd.quiet).await
        }
    })
}

async fn run_once(
    client: &PLCClient,
    manager: &mut BundleManager,
    quiet: bool,
) -> Result<()> {
    let synced = manager.sync_once(client).await?;

    if !quiet {
        if synced == 0 {
            eprintln!("\n✓ Already up to date");
        } else {
            eprintln!("\n✓ Sync complete: {} bundle(s) fetched", synced);
        }
    }

    Ok(())
}

async fn run_continuous(
    client: &PLCClient,
    manager: &mut BundleManager,
    interval: Duration,
    max_bundles: usize,
    quiet: bool,
) -> Result<()> {
    let mut total_synced = 0;

    loop {
        let synced = manager.sync_once(client).await?;
        total_synced += synced;

        if !quiet && synced > 0 {
            eprintln!("✓ Synced {} bundles (total: {})", synced, total_synced);
        }

        if max_bundles > 0 && total_synced >= max_bundles {
            if !quiet {
                eprintln!("\n✓ Reached max bundles limit ({})", max_bundles);
            }
            break;
        }

        tokio::time::sleep(interval).await;
    }

    Ok(())
}
