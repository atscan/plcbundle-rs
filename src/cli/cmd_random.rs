use anyhow::Result;
use clap::Args;
use plcbundle::BundleManager;
use std::path::PathBuf;

/// Sample random DIDs without touching bundle files.
#[derive(Args, Debug)]
pub struct RandomCommand {
    /// Number of random DIDs to output
    #[arg(short = 'n', long = "count", default_value = "10")]
    pub count: usize,

    /// Optional deterministic seed
    #[arg(long)]
    pub seed: Option<u64>,

    /// Emit JSON array instead of newline-delimited text
    #[arg(long)]
    pub json: bool,
}

pub fn run(cmd: RandomCommand, dir: PathBuf) -> Result<()> {
    let manager = BundleManager::new(dir)?;
    let count = cmd.count.max(1);
    let dids = manager.sample_random_dids(count, cmd.seed)?;

    if cmd.json {
        println!("{}", serde_json::to_string_pretty(&dids)?);
    } else {
        for did in dids {
            println!("{}", did);
        }
    }

    Ok(())
}
