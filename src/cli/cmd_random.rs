use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args, Debug)]
#[command(about = "Output random DIDs sampled from the index")]
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
    let manager = super::utils::create_manager(dir, false, false)?;
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
