use anyhow::Result;
use clap::{Args, ValueEnum};
use std::path::PathBuf;

#[derive(Args)]
#[command(about = "Display statistics about bundles")]
pub struct StatsCommand {
    /// Bundle range
    #[arg(short, long)]
    pub bundles: Option<String>,

    /// Statistics type
    #[arg(short = 't', long, default_value = "summary")]
    pub stat_type: StatType,

    /// Output format
    #[arg(short = 'f', long, default_value = "human")]
    pub format: InfoFormat,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum StatType {
    /// Summary statistics
    Summary,
    /// Operation type distribution
    Operations,
    /// DID statistics
    Dids,
    /// Timeline statistics
    Timeline,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum InfoFormat {
    /// Human-readable output
    Human,
    /// JSON output
    Json,
    /// YAML output
    Yaml,
    /// Table format
    Table,
}

pub fn run(_cmd: StatsCommand, _dir: PathBuf) -> Result<()> {
    println!("Stats not yet implemented");
    Ok(())
}
