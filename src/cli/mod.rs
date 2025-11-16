use anyhow::Result;
use clap::{Parser, Subcommand};
use plcbundle::*;
use std::path::PathBuf;

// CLI Commands (cmd_ prefix)
mod cmd_bench;
mod cmd_did;
mod cmd_diff;
mod cmd_export;
mod cmd_index;
mod cmd_init;
mod cmd_inspect;
mod cmd_ls;
mod cmd_mempool;
mod cmd_migrate;
mod cmd_op;
mod cmd_query;
mod cmd_random;
mod cmd_rebuild;
mod cmd_rollback;
mod cmd_server;
mod cmd_stats;
mod cmd_status;
mod cmd_sync;
mod cmd_verify;

// Helper modules (no cmd_ prefix)
mod logger;
mod progress;
mod utils;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(name = "plcbundle")]
#[command(version = VERSION)]
#[command(about = concat!("plcbundle v", env!("CARGO_PKG_VERSION"), " (rust) - DID PLC Bundle Management"))]
#[command(long_about = concat!(
    "plcbundle v", env!("CARGO_PKG_VERSION"), " - DID PLC Bundle Management\n\n",
    "Tool for archiving AT Protocol's DID PLC Directory operations\n",
    "into immutable, cryptographically-chained bundles of 10,000\n",
    "operations each.\n\n",
    "Documentation: https://tangled.org/@atscan.net/plcbundle"
))]
#[command(author)]
#[command(propagate_version = true)]
struct Cli {
    /// Repository directory
    #[arg(short = 'C', long = "dir", global = true, default_value = ".")]
    dir: PathBuf,

    /// Suppress progress output
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Enable verbose output
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Query(cmd_query::QueryCommand),
    Init(cmd_init::InitCommand),
    Status(cmd_status::StatusCommand),
    Ls(cmd_ls::LsCommand),
    Verify(cmd_verify::VerifyCommand),
    Export(cmd_export::ExportCommand),
    Op(cmd_op::OpCommand),
    Stats(cmd_stats::StatsCommand),
    Did(cmd_did::DidCommand),
    Handle(cmd_did::HandleCommand),
    Index(cmd_index::IndexCommand),
    Mempool(cmd_mempool::MempoolCommand),
    Sync(cmd_sync::SyncCommand),
    Rollback(cmd_rollback::RollbackCommand),
    Diff(cmd_diff::DiffCommand),
    Inspect(cmd_inspect::InspectCommand),
    Server(cmd_server::ServerCommand),
    Migrate(cmd_migrate::MigrateCommand),
    Rebuild(cmd_rebuild::RebuildCommand),
    Bench(cmd_bench::BenchCommand),
    Random(cmd_random::RandomCommand),
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logger based on verbosity flags
    logger::init_logger(cli.verbose, cli.quiet);

    match cli.command {
        Commands::Query(cmd) => cmd_query::run(cmd, cli.dir, cli.quiet, cli.verbose)?,
        Commands::Init(cmd) => cmd_init::run(cmd)?,
        Commands::Status(cmd) => cmd_status::run(cmd, cli.dir)?,
        Commands::Ls(cmd) => cmd_ls::run(cmd, cli.dir, cli.verbose, cli.quiet)?,
        Commands::Verify(cmd) => cmd_verify::run(cmd, cli.dir, cli.verbose)?,
        Commands::Export(cmd) => cmd_export::run(cmd, cli.dir, cli.quiet, cli.verbose)?,
        Commands::Op(cmd) => cmd_op::run(cmd, cli.dir, cli.quiet)?,
        Commands::Stats(cmd) => cmd_stats::run(cmd, cli.dir)?,
        Commands::Handle(cmd) => cmd_did::run_handle(cmd, cli.dir)?,
        Commands::Did(cmd) => cmd_did::run_did(cmd, cli.dir)?,
        Commands::Index(cmd) => cmd_index::run(cmd, cli.dir)?,
        Commands::Mempool(cmd) => cmd_mempool::run(cmd)?,
        Commands::Sync(cmd) => cmd_sync::run(cmd)?,
        Commands::Rollback(cmd) => cmd_rollback::run(cmd, cli.dir)?,
        Commands::Diff(cmd) => cmd_diff::run(cmd, cli.dir)?,
        Commands::Inspect(cmd) => cmd_inspect::run(cmd, cli.dir)?,
        Commands::Server(cmd) => cmd_server::run(cmd, cli.dir)?,
        Commands::Migrate(cmd) => cmd_migrate::run(cmd, cli.dir, cli.verbose)?,
        Commands::Rebuild(cmd) => cmd_rebuild::run(cmd, cli.dir, cli.verbose)?,
        Commands::Bench(cmd) => cmd_bench::run(cmd, cli.dir)?,
        Commands::Random(cmd) => cmd_random::run(cmd, cli.dir)?,
    }

    Ok(())
}
