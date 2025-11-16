use anyhow::Result;
use clap::{Parser, Subcommand, ValueHint};
use plcbundle::*;
use std::path::PathBuf;

// CLI Commands (cmd_ prefix)
mod cmd_bench;
mod cmd_clean;
mod cmd_clone;
mod cmd_completions;
mod cmd_compare;
mod cmd_did;
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

/// Format custom help template with grouped commands
fn format_help_template() -> &'static str {
    concat!(
        "{about-with-newline}\n\n",
        "{usage-heading}\n  {usage}\n\n",
        "Options:\n{options}\n\n",
        "Repository Management:\n",
        "  init      Initialize a new bundle repository\n",
        "  clone     Clone a remote bundle repository\n",
        "  sync      Fetch new bundles from PLC directory\n",
        "  status    Show comprehensive repository status\n",
        "  ls        List bundles (machine-readable)\n",
        "\n",
        "Maintenance:\n",
        "  rebuild   Rebuild bundle index from existing files\n",
        "  migrate   Migrate bundles to new bundle format\n",
        "  clean     Remove all temporary files from the repository\n",
        "  rollback  Rollback repository to earlier state\n",
        "\n",
        "Query & Export:\n",
        "  query, q  Query bundles with JMESPath or simple path\n",
        "  export    Export operations to stdout or file\n",
        "  op        Operation queries and inspection\n",
        "  stats     Display statistics about bundles\n",
        "\n",
        "DID Operations:\n",
        "  did       DID operations and queries\n",
        "  handle    Resolve handle to DID\n",
        "  index     DID index management\n",
        "\n",
        "Verification:\n",
        "  verify    Verify bundle integrity and chain\n",
        "  compare   Compare bundle repositories\n",
        "  inspect   Deep analysis of bundle contents\n",
        "\n",
        "Server:\n",
        "  server    Start HTTP server\n",
        "\n",
        "Mempool:\n",
        "  mempool   Manage mempool operations\n",
        "\n",
        "Development:\n",
        "  bench     Benchmark bundle operations\n",
        "  random    Output random DIDs sampled from the index\n",
        "\n",
        "Utilities:\n",
        "  completions Generate shell completion scripts\n",
        "\n",
        "See 'plcbundle <COMMAND> --help' for more information on a specific command.\n"
    )
}

#[derive(Parser)]
#[command(bin_name = "plcbundle")]
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
#[command(help_template = format_help_template())]
pub struct Cli {
    /// Repository directory
    #[arg(short = 'C', long = "dir", global = true, default_value = ".", value_hint = ValueHint::DirPath)]
    dir: PathBuf,

    /// Suppress progress output
    #[arg(long, global = true)]
    quiet: bool,

    /// Enable verbose output
    #[arg(short = 'v', long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init(cmd_init::InitCommand),
    Clone(cmd_clone::CloneCommand),
    Sync(cmd_sync::SyncCommand),
    Status(cmd_status::StatusCommand),
    Ls(cmd_ls::LsCommand),
    Rebuild(cmd_rebuild::RebuildCommand),
    Migrate(cmd_migrate::MigrateCommand),
    Clean(cmd_clean::CleanCommand),
    Rollback(cmd_rollback::RollbackCommand),
    Query(cmd_query::QueryCommand),
    Export(cmd_export::ExportCommand),
    Op(cmd_op::OpCommand),
    Stats(cmd_stats::StatsCommand),
    Did(cmd_did::DidCommand),
    Handle(cmd_did::HandleCommand),
    Index(cmd_index::IndexCommand),
    Verify(cmd_verify::VerifyCommand),
    Compare(cmd_compare::CompareCommand),
    Inspect(cmd_inspect::InspectCommand),
    Server(cmd_server::ServerCommand),
    Mempool(cmd_mempool::MempoolCommand),
    Bench(cmd_bench::BenchCommand),
    Random(cmd_random::RandomCommand),
    Completions(cmd_completions::CompletionsCommand),
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logger based on verbosity flags
    logger::init_logger(cli.verbose, cli.quiet);

    match cli.command {
        Commands::Query(cmd) => cmd_query::run(cmd, cli.dir, cli.quiet, cli.verbose)?,
        Commands::Init(cmd) => cmd_init::run(cmd)?,
        Commands::Clone(cmd) => cmd_clone::run(cmd)?,
        Commands::Status(cmd) => cmd_status::run(cmd, cli.dir)?,
        Commands::Ls(cmd) => cmd_ls::run(cmd, cli.dir, cli.verbose, cli.quiet)?,
        Commands::Verify(cmd) => cmd_verify::run(cmd, cli.dir, cli.verbose)?,
        Commands::Export(cmd) => cmd_export::run(cmd, cli.dir, cli.quiet, cli.verbose)?,
        Commands::Op(cmd) => cmd_op::run(cmd, cli.dir, cli.quiet)?,
        Commands::Stats(cmd) => cmd_stats::run(cmd, cli.dir)?,
        Commands::Handle(cmd) => cmd_did::run_handle(cmd, cli.dir)?,
        Commands::Did(cmd) => cmd_did::run_did(cmd, cli.dir, cli.verbose)?,
        Commands::Index(cmd) => cmd_index::run(cmd, cli.dir, cli.verbose)?,
        Commands::Mempool(cmd) => cmd_mempool::run(cmd, cli.dir, cli.verbose)?,
        Commands::Sync(cmd) => cmd_sync::run(cmd, cli.dir, cli.quiet, cli.verbose)?,
        Commands::Rollback(cmd) => cmd_rollback::run(cmd, cli.dir, cli.verbose)?,
        Commands::Compare(cmd) => cmd_compare::run(cmd, cli.dir, cli.verbose)?,
        Commands::Inspect(cmd) => cmd_inspect::run(cmd, cli.dir)?,
        Commands::Server(cmd) => cmd_server::run(cmd, cli.dir, cli.verbose)?,
        Commands::Migrate(cmd) => cmd_migrate::run(cmd, cli.dir, cli.verbose)?,
        Commands::Rebuild(cmd) => cmd_rebuild::run(cmd, cli.dir, cli.verbose)?,
        Commands::Bench(cmd) => cmd_bench::run(cmd, cli.dir, cli.verbose)?,
        Commands::Random(cmd) => cmd_random::run(cmd, cli.dir)?,
        Commands::Clean(cmd) => cmd_clean::run(cmd, cli.dir, cli.verbose)?,
        Commands::Completions(cmd) => cmd_completions::run(cmd)?,
    }

    Ok(())
}

/// Macro to create clap help templates with examples
/// This works around the limitation that {bin} doesn't work in after_help
/// Uses env! macro to get binary name at compile time
#[macro_export]
macro_rules! clap_help {
    (examples: $examples:literal) => {{
        const BIN: &str = env!("CARGO_PKG_NAME");
        concat!(
            "{about-with-newline}\n",
            "{usage-heading} {usage}\n\n",
            "{all-args}\n\n",
            "Examples:\n",
            $examples
        ).replace("{bin}", BIN)
    }};

    (before: $before:literal, examples: $examples:literal) => {{
        const BIN: &str = env!("CARGO_PKG_NAME");
        concat!(
            "{about-with-newline}\n",
            $before, "\n\n",
            "{usage-heading} {usage}\n\n",
            "{all-args}\n\n",
            "Examples:\n",
            $examples
        ).replace("{bin}", BIN)
    }};
}
