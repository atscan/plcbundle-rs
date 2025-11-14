use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use plcbundle::*;
use std::path::PathBuf;

mod commands;

#[derive(Parser)]
#[command(name = "plcbundle")]
#[command(version, about = "High-performance PLC bundle query tool", long_about = None)]
#[command(author = "Your Name <you@example.com>")]
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
    /// Query bundles with JMESPath or simple path
    Query {
        /// Query expression (e.g., "did", "operation.type", etc.)
        expression: String,

        /// Bundle range (e.g., "1-10,15,20-25" or "latest:10" for last 10)
        #[arg(short, long)]
        bundles: Option<String>,

        /// Number of threads (0 = auto)
        #[arg(short = 'j', long, default_value = "0")]
        threads: usize,

        /// Query mode
        #[arg(short = 'm', long, default_value = "auto")]
        mode: QueryModeArg,

        /// Batch size for output
        #[arg(long, default_value = "2000")]
        batch_size: usize,

        /// Output format
        #[arg(short = 'f', long, default_value = "jsonl")]
        format: OutputFormat,

        /// Output file (default: stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Show statistics only, don't output results
        #[arg(long)]
        stats_only: bool,
    },

    /// Initialize a new PLC bundle repository
    ///
    /// Creates a new repository with an empty index file. Similar to 'git init'.
    #[command(
        after_help = "Examples:\n  \
            # Initialize in current directory\n  \
            plcbundle init\n\n  \
            # Initialize in specific directory\n  \
            plcbundle init /path/to/bundles\n\n  \
            # Set custom origin identifier\n  \
            plcbundle init --origin my-node\n\n  \
            # Force reinitialize existing repository\n  \
            plcbundle init --force"
    )]
    Init(commands::init::InitCommand),

    /// Show index and bundle information
    Info {
        /// Show detailed bundle information
        #[arg(short, long)]
        detailed: bool,

        /// Show specific bundle(s)
        #[arg(short, long)]
        bundle: Option<String>,

        /// Output format
        #[arg(short = 'f', long, default_value = "human")]
        format: InfoFormat,
    },

    /// Verify bundle integrity
    Verify {
        /// Bundle range to verify
        #[arg(short, long)]
        bundles: Option<String>,

        /// Verify checksums
        #[arg(long)]
        checksums: bool,

        /// Verify chain continuity
        #[arg(long)]
        chain: bool,

        /// Parallel verification
        #[arg(short = 'j', long, default_value = "0")]
        threads: usize,
    },

    /// Export operations to different formats
    Export {
        /// Bundle range (e.g., "1-100")
        #[arg(short, long)]
        range: Option<String>,

        /// Export all bundles
        #[arg(long)]
        all: bool,

        /// Bundle range (legacy, use --range instead)
        #[arg(long, hide = true)]
        bundles: Option<String>,

        /// Output format
        #[arg(short = 'f', long, default_value = "jsonl")]
        format: ExportFormat,

        /// Output file (default: stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Limit number of operations to export
        #[arg(long)]
        count: Option<usize>,

        /// Export operations after this timestamp (ISO 8601 format)
        #[arg(long)]
        after: Option<String>,

        /// Filter by DID
        #[arg(long)]
        did: Option<String>,

        /// Filter by operation type
        #[arg(long)]
        op_type: Option<String>,

        /// Compression
        #[arg(short = 'z', long)]
        compress: bool,
    },

    /// Operation queries and inspection
    ///
    /// Direct access to individual operations within bundles using either:
    ///   • Bundle number + position (e.g., 42 1337)
    ///   • Global position (e.g., 420000)
    ///
    /// Global position format: (bundleNumber × 10,000) + position
    /// Example: 88410345 = bundle 8841, position 345
    #[command(alias = "operation", alias = "record")]
    #[command(
        after_help = "Examples:\n  \
            # Get operation as JSON\n  \
            plcbundle op get 42 1337\n  \
            plcbundle op get 420000\n\n  \
            # Show operation (formatted)\n  \
            plcbundle op show 42 1337\n  \
            plcbundle op show 88410345\n\n  \
            # Find by CID\n  \
            plcbundle op find bafyreig3..."
    )]
    Op {
        #[command(subcommand)]
        command: OpCommands,
    },

    /// Display statistics about bundles
    Stats {
        /// Bundle range
        #[arg(short, long)]
        bundles: Option<String>,

        /// Statistics type
        #[arg(short = 't', long, default_value = "summary")]
        stat_type: StatType,

        /// Output format
        #[arg(short = 'f', long, default_value = "human")]
        format: InfoFormat,
    },

    /// DID operations and queries
    ///
    /// Query and analyze DIDs in the bundle repository. All commands
    /// require a DID index to be built for optimal performance.
    #[command(
        after_help = "Examples:\n  \
            # Resolve DID to current document\n  \
            plcbundle did resolve did:plc:524tuhdhh3m7li5gycdn6boe\n\n  \
            # Lookup all operations for a DID (TODO)\n  \
            plcbundle did lookup did:plc:524tuhdhh3m7li5gycdn6boe\n\n  \
            # Show complete audit log (TODO)\n  \
            plcbundle did history did:plc:524tuhdhh3m7li5gycdn6boe"
    )]
    Did {
        #[command(subcommand)]
        command: DIDCommands,
    },

    /// DID index management
    ///
    /// Manage the DID position index which maps DIDs to their bundle locations.
    /// This index enables fast O(1) DID lookups and is required for DID
    /// resolution and query operations.
    #[command(
        after_help = "Examples:\n  \
            # Build DID position index\n  \
            plcbundle index build\n\n  \
            # Repair DID index (rebuild from bundles)\n  \
            plcbundle index repair\n\n  \
            # Show DID index statistics\n  \
            plcbundle index stats\n\n  \
            # Verify DID index integrity\n  \
            plcbundle index verify"
    )]
    Index {
        #[command(subcommand)]
        command: IndexCommands,
    },

    /// Manage mempool operations
    ///
    /// The mempool stores operations waiting to be bundled. It maintains
    /// strict chronological order and automatically validates consistency.
    #[command(alias = "mp")]
    #[command(
        after_help = "Examples:\n  \
            # Show mempool status\n  \
            plcbundle mempool\n  \
            plcbundle mempool status\n\n  \
            # Clear all operations\n  \
            plcbundle mempool clear\n\n  \
            # Export operations as JSONL\n  \
            plcbundle mempool dump\n  \
            plcbundle mempool dump > operations.jsonl\n\n  \
            # Using alias\n  \
            plcbundle mp status"
    )]
    Mempool(commands::mempool::MempoolCommand),

    /// Fetch new bundles from PLC directory
    ///
    /// Download new operations from the PLC directory and create bundles.
    /// Similar to 'git fetch' - updates your local repository with new data.
    #[command(alias = "fetch")]
    #[command(
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
    Sync(commands::sync::SyncCommand),
}

#[derive(Subcommand)]
enum OpCommands {
    /// Get operation as JSON (machine-readable)
    ///
    /// Supports two input formats:
    ///   1. Bundle number + position: get 42 1337
    ///   2. Global position: get 420000
    ///
    /// Global position = (bundleNumber × 10,000) + position
    #[command(
        after_help = "Examples:\n  \
            # By bundle + position\n  \
            plcbundle op get 42 1337\n\n  \
            # By global position\n  \
            plcbundle op get 88410345\n\n  \
            # Pipe to jq\n  \
            plcbundle op get 42 1337 | jq .did"
    )]
    Get {
        /// Bundle number (or global position if only one arg)
        bundle: u32,

        /// Operation position within bundle (optional if using global position)
        position: Option<usize>,
    },

    /// Show operation with formatted output
    ///
    /// Displays operation in human-readable format with:
    ///   • Bundle location and global position
    ///   • DID and CID
    ///   • Timestamp
    ///   • Nullification status
    ///   • Parsed operation details
    ///   • Performance metrics (when not quiet)
    #[command(
        after_help = "Examples:\n  \
            # By bundle + position\n  \
            plcbundle op show 42 1337\n\n  \
            # By global position\n  \
            plcbundle op show 88410345\n\n  \
            # Quiet mode (minimal output)\n  \
            plcbundle op show 42 1337 -q"
    )]
    Show {
        /// Bundle number (or global position if only one arg)
        bundle: u32,

        /// Operation position within bundle (optional if using global position)
        position: Option<usize>,
    },

    /// Find operation by CID across all bundles
    ///
    /// Searches the entire repository for an operation with the given CID
    /// and returns its location (bundle + position).
    ///
    /// Note: This performs a full scan and can be slow on large repositories.
    #[command(
        after_help = "Examples:\n  \
            # Find by CID\n  \
            plcbundle op find bafyreig3tg4k...\n\n  \
            # Use with op get\n  \
            plcbundle op find bafyreig3... | awk '{print $3, $5}' | xargs plcbundle op get"
    )]
    Find {
        /// CID to search for
        cid: String,
    },
}

#[derive(Subcommand)]
enum DIDCommands {
    /// Resolve DID to current W3C DID document
    #[command(alias = "doc", alias = "document")]
    Resolve {
        /// DID to resolve
        did: String,

        /// Verbose output
        #[arg(short, long)]
        verbose: bool,
    },

    /// Find all operations for a DID (TODO)
    #[command(alias = "find", alias = "get")]
    Lookup {
        /// DID to lookup
        did: String,

        /// Verbose output
        #[arg(short, long)]
        verbose: bool,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Show complete DID audit log (TODO)
    #[command(alias = "log", alias = "audit")]
    History {
        /// DID to show history for
        did: String,

        /// Verbose output
        #[arg(short, long)]
        verbose: bool,

        /// Output as JSON
        #[arg(long)]
        json: bool,

        /// Compact one-line format
        #[arg(long)]
        compact: bool,

        /// Include nullified operations
        #[arg(long)]
        include_nullified: bool,
    },

    /// Process multiple DIDs from file or stdin (TODO)
    Batch {
        /// Action: lookup, resolve, export
        #[arg(long, default_value = "lookup")]
        action: String,

        /// Number of parallel workers
        #[arg(long, default_value = "4")]
        workers: usize,

        /// Output file
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Read from stdin
        #[arg(long)]
        stdin: bool,
    },

    /// Show DID activity statistics (TODO)
    Stats {
        /// DID to show stats for (omit for global stats)
        did: Option<String>,

        /// Show global index stats
        #[arg(long)]
        global: bool,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum IndexCommands {
    /// Build DID position index
    #[command(
        after_help = "Examples:\n  \
            # Build index\n  \
            plcbundle index build\n\n  \
            # Force rebuild from scratch\n  \
            plcbundle index build --force"
    )]
    Build {
        /// Rebuild even if index exists
        #[arg(long)]
        force: bool,
    },

    /// Repair DID index
    #[command(alias = "rebuild")]
    Repair {},

    /// Show DID index statistics
    #[command(alias = "info")]
    Stats {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Verify DID index integrity
    #[command(alias = "check")]
    Verify {
        /// Verbose output
        #[arg(short, long)]
        verbose: bool,
    },
}

#[derive(Debug, Clone, ValueEnum)]
pub enum QueryModeArg {
    /// Auto-detect based on query
    Auto,
    /// Simple path mode (faster)
    Simple,
    /// JMESPath mode (flexible)
    Jmespath,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormat {
    /// JSON Lines (one per line)
    Jsonl,
    /// Pretty JSON
    Json,
    /// CSV
    Csv,
    /// Plain text (values only)
    Plain,
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

#[derive(Debug, Clone, ValueEnum)]
pub enum ExportFormat {
    Jsonl,
    Json,
    Csv,
    Parquet,
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

// StdoutHandler moved to commands::query module

fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Initialize logger based on verbosity flags
    commands::logger::init_logger(cli.verbose, cli.quiet);

    match cli.command {
        Commands::Query {
            expression,
            bundles,
            threads,
            mode,
            batch_size,
            format,
            output,
            stats_only,
        } => {
            commands::query::cmd_query(
                expression,
                cli.dir,
                bundles,
                threads,
                mode,
                batch_size,
                format,
                output,
                stats_only,
                cli.quiet,
                cli.verbose,
            )?;
        }
        Commands::Init(cmd) => {
            commands::init::run(cmd)?;
        }

        Commands::Info {
            detailed,
            bundle,
            format,
        } => {
            cmd_info(cli.dir, detailed, bundle, format, cli.verbose)?;
        }
        Commands::Verify {
            bundles,
            checksums,
            chain,
            threads,
        } => {
            cmd_verify(cli.dir, bundles, checksums, chain, threads, cli.quiet)?;
        }
        Commands::Export {
            range,
            all,
            bundles,
            format,
            output,
            count,
            after,
            did,
            op_type,
            compress,
        } => {
            commands::export::cmd_export(
                cli.dir, range, all, bundles, format, output, count, after,
                did, op_type, compress, cli.quiet, cli.verbose
            )?;
        }
        Commands::Op { command } => {
            match command {
                OpCommands::Get { bundle, position } => {
                    commands::op::cmd_op_get(cli.dir.clone(), bundle, position, cli.quiet)?;
                }
                OpCommands::Show { bundle, position } => {
                    commands::op::cmd_op_show(cli.dir.clone(), bundle, position, cli.quiet)?;
                }
                OpCommands::Find { cid } => {
                    commands::op::cmd_op_find(cli.dir.clone(), cid, cli.quiet)?;
                }
            }
        }
        Commands::Stats {
            bundles,
            stat_type,
            format,
        } => {
            cmd_stats(cli.dir, bundles, stat_type, format)?;
        }

        Commands::Did { command } => {
            match command {
                DIDCommands::Resolve { did, verbose } => {
                    commands::did::cmd_did_resolve(cli.dir, did, verbose)?;
                }
                DIDCommands::Lookup { did, verbose, json } => {
                    commands::did::cmd_did_lookup(cli.dir, did, verbose, json)?;
                }
                DIDCommands::History { did, verbose, json, compact, include_nullified } => {
                    commands::did::cmd_did_history(cli.dir, did, verbose, json, compact, include_nullified)?;
                }
                DIDCommands::Batch { action, workers, output, stdin } => {
                    commands::did::cmd_did_batch(cli.dir, action, workers, output, stdin)?;
                }
                DIDCommands::Stats { did, global, json } => {
                    commands::did::cmd_did_stats(cli.dir, did, global, json)?;
                }
            }
        }

        Commands::Index { command } => {
            match command {
                IndexCommands::Build { force } => {
                    commands::index::cmd_index_build(cli.dir, force)?;
                }
                IndexCommands::Repair {} => {
                    commands::index::cmd_index_repair(cli.dir)?;
                }
                IndexCommands::Stats { json } => {
                    commands::index::cmd_index_stats(cli.dir, json)?;
                }
                IndexCommands::Verify { verbose } => {
                    commands::index::cmd_index_verify(cli.dir, verbose)?;
                }
            }
        }

        Commands::Mempool(cmd) => {
            commands::mempool::run(cmd)?;
        }

        Commands::Sync(cmd) => {
            commands::sync::run(cmd)?;
        }
    }

    Ok(())
}

// cmd_query moved to commands::query module

fn cmd_info(
    dir: PathBuf,
    detailed: bool,
    bundle_spec: Option<String>,
    format: InfoFormat,
    verbose: bool,
) -> Result<()> {
    let index = Index::load(&dir)?;

    match format {
        InfoFormat::Human => {
            println!("📦 PLC Bundle Index");
            println!("═══════════════════════════════════════");
            println!("Version:       {}", index.version);
            println!("Origin:        {}", index.origin);
            println!("Last Bundle:   #{}", index.last_bundle);
            println!("Updated:       {}", index.updated_at);
            println!("Total Size:    {}", commands::utils::format_bytes(index.total_size_bytes));
            println!("Uncompressed:  {}", commands::utils::format_bytes(index.total_uncompressed_size_bytes));
            println!(
                "Compression:   {:.1}%",
                (1.0 - index.total_size_bytes as f64 / index.total_uncompressed_size_bytes as f64)
                    * 100.0
            );

            if let Some(spec) = bundle_spec {
                let bundles = commands::utils::parse_bundle_spec(Some(spec), index.last_bundle)?;
                println!("\n📊 Bundle Details");
                println!("───────────────────────────────────────");
                for bundle_num in bundles {
                    if let Some(meta) = index.get_bundle(bundle_num) {
                        print_bundle_info(meta, detailed, verbose);
                    }
                }
            } else if detailed {
                println!("\n📊 All Bundles");
                println!("───────────────────────────────────────");
                for meta in &index.bundles {
                    print_bundle_info(meta, detailed, verbose);
                }
            }
        }
        InfoFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&index)?);
        }
        InfoFormat::Yaml => {
            // Would need yaml dependency
            eprintln!("YAML format not yet implemented");
        }
        InfoFormat::Table => {
            // Would need table formatting
            eprintln!("Table format not yet implemented");
        }
    }

    Ok(())
}

fn print_bundle_info(meta: &BundleMetadata, detailed: bool, _verbose: bool) {
    println!("\nBundle #{}", meta.bundle_number);
    println!("  Time Range:  {} → {}", meta.start_time, meta.end_time);
    println!("  Operations:  {}", commands::utils::format_number(meta.operation_count as u64));
    println!("  DIDs:        {}", commands::utils::format_number(meta.did_count as u64));
    println!("  Size:        {} ({} compressed)",
        commands::utils::format_bytes(meta.uncompressed_size),
        commands::utils::format_bytes(meta.compressed_size)
    );
    if detailed {
        println!("  Hash:        {}", meta.hash);
        println!("  Content:     {}", meta.content_hash);
        println!("  Compressed:  {}", meta.compressed_hash);
        if !meta.parent.is_empty() {
            println!("  Parent:      {}", meta.parent);
        }
        if !meta.cursor.is_empty() {
            println!("  Cursor:      {}", meta.cursor);
        }
    }
}

fn cmd_verify(
    dir: PathBuf,
    bundles_spec: Option<String>,
    _checksums: bool,
    _chain: bool,
    _threads: usize,
    quiet: bool,
) -> Result<()> {
    let index = Index::load(&dir)?;
    let bundle_numbers = commands::utils::parse_bundle_spec(bundles_spec, index.last_bundle)?;

    if !quiet {
        eprintln!("🔍 Verifying {} bundles...", bundle_numbers.len());
    }

    // Verification logic would go here
    println!("Verification not yet fully implemented");

    Ok(())
}

// cmd_export moved to commands::export module

fn cmd_stats(
    _dir: PathBuf,
    _bundles: Option<String>,
    _stat_type: StatType,
    _format: InfoFormat,
) -> Result<()> {
    println!("Stats not yet implemented");
    Ok(())
}