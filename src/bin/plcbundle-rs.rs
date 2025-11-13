use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle, HumanDuration};
use plcbundle::*;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Parser)]
#[command(name = "plcbundle")]
#[command(version, about = "High-performance PLC bundle query tool", long_about = None)]
#[command(author = "Your Name <you@example.com>")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Suppress progress output
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Enable verbose output
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Query bundles with JMESPath or simple path
    Query {
        /// Query expression (e.g., "did", "operation.type", etc.)
        expression: String,

        /// Bundle directory
        #[arg(short = 'd', long, default_value = ".")]
        dir: PathBuf,

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

    /// Show index and bundle information
    Info {
        /// Bundle directory
        #[arg(short = 'd', long, default_value = ".")]
        dir: PathBuf,

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
        /// Bundle directory
        #[arg(short = 'd', long, default_value = ".")]
        dir: PathBuf,

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
        /// Bundle directory
        #[arg(short = 'd', long, default_value = ".")]
        dir: PathBuf,

        /// Bundle range
        #[arg(short, long)]
        bundles: Option<String>,

        /// Output format
        #[arg(short = 'f', long, default_value = "jsonl")]
        format: ExportFormat,

        /// Output file
        #[arg(short, long)]
        output: PathBuf,

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

    /// Display statistics about bundles
    Stats {
        /// Bundle directory
        #[arg(short = 'd', long, default_value = ".")]
        dir: PathBuf,

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

    /// Interactive shell mode
    #[cfg(feature = "interactive")]
    Shell {
        /// Bundle directory
        #[arg(short = 'd', long, default_value = ".", env = "PLC_BUNDLE_DIR")]
        dir: PathBuf,
    },
}

#[derive(Debug, Clone, ValueEnum)]
enum QueryModeArg {
    /// Auto-detect based on query
    Auto,
    /// Simple path mode (faster)
    Simple,
    /// JMESPath mode (flexible)
    Jmespath,
}

#[derive(Debug, Clone, ValueEnum)]
enum OutputFormat {
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
enum InfoFormat {
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
enum ExportFormat {
    Jsonl,
    Json,
    Csv,
    Parquet,
}

#[derive(Debug, Clone, ValueEnum)]
enum StatType {
    /// Summary statistics
    Summary,
    /// Operation type distribution
    Operations,
    /// DID statistics
    Dids,
    /// Timeline statistics
    Timeline,
}

struct StdoutHandler {
    lock: Mutex<()>,
    stats_only: bool,
}

impl StdoutHandler {
    fn new(stats_only: bool) -> Self {
        Self {
            lock: Mutex::new(()),
            stats_only,
        }
    }
}

impl OutputHandler for StdoutHandler {
    fn write_batch(&self, batch: &str) -> Result<()> {
        if self.stats_only {
            return Ok(());
        }
        let _lock = self.lock.lock().unwrap();
        io::stdout().write_all(batch.as_bytes())?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Query {
            expression,
            dir,
            bundles,
            threads,
            mode,
            batch_size,
            format,
            output,
            stats_only,
        } => {
            cmd_query(
                expression,
                dir,
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
        Commands::Info {
            dir,
            detailed,
            bundle,
            format,
        } => {
            cmd_info(dir, detailed, bundle, format, cli.verbose)?;
        }
        Commands::Verify {
            dir,
            bundles,
            checksums,
            chain,
            threads,
        } => {
            cmd_verify(dir, bundles, checksums, chain, threads, cli.quiet)?;
        }
        Commands::Export {
            dir,
            bundles,
            format,
            output,
            did,
            op_type,
            compress,
        } => {
            cmd_export(dir, bundles, format, output, did, op_type, compress, cli.quiet)?;
        }
        Commands::Stats {
            dir,
            bundles,
            stat_type,
            format,
        } => {
            cmd_stats(dir, bundles, stat_type, format)?;
        }
    }

    Ok(())
}

fn cmd_query(
    expression: String,
    dir: PathBuf,
    bundles_spec: Option<String>,
    threads: usize,
    mode: QueryModeArg,
    batch_size: usize,
    _format: OutputFormat,
    _output: Option<PathBuf>,
    stats_only: bool,
    quiet: bool,
    verbose: bool,
) -> Result<()> {
    let num_threads = if threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        threads
    };

    // Auto-detect query mode
    let query_mode = match mode {
        QueryModeArg::Auto => {
            if expression.contains('[') || expression.contains('|') || expression.contains('@') {
                QueryMode::JmesPath
            } else {
                QueryMode::Simple
            }
        }
        QueryModeArg::Simple => QueryMode::Simple,
        QueryModeArg::Jmespath => QueryMode::JmesPath,
    };

    let options = OptionsBuilder::new()
        .directory(dir)
        .query(expression.clone())
        .query_mode(query_mode)
        .num_threads(num_threads)
        .batch_size(batch_size)
        .build();

    let processor = Processor::new(options)?;
    let index = processor.load_index()?;

    if verbose && !quiet {
        eprintln!("📦 Index: v{} ({})", index.version, index.origin);
        eprintln!("📊 Total bundles: {}", index.last_bundle);
    }

    let bundle_numbers = parse_bundle_spec(bundles_spec, index.last_bundle)?;

    if !quiet {
        let mode_str = match query_mode {
            QueryMode::Simple => "simple",
            QueryMode::JmesPath => "jmespath",
        };
        eprintln!(
            "🔍 Processing {} bundles | {} mode | {} threads\n",
            bundle_numbers.len(),
            mode_str,
            num_threads
        );
    }

    let pb = if quiet {
        ProgressBar::hidden()
    } else {
        let pb = ProgressBar::new(bundle_numbers.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
                )?
                .progress_chars("█▓▒░ "),
        );
        pb
    };

    let start = Instant::now();
    let output = Arc::new(StdoutHandler::new(stats_only));

    let stats = processor.process(
        &bundle_numbers,
        output,
        Some(|_, stats: &Stats| {
            if !quiet {
                let elapsed = start.elapsed().as_secs_f64();
                let ops_per_sec = if elapsed > 0.0 {
                    stats.operations as f64 / elapsed
                } else {
                    0.0
                };
                pb.set_message(format!(
                    "✓ {} matches | {:.1} ops/s",
                    format_number(stats.matches as u64),
                    ops_per_sec
                ));
                pb.inc(1);
            }
        }),
    )?;

    pb.finish_and_clear();

    let elapsed = start.elapsed();
    let match_pct = if stats.operations > 0 {
        (stats.matches as f64 / stats.operations as f64) * 100.0
    } else {
        0.0
    };

    if !quiet {
        eprintln!("\n✅ Complete in {}", HumanDuration(elapsed));
        eprintln!("   Operations: {} ({:.2}% matched)", 
            format_number(stats.operations as u64), match_pct);
        eprintln!("   Data processed: {}", format_bytes(stats.total_bytes));
        if elapsed.as_secs_f64() > 0.0 {
            eprintln!("   Throughput: {:.0} ops/sec | {}/s",
                stats.operations as f64 / elapsed.as_secs_f64(),
                format_bytes((stats.total_bytes as f64 / elapsed.as_secs_f64()) as u64)
            );
        }
    }

    Ok(())
}

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
            println!("Total Size:    {}", format_bytes(index.total_size_bytes));
            println!("Uncompressed:  {}", format_bytes(index.total_uncompressed_size_bytes));
            println!(
                "Compression:   {:.1}%",
                (1.0 - index.total_size_bytes as f64 / index.total_uncompressed_size_bytes as f64)
                    * 100.0
            );

            if let Some(spec) = bundle_spec {
                let bundles = parse_bundle_spec(Some(spec), index.last_bundle)?;
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
    println!("  Operations:  {}", format_number(meta.operation_count as u64));
    println!("  DIDs:        {}", format_number(meta.did_count as u64));
    println!("  Size:        {} ({} compressed)",
        format_bytes(meta.uncompressed_size),
        format_bytes(meta.compressed_size)
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
    let bundle_numbers = parse_bundle_spec(bundles_spec, index.last_bundle)?;

    if !quiet {
        eprintln!("🔍 Verifying {} bundles...", bundle_numbers.len());
    }

    // Verification logic would go here
    println!("Verification not yet fully implemented");

    Ok(())
}

fn cmd_export(
    _dir: PathBuf,
    _bundles: Option<String>,
    _format: ExportFormat,
    _output: PathBuf,
    _did: Option<String>,
    _op_type: Option<String>,
    _compress: bool,
    _quiet: bool,
) -> Result<()> {
    println!("Export not yet implemented");
    Ok(())
}

fn cmd_stats(
    _dir: PathBuf,
    _bundles: Option<String>,
    _stat_type: StatType,
    _format: InfoFormat,
) -> Result<()> {
    println!("Stats not yet implemented");
    Ok(())
}

fn parse_bundle_spec(spec: Option<String>, max_bundle: u32) -> Result<Vec<u32>> {
    match spec {
        None => Ok((1..=max_bundle).collect()),
        Some(s) => {
            if s.starts_with("latest:") {
                let count: u32 = s.strip_prefix("latest:").unwrap().parse()?;
                let start = max_bundle.saturating_sub(count - 1);
                Ok((start..=max_bundle).collect())
            } else {
                parse_bundle_range(&s, max_bundle)
            }
        }
    }
}

fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        format!("{} {}", bytes, UNITS[0])
    } else {
        format!("{:.2} {}", size, UNITS[unit_idx])
    }
}