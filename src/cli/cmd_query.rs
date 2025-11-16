use anyhow::Result;
use clap::{Args, ValueEnum};
use plcbundle::*;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::progress::ProgressBar as CustomProgressBar;
use super::utils;

#[derive(Args)]
#[command(
    about = "Query bundles with JMESPath or simple path",
    long_about = "Extract and filter operations from bundles using powerful query expressions.

The command supports three query modes:
  • auto     - Automatically detect based on query syntax (default)
  • simple   - Fast path extraction (e.g., 'did', 'operation.type')
  • jmespath - Full JMESPath expressions for complex filtering and transformation

Simple path mode is optimized for fast single-field access, while JMESPath mode
enables complex queries with filtering, projection, and transformation. The
command automatically detects which mode to use based on the query syntax, or you
can explicitly specify with --mode.

Queries are executed in parallel across multiple bundles for maximum
performance. Results are streamed as JSONL (one JSON object per line) by
default, making it easy to pipe to other tools like jq or process in scripts.

Use --json for pretty-printed JSON output, or --stats-only to see query
statistics without outputting results.",
    alias = "q"
)]
pub struct QueryCommand {
    /// Query expression (e.g., "did", "operation.type", etc.)
    pub expression: String,

    /// Bundle range (e.g., "1-10,15,20-25" or "latest:10" for last 10)
    #[arg(short, long)]
    pub bundles: Option<String>,

    /// Number of threads (0 = auto)
    #[arg(short = 'j', long, default_value = "0")]
    pub threads: usize,

    /// Query mode
    #[arg(short = 'm', long, default_value = "auto")]
    pub mode: QueryModeArg,

    /// Batch size for output
    #[arg(long, default_value = "2000")]
    pub batch_size: usize,

    /// Output as pretty-printed JSON (default: JSONL, one JSON object per line)
    #[arg(long)]
    pub json: bool,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Show statistics only, don't output results
    #[arg(long)]
    pub stats_only: bool,
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

pub struct StdoutHandler {
    lock: Mutex<()>,
    stats_only: bool,
}

pub fn run(cmd: QueryCommand, dir: PathBuf, quiet: bool, verbose: bool) -> Result<()> {
    let expression = cmd.expression;
    let bundles_spec = cmd.bundles;
    let threads = cmd.threads;
    let mode = cmd.mode;
    let batch_size = cmd.batch_size;
    let _output = cmd.output;
    let stats_only = cmd.stats_only;
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
        log::debug!("📦 Index: v{} ({})", index.version, index.origin);
        log::debug!("📊 Total bundles: {}", index.last_bundle);
    }

    let bundle_numbers = utils::parse_bundle_spec(bundles_spec, index.last_bundle)?;

    if !quiet {
        let mode_str = match query_mode {
            QueryMode::Simple => "simple",
            QueryMode::JmesPath => "jmespath",
        };
        log::debug!(
            "🔍 Processing {} bundles | {} mode | {} threads\n",
            bundle_numbers.len(),
            mode_str,
            num_threads
        );
    }

    // Calculate total uncompressed size for progress tracking
    let total_uncompressed_size = index.total_uncompressed_size_for_bundles(&bundle_numbers);

    let pb = if quiet {
        None
    } else {
        Some(CustomProgressBar::with_bytes(
            bundle_numbers.len(),
            total_uncompressed_size,
        ))
    };

    let start = Instant::now();
    let output = Arc::new(StdoutHandler::new(stats_only));
    
    // Track bundle count separately since callback gives increment, not total
    let bundle_count = Arc::new(Mutex::new(0usize));
    let pb_arc = if let Some(ref pb) = pb {
        Some(Arc::new(Mutex::new(pb)))
    } else {
        None
    };

    let stats = processor.process(
        &bundle_numbers,
        output,
        Some({
            let pb_arc = pb_arc.clone();
            let bundle_count = bundle_count.clone();
            move |_increment, stats: &Stats| {
                if let Some(ref pb_mutex) = pb_arc {
                    let mut count = bundle_count.lock().unwrap();
                    *count += 1;
                    let current_bundles = *count;
                    drop(count);
                    
                    let pb = pb_mutex.lock().unwrap();
                    
                    // Update progress with bundles processed and bytes
                    pb.set_with_bytes(current_bundles, stats.total_bytes);
                    
                    // Set message with matches
                    pb.set_message(format!(
                        "✓ {} matches",
                        utils::format_number(stats.matches as u64)
                    ));
                }
            }
        }),
    )?;

    if let Some(ref pb) = pb {
        pb.finish();
    }

    let elapsed = start.elapsed();
    let match_pct = if stats.operations > 0 {
        (stats.matches as f64 / stats.operations as f64) * 100.0
    } else {
        0.0
    };

    if !quiet {
        log::info!("\n✅ Complete in {:.2}s", elapsed.as_secs_f64());
        log::info!(
            "   Operations: {} ({:.2}% matched)",
            utils::format_number(stats.operations as u64),
            match_pct
        );
        log::info!(
            "   Data processed: {}",
            utils::format_bytes(stats.total_bytes)
        );
        if elapsed.as_secs_f64() > 0.0 {
            log::info!(
                "   Throughput: {:.0} ops/sec | {}/s",
                stats.operations as f64 / elapsed.as_secs_f64(),
                utils::format_bytes((stats.total_bytes as f64 / elapsed.as_secs_f64()) as u64)
            );
        }
    }

    Ok(())
}

impl StdoutHandler {
    pub fn new(stats_only: bool) -> Self {
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
        std::io::stdout().write_all(batch.as_bytes())?;
        Ok(())
    }
}
