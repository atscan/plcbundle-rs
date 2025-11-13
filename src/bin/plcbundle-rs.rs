use anyhow::Result;
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use plcbundle::*;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "plcbundle-rs")]
#[command(version, about = "High-performance PLC bundle query tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Query bundles with JMESPath or simple path
    Query {
        /// Query expression
        expression: String,
        
        /// Bundle directory
        #[arg(short = 'd', long, default_value = ".")]
        dir: PathBuf,
        
        /// Bundle range (e.g., "1-10,15,20-25")
        #[arg(short, long)]
        bundles: Option<String>,
        
        /// Number of threads (0 = auto)
        #[arg(short = 'j', long, default_value = "0")]
        threads: usize,
        
        /// Use simple query mode (faster for simple paths)
        #[arg(long)]
        simple: bool,
        
        /// Batch size for output
        #[arg(long, default_value = "2000")]
        batch_size: usize,
    },
    
    /// Show index information
    Info {
        /// Bundle directory
        #[arg(short, long, default_value = ".")]
        dir: PathBuf,
    },
    
    /// Verify bundle integrity
    Verify {
        /// Bundle directory
        #[arg(short, long, default_value = ".")]
        dir: PathBuf,
        
        /// Bundle range to verify
        #[arg(short, long)]
        bundles: Option<String>,
    },
}

struct StdoutHandler {
    lock: Mutex<()>,
}

impl StdoutHandler {
    fn new() -> Self {
        Self {
            lock: Mutex::new(()),
        }
    }
}

impl OutputHandler for StdoutHandler {
    fn write_batch(&self, batch: &str) -> Result<()> {
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
            simple,
            batch_size,
        } => {
            cmd_query(expression, dir, bundles, threads, simple, batch_size)?;
        }
        Commands::Info { dir } => {
            cmd_info(dir)?;
        }
        Commands::Verify { dir, bundles } => {
            cmd_verify(dir, bundles)?;
        }
    }

    Ok(())
}

fn cmd_query(
    expression: String,
    dir: PathBuf,
    bundles_spec: Option<String>,
    threads: usize,
    simple: bool,
    batch_size: usize,
) -> Result<()> {
    let num_threads = if threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        threads
    };

    let query_mode = if simple {
        QueryMode::Simple
    } else {
        QueryMode::JmesPath
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

    eprintln!("Index: v{}, origin: {}", index.version, index.origin);
    eprintln!("Total bundles: {}", index.last_bundle);

    let bundle_numbers = if let Some(spec) = bundles_spec {
        parse_bundle_range(&spec, index.last_bundle)?
    } else {
        (1..=index.last_bundle).collect()
    };

    eprintln!(
        "Processing {} bundles | {} mode | {} threads\n",
        bundle_numbers.len(),
        if simple { "simple" } else { "jmespath" },
        num_threads
    );

    let pb = ProgressBar::new(bundle_numbers.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")?
            .progress_chars("=>-"),
    );

    let start = Instant::now();
    let output = std::sync::Arc::new(StdoutHandler::new());

    let stats = processor.process(
        &bundle_numbers,
        output,
        Some(|_, stats: &Stats| {
            let elapsed = start.elapsed().as_secs_f64();
            let mb_per_sec = (stats.total_bytes as f64 / 1_048_576.0) / elapsed;
            pb.set_message(format!("{} matches | {:.1} MB/s", stats.matches, mb_per_sec));
            pb.inc(1);
        }),
    )?;

    pb.finish_and_clear();

    let elapsed = start.elapsed().as_secs_f64();
    eprintln!("\n✓ Complete");
    eprintln!("  Operations: {} ({:.2}% matched)", stats.operations, 
        (stats.matches as f64 / stats.operations as f64) * 100.0);
    eprintln!("  Time: {:.2}s | {:.0} ops/sec", elapsed, stats.operations as f64 / elapsed);

    Ok(())
}

fn cmd_info(dir: PathBuf) -> Result<()> {
    let index = Index::load(&dir)?;
    
    println!("PLC Bundle Index Information");
    println!("============================");
    println!("Version:       {}", index.version);
    println!("Origin:        {}", index.origin);
    println!("Last Bundle:   {}", index.last_bundle);
    println!("Updated:       {}", index.updated_at);
    println!("Total Size:    {} bytes", index.total_size_bytes);
    println!("Uncompressed:  {} bytes", index.total_uncompressed_size_bytes);
    println!("Bundles:       {}", index.bundles.len());

    Ok(())
}

fn cmd_verify(dir: PathBuf, _bundles: Option<String>) -> Result<()> {
    let _index = Index::load(&dir)?;
    println!("Verification not yet implemented");
    Ok(())
}
