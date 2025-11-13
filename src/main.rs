use anyhow::Result;
use bundleq::*;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(name = "bundleq", version = "0.1.0")]
#[command(about = "Query PLC bundles with JMESPath")]
struct Args {
    query: String,
    #[arg(short = 'd', long = "dir")]
    dir: Option<PathBuf>,
    #[arg(short = 'b', long = "bundles")]
    bundles: Option<String>,
    #[arg(short = 'j', long = "threads", default_value = "0")]
    threads: usize,
    #[arg(long = "simple")]
    simple: bool,
    #[arg(long = "batch-size", default_value = "2000")]
    batch_size: usize,
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

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let mut count = 0;
    
    for c in s.chars().rev() {
        if count > 0 && count % 3 == 0 {
            result.push(',');
        }
        result.push(c);
        count += 1;
    }
    
    result.chars().rev().collect()
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    
    let num_threads = if args.threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        args.threads
    };

    eprintln!("Using {} worker thread(s)", num_threads);

    let bundle_dir = args.dir.unwrap_or_else(|| PathBuf::from("."));
    
    let processor = BundleProcessor::new(
        bundle_dir,
        &args.query,
        args.simple,
        num_threads,
        args.batch_size,
    )?;

    eprintln!("Using {} query mode", if args.simple { "simple" } else { "JMESPath" });

    let index = processor.load_index()?;
    eprintln!("Index version: {}, origin: {}", index.version, index.origin);
    eprintln!("Total bundles available: {}", index.last_bundle);

    let bundle_numbers = if let Some(spec) = args.bundles {
        parse_bundle_range(&spec, index.last_bundle)?
    } else {
        (1..=index.last_bundle).collect()
    };

    eprintln!("Processing {} bundles with query: {}\n", bundle_numbers.len(), args.query);

    let pb = ProgressBar::new(bundle_numbers.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({percent}%) {msg} [ETA: {eta}]")?
            .progress_chars("=>-"),
    );

    let start = Instant::now();
    let output = std::sync::Arc::new(StdoutHandler::new());
    
    let stats = processor.process_bundles(
        &bundle_numbers,
        output,
        Some(|_, stats: &Stats| {
            let elapsed = start.elapsed().as_secs_f64();
            let mb_per_sec = (stats.total_bytes as f64 / 1_048_576.0) / elapsed;
            pb.set_message(format!("{} matches | {:.1} MB/s", format_number(stats.matches), mb_per_sec));
            pb.inc(1);
        }),
    )?;

    pb.finish_and_clear();

    let elapsed = start.elapsed().as_secs_f64();
    let ops_per_sec = stats.operations as f64 / elapsed;
    let mb_per_sec = (stats.total_bytes as f64 / 1_048_576.0) / elapsed;
    let match_percentage = if stats.operations > 0 {
        (stats.matches as f64 / stats.operations as f64) * 100.0
    } else {
        0.0
    };

    eprintln!("\n✓ Query complete");
    eprintln!("  Total operations:   {} ops", format_number(stats.operations));
    eprintln!("  Matches found:      {} ({:.2}%)", format_number(stats.matches), match_percentage);
    eprintln!("  Total bytes:        {}", format_bytes(stats.total_bytes));
    eprintln!("  Matched bytes:      {}", format_bytes(stats.matched_bytes));
    eprintln!("  Time elapsed:       {:.2}s", elapsed);
    eprintln!("  Throughput:         {} ops/sec | {:.1} MB/s", format_number(ops_per_sec as usize), mb_per_sec);
    eprintln!("  Threads:            {}", num_threads);

    Ok(())
}
