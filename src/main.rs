use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::Deserialize;
use sonic_rs::JsonValueTrait;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(name = "bundleq")]
#[command(version = "0.1.0")]
#[command(about = "Query PLC bundles with JMESPath", long_about = None)]
struct Args {
    /// JMESPath query to execute (e.g., "did" or "operation.handle")
    query: String,

    /// Directory with bundles (default: current directory)
    #[arg(short = 'd', long = "dir", value_name = "BUNDLE DIR")]
    dir: Option<PathBuf>,

    /// Bundles to process (e.g., "1-100" or "1,2,5")
    #[arg(short = 'b', long = "bundles", value_name = "BUNDLES")]
    bundles: Option<String>,

    /// Worker threads (0 = auto-detect)
    #[arg(short = 'j', long = "threads", default_value = "0")]
    threads: usize,

    /// Use simple dot notation for faster matching (e.g., "operation.handle")
    #[arg(long = "simple")]
    simple: bool,

    /// Batch size for output buffering (higher = less lock contention, more memory)
    #[arg(long = "batch-size", default_value = "2000")]
    batch_size: usize,
}

#[derive(Debug, Deserialize)]
struct Index {
    version: String,
    origin: String,
    last_bundle: u32,
    bundles: Vec<BundleMetadata>,
}

#[derive(Debug, Deserialize, Clone)]
struct BundleMetadata {
    bundle_number: u32,
    operation_count: u32,
}

enum QueryEngine {
    JmesPath(jmespath::Expression<'static>),
    Simple(Vec<String>),
}

#[derive(Default)]
struct Stats {
    operations: usize,
    matches: usize,
    total_bytes: u64,
    matched_bytes: u64,
}

fn parse_bundle_range(spec: &str, max_bundle: u32) -> Result<Vec<u32>> {
    let mut bundles = Vec::new();

    for part in spec.split(',') {
        let part = part.trim();
        if part.contains('-') {
            let range: Vec<&str> = part.split('-').collect();
            if range.len() != 2 {
                anyhow::bail!("Invalid range format: {}", part);
            }
            let start: u32 = range[0].parse()?;
            let end: u32 = range[1].parse()?;
            if start > end || start == 0 || end > max_bundle {
                anyhow::bail!("Invalid range: {}-{}", start, end);
            }
            bundles.extend(start..=end);
        } else {
            let num: u32 = part.parse()?;
            if num == 0 || num > max_bundle {
                anyhow::bail!("Bundle number {} out of range", num);
            }
            bundles.push(num);
        }
    }

    bundles.sort_unstable();
    bundles.dedup();
    Ok(bundles)
}

fn query_simple(value: &sonic_rs::Value, path: &[String]) -> Option<sonic_rs::Value> {
    let mut current = value;
    
    for key in path {
        current = current.get(key)?;
    }
    
    Some(current.clone())
}

struct OutputBuffer {
    buffer: String,
    capacity: usize,
    count: usize,
    matched_bytes: u64,
}

impl OutputBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: String::with_capacity(capacity * 100),
            capacity,
            count: 0,
            matched_bytes: 0,
        }
    }

    fn push(&mut self, line: &str) -> bool {
        self.matched_bytes += line.len() as u64 + 1; // +1 for newline
        self.buffer.push_str(line);
        self.buffer.push('\n');
        self.count += 1;
        self.count >= self.capacity
    }

    fn flush(&mut self) -> String {
        self.count = 0;
        std::mem::replace(&mut self.buffer, String::with_capacity(self.capacity * 100))
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn get_matched_bytes(&self) -> u64 {
        self.matched_bytes
    }
}

fn process_bundle_simple(
    bundle_path: &PathBuf,
    path: &[String],
    stdout_lock: &Mutex<()>,
    batch_size: usize,
) -> Result<Stats> {
    let file = File::open(bundle_path)?;
    let decoder = zstd::Decoder::new(file)?;
    let reader = BufReader::with_capacity(1024 * 1024, decoder);

    let mut stats = Stats::default();
    let mut output_buffer = OutputBuffer::new(batch_size);

    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }

        stats.operations += 1;
        stats.total_bytes += line.len() as u64 + 1;

        let data: sonic_rs::Value = sonic_rs::from_str(&line)?;
        
        if let Some(result) = query_simple(&data, path) {
            if !result.is_null() {
                stats.matches += 1;

                if result.is_str() {
                    if output_buffer.push(result.as_str().unwrap()) {
                        let _lock = stdout_lock.lock().unwrap();
                        let batch = output_buffer.flush();
                        io::stdout().write_all(batch.as_bytes())?;
                    }
                } else {
                    let output = sonic_rs::to_string(&result)?;
                    if output_buffer.push(&output) {
                        let _lock = stdout_lock.lock().unwrap();
                        let batch = output_buffer.flush();
                        io::stdout().write_all(batch.as_bytes())?;
                    }
                }
            }
        }
    }

    if !output_buffer.is_empty() {
        let _lock = stdout_lock.lock().unwrap();
        let batch = output_buffer.flush();
        io::stdout().write_all(batch.as_bytes())?;
    }

    stats.matched_bytes = output_buffer.get_matched_bytes();
    Ok(stats)
}

fn process_bundle_jmespath(
    bundle_path: &PathBuf,
    expr: &jmespath::Expression<'_>,
    stdout_lock: &Mutex<()>,
    batch_size: usize,
) -> Result<Stats> {
    let file = File::open(bundle_path)?;
    let decoder = zstd::Decoder::new(file)?;
    let reader = BufReader::with_capacity(1024 * 1024, decoder);

    let mut stats = Stats::default();
    let mut output_buffer = OutputBuffer::new(batch_size);
    
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }

        stats.operations += 1;
        stats.total_bytes += line.len() as u64 + 1;

        let data = jmespath::Variable::from_json(&line)
            .map_err(|e| anyhow::anyhow!("Failed to parse JSON: {}", e))?;
        
        let result = expr.search(&data)
            .map_err(|e| anyhow::anyhow!("JMESPath search failed: {}", e))?;
        
        if !result.is_null() {
            stats.matches += 1;

            if result.is_string() {
                if output_buffer.push(result.as_string().unwrap()) {
                    let _lock = stdout_lock.lock().unwrap();
                    let batch = output_buffer.flush();
                    io::stdout().write_all(batch.as_bytes())?;
                }
            } else {
                let output = sonic_rs::to_string(&result)?;
                if output_buffer.push(&output) {
                    let _lock = stdout_lock.lock().unwrap();
                    let batch = output_buffer.flush();
                    io::stdout().write_all(batch.as_bytes())?;
                }
            }
        }
    }

    if !output_buffer.is_empty() {
        let _lock = stdout_lock.lock().unwrap();
        let batch = output_buffer.flush();
        io::stdout().write_all(batch.as_bytes())?;
    }

    stats.matched_bytes = output_buffer.get_matched_bytes();
    Ok(stats)
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

    let query_engine = if args.simple {
        let path: Vec<String> = args.query.split('.').map(|s| s.to_string()).collect();
        eprintln!("Using simple query mode: {:?}", path);
        QueryEngine::Simple(path)
    } else {
        let expr = jmespath::compile(&args.query)
            .map_err(|e| anyhow::anyhow!("Failed to compile JMESPath query: {}", e))?;
        let expr = Box::leak(Box::new(expr));
        eprintln!("Using JMESPath query mode");
        QueryEngine::JmesPath(expr.clone())
    };

    if num_threads > 1 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
            .context("Failed to initialize thread pool")?;
    }

    let bundle_dir = args.dir.unwrap_or_else(|| PathBuf::from("."));
    let index_path = bundle_dir.join("plc_bundles.json");

    eprintln!("Loading index from {:?}...", index_path);
    let index_file = File::open(&index_path)?;
    let index: Index = sonic_rs::from_reader(index_file)?;

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

    let stdout_lock = Mutex::new(());
    let total_stats = Arc::new(Mutex::new(Stats::default()));
    let start_time = Instant::now();

    let batch_size = args.batch_size;
    let process_fn = |bundle_num: &u32| -> Result<()> {
        let bundle_path = bundle_dir.join(format!("{:06}.jsonl.zst", bundle_num));
        
        if !bundle_path.exists() {
            pb.inc(1);
            return Ok(());
        }

        let stats = match &query_engine {
            QueryEngine::Simple(path) => process_bundle_simple(&bundle_path, path, &stdout_lock, batch_size)?,
            QueryEngine::JmesPath(expr) => process_bundle_jmespath(&bundle_path, expr, &stdout_lock, batch_size)?,
        };
        
        {
            let mut total = total_stats.lock().unwrap();
            total.operations += stats.operations;
            total.matches += stats.matches;
            total.total_bytes += stats.total_bytes;
            total.matched_bytes += stats.matched_bytes;
        }
        
        let elapsed = start_time.elapsed().as_secs_f64();
        let current_stats = total_stats.lock().unwrap();
        let mb_per_sec = (current_stats.total_bytes as f64 / 1_048_576.0) / elapsed;
        
        pb.set_message(format!("{} matches | {:.1} MB/s", format_number(current_stats.matches), mb_per_sec));
        pb.inc(1);
        
        Ok(())
    };

    if num_threads == 1 {
        bundle_numbers.iter().try_for_each(process_fn)?;
    } else {
        bundle_numbers.par_iter().try_for_each(process_fn)?;
    }

    pb.finish_and_clear();

    // Print final statistics
    let final_stats = total_stats.lock().unwrap();
    let elapsed = start_time.elapsed().as_secs_f64();
    let ops_per_sec = final_stats.operations as f64 / elapsed;
    let mb_per_sec = (final_stats.total_bytes as f64 / 1_048_576.0) / elapsed;
    let match_percentage = if final_stats.operations > 0 {
        (final_stats.matches as f64 / final_stats.operations as f64) * 100.0
    } else {
        0.0
    };

    eprintln!("\n✓ Query complete");
    eprintln!("  Total operations:   {} ops", format_number(final_stats.operations));
    eprintln!("  Matches found:      {} ({:.2}%)", format_number(final_stats.matches), match_percentage);
    eprintln!("  Total bytes:        {}", format_bytes(final_stats.total_bytes));
    eprintln!("  Matched bytes:      {}", format_bytes(final_stats.matched_bytes));
    eprintln!("  Time elapsed:       {:.2}s", elapsed);
    eprintln!("  Throughput:         {} ops/sec | {:.1} MB/s", format_number(ops_per_sec as usize), mb_per_sec);
    eprintln!("  Threads:            {}", num_threads);

    Ok(())
}
