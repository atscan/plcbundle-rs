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

// Fast path: simple dot notation query
fn query_simple(value: &sonic_rs::Value, path: &[String]) -> Option<sonic_rs::Value> {
    let mut current = value;
    
    for key in path {
        current = current.get(key)?;
    }
    
    Some(current.clone())
}

// Batch output buffer to reduce lock contention
struct OutputBuffer {
    buffer: Vec<String>,
    capacity: usize,
}

impl OutputBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            capacity,
        }
    }

    fn push(&mut self, line: String) -> Option<Vec<String>> {
        self.buffer.push(line);
        if self.buffer.len() >= self.capacity {
            Some(std::mem::replace(&mut self.buffer, Vec::with_capacity(self.capacity)))
        } else {
            None
        }
    }

    fn flush(&mut self) -> Vec<String> {
        std::mem::take(&mut self.buffer)
    }
}

fn process_bundle_simple(
    bundle_path: &PathBuf,
    path: &[String],
    stdout_lock: &Mutex<()>,
) -> Result<(usize, u64)> {
    let file = File::open(bundle_path)?;
    let decoder = zstd::Decoder::new(file)?;
    let reader = BufReader::with_capacity(512 * 1024, decoder);

    let mut matches = 0;
    let mut uncompressed_bytes = 0u64;
    let mut output_buffer = OutputBuffer::new(1000);

    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }

        uncompressed_bytes += line.len() as u64 + 1;

        let data: sonic_rs::Value = sonic_rs::from_str(&line)?;
        
        if let Some(result) = query_simple(&data, path) {
            if !result.is_null() {
                let output = if result.is_str() {
                    result.as_str().unwrap().to_string()
                } else {
                    sonic_rs::to_string(&result)?
                };
                
                matches += 1;

                if let Some(batch) = output_buffer.push(output) {
                    let _lock = stdout_lock.lock().unwrap();
                    let stdout = io::stdout();
                    let mut out = stdout.lock();
                    for line in batch {
                        writeln!(out, "{}", line)?;
                    }
                }
            }
        }
    }

    let remaining = output_buffer.flush();
    if !remaining.is_empty() {
        let _lock = stdout_lock.lock().unwrap();
        let stdout = io::stdout();
        let mut out = stdout.lock();
        for line in remaining {
            writeln!(out, "{}", line)?;
        }
    }

    Ok((matches, uncompressed_bytes))
}

fn process_bundle_jmespath(
    bundle_path: &PathBuf,
    expr: &jmespath::Expression<'_>,
    stdout_lock: &Mutex<()>,
) -> Result<(usize, u64)> {
    let file = File::open(bundle_path)?;
    let decoder = zstd::Decoder::new(file)?;
    let reader = BufReader::with_capacity(512 * 1024, decoder);

    let mut matches = 0;
    let mut uncompressed_bytes = 0u64;
    let mut output_buffer = OutputBuffer::new(1000);
    
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }

        uncompressed_bytes += line.len() as u64 + 1;

        let data = jmespath::Variable::from_json(&line)
            .map_err(|e| anyhow::anyhow!("Failed to parse JSON: {}", e))?;
        
        let result = expr.search(&data)
            .map_err(|e| anyhow::anyhow!("JMESPath search failed: {}", e))?;
        
        if !result.is_null() {
            let output = if result.is_string() {
                result.as_string().unwrap().to_string()
            } else {
                sonic_rs::to_string(&result)?
            };
            
            matches += 1;

            if let Some(batch) = output_buffer.push(output) {
                let _lock = stdout_lock.lock().unwrap();
                let stdout = io::stdout();
                let mut out = stdout.lock();
                for line in batch {
                    writeln!(out, "{}", line)?;
                }
            }
        }
    }

    let remaining = output_buffer.flush();
    if !remaining.is_empty() {
        let _lock = stdout_lock.lock().unwrap();
        let stdout = io::stdout();
        let mut out = stdout.lock();
        for line in remaining {
            writeln!(out, "{}", line)?;
        }
    }

    Ok((matches, uncompressed_bytes))
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Auto-detect thread count if 0
    let num_threads = if args.threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        args.threads
    };

    eprintln!("Using {} worker thread(s)", num_threads);

    // Prepare query engine
    let query_engine = if args.simple {
        let path: Vec<String> = args.query.split('.').map(|s| s.to_string()).collect();
        eprintln!("Using simple query mode: {:?}", path);
        QueryEngine::Simple(path)
    } else {
        let expr = jmespath::compile(&args.query)
            .map_err(|e| anyhow::anyhow!("Failed to compile JMESPath query: {}", e))?;
        let expr = Box::leak(Box::new(expr));
        QueryEngine::JmesPath(expr.clone())
    };

    // Set thread pool size
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

    eprintln!("Processing {} bundles with query: {}", bundle_numbers.len(), args.query);

    let pb = ProgressBar::new(bundle_numbers.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({percent}%) {msg} [ETA: {eta}]")?
            .progress_chars("=>-"),
    );

    let stdout_lock = Mutex::new(());
    let total_matches = Mutex::new(0usize);
    let total_bytes = Arc::new(Mutex::new(0u64));
    let start_time = Instant::now();

    let process_fn = |bundle_num: &u32| -> Result<()> {
        let bundle_path = bundle_dir.join(format!("{:06}.jsonl.zst", bundle_num));
        
        if !bundle_path.exists() {
            pb.inc(1);
            return Ok(());
        }

        let (matches, bytes) = match &query_engine {
            QueryEngine::Simple(path) => process_bundle_simple(&bundle_path, path, &stdout_lock)?,
            QueryEngine::JmesPath(expr) => process_bundle_jmespath(&bundle_path, expr, &stdout_lock)?,
        };
        
        *total_matches.lock().unwrap() += matches;
        *total_bytes.lock().unwrap() += bytes;
        
        let elapsed = start_time.elapsed().as_secs_f64();
        let bytes_processed = *total_bytes.lock().unwrap();
        let mb_per_sec = (bytes_processed as f64 / 1_048_576.0) / elapsed;
        
        pb.set_message(format!("{} matches | {:.1} MB/s", *total_matches.lock().unwrap(), mb_per_sec));
        pb.inc(1);
        
        Ok(())
    };

    if num_threads == 1 {
        bundle_numbers.iter().try_for_each(process_fn)?;
    } else {
        bundle_numbers.par_iter().try_for_each(process_fn)?;
    }

    pb.finish_with_message("Done");

    let final_matches = *total_matches.lock().unwrap();
    eprintln!("\nTotal matches: {}", final_matches);

    Ok(())
}
