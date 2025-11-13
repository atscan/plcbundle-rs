use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::Deserialize;
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

    /// Worker threads
    #[arg(short = 'j', long = "threads", default_value = "1")]
    threads: usize,
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

fn process_bundle(
    bundle_path: &PathBuf,
    expr: &jmespath::Expression<'_>,
    stdout_lock: &Mutex<()>,
) -> Result<(usize, u64)> {
    // Open and decompress bundle
    let file = File::open(bundle_path)
        .context("Failed to open bundle file")?;
    let decoder = zstd::Decoder::new(file)
        .context("Failed to create zstd decoder")?;
    let reader = BufReader::with_capacity(256 * 1024, decoder);

    let mut matches = 0;
    let mut uncompressed_bytes = 0u64;
    
    // Process each line (operation)
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        // Count uncompressed bytes
        uncompressed_bytes += line.len() as u64 + 1; // +1 for newline

        // Parse JSON and evaluate JMESPath expression
        let data = jmespath::Variable::from_json(&line)
            .map_err(|e| anyhow::anyhow!("Failed to parse JSON: {}", e))?;
        
        let result = expr.search(&data)
            .map_err(|e| anyhow::anyhow!("JMESPath search failed: {}", e))?;
        
        // Check if result is not null
        if !result.is_null() {
            let _lock = stdout_lock.lock().unwrap();
            let stdout = io::stdout();
            let mut out = stdout.lock();
            
            // Convert result to JSON string
            let output = if result.is_string() {
                // For string results, output without quotes
                result.as_string().unwrap().to_string()
            } else {
                // For other types, serialize to JSON
                sonic_rs::to_string(&result)?
            };
            
            writeln!(out, "{}", output)?;
            matches += 1;
        }
    }

    Ok((matches, uncompressed_bytes))
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Compile JMESPath expression once
    let expr = jmespath::compile(&args.query)
        .map_err(|e| anyhow::anyhow!("Failed to compile JMESPath query: {}", e))?;

    // Set thread pool size
    if args.threads > 1 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(args.threads)
            .build_global()
            .context("Failed to initialize thread pool")?;
    }

    // Determine bundle directory
    let bundle_dir = args.dir.unwrap_or_else(|| PathBuf::from("."));
    let index_path = bundle_dir.join("plc_bundles.json");

    // Load index
    eprintln!("Loading index from {:?}...", index_path);
    let index_file = File::open(&index_path)
        .context("Failed to open plc_bundles.json")?;
    let index: Index = sonic_rs::from_reader(index_file)
        .context("Failed to parse index")?;

    eprintln!("Index version: {}, origin: {}", index.version, index.origin);
    eprintln!("Total bundles available: {}", index.last_bundle);

    // Determine which bundles to process
    let bundle_numbers = if let Some(spec) = args.bundles {
        parse_bundle_range(&spec, index.last_bundle)?
    } else {
        (1..=index.last_bundle).collect()
    };

    eprintln!("Processing {} bundles with query: {}", bundle_numbers.len(), args.query);

    // Create progress bar
    let pb = ProgressBar::new(bundle_numbers.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({percent}%) {msg} [ETA: {eta}]")?
            .progress_chars("=>-"),
    );

    // Shared stdout lock for thread safety
    let stdout_lock = Mutex::new(());
    let total_matches = Mutex::new(0usize);
    let total_bytes = Arc::new(Mutex::new(0u64));
    let start_time = Instant::now();

    // Process bundles
    let process_fn = |bundle_num: &u32| -> Result<()> {
        let bundle_path = bundle_dir.join(format!("{:06}.jsonl.zst", bundle_num));
        
        if !bundle_path.exists() {
            eprintln!("Warning: Bundle {} not found, skipping", bundle_num);
            pb.inc(1);
            return Ok(());
        }

        let (matches, bytes) = process_bundle(&bundle_path, &expr, &stdout_lock)?;
        
        *total_matches.lock().unwrap() += matches;
        *total_bytes.lock().unwrap() += bytes;
        
        let elapsed = start_time.elapsed().as_secs_f64();
        let bytes_processed = *total_bytes.lock().unwrap();
        let mb_per_sec = (bytes_processed as f64 / 1_048_576.0) / elapsed;
        
        pb.set_message(format!("{} matches | {:.1} MB/s", *total_matches.lock().unwrap(), mb_per_sec));
        pb.inc(1);
        
        Ok(())
    };

    if args.threads == 1 {
        bundle_numbers.iter().try_for_each(process_fn)?;
    } else {
        bundle_numbers.par_iter().try_for_each(process_fn)?;
    }

    pb.finish_with_message("Done");

    let final_matches = *total_matches.lock().unwrap();
    eprintln!("\nTotal matches: {}", final_matches);

    Ok(())
}