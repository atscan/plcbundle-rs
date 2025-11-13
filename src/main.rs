use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rsonpath::engine::{Compiler, Engine, RsonpathEngine};
use rsonpath::input::BorrowedBytes;
use rsonpath::result::MatchWriter;
use serde::Deserialize;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Mutex;

#[derive(Parser, Debug)]
#[command(name = "bundleq")]
#[command(version = "0.1.0")]
#[command(about = "Query PLC bundles with JSONPath", long_about = None)]
struct Args {
    /// JSONPath query to execute (e.g., "$.did" or just "did")
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

struct Span {
    start: usize,
    end: usize,
}

fn parse_span(s: &str) -> Option<Span> {
    // Parse format: [start..end]
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        return None;
    }
    
    let inner = &s[1..s.len()-1];
    let parts: Vec<&str> = inner.split("..").collect();
    if parts.len() != 2 {
        return None;
    }
    
    let start = parts[0].parse().ok()?;
    let end = parts[1].parse().ok()?;
    
    Some(Span { start, end })
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
    query_str: &str,
    stdout_lock: &Mutex<()>,
) -> Result<usize> {
    // Normalize query: add $ prefix if missing
    let query_str = if query_str.starts_with('$') {
        query_str.to_string()
    } else {
        format!("$.{}", query_str)
    };

    // Parse and compile query
    let parsed_query = rsonpath_syntax::parse(&query_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse JSONPath query: {}", e))?;
    
    let engine = RsonpathEngine::compile_query(&parsed_query)
        .context("Failed to compile query")?;

    // Open and decompress bundle
    let file = File::open(bundle_path)
        .context("Failed to open bundle file")?;
    let decoder = zstd::Decoder::new(file)
        .context("Failed to create zstd decoder")?;
    let reader = BufReader::with_capacity(256 * 1024, decoder);

    let mut matches = 0;
    
    // Process each line (operation)
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        // Use rsonpath to get byte spans of matches
        let line_bytes = line.as_bytes();
        let input = BorrowedBytes::new(line_bytes);
        
        // Collect matches using approximate_spans
        let mut buffer = Vec::new();
        {
            let mut writer = MatchWriter::from(&mut buffer);
            engine.approximate_spans(&input, &mut writer)?;
        }
        
        // Extract and output the actual matched values
        if !buffer.is_empty() {
            let span_output = String::from_utf8_lossy(&buffer);
            let _lock = stdout_lock.lock().unwrap();
            let stdout = io::stdout();
            let mut out = stdout.lock();
            
            for span_line in span_output.lines() {
                if let Some(span) = parse_span(span_line) {
                    if span.end <= line_bytes.len() {
                        let matched_bytes = &line_bytes[span.start..span.end];
                        out.write_all(matched_bytes)?;
                        out.write_all(b"\n")?;
                        matches += 1;
                    }
                }
            }
        }
    }

    Ok(matches)
}

fn main() -> Result<()> {
    let args = Args::parse();

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

    eprintln!("Processing {} bundles...", bundle_numbers.len());

    // Create progress bar
    let pb = ProgressBar::new(bundle_numbers.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} bundles ({percent}%) {msg}")?
            .progress_chars("=>-"),
    );

    // Shared stdout lock for thread safety
    let stdout_lock = Mutex::new(());
    let total_matches = Mutex::new(0usize);

    // Process bundles
    let process_fn = |bundle_num: &u32| -> Result<()> {
        let bundle_path = bundle_dir.join(format!("{:06}.jsonl.zst", bundle_num));
        
        if !bundle_path.exists() {
            eprintln!("Warning: Bundle {} not found, skipping", bundle_num);
            pb.inc(1);
            return Ok(());
        }

        let matches = process_bundle(&bundle_path, &args.query, &stdout_lock)?;
        
        *total_matches.lock().unwrap() += matches;
        pb.set_message(format!("{} matches", *total_matches.lock().unwrap()));
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