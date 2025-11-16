use super::progress::ProgressBar;
use super::utils::format_number;
use anyhow::Result;
use clap::Args;
use plcbundle::BundleManager;
use std::io::Read;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Args)]
#[command(
    about = "Benchmark bundle operations",
    after_help = "Examples:\n  \
            # Run all benchmarks with default iterations\n  \
            {bin} bench\n\n  \
            # Benchmark specific operation\n  \
            {bin} bench --op-read --iterations 1000\n\n  \
            # Benchmark DID lookup\n  \
            {bin} bench --did-lookup -n 500\n\n  \
            # Run on specific bundle\n  \
            {bin} bench --bundles 100\n\n  \
            # JSON output for analysis\n  \
            {bin} bench --json > benchmark.json"
)]
pub struct BenchCommand {
    /// Number of iterations for each benchmark
    #[arg(short = 'n', long, default_value = "100")]
    pub iterations: usize,

    /// Bundle number to benchmark (default: uses multiple bundles)
    #[arg(long)]
    pub bundles: Option<String>,

    /// Run all benchmarks (default)
    #[arg(short, long)]
    pub all: bool,

    /// Benchmark operation reading
    #[arg(long)]
    pub op_read: bool,

    /// Benchmark DID index lookup
    #[arg(long)]
    pub did_lookup: bool,

    /// Benchmark bundle loading
    #[arg(long)]
    pub bundle_load: bool,

    /// Benchmark bundle decompression
    #[arg(long)]
    pub decompress: bool,

    /// Benchmark DID resolution (includes index + operations)
    #[arg(long)]
    pub did_resolve: bool,

    /// Benchmark sequential bundle access pattern
    #[arg(long)]
    pub sequential: bool,

    /// Warmup iterations before benchmarking
    #[arg(long, default_value = "10")]
    pub warmup: usize,

    /// Show interactive progress during benchmarks
    #[arg(long)]
    pub interactive: bool,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, serde::Serialize)]
struct BenchmarkResult {
    name: String,
    iterations: usize,
    total_ms: f64,
    avg_ms: f64,
    min_ms: f64,
    max_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    stddev_ms: f64,
    ops_per_sec: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    throughput_mbs: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    avg_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bundles_accessed: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_hits: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_misses: Option<usize>,
}

pub fn run(cmd: BenchCommand, dir: PathBuf, global_verbose: bool) -> Result<()> {
    let manager = super::utils::create_manager(dir.clone(), global_verbose, false)?;

    // Determine which benchmarks to run
    let run_all = cmd.all
        || (!cmd.op_read
            && !cmd.did_lookup
            && !cmd.bundle_load
            && !cmd.decompress
            && !cmd.did_resolve
            && !cmd.sequential);

    // Get repository info
    if super::utils::is_repository_empty(&manager) {
        anyhow::bail!("No bundles found in repository");
    }
    let last_bundle = manager.get_last_bundle();

    // Print benchmark header
    eprintln!("\n{}", "=".repeat(80));
    eprintln!("{:^80}", "BENCHMARK SUITE");
    eprintln!("{}", "=".repeat(80));
    eprintln!("Repository: {} ({} bundles)", dir.display(), last_bundle);
    eprintln!("Iterations: {} (warmup: {})", cmd.iterations, cmd.warmup);
    eprintln!("Interactive: {}", cmd.interactive);
    eprintln!("{}", "=".repeat(80));
    eprintln!();

    let mut results = Vec::new();

    // All benchmarks now use random data from across the repository
    if run_all || cmd.bundle_load {
        results.push(bench_bundle_load(
            &manager,
            last_bundle,
            cmd.iterations,
            cmd.warmup,
            cmd.interactive,
        )?);
    }

    if run_all || cmd.decompress {
        results.push(bench_bundle_decompress(
            &manager,
            last_bundle,
            cmd.iterations,
            cmd.warmup,
            cmd.interactive,
        )?);
    }

    if run_all || cmd.op_read {
        results.push(bench_operation_read(
            &manager,
            last_bundle,
            cmd.iterations,
            cmd.warmup,
            cmd.interactive,
        )?);
    }

    if run_all || cmd.did_lookup {
        results.push(bench_did_index_lookup(
            &manager,
            last_bundle,
            cmd.iterations,
            cmd.warmup,
            cmd.interactive,
        )?);
    }

    if run_all || cmd.did_resolve {
        results.push(bench_did_resolution(
            &manager,
            last_bundle,
            cmd.iterations,
            cmd.warmup,
            cmd.interactive,
        )?);
    }

    if cmd.sequential {
        results.push(bench_sequential_access(
            &manager,
            last_bundle,
            cmd.iterations.min(50),
            cmd.warmup,
            cmd.interactive,
        )?);
    }

    // Output results
    if cmd.json {
        print_json_results(&results)?;
    } else {
        print_human_results(&results);
    }

    Ok(())
}

/// Generate random bundle numbers using deterministic hash
fn generate_random_bundles(last_bundle: u32, count: usize) -> Vec<u32> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    (0..count)
        .map(|i| {
            let mut hasher = DefaultHasher::new();
            i.hash(&mut hasher);
            (hasher.finish() % last_bundle as u64) as u32 + 1
        })
        .collect()
}

fn bundle_compressed_size(manager: &BundleManager, bundle_num: u32) -> Result<Option<u64>> {
    Ok(manager
        .get_bundle_metadata(bundle_num)?
        .map(|meta| meta.compressed_size))
}

/// Benchmark bundle loading (full bundle read + decompression + parsing)
/// Iterates over random bundles from the entire repository
fn bench_bundle_load(
    manager: &BundleManager,
    last_bundle: u32,
    iterations: usize,
    warmup: usize,
    interactive: bool,
) -> Result<BenchmarkResult> {
    use plcbundle::LoadOptions;

    if interactive {
        eprintln!("[Benchmark] Bundle Load (full)...");
    }

    let bundles = generate_random_bundles(last_bundle, iterations);

    // Warmup
    for i in 0..warmup.min(10) {
        let _ = manager.load_bundle(bundles[i % bundles.len()], LoadOptions::default())?;
    }

    // Benchmark - iterate over different bundles each time
    manager.clear_caches();
    let mut timings = Vec::with_capacity(iterations);
    let mut total_bytes = 0u64;

    let pb = if interactive {
        Some(ProgressBar::new(iterations))
    } else {
        None
    };

    for (i, &bundle_num) in bundles.iter().enumerate() {
        if let Some(ref pb) = pb {
            pb.set(i + 1);
        }

        if let Some(size) = bundle_compressed_size(manager, bundle_num)? {
            total_bytes += size;
        }

        let start = Instant::now();
        let _ = manager.load_bundle(bundle_num, LoadOptions::default())?;
        timings.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    if let Some(ref pb) = pb {
        pb.finish();
    }

    let unique_bundles = bundles
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    let avg_size = total_bytes / iterations as u64;

    let mut result = calculate_stats("Bundle Load (full)", iterations, timings);
    result.avg_size_bytes = Some(avg_size);
    result.total_bytes = Some(total_bytes);
    result.bundles_accessed = Some(unique_bundles);
    result.throughput_mbs =
        Some((total_bytes as f64 / 1024.0 / 1024.0) / (result.total_ms / 1000.0));
    Ok(result)
}

/// Benchmark bundle decompression only (read + decompress, no parsing)
/// Iterates over random bundles from the entire repository
fn bench_bundle_decompress(
    manager: &BundleManager,
    last_bundle: u32,
    iterations: usize,
    warmup: usize,
    interactive: bool,
) -> Result<BenchmarkResult> {
    if interactive {
        eprintln!("[Benchmark] Bundle Decompression...");
    }

    let bundles = generate_random_bundles(last_bundle, iterations);

    // Warmup
    for i in 0..warmup.min(10) {
        let bundle_num = bundles[i % bundles.len()];
        if bundle_compressed_size(manager, bundle_num)?.is_none() {
            continue;
        }

        let file = manager.stream_bundle_raw(bundle_num)?;
        let mut decoder = zstd::Decoder::new(file)?;
        let mut buffer = Vec::new();
        decoder.read_to_end(&mut buffer)?;
    }

    // Benchmark - iterate over different bundles each time
    let mut timings = Vec::with_capacity(iterations);
    let mut total_bytes = 0u64;

    let pb = if interactive {
        Some(ProgressBar::new(iterations))
    } else {
        None
    };

    let mut processed = 0;
    for (_i, &bundle_num) in bundles.iter().enumerate() {
        let size = match bundle_compressed_size(manager, bundle_num)? {
            Some(size) => size,
            None => continue,
        };
        total_bytes += size;
        processed += 1;

        if let Some(ref pb) = pb {
            pb.set(processed);
        }

        let start = Instant::now();
        let file = manager.stream_bundle_raw(bundle_num)?;
        let mut decoder = zstd::Decoder::new(file)?;
        let mut buffer = Vec::new();
        decoder.read_to_end(&mut buffer)?;
        timings.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    if let Some(ref pb) = pb {
        pb.finish();
    }

    let unique_bundles = bundles
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    let avg_size = total_bytes / timings.len() as u64;

    let mut result = calculate_stats("Bundle Decompression", timings.len(), timings);
    result.avg_size_bytes = Some(avg_size);
    result.total_bytes = Some(total_bytes);
    result.bundles_accessed = Some(unique_bundles);
    result.throughput_mbs =
        Some((total_bytes as f64 / 1024.0 / 1024.0) / (result.total_ms / 1000.0));
    Ok(result)
}

/// Benchmark single operation read from random bundles and positions
fn bench_operation_read(
    manager: &BundleManager,
    last_bundle: u32,
    iterations: usize,
    warmup: usize,
    interactive: bool,
) -> Result<BenchmarkResult> {
    use plcbundle::LoadOptions;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    if interactive {
        eprintln!("[Benchmark] Operation Read...");
    }

    let bundles = generate_random_bundles(last_bundle, iterations);

    // Load bundles to get operation counts
    let mut bundle_op_counts = Vec::with_capacity(bundles.len());
    for &bundle_num in &bundles {
        if let Ok(bundle) = manager.load_bundle(bundle_num, LoadOptions::default()) {
            if !bundle.operations.is_empty() {
                bundle_op_counts.push((bundle_num, bundle.operations.len()));
            }
        }
    }

    if bundle_op_counts.is_empty() {
        anyhow::bail!("No bundles with operations found");
    }

    // Warmup
    for i in 0..warmup.min(10) {
        let (bundle_num, op_count) = bundle_op_counts[i % bundle_op_counts.len()];
        let pos = op_count / 2;
        let _ = manager.get_operation_raw(bundle_num, pos)?;
    }

    // Benchmark - random bundle and random position each iteration
    let mut timings = Vec::with_capacity(iterations);
    
    let pb = if interactive {
        Some(ProgressBar::new(iterations))
    } else {
        None
    };

    for i in 0..iterations {
        if let Some(ref pb) = pb {
            pb.set(i + 1);
        }

        let (bundle_num, op_count) = bundle_op_counts[i % bundle_op_counts.len()];

        // Generate random position within this bundle
        let mut hasher = DefaultHasher::new();
        (i * 1000).hash(&mut hasher);
        let pos = (hasher.finish() % op_count as u64) as usize;

        let start = Instant::now();
        let _ = manager.get_operation_raw(bundle_num, pos)?;
        timings.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    if let Some(ref pb) = pb {
        pb.finish();
    }

    let unique_bundles = bundle_op_counts
        .iter()
        .map(|(b, _)| b)
        .collect::<std::collections::HashSet<_>>()
        .len();
    let mut result = calculate_stats("Operation Read", iterations, timings);
    result.bundles_accessed = Some(unique_bundles);
    Ok(result)
}

/// Benchmark DID index lookup from random DIDs across repository
fn bench_did_index_lookup(
    manager: &BundleManager,
    _last_bundle: u32,
    iterations: usize,
    warmup: usize,
    interactive: bool,
) -> Result<BenchmarkResult> {
    if interactive {
        eprintln!("[Benchmark] DID Index Lookup...");
    }

    let sample_count = iterations.max(warmup.min(10)).max(1);
    let dids = manager.sample_random_dids(sample_count, None)?;

    if dids.is_empty() {
        anyhow::bail!("No DIDs found in repository");
    }

    // Ensure DID index is loaded (sample_random_dids already does this, but be explicit)
    // The did_index will be loaded by sample_random_dids above
    let did_index = manager.get_did_index();
    
    // Ensure it's actually loaded (in case sample_random_dids didn't load it)
    {
        let guard = did_index.read().unwrap();
        if guard.is_none() {
            anyhow::bail!("DID index not available");
        }
    }

    // Warmup
    for i in 0..warmup.min(10) {
        let _ = did_index
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .get_did_locations(&dids[i % dids.len()])?;
    }

    // Benchmark - different DID each iteration
    let mut timings = Vec::with_capacity(iterations);
    
    let pb = if interactive {
        Some(ProgressBar::new(iterations))
    } else {
        None
    };

    for i in 0..iterations {
        if let Some(ref pb) = pb {
            pb.set(i + 1);
        }

        let did = &dids[i % dids.len()];
        let start = Instant::now();
        let _ = did_index.read().unwrap().as_ref().unwrap().get_did_locations(did)?;
        timings.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    if let Some(ref pb) = pb {
        pb.finish();
    }

    Ok(calculate_stats("DID Index Lookup", iterations, timings))
}

/// Benchmark DID resolution from random DIDs: index lookup → operations → W3C document
fn bench_did_resolution(
    manager: &BundleManager,
    _last_bundle: u32,
    iterations: usize,
    warmup: usize,
    interactive: bool,
) -> Result<BenchmarkResult> {
    if interactive {
        eprintln!("[Benchmark] DID Resolution (index→document)...");
    }

    let sample_count = iterations.max(warmup.min(10)).max(1);
    let dids = manager.sample_random_dids(sample_count, None)?;

    if dids.is_empty() {
        anyhow::bail!("No DIDs found in repository");
    }

    // Warmup
    for i in 0..warmup.min(10) {
        let _ = manager.resolve_did(&dids[i % dids.len()])?;
    }

    // Benchmark - different DID each iteration
    manager.clear_caches();
    let mut timings = Vec::with_capacity(iterations);
    
    let pb = if interactive {
        Some(ProgressBar::new(iterations))
    } else {
        None
    };

    for i in 0..iterations {
        if let Some(ref pb) = pb {
            pb.set(i + 1);
        }

        let did = &dids[i % dids.len()];
        let start = Instant::now();
        let _ = manager.resolve_did(did)?;
        timings.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    if let Some(ref pb) = pb {
        pb.finish();
    }

    Ok(calculate_stats(
        "DID Resolution (index→document)",
        iterations,
        timings,
    ))
}

fn calculate_stats(name: &str, iterations: usize, mut timings: Vec<f64>) -> BenchmarkResult {
    timings.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let total_ms: f64 = timings.iter().sum();
    let avg_ms = total_ms / iterations as f64;
    let min_ms = timings[0];
    let max_ms = timings[timings.len() - 1];

    let p50_idx = (iterations as f64 * 0.50) as usize;
    let p95_idx = (iterations as f64 * 0.95) as usize;
    let p99_idx = (iterations as f64 * 0.99) as usize;

    let p50_ms = timings[p50_idx.min(timings.len() - 1)];
    let p95_ms = timings[p95_idx.min(timings.len() - 1)];
    let p99_ms = timings[p99_idx.min(timings.len() - 1)];

    // Calculate standard deviation
    let variance: f64 = timings
        .iter()
        .map(|&x| {
            let diff = x - avg_ms;
            diff * diff
        })
        .sum::<f64>()
        / iterations as f64;
    let stddev_ms = variance.sqrt();

    let ops_per_sec = 1000.0 / avg_ms;

    BenchmarkResult {
        name: name.to_string(),
        iterations,
        total_ms,
        avg_ms,
        min_ms,
        max_ms,
        p50_ms,
        p95_ms,
        p99_ms,
        stddev_ms,
        ops_per_sec,
        throughput_mbs: None,
        avg_size_bytes: None,
        total_bytes: None,
        bundles_accessed: None,
        cache_hits: None,
        cache_misses: None,
    }
}

/// Benchmark sequential bundle access
fn bench_sequential_access(
    manager: &BundleManager,
    last_bundle: u32,
    iterations: usize,
    warmup: usize,
    interactive: bool,
) -> Result<BenchmarkResult> {
    use plcbundle::LoadOptions;

    if interactive {
        eprintln!("[Benchmark] Sequential Bundle Access...");
    }

    let count = iterations.min(last_bundle as usize).min(50);
    let start_bundle = (last_bundle / 2).saturating_sub(count as u32 / 2).max(1);

    // Warmup
    for i in 0..warmup.min(5) {
        let bundle_num = start_bundle + (i as u32 % count as u32);
        let _ = manager.load_bundle(bundle_num, LoadOptions::default())?;
    }

    // Benchmark
    manager.clear_caches();
    let start_stats = manager.get_stats();
    let mut timings = Vec::with_capacity(count);
    let mut total_bytes = 0u64;

    let pb = if interactive {
        Some(ProgressBar::new(count))
    } else {
        None
    };

    for i in 0..count {
        if let Some(ref pb) = pb {
            pb.set(i + 1);
        }

        let bundle_num = start_bundle + i as u32;
        if let Some(size) = bundle_compressed_size(manager, bundle_num)? {
            total_bytes += size;
        }

        let start = Instant::now();
        let _ = manager.load_bundle(bundle_num, LoadOptions::default())?;
        timings.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    if let Some(ref pb) = pb {
        pb.finish();
    }

    let end_stats = manager.get_stats();
    let mut result = calculate_stats("Sequential Bundle Access", count, timings);
    result.bundles_accessed = Some(count);
    result.total_bytes = Some(total_bytes);
    result.throughput_mbs =
        Some((total_bytes as f64 / 1024.0 / 1024.0) / (result.total_ms / 1000.0));
    result.cache_hits = Some((end_stats.cache_hits - start_stats.cache_hits) as usize);
    result.cache_misses = Some((end_stats.cache_misses - start_stats.cache_misses) as usize);

    Ok(result)
}

/// Format time with appropriate units (ms, μs, or ns)
fn format_time(ms: f64) -> String {
    if ms >= 1.0 {
        format!("{:.3} ms", ms)
    } else if ms >= 0.001 {
        format!("{:.3} μs", ms * 1000.0)
    } else {
        format!("{:.1} ns", ms * 1_000_000.0)
    }
}

fn print_human_results(results: &[BenchmarkResult]) {
    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "BENCHMARK RESULTS");
    println!("{}", "=".repeat(80));
    println!();

    for result in results {
        println!("{}:", result.name);
        println!(
            "  Iterations:    {}",
            format_number(result.iterations as u64)
        );
        println!("  Total Time:    {:.2} ms", result.total_ms);
        println!(
            "  Average:       {}  ({:.0} ops/sec)",
            format_time(result.avg_ms),
            result.ops_per_sec
        );

        if let Some(size) = result.avg_size_bytes {
            println!("  Bundle Size:   {:.2} MB", size as f64 / 1024.0 / 1024.0);
        }
        if let Some(throughput) = result.throughput_mbs {
            println!("  Throughput:    {:.2} MB/s", throughput);
        }

        println!("  Min:           {}", format_time(result.min_ms));
        println!("  Max:           {}", format_time(result.max_ms));
        println!("  Median (p50):  {}", format_time(result.p50_ms));
        println!("  p95:           {}", format_time(result.p95_ms));
        println!("  p99:           {}", format_time(result.p99_ms));

        if result.stddev_ms > 0.0 {
            println!("  Std Dev:       {}", format_time(result.stddev_ms));
        }

        if let Some(bundles) = result.bundles_accessed {
            println!("  Bundles:       {}", bundles);
        }
        if let Some(hits) = result.cache_hits {
            println!("  Cache Hits:    {}", format_number(hits as u64));
        }
        if let Some(misses) = result.cache_misses {
            println!("  Cache Misses:  {}", format_number(misses as u64));
            if let Some(hits) = result.cache_hits {
                let total = hits + misses;
                if total > 0 {
                    let hit_rate = (hits as f64 / total as f64) * 100.0;
                    println!("  Cache Hit Rate: {:.1}%", hit_rate);
                }
            }
        }

        println!();
    }

    println!("{}", "=".repeat(80));
}

fn print_json_results(results: &[BenchmarkResult]) -> Result<()> {
    let json = sonic_rs::to_string_pretty(results)?;
    println!("{}", json);
    Ok(())
}
