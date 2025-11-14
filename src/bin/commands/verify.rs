use anyhow::{Result, bail};
use clap::Args;
use plcbundle::{BundleManager, VerifySpec};
use std::path::PathBuf;
use std::time::Instant;
use super::progress::ProgressBar;
use super::utils::parse_bundle_range_simple;

#[derive(Args)]
pub struct VerifyCommand {
    /// Verify specific bundle number
    #[arg(short, long)]
    pub bundle: Option<u32>,

    /// Verify bundle range (e.g., "1-100")
    #[arg(short, long)]
    pub range: Option<String>,

    /// Verify entire chain (default)
    #[arg(short, long)]
    pub chain: bool,

    /// Use parallel verification for ranges
    #[arg(long)]
    pub parallel: bool,

    /// Number of parallel workers
    #[arg(long, default_value = "4")]
    pub workers: usize,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Full verification (includes content hash check)
    #[arg(long)]
    pub full: bool,

    /// Number of threads to use (0 = auto-detect)
    #[arg(short = 'j', long, default_value = "0")]
    pub threads: usize,
}

pub fn run(cmd: VerifyCommand, dir: PathBuf) -> Result<()> {
    let manager = BundleManager::new(dir.clone())?;

    // Determine number of threads
    let num_threads = if cmd.threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    } else {
        cmd.threads
    };

    // Show thread count in debug/verbose mode
    if cmd.verbose {
        eprintln!("[DEBUG] Using {} thread(s) for verification", num_threads);
    }

    if !cmd.verbose {
        let full_path = std::fs::canonicalize(&dir)
            .unwrap_or_else(|_| dir.clone());
        eprintln!("Working in: {}\n", full_path.display());
    }

    // Determine what to verify
    if let Some(range_str) = cmd.range {
        verify_range(&manager, &range_str, cmd.verbose, cmd.parallel, cmd.workers, cmd.full, num_threads)?;
    } else if let Some(bundle_num) = cmd.bundle {
        verify_single_bundle(&manager, bundle_num, cmd.verbose, cmd.full)?;
    } else {
        // Default: verify entire chain
        verify_chain(&manager, cmd.verbose, cmd.full, num_threads)?;
    }

    Ok(())
}

fn verify_single_bundle(manager: &BundleManager, bundle_num: u32, verbose: bool, full: bool) -> Result<()> {
    eprintln!("Verifying bundle {:06}...", bundle_num);

    let start = Instant::now();
    // For single bundle, check content hash if --full flag is set
    let spec = VerifySpec {
        check_hash: true,
        check_content_hash: full,
        check_operations: full,
    };
    let result = manager.verify_bundle(bundle_num, spec)?;
    let elapsed = start.elapsed();

    if result.valid {
        eprintln!("✓ Bundle {:06} is valid ({:?})", bundle_num, elapsed);
        if verbose {
            eprintln!("\nDetails:");
            eprintln!("  Errors: {}", if result.errors.is_empty() { "none" } else { "yes" });
            if !result.errors.is_empty() {
                for err in &result.errors {
                    eprintln!("    - {}", err);
                }
            }
            eprintln!("  Verification time: {:?}", elapsed);
        }
        Ok(())
    } else {
        eprintln!("✗ Bundle {:06} is invalid ({:?})", bundle_num, elapsed);
        if !result.errors.is_empty() {
            for err in &result.errors {
                eprintln!("  Error: {}", err);
            }
        }
        bail!("bundle verification failed")
    }
}

fn verify_chain(manager: &BundleManager, verbose: bool, full: bool, num_threads: usize) -> Result<()> {
    let last_bundle = manager.get_last_bundle();
    
    if last_bundle == 0 {
        eprintln!("No bundles to verify");
        return Ok(());
    }

    // Get all bundle metadata
    let mut bundles = Vec::new();
    for i in 1..=last_bundle {
        if let Ok(Some(meta)) = manager.get_bundle_metadata(i) {
            bundles.push(meta);
        }
    }

    if bundles.is_empty() {
        eprintln!("No bundles to verify");
        return Ok(());
    }

    // Print root hash (first bundle) and head hash (latest) at start
    eprintln!("Chain root:   {}", bundles[0].hash);
    eprintln!("Chain head:   {} (target)", bundles[bundles.len() - 1].hash);
    eprintln!("Total bundles: {}", bundles.len());
    eprintln!("");

    eprintln!("Verifying chain of {} bundles...\n", bundles.len());

    let start = Instant::now();
    
    // Two-pass parallel verification:
    // Pass 1: Verify all bundle hashes in parallel
    // Pass 2: Verify chain links sequentially (needs previous results)
    
    let spec = VerifySpec {
        check_hash: true,
        check_content_hash: full,  // Only if --full flag
        check_operations: false,
    };

    // Calculate total uncompressed size for progress tracking
    let total_uncompressed_size: u64 = bundles.iter().map(|b| b.uncompressed_size).sum();

    // Pass 1: Parallel bundle hash verification
    eprintln!("Pass 1: Verifying bundle hashes...");
    // Always show progress bar (it will detect if TTY and show appropriate format)
    let progress = Some(ProgressBar::with_bytes(bundles.len(), total_uncompressed_size));

    use std::sync::mpsc;
    use std::thread;
    use std::sync::Arc;
    
    let (job_tx, job_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let manager_clone = manager.clone_for_arc();
    let job_rx = Arc::new(std::sync::Mutex::new(job_rx));
    
    // Spawn worker threads
    let num_workers = num_threads.min(bundles.len());
    for _ in 0..num_workers {
        let job_rx = Arc::clone(&job_rx);
        let result_tx = result_tx.clone();
        let manager = manager_clone.clone_for_arc();
        let spec = spec.clone();
        
        thread::spawn(move || {
            loop {
                let job = {
                    let rx = job_rx.lock().unwrap();
                    rx.recv()
                };
                match job {
                    Ok((idx, bundle_num)) => {
                        let result = manager.verify_bundle(bundle_num, spec.clone());
                        let valid = match &result {
                            Ok(r) => r.valid,
                            Err(_) => false,
                        };
                        let err = result.err();
                        result_tx.send((idx, bundle_num, valid, err)).unwrap();
                    }
                    Err(_) => break, // Channel closed, worker done
                }
            }
        });
    }
    
    // Send jobs
    for (idx, meta) in bundles.iter().enumerate() {
        job_tx.send((idx, meta.bundle_number))?;
    }
    drop(job_tx); // Close sender, workers will finish

    // Collect results and update progress in real-time
    let mut results: Vec<(usize, u32, bool, Option<anyhow::Error>)> = Vec::new();
    results.reserve(bundles.len());
    
    let mut verified_count = 0;
    let mut error_count = 0;
    let mut first_error: Option<anyhow::Error> = None;
    let mut failed_bundles = Vec::new();
    let mut completed = 0;

    // Collect results as they arrive and update progress immediately
    let mut total_uncompressed_processed = 0u64;
    for _ in 0..bundles.len() {
        let (idx, bundle_num, valid, err) = result_rx.recv()?;
        completed += 1;
        
        // Track uncompressed bytes processed
        if let Some(meta) = bundles.iter().find(|b| b.bundle_number == bundle_num) {
            total_uncompressed_processed += meta.uncompressed_size;
        }
        
        // Update progress bar immediately with bytes
        if let Some(ref pb) = progress {
            pb.set_with_bytes(completed, total_uncompressed_processed);
        }
        
        if !valid {
            if verbose {
                eprintln!("  Bundle {:06}... INVALID", bundle_num);
            }
            eprintln!("\n✗ Bundle {:06} hash verification failed", bundle_num);
            if let Some(ref e) = err {
                eprintln!("  Error: {}", e);
                if first_error.is_none() {
                    first_error = Some(anyhow::anyhow!("{}", e));
                }
            }
            error_count += 1;
            failed_bundles.push(bundle_num);
        } else {
            verified_count += 1;
        }
        
        results.push((idx, bundle_num, valid, err));
    }
    
    // Sort results by index for consistent error reporting
    results.sort_by_key(|r| r.0);

    if let Some(ref pb) = progress {
        pb.finish();
    }

    // Pass 2: Verify chain links sequentially
    if error_count == 0 {
        eprintln!("\nPass 2: Verifying chain links...");
        for i in 1..bundles.len() {
            let prev_meta = &bundles[i - 1];
            let meta = &bundles[i];
            
            if meta.parent != prev_meta.hash {
                eprintln!("\n✗ Chain broken at bundle {:06}", meta.bundle_number);
                eprintln!("  Expected parent: {}...", &prev_meta.hash[..16.min(prev_meta.hash.len())]);
                eprintln!("  Actual parent:   {}...", &meta.parent[..16.min(meta.parent.len())]);
                error_count += 1;
                if first_error.is_none() {
                    first_error = Some(anyhow::anyhow!("chain broken at bundle {}", meta.bundle_number));
                }
            }
        }
        if error_count == 0 {
            eprintln!("✓ All chain links valid");
        }
    }

    let elapsed = start.elapsed();

    eprintln!();
    if error_count == 0 {
        if !full {
            eprintln!("⚠ Note: This was a partial verification (compressed hash only).");
            eprintln!("  For full integrity check including content hash, run with --full flag.");
            eprintln!();
        }
        eprintln!("✓ Chain is valid ({} bundles verified)", verified_count);
        eprintln!("  First bundle: {:06}", bundles[0].bundle_number);
        eprintln!("  Last bundle:  {:06}", bundles[bundles.len() - 1].bundle_number);
        eprintln!("  Chain root:   {}", bundles[0].hash);
        eprintln!("  Chain head:   {}", bundles[bundles.len() - 1].hash);

        // Additional stats
        let total_size: u64 = bundles.iter().map(|b| b.compressed_size).sum();
        let total_ops: u64 = bundles.iter().map(|b| b.operation_count as u64).sum();
        let total_dids: u64 = bundles.iter().map(|b| b.did_count as u64).sum();
        
        eprintln!("\nStatistics:");
        eprintln!("  Total size:     {} ({:.2} MB compressed)", 
            total_size, total_size as f64 / (1024.0 * 1024.0));
        eprintln!("  Total ops:      {}", total_ops);
        eprintln!("  Total DIDs:     {}", total_dids);
        eprintln!("  Avg ops/bundle: {:.1}", total_ops as f64 / bundles.len() as f64);
        eprintln!("  Avg size/bundle: {:.2} MB", (total_size as f64 / bundles.len() as f64) / (1024.0 * 1024.0));

        // Timing information
        eprintln!("\nPerformance:");
        eprintln!("  Time:       {:?}", elapsed);
        if elapsed.as_secs_f64() > 0.0 {
            let bundles_per_sec = verified_count as f64 / elapsed.as_secs_f64();
            eprintln!("  Throughput: {:.1} bundles/sec", bundles_per_sec);

            if total_size > 0 {
                let mb_per_sec_compressed = total_size as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
                eprintln!("  Data rate:  {:.1} MB/sec (compressed)", mb_per_sec_compressed);
            }
            
            if total_uncompressed_size > 0 {
                let mb_per_sec_uncompressed = total_uncompressed_size as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
                eprintln!("  Data rate:  {:.1} MB/sec (uncompressed)", mb_per_sec_uncompressed);
            }
        }
        Ok(())
    } else {
        eprintln!("✗ Chain verification failed");
        eprintln!("  Verified: {}/{} bundles", verified_count, bundles.len());
        eprintln!("  Errors: {}", error_count);
        if !failed_bundles.is_empty() && failed_bundles.len() <= 10 {
            eprintln!("  Failed bundles: {:?}", failed_bundles);
        }
        eprintln!("  Time: {:?}", elapsed);
        if let Some(err) = first_error {
            Err(err)
        } else {
            bail!("chain verification failed")
        }
    }
}

fn verify_range(
    manager: &BundleManager,
    range_str: &str,
    verbose: bool,
    parallel: bool,
    workers: usize,
    full: bool,
    num_threads: usize,
) -> Result<()> {
    let (start, end) = parse_bundle_range_simple(range_str)?;

    eprint!("Verifying bundles {:06} - {:06}", start, end);
    if parallel {
        eprint!(" (parallel, {} workers)", workers);
    }
    eprintln!();

    let total = end - start + 1;
    let overall_start = Instant::now();

    let verify_err = if parallel {
        verify_range_parallel(manager, start, end, num_threads.min(workers), verbose, full)
    } else {
        verify_range_sequential(manager, start, end, total as usize, verbose, full)
    };

    let elapsed = overall_start.elapsed();

    // Add timing summary
    eprintln!("\nPerformance:");
    eprintln!("  Time:       {:?}", elapsed);
    if elapsed.as_secs_f64() > 0.0 {
        let bundles_per_sec = total as f64 / elapsed.as_secs_f64();
        eprintln!("  Throughput: {:.1} bundles/sec", bundles_per_sec);

        let avg_time = elapsed / total as u32;
        eprintln!("  Avg/bundle: {:?}", avg_time);
    }

    verify_err
}

fn verify_range_sequential(
    manager: &BundleManager,
    start: u32,
    end: u32,
    total: usize,
    verbose: bool,
    full: bool,
) -> Result<()> {
    let progress = if !verbose {
        Some(ProgressBar::new(total))
    } else {
        None
    };

    let mut verified = 0;
    let mut failed = 0;
    let mut failed_bundles = Vec::new();

    // Verify compressed hash, content hash only if --full
    let spec = VerifySpec {
        check_hash: true,
        check_content_hash: full,
        check_operations: false,
    };

    for bundle_num in start..=end {
        let result = manager.verify_bundle(bundle_num, spec.clone());

        if verbose {
            eprint!("Bundle {:06}: ", bundle_num);
        }

        match result {
            Err(e) => {
                if verbose {
                    eprintln!("ERROR - {}", e);
                }
                failed += 1;
                failed_bundles.push(bundle_num);
            }
            Ok(result) => {
                if !result.valid {
                    if verbose {
                        let err_msg = if result.errors.is_empty() {
                            "invalid".to_string()
                        } else {
                            result.errors[0].clone()
                        };
                        eprintln!("INVALID - {}", err_msg);
                    }
                    failed += 1;
                    failed_bundles.push(bundle_num);
                } else {
                    if verbose {
                        eprintln!("✓");
                    }
                    verified += 1;
                }
            }
        }

        if let Some(ref pb) = progress {
            pb.set((bundle_num - start + 1) as usize);
        }
    }

    if let Some(ref pb) = progress {
        pb.finish();
    }

    eprintln!();
    if failed == 0 {
        eprintln!("✓ All {} bundles verified successfully", verified);
        Ok(())
    } else {
        eprintln!("✗ Verification failed");
        eprintln!("  Verified: {}/{}", verified, total);
        eprintln!("  Failed: {}", failed);

        if !failed_bundles.is_empty() && failed_bundles.len() <= 20 {
            eprint!("\nFailed bundles: ");
            for (i, num) in failed_bundles.iter().enumerate() {
                if i > 0 {
                    eprint!(", ");
                }
                eprint!("{:06}", num);
            }
            eprintln!();
        } else if failed_bundles.len() > 20 {
            eprintln!("\nFailed bundles: {:06}, {:06}, ... and {} more",
                failed_bundles[0], failed_bundles[1], failed_bundles.len() - 2);
        }

        bail!("verification failed for {} bundles", failed)
    }
}

fn verify_range_parallel(
    manager: &BundleManager,
    start: u32,
    end: u32,
    workers: usize,
    verbose: bool,
    full: bool,
) -> Result<()> {
    // Note: Parallel verification requires Arc<BundleManager> which needs to be implemented
    // For now, fall back to sequential with progress
    let total = (end - start + 1) as usize;
    if verbose {
        eprintln!("[DEBUG] Using {} worker thread(s) for parallel verification", workers);
    }
    eprintln!("Note: Parallel verification not yet fully implemented, using sequential");
    verify_range_sequential(manager, start, end, total, verbose, full)
}
