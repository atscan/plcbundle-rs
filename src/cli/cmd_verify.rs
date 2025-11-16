use super::progress::ProgressBar;
use super::utils::{format_bytes, format_bytes_per_sec, format_number, HasGlobalFlags};
use anyhow::{Result, bail};
use clap::Args;
use plcbundle::{BundleManager, VerifyResult, VerifySpec};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Args)]
#[command(
    about = "Verify bundle integrity and chain",
    long_about = "Validates the cryptographic integrity of bundles and ensures the chain
of bundles is properly linked. This is essential for verifying that your
repository hasn't been corrupted or tampered with.

Verification operates in three modes:
  • fast   - Only check metadata frame (fastest, least thorough)
  • normal - Verify compressed hash (default, balanced)
  • full   - Verify compressed + content hash (slowest, most thorough)

Fast mode is useful for quick checks, while full mode provides complete
assurance that bundle contents match their cryptographic commitments.

When verifying the entire chain, the command performs a two-pass validation:
first verifying all bundle hashes in parallel, then sequentially checking
that each bundle correctly references its parent's hash. This ensures both
individual bundle integrity and chain continuity.

Use --bundles to verify specific bundles or ranges, or omit it to verify
the entire repository chain.",
    help_template = crate::clap_help!(
        examples: "  # Verify entire chain\n  \
                   {bin} verify\n  \
                   {bin} verify --chain\n\n  \
                   # Verify specific bundle\n  \
                   {bin} verify --bundles 42\n\n  \
                   # Verify range of bundles\n  \
                   {bin} verify --bundles 1-100\n\n  \
                   # Verify multiple ranges\n  \
                   {bin} verify --bundles 1-10,20-30\n\n  \
                   # Fast verification (metadata only)\n  \
                   {bin} verify --fast\n\n  \
                   # Full verification (content hash)\n  \
                   {bin} verify --full\n\n  \
                   # Parallel verification (faster for ranges)\n  \
                   {bin} verify --bundles 1-1000 -j 8"
    )
)]
pub struct VerifyCommand {
    /// Bundle range to verify (e.g., "42", "1-100", or "1-10,20-30")
    #[arg(long)]
    pub bundles: Option<String>,

    /// Verify entire chain (default)
    #[arg(short, long)]
    pub chain: bool,

    /// Full verification (includes content hash check)
    #[arg(long)]
    pub full: bool,

    /// Fast verification (only check metadata frame, skip hash calculations)
    #[arg(long)]
    pub fast: bool,

    /// Number of threads to use (0 = auto-detect)
    #[arg(short = 'j', long, default_value = "0")]
    pub threads: usize,
}

impl HasGlobalFlags for VerifyCommand {
    fn verbose(&self) -> bool { false }
    fn quiet(&self) -> bool { false }
}

pub fn run(cmd: VerifyCommand, dir: PathBuf, global_verbose: bool) -> Result<()> {
    let manager = super::utils::create_manager_from_cmd(dir.clone(), &cmd)?;

    // Determine number of threads
    let num_threads = if cmd.threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    } else {
        cmd.threads
    };

    // Show thread count in debug/verbose mode
    if global_verbose {
        eprintln!("[DEBUG] Using {} thread(s) for verification", num_threads);
    }

    if !global_verbose {
        eprintln!(
            "\n📁 Working in: {}\n",
            super::utils::display_path(&dir).display()
        );
    }

    // Determine what to verify
    if let Some(bundles_str) = cmd.bundles {
        let last_bundle = manager.get_last_bundle();
        let bundle_nums = super::utils::parse_bundle_spec(Some(bundles_str), last_bundle)?;
        
        if bundle_nums.len() == 1 {
            verify_single_bundle(&manager, bundle_nums[0], global_verbose, cmd.full, cmd.fast)?;
        } else {
            // For multiple bundles, verify as range
            let start = bundle_nums[0];
            let end = bundle_nums[bundle_nums.len() - 1];
            verify_range(
                &manager,
                start,
                end,
                global_verbose,
                cmd.full,
                cmd.fast,
                num_threads,
            )?;
        }
    } else {
        // Default: verify entire chain
        verify_chain(&manager, global_verbose, cmd.full, cmd.fast, num_threads)?;
    }

    Ok(())
}

fn verify_single_bundle(
    manager: &BundleManager,
    bundle_num: u32,
    verbose: bool,
    full: bool,
    fast: bool,
) -> Result<()> {
    // Print verification mode at the beginning
    eprintln!("\n🔍 Verification Mode:");
    if fast {
        eprintln!("  ⚡ FAST (metadata frame only)");
        eprintln!("  ℹ️  Use without --fast for normal verification (compressed hash)");
        eprintln!("  ℹ️  Use --full for complete verification (compressed + content hash)");
    } else if full {
        eprintln!("  🔐 FULL (compressed hash + content hash)");
        eprintln!("  ℹ️  Use without --full for normal verification (compressed hash only)");
        eprintln!("  ℹ️  Use --fast for fast verification (metadata frame only)");
    } else {
        eprintln!("  ✓ NORMAL (compressed hash only)");
        eprintln!("  ℹ️  Use --full for complete verification (compressed + content hash)");
        eprintln!("  ℹ️  Use --fast for fast verification (metadata frame only)");
    }
    eprintln!();

    eprintln!("🔬 Verifying bundle {}...", bundle_num);

    let start = Instant::now();
    // For single bundle, check content hash if --full flag is set
    let spec = VerifySpec {
        check_hash: !fast,                 // Skip hash check in fast mode
        check_content_hash: full && !fast, // Skip content hash in fast mode
        check_operations: full && !fast,   // Skip operation count in fast mode
        fast,
    };
    let result = manager.verify_bundle(bundle_num, spec)?;
    let elapsed = start.elapsed();

    if result.valid {
        eprintln!("✅ Bundle {} is valid ({:?})", bundle_num, elapsed);

        // Show what was verified
        let mut verified_items = Vec::new();
        if fast {
            verified_items.push("metadata frame");
        } else {
            verified_items.push("compressed hash");
            if full {
                verified_items.push("content hash");
            }
        }
        eprintln!("  ✓ Verified: {}", verified_items.join(", "));

        if fast {
            eprintln!("\nℹ️  Note: This was a fast verification (metadata frame only).");
            eprintln!("   Use without --fast for normal verification (compressed hash)");
            eprintln!("   Use --full for complete verification (compressed + content hash)");
        } else if !full {
            eprintln!("\n⚠️  Note: This was a partial verification (compressed hash only).");
            eprintln!("   Use --full for complete verification (compressed + content hash)");
            eprintln!("   Use --fast for fast verification (metadata frame only)");
        }

        if verbose {
            eprintln!("\nDetails:");
            eprintln!(
                "  Errors: {}",
                if result.errors.is_empty() {
                    "none"
                } else {
                    "yes"
                }
            );
            if !result.errors.is_empty() {
                for err in &result.errors {
                    eprintln!("    - {}", err);
                }
            }
            eprintln!("  Verification time: {:?}", elapsed);
        }
        Ok(())
    } else {
        eprintln!("❌ Bundle {} is invalid ({:?})", bundle_num, elapsed);
        if !result.errors.is_empty() {
            eprintln!("\n⚠️  Errors:");
            for err in &result.errors {
                eprintln!("   • {}", err);
            }
        }
        bail!("bundle verification failed")
    }
}

fn verify_chain(
    manager: &BundleManager,
    verbose: bool,
    full: bool,
    fast: bool,
    num_threads: usize,
) -> Result<()> {
    if super::utils::is_repository_empty(manager) {
        eprintln!("ℹ️  No bundles to verify");
        return Ok(());
    }

    // Get all bundle metadata
    let bundles = super::utils::get_all_bundle_metadata(manager);

    if bundles.is_empty() {
        eprintln!("ℹ️  No bundles to verify");
        return Ok(());
    }

    // Print verification mode at the beginning
    eprintln!("\n🔍 Verification Mode:");
    if fast {
        eprintln!("  ⚡ FAST (metadata frame only)");
        eprintln!("  ℹ️  Use without --fast for normal verification (compressed hash)");
        eprintln!("  ℹ️  Use --full for complete verification (compressed + content hash)");
    } else if full {
        eprintln!("  🔐 FULL (compressed hash + content hash)");
        eprintln!("  ℹ️  Use without --full for normal verification (compressed hash only)");
        eprintln!("  ℹ️  Use --fast for fast verification (metadata frame only)");
    } else {
        eprintln!("  ✓ NORMAL (compressed hash only)");
        eprintln!("  ℹ️  Use --full for complete verification (compressed + content hash)");
        eprintln!("  ℹ️  Use --fast for fast verification (metadata frame only)");
    }
    eprintln!();

    // Print root hash (first bundle) and head hash (latest) at start
    eprintln!("🔗 Chain Information:");
    eprintln!(
        "   Root:   {} (bundle {})",
        bundles[0].hash, bundles[0].bundle_number
    );
    eprintln!(
        "   Head:   {} (bundle {})",
        bundles[bundles.len() - 1].hash,
        bundles[bundles.len() - 1].bundle_number
    );
    eprintln!("   Total:  {} bundles", format_number(bundles.len() as u64));
    eprintln!();

    eprintln!(
        "🔬 Verifying chain of {} bundles...\n",
        format_number(bundles.len() as u64)
    );

    let start = Instant::now();

    // Two-pass parallel verification:
    // Pass 1: Verify all bundle hashes in parallel
    // Pass 2: Verify chain links sequentially (needs previous results)

    let spec = VerifySpec {
        check_hash: !fast,                 // Skip hash check in fast mode
        check_content_hash: full && !fast, // Skip content hash in fast mode
        check_operations: false,
        fast,
    };

    // Calculate total uncompressed size for progress tracking
    let total_uncompressed_size: u64 = bundles.iter().map(|b| b.uncompressed_size).sum();

    // Pass 1: Parallel bundle hash verification
    eprintln!("📦 Pass 1: Verifying bundle hashes...");
    // Always show progress bar (it will detect if TTY and show appropriate format)
    let progress = Some(ProgressBar::with_bytes(
        bundles.len(),
        total_uncompressed_size,
    ));

    use std::sync::Arc;
    use std::sync::mpsc;
    use std::thread;

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
                        let verify_result = match result {
                            Ok(vr) => vr,
                            Err(e) => {
                                // Convert error to VerifyResult for consistency
                                VerifyResult {
                                    valid: false,
                                    errors: vec![e.to_string()],
                                }
                            }
                        };
                        result_tx.send((idx, bundle_num, verify_result)).unwrap();
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
    let mut results: Vec<(usize, u32, VerifyResult)> = Vec::new();
    results.reserve(bundles.len());

    let mut verified_count = 0;
    let mut error_count = 0;
    let mut first_error: Option<anyhow::Error> = None;
    let mut failed_bundles: Vec<(u32, Vec<String>)> = Vec::new();
    let mut completed = 0;

    // Collect results as they arrive and update progress immediately
    let mut total_uncompressed_processed = 0u64;
    for _ in 0..bundles.len() {
        let (idx, bundle_num, verify_result) = result_rx.recv()?;
        completed += 1;

        // Track uncompressed bytes processed
        if let Some(meta) = bundles.iter().find(|b| b.bundle_number == bundle_num) {
            total_uncompressed_processed += meta.uncompressed_size;
        }

        // Update progress bar immediately with bytes
        if let Some(ref pb) = progress {
            pb.set_with_bytes(completed, total_uncompressed_processed);
        }

        if !verify_result.valid {
            error_count += 1;
            let errors = verify_result.errors.clone();
            failed_bundles.push((bundle_num, errors.clone()));

            // Only print per-bundle errors in verbose mode
            if verbose {
                eprintln!("\n❌ Bundle {} verification failed", bundle_num);
                if !errors.is_empty() {
                    eprintln!("  ⚠️  Errors:");
                    for err in &errors {
                        eprintln!("     • {}", err);
                    }

                    // Provide helpful hint for common issues
                    let has_hash_mismatch = errors.iter().any(|e| {
                        e.contains("hash") && e.contains("mismatch")
                    });
                    if has_hash_mismatch {
                        eprintln!(
                            "  💡 Hint: Bundle file may have been migrated but index wasn't updated."
                        );
                        eprintln!("          Run 'migrate --force' to recalculate all hashes.");
                    }
                } else {
                    eprintln!("  ⚠️  Verification failed (no error details available)");
                }
            }

            // Store first error for summary
            if first_error.is_none() {
                if let Some(first_err) = errors.first() {
                    first_error = Some(anyhow::anyhow!("{}", first_err));
                }
            }
        } else {
            verified_count += 1;
        }

        results.push((idx, bundle_num, verify_result));
    }

    // Sort results by index for consistent error reporting
    results.sort_by_key(|r| r.0);

    if let Some(ref pb) = progress {
        pb.finish();
    }

    // Pass 2: Verify chain links sequentially
    if error_count == 0 {
        eprintln!("\n🔗 Pass 2: Verifying chain links...");
        for i in 1..bundles.len() {
            let prev_meta = &bundles[i - 1];
            let meta = &bundles[i];

            if meta.parent != prev_meta.hash {
                eprintln!("\n❌ Chain broken at bundle {}", meta.bundle_number);
                eprintln!(
                    "  ⚠️  Expected parent: {}...",
                    &prev_meta.hash[..16.min(prev_meta.hash.len())]
                );
                eprintln!(
                    "  ⚠️  Actual parent:   {}...",
                    &meta.parent[..16.min(meta.parent.len())]
                );
                error_count += 1;
                if first_error.is_none() {
                    first_error = Some(anyhow::anyhow!(
                        "chain broken at bundle {}",
                        meta.bundle_number
                    ));
                }
            }
        }
        if error_count == 0 {
            eprintln!("✅ All chain links valid");
        }
    }

    let elapsed = start.elapsed();

    eprintln!();
    if error_count == 0 {
        // Show what was verified
        let mut verified_items = Vec::new();
        if fast {
            verified_items.push("metadata frames");
        } else {
            verified_items.push("compressed hashes");
            if full {
                verified_items.push("content hashes");
            }
        }
        verified_items.push("chain links");
        eprintln!(
            "\n✅ Chain is valid ({} bundles verified)",
            format_number(verified_count as u64)
        );
        eprintln!("   ✓ Verified: {}", verified_items.join(", "));
        eprintln!();

        if fast {
            eprintln!("ℹ️  Note: This was a fast verification (metadata frame only).");
            eprintln!("   Use without --fast for normal verification (compressed hash)");
            eprintln!("   Use --full for complete verification (compressed + content hash)");
        } else if !full {
            eprintln!("⚠️  Note: This was a partial verification (compressed hash only).");
            eprintln!("   Use --full for complete verification (compressed + content hash)");
            eprintln!("   Use --fast for fast verification (metadata frame only)");
        }
        eprintln!();

        eprintln!("📊 Chain Summary:");
        eprintln!("   First bundle: {}", bundles[0].bundle_number);
        eprintln!(
            "   Last bundle:  {}",
            bundles[bundles.len() - 1].bundle_number
        );
        eprintln!("   Chain root:   {}", bundles[0].hash);
        eprintln!("   Chain head:   {}", bundles[bundles.len() - 1].hash);

        // Additional stats
        let total_size: u64 = bundles.iter().map(|b| b.compressed_size).sum();
        let total_ops: u64 = bundles.iter().map(|b| b.operation_count as u64).sum();
        let total_dids: u64 = bundles.iter().map(|b| b.did_count as u64).sum();

        eprintln!("\n📈 Statistics:");
        eprintln!(
            "   Total size:     {} (compressed)",
            format_bytes(total_size)
        );
        eprintln!("   Total ops:      {}", format_number(total_ops));
        eprintln!("   Total DIDs:     {}", format_number(total_dids));
        eprintln!(
            "   Avg ops/bundle: {}",
            format_number(total_ops / bundles.len() as u64)
        );
        eprintln!(
            "   Avg size/bundle: {}",
            format_bytes(total_size / bundles.len() as u64)
        );

        // Timing information
        eprintln!("\n⚡ Performance:");
        eprintln!("   Time:       {:?}", elapsed);
        if elapsed.as_secs_f64() > 0.0 {
            let bundles_per_sec = verified_count as f64 / elapsed.as_secs_f64();
            eprintln!("   Throughput: {:.1} bundles/sec", bundles_per_sec);

            if total_size > 0 {
                let bytes_per_sec_compressed = total_size as f64 / elapsed.as_secs_f64();
                eprintln!(
                    "   Data rate:  {} (compressed)",
                    format_bytes_per_sec(bytes_per_sec_compressed)
                );
            }

            if total_uncompressed_size > 0 {
                let bytes_per_sec_uncompressed = total_uncompressed_size as f64 / elapsed.as_secs_f64();
                eprintln!(
                    "   Data rate:  {} (uncompressed)",
                    format_bytes_per_sec(bytes_per_sec_uncompressed)
                );
            }
        }
        Ok(())
    } else {
        eprintln!("\n❌ Chain verification failed");
        eprintln!("   Verified: {}/{} bundles", verified_count, bundles.len());
        eprintln!("   Errors:   {}", error_count);
        
        // Show failed bundles with their error messages
        if !failed_bundles.is_empty() {
            if failed_bundles.len() <= 10 {
                eprintln!("\n   ⚠️  Failed bundles:");
                for (bundle_num, errors) in &failed_bundles {
                    eprintln!("      Bundle {}:", bundle_num);
                    if errors.is_empty() {
                        eprintln!("         • Verification failed (no error details)");
                    } else {
                        for err in errors {
                            eprintln!("         • {}", err);
                        }
                    }
                }
            } else {
                eprintln!(
                    "   ⚠️  Failed bundles: {} (too many to list)",
                    failed_bundles.len()
                );
                // Show first few with details
                eprintln!("\n   First few failures:");
                for (bundle_num, errors) in failed_bundles.iter().take(5) {
                    eprintln!("      Bundle {}:", bundle_num);
                    if errors.is_empty() {
                        eprintln!("         • Verification failed (no error details)");
                    } else {
                        for err in errors.iter().take(3) {
                            eprintln!("         • {}", err);
                        }
                        if errors.len() > 3 {
                            eprintln!("         • ... and {} more error(s)", errors.len() - 3);
                        }
                    }
                }
                eprintln!("      ... and {} more failed bundles", failed_bundles.len() - 5);
            }
        }
        
        eprintln!("   Time:     {:?}", elapsed);

        // Show helpful hint if hash mismatch detected
        if let Some(ref err) = first_error {
            let err_msg = err.to_string();
            if err_msg.contains("hash") && err_msg.contains("mismatch") {
                eprintln!(
                    "\n💡 Hint: Bundle files may have been migrated but index wasn't updated."
                );
                eprintln!("        Run 'migrate --force' to recalculate all hashes.");
            }
        }

        if !verbose {
            eprintln!("\n   Use --verbose to see details of each failed bundle as they are found.");
        }

        if let Some(err) = first_error {
            Err(err)
        } else {
            bail!("chain verification failed")
        }
    }
}

fn verify_range(
    manager: &BundleManager,
    start: u32,
    end: u32,
    verbose: bool,
    full: bool,
    fast: bool,
    num_threads: usize,
) -> Result<()> {
    let use_parallel = num_threads > 1;
    
    eprintln!("\n🔬 Verifying bundles {} - {}", start, end);
    if use_parallel {
        eprintln!("   Using {} worker thread(s)", num_threads);
    }
    eprintln!();

    let total = end - start + 1;
    let overall_start = Instant::now();

    let verify_err = if use_parallel {
        verify_range_parallel(
            manager,
            start,
            end,
            num_threads,
            verbose,
            full,
            fast,
        )
    } else {
        verify_range_sequential(manager, start, end, total as usize, verbose, full, fast)
    };

    let elapsed = overall_start.elapsed();

    // Add timing summary
    eprintln!("\n⚡ Performance:");
    eprintln!("   Time:       {:?}", elapsed);
    if elapsed.as_secs_f64() > 0.0 {
        let bundles_per_sec = total as f64 / elapsed.as_secs_f64();
        eprintln!("   Throughput: {:.1} bundles/sec", bundles_per_sec);

        let avg_time = elapsed / total as u32;
        eprintln!("   Avg/bundle: {:?}", avg_time);
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
    fast: bool,
) -> Result<()> {
    // Prefer bytes-aware progress bar to show MB/s if metadata available
    let (progress, mut processed_uncompressed, per_bundle_uncompressed): (
        Option<ProgressBar>,
        u64,
        Vec<(u32, u64)>,
    ) = if !verbose {
        let index = manager.get_index();
        let mut sizes = Vec::with_capacity(total);
        let mut total_uncompressed_size: u64 = 0;
        for num in start..=end {
            if let Some(meta) = index.get_bundle(num) {
                sizes.push((num, meta.uncompressed_size));
                total_uncompressed_size += meta.uncompressed_size;
            } else {
                sizes.push((num, 0));
            }
        }
        (
            Some(ProgressBar::with_bytes(total, total_uncompressed_size)),
            0u64,
            sizes,
        )
    } else {
        (None, 0, Vec::new())
    };

    let mut verified = 0;
    let mut failed = 0;
    let mut failed_bundles: Vec<(u32, Vec<String>)> = Vec::new();

    // Verify compressed hash, content hash only if --full
    let spec = VerifySpec {
        check_hash: !fast,                 // Skip hash check in fast mode
        check_content_hash: full && !fast, // Skip content hash in fast mode
        check_operations: false,
        fast,
    };

    for bundle_num in start..=end {
        let result = manager.verify_bundle(bundle_num, spec.clone());

        if verbose {
            eprint!("Bundle {}: ", bundle_num);
        }

        match result {
            Err(e) => {
                let errors = vec![e.to_string()];
                if verbose {
                    eprintln!("❌ ERROR - {}", errors[0]);
                }
                failed += 1;
                failed_bundles.push((bundle_num, errors));
            }
            Ok(result) => {
                if !result.valid {
                    if verbose {
                        if result.errors.is_empty() {
                            eprintln!("❌ INVALID - verification failed (no error details)");
                        } else {
                            eprintln!("❌ INVALID:");
                            for err in &result.errors {
                                eprintln!("   • {}", err);
                            }
                        }
                    }
                    failed += 1;
                    failed_bundles.push((bundle_num, result.errors));
                } else {
                    if verbose {
                        eprintln!("✅");
                    }
                    verified += 1;
                }
            }
        }

        if let Some(ref pb) = progress {
            if let Some((_, sz)) = per_bundle_uncompressed
                .get((bundle_num - start) as usize)
                .cloned()
            {
                processed_uncompressed = processed_uncompressed.saturating_add(sz);
                pb.set_with_bytes((bundle_num - start + 1) as usize, processed_uncompressed);
            } else {
                pb.set((bundle_num - start + 1) as usize);
            }
        }
    }

    if let Some(ref pb) = progress {
        pb.finish();
    }

    eprintln!();
    if failed == 0 {
        eprintln!("✅ All {} bundles verified successfully", verified);
        Ok(())
    } else {
        eprintln!("❌ Verification failed");
        eprintln!("   Verified: {}/{}", verified, total);
        eprintln!("   Failed:   {}", failed);

        if !failed_bundles.is_empty() {
            if failed_bundles.len() <= 20 {
                eprintln!("\n   ⚠️  Failed bundles:");
                for (bundle_num, errors) in &failed_bundles {
                    eprintln!("      Bundle {}:", bundle_num);
                    if errors.is_empty() {
                        eprintln!("         • Verification failed (no error details)");
                    } else {
                        for err in errors {
                            eprintln!("         • {}", err);
                        }
                    }
                }
            } else {
                eprintln!(
                    "\n   ⚠️  Failed bundles: {} (too many to list)",
                    failed_bundles.len()
                );
                // Show first few with details
                eprintln!("\n   First few failures:");
                for (bundle_num, errors) in failed_bundles.iter().take(5) {
                    eprintln!("      Bundle {}:", bundle_num);
                    if errors.is_empty() {
                        eprintln!("         • Verification failed (no error details)");
                    } else {
                        for err in errors.iter().take(3) {
                            eprintln!("         • {}", err);
                        }
                        if errors.len() > 3 {
                            eprintln!("         • ... and {} more error(s)", errors.len() - 3);
                        }
                    }
                }
                eprintln!("      ... and {} more failed bundles", failed_bundles.len() - 5);
            }
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
    fast: bool,
) -> Result<()> {
    // Note: Parallel verification requires Arc<BundleManager> which needs to be implemented
    // For now, fall back to sequential with progress
    let total = (end - start + 1) as usize;
    if verbose {
        eprintln!(
            "[DEBUG] Using {} worker thread(s) for parallel verification",
            workers
        );
    }
    eprintln!("Note: Parallel verification not yet fully implemented, using sequential");
    verify_range_sequential(manager, start, end, total, verbose, full, fast)
}
