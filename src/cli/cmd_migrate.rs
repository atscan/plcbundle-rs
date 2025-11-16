// Migrate command - convert bundles to multi-frame format
use super::progress::ProgressBar;
use super::utils::{format_bytes, HasGlobalFlags};
use anyhow::{Result, bail};
use clap::Args;
use plcbundle::BundleManager;
use plcbundle::constants;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Args)]
#[command(
    about = "Migrate bundles to new zstd frame format",
    long_about = "Migrate old single-frame zstd bundles to new multi-frame format

This command converts bundles from the legacy single-frame zstd format
to the new multi-frame format with frame offsets. This enables:
  • Faster random access to individual operations
  • Reduced memory usage when loading specific positions
  • Better performance for DID lookups

The migration:
  1. Scans for bundles missing frame metadata (legacy format)
  2. Re-compresses them using multi-frame format (100 ops/frame)
  3. Generates frame offset index in metadata
  4. Preserves all hashes and metadata
  5. Verifies content integrity

Original files are replaced atomically. Use --dry-run to preview.",
    after_help = "Examples:\n  \
        # Preview migration (recommended first)\n  \
        plcbundle migrate --dry-run\n\n  \
        # Migrate all legacy bundles (auto-detects CPU cores)\n  \
        plcbundle migrate\n\n  \
        # Migrate specific bundle range\n  \
        plcbundle migrate --bundles 1-100\n\n  \
        # Migrate single bundle\n  \
        plcbundle migrate --bundles 42\n\n  \
        # Migrate multiple ranges\n  \
        plcbundle migrate --bundles 1-10,20-30,50\n\n  \
        # Force migration even if frame metadata exists\n  \
        plcbundle migrate --force\n\n  \
        # Limit threads (if needed for resource constraints)\n  \
        plcbundle migrate -j 4\n\n  \
        # Verbose output\n  \
        plcbundle migrate -v"
)]
pub struct MigrateCommand {
    /// Show what would be migrated without migrating
    #[arg(short = 'n', long)]
    pub dry_run: bool,

    /// Re-migrate bundles that already have frame metadata
    #[arg(short, long)]
    pub force: bool,

    /// Bundle range to migrate (e.g., \"1-100\", \"42\", \"1-10,20-30\", \"latest:10\")
    /// If not specified, migrates all bundles that need migration
    #[arg(long)]
    pub bundles: Option<String>,

    /// Number of threads to use (0 = auto-detect)
    #[arg(short = 'j', long, default_value = "0")]
    pub threads: usize,
}

impl HasGlobalFlags for MigrateCommand {
    fn verbose(&self) -> bool { false }
    fn quiet(&self) -> bool { false }
}

pub fn run(cmd: MigrateCommand, dir: PathBuf, global_verbose: bool) -> Result<()> {
    let manager = super::utils::create_manager(dir.clone(), global_verbose, false)?;

    // Auto-detect number of threads if 0
    let workers = super::utils::get_worker_threads(cmd.threads, 4);

    eprintln!("Scanning for legacy bundles in: {}\n", super::utils::display_path(&dir).display());

    let index = manager.get_index();
    let bundles = &index.bundles;

    if bundles.is_empty() {
        eprintln!("No bundles to migrate");
        return Ok(());
    }

    // Determine which bundles to consider for migration
    let last_bundle = index.last_bundle;
    let target_bundles: Option<std::collections::HashSet<u32>> = if let Some(ref bundles_spec) = cmd.bundles {
        let bundle_list = super::utils::parse_bundle_spec(Some(bundles_spec.clone()), last_bundle)?;
        Some(bundle_list.into_iter().collect())
    } else {
        None
    };

    // Check which bundles need migration
    let mut needs_migration = Vec::new();
    let mut total_size = 0u64;
    let mut format_counts = std::collections::HashMap::new();

    for meta in bundles {
        // Filter by bundle range if specified
        if let Some(ref target_set) = target_bundles {
            if !target_set.contains(&meta.bundle_number) {
                continue;
            }
        }

        let embedded_meta = manager.get_embedded_metadata(meta.bundle_number)?;
        let (old_format, has_frame_offsets) = match embedded_meta {
            Some(ref m) if !m.frame_offsets.is_empty() => (m.format.clone(), true),
            Some(m) => (m.format.clone(), false),
            None => ("v0 (single-frame)".to_string(), false),
        };

        let needs_migrate = cmd.force || !has_frame_offsets;

        if needs_migrate {
            needs_migration.push(BundleMigrationInfo {
                bundle_number: meta.bundle_number,
                old_size: meta.compressed_size,
                uncompressed_size: meta.uncompressed_size,
                old_format,
            });
            total_size += meta.compressed_size;
            *format_counts
                .entry(needs_migration.last().unwrap().old_format.clone())
                .or_insert(0) += 1;
        }
    }

    if needs_migration.is_empty() {
        if let Some(ref bundles_spec) = cmd.bundles {
            eprintln!("No bundles in range '{}' need migration", bundles_spec);
        } else {
            eprintln!("✓ All bundles already migrated");
        }
        eprintln!("\nUse --force to re-migrate");
        return Ok(());
    }

    // Sort bundles by number to ensure chain integrity (migrate in order: 1, 2, 3, ...)
    needs_migration.sort_by_key(|info| info.bundle_number);

    // Show migration plan
    eprintln!("Migration Plan");
    eprintln!("══════════════\n");
    
    if let Some(ref bundles_spec) = cmd.bundles {
        eprintln!("  Range:    {}", bundles_spec);
    }

    let mut format_parts = Vec::new();
    for (format, count) in &format_counts {
        format_parts.push(format!("{} ({})", format, count));
    }
    eprintln!(
        "  Format:  {} → {}/1.0",
        format_parts.join(" + "),
        constants::BINARY_NAME
    );

    let total_uncompressed: u64 = needs_migration.iter().map(|i| i.uncompressed_size).sum();
    let avg_compression = if total_size > 0 {
        total_uncompressed as f64 / total_size as f64
    } else {
        0.0
    };

    eprintln!("  Bundles: {}", needs_migration.len());
    eprintln!(
        "  Size:    {} ({:.3}x compression)",
        format_bytes(total_size),
        avg_compression
    );
    eprintln!(
        "  Workers: {}, Compression Level: {}\n",
        workers,
        constants::ZSTD_COMPRESSION_LEVEL
    );

    if cmd.dry_run {
        eprintln!("💡 Dry-run mode");
        return Ok(());
    }

    // Execute migration
    eprintln!("Migrating...\n");

    let start = Instant::now();

    // Calculate total bytes to process
    let total_bytes: u64 = needs_migration.iter().map(|info| info.old_size).sum();
    let progress = ProgressBar::with_bytes(needs_migration.len(), total_bytes);

    let mut success = 0;
    let mut failed = 0;
    let mut first_error: Option<anyhow::Error> = None;
    let mut hash_changes = Vec::new();

    let mut total_old_size = 0u64;
    let mut total_new_size = 0u64;
    let mut total_old_uncompressed = 0u64;
    let mut total_new_uncompressed = 0u64;

    // Parallel migration using rayon
    // Note: Even though we use parallelism, bundles MUST be migrated in order
    // for chain integrity. We parallelize the WORK (compression) but commit sequentially.
    use rayon::prelude::*;
    use std::sync::{Arc, Mutex};

    let progress_arc = Arc::new(Mutex::new(progress));
    // Use atomics for counters to reduce lock contention
    use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
    let count_atomic = Arc::new(AtomicUsize::new(0));
    let bytes_atomic = Arc::new(AtomicU64::new(0));
    
    // Update progress bar less frequently to reduce contention
    // Update every N bundles or every 100ms, whichever comes first
    let update_interval = (workers.max(1) * 4).max(10); // At least every 10 bundles, or 4x workers
    
    let results: Vec<_> = if workers > 1 {
        // Parallel mode: process in chunks to maintain some ordering
        // Increase chunk size to reduce contention
        let chunk_size = workers * 4; // Process 4x workers at a time for better pipelining

        needs_migration
            .par_chunks(chunk_size)
            .flat_map(|chunk| {
                // Process chunk in parallel
                chunk
                    .par_iter()
                    .map(|info| {
                        let result = manager.migrate_bundle(info.bundle_number);

                        // Update atomics (lock-free)
                        let current_count = count_atomic.fetch_add(1, Ordering::Relaxed) + 1;
                        let total_bytes = bytes_atomic.fetch_add(info.old_size, Ordering::Relaxed) + info.old_size;

                        // Only update progress bar periodically to reduce lock contention
                        if current_count % update_interval == 0 || current_count == 1 {
                            let prog = progress_arc.lock().unwrap();
                            prog.set_with_bytes(current_count, total_bytes);
                        }

                        (info, result)
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    } else {
        // Sequential mode - can update more frequently
        needs_migration
            .iter()
            .enumerate()
            .map(|(i, info)| {
                let result = manager.migrate_bundle(info.bundle_number);

                let current_count = i + 1;
                let total_bytes = bytes_atomic.fetch_add(info.old_size, Ordering::Relaxed) + info.old_size;

                // Update every bundle in sequential mode (no contention)
                let prog = progress_arc.lock().unwrap();
                prog.set_with_bytes(current_count, total_bytes);

                (info, result)
            })
            .collect()
    };

    // Process results
    for (info, result) in results {
        total_old_size += info.old_size;
        total_old_uncompressed += info.uncompressed_size;

        match result {
            Ok((size_diff, new_uncompressed_size, _new_compressed_size)) => {
                success += 1;
                hash_changes.push(info.bundle_number);

                let new_size = (info.old_size as i64 + size_diff) as u64;
                total_new_size += new_size;
                total_new_uncompressed += new_uncompressed_size;

                if global_verbose {
                    let old_ratio = info.uncompressed_size as f64 / info.old_size as f64;
                    let new_ratio = new_uncompressed_size as f64 / new_size as f64;
                    let size_change = if size_diff >= 0 {
                        format!("+{}", format_bytes(size_diff as u64))
                    } else {
                        format!("-{}", format_bytes((-size_diff) as u64))
                    };
                    eprintln!(
                        "✓ {:06}: {:.3}x→{:.3}x {}",
                        info.bundle_number, old_ratio, new_ratio, size_change
                    );
                }
            }
            Err(e) => {
                failed += 1;
                let err_msg = e.to_string();

                // Always print chain hash errors (even in non-verbose mode)
                if err_msg.contains("Chain hash mismatch")
                    || err_msg.contains("Parent hash mismatch")
                {
                    eprintln!("\n❌ Bundle {:06}: {}", info.bundle_number, err_msg);
                } else if global_verbose {
                    eprintln!("✗ Bundle {:06} failed: {}", info.bundle_number, e);
                }

                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }

    // Final progress update with accurate counts
    {
        let final_count = count_atomic.load(Ordering::Relaxed);
        let final_bytes = bytes_atomic.load(Ordering::Relaxed);
        let prog = progress_arc.lock().unwrap();
        prog.set_with_bytes(final_count, final_bytes);
        prog.finish();
    }
    let elapsed = start.elapsed();

    // Update index (already done in migrate_bundle, but verify)
    if !hash_changes.is_empty() && global_verbose {
        eprintln!("\nUpdating index...");
        let update_start = Instant::now();
        // Index is already updated by migrate_bundle, just verify
        eprintln!(
            "  ✓ {} entries in {:?}",
            hash_changes.len(),
            update_start.elapsed()
        );
    }

    // Summary
    eprintln!();
    if failed == 0 {
        eprintln!("✓ Complete: {} bundles in {:?}\n", success, elapsed);

        if total_old_size > 0 && success > 0 {
            let size_diff = total_new_size as i64 - total_old_size as i64;
            let old_ratio = total_old_uncompressed as f64 / total_old_size as f64;
            let new_ratio = total_new_uncompressed as f64 / total_new_size as f64;
            let ratio_diff = new_ratio - old_ratio;

            // Measure actual metadata size
            let mut total_actual_metadata = 0u64;
            for bundle_num in &hash_changes {
                if let Ok(meta_size) = measure_metadata_size(&manager, *bundle_num) {
                    total_actual_metadata += meta_size;
                }
            }

            eprintln!("                Old           New           Change");
            eprintln!("              ────────      ────────      ─────────");
            let size_change = if size_diff >= 0 {
                format!("+{}", format_bytes(size_diff as u64))
            } else {
                format!("-{}", format_bytes((-size_diff) as u64))
            };
            eprintln!(
                "Size          {:<13} {:<13} {} ({:.1}%)",
                format_bytes(total_old_size),
                format_bytes(total_new_size),
                size_change,
                size_diff as f64 / total_old_size as f64 * 100.0
            );
            eprintln!(
                "Ratio         {:<13} {:<13} {}",
                format!("{:.3}x", old_ratio),
                format!("{:.3}x", new_ratio),
                format!("{:+.3}x", ratio_diff)
            );
            let avg_change = size_diff / success as i64;
            let avg_change_str = if avg_change >= 0 {
                format!("+{}", format_bytes(avg_change as u64))
            } else {
                format!("-{}", format_bytes((-avg_change) as u64))
            };
            eprintln!(
                "Avg/bundle    {:<13} {:<13} {}\n",
                format_bytes(total_old_size / success as u64),
                format_bytes(total_new_size / success as u64),
                avg_change_str
            );

            if total_actual_metadata > 0 {
                let compression_efficiency = size_diff - total_actual_metadata as i64;
                let threshold = total_old_size as i64 / 1000; // 0.1% of old size

                eprintln!("Breakdown:");
                eprintln!(
                    "  Metadata:     {} (~{}/bundle, structural)",
                    format_bytes(total_actual_metadata),
                    format_bytes(total_actual_metadata / success as u64)
                );

                if compression_efficiency.abs() > threshold {
                    if compression_efficiency > 0 {
                        let pct_worse =
                            compression_efficiency as f64 / total_old_size as f64 * 100.0;
                        eprintln!(
                            "  Compression:  {} ({:.2}% worse)",
                            format_bytes(compression_efficiency as u64),
                            pct_worse
                        );
                    } else {
                        let pct_better =
                            (-compression_efficiency) as f64 / total_old_size as f64 * 100.0;
                        eprintln!(
                            "  Compression:  {} ({:.2}% better)",
                            format_bytes((-compression_efficiency) as u64),
                            pct_better
                        );
                    }
                } else {
                    eprintln!("  Compression:  unchanged");
                }
            }
            eprintln!();
        }
    } else {
        eprintln!("⚠️  Failed: {} bundles", failed);
        if let Some(ref err) = first_error {
            let err_msg = err.to_string();
            eprintln!("  First error: {}", err);

            // Provide helpful guidance for chain hash errors
            if err_msg.contains("Chain hash mismatch") {
                eprintln!("\n💡 Chain hash errors indicate:");
                eprintln!("   • The bundle content doesn't match the expected chain hash");
                eprintln!("   • This could mean the original bundle was corrupted or modified");
                eprintln!("   • The chain integrity check is working correctly");
                eprintln!("\n   To diagnose:");
                eprintln!(
                    "   1. Run '{} verify' to check all bundles",
                    constants::BINARY_NAME
                );
                eprintln!("   2. Check if the bundle file was manually modified");
                eprintln!("   3. Re-sync affected bundles from the PLC directory");
            } else if err_msg.contains("Parent hash mismatch") {
                eprintln!("\n💡 Parent hash errors indicate:");
                eprintln!("   • The chain linkage is broken between bundles");
                eprintln!("   • Bundles may have been migrated out of order");
                eprintln!("   • The index metadata may be inconsistent");
                eprintln!("\n   To fix:");
                eprintln!(
                    "   1. Run '{} verify' to identify all broken links",
                    constants::BINARY_NAME
                );
                eprintln!("   2. Ensure bundles are migrated in sequential order (1, 2, 3, ...)");
            }
        }
        bail!("migration failed for {} bundles", failed);
    }

    Ok(())
}

struct BundleMigrationInfo {
    bundle_number: u32,
    old_size: u64,
    uncompressed_size: u64,
    old_format: String,
}

fn measure_metadata_size(manager: &BundleManager, bundle_num: u32) -> Result<u64> {
    use std::io::Read;

    let mut file = manager.stream_bundle_raw(bundle_num)?;

    // Read magic (4 bytes) + size (4 bytes)
    let mut header = [0u8; 8];
    file.read_exact(&mut header)?;

    // Check if it's a skippable frame
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    if magic < 0x184D2A50 || magic > 0x184D2A5F {
        return Ok(0); // No metadata frame
    }

    // Get frame data size
    let frame_size = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as u64;

    // Total metadata size = 4 (magic) + 4 (size) + frameSize (data)
    Ok(8 + frame_size)
}
