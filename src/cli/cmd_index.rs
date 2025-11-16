// DID Index CLI commands
use super::utils;
use anyhow::Result;
use clap::{Args, Subcommand};
use plcbundle::{BundleManager, constants};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Args)]
#[command(
    about = "DID index management",
    long_about = "Manage the DID position index which maps DIDs to their bundle locations.\nThis index enables fast O(1) DID lookups and is required for DID\nresolution and query operations.",
    after_help = "Examples:\n  \
            # Build DID position index\n  \
            plcbundle index build\n\n  \
            # Repair DID index (rebuild from bundles)\n  \
            plcbundle index repair\n\n  \
            # Show DID index statistics\n  \
            plcbundle index stats\n\n  \
            # Verify DID index integrity\n  \
            plcbundle index verify"
)]
pub struct IndexCommand {
    #[command(subcommand)]
    pub command: IndexCommands,
}

#[derive(Subcommand)]
pub enum IndexCommands {
    /// Build DID position index
    #[command(after_help = "Examples:\n  \
            # Build index\n  \
            plcbundle index build\n\n  \
            # Force rebuild from scratch\n  \
            plcbundle index build --force\n\n  \
            # Use 8 parallel threads\n  \
            plcbundle index build -j 8")]
    Build {
        /// Rebuild even if index exists
        #[arg(short, long)]
        force: bool,

        /// Number of threads to use (0 = auto-detect)
        #[arg(short = 'j', long, default_value = "0")]
        threads: usize,
    },

    /// Repair DID index (incremental update + compaction)
    #[command(
        alias = "rebuild",
        after_help = "Intelligently repairs the DID index by:\n  \
            - Incrementally updating missing bundles (if < 1000 behind)\n  \
            - Performing full rebuild (if > 1000 bundles behind)\n  \
            - Compacting delta segments (if > 50 segments)\n\n  \
            Use this after syncing new bundles or if index is corrupted.\n\n  \
            Examples:\n  \
            plcbundle index repair\n  \
            plcbundle index repair -j 8"
    )]
    Repair {
        /// Number of threads to use (0 = auto-detect)
        #[arg(short = 'j', long, default_value = "0")]
        threads: usize,
    },

    /// Show DID index statistics
    #[command(alias = "info")]
    Stats {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Verify DID index integrity
    #[command(alias = "check")]
    Verify {},

    /// Debug and inspect DID index internals
    #[command(alias = "inspect")]
    Debug {
        /// Show specific shard (0-255 or hex like 0xac)
        #[arg(short, long)]
        shard: Option<String>,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Compact delta segments in DID index
    #[command(after_help = "Examples:\n  \
            # Compact all shards\n  \
            plcbundle index compact\n\n  \
            # Compact specific shards\n  \
            plcbundle index compact --shards 0xac 0x12 0xff")]
    Compact {
        /// Specific shards to compact (0-255 or hex like 0xac)
        #[arg(short, long, value_delimiter = ' ')]
        shards: Option<Vec<String>>,
    },
}

pub fn run(cmd: IndexCommand, dir: PathBuf, global_verbose: bool) -> Result<()> {
    match cmd.command {
        IndexCommands::Build { force, threads } => {
            cmd_index_build(dir, force, threads)?;
        }
        IndexCommands::Repair { threads } => {
            cmd_index_repair(dir, threads)?;
        }
        IndexCommands::Stats { json } => {
            cmd_index_stats(dir, json)?;
        }
        IndexCommands::Verify {} => {
            cmd_index_verify(dir, global_verbose)?;
        }
        IndexCommands::Debug { shard, json } => {
            let shard_num = shard.map(|s| parse_shard(&s)).transpose()?;
            cmd_index_debug(dir, shard_num, json)?;
        }
        IndexCommands::Compact { shards } => {
            let shard_nums = shards
                .map(|shard_list| {
                    shard_list
                        .iter()
                        .map(|s| parse_shard(s))
                        .collect::<Result<Vec<u8>>>()
                })
                .transpose()?;
            cmd_index_compact(dir, shard_nums)?;
        }
    }
    Ok(())
}

/// Parse shard number from string (supports hex 0xac or decimal)
fn parse_shard(s: &str) -> Result<u8> {
    if s.starts_with("0x") || s.starts_with("0X") {
        u8::from_str_radix(&s[2..], 16)
            .map_err(|_| anyhow::anyhow!("Invalid shard number: {}", s))
    } else {
        s.parse::<u8>()
            .map_err(|_| anyhow::anyhow!("Invalid shard number: {}", s))
    }
}

pub fn cmd_index_build(dir: PathBuf, force: bool, threads: usize) -> Result<()> {
    // Set thread pool size
    let num_threads = super::utils::get_worker_threads(threads, 4);
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .ok(); // Ignore error if already initialized

    let manager = super::utils::create_manager(dir.clone(), false, false)?;

    // Check if index exists
    let did_index = manager.get_did_index_stats();
    let total_dids = did_index
        .get("total_dids")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    if total_dids > 0 && !force {
        log::info!("DID index already exists (use --force to rebuild)");
        log::info!("Total DIDs: {}", total_dids);
        return Ok(());
    }

    if super::utils::is_repository_empty(&manager) {
        log::info!("No bundles to index");
        return Ok(());
    }

    // Get total uncompressed size for progress tracking
    let index = manager.get_index();
    let bundle_numbers: Vec<u32> = (1..=manager.get_last_bundle()).collect();
    let total_bytes = index.total_uncompressed_size_for_bundles(&bundle_numbers);

    // Create progress bar with byte tracking
    use super::progress::ProgressBar;
    let progress = ProgressBar::with_bytes(manager.get_last_bundle() as usize, total_bytes);

    manager.build_did_index(Some(|current, _total, bytes_processed, _total_bytes| {
        progress.set_with_bytes(current as usize, bytes_processed);
    }))?;

    progress.finish();

    // Show final info
    let stats = manager.get_did_index_stats();
    let total_dids = stats
        .get("total_dids")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    eprintln!("   Total DIDs: {}", utils::format_number(total_dids as u64));
    eprintln!("   Location:   {}/{}/", utils::display_path(&dir).display(), constants::DID_INDEX_DIR);

    Ok(())
}

pub fn cmd_index_repair(dir: PathBuf, threads: usize) -> Result<()> {
    // Set thread pool size
    let num_threads = super::utils::get_worker_threads(threads, 4);
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .ok(); // Ignore error if already initialized

    let manager = super::utils::create_manager(dir.clone(), false, false)?;

    // Check if index config exists (even if corrupted)
    let stats_map = manager.get_did_index_stats();
    let index_exists = stats_map
        .get("exists")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if !index_exists {
        log::error!("DID index does not exist");
        log::info!("Use: {} index build", constants::BINARY_NAME);
        return Ok(());
    }

    // Check if there are bundles to index
    if super::utils::is_repository_empty(&manager) {
        log::info!("No bundles to index");
        return Ok(());
    }

    log::info!("Analyzing DID index for repair...\n");

    let last_bundle = manager.get_last_bundle();
    let index_last_bundle = stats_map
        .get("last_bundle")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as u32;
    let delta_segments = stats_map
        .get("delta_segments")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let needs_rebuild = index_last_bundle < last_bundle;
    let needs_compact = delta_segments > 50; // Threshold for compaction

    if !needs_rebuild && !needs_compact {
        log::info!("✓ Index is up-to-date and optimized");
        log::info!("  Last bundle: {:06}", index_last_bundle);
        log::info!("  Delta segments: {}", delta_segments);
        return Ok(());
    }

    let start = Instant::now();

    // Case 1: Index is behind - do incremental update
    if needs_rebuild {
        let missing_bundles = last_bundle - index_last_bundle;
        log::info!("Index is behind by {} bundles ({:06} → {:06})",
            missing_bundles, index_last_bundle + 1, last_bundle);
        if num_threads > 1 {
            log::info!("Using {} threads", num_threads);
        }

        // If missing too many bundles, full rebuild is faster
        if missing_bundles > 1000 {
            log::info!("Too many missing bundles - performing full rebuild...\n");

            // Get total uncompressed size for progress tracking
            let index = manager.get_index();
            let bundle_numbers: Vec<u32> = (1..=last_bundle).collect();
            let total_bytes = index.total_uncompressed_size_for_bundles(&bundle_numbers);

            use super::progress::ProgressBar;
            let progress = ProgressBar::with_bytes(last_bundle as usize, total_bytes);

            manager.build_did_index(Some(|current, _total, bytes_processed, _total_bytes| {
                progress.set_with_bytes(current as usize, bytes_processed);
            }))?;

            progress.finish();
        } else {
            // Incremental update is faster
            log::info!("Performing incremental update...\n");

            // Get total uncompressed size for missing bundles
            let index = manager.get_index();
            let bundle_numbers: Vec<u32> = ((index_last_bundle + 1)..=last_bundle).collect();
            let total_bytes = index.total_uncompressed_size_for_bundles(&bundle_numbers);

            use super::progress::ProgressBar;
            let progress = ProgressBar::with_bytes(missing_bundles as usize, total_bytes);

            let did_index = manager.get_did_index();
            let mut bytes_processed = 0u64;

            for (i, bundle_num) in ((index_last_bundle + 1)..=last_bundle).enumerate() {
                // Load bundle
                let result = manager.load_bundle(bundle_num, crate::LoadOptions::default())?;

                // Extract operations
                let operations: Vec<(String, bool)> = result
                    .operations
                    .iter()
                    .map(|op| (op.did.clone(), op.nullified))
                    .collect();

                // Update index
                let mut index_guard = did_index.write().unwrap();
                if let Some(ref mut idx) = *index_guard {
                    idx.update_for_bundle(bundle_num, operations)?;
                }
                drop(index_guard);

                // Track bytes
                let index = manager.get_index();
                if let Some(meta) = index.get_bundle(bundle_num) {
                    bytes_processed += meta.uncompressed_size;
                }

                progress.set_with_bytes(i + 1, bytes_processed);
            }

            progress.finish();
        }
    }

    // Case 2: Compact delta segments if needed
    if needs_compact {
        log::info!("\nCompacting {} delta segments...", delta_segments);
        let compact_start = Instant::now();
        let did_index = manager.get_did_index();
        did_index
            .write()
            .unwrap()
            .as_mut()
            .unwrap()
            .compact_pending_segments(None)?;
        let compact_elapsed = compact_start.elapsed();
        log::info!("✓ Compacted in {:?}", compact_elapsed);
    }

    let elapsed = start.elapsed();
    let stats = manager.get_did_index_stats();
    let final_delta_segments = stats
        .get("delta_segments")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    log::info!("\n✓ DID index repaired in {:?}", elapsed);
    let total_dids = stats
        .get("total_dids")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    log::info!("  Total DIDs: {}", total_dids);
    log::info!("  Last bundle: {:06}", last_bundle);
    log::info!("  Delta segments: {}", final_delta_segments);

    Ok(())
}

pub fn cmd_index_stats(dir: PathBuf, json: bool) -> Result<()> {
    let manager = super::utils::create_manager(dir.clone(), false, false)?;

    // Get raw stats from did_index
    let stats_map = manager.get_did_index_stats();

    if json {
        let json_str = serde_json::to_string_pretty(&stats_map)?;
        println!("{}", json_str);
        return Ok(());
    }

    // Check if index exists
    if !stats_map
        .get("exists")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        log::error!("DID index does not exist");
        log::info!("Run: {} index build", constants::BINARY_NAME);
        return Ok(());
    }

    let total_dids = stats_map
        .get("total_dids")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let shard_count = stats_map
        .get("shard_count")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let shards_with_data = stats_map
        .get("shards_with_data")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let shards_with_segments = stats_map
        .get("shards_with_segments")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let max_segments_per_shard = stats_map
        .get("max_segments_per_shard")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let last_bundle = stats_map
        .get("last_bundle")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let cached_shards = stats_map
        .get("cached_shards")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let cache_limit = stats_map
        .get("cache_limit")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let cache_hit_rate = stats_map
        .get("cache_hit_rate")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let total_lookups = stats_map
        .get("total_lookups")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let delta_segments = stats_map
        .get("delta_segments")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let total_shard_size = stats_map
        .get("total_shard_size_bytes")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let total_delta_size = stats_map
        .get("total_delta_size_bytes")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let compaction_strategy = stats_map
        .get("compaction_strategy")
        .and_then(|v| v.as_str())
        .unwrap_or("manual");

    println!("\nDID Index Statistics");
    println!("════════════════════\n");
    println!(
        "  Location:      {}/{}/",
        utils::display_path(&dir).display(),
        constants::DID_INDEX_DIR
    );
    println!(
        "  Total DIDs:    {}",
        utils::format_number(total_dids as u64)
    );
    println!(
        "  Shard count:   {} ({} with data, {} with segments)",
        shard_count, shards_with_data, shards_with_segments
    );
    println!("  Last bundle:   {:06}", last_bundle);
    println!();
    println!("  Storage:");
    println!(
        "    Base shards:  {} ({})",
        shards_with_data,
        utils::format_bytes(total_shard_size)
    );
    println!(
        "    Delta segs:   {} ({})",
        delta_segments,
        utils::format_bytes(total_delta_size)
    );
    println!(
        "    Total size:   {}",
        utils::format_bytes(total_shard_size + total_delta_size)
    );
    if max_segments_per_shard > 0 {
        println!("    Max seg/shard: {}", max_segments_per_shard);
    }
    println!("    Strategy:     {}", compaction_strategy);
    println!();
    println!("  Cache:");
    println!("    Cached:       {} / {}", cached_shards, cache_limit);
    if total_lookups > 0 {
        println!("    Hit rate:      {:.1}%", cache_hit_rate * 100.0);
        println!(
            "    Total lookups: {}",
            utils::format_number(total_lookups as u64)
        );
    }

    println!();

    Ok(())
}

#[derive(Debug)]
struct RebuildResult {
    bundles_scanned: u32,
    dids_checked: usize,
    errors: usize,
    missing_in_index: usize,
    extra_in_index: usize,
    location_mismatches: usize,
}

fn rebuild_and_compare_index(
    manager: &BundleManager,
    last_bundle: u32,
    verbose: bool,
) -> Result<RebuildResult> {
    use super::progress::ProgressBar;
    use std::fs;

    // Create temporary directory for rebuilt index
    let temp_dir = std::env::temp_dir().join(format!("plcbundle_verify_{}", std::process::id()));
    fs::create_dir_all(&temp_dir)?;

    // Create subdirectory for bundle data files
    let bundle_data_dir = temp_dir.join("bundle_data");
    fs::create_dir_all(&bundle_data_dir)?;

    // Create temporary DID index
    let temp_did_index = crate::did_index::Manager::new(temp_dir.clone())?;

    // Build index using streaming approach (no need to load bundles into memory!)
    log::info!("Building temporary index using streaming approach...");

    // Calculate total uncompressed size for progress tracking
    let index = manager.get_index();
    let bundle_numbers: Vec<u32> = (1..=last_bundle).collect();
    let total_uncompressed_size = index.total_uncompressed_size_for_bundles(&bundle_numbers);

    let progress = ProgressBar::with_bytes(last_bundle as usize, total_uncompressed_size);

    // Stream bundles directly from disk - memory efficient!
    temp_did_index.build_from_scratch(
        manager.directory(),
        last_bundle,
        Some(|current, _total, bytes_processed| {
            progress.set_with_bytes(current as usize, bytes_processed);
        })
    )?;

    progress.finish();

    // Compare shard files between temporary and existing index
    let existing_index_dir = manager.directory().join(crate::constants::DID_INDEX_DIR);
    let existing_shards = existing_index_dir.join("shards");
    let temp_shards = temp_dir.join("shards");

    // Get stats from both indexes
    let temp_stats = temp_did_index.get_stats();
    let temp_total_dids = temp_stats
        .get("total_dids")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;

    let existing_stats = manager.get_did_index_stats();
    let existing_total_dids = existing_stats
        .get("total_dids")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;
    let existing_delta_segments = existing_stats
        .get("delta_segments")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let mut errors = 0;
    let mut mismatched_shards = 0;

    // Compare DID counts first (ultimate sanity check)
    if temp_total_dids != existing_total_dids {
        errors += 1;
        log::error!(
            "  Total DID count mismatch: existing={}, rebuilt={}",
            existing_total_dids,
            temp_total_dids
        );
    } else if verbose {
        log::info!("  ✓ Total DID count matches: {}", temp_total_dids);
    }

    // Compare shard files
    // Note: If existing index has delta segments, base shards won't match exactly
    // because build_from_scratch creates a clean index with all data in base shards
    if existing_delta_segments > 0 {
        if verbose {
            log::info!(
                "  Note: Existing index has {} delta segments, skipping file-by-file comparison",
                existing_delta_segments
            );
            log::info!("  (Base shards won't match because existing index needs compaction)");
        }
    } else {
        log::info!("Comparing shard files...");
        for shard_num in 0..=255u8 {
            let existing_shard = existing_shards.join(format!("{:02x}.idx", shard_num));
            let temp_shard = temp_shards.join(format!("{:02x}.idx", shard_num));

            // Check if both exist or both don't exist
            let existing_exists = existing_shard.exists();
            let temp_exists = temp_shard.exists();

            if existing_exists != temp_exists {
                errors += 1;
                mismatched_shards += 1;
                if verbose {
                    log::error!(
                        "  Shard {:02x}: existence mismatch (existing={}, rebuilt={})",
                        shard_num,
                        existing_exists,
                        temp_exists
                    );
                }
                continue;
            }

            if !existing_exists {
                // Both don't exist, skip
                continue;
            }

            // Compare file contents
            let existing_data = fs::read(&existing_shard)?;
            let temp_data = fs::read(&temp_shard)?;

            if existing_data != temp_data {
                errors += 1;
                mismatched_shards += 1;
                if verbose {
                    log::error!(
                        "  Shard {:02x}: content mismatch (existing={} bytes, rebuilt={} bytes)",
                        shard_num,
                        existing_data.len(),
                        temp_data.len()
                    );
                }
            } else if verbose {
                log::info!(
                    "  ✓ Shard {:02x}: matches ({} bytes)",
                    shard_num,
                    existing_data.len()
                );
            }
        }
    }

    // Clean up temporary directory
    fs::remove_dir_all(&temp_dir).ok();

    Ok(RebuildResult {
        bundles_scanned: last_bundle,
        dids_checked: temp_total_dids,
        errors,
        missing_in_index: 0,
        extra_in_index: 0,
        location_mismatches: mismatched_shards,
    })
}

pub fn cmd_index_verify(dir: PathBuf, verbose: bool) -> Result<()> {
    let manager = super::utils::create_manager(dir.clone(), false, false)?;

    let stats_map = manager.get_did_index_stats();

    if !stats_map
        .get("exists")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        log::error!("DID index does not exist");
        log::info!("Run: {} index build", constants::BINARY_NAME);
        return Ok(());
    }

    log::info!("Verifying DID index...\n");

    let total_dids = stats_map
        .get("total_dids")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let last_bundle = stats_map
        .get("last_bundle")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let mut errors = 0;
    let mut warnings = 0;

    // Check 1: Last bundle consistency
    log::info!("Checking bundle consistency...");
    let manager_last = manager.get_last_bundle();

    if last_bundle < manager_last as i64 {
        log::warn!(
            "  ⚠️  Index is behind (has bundle {}, repo has {})",
            last_bundle,
            manager_last
        );
        log::info!("      Run: {} index repair", constants::BINARY_NAME);
        warnings += 1;
    } else if verbose {
        log::info!("  ✓ Last bundle matches: {:06}", last_bundle);
    }

    // Check 2: Verify shard files exist and are readable
    log::info!("Checking shard files...");
    let did_index = manager.get_did_index();
    let shard_details = did_index.read().unwrap().as_ref().unwrap().get_shard_details(None)?;

    let mut missing_base_shards = 0;
    let mut missing_delta_segments = 0;
    let mut shards_checked = 0;
    let mut segments_checked = 0;

    for detail in &shard_details {
        let shard_hex = detail
            .get("shard_hex")
            .and_then(|v| v.as_str())
            .unwrap_or("??");

        let base_exists = detail
            .get("base_exists")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let did_count = detail
            .get("did_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        // Only check shards that should have data
        if did_count > 0 {
            shards_checked += 1;

            if !base_exists {
                if verbose {
                    log::error!("  ✗ Missing base shard: {}", shard_hex);
                }
                missing_base_shards += 1;
                errors += 1;
            } else if verbose {
                log::info!(
                    "  ✓ Shard {}: {} DIDs",
                    shard_hex,
                    utils::format_number(did_count)
                );
            }
        }

        // Check delta segments
        if let Some(segments) = detail.get("segments").and_then(|v| v.as_array()) {
            for seg in segments {
                let exists = seg.get("exists").and_then(|v| v.as_bool()).unwrap_or(false);
                let file_name = seg.get("file_name").and_then(|v| v.as_str()).unwrap_or("?");

                segments_checked += 1;

                if !exists {
                    if verbose {
                        log::error!(
                            "  ✗ Missing delta segment: {} (shard {})",
                            file_name,
                            shard_hex
                        );
                    }
                    missing_delta_segments += 1;
                    errors += 1;
                }
            }
        }
    }

    if !verbose {
        if missing_base_shards > 0 {
            log::error!("  ✗ Missing {} base shard(s)", missing_base_shards);
        }
        if missing_delta_segments > 0 {
            log::error!("  ✗ Missing {} delta segment(s)", missing_delta_segments);
        }
        if missing_base_shards == 0 && missing_delta_segments == 0 {
            log::info!(
                "  ✓ All shard files exist ({} shards, {} segments)",
                shards_checked,
                segments_checked
            );
        }
    }

    // Check 3: Verify index configuration
    log::info!("Checking index configuration...");
    let shard_count = stats_map
        .get("shard_count")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    if shard_count != 256 {
        log::warn!(
            "  ⚠️  Unexpected shard count: {} (expected 256)",
            shard_count
        );
        warnings += 1;
    } else if verbose {
        log::info!("  ✓ Shard count: {}", shard_count);
    }

    // Check 4: Check delta segment accumulation
    log::info!("Checking delta segments...");
    let delta_segments = stats_map
        .get("delta_segments")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let max_segments_per_shard = stats_map
        .get("max_segments_per_shard")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let compaction_strategy = stats_map
        .get("compaction_strategy")
        .and_then(|v| v.as_str())
        .unwrap_or("manual");

    // Warn if too many delta segments (potential performance issue)
    const SEGMENT_WARNING_THRESHOLD: u64 = 50;
    const SEGMENT_ERROR_THRESHOLD: u64 = 100;

    if delta_segments >= SEGMENT_ERROR_THRESHOLD {
        log::error!(
            "  ✗ Too many delta segments: {} (performance will degrade)",
            delta_segments
        );
        log::info!("      Run: {} index compact", constants::BINARY_NAME);
        errors += 1;
    } else if delta_segments >= SEGMENT_WARNING_THRESHOLD {
        log::warn!(
            "  ⚠️  Many delta segments: {} (consider compacting)",
            delta_segments
        );
        log::info!("      Run: {} index compact", constants::BINARY_NAME);
        warnings += 1;
    } else if verbose {
        log::info!(
            "  ✓ Delta segments: {} (max per shard: {})",
            delta_segments,
            max_segments_per_shard
        );
    }

    if verbose && compaction_strategy != "manual" {
        log::info!("  ✓ Compaction strategy: {}", compaction_strategy);
    }

    // Check 5: Rebuild index in memory and compare
    log::info!("Rebuilding index in memory for verification...");
    let rebuild_result = rebuild_and_compare_index(&manager, last_bundle as u32, verbose)?;

    if rebuild_result.errors > 0 {
        errors += rebuild_result.errors;
        log::error!(
            "  ✗ Index rebuild comparison failed with {} errors",
            rebuild_result.errors
        );
        if rebuild_result.missing_in_index > 0 {
            log::error!(
                "    - {} DIDs missing from existing index",
                rebuild_result.missing_in_index
            );
        }
        if rebuild_result.extra_in_index > 0 {
            log::error!(
                "    - {} DIDs in index but not in bundles",
                rebuild_result.extra_in_index
            );
        }
        if rebuild_result.location_mismatches > 0 {
            log::error!(
                "    - {} DIDs with location mismatches",
                rebuild_result.location_mismatches
            );
        }
    } else if verbose {
        log::info!("  ✓ Index rebuild verification passed");
        log::info!("    - {} DIDs verified", rebuild_result.dids_checked);
        log::info!("    - {} bundles scanned", rebuild_result.bundles_scanned);
    }

    // Summary
    println!();
    if errors > 0 {
        log::error!("✗ Index verification failed");
        log::error!("  Errors:   {}", errors);
        log::error!("  Warnings: {}", warnings);
        log::info!("\n  Run: {} index repair", constants::BINARY_NAME);
        std::process::exit(1);
    } else if warnings > 0 {
        log::warn!("⚠️  Index verification passed with warnings");
        log::info!("  Warnings: {}", warnings);
        log::info!("  Total DIDs:  {}", total_dids);
        log::info!("  Last bundle: {:06}", last_bundle);
    } else {
        log::info!("✓ DID index is valid");
        log::info!("  Total DIDs:    {}", total_dids);
        log::info!("  Last bundle:   {:06}", last_bundle);
        log::info!("  Shards:        {}", shards_checked);
        log::info!("  Delta segments: {}", delta_segments);
        if delta_segments > 0 && delta_segments < SEGMENT_WARNING_THRESHOLD {
            log::info!("  (compaction not needed)");
        }
    }

    Ok(())
}

pub fn cmd_index_debug(dir: PathBuf, shard: Option<u8>, json: bool) -> Result<()> {
    let manager = super::utils::create_manager(dir.clone(), false, false)?;

    let stats_map = manager.get_did_index_stats();

    if !stats_map
        .get("exists")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        log::error!("DID index does not exist");
        log::info!("Run: {} index build", constants::BINARY_NAME);
        return Ok(());
    }

    let did_index = manager.get_did_index();
    let shard_details = did_index.read().unwrap().as_ref().unwrap().get_shard_details(shard)?;

    if json {
        let json_str = serde_json::to_string_pretty(&shard_details)?;
        println!("{}", json_str);
        return Ok(());
    }

    if let Some(shard_num) = shard {
        // Show single shard details
        if let Some(detail) = shard_details.first() {
            println!("\nShard {:02x} Details", shard_num);
            println!("═══════════════════════════════════════\n");

            let did_count = detail
                .get("did_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let segment_count = detail
                .get("segment_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let base_exists = detail
                .get("base_exists")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let base_size = detail
                .get("base_size_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let total_segment_size = detail
                .get("total_segment_size_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let total_size = detail
                .get("total_size_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let next_segment_id = detail
                .get("next_segment_id")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            println!("  DIDs:              {}", utils::format_number(did_count));
            println!(
                "  Base shard:        {} ({})",
                if base_exists { "exists" } else { "missing" },
                utils::format_bytes(base_size)
            );
            println!("  Delta segments:    {}", segment_count);
            println!(
                "  Segment size:      {}",
                utils::format_bytes(total_segment_size)
            );
            println!("  Total size:        {}", utils::format_bytes(total_size));
            println!("  Next segment ID:   {}", next_segment_id);

            if let Some(segments) = detail.get("segments").and_then(|v| v.as_array()) {
                if !segments.is_empty() {
                    println!("\n  Delta Segments:");
                    println!("  ───────────────────────────────────────");
                    for (idx, seg) in segments.iter().enumerate() {
                        let file_name =
                            seg.get("file_name").and_then(|v| v.as_str()).unwrap_or("?");
                        let exists = seg.get("exists").and_then(|v| v.as_bool()).unwrap_or(false);
                        let size = seg.get("size_bytes").and_then(|v| v.as_u64()).unwrap_or(0);
                        let did_count = seg.get("did_count").and_then(|v| v.as_u64()).unwrap_or(0);
                        let bundle_start = seg
                            .get("bundle_start")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        let bundle_end =
                            seg.get("bundle_end").and_then(|v| v.as_u64()).unwrap_or(0);

                        println!(
                            "    [{:2}] {} {} ({})",
                            idx + 1,
                            if exists { "✓" } else { "✗" },
                            file_name,
                            utils::format_bytes(size)
                        );
                        println!(
                            "         Bundles: {:06}-{:06}, DIDs: {}, Locations: {}",
                            bundle_start,
                            bundle_end,
                            utils::format_number(did_count),
                            seg.get("location_count")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0)
                        );
                    }
                }
            }

            println!();
        }
    } else {
        // Show summary of all shards
        println!("\nShard Summary");
        println!("═══════════════════════════════════════\n");

        let mut shards_with_data = 0;
        let mut shards_with_segments = 0;
        let mut total_dids = 0u64;
        let mut total_base_size = 0u64;
        let mut total_segment_size = 0u64;
        let mut max_segments = 0;

        for detail in &shard_details {
            let did_count = detail
                .get("did_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let segment_count = detail
                .get("segment_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let base_size = detail
                .get("base_size_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let seg_size = detail
                .get("total_segment_size_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            if did_count > 0 {
                shards_with_data += 1;
            }
            if segment_count > 0 {
                shards_with_segments += 1;
            }
            total_dids += did_count;
            total_base_size += base_size;
            total_segment_size += seg_size;
            max_segments = max_segments.max(segment_count);
        }

        println!(
            "  Shards with data:    {} / {}",
            shards_with_data,
            shard_details.len()
        );
        println!(
            "  Shards with segments: {} / {}",
            shards_with_segments,
            shard_details.len()
        );
        println!(
            "  Total DIDs:          {}",
            utils::format_number(total_dids)
        );
        println!(
            "  Base shard size:     {}",
            utils::format_bytes(total_base_size)
        );
        println!(
            "  Delta segment size:  {}",
            utils::format_bytes(total_segment_size)
        );
        println!(
            "  Total size:          {}",
            utils::format_bytes(total_base_size + total_segment_size)
        );
        println!("  Max segments/shard:   {}", max_segments);
        println!();

        // Show top 10 shards by DID count
        let mut sorted_shards: Vec<_> = shard_details
            .iter()
            .filter(|d| d.get("did_count").and_then(|v| v.as_u64()).unwrap_or(0) > 0)
            .collect();
        sorted_shards.sort_by_key(|d| {
            std::cmp::Reverse(d.get("did_count").and_then(|v| v.as_u64()).unwrap_or(0))
        });

        if !sorted_shards.is_empty() {
            println!("  Top 10 Shards by DID Count:");
            println!("  ───────────────────────────────────────");
            for (idx, detail) in sorted_shards.iter().take(10).enumerate() {
                let shard_hex = detail
                    .get("shard_hex")
                    .and_then(|v| v.as_str())
                    .unwrap_or("??");
                let did_count = detail
                    .get("did_count")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let segment_count = detail
                    .get("segment_count")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let total_size = detail
                    .get("total_size_bytes")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                println!(
                    "    [{:2}] Shard {:2}: {} DIDs, {} segments, {}",
                    idx + 1,
                    shard_hex,
                    utils::format_number(did_count),
                    segment_count,
                    utils::format_bytes(total_size)
                );
            }
            println!();
        }
    }

    Ok(())
}

pub fn cmd_index_compact(dir: PathBuf, shards: Option<Vec<u8>>) -> Result<()> {
    let manager = super::utils::create_manager(dir.clone(), false, false)?;

    let stats_map = manager.get_did_index_stats();

    if !stats_map
        .get("exists")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        log::error!("DID index does not exist");
        log::info!("Run: {} index build", constants::BINARY_NAME);
        return Ok(());
    }

    let delta_segments_before = stats_map
        .get("delta_segments")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    if delta_segments_before == 0 {
        log::info!("No pending delta segments to compact");
        return Ok(());
    }

    log::info!("Compacting delta segments...");
    if let Some(ref shard_list) = shards {
        log::info!("  Targeting {} specific shard(s)", shard_list.len());
    } else {
        log::info!("  Compacting all shards");
    }

    let did_index = manager.get_did_index();
    let start = Instant::now();
    did_index
        .write()
        .unwrap()
        .as_mut()
        .unwrap()
        .compact_pending_segments(shards)?;
    let elapsed = start.elapsed();

    let stats_map_after = manager.get_did_index_stats();
    let delta_segments_after = stats_map_after
        .get("delta_segments")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    log::info!("\n✓ Compaction complete in {:?}", elapsed);
    log::info!("  Delta segments before: {}", delta_segments_before);
    log::info!("  Delta segments after:  {}", delta_segments_after);
    log::info!(
        "  Segments compacted:    {}",
        delta_segments_before.saturating_sub(delta_segments_after)
    );

    Ok(())
}
