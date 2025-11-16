// DID Resolution and Query commands
use anyhow::Result;
use clap::{Args, Subcommand};
use plcbundle::{BundleManager, DIDLookupStats, DIDLookupTimings};
use std::path::PathBuf;

#[derive(Args)]
#[command(
    about = "DID operations and queries",
    long_about = "Query and analyze DIDs in the bundle repository. All commands\nrequire a DID index to be built for optimal performance.",
    after_help = "Examples:\n  \
            # Resolve DID to current document\n  \
            plcbundle did resolve did:plc:524tuhdhh3m7li5gycdn6boe\n\n  \
            # Show DID operation log\n  \
            plcbundle did log did:plc:524tuhdhh3m7li5gycdn6boe\n\n  \
            # Show complete audit log\n  \
            plcbundle did history did:plc:524tuhdhh3m7li5gycdn6boe"
)]
pub struct DidCommand {
    #[command(subcommand)]
    pub command: DIDCommands,
}

#[derive(Args)]
#[command(
    about = "Resolve handle to DID",
    long_about = "Resolves an AT Protocol handle to its DID using the handle resolver."
)]
pub struct HandleCommand {
    /// Handle to resolve (e.g., tree.fail)
    pub handle: String,

    /// Handle resolver URL (defaults to quickdid.smokesignal.tools)
    #[arg(long)]
    pub handle_resolver: Option<String>,
}

#[derive(Subcommand)]
pub enum DIDCommands {
    /// Resolve DID to current W3C DID document
    #[command(alias = "doc", alias = "document")]
    Resolve {
        /// DID or handle to resolve
        did: String,

        /// Handle resolver URL (e.g., https://quickdid.smokesignal.tools)
        #[arg(long)]
        handle_resolver: Option<String>,

        /// Verbose output
        #[arg(short, long)]
        verbose: bool,
    },

    /// Show DID operation log
    #[command(alias = "lookup", alias = "find", alias = "get", alias = "history")]
    Log {
        /// DID to show log for
        did: String,

        /// Verbose output
        #[arg(short, long)]
        verbose: bool,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Process multiple DIDs from file or stdin (TODO)
    Batch {
        /// Action: lookup, resolve, export
        #[arg(long, default_value = "lookup")]
        action: String,

        /// Number of parallel threads (0 = auto-detect)
        #[arg(short = 'j', long, default_value = "0")]
        threads: usize,

        /// Output file
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Read from stdin
        #[arg(long)]
        stdin: bool,
    },
}

pub fn run_did(cmd: DidCommand, dir: PathBuf) -> Result<()> {
    match cmd.command {
        DIDCommands::Resolve {
            did,
            handle_resolver,
            verbose,
        } => {
            cmd_did_resolve(dir, did, handle_resolver, verbose)?;
        }
        DIDCommands::Log { did, verbose, json } => {
            cmd_did_lookup(dir, did, verbose, json)?;
        }
        DIDCommands::Batch {
            action,
            threads,
            output,
            stdin,
        } => {
            cmd_did_batch(dir, action, threads, output, stdin)?;
        }
    }
    Ok(())
}

pub fn run_handle(cmd: HandleCommand, dir: PathBuf) -> Result<()> {
    cmd_did_handle(dir, cmd.handle, cmd.handle_resolver)?;
    Ok(())
}

// DID RESOLVE - Convert DID to W3C DID Document

pub fn cmd_did_resolve(
    dir: PathBuf,
    input: String,
    handle_resolver_url: Option<String>,
    verbose: bool,
) -> Result<()> {
    use plcbundle::constants;

    // Use default resolver if none provided
    let resolver_url =
        handle_resolver_url.or_else(|| Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string()));

    // Initialize manager with handle resolver (default or provided)
    let manager = BundleManager::with_handle_resolver(dir, resolver_url)?;

    // Resolve handle to DID if needed
    let (did, handle_resolve_time) = manager.resolve_handle_or_did(&input)?;

    if handle_resolve_time > 0 {
        log::info!(
            "Resolved handle: {} → {} (in {}ms)",
            input,
            did,
            handle_resolve_time
        );
    } else {
        log::info!("Resolving DID: {}", did);
    }

    // Get resolve result with stats
    let result = manager.resolve_did_with_stats(&did)?;

    // Get DID index for shard calculation (only for PLC DIDs)
    if did.starts_with("did:plc:") {
        let identifier = &did[8..]; // Strip "did:plc:" prefix
        let shard_num = calculate_shard_for_display(identifier);
        log::debug!(
            "DID {} -> identifier '{}' -> shard {:02x}",
            did,
            identifier,
            shard_num
        );
    }

    if let Some(stats) = &result.shard_stats {
        log::debug!(
            "Shard {:02x} loaded, size: {} bytes",
            result.shard_num,
            stats.shard_size
        );

        let reduction = if stats.total_entries > 0 {
            ((stats.total_entries - stats.prefix_narrowed_to) as f64 / stats.total_entries as f64)
                * 100.0
        } else {
            0.0
        };

        log::debug!(
            "Prefix index narrowed search: {} entries → {} entries ({:.1}% reduction)",
            stats.total_entries,
            stats.prefix_narrowed_to,
            reduction
        );
        log::debug!(
            "Binary search found {} locations after {} attempts",
            stats.locations_found,
            stats.binary_search_attempts
        );
    }

    if verbose {
        // Convert handle resolution time to Duration
        let handle_resolve_duration = std::time::Duration::from_millis(handle_resolve_time);

        if handle_resolve_time > 0 {
            log::info!("Handle resolution: {:?}", handle_resolve_duration);
        }

        // Show detailed index lookup timings if available
        if let Some(ref timings) = result.lookup_timings {
            log::info!("Index Lookup Breakdown:");
            log::info!("  Extract ID:    {:?}", timings.extract_identifier);
            log::info!("  Calc shard:    {:?}", timings.calculate_shard);
            log::info!(
                "  Load shard:    {:?} ({})",
                timings.load_shard,
                if timings.cache_hit {
                    "cache hit"
                } else {
                    "cache miss"
                }
            );

            // Search breakdown
            log::info!("  Search:");
            if let Some(ref base_time) = timings.base_search_time {
                log::info!("    Base shard:  {:?}", base_time);
            }
            if !timings.delta_segment_times.is_empty() {
                let total_delta_time: std::time::Duration = timings
                    .delta_segment_times
                    .iter()
                    .map(|(_, time)| *time)
                    .sum();
                log::info!(
                    "    Delta segs:  {:?} ({} segment{})",
                    total_delta_time,
                    timings.delta_segment_times.len(),
                    if timings.delta_segment_times.len() == 1 {
                        ""
                    } else {
                        "s"
                    }
                );

                // Show individual delta segments if there are multiple or if verbose
                if timings.delta_segment_times.len() > 1 || verbose {
                    for (seg_name, seg_time) in &timings.delta_segment_times {
                        log::info!("      - {}: {:?}", seg_name, seg_time);
                    }
                }
            }
            if timings.merge_time.as_nanos() > 0 {
                log::info!("    Merge/sort:  {:?}", timings.merge_time);
            }
            log::info!("    Search total: {:?}", timings.search);
            log::info!("  Index total:   {:?}", result.index_time);
        } else {
            log::info!("Index: {:?}", result.index_time);
        }

        log::info!("Load operation: {:?}", result.load_time);

        // Calculate true total including handle resolution
        let true_total = handle_resolve_duration + result.total_time;
        log::info!(
            "Total:          {:?} (handle: {:?} + did: {:?})",
            true_total,
            handle_resolve_duration,
            result.total_time
        );

        // Calculate global position: (bundle * BUNDLE_SIZE) + position
        let global_pos = (result.bundle_number as u64 * plcbundle::constants::BUNDLE_SIZE as u64)
            + result.position as u64;
        log::info!(
            "Source: bundle {:06}, position {} (global: {})\n",
            result.bundle_number,
            result.position,
            global_pos
        );
    }

    // Output document (always to stdout)
    let json = serde_json::to_string_pretty(&result.document)?;
    println!("{}", json);

    Ok(())
}

fn calculate_shard_for_display(identifier: &str) -> u8 {
    use fnv::FnvHasher;
    use std::hash::Hasher;

    let mut hasher = FnvHasher::default();
    hasher.write(identifier.as_bytes());
    let hash = hasher.finish() as u32;
    (hash % 256) as u8
}

// DID HANDLE - Resolve handle to DID

pub fn cmd_did_handle(
    dir: PathBuf,
    handle: String,
    handle_resolver_url: Option<String>,
) -> Result<()> {
    use plcbundle::constants;

    // Use default resolver if none provided
    let resolver_url =
        handle_resolver_url.or_else(|| Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string()));

    // Initialize manager with handle resolver (default or provided)
    let manager = BundleManager::with_handle_resolver(dir, resolver_url)?;

    // Resolve handle to DID
    let (did, resolve_time) = manager.resolve_handle_or_did(&handle)?;

    if resolve_time > 0 {
        println!("{}", did);
    } else {
        // If it was already a DID, just print it
        println!("{}", did);
    }

    Ok(())
}

// DID LOOKUP - Find all operations for a DID

pub fn cmd_did_lookup(dir: PathBuf, input: String, verbose: bool, json: bool) -> Result<()> {
    use plcbundle::constants;
    use std::time::Instant;

    // Use default resolver if none provided (same pattern as cmd_did_resolve)
    let resolver_url = Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string());

    // Initialize manager with handle resolver (default or provided)
    let manager = BundleManager::with_handle_resolver(dir, resolver_url)?;

    // Resolve handle to DID if needed
    let (did, handle_resolve_time) = manager.resolve_handle_or_did(&input)?;

    if handle_resolve_time > 0 {
        log::info!(
            "Resolved handle: {} → {} (in {}ms)",
            input,
            did,
            handle_resolve_time
        );
    } else {
        log::info!("Looking up DID: {}", did);
    }

    // Get DID index for shard calculation (only for PLC DIDs)
    if did.starts_with("did:plc:") {
        let identifier = &did[8..]; // Strip "did:plc:" prefix
        let shard_num = calculate_shard_for_display(identifier);
        log::debug!(
            "DID {} -> identifier '{}' -> shard {:02x}",
            did,
            identifier,
            shard_num
        );
    }

    let total_start = Instant::now();

    // Lookup operations with locations and stats (for verbose mode)
    let (ops_with_loc, shard_stats, shard_num, lookup_timings, load_time) = if verbose {
        manager.get_did_operations_with_locations_and_stats(&did)?
    } else {
        let lookup_start = Instant::now();
        let ops = manager.get_did_operations_with_locations(&did)?;
        let lookup_elapsed = lookup_start.elapsed();
        (
            ops,
            DIDLookupStats::default(),
            0,
            DIDLookupTimings::default(),
            lookup_elapsed,
        )
    };

    let index_time = lookup_timings.extract_identifier
        + lookup_timings.calculate_shard
        + lookup_timings.load_shard
        + lookup_timings.search;
    let lookup_elapsed = index_time + load_time;

    // Show shard stats if available
    if verbose && shard_stats.total_entries > 0 {
        log::debug!(
            "Shard {:02x} loaded, size: {} bytes",
            shard_num,
            shard_stats.shard_size
        );

        let reduction = if shard_stats.total_entries > 0 {
            ((shard_stats.total_entries - shard_stats.prefix_narrowed_to) as f64
                / shard_stats.total_entries as f64)
                * 100.0
        } else {
            0.0
        };

        log::debug!(
            "Prefix index narrowed search: {} entries → {} entries ({:.1}% reduction)",
            shard_stats.total_entries,
            shard_stats.prefix_narrowed_to,
            reduction
        );
        log::debug!(
            "Binary search found {} locations after {} attempts",
            shard_stats.locations_found,
            shard_stats.binary_search_attempts
        );
    }

    // Check mempool
    let mempool_start = Instant::now();
    let mempool_ops = manager.get_did_operations_from_mempool(&did)?;
    let mempool_elapsed = mempool_start.elapsed();

    let total_elapsed = total_start.elapsed();

    // Show verbose timing breakdown
    if verbose {
        // Convert handle resolution time to Duration
        let handle_resolve_duration = std::time::Duration::from_millis(handle_resolve_time);

        if handle_resolve_time > 0 {
            log::info!("Handle resolution: {:?}", handle_resolve_duration);
        }

        // Show detailed index lookup timings
        log::info!("Index Lookup Breakdown:");
        log::info!("  Extract ID:    {:?}", lookup_timings.extract_identifier);
        log::info!("  Calc shard:    {:?}", lookup_timings.calculate_shard);
        log::info!(
            "  Load shard:    {:?} ({})",
            lookup_timings.load_shard,
            if lookup_timings.cache_hit {
                "cache hit"
            } else {
                "cache miss"
            }
        );

        // Search breakdown
        log::info!("  Search:");
        if let Some(ref base_time) = lookup_timings.base_search_time {
            log::info!("    Base shard:  {:?}", base_time);
        }
        if !lookup_timings.delta_segment_times.is_empty() {
            let total_delta_time: std::time::Duration = lookup_timings
                .delta_segment_times
                .iter()
                .map(|(_, time)| *time)
                .sum();
            log::info!(
                "    Delta segs:  {:?} ({} segment{})",
                total_delta_time,
                lookup_timings.delta_segment_times.len(),
                if lookup_timings.delta_segment_times.len() == 1 {
                    ""
                } else {
                    "s"
                }
            );

            // Show individual delta segments if there are multiple
            if lookup_timings.delta_segment_times.len() > 1 {
                for (seg_name, seg_time) in &lookup_timings.delta_segment_times {
                    log::info!("      - {}: {:?}", seg_name, seg_time);
                }
            }
        }
        if lookup_timings.merge_time.as_nanos() > 0 {
            log::info!("    Merge/sort:  {:?}", lookup_timings.merge_time);
        }
        log::info!("    Search total: {:?}", lookup_timings.search);
        log::info!("  Index total:   {:?}", index_time);
        log::info!(
            "  Load operations: {:?} ({} operations)",
            load_time,
            ops_with_loc.len()
        );
        log::info!("  Mempool check: {:?}", mempool_elapsed);

        // Calculate true total including handle resolution
        let true_total = handle_resolve_duration + total_elapsed;
        log::info!(
            "Total:          {:?} (handle: {:?} + lookup: {:?})",
            true_total,
            handle_resolve_duration,
            total_elapsed
        );
        log::info!("");
    }

    if ops_with_loc.is_empty() && mempool_ops.is_empty() {
        if json {
            println!("{{\"found\": false, \"operations\": []}}");
        } else {
            println!("DID not found (searched in {:?})", total_elapsed);
        }
        return Ok(());
    }

    if json {
        return output_lookup_json(
            &did,
            &ops_with_loc,
            &mempool_ops,
            total_elapsed,
            lookup_elapsed,
            mempool_elapsed,
        );
    }

    display_lookup_results(
        &did,
        &ops_with_loc,
        &mempool_ops,
        total_elapsed,
        lookup_elapsed,
        mempool_elapsed,
        verbose,
    )
}

fn output_lookup_json(
    did: &str,
    ops_with_loc: &[plcbundle::OperationWithLocation],
    mempool_ops: &[plcbundle::Operation],
    total_elapsed: std::time::Duration,
    lookup_elapsed: std::time::Duration,
    mempool_elapsed: std::time::Duration,
) -> Result<()> {
    use serde_json::json;

    let mut bundled = Vec::new();
    for owl in ops_with_loc {
        bundled.push(json!({
            "bundle": owl.bundle,
            "position": owl.position,
            "cid": owl.operation.cid,
            "nullified": owl.nullified,
            "created_at": owl.operation.created_at,
        }));
    }

    let mut mempool = Vec::new();
    for op in mempool_ops {
        mempool.push(json!({
            "cid": op.cid,
            "nullified": op.nullified,
            "created_at": op.created_at,
        }));
    }

    let output = json!({
        "found": true,
        "did": did,
        "timing": {
            "total_ms": total_elapsed.as_millis(),
            "lookup_ms": lookup_elapsed.as_millis(),
            "mempool_ms": mempool_elapsed.as_millis(),
        },
        "bundled": bundled,
        "mempool": mempool,
    });

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn display_lookup_results(
    did: &str,
    ops_with_loc: &[plcbundle::OperationWithLocation],
    mempool_ops: &[plcbundle::Operation],
    total_elapsed: std::time::Duration,
    lookup_elapsed: std::time::Duration,
    mempool_elapsed: std::time::Duration,
    verbose: bool,
) -> Result<()> {
    let nullified_count = ops_with_loc.iter().filter(|owl| owl.nullified).count();
    let total_ops = ops_with_loc.len() + mempool_ops.len();
    let active_ops = ops_with_loc.len() - nullified_count + mempool_ops.len();

    println!("═══════════════════════════════════════════════════════════════");
    println!("                    DID Lookup Results");
    println!("═══════════════════════════════════════════════════════════════\n");
    println!("DID: {}\n", did);

    println!("Summary");
    println!("───────");
    println!("  Total operations:   {}", total_ops);
    println!("  Active operations:  {}", active_ops);
    if nullified_count > 0 {
        println!("  Nullified:          {}", nullified_count);
    }
    if !ops_with_loc.is_empty() {
        println!("  Bundled:            {}", ops_with_loc.len());
    }
    if !mempool_ops.is_empty() {
        println!("  Mempool:            {}", mempool_ops.len());
    }
    println!();

    println!("Performance");
    println!("───────────");
    println!("  Index lookup:       {:?}", lookup_elapsed);
    println!("  Mempool check:      {:?}", mempool_elapsed);
    println!("  Total time:         {:?}\n", total_elapsed);

    // Show operations
    if !ops_with_loc.is_empty() {
        println!("Bundled Operations ({} total)", ops_with_loc.len());
        println!("══════════════════════════════════════════════════════════════\n");

        for (i, owl) in ops_with_loc.iter().enumerate() {
            let status = if owl.nullified {
                "✗ Nullified"
            } else {
                "✓ Active"
            };

            println!(
                "Operation {} [Bundle {:06}, Position {:04}]",
                i + 1,
                owl.bundle,
                owl.position
            );
            println!(
                "   CID:        {}",
                owl.operation.cid.as_ref().unwrap_or(&"".to_string())
            );
            println!("   Created:    {}", owl.operation.created_at);
            println!("   Status:     {}\n", status);

            if verbose && !owl.nullified {
                show_operation_details(&owl.operation);
            }
        }
    }

    if !mempool_ops.is_empty() {
        println!("Mempool Operations ({} total)", mempool_ops.len());
        println!("══════════════════════════════════════════════════════════════\n");

        for (i, op) in mempool_ops.iter().enumerate() {
            println!("Operation {} [Mempool]", i + 1);
            println!(
                "   CID:        {}",
                op.cid.as_ref().unwrap_or(&"".to_string())
            );
            println!("   Created:    {}", op.created_at);
            println!("   Status:     ✓ Active\n");
        }
    }

    println!("✓ Lookup complete in {:?}", total_elapsed);
    Ok(())
}

fn show_operation_details(op: &plcbundle::Operation) {
    if let Some(op_data) = op.operation.as_object() {
        if let Some(op_type) = op_data.get("type").and_then(|v| v.as_str()) {
            println!("   Type:       {}", op_type);
        }

        if let Some(handle) = op_data.get("handle").and_then(|v| v.as_str()) {
            println!("   Handle:     {}", handle);
        } else if let Some(aka) = op_data.get("alsoKnownAs").and_then(|v| v.as_array()) {
            if let Some(aka_str) = aka.first().and_then(|v| v.as_str()) {
                let handle = aka_str.strip_prefix("at://").unwrap_or(aka_str);
                println!("   Handle:     {}", handle);
            }
        }
    }
}

// DID BATCH - Process multiple DIDs (TODO)

pub fn cmd_did_batch(
    _dir: PathBuf,
    _action: String,
    _threads: usize,
    _output: Option<PathBuf>,
    _from_stdin: bool,
) -> Result<()> {
    log::error!("`did batch` not yet implemented");
    Ok(())
}
