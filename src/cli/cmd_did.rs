// DID Resolution and Query commands
use anyhow::Result;
use plcbundle::BundleManager;
use std::path::PathBuf;

// ============================================================================
// DID RESOLVE - Convert DID to W3C DID Document
// ============================================================================

pub fn cmd_did_resolve(dir: PathBuf, input: String, handle_resolver_url: Option<String>, verbose: bool) -> Result<()> {
    use plcbundle::constants;
    
    // Use default resolver if none provided
    let resolver_url = handle_resolver_url.or_else(|| {
        Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string())
    });
    
    // Initialize manager with handle resolver (default or provided)
    let manager = BundleManager::with_handle_resolver(dir, resolver_url)?;
    
    // Resolve handle to DID if needed
    let (did, handle_resolve_time) = manager.resolve_handle_or_did(&input)?;
    
    if handle_resolve_time > 0 {
        log::info!("Resolved handle: {} → {} (in {}ms)", input, did, handle_resolve_time);
    } else {
        log::info!("Resolving DID: {}", did);
    }
    
    // Get resolve result with stats
    let result = manager.resolve_did_with_stats(&did)?;
    
    // Get DID index for shard calculation (only for PLC DIDs)
    if did.starts_with("did:plc:") {
        let identifier = &did[8..]; // Strip "did:plc:" prefix
        let shard_num = calculate_shard_for_display(identifier);
        log::debug!("DID {} -> identifier '{}' -> shard {:02x}", did, identifier, shard_num);
    }
    
    if let Some(stats) = &result.shard_stats {
        log::debug!("Shard {:02x} loaded, size: {} bytes", result.shard_num, stats.shard_size);
        
        let reduction = if stats.total_entries > 0 {
            ((stats.total_entries - stats.prefix_narrowed_to) as f64 / stats.total_entries as f64) * 100.0
        } else {
            0.0
        };
        
        log::debug!("Prefix index narrowed search: {} entries → {} entries ({:.1}% reduction)",
            stats.total_entries, stats.prefix_narrowed_to, reduction);
        log::debug!("Binary search found {} locations after {} attempts",
            stats.locations_found, stats.binary_search_attempts);
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
            log::info!("  Load shard:    {:?} ({})", 
                timings.load_shard,
                if timings.cache_hit { "cache hit" } else { "cache miss" });
            
            // Search breakdown
            log::info!("  Search:");
            if let Some(ref base_time) = timings.base_search_time {
                log::info!("    Base shard:  {:?}", base_time);
            }
            if !timings.delta_segment_times.is_empty() {
                let total_delta_time: std::time::Duration = timings.delta_segment_times.iter()
                    .map(|(_, time)| *time)
                    .sum();
                log::info!("    Delta segs:  {:?} ({} segment{})", 
                    total_delta_time,
                    timings.delta_segment_times.len(),
                    if timings.delta_segment_times.len() == 1 { "" } else { "s" });
                
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
        log::info!("Total:          {:?} (handle: {:?} + did: {:?})", 
            true_total, 
            handle_resolve_duration, 
            result.total_time);
        
        // Calculate global position: (bundle * BUNDLE_SIZE) + position
        let global_pos = (result.bundle_number as u64 * plcbundle::constants::BUNDLE_SIZE as u64) + result.position as u64;
        log::info!("Source: bundle {:06}, position {} (global: {})\n", 
            result.bundle_number, result.position, global_pos);
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

// ============================================================================
// DID LOOKUP - Find all operations for a DID (TODO)
// ============================================================================

pub fn cmd_did_lookup(_dir: PathBuf, _did: String, _verbose: bool, _json: bool) -> Result<()> {
    log::error!("`did lookup` not yet implemented");
    log::info!("Use: plcbundle-rs query <did> --bundles all");
    log::info!("Or wait for implementation");
    Ok(())
}

// ============================================================================
// DID HISTORY - Show complete audit log (TODO)
// ============================================================================

pub fn cmd_did_history(_dir: PathBuf, _did: String, _verbose: bool, _json: bool, _compact: bool, _include_nullified: bool) -> Result<()> {
    log::error!("`did history` not yet implemented");
    Ok(())
}

// ============================================================================
// DID BATCH - Process multiple DIDs (TODO)
// ============================================================================

pub fn cmd_did_batch(_dir: PathBuf, _action: String, _workers: usize, _output: Option<PathBuf>, _from_stdin: bool) -> Result<()> {
    log::error!("`did batch` not yet implemented");
    Ok(())
}

// ============================================================================
// DID STATS - Show DID statistics (TODO)
// ============================================================================

pub fn cmd_did_stats(_dir: PathBuf, _did: Option<String>, _global: bool, _json: bool) -> Result<()> {
    log::error!("`did stats` not yet implemented");
    log::info!("Use: plcbundle-rs index stats (for global stats)");
    Ok(())
}

