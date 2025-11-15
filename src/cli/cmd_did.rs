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
// DID LOOKUP - Find all operations for a DID
// ============================================================================

pub fn cmd_did_lookup(dir: PathBuf, input: String, verbose: bool, json: bool) -> Result<()> {
    use std::time::Instant;
    
    // Initialize manager
    let manager = BundleManager::new(dir)?;
    
    // Resolve handle to DID if needed
    let (did, handle_resolve_time) = manager.resolve_handle_or_did(&input)?;
    
    if handle_resolve_time > 0 {
        eprintln!("⚠️  Resolved handle: {} → {}", input, did);
    }
    
    // Check if DID index exists
    let did_index_stats = manager.get_did_index_stats();
    if !did_index_stats.get("exists").and_then(|v| v.as_bool()).unwrap_or(false) {
        eprintln!("⚠️  DID index not found. Run: plcbundle-rs index build");
        eprintln!("    Falling back to full scan (slow)...\n");
    }
    
    let total_start = Instant::now();
    
    // Lookup operations with locations
    let lookup_start = Instant::now();
    let ops_with_loc = manager.get_did_operations_with_locations(&did)?;
    let lookup_elapsed = lookup_start.elapsed();
    
    // Check mempool
    let mempool_start = Instant::now();
    let mempool_ops = manager.get_did_operations_from_mempool(&did)?;
    let mempool_elapsed = mempool_start.elapsed();
    
    let total_elapsed = total_start.elapsed();
    
    if ops_with_loc.is_empty() && mempool_ops.is_empty() {
        if json {
            println!("{{\"found\": false, \"operations\": []}}");
        } else {
            println!("DID not found (searched in {:?})", total_elapsed);
        }
        return Ok(());
    }
    
    if json {
        return output_lookup_json(&did, &ops_with_loc, &mempool_ops, total_elapsed, lookup_elapsed, mempool_elapsed);
    }
    
    display_lookup_results(&did, &ops_with_loc, &mempool_ops, total_elapsed, lookup_elapsed, mempool_elapsed, verbose)
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
            let status = if owl.nullified { "✗ Nullified" } else { "✓ Active" };
            
            println!("Operation {} [Bundle {:06}, Position {:04}]", i + 1, owl.bundle, owl.position);
            println!("   CID:        {}", owl.operation.cid.as_ref().unwrap_or(&"".to_string()));
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
            println!("   CID:        {}", op.cid.as_ref().unwrap_or(&"".to_string()));
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

// ============================================================================
// DID HISTORY - Show complete audit log
// ============================================================================

pub fn cmd_did_history(dir: PathBuf, input: String, _verbose: bool, json: bool, compact: bool, include_nullified: bool) -> Result<()> {
    // Initialize manager
    let manager = BundleManager::new(dir)?;
    
    // Resolve handle to DID if needed
    let (did, _) = manager.resolve_handle_or_did(&input)?;
    
    // Get all operations with locations
    let ops_with_loc = manager.get_did_operations_with_locations(&did)?;
    
    // Get mempool operations
    let mempool_ops = manager.get_did_operations_from_mempool(&did)?;
    
    if ops_with_loc.is_empty() && mempool_ops.is_empty() {
        eprintln!("DID not found: {}", did);
        return Ok(());
    }
    
    if json {
        return output_history_json(&did, &ops_with_loc, &mempool_ops);
    }
    
    if compact {
        display_history_compact(&did, &ops_with_loc, &mempool_ops, include_nullified)
    } else {
        display_history_detailed(&did, &ops_with_loc, &mempool_ops, include_nullified)
    }
}

fn output_history_json(
    did: &str,
    ops_with_loc: &[plcbundle::OperationWithLocation],
    mempool_ops: &[plcbundle::Operation],
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
        "did": did,
        "bundled": bundled,
        "mempool": mempool,
    });
    
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn display_history_compact(
    did: &str,
    ops_with_loc: &[plcbundle::OperationWithLocation],
    mempool_ops: &[plcbundle::Operation],
    include_nullified: bool,
) -> Result<()> {
    println!("DID History: {}\n", did);
    
    for owl in ops_with_loc {
        if !include_nullified && owl.nullified {
            continue;
        }
        
        let status = if owl.nullified { "✗" } else { "✓" };
        println!("{} [{:06}:{:04}] {}  {}", 
            status,
            owl.bundle,
            owl.position,
            owl.operation.created_at,
            owl.operation.cid.as_ref().unwrap_or(&"".to_string()));
    }
    
    for op in mempool_ops {
        println!("✓ [mempool  ] {}  {}", 
            op.created_at,
            op.cid.as_ref().unwrap_or(&"".to_string()));
    }
    
    Ok(())
}

fn display_history_detailed(
    did: &str,
    ops_with_loc: &[plcbundle::OperationWithLocation],
    mempool_ops: &[plcbundle::Operation],
    include_nullified: bool,
) -> Result<()> {
    println!("═══════════════════════════════════════════════════════════════");
    println!("                    DID Audit Log");
    println!("═══════════════════════════════════════════════════════════════\n");
    println!("DID: {}\n", did);
    
    for (i, owl) in ops_with_loc.iter().enumerate() {
        if !include_nullified && owl.nullified {
            continue;
        }
        
        let status = if owl.nullified { "✗ Nullified" } else { "✓ Active" };
        
        println!("Operation {} [Bundle {:06}, Position {:04}]", i + 1, owl.bundle, owl.position);
        println!("   CID:        {}", owl.operation.cid.as_ref().unwrap_or(&"".to_string()));
        println!("   Created:    {}", owl.operation.created_at);
        println!("   Status:     {}\n", status);
    }
    
    if !mempool_ops.is_empty() {
        println!("Mempool Operations ({})", mempool_ops.len());
        println!("══════════════════════════════════════════════════════════════\n");
        
        for (i, op) in mempool_ops.iter().enumerate() {
            println!("Operation {} [Mempool]", i + 1);
            println!("   CID:        {}", op.cid.as_ref().unwrap_or(&"".to_string()));
            println!("   Created:    {}", op.created_at);
            println!("   Status:     ✓ Active\n");
        }
    }
    
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
// DID STATS - Show DID statistics
// ============================================================================

pub fn cmd_did_stats(dir: PathBuf, did: Option<String>, global: bool, json: bool) -> Result<()> {
    let manager = BundleManager::new(dir.clone())?;
    
    // Global stats
    if did.is_none() || global {
        return show_global_did_stats(&manager, &dir, json);
    }
    
    // Specific DID stats
    let did_str = did.unwrap();
    show_did_stats(&manager, &did_str, json)
}

fn show_global_did_stats(manager: &BundleManager, dir: &PathBuf, json: bool) -> Result<()> {
    let stats = manager.get_did_index_stats();
    
    if !stats.get("exists").and_then(|v| v.as_bool()).unwrap_or(false) {
        println!("DID index does not exist");
        println!("Run: plcbundle-rs index build");
        return Ok(());
    }
    
    if json {
        println!("{}", serde_json::to_string_pretty(&stats)?);
        return Ok(());
    }
    
    let indexed_dids = stats.get("indexed_dids").and_then(|v| v.as_i64()).unwrap_or(0);
    let mempool_dids = stats.get("mempool_dids").and_then(|v| v.as_i64()).unwrap_or(0);
    let total_dids = stats.get("total_dids").and_then(|v| v.as_i64()).unwrap_or(0);
    let shard_count = stats.get("shard_count").and_then(|v| v.as_i64()).unwrap_or(0);
    let last_bundle = stats.get("last_bundle").and_then(|v| v.as_i64()).unwrap_or(0);
    let cached_shards = stats.get("cached_shards").and_then(|v| v.as_i64()).unwrap_or(0);
    let cache_limit = stats.get("cache_limit").and_then(|v| v.as_i64()).unwrap_or(0);
    
    println!("\nDID Index Statistics");
    println!("════════════════════\n");
    println!("  Location:      {}/.plcbundle/", dir.display());
    
    if mempool_dids > 0 {
        println!("  Indexed DIDs:  {} (in bundles)", format_number(indexed_dids as usize));
        println!("  Mempool DIDs:  {} (not yet bundled)", format_number(mempool_dids as usize));
        println!("  Total DIDs:    {}", format_number(total_dids as usize));
    } else {
        println!("  Total DIDs:    {}", format_number(total_dids as usize));
    }
    
    println!("  Shard count:   {}", shard_count);
    println!("  Last bundle:   {:06}", last_bundle);
    
    if let Some(updated_at) = stats.get("updated_at").and_then(|v| v.as_str()) {
        println!("  Updated:       {}\n", updated_at);
    }
    
    println!("  Cached shards: {} / {}", cached_shards, cache_limit);
    
    if let Some(cache_order) = stats.get("cache_order").and_then(|v| v.as_array()) {
        if !cache_order.is_empty() {
            print!("  Hot shards:    ");
            for (i, shard) in cache_order.iter().take(10).enumerate() {
                if i > 0 {
                    print!(", ");
                }
                if let Some(shard_num) = shard.as_i64() {
                    print!("{:02x}", shard_num as u8);
                }
            }
            if cache_order.len() > 10 {
                print!("... (+{} more)", cache_order.len() - 10);
            }
            println!();
        }
    }
    
    println!();
    Ok(())
}

fn show_did_stats(manager: &BundleManager, did: &str, json: bool) -> Result<()> {
    // Get operations
    let ops_with_loc = manager.get_did_operations_with_locations(did)?;
    let mempool_ops = manager.get_did_operations_from_mempool(did)?;
    
    if ops_with_loc.is_empty() && mempool_ops.is_empty() {
        eprintln!("DID not found: {}", did);
        return Ok(());
    }
    
    // Calculate stats
    let total_ops = ops_with_loc.len() + mempool_ops.len();
    let nullified_count = ops_with_loc.iter().filter(|owl| owl.nullified).count();
    let active_ops = total_ops - nullified_count;
    
    let bundle_span = if !ops_with_loc.is_empty() {
        let bundles: std::collections::HashSet<u32> = ops_with_loc.iter().map(|owl| owl.bundle).collect();
        bundles.len()
    } else {
        0
    };
    
    if json {
        use serde_json::json;
        let output = json!({
            "did": did,
            "total_operations": total_ops,
            "bundled": ops_with_loc.len(),
            "mempool": mempool_ops.len(),
            "nullified": nullified_count,
            "active": active_ops,
            "bundle_span": bundle_span,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
        return Ok(());
    }
    
    println!("\nDID Statistics");
    println!("══════════════\n");
    println!("  DID:              {}\n", did);
    println!("  Total operations: {}", total_ops);
    println!("  Active:           {}", active_ops);
    if nullified_count > 0 {
        println!("  Nullified:        {}", nullified_count);
    }
    if !ops_with_loc.is_empty() {
        println!("  Bundled:          {}", ops_with_loc.len());
        println!("  Bundle span:      {} bundles", bundle_span);
    }
    if !mempool_ops.is_empty() {
        println!("  Mempool:          {}", mempool_ops.len());
    }
    println!();
    
    Ok(())
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

