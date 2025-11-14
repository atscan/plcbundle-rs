// DID Resolution and Query commands
use anyhow::Result;
use plcbundle::BundleManager;
use std::path::PathBuf;

// ============================================================================
// DID RESOLVE - Convert DID to W3C DID Document
// ============================================================================

pub fn cmd_did_resolve(dir: PathBuf, did: String, verbose: bool) -> Result<()> {
    let manager = BundleManager::new(dir)?;
    
    log::info!("Resolving DID: {}", did);
    
    // Get resolve result with stats
    let result = manager.resolve_did_with_stats(&did)?;
    
    // Get DID index for shard calculation
    let identifier = &did[8..]; // Strip "did:plc:" prefix
    let shard_num = calculate_shard_for_display(identifier);
    
    log::debug!("DID {} -> identifier '{}' -> shard {:02x}", did, identifier, shard_num);
    
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
        log::info!("Index: {:?} | Load: {:?} | Total: {:?}",
            result.index_time, result.load_time, result.total_time);
        log::info!("Source: bundle {:06}, position {}\n", result.bundle_number, result.position);
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

