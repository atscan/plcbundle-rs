// DID Resolution and Query commands
use anyhow::Result;
use plcbundle::BundleManager;
use std::path::PathBuf;

// ============================================================================
// DID RESOLVE - Convert DID to W3C DID Document
// ============================================================================

pub fn cmd_did_resolve(dir: PathBuf, did: String, verbose: bool) -> Result<()> {
    let manager = BundleManager::new(dir)?;
    
    if verbose {
        eprintln!("Resolving DID: {}", did);
        
        // Get DID index stats
        let did_index = manager.get_did_index();
        let did_index_read = did_index.read().unwrap();
        let identifier = &did[8..]; // Strip "did:plc:" prefix
        let shard_num = calculate_shard_for_display(identifier);
        
        eprintln!("DEBUG: DID {} -> identifier '{}' -> shard {:02x}", did, identifier, shard_num);
        drop(did_index_read);
        
        // Get resolve result with stats
        let result = manager.resolve_did_with_stats(&did)?;
        
        if let Some(stats) = &result.shard_stats {
            eprintln!("DEBUG: Shard {:02x} loaded, size: {} bytes", result.shard_num, stats.shard_size);
            
            let reduction = if stats.total_entries > 0 {
                ((stats.total_entries - stats.prefix_narrowed_to) as f64 / stats.total_entries as f64) * 100.0
            } else {
                0.0
            };
            
            eprintln!("DEBUG: Prefix index narrowed search: {} entries → {} entries ({:.1}% reduction)",
                stats.total_entries, stats.prefix_narrowed_to, reduction);
            eprintln!("DEBUG: Binary search found {} locations after {} attempts",
                stats.locations_found, stats.binary_search_attempts);
        }
        
        eprintln!("Index: {:?} | Load: {:?} | Total: {:?}",
            result.index_time, result.load_time, result.total_time);
        eprintln!("Source: bundle {:06}, position {}\n", result.bundle_number, result.position);
        
        // Output document
        let json = serde_json::to_string_pretty(&result.document)?;
        println!("{}", json);
    } else {
        // Non-verbose: just output document
        let document = manager.resolve_did(&did)?;
        let json = serde_json::to_string_pretty(&document)?;
        println!("{}", json);
    }

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
    eprintln!("❌ `did lookup` not yet implemented");
    eprintln!("   Use: plcbundle-rs query <did> --bundles all");
    eprintln!("   Or wait for implementation");
    Ok(())
}

// ============================================================================
// DID HISTORY - Show complete audit log (TODO)
// ============================================================================

pub fn cmd_did_history(_dir: PathBuf, _did: String, _verbose: bool, _json: bool, _compact: bool, _include_nullified: bool) -> Result<()> {
    eprintln!("❌ `did history` not yet implemented");
    Ok(())
}

// ============================================================================
// DID BATCH - Process multiple DIDs (TODO)
// ============================================================================

pub fn cmd_did_batch(_dir: PathBuf, _action: String, _workers: usize, _output: Option<PathBuf>, _from_stdin: bool) -> Result<()> {
    eprintln!("❌ `did batch` not yet implemented");
    Ok(())
}

// ============================================================================
// DID STATS - Show DID statistics (TODO)
// ============================================================================

pub fn cmd_did_stats(_dir: PathBuf, _did: Option<String>, _global: bool, _json: bool) -> Result<()> {
    eprintln!("❌ `did stats` not yet implemented");
    eprintln!("   Use: plcbundle-rs index stats (for global stats)");
    Ok(())
}

