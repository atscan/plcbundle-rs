// DID Index CLI commands
use anyhow::Result;
use plcbundle::{BundleManager, LoadOptions};
use std::path::PathBuf;
use std::time::Instant;

pub fn cmd_index_build(dir: PathBuf, force: bool) -> Result<()> {
    let manager = BundleManager::new(dir.clone())?;
    
    // Check if index exists
    let did_index = manager.get_did_index_stats();
    if did_index.total_dids > 0 && !force {
        eprintln!("DID index already exists (use --force to rebuild)");
        eprintln!("Total DIDs: {}", did_index.total_dids);
        return Ok(());
    }
    
    let last_bundle = manager.get_last_bundle();
    if last_bundle == 0 {
        eprintln!("No bundles to index");
        return Ok(());
    }
    
    eprintln!("Building DID index...");
    eprintln!("Indexing {} bundles\n", last_bundle);
    
    let start = Instant::now();
    
    manager.rebuild_did_index(Some(|current, total| {
        if current % 10 == 0 || current == total {
            eprint!("\rProgress: {}/{} ({:.1}%)", current, total, 
                (current as f64 / total as f64) * 100.0);
        }
    }))?;
    
    eprintln!();
    
    let elapsed = start.elapsed();
    let stats = manager.get_did_index_stats();
    
    eprintln!("\n✓ DID index built in {:?}", elapsed);
    eprintln!("  Total DIDs: {}", stats.total_dids);
    eprintln!("  Location: {}/.plcbundle/", dir.display());
    
    Ok(())
}

pub fn cmd_index_repair(dir: PathBuf) -> Result<()> {
    let manager = BundleManager::new(dir.clone())?;
    
    let did_index = manager.get_did_index_stats();
    if did_index.total_dids == 0 {
        eprintln!("DID index does not exist");
        eprintln!("Use: plcbundle-rs index build");
        return Ok(());
    }
    
    eprintln!("Repairing DID index...\n");
    
    let start = Instant::now();
    
    manager.rebuild_did_index(Some(|current, total| {
        if current % 10 == 0 || current == total {
            eprint!("\rProgress: {}/{} ({:.1}%)", current, total, 
                (current as f64 / total as f64) * 100.0);
        }
    }))?;
    
    eprintln!();
    
    let elapsed = start.elapsed();
    let stats = manager.get_did_index_stats();
    
    eprintln!("\n✓ DID index repaired in {:?}", elapsed);
    eprintln!("  Total DIDs: {}", stats.total_dids);
    
    Ok(())
}

pub fn cmd_index_stats(dir: PathBuf, json: bool) -> Result<()> {
    let manager = BundleManager::new(dir.clone())?;
    
    // Get raw stats from did_index
    let did_index = manager.get_did_index();
    let stats_map = did_index.read().unwrap().get_stats();
    
    if json {
        let json_str = serde_json::to_string_pretty(&stats_map)?;
        println!("{}", json_str);
        return Ok(());
    }
    
    // Check if index exists
    if !stats_map.get("exists")
        .and_then(|v| v.as_bool())
        .unwrap_or(false) 
    {
        eprintln!("DID index does not exist");
        eprintln!("Run: plcbundle-rs index build");
        return Ok(());
    }
    
    let total_dids = stats_map.get("total_dids")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let shard_count = stats_map.get("shard_count")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let last_bundle = stats_map.get("last_bundle")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let cached_shards = stats_map.get("cached_shards")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let cache_limit = stats_map.get("cache_limit")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let cache_hit_rate = stats_map.get("cache_hit_rate")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let total_lookups = stats_map.get("total_lookups")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    
    println!("\nDID Index Statistics");
    println!("════════════════════\n");
    println!("  Location:      {}/.plcbundle/", dir.display());
    println!("  Total DIDs:    {}", total_dids);
    println!("  Shard count:   {}", shard_count);
    println!("  Last bundle:   {:06}", last_bundle);
    println!();
    println!("  Cached shards: {} / {}", cached_shards, cache_limit);
    
    if total_lookups > 0 {
        println!("  Cache hit rate: {:.1}%", cache_hit_rate * 100.0);
        println!("  Total lookups:  {}", total_lookups);
    }
    
    println!();
    
    Ok(())
}

pub fn cmd_index_verify(dir: PathBuf, _verbose: bool) -> Result<()> {
    let manager = BundleManager::new(dir.clone())?;
    
    let did_index = manager.get_did_index();
    let stats_map = did_index.read().unwrap().get_stats();
    
    if !stats_map.get("exists")
        .and_then(|v| v.as_bool())
        .unwrap_or(false) 
    {
        eprintln!("DID index does not exist");
        eprintln!("Run: plcbundle-rs index build");
        return Ok(());
    }
    
    eprintln!("Verifying DID index...\n");
    
    let total_dids = stats_map.get("total_dids")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let last_bundle = stats_map.get("last_bundle")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    
    // Basic verification - check if last bundle matches
    let manager_last = manager.get_last_bundle();
    
    if last_bundle < manager_last as i64 {
        eprintln!("⚠️  Warning: Index is behind (has bundle {}, repo has {})", 
            last_bundle, manager_last);
        eprintln!("    Run: plcbundle-rs index repair");
        return Ok(());
    }
    
    eprintln!("✓ DID index is valid");
    eprintln!("  Total DIDs:  {}", total_dids);
    eprintln!("  Last bundle: {:06}", last_bundle);
    
    Ok(())
}

