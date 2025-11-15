// DID Index CLI commands
use anyhow::Result;
use plcbundle::{BundleManager, constants};
use std::path::PathBuf;
use std::time::Instant;
use super::utils;

pub fn cmd_index_build(dir: PathBuf, force: bool) -> Result<()> {
    let manager = BundleManager::new(dir.clone())?;
    
    // Check if index exists
    let did_index = manager.get_did_index_stats();
    if did_index.total_dids > 0 && !force {
        log::info!("DID index already exists (use --force to rebuild)");
        log::info!("Total DIDs: {}", did_index.total_dids);
        return Ok(());
    }
    
    let last_bundle = manager.get_last_bundle();
    if last_bundle == 0 {
        log::info!("No bundles to index");
        return Ok(());
    }
    
    log::info!("Building DID index...");
    log::info!("Indexing {} bundles\n", last_bundle);
    
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

    log::info!("\n✓ DID index built in {:?}", elapsed);
    log::info!("  Total DIDs: {}", stats.total_dids);
    log::info!("  Location: {}/{}/", utils::display_path(&dir).display(), constants::DID_INDEX_DIR);
    
    Ok(())
}

pub fn cmd_index_repair(dir: PathBuf) -> Result<()> {
    let manager = BundleManager::new(dir.clone())?;
    
    // Check if index config exists (even if corrupted)
    let did_index = manager.get_did_index();
    let stats_map = did_index.read().unwrap().get_stats();
    let index_exists = stats_map.get("exists")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    
    if !index_exists {
        log::error!("DID index does not exist");
        log::info!("Use: plcbundle-rs index build");
        return Ok(());
    }
    
    // Check if there are bundles to index
    let last_bundle = manager.get_last_bundle();
    if last_bundle == 0 {
        log::info!("No bundles to index");
        return Ok(());
    }
    
    log::info!("Repairing DID index...\n");
    
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
    
    log::info!("\n✓ DID index repaired in {:?}", elapsed);
    log::info!("  Total DIDs: {}", stats.total_dids);
    
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
        log::error!("DID index does not exist");
        log::info!("Run: plcbundle-rs index build");
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
    let delta_segments = stats_map.get("delta_segments")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let compaction_strategy = stats_map.get("compaction_strategy")
        .and_then(|v| v.as_str())
        .unwrap_or("manual");
    
    println!("\nDID Index Statistics");
    println!("════════════════════\n");
    println!("  Location:      {}/{}/", utils::display_path(&dir).display(), constants::DID_INDEX_DIR);
    println!("  Total DIDs:    {}", total_dids);
    println!("  Shard count:   {}", shard_count);
    println!("  Last bundle:   {:06}", last_bundle);
    println!();
    println!("  Delta segments: {} (strategy: {})", delta_segments, compaction_strategy);
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
        log::error!("DID index does not exist");
        log::info!("Run: plcbundle-rs index build");
        return Ok(());
    }
    
    log::info!("Verifying DID index...\n");
    
    let total_dids = stats_map.get("total_dids")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let last_bundle = stats_map.get("last_bundle")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    
    // Basic verification - check if last bundle matches
    let manager_last = manager.get_last_bundle();
    
    if last_bundle < manager_last as i64 {
        log::warn!("⚠️  Warning: Index is behind (has bundle {}, repo has {})", 
            last_bundle, manager_last);
        log::info!("    Run: plcbundle-rs index repair");
        return Ok(());
    }
    
    log::info!("✓ DID index is valid");
    log::info!("  Total DIDs:  {}", total_dids);
    log::info!("  Last bundle: {:06}", last_bundle);
    
    Ok(())
}

#[allow(dead_code)]
pub fn cmd_index_compact(dir: PathBuf, shards: Option<Vec<u8>>) -> Result<()> {
    let manager = BundleManager::new(dir.clone())?;
    
    let did_index = manager.get_did_index();
    let stats_map = did_index.read().unwrap().get_stats();
    
    if !stats_map.get("exists")
        .and_then(|v| v.as_bool())
        .unwrap_or(false) 
    {
        log::error!("DID index does not exist");
        log::info!("Run: plcbundle-rs index build");
        return Ok(());
    }
    
    let delta_segments_before = stats_map.get("delta_segments")
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
    
    let start = Instant::now();
    did_index.write().unwrap().compact_pending_segments(shards)?;
    let elapsed = start.elapsed();
    
    let stats_map_after = did_index.read().unwrap().get_stats();
    let delta_segments_after = stats_map_after.get("delta_segments")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    
    log::info!("\n✓ Compaction complete in {:?}", elapsed);
    log::info!("  Delta segments before: {}", delta_segments_before);
    log::info!("  Delta segments after:  {}", delta_segments_after);
    log::info!("  Segments compacted:    {}", delta_segments_before.saturating_sub(delta_segments_after));
    
    Ok(())
}

