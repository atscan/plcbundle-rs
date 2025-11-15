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
    let shards_with_data = stats_map.get("shards_with_data")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let shards_with_segments = stats_map.get("shards_with_segments")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let max_segments_per_shard = stats_map.get("max_segments_per_shard")
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
    let total_shard_size = stats_map.get("total_shard_size_bytes")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let total_delta_size = stats_map.get("total_delta_size_bytes")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let compaction_strategy = stats_map.get("compaction_strategy")
        .and_then(|v| v.as_str())
        .unwrap_or("manual");
    
    println!("\nDID Index Statistics");
    println!("════════════════════\n");
    println!("  Location:      {}/{}/", utils::display_path(&dir).display(), constants::DID_INDEX_DIR);
    println!("  Total DIDs:    {}", utils::format_number(total_dids as u64));
    println!("  Shard count:   {} ({} with data, {} with segments)", 
        shard_count, shards_with_data, shards_with_segments);
    println!("  Last bundle:   {:06}", last_bundle);
    println!();
    println!("  Storage:");
    println!("    Base shards:  {} ({})", 
        shards_with_data, 
        utils::format_bytes(total_shard_size));
    println!("    Delta segs:   {} ({})", 
        delta_segments, 
        utils::format_bytes(total_delta_size));
    println!("    Total size:   {}", 
        utils::format_bytes(total_shard_size + total_delta_size));
    if max_segments_per_shard > 0 {
        println!("    Max seg/shard: {}", max_segments_per_shard);
    }
    println!("    Strategy:     {}", compaction_strategy);
    println!();
    println!("  Cache:");
    println!("    Cached:       {} / {}", cached_shards, cache_limit);
    if total_lookups > 0 {
        println!("    Hit rate:      {:.1}%", cache_hit_rate * 100.0);
        println!("    Total lookups: {}", utils::format_number(total_lookups as u64));
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

pub fn cmd_index_debug(dir: PathBuf, shard: Option<u8>, json: bool) -> Result<()> {
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
    
    let shard_details = did_index.read().unwrap().get_shard_details(shard)?;
    
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
            
            let did_count = detail.get("did_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let segment_count = detail.get("segment_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let base_exists = detail.get("base_exists")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let base_size = detail.get("base_size_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let total_segment_size = detail.get("total_segment_size_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let total_size = detail.get("total_size_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let next_segment_id = detail.get("next_segment_id")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            
            println!("  DIDs:              {}", utils::format_number(did_count));
            println!("  Base shard:        {} ({})", 
                if base_exists { "exists" } else { "missing" },
                utils::format_bytes(base_size));
            println!("  Delta segments:    {}", segment_count);
            println!("  Segment size:      {}", utils::format_bytes(total_segment_size));
            println!("  Total size:        {}", utils::format_bytes(total_size));
            println!("  Next segment ID:   {}", next_segment_id);
            
            if let Some(segments) = detail.get("segments").and_then(|v| v.as_array()) {
                if !segments.is_empty() {
                    println!("\n  Delta Segments:");
                    println!("  ───────────────────────────────────────");
                    for (idx, seg) in segments.iter().enumerate() {
                        let file_name = seg.get("file_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("?");
                        let exists = seg.get("exists")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        let size = seg.get("size_bytes")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        let did_count = seg.get("did_count")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        let bundle_start = seg.get("bundle_start")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        let bundle_end = seg.get("bundle_end")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        
                        println!("    [{:2}] {} {} ({})", 
                            idx + 1,
                            if exists { "✓" } else { "✗" },
                            file_name,
                            utils::format_bytes(size));
                        println!("         Bundles: {:06}-{:06}, DIDs: {}, Locations: {}",
                            bundle_start,
                            bundle_end,
                            utils::format_number(did_count),
                            seg.get("location_count")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0));
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
            let did_count = detail.get("did_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let segment_count = detail.get("segment_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let base_size = detail.get("base_size_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let seg_size = detail.get("total_segment_size_bytes")
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
        
        println!("  Shards with data:    {} / {}", shards_with_data, shard_details.len());
        println!("  Shards with segments: {} / {}", shards_with_segments, shard_details.len());
        println!("  Total DIDs:          {}", utils::format_number(total_dids));
        println!("  Base shard size:     {}", utils::format_bytes(total_base_size));
        println!("  Delta segment size:  {}", utils::format_bytes(total_segment_size));
        println!("  Total size:          {}", utils::format_bytes(total_base_size + total_segment_size));
        println!("  Max segments/shard:   {}", max_segments);
        println!();
        
        // Show top 10 shards by DID count
        let mut sorted_shards: Vec<_> = shard_details.iter()
            .filter(|d| d.get("did_count").and_then(|v| v.as_u64()).unwrap_or(0) > 0)
            .collect();
        sorted_shards.sort_by_key(|d| {
            std::cmp::Reverse(d.get("did_count").and_then(|v| v.as_u64()).unwrap_or(0))
        });
        
        if !sorted_shards.is_empty() {
            println!("  Top 10 Shards by DID Count:");
            println!("  ───────────────────────────────────────");
            for (idx, detail) in sorted_shards.iter().take(10).enumerate() {
                let shard_hex = detail.get("shard_hex")
                    .and_then(|v| v.as_str())
                    .unwrap_or("??");
                let did_count = detail.get("did_count")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let segment_count = detail.get("segment_count")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let total_size = detail.get("total_size_bytes")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                
                println!("    [{:2}] Shard {:2}: {} DIDs, {} segments, {}",
                    idx + 1,
                    shard_hex,
                    utils::format_number(did_count),
                    segment_count,
                    utils::format_bytes(total_size));
            }
            println!();
        }
    }
    
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

