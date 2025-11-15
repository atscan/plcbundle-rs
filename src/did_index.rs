// Simplified DID Index implementation matching Go version
use crate::constants;
use anyhow::Result;
use memmap2::{Mmap, MmapOptions};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
const DID_SHARD_COUNT: usize = 256;
const DID_PREFIX: &str = "did:plc:";
const DID_IDENTIFIER_LEN: usize = 24;

const DIDINDEX_MAGIC: &[u8; 4] = b"PLCD";
const DIDINDEX_VERSION: u32 = 4;

// ============================================================================
// OpLocation - Packed 32-bit location with nullified flag
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpLocation(u32);

impl OpLocation {
    pub fn new(bundle: u16, position: u16, nullified: bool) -> Self {
        let global_pos = (bundle as u32) * constants::BUNDLE_SIZE as u32 + (position as u32);
        let mut loc = global_pos << 1;
        if nullified {
            loc |= 1;
        }
        OpLocation(loc)
    }

    pub fn global_position(&self) -> u32 {
        self.0 >> 1
    }

    pub fn bundle(&self) -> u16 {
        (self.global_position() / constants::BUNDLE_SIZE as u32) as u16
    }

    pub fn position(&self) -> u16 {
        (self.global_position() % constants::BUNDLE_SIZE as u32) as u16
    }

    pub fn nullified(&self) -> bool {
        (self.0 & 1) == 1
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }

    pub fn from_u32(value: u32) -> Self {
        OpLocation(value)
    }
}

// ============================================================================
// Config - Index metadata
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub version: u32,
    pub format: String,
    pub shard_count: usize,
    pub total_dids: i64,
    pub updated_at: String,
    pub last_bundle: i32,
}

impl Config {
    fn new() -> Self {
        Config {
            version: DIDINDEX_VERSION,
            format: "binary_v4".to_string(),
            shard_count: DID_SHARD_COUNT,
            total_dids: 0,
            updated_at: chrono::Utc::now().to_rfc3339(),
            last_bundle: 0,
        }
    }
}

// ============================================================================
// Shard - Memory-mapped shard with LRU tracking
// ============================================================================

struct Shard {
    _shard_num: u8,
    mmap: Option<Mmap>,
    _file: Option<File>,
    last_used: AtomicU64,
    access_count: AtomicU64,
}

impl Shard {
    fn new_empty(shard_num: u8) -> Self {
        Shard {
            _shard_num: shard_num,
            mmap: None,
            _file: None,
            last_used: AtomicU64::new(unix_timestamp()),
            access_count: AtomicU64::new(0),
        }
    }

    fn load(shard_num: u8, shard_path: &Path) -> Result<Self> {
        let file = File::open(shard_path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        Ok(Shard {
            _shard_num: shard_num,
            mmap: Some(mmap),
            _file: Some(file),
            last_used: AtomicU64::new(unix_timestamp()),
            access_count: AtomicU64::new(1),
        })
    }

    fn touch(&self) {
        self.last_used.store(unix_timestamp(), Ordering::Relaxed);
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }

    fn data(&self) -> Option<&[u8]> {
        self.mmap.as_ref().map(|m| &m[..])
    }
}

// ============================================================================
// ShardBuilder - Accumulates entries before writing
// ============================================================================

pub struct ShardBuilder {
    entries: HashMap<String, Vec<OpLocation>>,
}

impl ShardBuilder {
    fn new() -> Self {
        ShardBuilder {
            entries: HashMap::new(),
        }
    }

    fn add(&mut self, identifier: String, loc: OpLocation) {
        self.entries.entry(identifier).or_insert_with(Vec::new).push(loc);
    }

    fn merge(&mut self, other: HashMap<String, Vec<OpLocation>>) {
        for (id, locs) in other {
            self.entries.entry(id).or_insert_with(Vec::new).extend(locs);
        }
    }
}

// ============================================================================
// Stats structures
// ============================================================================

#[derive(Debug, Clone)]
pub struct DIDLookupStats {
    pub shard_size: usize,
    pub total_entries: usize,
    pub prefix_narrowed_to: usize,
    pub binary_search_attempts: usize,
    pub locations_found: usize,
}

// ============================================================================
// Manager - Main DID index manager
// ============================================================================

pub struct Manager {
    _base_dir: PathBuf,
    index_dir: PathBuf,
    shard_dir: PathBuf,
    config_path: PathBuf,

    // LRU cache for hot shards
    shard_cache: Arc<RwLock<HashMap<u8, Arc<Shard>>>>,
    max_cache: usize,

    config: Arc<RwLock<Config>>,

    // Performance tracking
    cache_hits: AtomicI64,
    cache_misses: AtomicI64,
    total_lookups: AtomicI64,
}

impl Manager {
    pub fn new(base_dir: PathBuf) -> Result<Self> {
        let index_dir = base_dir.join(constants::DID_INDEX_DIR);
        let shard_dir = index_dir.join(constants::DID_INDEX_SHARDS);
        let config_path = index_dir.join(constants::DID_INDEX_CONFIG);

        let config = if config_path.exists() {
            let data = fs::read_to_string(&config_path)?;
            serde_json::from_str(&data)?
        } else {
            Config::new()
        };

        Ok(Manager {
            _base_dir: base_dir,
            index_dir,
            shard_dir,
            config_path,
            shard_cache: Arc::new(RwLock::new(HashMap::new())),
            max_cache: 5,
            config: Arc::new(RwLock::new(config)),
            cache_hits: AtomicI64::new(0),
            cache_misses: AtomicI64::new(0),
            total_lookups: AtomicI64::new(0),
        })
    }

    pub fn exists(&self) -> bool {
        self.config_path.exists()
    }

    // Get DID locations (main lookup method)
    pub fn get_did_locations(&self, did: &str) -> Result<Vec<OpLocation>> {
        self.total_lookups.fetch_add(1, Ordering::Relaxed);

        let identifier = extract_identifier(did)?;
        let shard_num = self.calculate_shard(&identifier);
        let shard = self.load_shard(shard_num)?;

        shard.touch();

        if let Some(data) = shard.data() {
            Ok(self.search_shard(data, &identifier))
        } else {
            Ok(Vec::new())
        }
    }

    // Get DID locations with detailed statistics
    pub fn get_did_locations_with_stats(&self, did: &str) -> Result<(Vec<OpLocation>, DIDLookupStats, u8)> {
        self.total_lookups.fetch_add(1, Ordering::Relaxed);

        let identifier = extract_identifier(did)?;
        let shard_num = self.calculate_shard(&identifier);
        let shard = self.load_shard(shard_num)?;

        shard.touch();

        if let Some(data) = shard.data() {
            let (locations, stats) = self.search_shard_with_stats(data, &identifier);
            Ok((locations, stats, shard_num))
        } else {
            Ok((Vec::new(), DIDLookupStats {
                shard_size: 0,
                total_entries: 0,
                prefix_narrowed_to: 0,
                binary_search_attempts: 0,
                locations_found: 0,
            }, shard_num))
        }
    }

    // Build index from scratch
    pub fn build_from_scratch<F>(
        &self,
        bundles: Vec<(u32, Vec<(String, bool)>)>, // (bundle_num, [(did, nullified)])
        progress: F,
    ) -> Result<()>
    where
        F: Fn(usize, usize),
    {
        use std::time::Instant;

        let total_start = Instant::now();
        log::info!("Starting DID index build from scratch for {} bundles", bundles.len());

        fs::create_dir_all(&self.shard_dir)?;

        // Pass 1: Accumulate entries in memory per shard
        // Use HashMap to avoid file descriptor limits
        let mut shard_entries: HashMap<u8, Vec<(String, OpLocation)>> = HashMap::new();
        let mut total_operations = 0usize;
        let mut total_valid_dids = 0usize;
        let mut flush_count = 0usize;

        let pass1_start = Instant::now();
        log::debug!("[DID Index] Pass 1: Accumulating entries in memory...");

        for (idx, (bundle_num, operations)) in bundles.iter().enumerate() {
            let bundle_start = Instant::now();
            progress(idx + 1, bundles.len());

            let ops_in_bundle = operations.len();
            total_operations += ops_in_bundle;
            let mut valid_dids_in_bundle = 0usize;

            for (position, (did, nullified)) in operations.iter().enumerate() {
                let identifier = match extract_identifier(did) {
                    Ok(id) => id,
                    Err(_) => continue,
                };

                valid_dids_in_bundle += 1;
                total_valid_dids += 1;

                let shard_num = self.calculate_shard(&identifier);
                let loc = OpLocation::new(*bundle_num as u16, position as u16, *nullified);

                shard_entries
                    .entry(shard_num)
                    .or_insert_with(Vec::new)
                    .push((identifier, loc));
            }

            let bundle_duration = bundle_start.elapsed();
            log::debug!(
                "[DID Index] Processed bundle {:06}: {} ops ({} valid DIDs) in {:.3}ms",
                bundle_num, ops_in_bundle, valid_dids_in_bundle, bundle_duration.as_secs_f64() * 1000.0
            );

            // Write to disk every 10 bundles to avoid excessive memory usage
            if idx % 10 == 0 && idx > 0 {
                let flush_start = Instant::now();
                let mem_before = shard_entries.values().map(|v| v.len()).sum::<usize>();

                self.flush_shard_entries(&mut shard_entries)?;
                flush_count += 1;

                let flush_duration = flush_start.elapsed();
                log::info!(
                    "[DID Index] Flushed {} entries to disk (flush #{}) in {:.3}s",
                    mem_before, flush_count, flush_duration.as_secs_f64()
                );
            }
        }

        let pass1_duration = pass1_start.elapsed();
        log::info!(
            "[DID Index] Pass 1 complete: {} bundles, {} operations, {} valid DIDs in {:.3}s ({:.0} ops/sec)",
            bundles.len(), total_operations, total_valid_dids,
            pass1_duration.as_secs_f64(),
            total_operations as f64 / pass1_duration.as_secs_f64()
        );

        // Flush any remaining entries
        let final_flush_start = Instant::now();
        let remaining = shard_entries.values().map(|v| v.len()).sum::<usize>();
        if remaining > 0 {
            log::debug!("[DID Index] Final flush: {} remaining entries", remaining);
            self.flush_shard_entries(&mut shard_entries)?;
            log::debug!("[DID Index] Final flush took {:.3}s", final_flush_start.elapsed().as_secs_f64());
        }

        // Pass 2: Sort and write final shards
        log::info!("[DID Index] Pass 2: Consolidating {} shards...", DID_SHARD_COUNT);
        let pass2_start = Instant::now();
        let mut total_dids = 0i64;
        let mut shards_with_data = 0usize;
        let mut total_shard_size = 0u64;

        for shard_num in 0..DID_SHARD_COUNT {
            let shard_start = Instant::now();
            let count = self.consolidate_shard(shard_num as u8)?;

            if count > 0 {
                shards_with_data += 1;
                total_dids += count;

                // Get shard file size
                let shard_path = self.shard_dir.join(format!("{:02x}.idx", shard_num));
                if let Ok(metadata) = fs::metadata(&shard_path) {
                    let size = metadata.len();
                    total_shard_size += size;

                    let shard_duration = shard_start.elapsed();
                    log::debug!(
                        "[DID Index] Consolidated shard {:02x}: {} DIDs, {} bytes in {:.3}ms",
                        shard_num, count, size, shard_duration.as_secs_f64() * 1000.0
                    );
                }
            }

            // Log progress every 32 shards
            if (shard_num + 1) % 32 == 0 {
                log::debug!(
                    "[DID Index] Consolidation progress: {}/{} shards ({:.1}%)",
                    shard_num + 1, DID_SHARD_COUNT,
                    (shard_num + 1) as f64 / DID_SHARD_COUNT as f64 * 100.0
                );
            }
        }

        let pass2_duration = pass2_start.elapsed();
        log::info!(
            "[DID Index] Pass 2 complete: {} shards with data, {} total DIDs, {:.2} MB total size in {:.3}s",
            shards_with_data, total_dids,
            total_shard_size as f64 / 1024.0 / 1024.0,
            pass2_duration.as_secs_f64()
        );

        // Update config
        let last_bundle = bundles.last().map(|(n, _)| *n as i32).unwrap_or(0);
        self.update_config(total_dids, last_bundle)?;

        let total_duration = total_start.elapsed();
        log::info!(
            "[DID Index] ✓ Build complete: {} DIDs indexed across {} bundles in {:.3}s (avg {:.0} DIDs/sec)",
            total_dids, bundles.len(), total_duration.as_secs_f64(),
            total_dids as f64 / total_duration.as_secs_f64()
        );

        Ok(())
    }

    // Flush accumulated shard entries to temporary files
    fn flush_shard_entries(&self, shard_entries: &mut HashMap<u8, Vec<(String, OpLocation)>>) -> Result<()> {
        use std::time::Instant;

        let start = Instant::now();
        let mut total_entries = 0usize;
        let mut shards_flushed = 0usize;
        let mut total_bytes = 0usize;

        for (shard_num, entries) in shard_entries.drain() {
            if entries.is_empty() {
                continue;
            }

            let entry_count = entries.len();
            total_entries += entry_count;
            shards_flushed += 1;

            let tmp_path = self.shard_dir.join(format!("{:02x}.tmp", shard_num));
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(tmp_path)?;

            for (identifier, loc) in entries {
                file.write_all(identifier.as_bytes())?;
                file.write_all(&loc.as_u32().to_le_bytes())?;
                total_bytes += 28; // 24 bytes identifier + 4 bytes location
            }

            log::debug!(
                "[DID Index] Flushed shard {:02x}: {} entries ({} bytes)",
                shard_num, entry_count, entry_count * 28
            );
        }

        let duration = start.elapsed();
        if total_entries > 0 {
            log::debug!(
                "[DID Index] Flush complete: {} entries across {} shards, {:.2} KB in {:.3}s ({:.0} entries/sec)",
                total_entries, shards_flushed,
                total_bytes as f64 / 1024.0,
                duration.as_secs_f64(),
                total_entries as f64 / duration.as_secs_f64()
            );
        }

        Ok(())
    }

    // Update index for new bundle (incremental)
    pub fn update_for_bundle(
        &self,
        bundle_num: u32,
        operations: Vec<(String, bool)>, // (did, nullified)
    ) -> Result<()> {
        use std::time::Instant;

        let start = Instant::now();
        let total_ops = operations.len();

        log::debug!("[DID Index] Updating index for bundle {:06} ({} operations)", bundle_num, total_ops);

        // Group by shard
        let grouping_start = Instant::now();
        let mut shard_ops: HashMap<u8, HashMap<String, Vec<OpLocation>>> = HashMap::new();
        let mut valid_dids = 0usize;

        for (position, (did, nullified)) in operations.iter().enumerate() {
            let identifier = match extract_identifier(did) {
                Ok(id) => id,
                Err(_) => continue,
            };

            valid_dids += 1;
            let shard_num = self.calculate_shard(&identifier);
            let loc = OpLocation::new(bundle_num as u16, position as u16, *nullified);

            shard_ops
                .entry(shard_num)
                .or_insert_with(HashMap::new)
                .entry(identifier)
                .or_insert_with(Vec::new)
                .push(loc);
        }

        let grouping_duration = grouping_start.elapsed();
        log::debug!(
            "[DID Index]   Grouped {} valid DIDs into {} shards in {:.3}ms",
            valid_dids, shard_ops.len(), grouping_duration.as_secs_f64() * 1000.0
        );

        // Update each shard
        let update_start = Instant::now();
        let mut delta_dids = 0i64;

        for (shard_num, new_ops) in shard_ops {
            let new_dids_in_shard = new_ops.len();
            let delta = self.update_shard(shard_num, new_ops)?;
            delta_dids += delta;

            log::debug!(
                "[DID Index]   Updated shard {:02x}: {} new entries, {} net new DIDs",
                shard_num, new_dids_in_shard, delta
            );
        }

        let update_duration = update_start.elapsed();

        // Update config
        let config_start = Instant::now();
        let config = self.config.read().unwrap();
        let new_total = config.total_dids + delta_dids;
        drop(config);
        self.update_config(new_total, bundle_num as i32)?;
        let config_duration = config_start.elapsed();

        let total_duration = start.elapsed();
        log::debug!(
            "[DID Index] ✓ Bundle {:06} indexed: {} ops → {} DIDs (+{} new) in {:.3}ms (group={:.1}ms, update={:.1}ms, config={:.1}ms)",
            bundle_num, total_ops, new_total, delta_dids,
            total_duration.as_secs_f64() * 1000.0,
            grouping_duration.as_secs_f64() * 1000.0,
            update_duration.as_secs_f64() * 1000.0,
            config_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    // Get bundle numbers for a DID (convenience method)
    pub fn get_bundles_for_did(&self, did: &str) -> Result<Vec<u32>> {
        let locations = self.get_did_locations(did)?;
        let mut bundles: Vec<u32> = locations.iter().map(|loc| loc.bundle() as u32).collect();
        bundles.sort_unstable();
        bundles.dedup();
        Ok(bundles)
    }

    // Get statistics
    pub fn get_stats(&self) -> HashMap<String, serde_json::Value> {
        use serde_json::json;
        
        let config = self.config.read().unwrap();
        let cache = self.shard_cache.read().unwrap();
        
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 { hits as f64 / total as f64 } else { 0.0 };

        let mut stats = HashMap::new();
        stats.insert("exists".to_string(), json!(self.exists()));
        stats.insert("total_dids".to_string(), json!(config.total_dids));
        stats.insert("last_bundle".to_string(), json!(config.last_bundle));
        stats.insert("shard_count".to_string(), json!(config.shard_count));
        stats.insert("cached_shards".to_string(), json!(cache.len()));
        stats.insert("cache_limit".to_string(), json!(self.max_cache));
        stats.insert("cache_hits".to_string(), json!(hits));
        stats.insert("cache_misses".to_string(), json!(misses));
        stats.insert("cache_hit_rate".to_string(), json!(hit_rate));
        stats.insert("total_lookups".to_string(), json!(self.total_lookups.load(Ordering::Relaxed)));

        stats
    }

    // ========================================================================
    // Internal methods
    // ========================================================================

    fn calculate_shard(&self, identifier: &str) -> u8 {
        use fnv::FnvHasher;
        use std::hash::Hasher;
        
        let mut hasher = FnvHasher::default();
        hasher.write(identifier.as_bytes());
        let hash = hasher.finish() as u32;
        (hash % DID_SHARD_COUNT as u32) as u8
    }

    fn load_shard(&self, shard_num: u8) -> Result<Arc<Shard>> {
        // Check cache
        {
            let cache = self.shard_cache.read().unwrap();
            if let Some(shard) = cache.get(&shard_num) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Arc::clone(shard));
            }
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Load from disk
        let shard_path = self.shard_dir.join(format!("{:02x}.idx", shard_num));
        
        let shard = if shard_path.exists() {
            Arc::new(Shard::load(shard_num, &shard_path)?)
        } else {
            Arc::new(Shard::new_empty(shard_num))
        };

        // Add to cache
        {
            let mut cache = self.shard_cache.write().unwrap();
            cache.insert(shard_num, Arc::clone(&shard));
            
            // Evict if needed
            if cache.len() > self.max_cache {
                self.evict_lru(&mut cache);
            }
        }

        Ok(shard)
    }

    fn evict_lru(&self, cache: &mut HashMap<u8, Arc<Shard>>) {
        if let Some((oldest_key, _)) = cache
            .iter()
            .min_by_key(|(_, s)| s.last_used.load(Ordering::Relaxed))
        {
            let key = *oldest_key;
            cache.remove(&key);
        }
    }

    fn search_shard(&self, data: &[u8], identifier: &str) -> Vec<OpLocation> {
        let (locations, _) = self.search_shard_with_stats(data, identifier);
        locations
    }

    fn search_shard_with_stats(&self, data: &[u8], identifier: &str) -> (Vec<OpLocation>, DIDLookupStats) {

        if data.len() < 1056 {
            return (Vec::new(), DIDLookupStats {
                shard_size: data.len(),
                total_entries: 0,
                prefix_narrowed_to: 0,
                binary_search_attempts: 0,
                locations_found: 0,
            });
        }

        // Validate header
        if &data[0..4] != DIDINDEX_MAGIC {
            return (Vec::new(), DIDLookupStats {
                shard_size: data.len(),
                total_entries: 0,
                prefix_narrowed_to: 0,
                binary_search_attempts: 0,
                locations_found: 0,
            });
        }

        let entry_count = u32::from_le_bytes([data[9], data[10], data[11], data[12]]) as usize;
        if entry_count == 0 {
            return (Vec::new(), DIDLookupStats {
                shard_size: data.len(),
                total_entries: 0,
                prefix_narrowed_to: 0,
                binary_search_attempts: 0,
                locations_found: 0,
            });
        }

        // Binary search with prefix index optimization
        let mut left = 0;
        let mut right = entry_count;

        // Use prefix index to narrow range
        if !identifier.is_empty() {
            let prefix_byte = identifier.as_bytes()[0];
            let prefix_pos = 32 + (prefix_byte as usize * 4);
            
            if prefix_pos + 4 <= data.len() {
                let start_idx = u32::from_le_bytes([
                    data[prefix_pos],
                    data[prefix_pos + 1],
                    data[prefix_pos + 2],
                    data[prefix_pos + 3],
                ]);
                
                if start_idx != 0xFFFFFFFF {
                    left = start_idx as usize;
                    
                    // Find end of prefix range
                    for next_prefix in (prefix_byte as usize + 1)..256 {
                        let next_pos = 32 + (next_prefix * 4);
                        if next_pos + 4 > data.len() {
                            break;
                        }
                        let next_idx = u32::from_le_bytes([
                            data[next_pos],
                            data[next_pos + 1],
                            data[next_pos + 2],
                            data[next_pos + 3],
                        ]);
                        if next_idx != 0xFFFFFFFF {
                            right = next_idx as usize;
                            break;
                        }
                    }
                }
            }
        }

        let prefix_narrowed_to = right - left;

        // Binary search in narrowed range
        let offset_table_start = 1056;
        let mut attempts = 0;
        
        while left < right {
            attempts += 1;
            let mid = (left + right) / 2;
            let offset_pos = offset_table_start + (mid * 4);
            
            if offset_pos + 4 > data.len() {
                return (Vec::new(), DIDLookupStats {
                    shard_size: data.len(),
                    total_entries: entry_count,
                    prefix_narrowed_to,
                    binary_search_attempts: attempts,
                    locations_found: 0,
                });
            }

            let entry_offset = u32::from_le_bytes([
                data[offset_pos],
                data[offset_pos + 1],
                data[offset_pos + 2],
                data[offset_pos + 3],
            ]) as usize;

            if entry_offset + DID_IDENTIFIER_LEN > data.len() {
                return (Vec::new(), DIDLookupStats {
                    shard_size: data.len(),
                    total_entries: entry_count,
                    prefix_narrowed_to,
                    binary_search_attempts: attempts,
                    locations_found: 0,
                });
            }

            let entry_id = std::str::from_utf8(&data[entry_offset..entry_offset + DID_IDENTIFIER_LEN])
                .unwrap_or("");

            match identifier.cmp(entry_id) {
                std::cmp::Ordering::Equal => {
                    let locations = self.read_locations(data, entry_offset);
                    return (locations.clone(), DIDLookupStats {
                        shard_size: data.len(),
                        total_entries: entry_count,
                        prefix_narrowed_to,
                        binary_search_attempts: attempts,
                        locations_found: locations.len(),
                    });
                }
                std::cmp::Ordering::Less => right = mid,
                std::cmp::Ordering::Greater => left = mid + 1,
            }
        }

        (Vec::new(), DIDLookupStats {
            shard_size: data.len(),
            total_entries: entry_count,
            prefix_narrowed_to,
            binary_search_attempts: attempts,
            locations_found: 0,
        })
    }

    fn read_locations(&self, data: &[u8], mut offset: usize) -> Vec<OpLocation> {
        offset += DID_IDENTIFIER_LEN;
        
        if offset + 2 > data.len() {
            return Vec::new();
        }

        let count = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;

        let mut locations = Vec::with_capacity(count);
        for _ in 0..count {
            if offset + 4 > data.len() {
                break;
            }

            let packed = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            locations.push(OpLocation::from_u32(packed));
            offset += 4;
        }

        locations
    }

    fn consolidate_shard(&self, shard_num: u8) -> Result<i64> {
        use std::time::Instant;

        let temp_path = self.shard_dir.join(format!("{:02x}.tmp", shard_num));

        let data = match fs::read(&temp_path) {
            Ok(d) => d,
            Err(_) => return Ok(0),
        };

        if data.is_empty() {
            fs::remove_file(temp_path).ok();
            return Ok(0);
        }

        let start = Instant::now();

        // Parse entries (28 bytes each)
        let parse_start = Instant::now();
        let entry_count = data.len() / 28;
        let mut entries: Vec<(String, OpLocation)> = Vec::with_capacity(entry_count);

        for i in 0..entry_count {
            let offset = i * 28;
            let identifier = String::from_utf8_lossy(&data[offset..offset + 24]).to_string();
            let loc_bytes = [
                data[offset + 24],
                data[offset + 25],
                data[offset + 26],
                data[offset + 27],
            ];
            let loc = OpLocation::from_u32(u32::from_le_bytes(loc_bytes));
            entries.push((identifier, loc));
        }
        let parse_duration = parse_start.elapsed();

        // Sort by identifier
        let sort_start = Instant::now();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let sort_duration = sort_start.elapsed();

        // Group by DID
        let group_start = Instant::now();
        let mut builder = ShardBuilder::new();
        for (id, loc) in entries {
            builder.add(id, loc);
        }
        let group_duration = group_start.elapsed();
        let unique_dids = builder.entries.len();

        // Write final shard
        let write_start = Instant::now();
        self.write_shard(shard_num, &builder)?;
        let write_duration = write_start.elapsed();

        fs::remove_file(temp_path).ok();

        let total_duration = start.elapsed();

        if entry_count > 0 {
            log::debug!(
                "[DID Index] Shard {:02x} consolidate: {} entries → {} DIDs in {:.3}ms (parse={:.1}ms, sort={:.1}ms, group={:.1}ms, write={:.1}ms)",
                shard_num, entry_count, unique_dids,
                total_duration.as_secs_f64() * 1000.0,
                parse_duration.as_secs_f64() * 1000.0,
                sort_duration.as_secs_f64() * 1000.0,
                group_duration.as_secs_f64() * 1000.0,
                write_duration.as_secs_f64() * 1000.0
            );
        }

        Ok(builder.entries.len() as i64)
    }

    fn update_shard(&self, shard_num: u8, new_ops: HashMap<String, Vec<OpLocation>>) -> Result<i64> {
        use std::time::Instant;

        let start = Instant::now();
        let shard_path = self.shard_dir.join(format!("{:02x}.idx", shard_num));
        let mut builder = ShardBuilder::new();

        // Read existing shard
        let read_start = Instant::now();
        let existing_dids = if shard_path.exists() {
            let data = fs::read(&shard_path)?;
            if data.len() >= 32 {
                self.parse_shard_data(&data, &mut builder)?;
            }
            builder.entries.len()
        } else {
            0
        };
        let read_duration = read_start.elapsed();

        let before_count = builder.entries.len();
        let new_dids_count = new_ops.len();

        // Merge new operations
        let merge_start = Instant::now();
        builder.merge(new_ops);
        let merge_duration = merge_start.elapsed();
        let after_count = builder.entries.len();

        // Write updated shard
        let write_start = Instant::now();
        self.write_shard(shard_num, &builder)?;
        let write_duration = write_start.elapsed();

        // Invalidate cache
        let cache_start = Instant::now();
        self.shard_cache.write().unwrap().remove(&shard_num);
        let cache_duration = cache_start.elapsed();

        let total_duration = start.elapsed();
        let delta = (after_count - before_count) as i64;

        log::debug!(
            "[DID Index] Shard {:02x} update: {} existing + {} new → {} total (+{} net) in {:.3}ms (read={:.1}ms, merge={:.1}ms, write={:.1}ms, cache={:.1}ms)",
            shard_num, existing_dids, new_dids_count, after_count, delta,
            total_duration.as_secs_f64() * 1000.0,
            read_duration.as_secs_f64() * 1000.0,
            merge_duration.as_secs_f64() * 1000.0,
            write_duration.as_secs_f64() * 1000.0,
            cache_duration.as_secs_f64() * 1000.0
        );

        Ok(delta)
    }

    fn write_shard(&self, shard_num: u8, builder: &ShardBuilder) -> Result<()> {
        use std::time::Instant;

        let start = Instant::now();

        // Ensure shard directory exists
        if !self.shard_dir.exists() {
            fs::create_dir_all(&self.shard_dir)?;
        }

        let shard_path = self.shard_dir.join(format!("{:02x}.idx", shard_num));

        if builder.entries.is_empty() {
            fs::write(&shard_path, &[])?;
            return Ok(());
        }

        // Sort identifiers
        let sort_start = Instant::now();
        let mut identifiers: Vec<&String> = builder.entries.keys().collect();
        identifiers.sort();
        let sort_duration = sort_start.elapsed();

        // Build prefix index
        let prefix_start = Instant::now();
        let mut prefix_index = [0xFFFFFFFFu32; 256];
        for (i, id) in identifiers.iter().enumerate() {
            if !id.is_empty() {
                let prefix_byte = id.as_bytes()[0] as usize;
                if prefix_index[prefix_byte] == 0xFFFFFFFF {
                    prefix_index[prefix_byte] = i as u32;
                }
            }
        }
        let prefix_duration = prefix_start.elapsed();

        // Calculate offsets
        let offset_start = Instant::now();
        let offset_table_start = 1056;
        let data_start = offset_table_start + (identifiers.len() * 4);
        let mut offsets = Vec::with_capacity(identifiers.len());
        let mut current_offset = data_start;
        let mut total_locations = 0usize;

        for id in &identifiers {
            offsets.push(current_offset);
            let locations = &builder.entries[*id];
            total_locations += locations.len();
            current_offset += DID_IDENTIFIER_LEN + 2 + (locations.len() * 4);
        }
        let offset_duration = offset_start.elapsed();

        // Build buffer
        let buffer_start = Instant::now();
        let mut buf = Vec::with_capacity(current_offset);

        // Header (32 bytes)
        buf.extend_from_slice(DIDINDEX_MAGIC);
        buf.extend_from_slice(&DIDINDEX_VERSION.to_le_bytes());
        buf.push(shard_num);
        buf.extend_from_slice(&(identifiers.len() as u32).to_le_bytes());
        buf.resize(32, 0);

        // Prefix index (1024 bytes)
        for idx in prefix_index.iter() {
            buf.extend_from_slice(&idx.to_le_bytes());
        }

        // Offset table
        for offset in offsets {
            buf.extend_from_slice(&(offset as u32).to_le_bytes());
        }

        // Entries
        for id in identifiers {
            let locations = &builder.entries[id];
            buf.extend_from_slice(id.as_bytes());
            buf.extend_from_slice(&(locations.len() as u16).to_le_bytes());
            for loc in locations {
                buf.extend_from_slice(&loc.as_u32().to_le_bytes());
            }
        }
        let buffer_duration = buffer_start.elapsed();

        // Write to disk
        let write_start = Instant::now();
        fs::write(&shard_path, &buf)?;
        let write_duration = write_start.elapsed();

        let total_duration = start.elapsed();

        log::debug!(
            "[DID Index] Write shard {:02x}: {} DIDs, {} locations, {} bytes in {:.3}ms (sort={:.1}ms, prefix={:.1}ms, offsets={:.1}ms, buffer={:.1}ms, disk={:.1}ms)",
            shard_num, builder.entries.len(), total_locations, buf.len(),
            total_duration.as_secs_f64() * 1000.0,
            sort_duration.as_secs_f64() * 1000.0,
            prefix_duration.as_secs_f64() * 1000.0,
            offset_duration.as_secs_f64() * 1000.0,
            buffer_duration.as_secs_f64() * 1000.0,
            write_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    fn parse_shard_data(&self, data: &[u8], builder: &mut ShardBuilder) -> Result<()> {
        if data.len() < 32 {
            return Ok(());
        }

        let entry_count = u32::from_le_bytes([data[9], data[10], data[11], data[12]]) as usize;
        let offset_table_start = 1056;

        for i in 0..entry_count {
            let offset_pos = offset_table_start + (i * 4);
            if offset_pos + 4 > data.len() {
                break;
            }

            let mut entry_offset = u32::from_le_bytes([
                data[offset_pos],
                data[offset_pos + 1],
                data[offset_pos + 2],
                data[offset_pos + 3],
            ]) as usize;

            if entry_offset + DID_IDENTIFIER_LEN + 2 > data.len() {
                break;
            }

            let identifier = String::from_utf8_lossy(
                &data[entry_offset..entry_offset + DID_IDENTIFIER_LEN]
            ).to_string();
            entry_offset += DID_IDENTIFIER_LEN;

            let loc_count = u16::from_le_bytes([data[entry_offset], data[entry_offset + 1]]) as usize;
            entry_offset += 2;

            let mut locations = Vec::with_capacity(loc_count);
            for _ in 0..loc_count {
                if entry_offset + 4 > data.len() {
                    break;
                }
                let packed = u32::from_le_bytes([
                    data[entry_offset],
                    data[entry_offset + 1],
                    data[entry_offset + 2],
                    data[entry_offset + 3],
                ]);
                locations.push(OpLocation::from_u32(packed));
                entry_offset += 4;
            }

            builder.entries.insert(identifier, locations);
        }

        Ok(())
    }

    fn update_config(&self, total_dids: i64, last_bundle: i32) -> Result<()> {
        let mut config = self.config.write().unwrap();
        config.total_dids = total_dids;
        config.last_bundle = last_bundle;
        config.updated_at = chrono::Utc::now().to_rfc3339();
        
        fs::create_dir_all(&self.index_dir)?;
        let json = serde_json::to_string_pretty(&*config)?;
        fs::write(&self.config_path, json)?;
        
        Ok(())
    }
}

// ============================================================================
// Helper functions
// ============================================================================

fn extract_identifier(did: &str) -> Result<String> {
    if !did.starts_with(DID_PREFIX) {
        anyhow::bail!("Invalid DID format");
    }

    let identifier = &did[DID_PREFIX.len()..];
    if identifier.len() != DID_IDENTIFIER_LEN {
        anyhow::bail!("Invalid DID identifier length");
    }

    Ok(identifier.to_string())
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
