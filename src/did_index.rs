// Simplified DID Index implementation matching Go version
use crate::constants;
use anyhow::{Context, Result};
use memmap2::{Mmap, MmapMut, MmapOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use rayon::prelude::*;
const DID_SHARD_COUNT: usize = 256;
const DID_PREFIX: &str = "did:plc:";
const DID_IDENTIFIER_LEN: usize = 24;

const DIDINDEX_MAGIC: &[u8; 4] = b"PLCD";
const DIDINDEX_VERSION: u32 = 4;

// ============================================================================
// OpLocation - Packed 32-bit global position with nullified flag
// 
// Storage format:
//   - Bits 31-1: Global position (0-indexed across all bundles)
//   - Bit 0: Nullified flag (1 if nullified, 0 otherwise)
// 
// Global position = ((bundle - 1) * BUNDLE_SIZE) + position
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpLocation(u32);

impl OpLocation {
    pub fn new(global_position: u32, nullified: bool) -> Self {
        let mut loc = global_position << 1;
        if nullified {
            loc |= 1;
        }
        OpLocation(loc)
    }

    pub fn global_position(&self) -> u32 {
        self.0 >> 1
    }

    pub fn bundle(&self) -> u16 {
        ((self.global_position() / constants::BUNDLE_SIZE as u32) + 1) as u16
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
    #[serde(default)]
    pub delta_segments_total: u64,
    #[serde(default = "default_compaction_strategy")]
    pub compaction_strategy: String,
    #[serde(default = "default_shard_meta_vec")]
    pub shards: Vec<ShardMeta>,
}

fn default_compaction_strategy() -> String {
    "manual".to_string()
}

fn default_shard_meta_vec() -> Vec<ShardMeta> {
    (0..DID_SHARD_COUNT)
        .map(|i| ShardMeta::new(i as u8))
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMeta {
    pub shard: u8,
    #[serde(default)]
    pub did_count: u32,
    #[serde(default = "default_next_segment_id")]
    pub next_segment_id: u64,
    #[serde(default)]
    pub segments: Vec<DeltaSegmentMeta>,
}

impl ShardMeta {
    fn new(shard: u8) -> Self {
        ShardMeta {
            shard,
            did_count: 0,
            next_segment_id: 1,
            segments: Vec::new(),
        }
    }
}

fn default_next_segment_id() -> u64 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaSegmentMeta {
    pub id: u64,
    pub file_name: String,
    pub bundle_start: u32,
    pub bundle_end: u32,
    pub did_count: u32,
    pub location_count: u32,
    pub created_at: String,
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
            delta_segments_total: 0,
            compaction_strategy: default_compaction_strategy(),
            shards: default_shard_meta_vec(),
        }
    }

    fn normalize(&mut self) {
        if self.shards.len() != DID_SHARD_COUNT {
            let mut normalized = default_shard_meta_vec();
            for meta in self.shards.iter() {
                let idx = meta.shard as usize;
                if idx < normalized.len() {
                    normalized[idx] = meta.clone();
                }
            }
            self.shards = normalized;
        }
    }
}

// ============================================================================
// Shard - Memory-mapped shard with LRU tracking
// ============================================================================

struct Shard {
    base: Option<Mmap>,
    segments: Vec<SegmentLayer>,
    last_used: AtomicU64,
    access_count: AtomicU64,
}

struct SegmentLayer {
    meta: DeltaSegmentMeta,
    mmap: Mmap,
    _file: File,
}

impl SegmentLayer {
    fn data(&self) -> &[u8] {
        &self.mmap[..]
    }
}

impl Shard {
    fn new_empty(_shard_num: u8) -> Self {
        Shard {
            base: None,
            segments: Vec::new(),
            last_used: AtomicU64::new(unix_timestamp()),
            access_count: AtomicU64::new(0),
        }
    }

    fn load(_shard_num: u8, shard_path: &Path) -> Result<Self> {
        let _file = File::open(shard_path)?;
        let mmap = unsafe { MmapOptions::new().map(&_file)? };

        Ok(Shard {
            base: Some(mmap),
            segments: Vec::new(),
            last_used: AtomicU64::new(unix_timestamp()),
            access_count: AtomicU64::new(1),
        })
    }

    fn touch(&self) {
        self.last_used.store(unix_timestamp(), Ordering::Relaxed);
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }

    fn base_data(&self) -> Option<&[u8]> {
        self.base.as_ref().map(|m| &m[..])
    }

    fn segments(&self) -> &[SegmentLayer] {
        &self.segments
    }

    fn with_segments(mut self, segments: Vec<SegmentLayer>) -> Self {
        self.segments = segments;
        self
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

    fn from_entries(entries: HashMap<String, Vec<OpLocation>>) -> Self {
        ShardBuilder { entries }
    }

    fn add(&mut self, identifier: String, loc: OpLocation) {
        self.entries
            .entry(identifier)
            .or_insert_with(Vec::new)
            .push(loc);
    }

    fn merge(&mut self, other: HashMap<String, Vec<OpLocation>>) {
        for (id, locs) in other {
            self.entries.entry(id).or_insert_with(Vec::new).extend(locs);
        }
    }

    fn total_locations(&self) -> usize {
        self.entries.values().map(|v| v.len()).sum()
    }

    fn into_entries(self) -> HashMap<String, Vec<OpLocation>> {
        self.entries
    }
}

// ============================================================================
// Stats structures
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct DIDLookupStats {
    pub shard_size: usize,
    pub total_entries: usize,
    pub prefix_narrowed_to: usize,
    pub binary_search_attempts: usize,
    pub locations_found: usize,
}

#[derive(Debug, Clone, Default)]
pub struct DIDLookupTimings {
    pub extract_identifier: std::time::Duration,
    pub calculate_shard: std::time::Duration,
    pub load_shard: std::time::Duration,
    pub search: std::time::Duration,
    pub cache_hit: bool,
    pub base_search_time: Option<std::time::Duration>,
    pub delta_segment_times: Vec<(String, std::time::Duration)>,
    pub merge_time: std::time::Duration,
}

#[derive(Debug, Clone, Default)]
struct SearchTimings {
    base_time: Option<std::time::Duration>,
    delta_times: Vec<(String, std::time::Duration)>,
    merge_time: std::time::Duration,
}

impl DIDLookupStats {
    fn accumulate(&mut self, other: &DIDLookupStats) {
        self.shard_size += other.shard_size;
        self.total_entries += other.total_entries;
        self.prefix_narrowed_to += other.prefix_narrowed_to;
        self.binary_search_attempts += other.binary_search_attempts;
        self.locations_found += other.locations_found;
    }
}

// ============================================================================
// Manager - Main DID index manager
// ============================================================================

pub struct Manager {
    _base_dir: PathBuf,
    index_dir: PathBuf,
    shard_dir: PathBuf,
    delta_dir: PathBuf,
    config_path: PathBuf,

    // LRU cache for hot shards
    shard_cache: Arc<RwLock<HashMap<u8, Arc<Shard>>>>,
    max_cache: usize,
    max_segments_per_shard: usize,

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
        let delta_dir = index_dir.join(constants::DID_INDEX_DELTAS);
        let config_path = index_dir.join(constants::DID_INDEX_CONFIG);

        let mut config = if config_path.exists() {
            let data = fs::read_to_string(&config_path)?;
            match sonic_rs::from_str::<Config>(&data) {
                Ok(mut loaded_config) => {
                    // Log if config needs repair
                    if loaded_config.shards.len() != DID_SHARD_COUNT {
                        log::warn!(
                            "[DID Index] Config corrupted: shards.len() = {} (expected {}). Auto-repairing...",
                            loaded_config.shards.len(),
                            DID_SHARD_COUNT
                        );
                        // Force create new shards array (discard corrupted one)
                        loaded_config.shards = default_shard_meta_vec();
                    }
                    loaded_config
                }
                Err(e) => {
                    log::warn!(
                        "[DID Index] Config parse error: {}. Creating new config.",
                        e
                    );
                    Config::new()
                }
            }
        } else {
            Config::new()
        };

        // CRITICAL: Normalize config to ensure all 256 shards exist
        config.normalize();

        // Verify we have exactly 256 shards (should always pass now)
        if config.shards.len() != DID_SHARD_COUNT {
            // This should never happen, but if it does, recreate from scratch
            log::error!(
                "[DID Index] Config normalization failed: shards.len() = {} (expected {}). Recreating config.",
                config.shards.len(),
                DID_SHARD_COUNT
            );
            config = Config::new();
            config.normalize();
        }

        let manager = Manager {
            _base_dir: base_dir,
            index_dir,
            shard_dir,
            delta_dir,
            config_path,
            shard_cache: Arc::new(RwLock::new(HashMap::new())),
            max_cache: 5,
            max_segments_per_shard: 8,
            config: Arc::new(RwLock::new(config.clone())),
            cache_hits: AtomicI64::new(0),
            cache_misses: AtomicI64::new(0),
            total_lookups: AtomicI64::new(0),
        };

        // Try to persist normalized config to fix corruption from old versions
        // This ensures the shards array is properly saved to disk
        // If this fails (e.g., read-only filesystem), we log a warning but continue
        // since read-only operations don't need to write the config
        if let Err(e) = manager.persist_config(&config) {
            log::warn!(
                "[DID Index] Failed to persist normalized config (read-only?): {}. Continuing with read-only access.",
                e
            );
        }

        log::debug!(
            "[DID Index] Config loaded and normalized ({} shards)",
            config.shards.len()
        );

        Ok(manager)
    }

    pub fn exists(&self) -> bool {
        self.config_path.exists()
    }

    // Get DID locations (main lookup method)
    pub fn get_did_locations(&self, did: &str) -> Result<Vec<OpLocation>> {
        self.total_lookups.fetch_add(1, Ordering::Relaxed);

        let identifier = extract_identifier(did)?;
        let shard_num = self.calculate_shard(&identifier);
        let (shard, _) = self.load_shard_with_cache_info(shard_num)?;

        shard.touch();

        let (locations, _) = self.search_shard_layers(&shard, &identifier);
        Ok(locations)
    }

    // Get DID locations with detailed statistics
    pub fn get_did_locations_with_stats(
        &self,
        did: &str,
    ) -> Result<(Vec<OpLocation>, DIDLookupStats, u8, DIDLookupTimings)> {
        use std::time::Instant;

        self.total_lookups.fetch_add(1, Ordering::Relaxed);
        let mut timings = DIDLookupTimings::default();

        let extract_start = Instant::now();
        let identifier = extract_identifier(did)?;
        timings.extract_identifier = extract_start.elapsed();

        let calc_start = Instant::now();
        let shard_num = self.calculate_shard(&identifier);
        timings.calculate_shard = calc_start.elapsed();

        let load_start = Instant::now();
        let (shard, cache_hit) = self.load_shard_with_cache_info(shard_num)?;
        timings.load_shard = load_start.elapsed();
        timings.cache_hit = cache_hit;

        shard.touch();

        let search_start = Instant::now();
        let (locations, stats, search_timings) =
            self.search_shard_layers_with_timings(&shard, &identifier);
        timings.search = search_start.elapsed();
        timings.base_search_time = search_timings.base_time;
        timings.delta_segment_times = search_timings.delta_times;
        timings.merge_time = search_timings.merge_time;

        Ok((locations, stats, shard_num, timings))
    }

    /// Sample deterministic random DID identifiers directly from the index.
    ///
    /// This method avoids reading bundle files by retrieving identifiers
    /// from the memory-mapped shard data.
    pub fn sample_random_dids(&self, count: usize, seed: Option<u64>) -> Result<Vec<String>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let config = self.config.read().unwrap().clone();
        if config.total_dids <= 0 {
            anyhow::bail!("DID index is empty. Build the index first.");
        }

        let shard_weights: Vec<(u8, u32)> = config
            .shards
            .iter()
            .enumerate()
            .filter_map(|(idx, meta)| {
                if meta.did_count > 0 {
                    Some((idx as u8, meta.did_count))
                } else {
                    None
                }
            })
            .collect();

        if shard_weights.is_empty() {
            anyhow::bail!("No DIDs available in index.");
        }

        let total_entries: u64 = shard_weights.iter().map(|(_, count)| *count as u64).sum();

        if total_entries == 0 {
            anyhow::bail!("DID index has zero entries.");
        }

        let seed = seed.unwrap_or_else(|| unix_timestamp());
        let mut attempts = 0usize;
        let mut results = Vec::with_capacity(count);

        while results.len() < count {
            if attempts > count * 20 {
                anyhow::bail!("Unable to sample random DIDs (index inconsistent).");
            }

            let rand_value = deterministic_u64(seed, attempts) % total_entries;
            attempts += 1;

            let Some((shard_num, local_index)) = select_shard_by_weight(&shard_weights, rand_value)
            else {
                continue;
            };

            match self.identifier_from_shard(shard_num, local_index as usize) {
                Ok(identifier) => {
                    results.push(format!("{}{}", DID_PREFIX, identifier));
                }
                Err(err) => {
                    log::warn!(
                        "[DID Index] Failed to read shard {:02x} entry {}: {}",
                        shard_num,
                        local_index,
                        err
                    );
                }
            }
        }

        Ok(results)
    }

    fn load_shard_with_cache_info(&self, shard_num: u8) -> Result<(Arc<Shard>, bool)> {
        // Check cache
        {
            let cache = self.shard_cache.read().unwrap();
            if let Some(shard) = cache.get(&shard_num) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok((Arc::clone(shard), true));
            }
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Load from disk
        let shard_path = self.shard_path(shard_num);
        let mut shard = if shard_path.exists() {
            Shard::load(shard_num, &shard_path)?
        } else {
            Shard::new_empty(shard_num)
        };

        let segments = self.load_shard_segments(shard_num)?;
        shard = shard.with_segments(segments);
        let shard = Arc::new(shard);

        // Add to cache
        {
            let mut cache = self.shard_cache.write().unwrap();
            cache.insert(shard_num, Arc::clone(&shard));

            // Evict if needed
            if cache.len() > self.max_cache {
                self.evict_lru(&mut cache);
            }
        }

        Ok((shard, false))
    }

    fn identifier_from_shard(&self, shard_num: u8, local_index: usize) -> Result<String> {
        let (shard, _) = self.load_shard_with_cache_info(shard_num)?;
        Self::identifier_from_layers(&shard, local_index).with_context(|| {
            format!(
                "Shard {:02x} does not have index {}",
                shard_num, local_index
            )
        })
    }

    fn identifier_from_layers(shard: &Shard, mut index: usize) -> Option<String> {
        if let Some(base) = shard.base_data() {
            let base_entries = entry_count_from_data(base);
            if index < base_entries {
                return read_identifier_at(base, index);
            }
            index = index.saturating_sub(base_entries);
        }

        for segment in shard.segments() {
            let data = segment.data();
            let segment_entries = entry_count_from_data(data);
            if index < segment_entries {
                return read_identifier_at(data, index);
            }
            index = index.saturating_sub(segment_entries);
        }

        None
    }

    // Build index from scratch

    // Flush accumulated shard entries to temporary files
    fn flush_shard_entries(
        &self,
        shard_entries: &mut HashMap<u8, Vec<(String, OpLocation)>>,
    ) -> Result<()> {
        use std::time::Instant;

        let start = Instant::now();
        let mut total_entries = 0usize;
        let mut shards_flushed = 0usize;
        let mut total_bytes = 0usize;
        let mut total_write_time = std::time::Duration::ZERO;
        let mut total_flush_time = std::time::Duration::ZERO;

        for (shard_num, entries) in shard_entries.drain() {
            if entries.is_empty() {
                continue;
            }

            let entry_count = entries.len();
            total_entries += entry_count;
            shards_flushed += 1;

            let tmp_path = self.shard_dir.join(format!("{:02x}.tmp", shard_num));
            let file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(tmp_path)?;

            // Use BufWriter to batch writes and reduce syscalls
            let mut writer = std::io::BufWriter::with_capacity(65536, file);

            let write_start = Instant::now();
            for (identifier, loc) in entries {
                let id_bytes = normalize_identifier_bytes(&identifier);
                writer.write_all(&id_bytes)?;
                writer.write_all(&loc.as_u32().to_le_bytes())?;
                total_bytes += 28; // 24 bytes identifier + 4 bytes location
            }
            let write_elapsed = write_start.elapsed();
            total_write_time += write_elapsed;

            let flush_start = Instant::now();
            writer.flush()?;
            let flush_elapsed = flush_start.elapsed();
            total_flush_time += flush_elapsed;

            log::trace!(
                "[DID Index] Flushed shard {:02x}: {} entries, write={:.3}ms, flush={:.3}ms",
                shard_num,
                entry_count,
                write_elapsed.as_secs_f64() * 1000.0,
                flush_elapsed.as_secs_f64() * 1000.0
            );
        }

        let duration = start.elapsed();
        if total_entries > 0 {
            let throughput_mbps = (total_bytes as f64 / 1024.0 / 1024.0) / duration.as_secs_f64();
            let avg_entries_per_shard = total_entries as f64 / shards_flushed as f64;

            log::debug!(
                "[DID Index] Flush complete: {} entries across {} shards ({:.0} avg/shard)",
                total_entries,
                shards_flushed,
                avg_entries_per_shard
            );
            log::debug!(
                "[DID Index]   Data: {:.2} MB in {:.3}s ({:.2} MB/s)",
                total_bytes as f64 / 1024.0 / 1024.0,
                duration.as_secs_f64(),
                throughput_mbps
            );
            log::debug!(
                "[DID Index]   Timing: write={:.3}s ({:.1}%), flush={:.3}s ({:.1}%), overhead={:.3}s ({:.1}%)",
                total_write_time.as_secs_f64(),
                total_write_time.as_secs_f64() / duration.as_secs_f64() * 100.0,
                total_flush_time.as_secs_f64(),
                total_flush_time.as_secs_f64() / duration.as_secs_f64() * 100.0,
                (duration - total_write_time - total_flush_time).as_secs_f64(),
                (duration - total_write_time - total_flush_time).as_secs_f64() / duration.as_secs_f64() * 100.0
            );
            log::debug!(
                "[DID Index]   Throughput: {:.0} entries/sec, {:.0} syscalls/sec (est.)",
                total_entries as f64 / duration.as_secs_f64(),
                shards_flushed as f64 / duration.as_secs_f64()
            );
        }

        Ok(())
    }

    /// Process a single bundle and return entries grouped by shard
    /// This is the shared implementation used by both parallel and sequential processing
    fn process_bundle_for_index(
        bundle_dir: &PathBuf,
        bundle_num: u32,
    ) -> Result<(HashMap<u8, Vec<(String, OpLocation)>>, u64, u64)> {
        use std::fs::File;
        use std::io::BufReader;

        let bundle_path = crate::constants::bundle_path(bundle_dir, bundle_num);
        if !bundle_path.exists() {
            return Ok((HashMap::new(), 0, 0));
        }

        let file = match File::open(&bundle_path) {
            Ok(f) => f,
            Err(_) => return Ok((HashMap::new(), 0, 0)),
        };

        let decoder = match zstd::Decoder::new(file) {
            Ok(d) => d,
            Err(_) => return Ok((HashMap::new(), 0, 0)),
        };

        let reader = BufReader::with_capacity(1024 * 1024, decoder);
        Self::process_bundle_lines(bundle_num, reader)
    }

    /// Core bundle processing logic - processes lines from a reader and groups by shard
    /// This shared function eliminates duplication between parallel and sequential processing
    fn process_bundle_lines<R: std::io::BufRead>(
        bundle_num: u32,
        reader: R,
    ) -> Result<(HashMap<u8, Vec<(String, OpLocation)>>, u64, u64)> {
        use sonic_rs::JsonValueTrait;

        let mut shard_entries: HashMap<u8, Vec<(String, OpLocation)>> = HashMap::new();
        let mut bundle_bytes = 0u64;
        let mut total_operations = 0u64;
        let mut position = 0u16;

        for line_result in reader.lines() {
            let line = match line_result {
                Ok(l) => l,
                Err(_) => continue,
            };

            if line.is_empty() {
                continue;
            }

            bundle_bytes += line.len() as u64 + 1;
            total_operations += 1;

            // Parse only the fields we need (did and nullified)
            if let Ok(value) = sonic_rs::from_str::<sonic_rs::Value>(&line) {
                if let Some(did) = value.get("did").and_then(|v| v.as_str()) {
                    let nullified = value.get("nullified")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);

                    // Calculate shard directly from DID bytes (no String allocation)
                    if let Some(shard_num) = calculate_shard_from_did(did) {
                        // Only allocate String when we actually need to store it
                        let identifier = &did[DID_PREFIX.len()..DID_PREFIX.len() + DID_IDENTIFIER_LEN];
                        let identifier = identifier.to_string();
                        
                        let global_pos = ((bundle_num as u32).saturating_sub(1)) * constants::BUNDLE_SIZE as u32 + (position as u32);
                        let loc = OpLocation::new(global_pos, nullified);

                        shard_entries
                            .entry(shard_num)
                            .or_insert_with(Vec::new)
                            .push((identifier, loc));
                    }
                }
            }

            position += 1;
            if position >= constants::BUNDLE_SIZE as u16 {
                break; // Safety: bundles shouldn't exceed BUNDLE_SIZE
            }
        }

        Ok((shard_entries, bundle_bytes, total_operations))
    }

    /// Build index from scratch using streaming approach
    /// This method processes bundles one at a time by streaming from disk,
    /// avoiding loading all bundles into memory at once.
    ///
    /// # Arguments
    /// * `bundle_dir` - Directory containing bundle files
    /// * `last_bundle` - Last bundle number to process
    /// * `flush_interval` - Flush to disk every N bundles (0 = only flush at end)
    /// * `progress_callback` - Optional callback for progress updates
    /// * `num_threads` - Number of threads for parallel processing (0 = auto, 1 = sequential)
    /// * `interrupted` - Optional atomic flag to stop progress updates when CTRL+C is pressed
    pub fn build_from_scratch<F>(
        &self,
        bundle_dir: &PathBuf,
        last_bundle: u32,
        flush_interval: u32,
        progress_callback: Option<F>,
        num_threads: usize,
        interrupted: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<(u64, u64, std::time::Duration, std::time::Duration)> // Returns (total_operations, bundles_processed, stage1_duration, stage2_duration)
    where
        F: Fn(u32, u32, u64, Option<String>) + Send + Sync, // (current, total, bytes_processed, stage)
    {
        use std::time::Instant;
        use std::sync::atomic::AtomicU64;

        let build_start = Instant::now();

        log::debug!("[DID Index] Starting streaming build for {} bundles", last_bundle);

        // Clear existing index
        fs::create_dir_all(&self.shard_dir)?;
        fs::remove_dir_all(&self.delta_dir).ok();
        
        // Cleanup any leftover temp files from previous interrupted builds
        self.cleanup_temp_files()?;
        
        // Set up cleanup guard to remove temp files on panic or early exit
        struct CleanupGuard {
            shard_dir: PathBuf,
        }
        
        impl Drop for CleanupGuard {
            fn drop(&mut self) {
                // Clean up temp files on drop (panic or normal exit)
                if let Err(e) = Self::cleanup(&self.shard_dir) {
                    log::warn!("[DID Index] Failed to cleanup temp files: {}", e);
                }
            }
        }
        
        impl CleanupGuard {
            fn cleanup(shard_dir: &Path) -> Result<()> {
                if !shard_dir.exists() {
                    return Ok(());
                }
                let mut cleaned = 0;
                for entry in fs::read_dir(shard_dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if let Some(ext) = path.extension() {
                        if ext == "tmp" {
                            fs::remove_file(&path)?;
                            cleaned += 1;
                        }
                    }
                }
                if cleaned > 0 {
                    log::debug!("[DID Index] Cleaned up {} temp files", cleaned);
                }
                Ok(())
            }
        }
        
        let _cleanup_guard = CleanupGuard {
            shard_dir: self.shard_dir.clone(),
        };

        // Set up CTRL+C handler to automatically cleanup temp files on interrupt
        // This ensures cleanup happens even if the build is interrupted
        #[cfg(feature = "cli")]
        {
            let shard_dir_for_signal = self.shard_dir.clone();
            let interrupted_flag = interrupted.clone();
            match ctrlc::set_handler(move || {
                // Set interrupted flag first (if provided) to stop progress bar updates
                if let Some(ref flag) = interrupted_flag {
                    flag.store(true, std::sync::atomic::Ordering::Relaxed);
                }
                eprintln!("\n\n⚠️  Interrupted by user (CTRL+C)");
                // Cleanup temp files immediately using the same cleanup logic
                if let Err(e) = CleanupGuard::cleanup(&shard_dir_for_signal) {
                    eprintln!("⚠️  Failed to cleanup temp files: {}", e);
                } else {
                    eprintln!("🧹 Cleaned up temporary files");
                }
                std::process::exit(130); // Standard exit code for CTRL+C
            }) {
                Ok(()) => {
                    log::debug!("[DID Index] CTRL+C handler registered for build");
                }
                Err(e) => {
                    // Handler might already be set (e.g., by CLI layer)
                    // This is okay, but log it for debugging
                    log::debug!("[DID Index] CTRL+C handler registration: {} (may already be set)", e);
                }
            }
        }

        // Determine if we should use parallel processing
        let use_parallel = num_threads != 1;
        let actual_threads = if num_threads == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            num_threads
        };

        if use_parallel {
            log::debug!("[DID Index] Using {} threads for parallel bundle processing", actual_threads);
            rayon::ThreadPoolBuilder::new()
                .num_threads(actual_threads)
                .build_global()
                .ok(); // Ignore error if already initialized
        }

        // Accumulate entries in memory per shard
        let mut shard_entries: HashMap<u8, Vec<(String, OpLocation)>> = HashMap::new();
        let mut total_operations = 0u64;
        let bytes_processed = AtomicU64::new(0);
        let mut flush_count = 0usize;
        let mut total_flush_time = std::time::Duration::ZERO;
        
        // Metrics tracking (aggregated every N bundles)
        let metrics_interval = 100; // Log metrics every 100 bundles
        let mut metrics_start = Instant::now();
        let mut metrics_ops = 0u64;
        let mut metrics_bytes = 0u64;
        let mut metrics_bundle_total = std::time::Duration::ZERO;

        // Process bundles in batches (parallel or sequential)
        let batch_size = if use_parallel { 100 } else { 1 }; // Process 100 bundles at a time in parallel
        let bundle_numbers: Vec<u32> = (1..=last_bundle).collect();
        
        for batch_start in (0..bundle_numbers.len()).step_by(batch_size) {
            let batch_end = (batch_start + batch_size).min(bundle_numbers.len());
            let batch: Vec<u32> = bundle_numbers[batch_start..batch_end].to_vec();
            
            let batch_results: Vec<Result<(HashMap<u8, Vec<(String, OpLocation)>>, u64, u64)>> = if use_parallel {
                // Process batch in parallel
                batch.par_iter()
                    .map(|&bundle_num| Self::process_bundle_for_index(bundle_dir, bundle_num))
                    .collect()
            } else {
                // Process batch sequentially (for metrics tracking)
                // Use the shared process_bundle_for_index function to avoid code duplication
                batch.iter()
                    .map(|&bundle_num| {
                        let bundle_processing_start = Instant::now();
                        let result = Self::process_bundle_for_index(bundle_dir, bundle_num);
                        metrics_bundle_total += bundle_processing_start.elapsed();
                        
                        // Track aggregate metrics from result
                        if let Ok((_, bundle_bytes, bundle_operations)) = &result {
                            metrics_bytes += bundle_bytes;
                            metrics_ops += bundle_operations;
                        }
                        
                        result
                    })
                    .collect()
            };
            
            // Merge results from batch into main shard_entries
            // Zip batch with results to track bundle numbers (rayon preserves order)
            for (bundle_num, result) in batch.iter().zip(batch_results.iter()) {
                let (mut batch_entries, batch_bytes, batch_ops) = match result {
                    Ok(r) => r.clone(),
                    Err(e) => return Err(anyhow::anyhow!("Error processing bundle {}: {}", bundle_num, e)),
                };
                bytes_processed.fetch_add(batch_bytes, Ordering::Relaxed);
                
                // Track basic metrics from batch results (works in both parallel and sequential mode)
                metrics_ops += batch_ops;
                metrics_bytes += batch_bytes;
                total_operations += batch_ops; // Count operations in both parallel and sequential mode
                log::debug!(
                    "[DID Index] Batch result for bundle {}: {} ops, {} bytes (total_operations now: {})",
                    bundle_num,
                    batch_ops,
                    batch_bytes,
                    total_operations
                );
                
                // Merge batch entries into main shard_entries
                for (shard_num, mut entries) in batch_entries.drain() {
                    shard_entries.entry(shard_num)
                        .or_insert_with(Vec::new)
                        .append(&mut entries);
                }
                
                // Update progress after each bundle (more granular updates)
                let current_bytes = bytes_processed.load(Ordering::Relaxed);
                if let Some(ref cb) = progress_callback {
                    cb(*bundle_num, last_bundle, current_bytes, Some("Stage 1/2: Processing bundles".to_string()));
                }
                
                // Flush to disk periodically to avoid excessive memory (check after each bundle)
                if flush_interval > 0 && *bundle_num % flush_interval == 0 {
                    let flush_start = Instant::now();
                    let mem_before = shard_entries.values().map(|v| v.len()).sum::<usize>();
                    let shards_used = shard_entries.len();
                    
                    self.flush_shard_entries(&mut shard_entries)?;
                    flush_count += 1;
                    
                    let flush_duration = flush_start.elapsed();
                    total_flush_time += flush_duration;
                    
                    log::info!(
                        "[DID Index] Flush #{}: {} entries across {} shards (bundle {}/{}), took {:.3}s",
                        flush_count,
                        mem_before,
                        shards_used,
                        bundle_num,
                        last_bundle,
                        flush_duration.as_secs_f64()
                    );
                }
            }
            
            // Check for metrics logging (using last bundle in batch)
            let last_bundle_in_batch = batch.last().copied().unwrap_or(0);
            if last_bundle_in_batch > 0 {
                
                // Log detailed metrics every N bundles
                if last_bundle_in_batch % metrics_interval == 0 || last_bundle_in_batch == last_bundle {
                    let metrics_duration = metrics_start.elapsed();
                    let ops_per_sec = if metrics_duration.as_secs_f64() > 0.0 {
                        metrics_ops as f64 / metrics_duration.as_secs_f64()
                    } else {
                        0.0
                    };
                    let mb_per_sec = if metrics_duration.as_secs_f64() > 0.0 {
                        (metrics_bytes as f64 / 1_000_000.0) / metrics_duration.as_secs_f64()
                    } else {
                        0.0
                    };
                    
                    log::info!(
                        "[DID Index] Metrics (bundles {}..{}): {} ops, {:.1} MB | {:.1} ops/sec, {:.1} MB/sec",
                        last_bundle_in_batch.saturating_sub(metrics_interval as u32 - 1).max(1),
                        last_bundle_in_batch,
                        metrics_ops,
                        metrics_bytes as f64 / 1_000_000.0,
                        ops_per_sec,
                        mb_per_sec
                    );
                    
                    // Show bundle processing time (simplified metrics)
                    if metrics_bundle_total.as_secs_f64() > 0.0 {
                        log::info!(
                            "[DID Index]   Bundle processing time: {:.1}ms total",
                            metrics_bundle_total.as_secs_f64() * 1000.0
                        );
                    }
                    
                    // Reset metrics for next interval
                    metrics_start = Instant::now();
                    metrics_ops = 0;
                    metrics_bytes = 0;
                    metrics_bundle_total = std::time::Duration::ZERO;
                }
            }
        }

        // Flush any remaining entries
        let remaining = shard_entries.values().map(|v| v.len()).sum::<usize>();
        if remaining > 0 {
            let final_flush_start = Instant::now();
            log::info!("[DID Index] Final flush: {} remaining entries", remaining);
            self.flush_shard_entries(&mut shard_entries)?;
            let final_flush_duration = final_flush_start.elapsed();
            total_flush_time += final_flush_duration;
            log::info!("[DID Index] Final flush completed in {:.3}s", final_flush_duration.as_secs_f64());
        }

        // CRITICAL: Ensure all file handles are closed and data is synced to disk
        // before starting consolidation, especially important for parallel processing
        // where files might be written and read simultaneously
        std::thread::yield_now(); // Give OS a chance to sync file handles
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst); // Memory barrier

        // Calculate stage 1 duration (processing bundles)
        let stage1_duration = build_start.elapsed();

        // Pass 2: Consolidate all temporary files for each shard
        eprintln!("\n📊 Stage 2/2: Consolidating shards...");
        log::debug!("[DID Index] Pass 2: Consolidating shards...");
        let pass2_start = Instant::now();

        // Use parallel processing for consolidation (shards are independent)
        let use_parallel_consolidation = use_parallel;
        // CRITICAL: Use usize range then map to u8, because DID_SHARD_COUNT (256) as u8 wraps to 0
        let shard_numbers: Vec<u8> = (0..DID_SHARD_COUNT).map(|i| i as u8).collect();
        
        // Use atomic counter for progress tracking in parallel mode
        let progress_counter = std::sync::atomic::AtomicU32::new(0);
        let progress_cb = progress_callback.as_ref();
        
        let consolidation_results: Vec<Result<(u8, i64)>> = if use_parallel_consolidation {
            // Process shards in parallel
            // IMPORTANT: Use try_reduce or collect with explicit error handling
            // to ensure all shards are processed even if some fail
            shard_numbers.par_iter()
                .map(|&shard_num| {
                    log::debug!("[DID Index] Parallel consolidation starting for shard {:02x}", shard_num);
                    // Wrap in explicit error handling to ensure we process all shards
                    let result = match self.consolidate_shard(shard_num) {
                        Ok(shard_did_count) => {
                            // Update progress atomically
                            if let Some(ref cb) = progress_cb {
                                let count = progress_counter.fetch_add(1, Ordering::Relaxed) + 1;
                                cb(count, DID_SHARD_COUNT as u32, 0, Some("Stage 2/2: Consolidating shards".to_string()));
                            }
                            log::debug!("[DID Index] Shard {:02x} consolidation succeeded: {} DIDs", shard_num, shard_did_count);
                            Ok((shard_num, shard_did_count))
                        }
                        Err(e) => {
                            log::warn!(
                                "[DID Index] Failed to consolidate shard {:02x}: {}",
                                shard_num,
                                e
                            );
                            // Still update progress even on error
                            if let Some(ref cb) = progress_cb {
                                let count = progress_counter.fetch_add(1, Ordering::Relaxed) + 1;
                                cb(count, DID_SHARD_COUNT as u32, 0, Some("Stage 2/2: Consolidating shards".to_string()));
                            }
                            // Return 0 DIDs for failed shard, but don't fail the entire operation
                            log::debug!(
                                "[DID Index] Shard {:02x} consolidation failed, returning 0 DIDs",
                                shard_num
                            );
                            Ok((shard_num, 0))
                        }
                    };
                    log::debug!("[DID Index] Parallel consolidation result for shard {:02x}: {:?}", shard_num, result);
                    result
                })
                .collect()
        } else {
            // Process shards sequentially with progress updates
            log::debug!("[DID Index] Using sequential consolidation");
            shard_numbers.iter()
                .map(|&shard_num| {
                    log::debug!("[DID Index] Sequential consolidation starting for shard {:02x}", shard_num);
                    // Update progress callback for consolidation phase
                    if let Some(ref cb) = progress_callback {
                        // For consolidation, we use shard number as progress indicator
                        // Total is DID_SHARD_COUNT, current is shard_num + 1
                        cb((shard_num + 1) as u32, DID_SHARD_COUNT as u32, 0, Some("Stage 2/2: Consolidating shards".to_string()));
                    }
                    let result = self.consolidate_shard(shard_num);
                    log::debug!("[DID Index] Sequential consolidation result for shard {:02x}: {:?}", shard_num, result);
                    match result {
                        Ok(shard_did_count) => Ok((shard_num, shard_did_count)),
                        Err(e) => {
                            log::warn!("[DID Index] Sequential consolidation failed for shard {:02x}: {}", shard_num, e);
                            Ok((shard_num, 0))
                        }
                    }
                })
                .collect()
        };

        // Collect results and update config
        log::debug!(
            "[DID Index] Collecting {} consolidation results",
            consolidation_results.len()
        );
        let mut total_dids = 0i64;
        for result in consolidation_results {
            let (shard_num, shard_did_count) = result?;
            total_dids += shard_did_count;
            log::debug!(
                "[DID Index] Shard {:02x} contributed {} DIDs (total_dids now: {})",
                shard_num,
                shard_did_count,
                total_dids
            );
            
            // Update shard metadata with did_count
            self.modify_config(|config| {
                if let Some(shard_meta) = config.shards.get_mut(shard_num as usize) {
                    shard_meta.did_count = shard_did_count as u32;
                }
            })?;
        }
        
        log::debug!(
            "[DID Index] Final total_dids after collecting all shard results: {}",
            total_dids
        );
        
        // Explicitly cleanup temp files on successful completion
        // (Drop guard will also handle this, but explicit cleanup ensures it happens)
        self.cleanup_temp_files()?;

        let pass2_duration = pass2_start.elapsed();
        log::debug!(
            "[DID Index] Pass 2 complete: Consolidated {} shards in {:.3}s",
            DID_SHARD_COUNT,
            pass2_duration.as_secs_f64()
        );

        // Update config with last_bundle, total_dids, and reset delta segments
        self.modify_config(|config| {
            config.last_bundle = last_bundle as i32;
            config.total_dids = total_dids;
            config.delta_segments_total = 0;
        })?;

        let total_duration = build_start.elapsed();
        log::info!(
            "[DID Index] Streaming build complete: {} bundles, {} operations, {} DIDs in {:.3}s",
            last_bundle,
            total_operations,
            total_dids,
            total_duration.as_secs_f64()
        );

        Ok((total_operations, last_bundle as u64, stage1_duration, pass2_duration))
    }

    // Update index for new bundle (incremental)
    // Returns whether any shards were compacted during the update
    pub fn update_for_bundle(
        &self,
        bundle_num: u32,
        operations: Vec<(String, bool)>, // (did, nullified)
    ) -> Result<bool> {
        use std::time::Instant;

        let start = Instant::now();
        let total_ops = operations.len();

        log::debug!(
            "[DID Index] Updating index for bundle {:06} ({} operations)",
            bundle_num,
            total_ops
        );

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
            let global_pos = ((bundle_num as u32).saturating_sub(1)) * constants::BUNDLE_SIZE as u32 + (position as u32);
            let loc = OpLocation::new(global_pos, *nullified);

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
            valid_dids,
            shard_ops.len(),
            grouping_duration.as_secs_f64() * 1000.0
        );

        // Write delta segments per shard (in parallel)
        let update_start = Instant::now();

        use rayon::prelude::*;
        let shard_updates: Vec<_> = shard_ops.into_iter().collect();

        let results: Vec<_> = shard_updates
            .into_par_iter()
            .map(|(shard_num, new_ops)| {
                let new_dids_in_shard = new_ops.len();
                let meta_opt = self.write_delta_segment(shard_num, new_ops, bundle_num)?;

                if let Some(meta) = &meta_opt {
                    log::debug!(
                        "[DID Index]   Delta segment {:02x}/#{:016x}: {} DIDs, {} locations",
                        shard_num,
                        meta.id,
                        new_dids_in_shard,
                        meta.location_count
                    );
                } else {
                    log::debug!(
                        "[DID Index]   Skipped shard {:02x}: no valid DIDs to append",
                        shard_num
                    );
                }

                let compacted = if meta_opt.is_some() {
                    self.auto_compact_if_needed(shard_num)?
                } else {
                    false
                };

                Ok::<_, anyhow::Error>((meta_opt.is_some(), compacted))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut segments_written = 0usize;
        let mut shards_compacted = 0usize;
        for (written, compacted) in results {
            if written {
                segments_written += 1;
            }
            if compacted {
                shards_compacted += 1;
            }
        }

        let update_duration = update_start.elapsed();

        if segments_written == 0 {
            self.modify_config(|cfg| {
                cfg.last_bundle = bundle_num as i32;
            })?;
        }

        let total_duration = start.elapsed();
        log::debug!(
            "[DID Index] ✓ Bundle {:06} indexed: {} ops, {} delta segments (compacted {}) in {:.3}ms (group={:.1}ms, delta={:.1}ms)",
            bundle_num,
            total_ops,
            segments_written,
            shards_compacted,
            total_duration.as_secs_f64() * 1000.0,
            grouping_duration.as_secs_f64() * 1000.0,
            update_duration.as_secs_f64() * 1000.0
        );

        Ok(shards_compacted > 0)
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
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };

        // Calculate shard statistics
        let shards_with_data = config.shards.iter().filter(|s| s.did_count > 0).count();
        let shards_with_segments = config
            .shards
            .iter()
            .filter(|s| !s.segments.is_empty())
            .count();
        let max_segments_per_shard = config
            .shards
            .iter()
            .map(|s| s.segments.len())
            .max()
            .unwrap_or(0);
        let total_shard_size: u64 = (0..DID_SHARD_COUNT)
            .map(|i| {
                let shard_path = self.shard_path(i as u8);
                if shard_path.exists() {
                    fs::metadata(&shard_path).map(|m| m.len()).unwrap_or(0)
                } else {
                    0
                }
            })
            .sum();
        let total_delta_size: u64 = config
            .shards
            .iter()
            .enumerate()
            .flat_map(|(idx, s)| s.segments.iter().map(move |seg| (idx as u8, seg)))
            .map(|(shard_num, seg)| {
                let path = self.segment_path(shard_num, &seg.file_name);
                if path.exists() {
                    fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
                } else {
                    0
                }
            })
            .sum();

        let mut stats = HashMap::new();
        stats.insert("exists".to_string(), json!(self.exists()));
        stats.insert("total_dids".to_string(), json!(config.total_dids));
        stats.insert("last_bundle".to_string(), json!(config.last_bundle));
        stats.insert("shard_count".to_string(), json!(config.shard_count));
        stats.insert("shards_with_data".to_string(), json!(shards_with_data));
        stats.insert(
            "shards_with_segments".to_string(),
            json!(shards_with_segments),
        );
        stats.insert(
            "max_segments_per_shard".to_string(),
            json!(max_segments_per_shard),
        );
        stats.insert(
            "total_shard_size_bytes".to_string(),
            json!(total_shard_size),
        );
        stats.insert(
            "total_delta_size_bytes".to_string(),
            json!(total_delta_size),
        );
        stats.insert("cached_shards".to_string(), json!(cache.len()));
        stats.insert("cache_limit".to_string(), json!(self.max_cache));
        stats.insert("cache_hits".to_string(), json!(hits));
        stats.insert("cache_misses".to_string(), json!(misses));
        stats.insert("cache_hit_rate".to_string(), json!(hit_rate));
        stats.insert(
            "total_lookups".to_string(),
            json!(self.total_lookups.load(Ordering::Relaxed)),
        );
        stats.insert(
            "delta_segments".to_string(),
            json!(config.delta_segments_total),
        );
        stats.insert(
            "compaction_strategy".to_string(),
            json!(config.compaction_strategy),
        );

        stats
    }

    // Get detailed shard information for debugging
    pub fn get_shard_details(
        &self,
        shard_num: Option<u8>,
    ) -> Result<Vec<HashMap<String, serde_json::Value>>> {
        use serde_json::json;
        use std::collections::HashMap as Map;

        let config = self.config.read().unwrap();
        let shards_to_check: Vec<u8> = if let Some(num) = shard_num {
            vec![num]
        } else {
            (0..DID_SHARD_COUNT).map(|i| i as u8).collect()
        };

        let mut details = Vec::new();
        for shard_num in shards_to_check {
            let shard_meta = config
                .shards
                .get(shard_num as usize)
                .cloned()
                .unwrap_or_else(|| ShardMeta::new(shard_num));

            let shard_path = self.shard_path(shard_num);
            let base_exists = shard_path.exists();
            let base_size = if base_exists {
                fs::metadata(&shard_path).map(|m| m.len()).unwrap_or(0)
            } else {
                0
            };

            let mut segment_details = Vec::new();
            let mut total_segment_size = 0u64;
            for seg in &shard_meta.segments {
                let seg_path = self.segment_path(shard_num, &seg.file_name);
                let seg_exists = seg_path.exists();
                let seg_size = if seg_exists {
                    fs::metadata(&seg_path).map(|m| m.len()).unwrap_or(0)
                } else {
                    0
                };
                total_segment_size += seg_size;

                segment_details.push(json!({
                    "id": seg.id,
                    "file_name": seg.file_name,
                    "exists": seg_exists,
                    "size_bytes": seg_size,
                    "bundle_start": seg.bundle_start,
                    "bundle_end": seg.bundle_end,
                    "did_count": seg.did_count,
                    "location_count": seg.location_count,
                    "created_at": seg.created_at,
                }));
            }

            let mut detail = Map::new();
            detail.insert("shard".to_string(), json!(shard_num));
            detail.insert("shard_hex".to_string(), json!(format!("{:02x}", shard_num)));
            detail.insert("did_count".to_string(), json!(shard_meta.did_count));
            detail.insert(
                "next_segment_id".to_string(),
                json!(shard_meta.next_segment_id),
            );
            detail.insert(
                "segment_count".to_string(),
                json!(shard_meta.segments.len()),
            );
            detail.insert("base_exists".to_string(), json!(base_exists));
            detail.insert("base_size_bytes".to_string(), json!(base_size));
            detail.insert(
                "total_segment_size_bytes".to_string(),
                json!(total_segment_size),
            );
            detail.insert(
                "total_size_bytes".to_string(),
                json!(base_size + total_segment_size),
            );
            detail.insert("segments".to_string(), json!(segment_details));

            details.push(detail);
        }

        Ok(details)
    }

    /// Compact pending delta segments. If `shards` is `None`, all shards are compacted.
    pub fn compact_pending_segments(&self, shards: Option<Vec<u8>>) -> Result<()> {
        match shards {
            Some(list) if !list.is_empty() => {
                for shard in list {
                    self.compact_shard(shard)?;
                }
            }
            _ => {
                for shard in 0..DID_SHARD_COUNT {
                    self.compact_shard(shard as u8)?;
                }
            }
        }

        Ok(())
    }

    // ========================================================================
    // Internal methods
    // ========================================================================

    pub fn calculate_shard(&self, identifier: &str) -> u8 {
        self.calculate_shard_from_bytes(identifier.as_bytes())
    }

    /// Calculate shard number directly from identifier bytes (no String allocation)
    pub fn calculate_shard_from_bytes(&self, identifier_bytes: &[u8]) -> u8 {
        use fnv::FnvHasher;
        use std::hash::Hasher;

        let mut hasher = FnvHasher::default();
        hasher.write(identifier_bytes);
        let hash = hasher.finish() as u32;
        (hash % DID_SHARD_COUNT as u32) as u8
    }

    fn shard_path(&self, shard_num: u8) -> PathBuf {
        self.shard_dir.join(format!("{:02x}.idx", shard_num))
    }

    fn shard_delta_dir(&self, shard_num: u8) -> PathBuf {
        self.delta_dir.join(format!("{:02x}", shard_num))
    }

    fn segment_path(&self, shard_num: u8, file_name: &str) -> PathBuf {
        self.shard_delta_dir(shard_num).join(file_name)
    }

    fn load_shard_segments(&self, shard_num: u8) -> Result<Vec<SegmentLayer>> {
        let metas = {
            let config = self.config.read().unwrap();
            config
                .shards
                .get(shard_num as usize)
                .map(|meta| meta.segments.clone())
                .unwrap_or_default()
        };

        let mut layers = Vec::new();
        for meta in metas {
            let path = self.segment_path(shard_num, &meta.file_name);
            if !path.exists() {
                continue;
            }

            let file = match File::open(&path) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let mmap = unsafe { MmapOptions::new().map(&file)? };
            layers.push(SegmentLayer {
                meta,
                mmap,
                _file: file,
            });
        }

        Ok(layers)
    }

    fn auto_compact_if_needed(&self, shard_num: u8) -> Result<bool> {
        let should_compact = {
            let config = self.config.read().unwrap();
            config
                .shards
                .get(shard_num as usize)
                .map(|meta| meta.segments.len() >= self.max_segments_per_shard)
                .unwrap_or(false)
        };

        if should_compact {
            self.compact_shard(shard_num)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn compact_shard(&self, shard_num: u8) -> Result<()> {
        use std::time::Instant;

        let segments = {
            let config = self.config.read().unwrap();
            config
                .shards
                .get(shard_num as usize)
                .map(|meta| meta.segments.clone())
                .unwrap_or_default()
        };

        if segments.is_empty() {
            return Ok(());
        }

        let start = Instant::now();

        let mut segment_builder = ShardBuilder::new();
        for meta in &segments {
            let path = self.segment_path(shard_num, &meta.file_name);
            if !path.exists() {
                continue;
            }
            let data = fs::read(&path)?;
            if data.len() >= 32 {
                self.parse_shard_data(&data, &mut segment_builder)?;
            }
        }

        let merged_entries = segment_builder.into_entries();
        if merged_entries.is_empty() {
            self.remove_segment_files(shard_num, &segments);
            self.modify_config(|cfg| {
                if let Some(meta) = cfg.shards.get_mut(shard_num as usize) {
                    meta.segments.clear();
                }
                cfg.delta_segments_total = cfg
                    .delta_segments_total
                    .saturating_sub(segments.len() as u64);
            })?;
            return Ok(());
        }

        let delta = self.update_shard(shard_num, merged_entries)?;
        self.remove_segment_files(shard_num, &segments);

        self.modify_config(|cfg| {
            if let Some(meta) = cfg.shards.get_mut(shard_num as usize) {
                meta.segments.clear();
                let updated = (meta.did_count as i64 + delta).max(0);
                meta.did_count = updated as u32;
            }
            cfg.delta_segments_total = cfg
                .delta_segments_total
                .saturating_sub(segments.len() as u64);
            cfg.total_dids = cfg.shards.iter().map(|m| m.did_count as i64).sum();
        })?;

        self.invalidate_shard_cache(shard_num);

        let duration = start.elapsed();
        log::info!(
            "[DID Index] Compacted shard {:02x}: removed {} segments in {:.3}ms",
            shard_num,
            segments.len(),
            duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    fn remove_segment_files(&self, shard_num: u8, segments: &[DeltaSegmentMeta]) {
        for meta in segments {
            let path = self.segment_path(shard_num, &meta.file_name);
            let _ = fs::remove_file(path);
        }
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

    fn search_shard_layers(
        &self,
        shard: &Shard,
        identifier: &str,
    ) -> (Vec<OpLocation>, DIDLookupStats) {
        let (locations, stats, _) = self.search_shard_layers_with_timings(shard, identifier);
        (locations, stats)
    }

    fn search_shard_layers_with_timings(
        &self,
        shard: &Shard,
        identifier: &str,
    ) -> (Vec<OpLocation>, DIDLookupStats, SearchTimings) {
        use std::time::Instant;
        let mut combined = Vec::new();
        let mut aggregated = DIDLookupStats::default();
        let mut search_timings = SearchTimings::default();

        // Search base shard
        if let Some(base) = shard.base_data() {
            let base_start = Instant::now();
            let (locations, stats) = self.search_shard_with_stats(base, identifier);
            search_timings.base_time = Some(base_start.elapsed());
            combined.extend(locations);
            aggregated.accumulate(&stats);
        }

        // Search delta segments
        for segment in shard.segments() {
            let seg_start = Instant::now();
            let (locations, stats) = self.search_shard_with_stats(segment.data(), identifier);
            let seg_time = seg_start.elapsed();

            let seg_name = segment.meta.file_name.clone();
            search_timings.delta_times.push((seg_name, seg_time));

            combined.extend(locations);
            aggregated.accumulate(&stats);
        }

        // Merge and sort results
        let merge_start = Instant::now();
        combined.sort_by_key(|loc| loc.as_u32());
        search_timings.merge_time = merge_start.elapsed();

        aggregated.locations_found = combined.len();

        (combined, aggregated, search_timings)
    }

    fn search_shard_with_stats(
        &self,
        data: &[u8],
        identifier: &str,
    ) -> (Vec<OpLocation>, DIDLookupStats) {
        if data.len() < 1056 {
            return (
                Vec::new(),
                DIDLookupStats {
                    shard_size: data.len(),
                    total_entries: 0,
                    prefix_narrowed_to: 0,
                    binary_search_attempts: 0,
                    locations_found: 0,
                },
            );
        }

        // Validate header
        if &data[0..4] != DIDINDEX_MAGIC {
            return (
                Vec::new(),
                DIDLookupStats {
                    shard_size: data.len(),
                    total_entries: 0,
                    prefix_narrowed_to: 0,
                    binary_search_attempts: 0,
                    locations_found: 0,
                },
            );
        }

        let entry_count = u32::from_le_bytes([data[9], data[10], data[11], data[12]]) as usize;
        if entry_count == 0 {
            return (
                Vec::new(),
                DIDLookupStats {
                    shard_size: data.len(),
                    total_entries: 0,
                    prefix_narrowed_to: 0,
                    binary_search_attempts: 0,
                    locations_found: 0,
                },
            );
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
                return (
                    Vec::new(),
                    DIDLookupStats {
                        shard_size: data.len(),
                        total_entries: entry_count,
                        prefix_narrowed_to,
                        binary_search_attempts: attempts,
                        locations_found: 0,
                    },
                );
            }

            let entry_offset = u32::from_le_bytes([
                data[offset_pos],
                data[offset_pos + 1],
                data[offset_pos + 2],
                data[offset_pos + 3],
            ]) as usize;

            if entry_offset + DID_IDENTIFIER_LEN > data.len() {
                return (
                    Vec::new(),
                    DIDLookupStats {
                        shard_size: data.len(),
                        total_entries: entry_count,
                        prefix_narrowed_to,
                        binary_search_attempts: attempts,
                        locations_found: 0,
                    },
                );
            }

            let entry_id =
                std::str::from_utf8(&data[entry_offset..entry_offset + DID_IDENTIFIER_LEN])
                    .unwrap_or("");

            match identifier.cmp(entry_id) {
                std::cmp::Ordering::Equal => {
                    let locations = self.read_locations(data, entry_offset);
                    return (
                        locations.clone(),
                        DIDLookupStats {
                            shard_size: data.len(),
                            total_entries: entry_count,
                            prefix_narrowed_to,
                            binary_search_attempts: attempts,
                            locations_found: locations.len(),
                        },
                    );
                }
                std::cmp::Ordering::Less => right = mid,
                std::cmp::Ordering::Greater => left = mid + 1,
            }
        }

        (
            Vec::new(),
            DIDLookupStats {
                shard_size: data.len(),
                total_entries: entry_count,
                prefix_narrowed_to,
                binary_search_attempts: attempts,
                locations_found: 0,
            },
        )
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

        // Guard to ensure temp file is deleted even on error
        struct TempFileGuard {
            path: PathBuf,
        }
        
        impl Drop for TempFileGuard {
            fn drop(&mut self) {
                if self.path.exists() {
                    if let Err(e) = fs::remove_file(&self.path) {
                        log::warn!("[DID Index] Failed to remove temp file {}: {}", self.path.display(), e);
                    }
                }
            }
        }
        
        let _temp_guard = TempFileGuard {
            path: temp_path.clone(),
        };

        // Check if temp file exists and get its size
        // With parallel processing, files might not be immediately visible, so retry a few times
        let file_metadata = {
            let mut retries = 3;
            loop {
                match fs::metadata(&temp_path) {
                    Ok(m) => break Ok(m),
                    Err(e) => {
                        retries -= 1;
                        if retries > 0 {
                            // Brief delay to allow filesystem to sync (especially important for parallel processing)
                            std::thread::yield_now();
                            std::hint::spin_loop(); // Give CPU a moment
                            continue;
                        } else {
                            break Err(e);
                        }
                    }
                }
            }
        };

        let file_metadata = match file_metadata {
            Ok(m) => m,
            Err(e) => {
                log::debug!(
                    "[DID Index] Shard {:02x} consolidate: temp file {} not found after retries: {}",
                    shard_num,
                    temp_path.display(),
                    e
                );
                return Ok(0);
            }
        };

        if file_metadata.len() == 0 {
            log::debug!(
                "[DID Index] Shard {:02x} consolidate: temp file {} is empty (0 bytes)",
                shard_num,
                temp_path.display()
            );
            // Delete empty temp file
            let _ = fs::remove_file(&temp_path);
            return Ok(0);
        }

        // Read the temp file
        let data = match fs::read(&temp_path) {
            Ok(d) => d,
            Err(e) => {
                log::warn!(
                    "[DID Index] Shard {:02x} consolidate: failed to read temp file {} ({} bytes): {}",
                    shard_num,
                    temp_path.display(),
                    file_metadata.len(),
                    e
                );
                return Ok(0);
            }
        };

        if data.is_empty() {
            log::debug!(
                "[DID Index] Shard {:02x} consolidate: temp file {} read as empty (expected {} bytes)",
                shard_num,
                temp_path.display(),
                file_metadata.len()
            );
            let _ = fs::remove_file(&temp_path);
            return Ok(0);
        }

        // Verify file size matches what we read
        if data.len() != file_metadata.len() as usize {
            log::warn!(
                "[DID Index] Shard {:02x} consolidate: temp file size mismatch: metadata says {} bytes, read {} bytes",
                shard_num,
                file_metadata.len(),
                data.len()
            );
        }

        log::trace!(
            "[DID Index] Shard {:02x} consolidate: reading {} bytes from {}",
            shard_num,
            data.len(),
            temp_path.display()
        );

        // Validate file size is a multiple of 28 bytes (entry size)
        let remainder = data.len() % 28;
        if remainder != 0 {
            log::warn!(
                "[DID Index] Shard {:02x} consolidate: temp file {} has invalid size: {} bytes (not a multiple of 28, remainder: {})",
                shard_num,
                temp_path.display(),
                data.len(),
                remainder
            );
            // Try to process what we can (truncate to multiple of 28)
            let truncated_len = data.len() - remainder;
            log::warn!(
                "[DID Index] Shard {:02x} consolidate: truncating to {} bytes (dropping {} trailing bytes)",
                shard_num,
                truncated_len,
                remainder
            );
        }

        let start = Instant::now();

        // Parse entries (28 bytes each)
        let parse_start = Instant::now();
        let entry_count = data.len() / 28;
        
        if entry_count == 0 {
            log::warn!(
                "[DID Index] Shard {:02x} consolidate: temp file {} has {} bytes but no complete entries (need at least 28 bytes per entry)",
                shard_num,
                temp_path.display(),
                data.len()
            );
            let _ = fs::remove_file(&temp_path);
            return Ok(0);
        }
        // Store (identifier_string, raw_bytes, location) so we can preserve exact bytes
        let mut entries: Vec<(String, [u8; DID_IDENTIFIER_LEN], OpLocation)> = Vec::with_capacity(entry_count);

        for i in 0..entry_count {
            let offset = i * 28;
            if offset + 28 > data.len() {
                anyhow::bail!(
                    "Invalid temp file: entry {} extends beyond file (offset={}, file_len={})",
                    i,
                    offset + 28,
                    data.len()
                );
            }
            let identifier_bytes = &data[offset..offset + DID_IDENTIFIER_LEN];
            // Create a string from the bytes for HashMap key (lossy conversion handles invalid UTF-8)
            let identifier = String::from_utf8_lossy(identifier_bytes).to_string();
            
            // Preserve the exact raw bytes for writing back
            let mut raw_bytes = [0u8; DID_IDENTIFIER_LEN];
            raw_bytes.copy_from_slice(identifier_bytes);
            
            let loc_bytes = [
                data[offset + 24],
                data[offset + 25],
                data[offset + 26],
                data[offset + 27],
            ];
            let loc = OpLocation::from_u32(u32::from_le_bytes(loc_bytes));
            entries.push((identifier, raw_bytes, loc));
        }
        let parse_duration = parse_start.elapsed();

        // Sort by identifier
        let sort_start = Instant::now();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let sort_duration = sort_start.elapsed();

        // Group by DID, preserving raw bytes mapping
        let group_start = Instant::now();
        let mut builder = ShardBuilder::new();
        let mut identifier_to_bytes: HashMap<String, [u8; DID_IDENTIFIER_LEN]> = HashMap::new();
        for (id_str, raw_bytes, loc) in entries {
            // Store mapping from string to raw bytes for writing
            identifier_to_bytes.insert(id_str.clone(), raw_bytes);
            builder.add(id_str, loc);
        }
        let group_duration = group_start.elapsed();
        let unique_dids = builder.entries.len();

        // Write final shard, using raw bytes when available
        let write_start = Instant::now();
        let target = self.shard_path(shard_num);
        self.write_shard_to_path_with_bytes(shard_num, &builder, &target, "base", Some(&identifier_to_bytes))?;
        let write_duration = write_start.elapsed();

        // Explicitly remove temp file immediately after successful write
        // (Drop guard will also handle this, but explicit removal ensures immediate cleanup)
        if let Err(e) = fs::remove_file(&temp_path) {
            log::warn!("[DID Index] Failed to remove temp file {}: {}", temp_path.display(), e);
        }

        let total_duration = start.elapsed();

        if entry_count > 0 {
            log::debug!(
                "[DID Index] Shard {:02x} consolidate: {} entries → {} DIDs in {:.3}ms (parse={:.1}ms, sort={:.1}ms, group={:.1}ms, write={:.1}ms)",
                shard_num,
                entry_count,
                unique_dids,
                total_duration.as_secs_f64() * 1000.0,
                parse_duration.as_secs_f64() * 1000.0,
                sort_duration.as_secs_f64() * 1000.0,
                group_duration.as_secs_f64() * 1000.0,
                write_duration.as_secs_f64() * 1000.0
            );
        }

        let did_count = builder.entries.len() as i64;
        log::debug!(
            "[DID Index] Shard {:02x} consolidate returning: {} DIDs",
            shard_num,
            did_count
        );
        Ok(did_count)
    }

    fn update_shard(
        &self,
        shard_num: u8,
        new_ops: HashMap<String, Vec<OpLocation>>,
    ) -> Result<i64> {
        use std::time::Instant;

        let start = Instant::now();
        let shard_path = self.shard_path(shard_num);
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
        self.invalidate_shard_cache(shard_num);
        let cache_duration = cache_start.elapsed();

        let total_duration = start.elapsed();
        let delta = (after_count - before_count) as i64;

        log::debug!(
            "[DID Index] Shard {:02x} update: {} existing + {} new → {} total (+{} net) in {:.3}ms (read={:.1}ms, merge={:.1}ms, write={:.1}ms, cache={:.1}ms)",
            shard_num,
            existing_dids,
            new_dids_count,
            after_count,
            delta,
            total_duration.as_secs_f64() * 1000.0,
            read_duration.as_secs_f64() * 1000.0,
            merge_duration.as_secs_f64() * 1000.0,
            write_duration.as_secs_f64() * 1000.0,
            cache_duration.as_secs_f64() * 1000.0
        );

        Ok(delta)
    }

    fn write_delta_segment(
        &self,
        shard_num: u8,
        new_ops: HashMap<String, Vec<OpLocation>>,
        bundle_num: u32,
    ) -> Result<Option<DeltaSegmentMeta>> {
        if new_ops.is_empty() {
            return Ok(None);
        }

        let builder = ShardBuilder::from_entries(new_ops);
        if builder.entries.is_empty() {
            return Ok(None);
        }

        let (segment_id, file_name) = {
            // Ensure config is normalized before accessing shards
            let mut config = self.config.write().unwrap();
            config.normalize();

            // Verify normalization succeeded - this should never fail, but defensive check
            if config.shards.len() != DID_SHARD_COUNT {
                log::error!(
                    "[DID Index] Config normalization failed in write_delta_segment: shards.len() = {} (expected {}). Recreating.",
                    config.shards.len(),
                    DID_SHARD_COUNT
                );
                *config = Config::new();
                config.normalize();

                // Verify the new config is valid before proceeding
                if config.shards.len() != DID_SHARD_COUNT {
                    anyhow::bail!(
                        "Failed to create valid config: shards.len() = {} (expected {})",
                        config.shards.len(),
                        DID_SHARD_COUNT
                    );
                }

                // Persist the fixed config immediately
                let config_snapshot = config.clone();
                drop(config); // Release lock before persisting
                self.persist_config(&config_snapshot)?;

                // Re-acquire lock for segment ID
                config = self.config.write().unwrap();
            }

            // Ensure shard_num is within valid range (should always be 0-255 after normalize)
            let shard_meta = config.shards.get_mut(shard_num as usize).ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid shard number: {} (expected 0-{})",
                    shard_num,
                    DID_SHARD_COUNT - 1
                )
            })?;
            let id = shard_meta.next_segment_id;
            shard_meta.next_segment_id += 1;
            (id, format!("seg_{id:016x}.idx"))
        };

        let segment_path = self.segment_path(shard_num, &file_name);
        if let Some(parent) = segment_path.parent() {
            fs::create_dir_all(parent)?;
        }
        self.write_shard_to_path(shard_num, &builder, &segment_path, "delta")?;

        let location_count = builder.total_locations() as u32;
        let meta = DeltaSegmentMeta {
            id: segment_id,
            file_name: file_name.clone(),
            bundle_start: bundle_num,
            bundle_end: bundle_num,
            did_count: builder.entries.len() as u32,
            location_count,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        self.modify_config(|cfg| {
            // Ensure config is normalized before accessing shards
            cfg.normalize();
            if let Some(shard_meta) = cfg.shards.get_mut(shard_num as usize) {
                shard_meta.segments.push(meta.clone());
            }
            cfg.delta_segments_total += 1;
            cfg.last_bundle = bundle_num as i32;
        })?;

        self.invalidate_shard_cache(shard_num);

        Ok(Some(meta))
    }

    fn write_shard(&self, shard_num: u8, builder: &ShardBuilder) -> Result<()> {
        let target = self.shard_path(shard_num);
        self.write_shard_to_path(shard_num, builder, &target, "base")
    }

    fn write_shard_to_path(
        &self,
        shard_num: u8,
        builder: &ShardBuilder,
        target: &Path,
        label: &str,
    ) -> Result<()> {
        self.write_shard_to_path_with_bytes(shard_num, builder, target, label, None)
    }

    fn write_shard_to_path_with_bytes(
        &self,
        shard_num: u8,
        builder: &ShardBuilder,
        target: &Path,
        label: &str,
        identifier_bytes: Option<&HashMap<String, [u8; DID_IDENTIFIER_LEN]>>,
    ) -> Result<()> {
        use std::time::Instant;

        let start = Instant::now();

        // Ensure shard directory exists
        if let Some(parent) = target.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        if builder.entries.is_empty() {
            fs::write(target, &[])?;
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

        // Prepare temp file + mmap for zero-copy writes
        let total_size = current_offset;
        let temp_path = target
            .file_name()
            .map(|name| target.with_file_name(format!("{}.tmp", name.to_string_lossy())))
            .unwrap_or_else(|| target.with_extension("tmp"));

        if temp_path.exists() {
            fs::remove_file(&temp_path).ok();
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)
            .with_context(|| format!("Failed to create shard temp file {}", temp_path.display()))?;
        file.set_len(total_size as u64)
            .with_context(|| format!("Failed to resize shard temp file {}", temp_path.display()))?;

        let mut mmap = unsafe {
            MmapMut::map_mut(&file).with_context(|| {
                format!("Failed to mmap shard temp file {}", temp_path.display())
            })?
        };

        // Header (32 bytes) + prefix + offsets + entries
        {
            let buf = &mut mmap[..];

            // Header
            buf[0..4].copy_from_slice(DIDINDEX_MAGIC);
            buf[4..8].copy_from_slice(&DIDINDEX_VERSION.to_le_bytes());
            buf[8] = shard_num;
            buf[9..13].copy_from_slice(&(identifiers.len() as u32).to_le_bytes());
            // Remaining header bytes already zeroed by truncate/set_len

            let mut cursor = 32;

            // Prefix index (1024 bytes)
            for idx in prefix_index.iter() {
                buf[cursor..cursor + 4].copy_from_slice(&idx.to_le_bytes());
                cursor += 4;
            }

            // Offset table
            for offset in &offsets {
                buf[cursor..cursor + 4].copy_from_slice(&(*offset as u32).to_le_bytes());
                cursor += 4;
            }

            // Entries
            for id in identifiers {
                // Use raw bytes if available, otherwise normalize from string
                let id_bytes = if let Some(bytes_map) = identifier_bytes {
                    bytes_map.get(id).copied().unwrap_or_else(|| normalize_identifier_bytes(id))
                } else {
                    normalize_identifier_bytes(id)
                };
                buf[cursor..cursor + DID_IDENTIFIER_LEN].copy_from_slice(&id_bytes);
                cursor += DID_IDENTIFIER_LEN;

                let locations = &builder.entries[id];
                let loc_count = locations.len();
                buf[cursor..cursor + 2].copy_from_slice(&(loc_count as u16).to_le_bytes());
                cursor += 2;

                for loc in locations {
                    buf[cursor..cursor + 4].copy_from_slice(&loc.as_u32().to_le_bytes());
                    cursor += 4;
                }
            }

            debug_assert_eq!(cursor, total_size, "Shard serialization math mismatch");
        }

        // Explicitly flush to ensure OS sees the writes before the mmap is dropped
        mmap.flush()
            .with_context(|| format!("Failed to flush shard temp file {}", temp_path.display()))?;
        drop(mmap);
        drop(file);

        fs::rename(&temp_path, target)
            .with_context(|| format!("Failed to replace shard file {}", target.display()))?;

        let total_duration = start.elapsed();
        let write_duration =
            total_duration.saturating_sub(sort_duration + prefix_duration + offset_duration);

        log::debug!(
            "[DID Index] Write {label} shard {:02x}: {} DIDs, {} locations, {} bytes in {:.3}ms (sort={:.1}ms, prefix={:.1}ms, offsets={:.1}ms, mmap+disk={:.1}ms)",
            shard_num,
            builder.entries.len(),
            total_locations,
            total_size,
            total_duration.as_secs_f64() * 1000.0,
            sort_duration.as_secs_f64() * 1000.0,
            prefix_duration.as_secs_f64() * 1000.0,
            offset_duration.as_secs_f64() * 1000.0,
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

            let identifier =
                String::from_utf8_lossy(&data[entry_offset..entry_offset + DID_IDENTIFIER_LEN])
                    .to_string();
            entry_offset += DID_IDENTIFIER_LEN;

            let loc_count =
                u16::from_le_bytes([data[entry_offset], data[entry_offset + 1]]) as usize;
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

    fn modify_config<F>(&self, mutator: F) -> Result<()>
    where
        F: FnOnce(&mut Config),
    {
        let mut config = self.config.write().unwrap();
        mutator(&mut config);
        config.updated_at = chrono::Utc::now().to_rfc3339();
        self.persist_config(&config)
    }

    fn persist_config(&self, config: &Config) -> Result<()> {
        fs::create_dir_all(&self.index_dir)?;
        let json = serde_json::to_string_pretty(config)?;

        // Atomic write: write to temp file first, then rename
        // This ensures the config file is never partially written, even if process is killed
        let temp_path = self.config_path.with_extension("json.tmp");
        fs::write(&temp_path, json)
            .with_context(|| format!("Failed to write temp config to: {}", temp_path.display()))?;

        // Atomic rename - this is guaranteed to be atomic on most filesystems
        fs::rename(&temp_path, &self.config_path).with_context(|| {
            format!(
                "Failed to rename temp config to: {}",
                self.config_path.display()
            )
        })?;

        Ok(())
    }

    fn invalidate_shard_cache(&self, shard_num: u8) {
        self.shard_cache.write().unwrap().remove(&shard_num);
    }
    
    /// Clean up temporary files from interrupted builds
    pub fn cleanup_temp_files(&self) -> Result<()> {
        if !self.shard_dir.exists() {
            return Ok(());
        }
        let mut cleaned = 0;
        for entry in fs::read_dir(&self.shard_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "tmp" {
                    fs::remove_file(&path)?;
                    cleaned += 1;
                }
            }
        }
        if cleaned > 0 {
            log::info!("[DID Index] Cleaned up {} leftover temp files from previous build", cleaned);
        }
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
        anyhow::bail!("Invalid DID identifier length: expected {} bytes, got {} bytes", DID_IDENTIFIER_LEN, identifier.len());
    }

    Ok(identifier.to_string())
}

/// Calculate shard number directly from DID string without allocating identifier String
#[inline]
fn calculate_shard_from_did(did: &str) -> Option<u8> {
    if !did.starts_with(DID_PREFIX) {
        return None;
    }
    
    let identifier_start = DID_PREFIX.len();
    let identifier_end = identifier_start + DID_IDENTIFIER_LEN;
    
    if did.len() < identifier_end {
        return None;
    }
    
    let identifier_bytes = &did.as_bytes()[identifier_start..identifier_end];
    
    use fnv::FnvHasher;
    use std::hash::Hasher;
    
    let mut hasher = FnvHasher::default();
    hasher.write(identifier_bytes);
    let hash = hasher.finish() as u32;
    Some((hash % DID_SHARD_COUNT as u32) as u8)
}

/// Normalize an identifier to exactly DID_IDENTIFIER_LEN bytes.
/// Truncates if longer, pads with null bytes if shorter.
fn normalize_identifier_bytes(identifier: &str) -> [u8; DID_IDENTIFIER_LEN] {
    let mut result = [0u8; DID_IDENTIFIER_LEN];
    let id_bytes = identifier.as_bytes();
    let len = id_bytes.len().min(DID_IDENTIFIER_LEN);
    result[..len].copy_from_slice(&id_bytes[..len]);
    result
}

fn entry_count_from_data(data: &[u8]) -> usize {
    if data.len() < 1056 {
        return 0;
    }
    u32::from_le_bytes([data[9], data[10], data[11], data[12]]) as usize
}

fn read_identifier_at(data: &[u8], index: usize) -> Option<String> {
    if data.len() < 1056 {
        return None;
    }

    let entry_count = entry_count_from_data(data);
    if index >= entry_count {
        return None;
    }

    let offset_table_start = 1056;
    let offset_pos = offset_table_start + (index * 4);
    if offset_pos + 4 > data.len() {
        return None;
    }

    let entry_offset = u32::from_le_bytes([
        data[offset_pos],
        data[offset_pos + 1],
        data[offset_pos + 2],
        data[offset_pos + 3],
    ]) as usize;

    if entry_offset + DID_IDENTIFIER_LEN > data.len() {
        return None;
    }

    std::str::from_utf8(&data[entry_offset..entry_offset + DID_IDENTIFIER_LEN])
        .ok()
        .map(|id| id.to_string())
}

fn select_shard_by_weight(shards: &[(u8, u32)], mut value: u64) -> Option<(u8, u32)> {
    for (shard, weight) in shards {
        if *weight == 0 {
            continue;
        }

        if value < *weight as u64 {
            return Some((*shard, value as u32));
        }

        value -= *weight as u64;
    }

    None
}

fn deterministic_u64(seed: u64, counter: usize) -> u64 {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    counter.hash(&mut hasher);
    hasher.finish()
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_did() -> &'static str {
        "did:plc:abcdefghijklmnopqrstuvwx"
    }

    #[test]
    fn delta_segments_round_trip_and_compact() {
        let tmp = TempDir::new().expect("temp dir");
        let manager = Manager::new(tmp.path().to_path_buf()).expect("manager");

        let did = sample_did();
        manager
            .update_for_bundle(1, vec![(did.to_string(), false)])
            .expect("bundle update");

        let locations = manager.get_did_locations(did).expect("lookup");
        assert_eq!(locations.len(), 1);
        assert_eq!(locations[0].bundle(), 1);

        {
            let stats = manager.get_stats();
            let delta_segments = stats
                .get("delta_segments")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            assert!(
                delta_segments > 0,
                "expected pending delta segments, got {}",
                delta_segments
            );
        }

        let shard = manager.calculate_shard(&extract_identifier(did).unwrap());

        manager
            .compact_pending_segments(Some(vec![shard]))
            .expect("compact");

        {
            let stats = manager.get_stats();
            let delta_segments = stats
                .get("delta_segments")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            assert_eq!(
                delta_segments, 0,
                "expected no delta segments after compaction"
            );
        }

        let after = manager.get_did_locations(did).expect("lookup after");
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].bundle(), 1);
    }
}
