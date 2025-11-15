// src/manager.rs
use crate::constants;
use crate::index::{Index, BundleMetadata};
use crate::operations::{Operation, OperationFilter, OperationRequest, OperationWithLocation};
use crate::iterators::{QueryIterator, ExportIterator, RangeIterator};
use crate::options::QueryMode;
use crate::{cache, did_index, verification, mempool, handle_resolver};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::io::Write;
use chrono::{DateTime, Utc};

/// Result of a sync_next_bundle operation
#[derive(Debug, Clone)]
pub enum SyncResult {
    /// Successfully created a bundle
    BundleCreated {
        bundle_num: u32,
        mempool_count: usize,
        duration_ms: u64,
        fetch_duration_ms: u64,
        bundle_save_ms: u64,
        index_ms: u64,
        fetch_requests: usize,
        hash: String,
        age: String,
        did_index_compacted: bool,
        unique_dids: u32,
        size_bytes: u64,
    },
    /// Caught up to latest PLC data, mempool has partial operations
    CaughtUp {
        next_bundle: u32,
        mempool_count: usize,
        new_ops: usize,
        fetch_duration_ms: u64,
    },
}

pub struct BundleManager {
    directory: PathBuf,
    index: Arc<RwLock<Index>>,
    cache: Arc<cache::BundleCache>,
    did_index: Arc<RwLock<did_index::Manager>>,
    stats: Arc<RwLock<ManagerStats>>,
    mempool: Arc<RwLock<Option<mempool::Mempool>>>,
    handle_resolver: Option<Arc<handle_resolver::HandleResolver>>,
    verbose: Arc<Mutex<bool>>,
}

#[derive(Debug, Clone, Default)]
pub struct ManagerStats {
    pub bundles_loaded: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub operations_read: u64,
    pub queries_executed: u64,
}

#[derive(Debug, Clone)]
pub struct ResolveResult {
    pub document: crate::resolver::DIDDocument,
    pub bundle_number: u32,
    pub position: usize,
    pub index_time: std::time::Duration,
    pub load_time: std::time::Duration,
    pub total_time: std::time::Duration,
    pub locations_found: usize,
    pub shard_num: u8,
    pub shard_stats: Option<did_index::DIDLookupStats>,
    pub lookup_timings: Option<did_index::DIDLookupTimings>,
}

#[derive(Debug, Clone, Default)]
pub struct DIDIndexStats {
    pub total_dids: usize,
    pub total_entries: usize,
    pub avg_operations_per_did: f64,
}

#[derive(Debug, Clone)]
pub struct RollbackFileStats {
    pub deleted: usize,
    pub failed: usize,
    pub deleted_size: u64,
}

impl BundleManager {
    pub fn new(directory: PathBuf) -> Result<Self> {
        Self::with_handle_resolver(directory, None)
    }

    pub fn with_handle_resolver(directory: PathBuf, handle_resolver_url: Option<String>) -> Result<Self> {
        let index = Index::load(&directory)?;
        let did_index = did_index::Manager::new(directory.clone())?;

        let handle_resolver = handle_resolver_url
            .map(|url| Arc::new(handle_resolver::HandleResolver::new(url)));

        Ok(Self {
            directory: directory.clone(),
            index: Arc::new(RwLock::new(index)),
            cache: Arc::new(cache::BundleCache::new(100)),
            did_index: Arc::new(RwLock::new(did_index)),
            stats: Arc::new(RwLock::new(ManagerStats::default())),
            mempool: Arc::new(RwLock::new(None)),
            handle_resolver,
            verbose: Arc::new(Mutex::new(false)),
        })
    }

    pub fn with_verbose(self, verbose: bool) -> Self {
        *self.verbose.lock().unwrap() = verbose;
        self
    }

    /// Get a clone of the verbose state Arc for external access
    pub fn verbose_handle(&self) -> Arc<Mutex<bool>> {
        self.verbose.clone()
    }

    // === Smart Loading ===
    pub fn load_bundle(&self, num: u32, options: LoadOptions) -> Result<LoadResult> {
        self.stats.write().unwrap().bundles_loaded += 1;
        
        if let Some(cached) = self.cache.get(num) {
            self.stats.write().unwrap().cache_hits += 1;
            return Ok(self.filter_load_result(cached, &options));
        }
        
        self.stats.write().unwrap().cache_misses += 1;
        
        let bundle_path = self.directory.join(format!("{:06}.jsonl.zst", num));
        let operations = self.load_bundle_from_disk(&bundle_path)?;
        
        if options.cache {
            self.cache.insert(num, operations.clone());
        }
        
        Ok(self.filter_load_result(operations, &options))
    }

    // === Single Operation Access ===
    
    /// Get a single operation as raw JSON (fastest, preserves field order)
    /// 
    /// This method uses frame-based access for efficient random reads.
    /// Falls back to legacy sequential scan if no frame index is available.
    pub fn get_operation_raw(&self, bundle_num: u32, position: usize) -> Result<String> {
        let bundle_path = self.directory.join(format!("{:06}.jsonl.zst", bundle_num));
        
        if !bundle_path.exists() {
            anyhow::bail!("Bundle {} not found", bundle_num);
        }
        
        // Try frame-based access first (new format)
        match self.get_operation_raw_with_frames(&bundle_path, position) {
            Ok(json) => return Ok(json),
            Err(e) => {
                // Fall back to legacy sequential scan
                // This happens for old bundles without frame index
                if let Ok(json) = self.get_operation_raw_legacy(&bundle_path, position) {
                    return Ok(json);
                }
                return Err(e);
            }
        }
    }
    
    /// Frame-based operation access (new format with metadata)
    fn get_operation_raw_with_frames(&self, bundle_path: &std::path::Path, position: usize) -> Result<String> {
        use crate::bundle_format;
        use std::io::{Read, Seek, SeekFrom};
        
        // Open file and read actual metadata frame size
        let mut file = std::fs::File::open(bundle_path)?;
        
        // Read magic (4 bytes)
        let mut magic_buf = [0u8; 4];
        file.read_exact(&mut magic_buf)?;
        let magic = u32::from_le_bytes(magic_buf);
        
        if magic != bundle_format::SKIPPABLE_MAGIC_METADATA {
            anyhow::bail!("No metadata frame at start of bundle");
        }
        
        // Read frame size (4 bytes)
        let mut size_buf = [0u8; 4];
        file.read_exact(&mut size_buf)?;
        let frame_data_size = u32::from_le_bytes(size_buf) as i64;
        
        // Metadata frame total size = magic(4) + size(4) + data
        let metadata_frame_size = 8 + frame_data_size;
        
        // Read the actual metadata
        let mut metadata_data = vec![0u8; frame_data_size as usize];
        file.read_exact(&mut metadata_data)?;
        let metadata: bundle_format::BundleMetadata = serde_json::from_slice(&metadata_data)?;
        
        if metadata.frame_offsets.is_empty() {
            anyhow::bail!("No frame offsets in metadata");
        }
        
        // Now seek back to start and use the frame-based loader
        file.seek(SeekFrom::Start(0))?;
        bundle_format::load_operation_at_position(
            &mut file,
            position,
            &metadata.frame_offsets,
            metadata_frame_size,
        )
    }
    
    /// Legacy sequential scan (for old bundles without frame index)
    fn get_operation_raw_legacy(&self, bundle_path: &std::path::Path, position: usize) -> Result<String> {
        let file = std::fs::File::open(bundle_path)?;
        let decoder = zstd::Decoder::new(file)?;
        let reader = std::io::BufReader::new(decoder);
        
        use std::io::BufRead;
        
        for (idx, line_result) in reader.lines().enumerate() {
            if idx == position {
                return Ok(line_result?);
            }
        }
        
        anyhow::bail!("Operation position {} out of bounds", position)
    }
    
    /// Get a single operation as parsed struct
    /// 
    /// This method retrieves the raw JSON and parses it into an Operation struct.
    /// Use `get_operation_raw()` if you only need the JSON.
    pub fn get_operation(&self, bundle_num: u32, position: usize) -> Result<Operation> {
        let json = self.get_operation_raw(bundle_num, position)?;
        let op: Operation = serde_json::from_str(&json)?;
        Ok(op)
    }
    
    /// Get operation with timing statistics (for CLI verbose mode)
    pub fn get_operation_with_stats(&self, bundle_num: u32, position: usize) -> Result<OperationResult> {
        let start = std::time::Instant::now();
        let json = self.get_operation_raw(bundle_num, position)?;
        let duration = start.elapsed();
        
        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.operations_read += 1;
        }
        
        Ok(OperationResult {
            raw_json: json.clone(),
            size_bytes: json.len(),
            load_duration: duration,
        })
    }

    // === Batch Operations ===
    pub fn get_operations_batch(&self, requests: Vec<OperationRequest>) -> Result<Vec<Operation>> {
        let mut results = Vec::new();
        
        let mut by_bundle: HashMap<u32, Vec<&OperationRequest>> = HashMap::new();
        for req in &requests {
            by_bundle.entry(req.bundle).or_default().push(req);
        }
        
        for (bundle_num, reqs) in by_bundle {
            let load_result = self.load_bundle(bundle_num, LoadOptions::default())?;
            
            for req in reqs {
                for op in &load_result.operations {
                    if self.matches_request(op, req) {
                        results.push(op.clone());
                    }
                }
            }
        }
        
        Ok(results)
    }

    pub fn get_operations_range(
        &self,
        start: u32,
        end: u32,
        filter: Option<OperationFilter>,
    ) -> RangeIterator {
        RangeIterator::new(Arc::new(self.clone_for_arc()), start, end, filter)
    }

    // === DID Operations ===
    pub fn get_did_operations(&self, did: &str) -> Result<Vec<Operation>> {
        let did_index = self.did_index.read().unwrap();
        let bundle_refs = did_index.get_bundles_for_did(did)?;
        
        let mut operations = Vec::new();
        for bundle_num in bundle_refs {
            let result = self.load_bundle(bundle_num, LoadOptions {
                filter: Some(OperationFilter {
                    did: Some(did.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            })?;
            operations.extend(result.operations);
        }
        
        Ok(operations)
    }

    /// Get DID operations with location information (bundle number and position)
    pub fn get_did_operations_with_locations(&self, did: &str) -> Result<Vec<OperationWithLocation>> {
        let did_index = self.did_index.read().unwrap();
        let locations = did_index.get_did_locations(did)?;
        
        let mut ops_with_loc = Vec::new();
        for loc in locations {
            let bundle_num = loc.bundle() as u32;
            let position = loc.position() as usize;
            
            match self.get_operation(bundle_num, position) {
                Ok(op) => {
                    ops_with_loc.push(OperationWithLocation {
                        operation: op,
                        bundle: bundle_num,
                        position,
                        nullified: loc.nullified(),
                    });
                }
                Err(e) => {
                    log::warn!("Failed to load operation at bundle {} position {}: {}", bundle_num, position, e);
                }
            }
        }
        
        // Sort by global position (bundle * BUNDLE_SIZE + position)
        ops_with_loc.sort_by_key(|owl| {
            (owl.bundle as u64) * constants::BUNDLE_SIZE as u64 + owl.position as u64
        });
        
        Ok(ops_with_loc)
    }

    /// Get DID operations with locations and detailed statistics (for verbose logging)
    pub fn get_did_operations_with_locations_and_stats(&self, did: &str) -> Result<(Vec<OperationWithLocation>, did_index::DIDLookupStats, u8, did_index::DIDLookupTimings, std::time::Duration)> {
        use std::time::Instant;
        
        let index_start = Instant::now();
        let did_index = self.did_index.read().unwrap();
        let (locations, shard_stats, shard_num, lookup_timings) = did_index.get_did_locations_with_stats(did)?;
        let _index_time = index_start.elapsed();
        
        let mut ops_with_loc = Vec::new();
        let load_start = Instant::now();
        for loc in locations {
            let bundle_num = loc.bundle() as u32;
            let position = loc.position() as usize;
            
            match self.get_operation(bundle_num, position) {
                Ok(op) => {
                    ops_with_loc.push(OperationWithLocation {
                        operation: op,
                        bundle: bundle_num,
                        position,
                        nullified: loc.nullified(),
                    });
                }
                Err(e) => {
                    log::warn!("Failed to load operation at bundle {} position {}: {}", bundle_num, position, e);
                }
            }
        }
        let load_time = load_start.elapsed();
        
        // Sort by global position (bundle * BUNDLE_SIZE + position)
        ops_with_loc.sort_by_key(|owl| {
            (owl.bundle as u64) * constants::BUNDLE_SIZE as u64 + owl.position as u64
        });
        
        Ok((ops_with_loc, shard_stats, shard_num, lookup_timings, load_time))
    }

    /// Get DID operations from mempool
    pub fn get_did_operations_from_mempool(&self, did: &str) -> Result<Vec<Operation>> {
        let mempool_guard = self.mempool.read().unwrap();
        match mempool_guard.as_ref() {
            Some(mp) => Ok(mp.find_did_operations(did)),
            None => Ok(Vec::new()),
        }
    }

    /// Resolve DID to current W3C DID Document
    pub fn resolve_did(&self, did: &str) -> Result<crate::resolver::DIDDocument> {
        let result = self.resolve_did_with_stats(did)?;
        Ok(result.document)
    }

    /// Resolve DID with detailed timing statistics
    pub fn resolve_did_with_stats(&self, did: &str) -> Result<ResolveResult> {
        use std::time::Instant;

        let total_start = Instant::now();

        // Validate DID format
        crate::resolver::validate_did_format(did)?;

        // Get all operations for this DID with timing
        let index_start = Instant::now();
        let did_index = self.did_index.read().unwrap();
        let (locations, shard_stats, shard_num, lookup_timings) = did_index.get_did_locations_with_stats(did)?;
        let index_time = index_start.elapsed();

        if locations.is_empty() {
            anyhow::bail!("DID not found: {}", did);
        }

        // Find latest non-nullified operation
        let latest = locations
            .iter()
            .filter(|loc| !loc.nullified())
            .max_by_key(|loc| loc.global_position())
            .ok_or_else(|| anyhow::anyhow!("All operations nullified"))?;

        // Load the operation
        let load_start = Instant::now();
        let operation = self.get_operation(latest.bundle() as u32, latest.position() as usize)?;
        let load_time = load_start.elapsed();

        // Build document
        let document = crate::resolver::resolve_did_document(did, &[operation])?;

        Ok(ResolveResult {
            document,
            bundle_number: latest.bundle() as u32,
            position: latest.position() as usize,
            index_time,
            load_time,
            total_time: total_start.elapsed(),
            locations_found: locations.len(),
            shard_num,
            shard_stats: Some(shard_stats),
            lookup_timings: Some(lookup_timings),
        })
    }

    pub fn batch_resolve_dids(&self, dids: Vec<String>) -> Result<HashMap<String, Vec<Operation>>> {
        let mut results = HashMap::new();
        
        for did in dids {
            let ops = self.get_did_operations(&did)?;
            results.insert(did, ops);
        }
        
        Ok(results)
    }

    // === Query/Export ===
    pub fn query(&self, spec: QuerySpec) -> QueryIterator {
        self.stats.write().unwrap().queries_executed += 1;
        QueryIterator::new(Arc::new(self.clone_for_arc()), spec)
    }

    pub fn export(&self, spec: ExportSpec) -> ExportIterator {
        ExportIterator::new(Arc::new(self.clone_for_arc()), spec)
    }

    pub fn export_to_writer<F>(&self, spec: ExportSpec, mut writer_fn: F) -> Result<ExportStats>
    where
        F: FnMut() -> Box<dyn Write>,
    {
        let mut writer = writer_fn();
        let mut stats = ExportStats::default();
        
        for item in self.export(spec) {
            let data = item?;
            writer.write_all(data.as_bytes())?;
            writer.write_all(b"\n")?;
            stats.records_written += 1;
            stats.bytes_written += data.len() as u64 + 1;
        }
        
        Ok(stats)
    }

    // === Verification ===
    pub fn verify_bundle(&self, num: u32, spec: VerifySpec) -> Result<VerifyResult> {
        let index = self.index.read().unwrap();
        let metadata = index.get_bundle(num)
            .ok_or_else(|| anyhow::anyhow!("Bundle {} not in index", num))?;
        
        verification::verify_bundle(&self.directory, metadata, spec)
    }

    pub fn verify_chain(&self, spec: ChainVerifySpec) -> Result<ChainVerifyResult> {
        verification::verify_chain(&self.directory, &self.index.read().unwrap(), spec)
    }

    // === Multi-info ===
    pub fn get_bundle_info(&self, num: u32, flags: InfoFlags) -> Result<BundleInfo> {
        let index = self.index.read().unwrap();
        let metadata = index.get_bundle(num)
            .ok_or_else(|| anyhow::anyhow!("Bundle {} not found", num))?;
        
        let mut info = BundleInfo {
            metadata: metadata.clone(),
            exists: self.directory.join(format!("{:06}.jsonl.zst", num)).exists(),
            cached: self.cache.contains(num),
            operations: None,
            size_info: None,
        };
        
        if flags.include_operations {
            let result = self.load_bundle(num, LoadOptions::default())?;
            info.operations = Some(result.operations);
        }
        
        if flags.include_size_info {
            info.size_info = Some(SizeInfo {
                compressed: metadata.compressed_size,
                uncompressed: metadata.uncompressed_size,
            });
        }
        
        Ok(info)
    }

    // === Rollback ===
    pub fn rollback_plan(&self, spec: RollbackSpec) -> Result<RollbackPlan> {
        let affected_bundles: Vec<u32> = (spec.target_bundle..=self.get_last_bundle())
            .collect();
        
        let mut affected_operations = 0;
        let mut affected_dids = std::collections::HashSet::new();
        
        for bundle_num in &affected_bundles {
            if let Ok(result) = self.load_bundle(*bundle_num, LoadOptions::default()) {
                affected_operations += result.operations.len();
                for op in result.operations {
                    affected_dids.insert(op.did);
                }
            }
        }
        
        Ok(RollbackPlan {
            target_bundle: spec.target_bundle,
            affected_bundles: affected_bundles.clone(),  // Clone here
            affected_operations,
            affected_dids: affected_dids.len(),
            estimated_time_ms: affected_bundles.len() as u64 * 10,
        })
    }

    pub fn rollback(&self, spec: RollbackSpec) -> Result<RollbackResult> {
        let plan = self.rollback_plan(spec.clone())?;
        
        if spec.dry_run {
            return Ok(RollbackResult {
                success: true,
                bundles_removed: 0,
                plan: Some(plan),
            });
        }
        
        for bundle_num in &plan.affected_bundles {
            let path = self.directory.join(format!("{:06}.jsonl.zst", bundle_num));
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            self.cache.remove(*bundle_num);
        }
        
        let mut index = self.index.write().unwrap();
        index.last_bundle = spec.target_bundle;
        index.bundles.retain(|b| b.bundle_number <= spec.target_bundle);
        
        self.rebuild_did_index(None::<fn(u32, u32)>)?;
        
        Ok(RollbackResult {
            success: true,
            bundles_removed: plan.affected_bundles.len(),
            plan: Some(plan),
        })
    }

    // === Cache Hints ===
    pub fn prefetch_bundles(&self, nums: Vec<u32>) -> Result<()> {
        for num in nums {
            self.load_bundle(num, LoadOptions {
                cache: true,
                ..Default::default()
            })?;
        }
        Ok(())
    }

    pub fn warm_up(&self, spec: WarmUpSpec) -> Result<()> {
        let bundles: Vec<u32> = match spec.strategy {
            WarmUpStrategy::Recent(n) => {
                let last = self.get_last_bundle();
                (last.saturating_sub(n - 1)..=last).collect()
            }
            WarmUpStrategy::Range(start, end) => (start..=end).collect(),
            WarmUpStrategy::All => (1..=self.get_last_bundle()).collect(),
        };
        
        self.prefetch_bundles(bundles)
    }

    // === DID Index ===
    pub fn rebuild_did_index<F>(&self, progress_cb: Option<F>) -> Result<RebuildStats>
    where
        F: Fn(u32, u32) + Send + Sync,
    {
        let last_bundle = self.get_last_bundle();
        let new_index = did_index::Manager::new(self.directory.clone())?;
        let mut stats = RebuildStats::default();
        
        // Collect all bundles with their operations
        let mut bundles_data = Vec::new();
        for bundle_num in 1..=last_bundle {
            if let Some(ref cb) = progress_cb {
                cb(bundle_num, last_bundle);
            }
            
            if let Ok(result) = self.load_bundle(bundle_num, LoadOptions::default()) {
                let operations: Vec<(String, bool)> = result.operations
                    .iter()
                    .map(|op| (op.did.clone(), op.nullified))
                    .collect();
                
                stats.operations_indexed += operations.len() as u64;
                stats.bundles_processed += 1;
                
                bundles_data.push((bundle_num, operations));
            }
        }
        
        // Build index from scratch
        new_index.build_from_scratch(bundles_data, |current, total| {
            if let Some(ref cb) = progress_cb {
                cb(current as u32, total as u32);
            }
        })?;
        
        *self.did_index.write().unwrap() = new_index;
        
        Ok(stats)
    }

    pub fn get_did_index_stats(&self) -> HashMap<String, serde_json::Value> {
        self.did_index.read().unwrap().get_stats()
    }
    
    /// Get DID index stats as struct (legacy format)
    pub fn get_did_index_stats_struct(&self) -> DIDIndexStats {
        let stats_map = self.get_did_index_stats();
        
        // Convert to old format
        DIDIndexStats {
            total_dids: stats_map.get("total_dids")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as usize,
            total_entries: 0, // Not tracked in new version
            avg_operations_per_did: 0.0, // Not tracked in new version
        }
    }

    pub fn get_did_index(&self) -> Arc<RwLock<did_index::Manager>> {
        Arc::clone(&self.did_index)
    }

    // === Observability ===
    pub fn get_stats(&self) -> ManagerStats {
        self.stats.read().unwrap().clone()
    }

    pub fn clear_caches(&self) {
        self.cache.clear();
        self.stats.write().unwrap().cache_hits = 0;
        self.stats.write().unwrap().cache_misses = 0;
    }

    // === Mempool Management ===

    /// Initialize or get existing mempool for the next bundle
    pub fn get_mempool(&self) -> Result<()> {
        let mut mempool_guard = self.mempool.write().unwrap();

        if mempool_guard.is_none() {
            let last_bundle = self.get_last_bundle();
            let target_bundle = last_bundle + 1;

            // Get min timestamp from last bundle's last operation
            let min_timestamp = self.get_last_bundle_timestamp()?;

            let mp = mempool::Mempool::new(
                &self.directory,
                target_bundle,
                min_timestamp,
                *self.verbose.lock().unwrap(),
            )?;

            *mempool_guard = Some(mp);
        }

        Ok(())
    }

    /// Get mempool statistics
    pub fn get_mempool_stats(&self) -> Result<mempool::MempoolStats> {
        let mempool_guard = self.mempool.read().unwrap();

        match mempool_guard.as_ref() {
            Some(mp) => Ok(mp.stats()),
            None => {
                // Return empty stats if no mempool
                let last_bundle = self.get_last_bundle();
                let min_timestamp = self.get_last_bundle_timestamp()?;
                Ok(mempool::MempoolStats {
                    count: 0,
                    can_create_bundle: false,
                    target_bundle: last_bundle + 1,
                    min_timestamp,
                    validated: false,
                    first_time: None,
                    last_time: None,
                    size_bytes: None,
                    did_count: None,
                })
            }
        }
    }

    /// Get all mempool operations
    pub fn get_mempool_operations(&self) -> Result<Vec<Operation>> {
        let mempool_guard = self.mempool.read().unwrap();

        match mempool_guard.as_ref() {
            Some(mp) => Ok(mp.get_operations().to_vec()),
            None => Ok(Vec::new()),
        }
    }

    /// Clear mempool
    pub fn clear_mempool(&self) -> Result<()> {
        let mut mempool_guard = self.mempool.write().unwrap();

        if let Some(mp) = mempool_guard.as_mut() {
            mp.clear();
            mp.save()?;
        }
        
        // Also delete all mempool files to prevent stale data from previous bundles
        if let Ok(entries) = std::fs::read_dir(&self.directory) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with(constants::MEMPOOL_FILE_PREFIX) && name.ends_with(".jsonl") {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }

        Ok(())
    }

    /// Add operations to mempool
    pub fn add_to_mempool(&self, ops: Vec<Operation>) -> Result<usize> {
        self.get_mempool()?;

        let mut mempool_guard = self.mempool.write().unwrap();

        if let Some(mp) = mempool_guard.as_mut() {
            let added = mp.add(ops)?;
            mp.save_if_needed()?;
            Ok(added)
        } else {
            anyhow::bail!("Mempool not initialized")
        }
    }

    /// Get the last bundle's last operation timestamp
    fn get_last_bundle_timestamp(&self) -> Result<DateTime<Utc>> {
        let last_bundle = self.get_last_bundle();

        if last_bundle == 0 {
            // No bundles yet, use epoch
            return Ok(DateTime::from_timestamp(0, 0).unwrap());
        }

        // Load last bundle and get last operation's timestamp
        let result = self.load_bundle(last_bundle, LoadOptions::default())?;

        if let Some(last_op) = result.operations.last() {
            let timestamp = DateTime::parse_from_rfc3339(&last_op.created_at)?
                .with_timezone(&Utc);
            Ok(timestamp)
        } else {
            // Bundle is empty (shouldn't happen), use epoch
            Ok(DateTime::from_timestamp(0, 0).unwrap())
        }
    }

    // === Sync Operations ===

    /// Validate and clean repository state before sync
    fn validate_sync_state(&self) -> Result<()> {
        let last_bundle = self.get_last_bundle();
        let next_bundle_num = last_bundle + 1;
        
        // Check for and delete mempool files for already-completed bundles
        let mut found_stale_files = false;
        if let Ok(entries) = std::fs::read_dir(&self.directory) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with(constants::MEMPOOL_FILE_PREFIX) && name.ends_with(".jsonl") {
                        // Extract bundle number from filename: plc_mempool_NNNNNN.jsonl
                        if let Some(num_str) = name.strip_prefix(constants::MEMPOOL_FILE_PREFIX).and_then(|s| s.strip_suffix(".jsonl")) {
                            if let Ok(bundle_num) = num_str.parse::<u32>() {
                                // Delete mempool files for completed bundles or way future bundles
                                if bundle_num <= last_bundle || bundle_num > next_bundle_num {
                                    log::warn!("Removing stale mempool file for bundle {:06}", bundle_num);
                                    let _ = std::fs::remove_file(entry.path());
                                    found_stale_files = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        if found_stale_files {
            log::info!("Cleaned up stale mempool files");
        }
        
        let mempool_stats = self.get_mempool_stats()?;
        
        if mempool_stats.count == 0 {
            return Ok(()); // Empty mempool is always valid
        }
        
        // Check if mempool operations are for the correct bundle
        let mempool_ops = self.get_mempool_operations()?;
        if mempool_ops.is_empty() {
            return Ok(());
        }
        
        // Get the last operation from the previous bundle
        let last_bundle_time = if next_bundle_num > 1 {
            let last_bundle_result = self.load_bundle(next_bundle_num - 1, LoadOptions::default())?;
            if let Some(last_op) = last_bundle_result.operations.last() {
                chrono::DateTime::parse_from_rfc3339(&last_op.created_at)
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
            } else {
                None
            }
        } else {
            None
        };
        
        // Special case: When creating the first bundle (next_bundle_num == 1, meaning
        // last_bundle == 0, i.e., empty repository), any existing mempool is likely stale
        // from a previous sync attempt. Clear it to start fresh from the beginning.
        if next_bundle_num == 1 && mempool_stats.count > 0 {
            log::warn!("Starting first bundle (empty repository), but mempool has {} operations", mempool_stats.count);
            if let Some(first_time) = mempool_stats.first_time {
                log::warn!("Mempool operations start at: {}", first_time.format("%Y-%m-%d %H:%M:%S"));
            }
            log::warn!("Clearing mempool to start fresh from the beginning...");
            self.clear_mempool()?;
            return Ok(());
        }
        
        // Check if mempool operations are chronologically valid relative to last bundle
        if let Some(last_time) = last_bundle_time {
            if let Some(first_mempool_time) = mempool_stats.first_time {
                // Case 1: Mempool operations are BEFORE the last bundle (definitely stale)
                if first_mempool_time < last_time {
                    log::warn!("Detected stale mempool data (operations before last bundle)");
                    log::warn!("First mempool op: {}, Last bundle op: {}", 
                        first_mempool_time.format("%Y-%m-%d %H:%M:%S"),
                        last_time.format("%Y-%m-%d %H:%M:%S"));
                    log::warn!("Clearing mempool to start fresh...");
                    self.clear_mempool()?;
                    return Ok(());
                }
                
                // Case 2: Mempool operations are slightly after last bundle, but way too close
                // This indicates they're from a previous failed attempt at this bundle
                // BUT: Only clear if the mempool file is old (modified > 1 hour ago)
                // If it's recent, it might be a legitimate resume of a slow sync
                let time_diff = first_mempool_time.signed_duration_since(last_time);
                if time_diff < chrono::Duration::seconds(constants::MIN_BUNDLE_CREATION_INTERVAL_SECS) && mempool_stats.count < constants::BUNDLE_SIZE {
                    // Check mempool file modification time
                    let mempool_filename = format!("{}{:06}.jsonl", constants::MEMPOOL_FILE_PREFIX, next_bundle_num);
                    let mempool_path = self.directory.join(mempool_filename);

                    let is_stale = if let Ok(metadata) = std::fs::metadata(&mempool_path) {
                        if let Ok(modified) = metadata.modified() {
                            let modified_time = std::time::SystemTime::now()
                                .duration_since(modified)
                                .unwrap_or(std::time::Duration::from_secs(0));
                            modified_time > std::time::Duration::from_secs(3600) // 1 hour
                        } else {
                            false // Can't get modified time, assume not stale
                        }
                    } else {
                        false // File doesn't exist, assume not stale
                    };

                    if is_stale {
                        log::warn!("Detected potentially stale mempool data (too close to last bundle timestamp)");
                        log::warn!("Time difference: {}s, Operations: {}/{}",
                            time_diff.num_seconds(), mempool_stats.count, constants::BUNDLE_SIZE);
                        log::warn!("This likely indicates a previous failed sync attempt. Clearing mempool...");
                        self.clear_mempool()?;
                    } else {
                        if *self.verbose.lock().unwrap() {
                            log::debug!("Mempool appears recent, allowing resume despite close timestamp");
                        }
                    }
                    return Ok(());
                }
            }
        }
        
        // Check if mempool has way too many operations (likely from failed previous attempt)
        if mempool_stats.count > constants::BUNDLE_SIZE {
            log::warn!("Mempool has {} operations (expected max {})", 
                mempool_stats.count, constants::BUNDLE_SIZE);
            log::warn!("This indicates a previous sync attempt failed. Clearing mempool...");
            self.clear_mempool()?;
            return Ok(());
        }
        
        Ok(())
    }

    /// Batch update DID index for a range of bundles (for initial sync optimization)
    ///
    /// IMPORTANT: This method performs heavy blocking I/O and should be called from async
    /// contexts using spawn_blocking to avoid freezing the async runtime (and HTTP server).
    pub fn batch_update_did_index(&self, start_bundle: u32, end_bundle: u32) -> Result<()> {
        use std::time::Instant;

        if start_bundle > end_bundle {
            return Ok(());
        }

        let total_start = Instant::now();
        let bundle_count = end_bundle - start_bundle + 1;

        if *self.verbose.lock().unwrap() {
            log::info!("Batch updating DID index for bundles {:06} to {:06}... ({} bundles)",
                start_bundle, end_bundle, bundle_count);
        }

        // Load all bundles
        let load_start = Instant::now();
        let mut bundles_data = Vec::new();
        let mut total_operations = 0usize;

        for bundle_num in start_bundle..=end_bundle {
            if let Ok(result) = self.load_bundle(bundle_num, LoadOptions::default()) {
                total_operations += result.operations.len();
                let operations: Vec<(String, bool)> = result.operations
                    .iter()
                    .map(|op| (op.did.clone(), op.nullified))
                    .collect();
                bundles_data.push((bundle_num, operations));
            }
        }
        let load_duration = load_start.elapsed();

        if bundles_data.is_empty() {
            return Ok(());
        }

        log::debug!(
            "[Batch DID Index] Loaded {} bundles ({} operations) in {:.3}s ({:.0} ops/sec)",
            bundles_data.len(), total_operations,
            load_duration.as_secs_f64(),
            total_operations as f64 / load_duration.as_secs_f64()
        );

        // Update DID index for each bundle
        let update_start = Instant::now();
        for (bundle_num, operations) in bundles_data {
            let _ = self.did_index.write().unwrap().update_for_bundle(bundle_num, operations)?;
        }
        let update_duration = update_start.elapsed();

        let total_duration = total_start.elapsed();

        if *self.verbose.lock().unwrap() {
            log::info!(
                "✓ DID index updated for bundles {:06} to {:06} in {:.3}s (load={:.1}s, update={:.1}s, {:.0} ops/sec overall)",
                start_bundle, end_bundle,
                total_duration.as_secs_f64(),
                load_duration.as_secs_f64(),
                update_duration.as_secs_f64(),
                total_operations as f64 / total_duration.as_secs_f64()
            );
        }

        Ok(())
    }

    /// Async wrapper for batch_update_did_index that runs in a blocking task
    ///
    /// This prevents blocking the async runtime (and HTTP server) during heavy I/O operations.
    pub async fn batch_update_did_index_async(&self, start_bundle: u32, end_bundle: u32) -> Result<()> {
        let manager = self.clone_for_arc();

        tokio::task::spawn_blocking(move || {
            manager.batch_update_did_index(start_bundle, end_bundle)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Batch DID index update task failed: {}", e))?
    }

    /// Fetch and save next bundle from PLC directory
    /// DID index is updated on every bundle (fast with delta segments)
    pub async fn sync_next_bundle(&self, client: &crate::sync::PLCClient) -> Result<SyncResult> {
        use crate::sync::{get_boundary_cids, strip_boundary_duplicates};
        use std::time::Instant;

        // Validate repository state before starting
        self.validate_sync_state()?;

        let next_bundle_num = self.get_last_bundle() + 1;

        // ALWAYS get boundaries from last bundle initially
        let (mut after_time, mut prev_boundary) = if next_bundle_num > 1 {
            let last = self.load_bundle(next_bundle_num - 1, LoadOptions::default())?;
            let boundary = get_boundary_cids(&last.operations);
            let cursor = last.operations.last()
                .map(|op| op.created_at.clone())
                .unwrap_or_default();
            
            if *self.verbose.lock().unwrap() {
                log::info!("Loaded {} boundary CIDs from bundle {:06} (at {})", 
                    boundary.len(), next_bundle_num - 1, cursor);
            }
            
            (cursor, boundary)
        } else {
            ("1970-01-01T00:00:00Z".to_string(), HashSet::new())
        };

        // If mempool has operations, update cursor AND boundaries from mempool
        // (mempool operations already had boundary dedup applied when they were added)
        let mempool_stats = self.get_mempool_stats()?;
        if mempool_stats.count > 0 {
            if let Some(last_time) = mempool_stats.last_time {
                if *self.verbose.lock().unwrap() {
                    log::debug!("Mempool has {} ops, resuming from {}", 
                        mempool_stats.count, last_time.format("%Y-%m-%dT%H:%M:%S"));
                }
                after_time = last_time.to_rfc3339();
                
                // Calculate boundaries from MEMPOOL for next fetch
                let mempool_ops = self.get_mempool_operations()?;
                if !mempool_ops.is_empty() {
                    prev_boundary = get_boundary_cids(&mempool_ops);
                    if *self.verbose.lock().unwrap() {
                        log::info!("Using {} boundary CIDs from mempool", prev_boundary.len());
                    }
                }
            }
        }

        log::debug!("Preparing bundle {:06} (mempool: {} ops)...",
            next_bundle_num, mempool_stats.count);
        log::debug!("Starting cursor: {}", if after_time.is_empty() || after_time == "1970-01-01T00:00:00Z" { "" } else { &after_time });
        
        if !prev_boundary.is_empty() && *self.verbose.lock().unwrap() && mempool_stats.count == 0 {
            log::info!("  Starting with {} boundary CIDs from previous bundle", prev_boundary.len());
        }

        // Ensure mempool is initialized
        self.get_mempool()?;

        // Fetch until we have 10,000 operations
        let mut fetch_num = 0;
        let mut total_fetched = 0;
        let mut total_dupes = 0;
        let mut total_boundary_dupes = 0;
        let fetch_start = Instant::now();
        let mut caught_up = false;
        const MAX_ATTEMPTS: usize = 50;

        while fetch_num < MAX_ATTEMPTS {
            let stats = self.get_mempool_stats()?;

            if stats.count >= constants::BUNDLE_SIZE {
                break;
            }

            fetch_num += 1;
            let needed = constants::BUNDLE_SIZE - stats.count;
            
            // Smart batch sizing - request more than exact amount to account for duplicates
            let request_count = match needed {
                n if n <= 50 => 50,
                n if n <= 100 => 100,
                n if n <= 500 => 200,
                _ => 1000,
            };

            if *self.verbose.lock().unwrap() {
                log::info!("  Fetch #{}: requesting {} (need {} more, have {}/{})",
                    fetch_num, request_count, needed, stats.count, constants::BUNDLE_SIZE);
            }

            let fetch_op_start = Instant::now();
            let plc_ops = client.fetch_operations(&after_time, request_count).await?;

            let fetched_count = plc_ops.len();
            
            // Check for incomplete batch (indicates caught up)
            let got_incomplete_batch = fetched_count > 0 && fetched_count < request_count;
            
            if plc_ops.is_empty() || got_incomplete_batch {
                caught_up = true;
                if *self.verbose.lock().unwrap() && fetch_num > 0 {
                    log::debug!("Caught up to latest PLC data");
                }
                if plc_ops.is_empty() {
                    break;
                }
            }

            total_fetched += fetched_count;

            // Convert and deduplicate
            let mut ops: Vec<Operation> = plc_ops.into_iter().map(Into::into).collect();
            let before_dedup = ops.len();
            ops = strip_boundary_duplicates(ops, &prev_boundary);
            let after_dedup = ops.len();
            
            let boundary_removed = before_dedup - after_dedup;
            if boundary_removed > 0 {
                total_boundary_dupes += boundary_removed;
                if *self.verbose.lock().unwrap() {
                    log::info!("  Stripped {} boundary duplicates from fetch", boundary_removed);
                }
            }

            // Add to mempool
            let added = if !ops.is_empty() {
                self.add_to_mempool(ops)?
            } else {
                0
            };
            
            let dupes_in_fetch = after_dedup - added;
            total_dupes += dupes_in_fetch;

            let fetch_duration = fetch_op_start.elapsed();
            let new_stats = self.get_mempool_stats()?;
            let ops_per_sec = if fetch_duration.as_secs_f64() > 0.0 {
                added as f64 / fetch_duration.as_secs_f64()
            } else {
                0.0
            };

            if *self.verbose.lock().unwrap() {
                if boundary_removed > 0 || dupes_in_fetch > 0 {
                    log::info!("  → +{} unique ({} dupes, {} boundary) in {:.9}s • Running: {}/{} ({:.0} ops/sec)",
                        added, dupes_in_fetch, boundary_removed, fetch_duration.as_secs_f64(),
                        new_stats.count, constants::BUNDLE_SIZE, ops_per_sec);
                } else {
                    log::info!("  → +{} unique in {:.9}s • Running: {}/{} ({:.0} ops/sec)",
                        added, fetch_duration.as_secs_f64(), new_stats.count, constants::BUNDLE_SIZE, ops_per_sec);
                }
            }

            // Update cursor
            if let Some(last_time) = new_stats.last_time {
                after_time = last_time.to_rfc3339();
            }
            
            // Stop if we got an incomplete batch or made no progress
            if got_incomplete_batch || added == 0 {
                caught_up = true;
                if *self.verbose.lock().unwrap() {
                    log::debug!("Caught up to latest PLC data");
                }
                break;
            }
        }

        let fetch_total_duration = fetch_start.elapsed();
        let dedup_pct = if total_fetched > 0 {
            (total_dupes + total_boundary_dupes) as f64 / total_fetched as f64 * 100.0
        } else {
            0.0
        };

        let final_stats = self.get_mempool_stats()?;
        
        // Bundles must contain exactly BUNDLE_SIZE operations (no partial bundles allowed)
        if final_stats.count < constants::BUNDLE_SIZE {
            if caught_up {
                // Caught up to latest PLC data without enough ops for a full bundle
                // Return CaughtUp result instead of error
                return Ok(SyncResult::CaughtUp {
                    next_bundle: next_bundle_num,
                    mempool_count: final_stats.count,
                    new_ops: total_fetched - total_dupes - total_boundary_dupes,
                    fetch_duration_ms: fetch_total_duration.as_millis() as u64,
                });
            } else {
                anyhow::bail!(
                    "Insufficient operations: have {}, need exactly {} (max attempts reached)",
                    final_stats.count,
                    constants::BUNDLE_SIZE
                );
            }
        }

        if *self.verbose.lock().unwrap() {
            log::info!("  ✓ Collected {} unique ops from {} fetches ({:.1}% dedup)",
                final_stats.count, fetch_num, dedup_pct);
        }

        // Take operations and create bundle
        log::debug!("Calling operations.SaveBundle with bundle={}", next_bundle_num);

        let operations = {
            let mut mempool = self.mempool.write().unwrap();
            let mem = mempool.as_mut().ok_or_else(|| anyhow::anyhow!("Mempool not initialized"))?;
            // Take up to BUNDLE_SIZE operations (or all if less)
            let count = mem.count().min(constants::BUNDLE_SIZE);
            mem.take(count)?
        };

        if operations.is_empty() {
            anyhow::bail!("No operations to create bundle");
        }

        // Bundles must contain exactly BUNDLE_SIZE operations
        if operations.len() != constants::BUNDLE_SIZE {
            anyhow::bail!(
                "Invalid operation count: expected exactly {}, got {}",
                constants::BUNDLE_SIZE,
                operations.len()
            );
        }

        log::debug!("SaveBundle SUCCESS, setting bundle fields");

        // CRITICAL: Clear mempool BEFORE saving to ensure atomicity
        // If interrupted after this point, the operations are no longer in mempool
        // and won't be re-fetched on restart, preventing duplicate/inconsistent bundles.
        // If save fails after clearing, we bail out and the operations are lost,
        // but this is better than creating bundles with inconsistent content.
        self.clear_mempool()?;

        // Save bundle to disk with timing breakdown
        // Save bundle and update DID index (now fast with delta segments)
        let save_start = Instant::now();
        let (serialize_time, compress_time, hash_time, did_index_time, index_write_time, did_index_compacted) =
            self.save_bundle_with_timing(next_bundle_num, operations).await?;
        let save_duration = save_start.elapsed();

        // Show timing breakdown in verbose mode only
        if *self.verbose.lock().unwrap() {
            log::debug!("  Save timing: serialize={:.3}ms, compress={:.3}ms, hash={:.3}ms, did_index={:.3}ms, index_write={:.3}ms, total={:.1}ms",
                serialize_time.as_secs_f64() * 1000.0,
                compress_time.as_secs_f64() * 1000.0,
                hash_time.as_secs_f64() * 1000.0,
                did_index_time.as_secs_f64() * 1000.0,
                index_write_time.as_secs_f64() * 1000.0,
                save_duration.as_secs_f64() * 1000.0);
        }

        log::debug!("Adding bundle {} to index", next_bundle_num);
        log::debug!("Index now has {} bundles", next_bundle_num);
        log::debug!("Index saved, last bundle = {}", next_bundle_num);

        // Get bundle info for display
        let (short_hash, age_str, unique_dids, size_bytes) = {
            let index = self.index.read().unwrap();
            let bundle_meta = index.get_bundle(next_bundle_num).unwrap();
            // Use chain hash (first 7 chars) for display
            let hash = bundle_meta.hash[..7].to_string();

            // Calculate age
            let created_time = chrono::DateTime::parse_from_rfc3339(&bundle_meta.start_time)
                .unwrap()
                .with_timezone(&chrono::Utc);
            let now = chrono::Utc::now();
            let age = now.signed_duration_since(created_time);
            let age_str = format_age(age);

            (hash, age_str, bundle_meta.did_count, bundle_meta.compressed_size)
        };

        // Get mempool count after clearing (should be 0, but check anyway)
        let mempool_count = self.get_mempool_stats().map(|s| s.count).unwrap_or(0);
        let total_duration_ms = (fetch_total_duration + save_duration).as_millis() as u64;
        let fetch_duration_ms = fetch_total_duration.as_millis() as u64;
        
        // Calculate separate timings: bundle save (serialize + compress + hash) vs index (did_index + index_write)
        let bundle_save_ms = (serialize_time + compress_time + hash_time).as_millis() as u64;
        let index_ms = (did_index_time + index_write_time).as_millis() as u64;

        // Only log detailed info in verbose mode
        if *self.verbose.lock().unwrap() {
            log::info!("→ Bundle {:06} | {} | fetch: {:.3}s ({} reqs) | {}",
                next_bundle_num, short_hash, fetch_total_duration.as_secs_f64(),
                fetch_num, age_str);
            log::debug!("Bundle done = {}, finish duration = {:.3}ms",
                next_bundle_num, save_duration.as_secs_f64() * 1000.0);
        }

        Ok(SyncResult::BundleCreated {
            bundle_num: next_bundle_num,
            mempool_count,
            duration_ms: total_duration_ms,
            fetch_duration_ms,
            bundle_save_ms,
            index_ms,
            fetch_requests: fetch_num,
            hash: short_hash,
            age: age_str,
            did_index_compacted,
            unique_dids,
            size_bytes,
        })
    }

    /// Run single sync cycle
    ///
    /// If max_bundles is Some(n), stop after syncing n bundles
    /// If max_bundles is None, sync until caught up
    pub async fn sync_once(&self, client: &crate::sync::PLCClient, max_bundles: Option<usize>) -> Result<usize> {
        let mut synced = 0;

        loop {
            match self.sync_next_bundle(client).await {
                Ok(SyncResult::BundleCreated { .. }) => {
                    synced += 1;

                    // Check if we've reached the limit
                    if let Some(max) = max_bundles {
                        if synced >= max {
                            break;
                        }
                    }
                }
                Ok(SyncResult::CaughtUp { .. }) => {
                    // Caught up to latest PLC data
                    break;
                }
                Err(e) => return Err(e),
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        Ok(synced)
    }

    /// Save bundle to disk with compression and index updates (with timing)
    async fn save_bundle_with_timing(&self, bundle_num: u32, operations: Vec<Operation>) -> Result<(std::time::Duration, std::time::Duration, std::time::Duration, std::time::Duration, std::time::Duration, bool)> {
        use std::time::Instant;
        use anyhow::Context;
        use std::collections::HashSet;
        use std::fs::File;
        use std::io::Write;

        if operations.is_empty() {
            anyhow::bail!("Cannot save empty bundle");
        }

        // Extract metadata
        let start_time = operations.first().unwrap().created_at.clone();
        let end_time = operations.last().unwrap().created_at.clone();
        let operation_count = operations.len() as u32;

        // Count unique DIDs
        let unique_dids: HashSet<String> = operations.iter().map(|op| op.did.clone()).collect();
        let did_count = unique_dids.len() as u32;

        // Use multi-frame compression for better performance on large bundles
        let serialize_time;
        let compress_time;
        let uncompressed_size;
        let compressed_size;
        let frame_count;
        let frame_offsets;
        let compressed_frames;
        let content_hash;

        // Compress operations to frames
        let compress_result = {
            let result = crate::bundle_format::compress_operations_to_frames(&operations)?;
            serialize_time = std::time::Duration::from_secs_f64(result.serialize_time_ms / 1000.0);
            compress_time = std::time::Duration::from_secs_f64(result.compress_time_ms / 1000.0);
            result
        };

        uncompressed_size = compress_result.uncompressed_size;
        compressed_size = compress_result.compressed_size;
        frame_count = compress_result.compressed_frames.len();
        frame_offsets = compress_result.frame_offsets;
        compressed_frames = compress_result.compressed_frames;

        // Calculate content hash from uncompressed data
        let hash_start = Instant::now();
        content_hash = {
            use sha2::{Sha256, Digest};
            let mut hasher = Sha256::new();
            let mut missing_raw_json = 0;

            // Hash all operations in order (reconstructing uncompressed JSONL)
            for op in &operations {
                let json = if let Some(raw) = &op.raw_json {
                    raw.clone()
                } else {
                    missing_raw_json += 1;
                    if missing_raw_json == 1 && *self.verbose.lock().unwrap() {
                        log::warn!("⚠️  Bundle {}: Operation missing raw_json, using re-serialized JSON (may cause hash mismatch!)", bundle_num);
                    }
                    sonic_rs::to_string(op)?
                };
                hasher.update(json.as_bytes());
                hasher.update(b"\n");
            }

            if missing_raw_json > 0 && *self.verbose.lock().unwrap() {
                log::warn!("⚠️  Bundle {}: {} operations missing raw_json (content hash may be incorrect!)", bundle_num, missing_raw_json);
            }

            format!("{:x}", hasher.finalize())
        };

        // Calculate compressed hash - will be calculated after writing the file
        // because it needs to include the metadata frame (verification hashes entire file)
        // We'll calculate it after the file is written
        let hash_time = hash_start.elapsed();

        // Calculate chain hash per spec (Section 6.3)
        // Genesis bundle: SHA256("plcbundle:genesis:" + content_hash)
        // Subsequent: SHA256(parent_chain_hash + ":" + current_content_hash)
        let (parent, chain_hash) = if bundle_num > 1 {
            use sha2::{Sha256, Digest};
            let parent_chain_hash = self.index
                .read()
                .unwrap()
                .get_bundle(bundle_num - 1)
                .map(|b| b.hash.clone())
                .unwrap_or_default();
            
            // Debug logging for hash calculation issues
            if parent_chain_hash.is_empty() {
                log::warn!("⚠️  Bundle {}: Parent bundle {} not found in index! Using empty parent hash.", bundle_num, bundle_num - 1);
            } else if *self.verbose.lock().unwrap() {
                log::debug!("Bundle {}: Parent hash from bundle {}: {}", bundle_num, bundle_num - 1, &parent_chain_hash[..16]);
                log::debug!("Bundle {}: Content hash: {}", bundle_num, &content_hash[..16]);
            }
            
            let chain_input = format!("{}:{}", parent_chain_hash, content_hash);
            let mut hasher = Sha256::new();
            hasher.update(chain_input.as_bytes());
            let hash = format!("{:x}", hasher.finalize());
            
            if *self.verbose.lock().unwrap() {
                log::debug!("Bundle {}: Chain hash: {}", bundle_num, &hash[..16]);
            }
            
            (parent_chain_hash, hash)
        } else {
            // Genesis bundle
            use sha2::{Sha256, Digest};
            let chain_input = format!("plcbundle:genesis:{}", content_hash);
            let mut hasher = Sha256::new();
            hasher.update(chain_input.as_bytes());
            let hash = format!("{:x}", hasher.finalize());
            
            (String::new(), hash)
        };

        // Get cursor (end_time of previous bundle per spec)
        // For the first bundle, cursor is empty string
        let cursor = if bundle_num > 1 {
            let prev_end_time = self.index
                .read()
                .unwrap()
                .get_bundle(bundle_num - 1)
                .map(|b| b.end_time.clone())
                .unwrap_or_default();
            
            // Validate cursor matches previous bundle's end_time
            if prev_end_time.is_empty() {
                log::warn!("⚠️  Bundle {}: Previous bundle {} has empty end_time, cursor will be empty", bundle_num, bundle_num - 1);
            }
            
            prev_end_time
        } else {
            String::new()
        };
        
        // Validate cursor correctness (for non-genesis bundles)
        if bundle_num > 1 {
            let expected_cursor = {
                let index = self.index.read().unwrap();
                index.get_bundle(bundle_num - 1)
                    .map(|b| b.end_time.clone())
                    .unwrap_or_default()
            };
            if cursor != expected_cursor {
                anyhow::bail!(
                    "Cursor validation failed for bundle {}: expected {} (previous bundle end_time), got {}",
                    bundle_num, expected_cursor, cursor
                );
            }
        } else if !cursor.is_empty() {
            anyhow::bail!(
                "Cursor validation failed for bundle {} (genesis): cursor should be empty, got {}",
                bundle_num, cursor
            );
        }

        // Prepare bundle metadata for skippable frame
        let bundle_metadata_frame = crate::bundle_format::BundleMetadata {
            format: "plcbundle/1.0".to_string(),
            bundle_number: bundle_num,
            origin: self.index.read().unwrap().origin.clone(),
            content_hash: content_hash.clone(),
            parent_hash: if !parent.is_empty() { Some(parent.clone()) } else { None },
            operation_count: operation_count as usize,
            did_count: did_count as usize,
            start_time: start_time.clone(),
            end_time: end_time.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            created_by: constants::created_by(),
            frame_count,
            frame_size: constants::FRAME_SIZE,
            frame_offsets: frame_offsets.clone(),
        };

        // Write to disk with metadata skippable frame (move to blocking task to avoid blocking async runtime)
        // CRITICAL: We need to calculate compressed_hash from the entire file (including metadata frame)
        // because verification hashes the entire file. So we write the file first, then read it back to calculate the hash.
        let bundle_path = self.directory.join(format!("{:06}.jsonl.zst", bundle_num));
        let bundle_path_clone = bundle_path.clone();
        let bundle_metadata_frame_clone = bundle_metadata_frame.clone();
        let compressed_frames_clone = compressed_frames.clone();
        
        // Write file first (metadata frame doesn't contain compressed_hash, so we can write it)
        tokio::task::spawn_blocking({
            let bundle_path_clone = bundle_path_clone.clone();
            let bundle_metadata_frame_clone = bundle_metadata_frame_clone.clone();
            let compressed_frames_clone = compressed_frames_clone.clone();
            move || {
                let mut file = File::create(&bundle_path_clone)
                    .with_context(|| format!("Failed to create bundle file: {}", bundle_path_clone.display()))?;
                
                // Write metadata as skippable frame first
                crate::bundle_format::write_metadata_frame(&mut file, &bundle_metadata_frame_clone)
                    .with_context(|| format!("Failed to write metadata frame to: {}", bundle_path_clone.display()))?;

                // Write all compressed frames
                for frame in &compressed_frames_clone {
                    file.write_all(frame)
                        .with_context(|| format!("Failed to write compressed frame to: {}", bundle_path_clone.display()))?;
                }
                file.flush()
                    .with_context(|| format!("Failed to flush bundle file: {}", bundle_path_clone.display()))?;
                
                Ok::<(), anyhow::Error>(())
            }
        })
        .await
        .context("Bundle file write task failed")??;

        // Now calculate compressed_hash from the entire file (as verification does)
        let compressed_hash = tokio::task::spawn_blocking({
            let bundle_path_clone = bundle_path_clone.clone();
            move || {
                use sha2::{Sha256, Digest};
                let file_data = std::fs::read(&bundle_path_clone)
                    .with_context(|| format!("Failed to read bundle file for hash: {}", bundle_path_clone.display()))?;
                
                let mut hasher = Sha256::new();
                hasher.update(&file_data);
                Ok::<String, anyhow::Error>(format!("{:x}", hasher.finalize()))
            }
        })
        .await
        .context("Compressed hash calculation task failed")??;

        if *self.verbose.lock().unwrap() {
            log::debug!(
                "Saved bundle {} ({} ops, {} DIDs, {} → {} bytes, {:.1}% compression)",
                bundle_num,
                operation_count,
                did_count,
                uncompressed_size,
                compressed_size,
                100.0 * (1.0 - compressed_size as f64 / uncompressed_size as f64)
            );
        }

        // Update DID index (now fast with delta segments)
        let did_index_start = Instant::now();
        let did_ops: Vec<(String, bool)> = operations
            .iter()
            .map(|op| (op.did.clone(), op.nullified))
            .collect();

        let did_index_compacted = self.did_index.write().unwrap().update_for_bundle(bundle_num, did_ops)?;
        let did_index_time = did_index_start.elapsed();

        // Update main index
        let index_write_start = Instant::now();
        let bundle_metadata = crate::index::BundleMetadata {
            bundle_number: bundle_num,
            start_time,
            end_time,
            operation_count,
            did_count,
            hash: chain_hash, // Chain hash per spec
            content_hash,
            parent,
            compressed_hash,
            compressed_size,
            uncompressed_size,
            cursor,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        // Add to index
        // CRITICAL: Clone index data while holding lock briefly, then release lock
        // before doing expensive serialization and file I/O in spawn_blocking
        let index_path = self.directory.join("plc_bundles.json");
        let index_json = {
            let mut index = self.index.write().unwrap();
            index.bundles.push(bundle_metadata);
            index.last_bundle = bundle_num;
            index.updated_at = chrono::Utc::now().to_rfc3339();
            index.total_size_bytes += compressed_size;
            index.total_uncompressed_size_bytes += uncompressed_size;

            // Clone the index for serialization outside the lock
            // This prevents blocking the async runtime while holding the lock
            index.clone()
        };
        
        // Serialize and write index in blocking task to avoid blocking async runtime
        // Use atomic write (temp file + rename) to prevent corruption on shutdown
        let index_path_clone = index_path.clone();
        tokio::task::spawn_blocking(move || {
            let index_json = serde_json::to_string_pretty(&index_json)?;
            
            // Atomic write: write to temp file first, then rename
            // This ensures the index file is never partially written, even if process is killed
            let temp_path = index_path_clone.with_extension("json.tmp");
            std::fs::write(&temp_path, index_json)
                .with_context(|| format!("Failed to write temp index to: {}", temp_path.display()))?;
            
            // Atomic rename - this is guaranteed to be atomic on most filesystems
            std::fs::rename(&temp_path, &index_path_clone)
                .with_context(|| format!("Failed to rename temp index to: {}", index_path_clone.display()))?;
            
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("Index write task failed")??;
        let index_write_time = index_write_start.elapsed();

        Ok((serialize_time, compress_time, hash_time, did_index_time, index_write_time, did_index_compacted))
    }
    
    /// Save bundle to disk with compression and index updates (backwards compatibility)
    #[allow(dead_code)]
    async fn save_bundle(&self, bundle_num: u32, operations: Vec<Operation>) -> Result<()> {
        self.save_bundle_with_timing(bundle_num, operations).await?;
        Ok(())
    }

    /// Migrate a bundle to multi-frame format
    /// 
    /// This method loads a bundle and re-saves it with multi-frame compression
    /// (100 operations per frame) with frame offsets for efficient random access.
    /// 
    /// Returns: (size_diff, new_uncompressed_size, new_compressed_size)
    pub fn migrate_bundle(&self, bundle_num: u32) -> Result<(i64, u64, u64)> {
        use anyhow::Context;
        use std::fs::File;
        use std::collections::HashSet;

        // Get existing bundle metadata
        let meta = self.get_bundle_metadata(bundle_num)?
            .ok_or_else(|| anyhow::anyhow!("Bundle {} not in index", bundle_num))?;
        
        let old_size = meta.compressed_size;

        // Load bundle operations
        let load_result = self.load_bundle(bundle_num, LoadOptions {
            decompress: true,
            cache: false,
            filter: None,
            limit: None,
        })?;

        let operations = load_result.operations;
        if operations.is_empty() {
            anyhow::bail!("Bundle {} has no operations", bundle_num);
        }

        // Extract metadata
        let start_time = operations.first().unwrap().created_at.clone();
        let end_time = operations.last().unwrap().created_at.clone();
        let operation_count = operations.len() as u32;

        // Count unique DIDs
        let unique_dids: HashSet<String> = operations.iter().map(|op| op.did.clone()).collect();
        let did_count = unique_dids.len() as u32;

        // Compress operations into frames using library function
        let frame_result = crate::bundle_format::compress_operations_to_frames(&operations)?;
        let compressed_size = frame_result.compressed_size;
        let uncompressed_size = frame_result.uncompressed_size;

        // Calculate hashes using library functions
        let content_hash = crate::bundle_format::calculate_content_hash(&operations)?;

        // Compressed hash will be calculated after writing the file
        // because it needs to include the metadata frame (verification hashes entire file)

        // Recalculate chain hash to verify correctness
        let (expected_parent, recalculated_chain_hash) = if bundle_num > 1 {
            use sha2::{Sha256, Digest};
            let parent_chain_hash = self.index
                .read()
                .unwrap()
                .get_bundle(bundle_num - 1)
                .map(|b| b.hash.clone())
                .unwrap_or_default();

            let chain_input = format!("{}:{}", parent_chain_hash, content_hash);
            let mut hasher = Sha256::new();
            hasher.update(chain_input.as_bytes());
            let hash = format!("{:x}", hasher.finalize());

            (parent_chain_hash, hash)
        } else {
            use sha2::{Sha256, Digest};
            let chain_input = format!("plcbundle:genesis:{}", content_hash);
            let mut hasher = Sha256::new();
            hasher.update(chain_input.as_bytes());
            let hash = format!("{:x}", hasher.finalize());

            (String::new(), hash)
        };

        // Verify chain hash matches original
        if recalculated_chain_hash != meta.hash {
            anyhow::bail!(
                "Chain hash mismatch in bundle {}: original={}, recalculated={}\n\
                This indicates the original bundle content may be corrupted or the chain was broken.",
                bundle_num, meta.hash, recalculated_chain_hash
            );
        }

        // Verify parent hash matches
        if expected_parent != meta.parent {
            anyhow::bail!(
                "Parent hash mismatch in bundle {}: original={}, expected={}\n\
                This indicates the chain linkage is broken.",
                bundle_num, meta.parent, expected_parent
            );
        }

        // Use verified hashes from original bundle
        let chain_hash = meta.hash.clone();
        let parent = meta.parent.clone();

        // Get cursor (end_time of previous bundle per spec)
        // For the first bundle, cursor is empty string
        let cursor = if bundle_num > 1 {
            let prev_end_time = self.index
                .read()
                .unwrap()
                .get_bundle(bundle_num - 1)
                .map(|b| b.end_time.clone())
                .unwrap_or_default();
            
            // Validate cursor matches previous bundle's end_time
            if prev_end_time.is_empty() {
                log::warn!("⚠️  Bundle {}: Previous bundle {} has empty end_time, cursor will be empty", bundle_num, bundle_num - 1);
            }
            
            prev_end_time
        } else {
            String::new()
        };
        
        // Validate cursor correctness (for non-genesis bundles)
        if bundle_num > 1 {
            let expected_cursor = {
                let index = self.index.read().unwrap();
                index.get_bundle(bundle_num - 1)
                    .map(|b| b.end_time.clone())
                    .unwrap_or_default()
            };
            if cursor != expected_cursor {
                anyhow::bail!(
                    "Cursor validation failed for bundle {}: expected {} (previous bundle end_time), got {}",
                    bundle_num, expected_cursor, cursor
                );
            }
        } else if !cursor.is_empty() {
            anyhow::bail!(
                "Cursor validation failed for bundle {} (genesis): cursor should be empty, got {}",
                bundle_num, cursor
            );
        }
        
        let origin = self.index.read().unwrap().origin.clone();

        // Create bundle metadata using library function
        let bundle_metadata_frame = crate::bundle_format::create_bundle_metadata(
            bundle_num,
            &origin,
            &content_hash,
            if !parent.is_empty() { Some(&parent) } else { None },
            operation_count as usize,
            did_count as usize,
            &start_time,
            &end_time,
            frame_result.frame_offsets.len() - 1,
            constants::FRAME_SIZE,
            &frame_result.frame_offsets,
        );

        // Create backup path
        let bundle_path = self.directory.join(format!("{:06}.jsonl.zst", bundle_num));
        let backup_path = bundle_path.with_extension("jsonl.zst.bak");

        // Backup existing file
        if bundle_path.exists() {
            std::fs::copy(&bundle_path, &backup_path)
                .with_context(|| format!("Failed to backup bundle: {}", bundle_path.display()))?;
        }

        // Write new bundle with multi-frame format using library function
        let mut file = File::create(&bundle_path)
            .with_context(|| format!("Failed to create bundle file: {}", bundle_path.display()))?;
        
        crate::bundle_format::write_bundle_with_frames(
            &mut file,
            &bundle_metadata_frame,
            &frame_result.compressed_frames,
        )
        .with_context(|| format!("Failed to write bundle: {}", bundle_path.display()))?;

        // Verify metadata was written correctly
        let embedded_meta = crate::bundle_format::extract_metadata_from_file(&bundle_path)
            .with_context(|| "Failed to extract embedded metadata after migration")?;

        if embedded_meta.frame_offsets.is_empty() {
            // Restore backup on failure
            if backup_path.exists() {
                std::fs::rename(&backup_path, &bundle_path)?;
            }
            anyhow::bail!("Frame offsets missing in metadata after migration");
        }

        // Verify content hash matches
        if embedded_meta.content_hash != content_hash {
            // Restore backup on failure
            if backup_path.exists() {
                std::fs::rename(&backup_path, &bundle_path)?;
            }
            anyhow::bail!("Content hash mismatch after migration");
        }

        // Calculate compressed_hash from the entire file (as verification does)
        // This must be done AFTER writing the file because it includes the metadata frame
        use sha2::{Sha256, Digest};
        let file_data = std::fs::read(&bundle_path)
            .with_context(|| format!("Failed to read bundle file for hash: {}", bundle_path.display()))?;
        
        let mut hasher = Sha256::new();
        hasher.update(&file_data);
        let compressed_hash = format!("{:x}", hasher.finalize());

        // Update index BEFORE removing backup (so if interrupted, index is consistent with file)
        let bundle_metadata = crate::index::BundleMetadata {
            bundle_number: bundle_num,
            start_time,
            end_time,
            operation_count,
            did_count,
            hash: chain_hash,
            content_hash,
            parent,
            compressed_hash,
            compressed_size,
            uncompressed_size,
            cursor,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        {
            let mut index = self.index.write().unwrap();
            // Update existing bundle metadata
            if let Some(existing) = index.bundles.iter_mut().find(|b| b.bundle_number == bundle_num) {
                *existing = bundle_metadata.clone();
            } else {
                index.bundles.push(bundle_metadata.clone());
            }

            // Recalculate totals
            index.total_size_bytes = index.bundles.iter().map(|b| b.compressed_size).sum();
            index.total_uncompressed_size_bytes = index.bundles.iter().map(|b| b.uncompressed_size).sum();
            index.updated_at = chrono::Utc::now().to_rfc3339();

            // Save index to disk
            let index_path = self.directory.join("plc_bundles.json");
            let index_json = serde_json::to_string_pretty(&*index)?;
            std::fs::write(&index_path, index_json)
                .with_context(|| format!("Failed to write index to: {}", index_path.display()))?;
        }

        // Remove backup only after index is successfully updated
        if backup_path.exists() {
            std::fs::remove_file(&backup_path)
                .with_context(|| format!("Failed to remove backup: {}", backup_path.display()))?;
        }

        let size_diff = compressed_size as i64 - old_size as i64;
        Ok((size_diff, uncompressed_size, compressed_size))
    }

    // === Helpers ===
    pub fn get_last_bundle(&self) -> u32 {
        self.index.read().unwrap().last_bundle
    }

    pub fn directory(&self) -> &PathBuf {
        &self.directory
    }

    /// Get a copy of the current index
    pub fn get_index(&self) -> Index {
        self.index.read().unwrap().clone()
    }

    // === Remote Access ===
    
    /// Fetch index from remote URL or local file path
    /// 
    /// This is an async method that requires a tokio runtime.
    /// For synchronous usage, use the remote module functions directly.
    pub async fn fetch_remote_index(&self, target: &str) -> Result<Index> {
        crate::remote::fetch_index(target).await
    }
    
    /// Fetch bundle operations from remote URL
    /// 
    /// This is an async method that requires a tokio runtime.
    pub async fn fetch_remote_bundle(&self, base_url: &str, bundle_num: u32) -> Result<Vec<Operation>> {
        crate::remote::fetch_bundle_operations(base_url, bundle_num).await
    }
    
    /// Fetch a single operation from remote URL
    /// 
    /// This is an async method that requires a tokio runtime.
    pub async fn fetch_remote_operation(&self, base_url: &str, bundle_num: u32, position: usize) -> Result<String> {
        crate::remote::fetch_operation(base_url, bundle_num, position).await
    }

    /// Rollback repository to a specific bundle
    pub fn rollback_to_bundle(&mut self, target_bundle: u32) -> Result<()> {
        use anyhow::Context;
        
        let mut index = self.index.write().unwrap();
        
        // Keep only bundles up to target
        index.bundles.retain(|b| b.bundle_number <= target_bundle);
        index.last_bundle = target_bundle;
        index.updated_at = chrono::Utc::now().to_rfc3339();
        
        // Recalculate total sizes
        index.total_size_bytes = index.bundles.iter().map(|b| b.compressed_size).sum();
        index.total_uncompressed_size_bytes = index.bundles.iter().map(|b| b.uncompressed_size).sum();
        
        // Save updated index
        let index_path = self.directory.join("plc_bundles.json");
        let index_json = serde_json::to_string_pretty(&*index)?;
        std::fs::write(&index_path, index_json)
            .with_context(|| format!("Failed to write index to: {}", index_path.display()))?;
        
        Ok(())
    }

    /// Get bundle metadata from index
    pub fn get_bundle_metadata(&self, bundle_num: u32) -> Result<Option<crate::index::BundleMetadata>> {
        let index = self.index.read().unwrap();
        Ok(index.get_bundle(bundle_num).cloned())
    }

    /// Get embedded metadata from bundle's skippable frame
    pub fn get_embedded_metadata(&self, bundle_num: u32) -> Result<Option<crate::bundle_format::BundleMetadata>> {
        let bundle_path = self.directory.join(format!("{:06}.jsonl.zst", bundle_num));

        if !bundle_path.exists() {
            return Ok(None);
        }

        match crate::bundle_format::extract_metadata_from_file(&bundle_path) {
            Ok(meta) => Ok(Some(meta)),
            Err(_) => Ok(None), // Bundle may not have embedded metadata
        }
    }

    /// Delete bundle files from disk
    pub fn delete_bundle_files(&self, bundle_numbers: &[u32]) -> Result<RollbackFileStats> {
        let mut deleted = 0;
        let mut failed = 0;
        let mut deleted_size = 0u64;

        for &bundle_num in bundle_numbers {
            let bundle_path = self.directory.join(format!("{:06}.jsonl.zst", bundle_num));
            
            // Get file size before deletion
            if let Ok(metadata) = std::fs::metadata(&bundle_path) {
                deleted_size += metadata.len();
            }
            
            match std::fs::remove_file(&bundle_path) {
                Ok(_) => deleted += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => deleted += 1,
                Err(_) => failed += 1,
            }
        }

        Ok(RollbackFileStats {
            deleted,
            failed,
            deleted_size,
        })
    }

    // === Server API Methods ===
    
    /// Get PLC origin from index
    pub fn get_plc_origin(&self) -> String {
        self.index.read().unwrap().origin.clone()
    }
    
    /// Stream bundle raw (compressed) data
    /// Returns a reader that can be used to stream the compressed bundle file
    pub fn stream_bundle_raw(&self, bundle_num: u32) -> Result<std::fs::File> {
        // Validate bundle exists in index first
        if self.get_bundle_metadata(bundle_num)?.is_none() {
            anyhow::bail!("Bundle {} not found in index", bundle_num);
        }

        let bundle_path = self.directory.join(format!("{:06}.jsonl.zst", bundle_num));
        if !bundle_path.exists() {
            anyhow::bail!("Bundle {} file not found (exists in index but missing on disk)", bundle_num);
        }
        Ok(std::fs::File::open(bundle_path)?)
    }
    
    /// Stream bundle decompressed (JSONL) data
    /// Returns a reader that decompresses the bundle on-the-fly
    pub fn stream_bundle_decompressed(&self, bundle_num: u32) -> Result<Box<dyn std::io::Read + Send>> {
        let file = self.stream_bundle_raw(bundle_num)?;
        Ok(Box::new(zstd::Decoder::new(file)?))
    }
    
    /// Get current cursor (global position of last operation)
    /// Cursor = (last_bundle * BUNDLE_SIZE) + mempool_ops_count
    pub fn get_current_cursor(&self) -> u64 {
        let index = self.index.read().unwrap();
        let bundled_ops = index.last_bundle as u64 * constants::BUNDLE_SIZE as u64;
        
        // Add mempool operations if available
        let mempool_guard = self.mempool.read().unwrap();
        let mempool_ops = if let Some(mp) = mempool_guard.as_ref() {
            mp.get_operations().len() as u64
        } else {
            0
        };
        
        bundled_ops + mempool_ops
    }
    
    /// Resolve handle to DID or validate DID format (async version)
    /// Returns (did, handle_resolve_time_ms)
    /// Use this version when calling from async code (e.g., server handlers)
    pub async fn resolve_handle_or_did_async(&self, input: &str) -> Result<(String, u64)> {
        use std::time::Instant;
        
        let input = input.trim();
        
        // Normalize handle format (remove at://, @ prefixes)
        let normalized = if !input.starts_with("did:") {
            handle_resolver::normalize_handle(input)
        } else {
            input.to_string()
        };
        
        // If already a DID, validate and return
        if normalized.starts_with("did:plc:") {
            crate::resolver::validate_did_format(&normalized)?;
            return Ok((normalized, 0));
        }
        
        // Support did:web too
        if normalized.starts_with("did:web:") {
            return Ok((normalized, 0));
        }
        
        // It's a handle - need resolver
        let resolver = match &self.handle_resolver {
            Some(r) => r,
            None => {
                anyhow::bail!(
                    "Input '{}' appears to be a handle, but handle resolver is not configured\n\n\
                    Configure resolver with:\n\
                      plcbundle --handle-resolver {} did resolve {}\n\n\
                    Or set default in config",
                    normalized, constants::DEFAULT_HANDLE_RESOLVER_URL, normalized
                );
            }
        };
        
        // Resolve handle (async operation)
        let resolve_start = Instant::now();
        let did = resolver.resolve_handle(&normalized).await?;
        let resolve_time = resolve_start.elapsed();
        
        Ok((did, resolve_time.as_millis() as u64))
    }
    
    /// Resolve handle to DID or validate DID format
    /// Returns (did, handle_resolve_time_ms)
    /// This is a synchronous wrapper that uses tokio runtime for async resolution
    /// For async code, use resolve_handle_or_did_async instead
    pub fn resolve_handle_or_did(&self, input: &str) -> Result<(String, u64)> {
        use std::time::Instant;
        
        let input = input.trim();
        
        // Normalize handle format (remove at://, @ prefixes)
        let normalized = if !input.starts_with("did:") {
            handle_resolver::normalize_handle(input)
        } else {
            input.to_string()
        };
        
        // If already a DID, validate and return
        if normalized.starts_with("did:plc:") {
            crate::resolver::validate_did_format(&normalized)?;
            return Ok((normalized, 0));
        }
        
        // Support did:web too
        if normalized.starts_with("did:web:") {
            return Ok((normalized, 0));
        }
        
        // It's a handle - need resolver
        let resolver = match &self.handle_resolver {
            Some(r) => r,
            None => {
                anyhow::bail!(
                    "Input '{}' appears to be a handle, but handle resolver is not configured\n\n\
                    Configure resolver with:\n\
                      plcbundle --handle-resolver {} did resolve {}\n\n\
                    Or set default in config",
                    normalized, constants::DEFAULT_HANDLE_RESOLVER_URL, normalized
                );
            }
        };
        
        // Use tokio runtime to resolve handle (async operation)
        // Not in a runtime - safe to create one and use block_on
        let resolve_start = Instant::now();
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| anyhow::anyhow!("Failed to create tokio runtime: {}", e))?;
        let did = runtime.block_on(resolver.resolve_handle(&normalized))?;
        let resolve_time = resolve_start.elapsed();
        
        Ok((did, resolve_time.as_millis() as u64))
    }
    
    /// Get resolver statistics
    /// Returns a HashMap with resolver performance metrics
    pub fn get_resolver_stats(&self) -> HashMap<String, serde_json::Value> {
        // For now, return empty stats
        // TODO: Track resolver statistics
        HashMap::new()
    }
    
    /// Get handle resolver base URL
    /// Returns None if handle resolver is not configured
    pub fn get_handle_resolver_base_url(&self) -> Option<String> {
        self.handle_resolver.as_ref().map(|r| r.get_base_url().to_string())
    }

    /// Get a reference to the handle resolver
    /// Returns None if handle resolver is not configured
    pub fn get_handle_resolver(&self) -> Option<Arc<handle_resolver::HandleResolver>> {
        self.handle_resolver.clone()
    }

    pub fn clone_for_arc(&self) -> Self {
        Self {
            directory: self.directory.clone(),
            index: Arc::clone(&self.index),
            cache: Arc::clone(&self.cache),
            did_index: Arc::clone(&self.did_index),
            stats: Arc::clone(&self.stats),
            mempool: Arc::clone(&self.mempool),
            handle_resolver: self.handle_resolver.clone(),
            verbose: Arc::clone(&self.verbose),
        }
    }
    fn load_bundle_from_disk(&self, path: &PathBuf) -> Result<Vec<Operation>> {
        use std::io::BufRead;

        let file = std::fs::File::open(path)?;
        let decoder = zstd::Decoder::new(file)?;
        let reader = std::io::BufReader::new(decoder);

        let mut operations = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            // CRITICAL: Preserve raw JSON for content hash calculation
            // This is required by the V1 specification (docs/specification.md § 4.2)
            // to ensure content_hash remains reproducible during migration.
            // Without this, re-serialization would change the hash.
            let mut op: Operation = serde_json::from_str(&line)?;
            op.raw_json = Some(line);
            operations.push(op);
        }

        Ok(operations)
    }

    fn filter_load_result(&self, operations: Vec<Operation>, options: &LoadOptions) -> LoadResult {
        let mut filtered = operations;
        
        if let Some(ref filter) = options.filter {
            filtered.retain(|op| self.matches_filter(op, filter));
        }
        
        if let Some(limit) = options.limit {
            filtered.truncate(limit);
        }
        
        LoadResult {
            bundle_number: 0,
            operations: filtered,
            metadata: None,
        }
    }

    fn matches_filter(&self, op: &Operation, filter: &OperationFilter) -> bool {
        if let Some(ref did) = filter.did {
            if &op.did != did {
                return false;
            }
        }
        
        if let Some(ref op_type) = filter.operation_type {
            if &op.operation != op_type {
                return false;
            }
        }
        
        if !filter.include_nullified && op.nullified {
            return false;
        }
        
        true
    }

    fn matches_request(&self, op: &Operation, req: &OperationRequest) -> bool {
        if let Some(ref filter) = req.filter {
            return self.matches_filter(op, filter);
        }
        true
    }
}

// Supporting types moved here
#[derive(Debug, Clone)]
pub struct LoadOptions {
    pub cache: bool,
    pub decompress: bool,
    pub filter: Option<OperationFilter>,
    pub limit: Option<usize>,
}

impl Default for LoadOptions {
    fn default() -> Self {
        Self {
            cache: true,
            decompress: true,
            filter: None,
            limit: None,
        }
    }
}

#[derive(Debug)]
pub struct LoadResult {
    pub bundle_number: u32,
    pub operations: Vec<Operation>,
    pub metadata: Option<BundleMetadata>,
}

#[derive(Debug)]
pub struct OperationResult {
    pub raw_json: String,
    pub size_bytes: usize,
    pub load_duration: std::time::Duration,
}

#[derive(Debug, Clone)]
pub struct QuerySpec {
    pub bundles: BundleRange,
    pub filter: Option<OperationFilter>,
    pub query: String,
    pub mode: QueryMode,
}

// Helper function to format age duration
fn format_age(duration: chrono::Duration) -> String {
    let days = duration.num_days();
    if days >= 365 {
        let years = days as f64 / 365.25;
        format!("{:.1} years ago", years)
    } else if days >= 30 {
        let months = days as f64 / 30.0;
        format!("{:.1} months ago", months)
    } else if days > 0 {
        format!("{} days ago", days)
    } else {
        let hours = duration.num_hours();
        if hours > 0 {
            format!("{} hours ago", hours)
        } else {
            let mins = duration.num_minutes();
            if mins > 0 {
                format!("{} minutes ago", mins)
            } else {
                "just now".to_string()
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum BundleRange {
    All,
    Single(u32),
    Range(u32, u32),
    List(Vec<u32>),
}

#[derive(Debug, Clone)]
pub struct ExportSpec {
    pub bundles: BundleRange,
    pub format: ExportFormat,
    pub filter: Option<OperationFilter>,
    pub compression: Option<CompressionType>,
    pub count: Option<usize>,
    pub after_timestamp: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ExportFormat {
    JsonLines,
    Csv,
    Parquet,
}

#[derive(Debug, Clone)]
pub enum CompressionType {
    Zstd,
    Gzip,
    None,
}

#[derive(Debug, Default)]
pub struct ExportStats {
    pub records_written: u64,
    pub bytes_written: u64,
}

#[derive(Debug, Clone)]
pub struct VerifySpec {
    pub check_hash: bool,
    pub check_content_hash: bool,
    pub check_operations: bool,
    pub fast: bool,  // Fast mode: only check metadata frame, skip hash calculations
}

#[derive(Debug)]
pub struct VerifyResult {
    pub valid: bool,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ChainVerifySpec {
    pub start_bundle: u32,
    pub end_bundle: Option<u32>,
    pub check_parent_links: bool,
}

#[derive(Debug)]
pub struct ChainVerifyResult {
    pub valid: bool,
    pub bundles_checked: u32,
    pub errors: Vec<(u32, String)>,
}

#[derive(Debug)]
pub struct BundleInfo {
    pub metadata: BundleMetadata,
    pub exists: bool,
    pub cached: bool,
    pub operations: Option<Vec<Operation>>,
    pub size_info: Option<SizeInfo>,
}

#[derive(Debug)]
pub struct SizeInfo {
    pub compressed: u64,
    pub uncompressed: u64,
}

#[derive(Debug, Clone)]
pub struct InfoFlags {
    pub include_operations: bool,
    pub include_size_info: bool,
}

#[derive(Debug, Clone)]
pub struct RollbackSpec {
    pub target_bundle: u32,
    pub dry_run: bool,
}

#[derive(Debug)]
pub struct RollbackPlan {
    pub target_bundle: u32,
    pub affected_bundles: Vec<u32>,
    pub affected_operations: usize,
    pub affected_dids: usize,
    pub estimated_time_ms: u64,
}

#[derive(Debug)]
pub struct RollbackResult {
    pub success: bool,
    pub bundles_removed: usize,
    pub plan: Option<RollbackPlan>,
}

#[derive(Debug, Clone)]
pub struct WarmUpSpec {
    pub strategy: WarmUpStrategy,
}

#[derive(Debug, Clone)]
pub enum WarmUpStrategy {
    Recent(u32),
    Range(u32, u32),
    All,
}

#[derive(Debug, Default)]
pub struct RebuildStats {
    pub bundles_processed: u32,
    pub operations_indexed: u64,
}

