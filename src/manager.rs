// src/manager.rs
use crate::index::{Index, BundleMetadata};
use crate::operations::{Operation, OperationFilter, OperationRequest};
use crate::iterators::{QueryIterator, ExportIterator, RangeIterator};
use crate::options::QueryMode;
use crate::{cache, did_index, verification, mempool};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::io::Write;
use chrono::{DateTime, Utc};

pub struct BundleManager {
    directory: PathBuf,
    index: Arc<RwLock<Index>>,
    cache: Arc<cache::BundleCache>,
    did_index: Arc<RwLock<did_index::Manager>>,
    stats: Arc<RwLock<ManagerStats>>,
    mempool: Arc<RwLock<Option<mempool::Mempool>>>,
    verbose: bool,
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
}

#[derive(Debug, Clone, Default)]
pub struct DIDIndexStats {
    pub total_dids: usize,
    pub total_entries: usize,
    pub avg_operations_per_did: f64,
}

impl BundleManager {
    pub fn new(directory: PathBuf) -> Result<Self> {
        let index = Index::load(&directory)?;
        let did_index = did_index::Manager::new(directory.clone())?;

        Ok(Self {
            directory: directory.clone(),
            index: Arc::new(RwLock::new(index)),
            cache: Arc::new(cache::BundleCache::new(100)),
            did_index: Arc::new(RwLock::new(did_index)),
            stats: Arc::new(RwLock::new(ManagerStats::default())),
            mempool: Arc::new(RwLock::new(None)),
            verbose: false,
        })
    }

    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
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
        let (locations, shard_stats, shard_num) = did_index.get_did_locations_with_stats(did)?;
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

    pub fn get_did_index_stats(&self) -> DIDIndexStats {
        let stats_map = self.did_index.read().unwrap().get_stats();
        
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
                self.verbose,
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

    /// Fetch and save next bundle from PLC directory
    pub async fn sync_next_bundle(&mut self, client: &crate::sync::PLCClient) -> Result<u32> {
        use crate::sync::{get_boundary_cids, strip_boundary_duplicates, BUNDLE_SIZE};

        let next_bundle_num = self.get_last_bundle() + 1;

        // Get boundary CIDs from last bundle to prevent duplicates
        let prev_boundary = if next_bundle_num > 1 {
            let last = self.load_bundle(next_bundle_num - 1, LoadOptions::default())?;
            get_boundary_cids(&last.operations)
        } else {
            HashSet::new()
        };

        // Get last timestamp
        let mut after_time = if next_bundle_num > 1 {
            let index = self.index.read().unwrap();
            index
                .get_bundle(next_bundle_num - 1)
                .map(|m| m.end_time.clone())
                .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
        } else {
            "1970-01-01T00:00:00Z".to_string()
        };

        // Ensure mempool is initialized
        self.get_mempool()?;

        // Fetch until we have 10,000 operations
        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 20;

        while attempts < MAX_ATTEMPTS {
            let stats = self.get_mempool_stats()?;

            if stats.count >= BUNDLE_SIZE {
                break;
            }

            // Fetch operations from PLC
            let needed = BUNDLE_SIZE - stats.count;
            let plc_ops = client.fetch_operations(&after_time, needed).await?;

            if plc_ops.is_empty() {
                anyhow::bail!(
                    "Caught up: only {} operations available (need {})",
                    stats.count,
                    BUNDLE_SIZE
                );
            }

            // Convert and deduplicate
            let mut ops: Vec<Operation> = plc_ops.into_iter().map(Into::into).collect();
            ops = strip_boundary_duplicates(ops, &prev_boundary);

            if ops.is_empty() {
                anyhow::bail!("All fetched operations were duplicates");
            }

            // Add to mempool
            self.add_to_mempool(ops)?;

            // Update cursor
            if let Some(last_time) = self.get_mempool_stats()?.last_time {
                after_time = last_time.to_rfc3339();
            }

            attempts += 1;
        }

        // Take 10,000 operations and create bundle
        // TODO: Implement bundle saving
        eprintln!("TODO: Create and save bundle {}", next_bundle_num);

        Ok(next_bundle_num)
    }

    /// Run single sync cycle
    pub async fn sync_once(&mut self, client: &crate::sync::PLCClient) -> Result<usize> {
        let mut synced = 0;

        loop {
            match self.sync_next_bundle(client).await {
                Ok(_) => synced += 1,
                Err(e) if e.to_string().contains("Caught up") => break,
                Err(e) => return Err(e),
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        Ok(synced)
    }

    // === Helpers ===
    pub fn get_last_bundle(&self) -> u32 {
        self.index.read().unwrap().last_bundle
    }

    pub fn directory(&self) -> &PathBuf {
        &self.directory
    }

    fn clone_for_arc(&self) -> Self {
        Self {
            directory: self.directory.clone(),
            index: Arc::clone(&self.index),
            cache: Arc::clone(&self.cache),
            did_index: Arc::clone(&self.did_index),
            stats: Arc::clone(&self.stats),
            mempool: Arc::clone(&self.mempool),
            verbose: self.verbose,
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
            let op: Operation = serde_json::from_str(&line)?;
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

