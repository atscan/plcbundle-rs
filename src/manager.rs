// src/manager.rs
use crate::index::{Index, BundleMetadata};
use crate::operations::{Operation, OperationFilter, OperationRequest};
use crate::iterators::{QueryIterator, ExportIterator, RangeIterator};
use crate::options::QueryMode;
use crate::{cache, did_index, verification};
use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::io::Write;

pub struct BundleManager {
    directory: PathBuf,
    index: Arc<RwLock<Index>>,
    cache: Arc<cache::BundleCache>,
    did_index: Arc<RwLock<did_index::DIDIndex>>,
    stats: Arc<RwLock<ManagerStats>>,
}

#[derive(Debug, Clone, Default)]
pub struct ManagerStats {
    pub bundles_loaded: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub operations_read: u64,
    pub queries_executed: u64,
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
        let did_index = did_index::DIDIndex::load_or_create(&directory)?;
        
        Ok(Self {
            directory: directory.clone(),
            index: Arc::new(RwLock::new(index)),
            cache: Arc::new(cache::BundleCache::new(100)),
            did_index: Arc::new(RwLock::new(did_index)),
            stats: Arc::new(RwLock::new(ManagerStats::default())),
        })
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
        let mut new_index = did_index::DIDIndex::new();
        let mut stats = RebuildStats::default();
        
        for bundle_num in 1..=last_bundle {
            if let Some(ref cb) = progress_cb {
                cb(bundle_num, last_bundle);
            }
            
            if let Ok(result) = self.load_bundle(bundle_num, LoadOptions::default()) {
                for op in result.operations {
                    new_index.add_operation(bundle_num, &op.did);
                    stats.operations_indexed += 1;
                }
                stats.bundles_processed += 1;
            }
        }
        
        new_index.save(&self.directory)?;
        *self.did_index.write().unwrap() = new_index;
        
        Ok(stats)
    }

    pub fn get_did_index_stats(&self) -> DIDIndexStats {
        self.did_index.read().unwrap().get_stats()
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
