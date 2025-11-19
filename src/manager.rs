//! High-level manager orchestrating loading, random access, DID resolution, querying/export, sync, verification, rollback/migration, and mempool management
// src/manager.rs
use crate::constants::{self, bundle_position_to_global, total_operations_from_bundles};
use crate::index::{BundleMetadata, Index};
use crate::iterators::{ExportIterator, QueryIterator, RangeIterator};
use crate::operations::{Operation, OperationFilter, OperationRequest, OperationWithLocation};
use crate::options::QueryMode;
use crate::{cache, did_index, handle_resolver, mempool, verification};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

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
        fetch_wait_ms: u64,
        fetch_http_ms: u64,
    },
    /// Caught up to latest PLC data, mempool has partial operations
    CaughtUp {
        next_bundle: u32,
        mempool_count: usize,
        new_ops: usize,
        fetch_duration_ms: u64,
    },
}

/// High-level manager for PLC bundle repositories
///
/// Provides fast, memory-efficient access to operations stored in
/// compressed JSONL bundle files, along with a DID index for quick lookups.
///
/// Key capabilities:
/// - Smart loading with caching and frame-based random access
/// - DID resolution and per-DID operation queries (bundles + mempool)
/// - Batch operations, range iterators, query and export pipelines
/// - Sync to fetch, deduplicate, and create new bundles with verified hashes
/// - Maintenance utilities: warm-up, prefetch, rollback, migrate, clean
///
/// # Quickstart
///
/// ```no_run
/// use plcbundle::{BundleManager, ManagerOptions, QuerySpec, BundleRange, QueryMode};
/// use std::path::PathBuf;
///
/// // Create a manager for an existing repository
/// let mgr = BundleManager::new(PathBuf::from("/data/plc"), ManagerOptions::default())?;
///
/// // Load a bundle
/// let load = mgr.load_bundle(42, Default::default())?;
/// let _count = load.operations.len();
///
/// // Get an operation (bundle number, position within bundle)
/// let op = mgr.get_operation(42, 10)?;
/// assert!(!op.did.is_empty());
///
/// // Resolve a DID to its latest document
/// let resolved = mgr.resolve_did("did:plc:abcdef...")?;
/// println!("Resolved to shard {}", resolved.shard_num);
///
/// // Query a range and export
/// let spec = QuerySpec { bundles: BundleRange::Range(40, 45), filter: None, query: String::new(), mode: QueryMode::Simple };
/// let mut count = 0u64;
/// for item in mgr.query(spec) { count += 1; }
/// assert!(count > 0);
/// # Ok::<(), anyhow::Error>(())
/// ```
pub struct BundleManager {
    directory: PathBuf,
    index: Arc<RwLock<Index>>,
    cache: Arc<cache::BundleCache>,
    did_index: Arc<RwLock<Option<did_index::Manager>>>,
    stats: Arc<RwLock<ManagerStats>>,
    mempool: Arc<RwLock<Option<mempool::Mempool>>>,
    mempool_checked: Arc<RwLock<bool>>, // Cache whether we've checked for mempool (to avoid repeated file checks)
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
    pub operation: Operation, // The operation used for resolution
    pub bundle_number: u32,
    pub position: usize,
    pub mempool_time: std::time::Duration,
    pub mempool_load_time: std::time::Duration,
    pub index_time: std::time::Duration,
    pub load_time: std::time::Duration,
    pub total_time: std::time::Duration,
    pub locations_found: usize,
    pub shard_num: u8,
    pub shard_stats: Option<did_index::DIDLookupStats>,
    pub lookup_timings: Option<did_index::DIDLookupTimings>,
}

#[derive(Debug, Clone)]
pub struct DIDOperationsResult {
    pub operations: Vec<Operation>,
    pub operations_with_locations: Option<Vec<OperationWithLocation>>,
    pub stats: Option<did_index::DIDLookupStats>,
    pub shard_num: Option<u8>,
    pub lookup_timings: Option<did_index::DIDLookupTimings>,
    pub load_time: Option<std::time::Duration>,
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

#[derive(Debug, Clone)]
pub struct CleanResult {
    pub files_removed: usize,
    pub bytes_freed: u64,
    pub errors: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct CleanPreview {
    pub files: Vec<CleanPreviewFile>,
    pub total_size: u64,
}

#[derive(Debug, Clone)]
pub struct CleanPreviewFile {
    pub path: PathBuf,
    pub size: u64,
}

/// Options for configuring BundleManager initialization
#[derive(Debug, Clone, Default)]
pub struct ManagerOptions {
    /// Optional handle resolver URL for resolving @handle.did identifiers
    pub handle_resolver_url: Option<String>,
    /// Whether to preload the mempool at initialization (for server use)
    pub preload_mempool: bool,
    /// Whether to enable verbose logging
    pub verbose: bool,
}

/// Trait to allow passing ManagerOptions or using defaults
pub trait IntoManagerOptions {
    fn into_options(self) -> ManagerOptions;
}

impl IntoManagerOptions for ManagerOptions {
    fn into_options(self) -> ManagerOptions {
        self
    }
}

impl IntoManagerOptions for () {
    fn into_options(self) -> ManagerOptions {
        ManagerOptions::default()
    }
}

// Convenience: allow creating from individual components
impl IntoManagerOptions for (Option<String>, bool, bool) {
    fn into_options(self) -> ManagerOptions {
        ManagerOptions {
            handle_resolver_url: self.0,
            preload_mempool: self.1,
            verbose: self.2,
        }
    }
}

impl BundleManager {
    /// Create a new BundleManager
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use plcbundle::{BundleManager, ManagerOptions};
    /// use std::path::PathBuf;
    ///
    /// // With default options
    /// let manager = BundleManager::new(PathBuf::from("."), ())?;
    ///
    /// // With custom options
    /// let options = ManagerOptions {
    ///     handle_resolver_url: Some("https://example.com".to_string()),
    ///     preload_mempool: true,
    ///     verbose: true,
    /// };
    /// let manager = BundleManager::new(PathBuf::from("."), options)?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn new<O: IntoManagerOptions>(directory: PathBuf, options: O) -> Result<Self> {
        let options = options.into_options();
        let init_start = std::time::Instant::now();
        let display_dir = directory
            .canonicalize()
            .unwrap_or_else(|_| directory.clone());
        log::debug!(
            "[BundleManager] Initializing BundleManager from: {}",
            display_dir.display()
        );
        let index = Index::load(&directory)?;

        let handle_resolver = options
            .handle_resolver_url
            .map(|url| Arc::new(handle_resolver::HandleResolver::new(url)));

        if handle_resolver.is_some() {
            log::debug!("[BundleManager] Handle resolver configured");
        }

        let manager = Self {
            directory: directory.clone(),
            index: Arc::new(RwLock::new(index)),
            cache: Arc::new(cache::BundleCache::new(100)),
            did_index: Arc::new(RwLock::new(None)),
            stats: Arc::new(RwLock::new(ManagerStats::default())),
            mempool: Arc::new(RwLock::new(None)),
            mempool_checked: Arc::new(RwLock::new(false)),
            handle_resolver,
            verbose: Arc::new(Mutex::new(options.verbose)),
        };

        // Pre-initialize mempool if requested (for server use)
        if options.preload_mempool {
            let mempool_preload_start = std::time::Instant::now();
            if let Err(e) = manager.load_mempool() {
                log::debug!("[BundleManager] Mempool preload failed: {}", e);
            } else {
                let mempool_preload_time = mempool_preload_start.elapsed();
                let mempool_preload_ms = mempool_preload_time.as_secs_f64() * 1000.0;
                if let Ok(stats) = manager.get_mempool_stats()
                    && stats.count > 0
                {
                    log::debug!(
                        "[BundleManager] Pre-loaded mempool: {} operations for bundle {} ({:.3}ms)",
                        stats.count,
                        stats.target_bundle,
                        mempool_preload_ms
                    );
                }
            }
        }

        let total_elapsed = init_start.elapsed();
        let total_elapsed_ms = total_elapsed.as_secs_f64() * 1000.0;
        log::debug!(
            "[BundleManager] BundleManager initialized successfully ({:.3}ms total)",
            total_elapsed_ms
        );
        Ok(manager)
    }

    /// Ensure DID index is loaded (lazy initialization)
    pub fn ensure_did_index(&self) -> Result<()> {
        let mut did_index_guard = self.did_index.write().unwrap();
        if did_index_guard.is_none() {
            let did_index_start = std::time::Instant::now();
            log::debug!("[BundleManager] Loading DID index...");
            let did_index = did_index::Manager::new(self.directory.clone())?;
            let did_index_elapsed = did_index_start.elapsed();
            let did_index_elapsed_ms = did_index_elapsed.as_secs_f64() * 1000.0;
            log::debug!(
                "[BundleManager] DID index loaded ({:.3}ms)",
                did_index_elapsed_ms
            );
            *did_index_guard = Some(did_index);
        }
        Ok(())
    }

    /// Get a clone of the verbose state Arc for external access
    pub fn verbose_handle(&self) -> Arc<Mutex<bool>> {
        self.verbose.clone()
    }

    // === Smart Loading ===
    /// Load a bundle's operations with optional caching and filtering
    ///
    /// Uses on-disk compressed JSONL and supports frame-based random access
    /// when available, falling back to sequential scan for legacy bundles.
    ///
    /// # Arguments
    /// - `num` Bundle number to load
    /// - `options` Loading options (cache, decompress, filter, limit)
    ///
    /// # Returns
    /// A `LoadResult` containing operations and optional metadata
    pub fn load_bundle(&self, num: u32, options: LoadOptions) -> Result<LoadResult> {
        self.stats.write().unwrap().bundles_loaded += 1;

        if let Some(cached) = self.cache.get(num) {
            self.stats.write().unwrap().cache_hits += 1;
            return Ok(self.filter_load_result(cached, &options));
        }

        self.stats.write().unwrap().cache_misses += 1;

        let bundle_path = constants::bundle_path(&self.directory, num);
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
        let bundle_path = constants::bundle_path(&self.directory, bundle_num);

        if !bundle_path.exists() {
            anyhow::bail!("Bundle {} not found", bundle_num);
        }

        // Try frame-based access first (new format)
        match self.get_operation_raw_with_frames(&bundle_path, position) {
            Ok(json) => Ok(json),
            Err(e) => {
                // Fall back to legacy sequential scan
                // This happens for old bundles without frame index
                if let Ok(json) = self.get_operation_raw_legacy(&bundle_path, position) {
                    Ok(json)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Frame-based operation access (new format with metadata)
    fn get_operation_raw_with_frames(
        &self,
        bundle_path: &std::path::Path,
        position: usize,
    ) -> Result<String> {
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
        let metadata: bundle_format::BundleMetadata = sonic_rs::from_slice(&metadata_data)?;

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
    fn get_operation_raw_legacy(
        &self,
        bundle_path: &std::path::Path,
        position: usize,
    ) -> Result<String> {
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
    /// Uses sonic_rs for parsing (not serde) to avoid compatibility issues.
    /// Use `get_operation_raw()` if you only need the JSON.
    pub fn get_operation(&self, bundle_num: u32, position: usize) -> Result<Operation> {
        let json = self.get_operation_raw(bundle_num, position)?;
        let op = Operation::from_json(&json).with_context(|| {
            format!(
                "Failed to parse operation JSON (bundle {}, position {})",
                bundle_num, position
            )
        })?;
        Ok(op)
    }

    /// Get operation with timing statistics (for CLI verbose mode)
    pub fn get_operation_with_stats(
        &self,
        bundle_num: u32,
        position: usize,
    ) -> Result<OperationResult> {
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
    /// Batch fetch operations across multiple bundles using match requests
    ///
    /// Groups requests by bundle for efficient loading and returns
    /// operations that match each request's optional filter.
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

    /// Create an iterator over operations across a bundle range
    ///
    /// Returns a `RangeIterator` that lazily loads bundles between
    /// `start` and `end` and yields operations optionally filtered.
    pub fn get_operations_range(
        &self,
        start: u32,
        end: u32,
        filter: Option<OperationFilter>,
    ) -> RangeIterator {
        RangeIterator::new(Arc::new(self.clone_for_arc()), start, end, filter)
    }

    // === DID Operations ===
    /// Get all operations for a DID from both bundles and mempool
    ///
    /// # Arguments
    /// * `did` - The DID to look up
    /// * `include_locations` - If true, also return operations with location information
    /// * `include_stats` - If true, include detailed lookup statistics
    pub fn get_did_operations(
        &self,
        did: &str,
        include_locations: bool,
        include_stats: bool,
    ) -> Result<DIDOperationsResult> {
        use chrono::DateTime;
        use std::time::Instant;

        self.ensure_did_index()?;

        let index_start = Instant::now();
        let (locations, shard_stats, shard_num, lookup_timings) = if include_stats {
            let did_index = self.did_index.read().unwrap();
            did_index
                .as_ref()
                .unwrap()
                .get_did_locations_with_stats(did)?
        } else {
            let did_index = self.did_index.read().unwrap();
            let locs = did_index.as_ref().unwrap().get_did_locations(did)?;
            (
                locs,
                did_index::DIDLookupStats::default(),
                0,
                did_index::DIDLookupTimings::default(),
            )
        };
        let _index_time = index_start.elapsed();

        // Get operations from bundles
        let (bundle_ops_with_loc, load_time) = self.collect_operations_for_locations(&locations)?;
        let mut bundle_operations: Vec<Operation> = bundle_ops_with_loc
            .iter()
            .map(|owl| owl.operation.clone())
            .collect();

        // Get operations from mempool (only once)
        let (mempool_ops, _mempool_load_time) = self.get_did_operations_from_mempool(did)?;

        // Merge bundle and mempool operations
        bundle_operations.extend(mempool_ops.clone());

        // Sort by created_at timestamp
        bundle_operations.sort_by(|a, b| {
            let time_a = DateTime::parse_from_rfc3339(&a.created_at)
                .unwrap_or_else(|_| DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap());
            let time_b = DateTime::parse_from_rfc3339(&b.created_at)
                .unwrap_or_else(|_| DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap());
            time_a.cmp(&time_b)
        });

        // Build operations_with_locations if requested
        let operations_with_locations = if include_locations {
            let mut ops_with_loc = bundle_ops_with_loc;

            // Add mempool operations with bundle=0
            for (idx, op) in mempool_ops.iter().enumerate() {
                ops_with_loc.push(OperationWithLocation {
                    operation: op.clone(),
                    bundle: 0, // Use 0 to indicate mempool
                    position: idx,
                    nullified: op.nullified,
                });
            }

            // Sort all operations by timestamp
            ops_with_loc.sort_by(|a, b| {
                let time_a =
                    DateTime::parse_from_rfc3339(&a.operation.created_at).unwrap_or_else(|_| {
                        DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap()
                    });
                let time_b =
                    DateTime::parse_from_rfc3339(&b.operation.created_at).unwrap_or_else(|_| {
                        DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap()
                    });
                time_a.cmp(&time_b)
            });

            Some(ops_with_loc)
        } else {
            None
        };

        Ok(DIDOperationsResult {
            operations: bundle_operations,
            operations_with_locations,
            stats: if include_stats {
                Some(shard_stats)
            } else {
                None
            },
            shard_num: if include_stats { Some(shard_num) } else { None },
            lookup_timings: if include_stats {
                Some(lookup_timings)
            } else {
                None
            },
            load_time: if include_stats { Some(load_time) } else { None },
        })
    }

    /// Sample random DIDs directly from the DID index without reading bundles.
    pub fn sample_random_dids(&self, count: usize, seed: Option<u64>) -> Result<Vec<String>> {
        self.ensure_did_index()?;
        let did_index = self.did_index.read().unwrap();
        did_index.as_ref().unwrap().sample_random_dids(count, seed)
    }

    /// Get DID operations from mempool (internal helper)
    /// Mempool should be preloaded at initialization, so this is just a fast in-memory lookup
    /// Returns (operations, load_time) where load_time is always ZERO (no lazy loading)
    fn get_did_operations_from_mempool(
        &self,
        did: &str,
    ) -> Result<(Vec<Operation>, std::time::Duration)> {
        use std::time::Instant;

        let mempool_start = Instant::now();

        // Mempool should be preloaded at initialization (no lazy loading)
        let mempool_guard = self.mempool.read().unwrap();
        match mempool_guard.as_ref() {
            Some(mp) => {
                // Mempool is initialized, use it directly (fast HashMap lookup)
                let ops = mp.find_did_operations(did);
                let mempool_elapsed = mempool_start.elapsed();
                log::debug!(
                    "[Mempool] Found {} operations for DID {} in {:?}",
                    ops.len(),
                    did,
                    mempool_elapsed
                );
                Ok((ops, std::time::Duration::ZERO))
            }
            None => {
                // Mempool not initialized (wasn't preloaded and doesn't exist)
                let mempool_elapsed = mempool_start.elapsed();
                log::debug!(
                    "[Mempool] No mempool initialized (checked in {:?})",
                    mempool_elapsed
                );
                Ok((Vec::new(), std::time::Duration::ZERO))
            }
        }
    }

    fn get_latest_did_operation_from_mempool(
        &self,
        did: &str,
    ) -> Result<(Option<(Operation, usize)>, std::time::Duration)> {
        use std::time::Instant;

        let mempool_start = Instant::now();

        // Mempool should be preloaded at initialization (no lazy loading)
        let mempool_guard = self.mempool.read().unwrap();
        let result = match mempool_guard.as_ref() {
            Some(mp) => {
                // Use mempool's method to find latest non-nullified operation (by index, operations are sorted)
                mp.find_latest_did_operation(did)
            }
            None => {
                // Mempool not initialized
                None
            }
        };

        let mempool_elapsed = mempool_start.elapsed();
        log::debug!(
            "[Mempool] Latest operation lookup for DID {} in {:?}",
            did,
            mempool_elapsed
        );

        Ok((result, std::time::Duration::ZERO))
    }

    /// Resolve DID to current W3C DID Document with detailed timing statistics
    /// Returns the latest non-nullified DID document.
    /// If mempool has operations, uses the latest from mempool and skips bundle/index lookup.
    pub fn resolve_did(&self, did: &str) -> Result<ResolveResult> {
        use chrono::DateTime;
        use std::time::Instant;

        let total_start = Instant::now();

        // Validate DID format
        crate::resolver::validate_did_format(did)?;

        // Check mempool first (most recent operations)
        log::debug!("[Resolve] Checking mempool first for DID: {}", did);
        let mempool_start = Instant::now();
        let (latest_mempool_op, mempool_load_time) =
            self.get_latest_did_operation_from_mempool(did)?;
        let mempool_time = mempool_start.elapsed();
        log::debug!(
            "[Resolve] Mempool check: found latest operation in {:?} (load: {:?})",
            mempool_time,
            mempool_load_time
        );

        // If mempool has a non-nullified operation, use it and skip bundle lookup
        if let Some((operation, position)) = latest_mempool_op {
            let load_start = Instant::now();
            log::debug!(
                "[Resolve] Found latest non-nullified operation in mempool, skipping bundle lookup"
            );

            // Build document from latest mempool operation
            let document =
                crate::resolver::resolve_did_document(did, std::slice::from_ref(&operation))?;
            let load_time = load_start.elapsed();

            return Ok(ResolveResult {
                document,
                operation,
                bundle_number: 0, // bundle=0 for mempool
                position,
                mempool_time,
                mempool_load_time,
                index_time: std::time::Duration::ZERO,
                load_time,
                total_time: total_start.elapsed(),
                locations_found: 1, // Found one operation in mempool
                shard_num: 0,       // No shard for mempool
                shard_stats: None,
                lookup_timings: None,
            });
        }

        // Mempool is empty or all nullified - check bundles
        log::debug!(
            "[Resolve] No non-nullified operations in mempool, checking bundles for DID: {}",
            did
        );
        self.ensure_did_index()?;
        let index_start = Instant::now();
        let did_index = self.did_index.read().unwrap();
        let (locations, shard_stats, shard_num, lookup_timings) = did_index
            .as_ref()
            .unwrap()
            .get_did_locations_with_stats(did)?;
        let index_time = index_start.elapsed();
        log::debug!(
            "[Resolve] Bundle index lookup: {} locations found in {:?}",
            locations.len(),
            index_time
        );

        // Find latest non-nullified operation from bundles
        let load_start = Instant::now();
        let mut latest_operation: Option<(Operation, u32, usize)> = None;
        let mut latest_time = DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap();

        for loc in &locations {
            if !loc.nullified()
                && let Ok(op) = self.get_operation(loc.bundle() as u32, loc.position() as usize)
                && let Ok(op_time) = DateTime::parse_from_rfc3339(&op.created_at)
                && op_time > latest_time
            {
                latest_time = op_time;
                latest_operation = Some((op, loc.bundle() as u32, loc.position() as usize));
            }
        }
        let load_time = load_start.elapsed();

        let (operation, bundle_number, position) = latest_operation.ok_or_else(|| {
            anyhow::anyhow!("DID not found: {} (checked bundles and mempool)", did)
        })?;

        // Build document from latest bundle operation
        let document =
            crate::resolver::resolve_did_document(did, std::slice::from_ref(&operation))?;

        Ok(ResolveResult {
            document,
            operation: operation.clone(),
            bundle_number,
            position,
            mempool_time,
            mempool_load_time,
            index_time,
            load_time,
            total_time: total_start.elapsed(),
            locations_found: locations.len(),
            shard_num,
            shard_stats: Some(shard_stats),
            lookup_timings: Some(lookup_timings),
        })
    }

    fn collect_operations_for_locations(
        &self,
        locations: &[did_index::OpLocation],
    ) -> Result<(Vec<OperationWithLocation>, std::time::Duration)> {
        use std::time::Instant;

        let load_start = Instant::now();
        let mut ops_with_loc = Vec::with_capacity(locations.len());
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
                    log::warn!(
                        "Failed to load operation at bundle {} position {}: {}",
                        bundle_num,
                        position,
                        e
                    );
                }
            }
        }

        ops_with_loc.sort_by_key(|owl| bundle_position_to_global(owl.bundle, owl.position));

        Ok((ops_with_loc, load_start.elapsed()))
    }

    /// Resolve multiple DIDs to their operations (bundles + mempool)
    ///
    /// Returns a map of DID → operations, without location metadata or stats.
    pub fn batch_resolve_dids(&self, dids: Vec<String>) -> Result<HashMap<String, Vec<Operation>>> {
        let mut results = HashMap::new();

        for did in dids {
            let result = self.get_did_operations(&did, false, false)?;
            results.insert(did, result.operations);
        }

        Ok(results)
    }

    // === Query/Export ===
    /// Execute a query over bundles with optional filters and modes
    ///
    /// Increments internal stats and returns a `QueryIterator` that yields
    /// serialized records matching the query specification.
    pub fn query(&self, spec: QuerySpec) -> QueryIterator {
        self.stats.write().unwrap().queries_executed += 1;
        QueryIterator::new(Arc::new(self.clone_for_arc()), spec)
    }

    /// Create an export iterator for streaming results to a sink
    ///
    /// Supports JSONL format.
    pub fn export(&self, spec: ExportSpec) -> ExportIterator {
        ExportIterator::new(Arc::new(self.clone_for_arc()), spec)
    }

    /// Export results to a provided writer factory and return statistics
    ///
    /// The `writer_fn` is invoked to obtain a fresh `Write` for streaming.
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
    /// Verify a single bundle's integrity and metadata
    pub fn verify_bundle(&self, num: u32, spec: VerifySpec) -> Result<VerifyResult> {
        let index = self.index.read().unwrap();
        let metadata = index
            .get_bundle(num)
            .ok_or_else(|| anyhow::anyhow!("Bundle {} not in index", num))?;

        verification::verify_bundle(&self.directory, metadata, spec)
    }

    /// Verify chain linkage and optional parent relationships across bundles
    pub fn verify_chain(&self, spec: ChainVerifySpec) -> Result<ChainVerifyResult> {
        verification::verify_chain(&self.directory, &self.index.read().unwrap(), spec)
    }

    // === Multi-info ===
    /// Get consolidated bundle information with optional operation and size details
    pub fn get_bundle_info(&self, num: u32, flags: InfoFlags) -> Result<BundleInfo> {
        let index = self.index.read().unwrap();
        let metadata = index
            .get_bundle(num)
            .ok_or_else(|| anyhow::anyhow!("Bundle {} not found", num))?;

        let mut info = BundleInfo {
            metadata: metadata.clone(),
            exists: constants::bundle_path(&self.directory, num).exists(),
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
    /// Plan a rollback to a target bundle and report estimated impact
    pub fn rollback_plan(&self, spec: RollbackSpec) -> Result<RollbackPlan> {
        let affected_bundles: Vec<u32> = (spec.target_bundle..=self.get_last_bundle()).collect();

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
            affected_bundles: affected_bundles.clone(), // Clone here
            affected_operations,
            affected_dids: affected_dids.len(),
            estimated_time_ms: affected_bundles.len() as u64 * 10,
        })
    }

    /// Execute a rollback to the target bundle, optionally as a dry run
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
            let path = constants::bundle_path(&self.directory, *bundle_num);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            self.cache.remove(*bundle_num);
        }

        let mut index = self.index.write().unwrap();
        index.last_bundle = spec.target_bundle;
        index
            .bundles
            .retain(|b| b.bundle_number <= spec.target_bundle);

        // Use default flush interval for rollback
        self.build_did_index(
            crate::constants::DID_INDEX_FLUSH_INTERVAL,
            None::<fn(u32, u32, u64, u64)>,
            None,
            None,
        )?;

        Ok(RollbackResult {
            success: true,
            bundles_removed: plan.affected_bundles.len(),
            plan: Some(plan),
        })
    }

    // === Cache Hints ===
    /// Preload specified bundles into the cache for faster subsequent access
    pub fn prefetch_bundles(&self, nums: Vec<u32>) -> Result<()> {
        for num in nums {
            self.load_bundle(
                num,
                LoadOptions {
                    cache: true,
                    ..Default::default()
                },
            )?;
        }
        Ok(())
    }

    /// Warm up caches according to strategy (recent, range, all)
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
    pub fn build_did_index<F>(
        &self,
        flush_interval: u32,
        progress_cb: Option<F>,
        num_threads: Option<usize>,
        interrupted: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<RebuildStats>
    where
        F: Fn(u32, u32, u64, u64) + Send + Sync, // (current, total, bytes_processed, total_bytes)
    {
        use std::time::Instant;

        let actual_threads = num_threads.unwrap_or(0); // 0 = auto-detect

        let last_bundle = self.get_last_bundle();
        let mut stats = RebuildStats::default();

        // Create new index (this clears any existing index)
        let new_index = did_index::Manager::new(self.directory.clone())?;
        *self.did_index.write().unwrap() = Some(new_index);

        self.ensure_did_index()?;

        // Get total uncompressed size for progress tracking
        let index = self.index.read().unwrap();
        let bundle_numbers: Vec<u32> = (1..=last_bundle).collect();
        let total_uncompressed_bytes = index.total_uncompressed_size_for_bundles(&bundle_numbers);
        drop(index);

        eprintln!("\n📦 Building DID Index");
        eprintln!("   Strategy: Streaming (memory-efficient)");
        eprintln!("   Bundles:  {}", last_bundle);
        if flush_interval > 0 {
            if flush_interval == crate::constants::DID_INDEX_FLUSH_INTERVAL {
                // Default value - show with tuning hint
                eprintln!(
                    "   Flush:    Every {} bundles (tune with --flush-interval)",
                    flush_interval
                );
            } else {
                // Non-default value - show with tuning hint
                eprintln!(
                    "   Flush:    {} bundles (you can tune with --flush-interval)",
                    flush_interval
                );
            }
        } else {
            eprintln!("   Flush:    Only at end (maximum memory usage)");
        }
        eprintln!();
        eprintln!("📊 Stage 1: Processing bundles...");

        let build_start = Instant::now();

        // Call the streaming build method in did_index
        let (total_operations, _bundles_processed, stage1_duration, stage2_duration) = {
            let did_index_guard = self.did_index.read().unwrap();
            if let Some(ref idx) = *did_index_guard {
                idx.build_from_scratch(
                    &self.directory,
                    last_bundle,
                    flush_interval,
                    progress_cb.map(|cb| {
                        move |current: u32, total: u32, bytes: u64, stage: Option<String>| {
                            // Always call the callback - let it handle stage detection
                            // For stage 1, use bytes tracking; for stage 2, use shard count
                            if let Some(ref stage_name) = stage {
                                if stage_name.contains("Stage 2") {
                                    // For consolidation, we don't have byte tracking, so just pass 0
                                    // The progress bar will show shard progress
                                    cb(current, total, 0, total_uncompressed_bytes);
                                } else {
                                    // Stage 1 or unknown - use bytes
                                    cb(current, total, bytes, total_uncompressed_bytes);
                                }
                            } else {
                                // Fallback for backward compatibility
                                cb(current, total, bytes, total_uncompressed_bytes);
                            }
                        }
                    }),
                    actual_threads,
                    interrupted,
                )?
            } else {
                return Err(anyhow::anyhow!("DID index not initialized"));
            }
        };

        stats.bundles_processed = last_bundle;
        stats.operations_indexed = total_operations;

        let total_duration = build_start.elapsed();

        eprintln!("\n");
        eprintln!("✅ Index Build Complete");
        eprintln!(
            "   Time:       {:.1}s (Stage 1: {:.1}s, Stage 2: {:.1}s)",
            total_duration.as_secs_f64(),
            stage1_duration.as_secs_f64(),
            stage2_duration.as_secs_f64()
        );
        eprintln!(
            "   Operations: {}",
            crate::format::format_number(total_operations)
        );

        // Get final stats
        let final_stats = self.get_did_index_stats();
        let total_dids = final_stats
            .get("total_dids")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        eprintln!(
            "   Total DIDs: {}",
            crate::format::format_number(total_dids as u64)
        );

        Ok(stats)
    }

    /// Get DID index statistics as a JSON-compatible map
    ///
    /// Returns keys like `exists`, `total_dids`, `last_bundle`, `delta_segments`, `shard_count` when available.
    pub fn get_did_index_stats(&self) -> HashMap<String, serde_json::Value> {
        self.ensure_did_index().ok(); // Stats might be called even if index doesn't exist
        self.did_index
            .read()
            .unwrap()
            .as_ref()
            .map(|idx| idx.get_stats())
            .unwrap_or_default()
    }

    /// Get DID index stats as struct (legacy format)
    pub fn get_did_index_stats_struct(&self) -> DIDIndexStats {
        let stats_map = self.get_did_index_stats();

        // Convert to old format
        DIDIndexStats {
            total_dids: stats_map
                .get("total_dids")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as usize,
            total_entries: 0,            // Not tracked in new version
            avg_operations_per_did: 0.0, // Not tracked in new version
        }
    }

    pub fn get_did_index(&self) -> Arc<RwLock<Option<did_index::Manager>>> {
        Arc::clone(&self.did_index)
    }

    /// Verify DID index and return detailed result
    ///
    /// Performs standard integrity check by default. If `full` is true, also rebuilds
    /// the index in a temporary directory and compares with the existing index.
    ///
    /// For server startup checks, call with `full=false` and check `verify_result.missing_base_shards`
    /// and `verify_result.missing_delta_segments` to determine if the index is corrupted.
    pub fn verify_did_index<F>(
        &self,
        verbose: bool,
        flush_interval: u32,
        full: bool,
        progress_callback: Option<F>,
    ) -> Result<did_index::VerifyResult>
    where
        F: Fn(u32, u32, u64, u64) + Send + Sync, // (current, total, bytes_processed, total_bytes)
    {
        self.ensure_did_index()?;

        let did_index = self.did_index.read().unwrap();
        let idx = did_index
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("DID index not initialized"))?;

        let last_bundle = self.get_last_bundle();
        let mut verify_result = idx.verify_integrity(last_bundle)?;

        // If full verification requested, rebuild and compare
        if full {
            // Adapt callback for build_from_scratch which expects Option<String> as 4th param
            let build_callback = progress_callback.map(|cb| {
                move |current: u32, total: u32, bytes: u64, _stage: Option<String>| {
                    cb(current, total, bytes, bytes); // Use bytes as total_bytes for now
                }
            });
            let rebuild_result = idx.verify_full(
                self.directory(),
                last_bundle,
                verbose,
                flush_interval,
                build_callback,
            )?;
            verify_result.errors += rebuild_result.errors;
            verify_result
                .error_categories
                .extend(rebuild_result.error_categories);
        }

        Ok(verify_result)
    }

    /// Repair DID index - intelligently rebuilds or updates as needed
    pub fn repair_did_index<F>(
        &self,
        num_threads: usize,
        flush_interval: u32,
        progress_callback: Option<F>,
    ) -> Result<did_index::RepairResult>
    where
        F: Fn(u32, u32, u64, u64) + Send + Sync, // (current, total, bytes_processed, total_bytes)
    {
        self.ensure_did_index()?;

        let last_bundle = self.get_last_bundle();

        // Create bundle loader closure
        let bundle_loader = |bundle_num: u32| -> Result<Vec<(String, bool)>> {
            let result = self.load_bundle(bundle_num, LoadOptions::default())?;
            Ok(result
                .operations
                .iter()
                .map(|op| (op.did.clone(), op.nullified))
                .collect())
        };

        let mut did_index = self.did_index.write().unwrap();
        let idx = did_index
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("DID index not initialized"))?;

        let mut repair_result = idx.repair(last_bundle, bundle_loader)?;

        // If repair indicates full rebuild is needed, do it
        if repair_result.repaired && repair_result.bundles_processed == 0 {
            drop(did_index);

            // Adapt callback signature for build_did_index
            let build_callback = progress_callback.map(|cb| {
                move |current: u32, total: u32, bytes: u64, total_bytes: u64| {
                    cb(current, total, bytes, total_bytes);
                }
            });
            self.build_did_index(flush_interval, build_callback, Some(num_threads), None)?;

            repair_result.bundles_processed = last_bundle;
        }

        Ok(repair_result)
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

    /// Check if the mempool is loaded (does not load it)
    ///
    /// Returns `Ok(())` if mempool is loaded, error otherwise.
    pub fn get_mempool(&self) -> Result<()> {
        let mempool_guard = self.mempool.read().unwrap();
        if mempool_guard.is_some() {
            Ok(())
        } else {
            anyhow::bail!("Mempool not loaded. Call load_mempool() first.")
        }
    }

    /// Explicitly load mempool from disk (or create empty if file doesn't exist)
    ///
    /// Intended for initialization/preload, not lazy loading.
    pub fn load_mempool(&self) -> Result<()> {
        // Check if already loaded
        {
            let mempool_guard = self.mempool.read().unwrap();
            if mempool_guard.is_some() {
                return Ok(()); // Already loaded
            }
        }

        // Acquire write lock to load
        let mut mempool_guard = self.mempool.write().unwrap();

        // Double-check after acquiring write lock
        if mempool_guard.is_some() {
            return Ok(());
        }

        // Load mempool
        let last_bundle = self.get_last_bundle();
        let target_bundle = last_bundle + 1;

        // Get min timestamp from last bundle's last operation
        let min_timestamp = self.get_last_bundle_timestamp()?;

        // Mempool::new will check if file exists and load it if it does
        // If file doesn't exist, it creates an empty mempool
        match mempool::Mempool::new(
            &self.directory,
            target_bundle,
            min_timestamp,
            *self.verbose.lock().unwrap(),
        ) {
            Ok(mp) => {
                // Mempool loaded (either from file or empty)
                *mempool_guard = Some(mp);
                *self.mempool_checked.write().unwrap() = true;
            }
            Err(e) => {
                // Mempool file doesn't exist or error loading
                // Mark as checked so we don't try again
                *self.mempool_checked.write().unwrap() = true;
                // Return error only if it's not a "file not found" type error
                if e.to_string().contains("No such file") || e.to_string().contains("not found") {
                    // File doesn't exist, that's fine - just return Ok with None mempool
                    return Ok(());
                }
                return Err(e);
            }
        }

        Ok(())
    }

    /// Get mempool statistics including counts and time bounds
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

    /// Get all operations currently in the mempool
    pub fn get_mempool_operations(&self) -> Result<Vec<Operation>> {
        let mempool_guard = self.mempool.read().unwrap();

        match mempool_guard.as_ref() {
            Some(mp) => Ok(mp.get_operations().to_vec()),
            None => Ok(Vec::new()),
        }
    }

    /// Clear all mempool data and remove on-disk mempool files
    pub fn clear_mempool(&self) -> Result<()> {
        let mut mempool_guard = self.mempool.write().unwrap();

        if let Some(mp) = mempool_guard.as_mut() {
            mp.clear();
            mp.save()?;
        }

        // Also delete all mempool files to prevent stale data from previous bundles
        if let Ok(entries) = std::fs::read_dir(&self.directory) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str()
                    && name.starts_with(constants::MEMPOOL_FILE_PREFIX)
                    && name.ends_with(".jsonl")
                {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }

        Ok(())
    }

    /// Add operations to mempool, returning number added
    ///
    /// Mempool must be loaded first (call `load_mempool()`).
    pub fn add_to_mempool(&self, ops: Vec<Operation>) -> Result<usize> {
        self.get_mempool()?; // Check if loaded

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
            let timestamp = DateTime::parse_from_rfc3339(&last_op.created_at)?.with_timezone(&Utc);
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
                if let Some(name) = entry.file_name().to_str()
                    && name.starts_with(constants::MEMPOOL_FILE_PREFIX)
                    && name.ends_with(".jsonl")
                {
                    // Extract bundle number from filename: plc_mempool_NNNNNN.jsonl
                    if let Some(num_str) = name
                        .strip_prefix(constants::MEMPOOL_FILE_PREFIX)
                        .and_then(|s| s.strip_suffix(".jsonl"))
                        && let Ok(bundle_num) = num_str.parse::<u32>()
                    {
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
        let last_bundle_time = if next_bundle_num > 1
            && let Ok(last_bundle_result) =
                self.load_bundle(next_bundle_num - 1, LoadOptions::default())
        {
            last_bundle_result.operations.last().and_then(|last_op| {
                chrono::DateTime::parse_from_rfc3339(&last_op.created_at)
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
            })
        } else {
            None
        };

        // Special case: When creating the first bundle (next_bundle_num == 1, meaning
        // last_bundle == 0, i.e., empty repository), any existing mempool is likely stale
        // from a previous sync attempt. Clear it to start fresh from the beginning.
        if next_bundle_num == 1 && mempool_stats.count > 0 {
            log::warn!(
                "Starting first bundle (empty repository), but mempool has {} operations",
                mempool_stats.count
            );
            if let Some(first_time) = mempool_stats.first_time {
                log::warn!(
                    "Mempool operations start at: {}",
                    first_time.format("%Y-%m-%d %H:%M:%S")
                );
            }
            log::warn!("Clearing mempool to start fresh from the beginning...");
            self.clear_mempool()?;
            return Ok(());
        }

        // Check if mempool operations are chronologically valid relative to last bundle
        if let Some(last_time) = last_bundle_time
            && let Some(first_mempool_time) = mempool_stats.first_time
        {
            // Case 1: Mempool operations are BEFORE the last bundle (definitely stale)
            if first_mempool_time < last_time {
                log::warn!("Detected stale mempool data (operations before last bundle)");
                log::warn!(
                    "First mempool op: {}, Last bundle op: {}",
                    first_mempool_time.format("%Y-%m-%d %H:%M:%S"),
                    last_time.format("%Y-%m-%d %H:%M:%S")
                );
                log::warn!("Clearing mempool to start fresh...");
                self.clear_mempool()?;
                return Ok(());
            }

            // Case 2: Mempool operations are slightly after last bundle, but way too close
            // This indicates they're from a previous failed attempt at this bundle
            // BUT: Only clear if the mempool file is old (modified > 1 hour ago)
            // If it's recent, it might be a legitimate resume of a slow sync
            let time_diff = first_mempool_time.signed_duration_since(last_time);
            if time_diff < chrono::Duration::seconds(constants::MIN_BUNDLE_CREATION_INTERVAL_SECS)
                && mempool_stats.count < constants::BUNDLE_SIZE
            {
                // Check mempool file modification time
                let mempool_filename = format!(
                    "{}{:06}.jsonl",
                    constants::MEMPOOL_FILE_PREFIX,
                    next_bundle_num
                );
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
                    log::warn!(
                        "Detected potentially stale mempool data (too close to last bundle timestamp)"
                    );
                    log::warn!(
                        "Time difference: {}s, Operations: {}/{}",
                        time_diff.num_seconds(),
                        mempool_stats.count,
                        constants::BUNDLE_SIZE
                    );
                    log::warn!(
                        "This likely indicates a previous failed sync attempt. Clearing mempool..."
                    );
                    self.clear_mempool()?;
                } else if *self.verbose.lock().unwrap() {
                    log::debug!("Mempool appears recent, allowing resume despite close timestamp");
                }
                return Ok(());
            }
        }

        // Check if mempool has way too many operations (likely from failed previous attempt)
        if mempool_stats.count > constants::BUNDLE_SIZE {
            log::warn!(
                "Mempool has {} operations (expected max {})",
                mempool_stats.count,
                constants::BUNDLE_SIZE
            );
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
    pub fn batch_update_did_index(
        &self,
        start_bundle: u32,
        end_bundle: u32,
        compact: bool,
    ) -> Result<()> {
        use std::time::Instant;

        if start_bundle > end_bundle {
            return Ok(());
        }

        let total_start = Instant::now();
        let bundle_count = end_bundle - start_bundle + 1;
        if bundle_count > 10 {
            use std::time::Instant;
            eprintln!(
                "[DID Index] Rebuild triggered for {} bundles ({} → {})",
                bundle_count, start_bundle, end_bundle
            );
            let rebuild_start = Instant::now();
            let _ = self.build_did_index(
                crate::constants::DID_INDEX_FLUSH_INTERVAL,
                Some(
                    |current: u32, total: u32, bytes_processed: u64, total_bytes: u64| {
                        let percent = if total_bytes > 0 {
                            (bytes_processed as f64 / total_bytes as f64) * 100.0
                        } else {
                            0.0
                        };
                        eprintln!(
                            "[DID Index] Rebuild progress: {}/{} ({:.1}%)",
                            current, total, percent
                        );
                    },
                ),
                None,
                None,
            )?;
            let dur = rebuild_start.elapsed();
            eprintln!("[DID Index] Rebuild complete in {:.1}s", dur.as_secs_f64());
            return Ok(());
        }

        if *self.verbose.lock().unwrap() {
            log::info!(
                "Batch updating DID index for bundles {:06} to {:06}... ({} bundles)",
                start_bundle,
                end_bundle,
                bundle_count
            );
        }

        // Process bundles incrementally (avoid loading all into memory)
        let load_start = Instant::now();
        let mut total_operations = 0usize;
        let mut bundles_processed = 0usize;

        // Update DID index for each bundle as we load it (memory efficient)
        self.ensure_did_index()?;
        let update_start = Instant::now();
        for bundle_num in start_bundle..=end_bundle {
            if let Ok(result) = self.load_bundle(bundle_num, LoadOptions::default()) {
                total_operations += result.operations.len();
                let operations: Vec<(String, bool)> = result
                    .operations
                    .iter()
                    .map(|op| (op.did.clone(), op.nullified))
                    .collect();

                // Process immediately instead of accumulating
                let _ = self
                    .did_index
                    .write()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .update_for_bundle(bundle_num, operations)?;
                bundles_processed += 1;
            }
        }
        let load_duration = load_start.elapsed();
        let update_duration = update_start.elapsed();

        if bundles_processed == 0 {
            return Ok(());
        }

        log::debug!(
            "[Batch DID Index] Processed {} bundles ({} operations) in {:.3}s ({:.0} ops/sec)",
            bundles_processed,
            total_operations,
            update_duration.as_secs_f64(),
            total_operations as f64 / update_duration.as_secs_f64()
        );

        // Optionally compact all shards immediately to avoid leaving delta segments
        if compact {
            let idx_guard = self.did_index.read().unwrap();
            if let Some(idx) = idx_guard.as_ref() {
                idx.compact_pending_segments(None)?;
            }
        }

        let total_duration = total_start.elapsed();

        if *self.verbose.lock().unwrap() {
            log::info!(
                "✓ DID index updated for bundles {:06} to {:06} in {:.3}s (load={:.1}s, update={:.1}s, {:.0} ops/sec overall)",
                start_bundle,
                end_bundle,
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
    pub async fn batch_update_did_index_async(
        &self,
        start_bundle: u32,
        end_bundle: u32,
        compact: bool,
    ) -> Result<()> {
        let manager = self.clone_for_arc();

        // First perform the batch update in a blocking task
        let _ = tokio::task::spawn_blocking(move || {
            manager.batch_update_did_index(start_bundle, end_bundle, compact)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Batch DID index update task failed: {}", e))?;

        Ok(())
    }

    /// Fetch and save next bundle from PLC directory
    /// DID index is updated on every bundle (fast with delta segments)
    pub async fn sync_next_bundle(
        &self,
        client: &crate::plc_client::PLCClient,
        shutdown_rx: Option<tokio::sync::watch::Receiver<bool>>,
        update_did_index: bool,
    ) -> Result<SyncResult> {
        use crate::sync::{get_boundary_cids, strip_boundary_duplicates};
        use std::time::Instant;

        // Validate repository state before starting
        self.validate_sync_state()?;

        let next_bundle_num = self.get_last_bundle() + 1;

        // ALWAYS get boundaries from last bundle initially
        let (mut after_time, mut prev_boundary) = if next_bundle_num > 1 {
            let last = self.load_bundle(
                next_bundle_num - 1,
                LoadOptions {
                    cache: false,
                    decompress: true,
                    filter: None,
                    limit: None,
                },
            )?;
            let boundary = get_boundary_cids(&last.operations);
            let cursor = last
                .operations
                .last()
                .map(|op| op.created_at.clone())
                .unwrap_or_default();

            if *self.verbose.lock().unwrap() {
                log::info!(
                    "Loaded {} boundary CIDs from bundle {:06} (at {})",
                    boundary.len(),
                    next_bundle_num - 1,
                    cursor
                );
            }

            (cursor, boundary)
        } else {
            ("1970-01-01T00:00:00Z".to_string(), HashSet::new())
        };

        // If mempool has operations, update cursor AND boundaries from mempool
        // (mempool operations already had boundary dedup applied when they were added)
        let mempool_stats = self.get_mempool_stats()?;
        if mempool_stats.count > 0
            && let Some(last_time) = mempool_stats.last_time
        {
            if *self.verbose.lock().unwrap() {
                log::debug!(
                    "Mempool has {} ops, resuming from {}",
                    mempool_stats.count,
                    last_time.format("%Y-%m-%dT%H:%M:%S")
                );
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

        log::debug!(
            "Preparing bundle {:06} (mempool: {} ops)...",
            next_bundle_num,
            mempool_stats.count
        );
        log::debug!(
            "Starting cursor: {}",
            if after_time.is_empty() || after_time == "1970-01-01T00:00:00Z" {
                ""
            } else {
                &after_time
            }
        );

        if !prev_boundary.is_empty() && *self.verbose.lock().unwrap() && mempool_stats.count == 0 {
            log::info!(
                "  Starting with {} boundary CIDs from previous bundle",
                prev_boundary.len()
            );
        }

        // Ensure mempool is loaded (load if needed)
        self.load_mempool()?;

        // Fetch until we have 10,000 operations
        let mut fetch_num = 0;
        let mut total_fetched = 0;
        let mut total_dupes = 0;
        let mut total_boundary_dupes = 0;
        let fetch_start = Instant::now();
        let mut caught_up = false;
        const MAX_ATTEMPTS: usize = 50;
        let mut total_wait = std::time::Duration::from_secs(0);
        let mut total_http = std::time::Duration::from_secs(0);

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
                log::info!(
                    "  Fetch #{}: requesting {} (need {} more, have {}/{})",
                    fetch_num,
                    request_count,
                    needed,
                    stats.count,
                    constants::BUNDLE_SIZE
                );
            }

            let fetch_op_start = Instant::now();
            if let Some(ref rx) = shutdown_rx
                && *rx.borrow()
            {
                anyhow::bail!("Shutdown requested");
            }
            let (plc_ops, wait_dur, http_dur) = if let Some(rx) = shutdown_rx.clone() {
                client
                    .fetch_operations_cancelable(&after_time, request_count, Some(rx))
                    .await?
            } else {
                client.fetch_operations(&after_time, request_count).await?
            };
            total_wait += wait_dur;
            total_http += http_dur;

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
                    log::info!(
                        "  Stripped {} boundary duplicates from fetch",
                        boundary_removed
                    );
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
                    log::info!(
                        "  → +{} unique ({} dupes, {} boundary) in {:.9}s • Running: {}/{} ({:.0} ops/sec)",
                        added,
                        dupes_in_fetch,
                        boundary_removed,
                        fetch_duration.as_secs_f64(),
                        new_stats.count,
                        constants::BUNDLE_SIZE,
                        ops_per_sec
                    );
                } else {
                    log::info!(
                        "  → +{} unique in {:.9}s • Running: {}/{} ({:.0} ops/sec)",
                        added,
                        fetch_duration.as_secs_f64(),
                        new_stats.count,
                        constants::BUNDLE_SIZE,
                        ops_per_sec
                    );
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
            log::info!(
                "  ✓ Collected {} unique ops from {} fetches ({:.1}% dedup)",
                final_stats.count,
                fetch_num,
                dedup_pct
            );
        }

        // Take operations and create bundle
        log::debug!(
            "Calling operations.SaveBundle with bundle={}",
            next_bundle_num
        );

        let operations = {
            let mut mempool = self.mempool.write().unwrap();
            let mem = mempool
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("Mempool not initialized"))?;
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
        let (
            serialize_time,
            compress_time,
            hash_time,
            did_index_time,
            index_write_time,
            did_index_compacted,
        ) = self
            .save_bundle_with_timing(next_bundle_num, operations, update_did_index)
            .await?;
        let save_duration = save_start.elapsed();

        // Show timing breakdown in verbose mode only
        if *self.verbose.lock().unwrap() {
            log::debug!(
                "  Save timing: serialize={:.3}ms, compress={:.3}ms, hash={:.3}ms, did_index={:.3}ms, index_write={:.3}ms, total={:.1}ms",
                serialize_time.as_secs_f64() * 1000.0,
                compress_time.as_secs_f64() * 1000.0,
                hash_time.as_secs_f64() * 1000.0,
                did_index_time.as_secs_f64() * 1000.0,
                index_write_time.as_secs_f64() * 1000.0,
                save_duration.as_secs_f64() * 1000.0
            );
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

            (
                hash,
                age_str,
                bundle_meta.did_count,
                bundle_meta.compressed_size,
            )
        };

        // Get mempool count after clearing (should be 0, but check anyway)
        let mempool_count = self.get_mempool_stats().map(|s| s.count).unwrap_or(0);
        let total_duration_ms = (fetch_total_duration + save_duration).as_millis() as u64;
        let fetch_duration_ms = fetch_total_duration.as_millis() as u64;

        // Calculate separate timings: bundle save vs index write/DID index
        let (bundle_save_ms, index_ms) = if update_did_index {
            (
                (serialize_time + compress_time + hash_time).as_millis() as u64,
                (did_index_time + index_write_time).as_millis() as u64,
            )
        } else {
            (
                (serialize_time + compress_time + hash_time + index_write_time).as_millis() as u64,
                0,
            )
        };

        // Only log detailed info in verbose mode
        if *self.verbose.lock().unwrap() {
            log::info!(
                "→ Bundle {:06} | {} | fetch: {:.3}s ({} reqs) | {}",
                next_bundle_num,
                short_hash,
                fetch_total_duration.as_secs_f64(),
                fetch_num,
                age_str
            );
            log::debug!(
                "Bundle done = {}, finish duration = {:.3}ms",
                next_bundle_num,
                save_duration.as_secs_f64() * 1000.0
            );
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
            fetch_wait_ms: total_wait.as_millis() as u64,
            fetch_http_ms: total_http.as_millis() as u64,
        })
    }

    /// Run single sync cycle
    ///
    /// If max_bundles is Some(n), stop after syncing n bundles
    /// If max_bundles is None, sync until caught up
    pub async fn sync_once(
        &self,
        client: &crate::plc_client::PLCClient,
        max_bundles: Option<usize>,
    ) -> Result<usize> {
        let mut synced = 0;

        loop {
            match self.sync_next_bundle(client, None, true).await {
                Ok(SyncResult::BundleCreated { .. }) => {
                    synced += 1;

                    // Check if we've reached the limit
                    if let Some(max) = max_bundles
                        && synced >= max
                    {
                        break;
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
    async fn save_bundle_with_timing(
        &self,
        bundle_num: u32,
        operations: Vec<Operation>,
        update_did_index: bool,
    ) -> Result<(
        std::time::Duration,
        std::time::Duration,
        std::time::Duration,
        std::time::Duration,
        std::time::Duration,
        bool,
    )> {
        use anyhow::Context;
        use std::collections::HashSet;
        use std::fs::File;
        use std::io::Write;
        use std::time::Instant;

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

        // Compress operations to frames using parallel compression
        let compress_result =
            crate::bundle_format::compress_operations_to_frames_parallel(&operations)?;

        let serialize_time =
            std::time::Duration::from_secs_f64(compress_result.serialize_time_ms / 1000.0);
        let compress_time =
            std::time::Duration::from_secs_f64(compress_result.compress_time_ms / 1000.0);

        let uncompressed_size = compress_result.uncompressed_size;
        let compressed_size = compress_result.compressed_size;
        let frame_count = compress_result.compressed_frames.len();
        let frame_offsets = compress_result.frame_offsets;
        let compressed_frames = compress_result.compressed_frames;

        // Calculate content hash from uncompressed data
        let hash_start = Instant::now();
        let content_hash = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            let mut missing_raw_json = 0;

            // Hash all operations in order (reconstructing uncompressed JSONL)
            for op in &operations {
                let json = if let Some(raw) = &op.raw_json {
                    raw.clone()
                } else {
                    missing_raw_json += 1;
                    if missing_raw_json == 1 && *self.verbose.lock().unwrap() {
                        log::warn!(
                            "⚠️  Bundle {}: Operation missing raw_json, using re-serialized JSON (may cause hash mismatch!)",
                            bundle_num
                        );
                    }
                    sonic_rs::to_string(op)?
                };
                hasher.update(json.as_bytes());
                hasher.update(b"\n");
            }

            if missing_raw_json > 0 && *self.verbose.lock().unwrap() {
                log::warn!(
                    "⚠️  Bundle {}: {} operations missing raw_json (content hash may be incorrect!)",
                    bundle_num,
                    missing_raw_json
                );
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
            use sha2::{Digest, Sha256};
            let parent_chain_hash = self
                .index
                .read()
                .unwrap()
                .get_bundle(bundle_num - 1)
                .map(|b| b.hash.clone())
                .unwrap_or_default();

            // Debug logging for hash calculation issues
            if parent_chain_hash.is_empty() {
                log::warn!(
                    "⚠️  Bundle {}: Parent bundle {} not found in index! Using empty parent hash.",
                    bundle_num,
                    bundle_num - 1
                );
            } else if *self.verbose.lock().unwrap() {
                log::debug!(
                    "Bundle {}: Parent hash from bundle {}: {}",
                    bundle_num,
                    bundle_num - 1,
                    &parent_chain_hash[..16]
                );
                log::debug!(
                    "Bundle {}: Content hash: {}",
                    bundle_num,
                    &content_hash[..16]
                );
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
            use sha2::{Digest, Sha256};
            let chain_input = format!("plcbundle:genesis:{}", content_hash);
            let mut hasher = Sha256::new();
            hasher.update(chain_input.as_bytes());
            let hash = format!("{:x}", hasher.finalize());

            (String::new(), hash)
        };

        // Get cursor (end_time of previous bundle per spec)
        // For the first bundle, cursor is empty string
        let cursor = if bundle_num > 1 {
            let prev_end_time = self
                .index
                .read()
                .unwrap()
                .get_bundle(bundle_num - 1)
                .map(|b| b.end_time.clone())
                .unwrap_or_default();

            // Validate cursor matches previous bundle's end_time
            if prev_end_time.is_empty() {
                log::warn!(
                    "⚠️  Bundle {}: Previous bundle {} has empty end_time, cursor will be empty",
                    bundle_num,
                    bundle_num - 1
                );
            }

            prev_end_time
        } else {
            String::new()
        };

        // Validate cursor correctness (for non-genesis bundles)
        if bundle_num > 1 {
            let expected_cursor = {
                let index = self.index.read().unwrap();
                index
                    .get_bundle(bundle_num - 1)
                    .map(|b| b.end_time.clone())
                    .unwrap_or_default()
            };
            if cursor != expected_cursor {
                anyhow::bail!(
                    "Cursor validation failed for bundle {}: expected {} (previous bundle end_time), got {}",
                    bundle_num,
                    expected_cursor,
                    cursor
                );
            }
        } else if !cursor.is_empty() {
            anyhow::bail!(
                "Cursor validation failed for bundle {} (genesis): cursor should be empty, got {}",
                bundle_num,
                cursor
            );
        }

        // Prepare bundle metadata for skippable frame
        let bundle_metadata_frame = crate::bundle_format::BundleMetadata {
            format: "plcbundle/1.0".to_string(),
            bundle_number: bundle_num,
            origin: self.index.read().unwrap().origin.clone(),
            content_hash: content_hash.clone(),
            parent_hash: if !parent.is_empty() {
                Some(parent.clone())
            } else {
                None
            },
            uncompressed_size: Some(uncompressed_size),
            compressed_size: Some(compressed_size),
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
        let bundle_path = constants::bundle_path(&self.directory, bundle_num);
        let bundle_path_clone = bundle_path.clone();
        let bundle_metadata_frame_clone = bundle_metadata_frame.clone();
        let compressed_frames_clone = compressed_frames.clone();

        // Write file first (metadata frame doesn't contain compressed_hash, so we can write it)
        tokio::task::spawn_blocking({
            let bundle_path_clone = bundle_path_clone.clone();
            let bundle_metadata_frame_clone = bundle_metadata_frame_clone.clone();
            let compressed_frames_clone = compressed_frames_clone.clone();
            move || {
                let mut file = File::create(&bundle_path_clone).with_context(|| {
                    format!(
                        "Failed to create bundle file: {}",
                        bundle_path_clone.display()
                    )
                })?;

                // Write metadata as skippable frame first
                crate::bundle_format::write_metadata_frame(&mut file, &bundle_metadata_frame_clone)
                    .with_context(|| {
                        format!(
                            "Failed to write metadata frame to: {}",
                            bundle_path_clone.display()
                        )
                    })?;

                // Write all compressed frames
                for frame in &compressed_frames_clone {
                    file.write_all(frame).with_context(|| {
                        format!(
                            "Failed to write compressed frame to: {}",
                            bundle_path_clone.display()
                        )
                    })?;
                }
                file.flush().with_context(|| {
                    format!(
                        "Failed to flush bundle file: {}",
                        bundle_path_clone.display()
                    )
                })?;

                Ok::<(), anyhow::Error>(())
            }
        })
        .await
        .context("Bundle file write task failed")??;

        // Now calculate compressed_hash from the entire file (as verification does)
        let compressed_hash = tokio::task::spawn_blocking({
            let bundle_path_clone = bundle_path_clone.clone();
            move || {
                use sha2::{Digest, Sha256};
                let file_data = std::fs::read(&bundle_path_clone).with_context(|| {
                    format!(
                        "Failed to read bundle file for hash: {}",
                        bundle_path_clone.display()
                    )
                })?;

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

        let (did_index_time, did_index_compacted) = if update_did_index {
            let did_index_start = Instant::now();
            let did_ops: Vec<(String, bool)> = operations
                .iter()
                .map(|op| (op.did.clone(), op.nullified))
                .collect();

            self.ensure_did_index()?;
            let compacted = self
                .did_index
                .write()
                .unwrap()
                .as_mut()
                .unwrap()
                .update_for_bundle(bundle_num, did_ops)?;
            (did_index_start.elapsed(), compacted)
        } else {
            (std::time::Duration::from_millis(0), false)
        };

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
        let index_clone = {
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
        // Use Index::save() which does atomic write (temp file + rename)
        let directory = self.directory.clone();
        tokio::task::spawn_blocking(move || index_clone.save(directory))
            .await
            .context("Index write task failed")??;
        let index_write_time = index_write_start.elapsed();

        Ok((
            serialize_time,
            compress_time,
            hash_time,
            did_index_time,
            index_write_time,
            did_index_compacted,
        ))
    }

    /// Migrate a bundle to multi-frame format
    ///
    /// This method loads a bundle and re-saves it with multi-frame compression
    /// (100 operations per frame) with frame offsets for efficient random access.
    ///
    /// Returns: (size_diff, new_uncompressed_size, new_compressed_size)
    pub fn migrate_bundle(&self, bundle_num: u32) -> Result<(i64, u64, u64)> {
        use anyhow::Context;
        use std::collections::HashSet;
        use std::fs::File;

        // Get existing bundle metadata
        let meta = self
            .get_bundle_metadata(bundle_num)?
            .ok_or_else(|| anyhow::anyhow!("Bundle {} not in index", bundle_num))?;

        let old_size = meta.compressed_size;

        // Load bundle operations
        let load_result = self.load_bundle(
            bundle_num,
            LoadOptions {
                decompress: true,
                cache: false,
                filter: None,
                limit: None,
            },
        )?;

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

        // Compress operations into frames using parallel compression
        let frame_result =
            crate::bundle_format::compress_operations_to_frames_parallel(&operations)?;
        let compressed_size = frame_result.compressed_size;
        let uncompressed_size = frame_result.uncompressed_size;

        // Calculate hashes using library functions
        let content_hash = crate::bundle_format::calculate_content_hash(&operations)?;

        // Compressed hash will be calculated after writing the file
        // because it needs to include the metadata frame (verification hashes entire file)

        // Recalculate chain hash to verify correctness
        let (expected_parent, recalculated_chain_hash) = if bundle_num > 1 {
            use sha2::{Digest, Sha256};
            let parent_chain_hash = self
                .index
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
            use sha2::{Digest, Sha256};
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
                bundle_num,
                meta.hash,
                recalculated_chain_hash
            );
        }

        // Verify parent hash matches
        if expected_parent != meta.parent {
            anyhow::bail!(
                "Parent hash mismatch in bundle {}: original={}, expected={}\n\
                This indicates the chain linkage is broken.",
                bundle_num,
                meta.parent,
                expected_parent
            );
        }

        // Use verified hashes from original bundle
        let chain_hash = meta.hash.clone();
        let parent = meta.parent.clone();

        // Get cursor (end_time of previous bundle per spec)
        // For the first bundle, cursor is empty string
        let cursor = if bundle_num > 1 {
            let prev_end_time = self
                .index
                .read()
                .unwrap()
                .get_bundle(bundle_num - 1)
                .map(|b| b.end_time.clone())
                .unwrap_or_default();

            // Validate cursor matches previous bundle's end_time
            if prev_end_time.is_empty() {
                log::warn!(
                    "⚠️  Bundle {}: Previous bundle {} has empty end_time, cursor will be empty",
                    bundle_num,
                    bundle_num - 1
                );
            }

            prev_end_time
        } else {
            String::new()
        };

        // Validate cursor correctness (for non-genesis bundles)
        if bundle_num > 1 {
            let expected_cursor = {
                let index = self.index.read().unwrap();
                index
                    .get_bundle(bundle_num - 1)
                    .map(|b| b.end_time.clone())
                    .unwrap_or_default()
            };
            if cursor != expected_cursor {
                anyhow::bail!(
                    "Cursor validation failed for bundle {}: expected {} (previous bundle end_time), got {}",
                    bundle_num,
                    expected_cursor,
                    cursor
                );
            }
        } else if !cursor.is_empty() {
            anyhow::bail!(
                "Cursor validation failed for bundle {} (genesis): cursor should be empty, got {}",
                bundle_num,
                cursor
            );
        }

        let origin = self.index.read().unwrap().origin.clone();

        // Create bundle metadata using library function
        let bundle_metadata_frame = crate::bundle_format::create_bundle_metadata(
            bundle_num,
            &origin,
            &content_hash,
            if !parent.is_empty() {
                Some(&parent)
            } else {
                None
            },
            Some(uncompressed_size),
            Some(compressed_size),
            operation_count as usize,
            did_count as usize,
            &start_time,
            &end_time,
            frame_result.frame_offsets.len() - 1,
            constants::FRAME_SIZE,
            &frame_result.frame_offsets,
        );

        // Create backup path
        let bundle_path = constants::bundle_path(&self.directory, bundle_num);
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
        use sha2::{Digest, Sha256};
        let file_data = std::fs::read(&bundle_path).with_context(|| {
            format!(
                "Failed to read bundle file for hash: {}",
                bundle_path.display()
            )
        })?;

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
            if let Some(existing) = index
                .bundles
                .iter_mut()
                .find(|b| b.bundle_number == bundle_num)
            {
                *existing = bundle_metadata.clone();
            } else {
                index.bundles.push(bundle_metadata.clone());
            }

            // Recalculate totals
            index.total_size_bytes = index.bundles.iter().map(|b| b.compressed_size).sum();
            index.total_uncompressed_size_bytes =
                index.bundles.iter().map(|b| b.uncompressed_size).sum();
            index.updated_at = chrono::Utc::now().to_rfc3339();

            // Save index to disk using Index::save() (atomic write)
            index.save(&self.directory)?;
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

    pub fn bundle_count(&self) -> usize {
        self.index.read().unwrap().bundles.len()
    }

    pub fn get_mempool_operations_from(&self, start: usize) -> Result<Vec<Operation>> {
        let mempool_guard = self.mempool.read().unwrap();
        match mempool_guard.as_ref() {
            Some(mp) => {
                let ops = mp.get_operations();
                if start >= ops.len() {
                    Ok(Vec::new())
                } else {
                    Ok(ops[start..].to_vec())
                }
            }
            None => Ok(Vec::new()),
        }
    }

    // === Remote Access ===

    /// Fetch index from remote URL or local file path
    ///
    /// This is an async method that requires a tokio runtime.
    /// For synchronous usage, use the remote module functions directly.
    pub async fn fetch_remote_index(&self, target: &str) -> Result<Index> {
        if target.starts_with("http://") || target.starts_with("https://") {
            let client = crate::remote::RemoteClient::new(target)?;
            client.fetch_index().await
        } else {
            crate::remote::load_local_index(target)
        }
    }

    /// Fetch bundle operations from remote URL
    ///
    /// This is an async method that requires a tokio runtime.
    pub async fn fetch_remote_bundle(
        &self,
        base_url: &str,
        bundle_num: u32,
    ) -> Result<Vec<Operation>> {
        let client = crate::remote::RemoteClient::new(base_url)?;
        client.fetch_bundle_operations(bundle_num).await
    }

    /// Fetch a single operation from remote URL
    ///
    /// This is an async method that requires a tokio runtime.
    pub async fn fetch_remote_operation(
        &self,
        base_url: &str,
        bundle_num: u32,
        position: usize,
    ) -> Result<String> {
        let client = crate::remote::RemoteClient::new(base_url)?;
        client.fetch_operation(bundle_num, position).await
    }

    /// Rollback repository to a specific bundle
    pub fn rollback_to_bundle(&mut self, target_bundle: u32) -> Result<()> {
        let mut index = self.index.write().unwrap();

        // Keep only bundles up to target
        index.bundles.retain(|b| b.bundle_number <= target_bundle);
        index.last_bundle = target_bundle;
        index.updated_at = chrono::Utc::now().to_rfc3339();

        // Recalculate total sizes
        index.total_size_bytes = index.bundles.iter().map(|b| b.compressed_size).sum();
        index.total_uncompressed_size_bytes =
            index.bundles.iter().map(|b| b.uncompressed_size).sum();

        // Save updated index using Index::save() (atomic write)
        index.save(&self.directory)?;

        Ok(())
    }

    /// Get bundle metadata from index
    pub fn get_bundle_metadata(
        &self,
        bundle_num: u32,
    ) -> Result<Option<crate::index::BundleMetadata>> {
        let index = self.index.read().unwrap();
        Ok(index.get_bundle(bundle_num).cloned())
    }

    /// Get embedded metadata from bundle's skippable frame
    pub fn get_embedded_metadata(
        &self,
        bundle_num: u32,
    ) -> Result<Option<crate::bundle_format::BundleMetadata>> {
        let bundle_path = constants::bundle_path(&self.directory, bundle_num);

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
            let bundle_path = constants::bundle_path(&self.directory, bundle_num);

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

    /// Preview what files would be cleaned without actually deleting them
    ///
    /// Scans for all `.tmp` files in:
    /// - Repository root directory (e.g., `plc_bundles.json.tmp`)
    /// - DID index directory `.plcbundle/` (e.g., `config.json.tmp`)
    /// - DID index shards directory `.plcbundle/shards/` (e.g., `00.tmp`, `01.tmp`, etc.)
    ///
    /// # Returns
    /// A preview of files that would be removed
    pub fn clean_preview(&self) -> Result<CleanPreview> {
        use std::fs;

        let mut files = Vec::new();
        let mut total_size = 0u64;

        // Scan repository root directory
        let root_dir = &self.directory;
        if let Ok(entries) = fs::read_dir(root_dir) {
            for entry in entries {
                let entry = match entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                let path = entry.path();
                if !path.is_file() {
                    continue;
                }

                if path.extension().is_some_and(|ext| ext == "tmp") {
                    let file_size = match fs::metadata(&path) {
                        Ok(meta) => meta.len(),
                        Err(_) => 0,
                    };
                    total_size += file_size;
                    files.push(CleanPreviewFile {
                        path,
                        size: file_size,
                    });
                }
            }
        }

        // Scan DID index directory (.plcbundle/)
        let did_index_dir = root_dir.join(constants::DID_INDEX_DIR);
        if did_index_dir.exists() {
            // Check config.json.tmp
            let config_tmp = did_index_dir.join(format!("{}.tmp", constants::DID_INDEX_CONFIG));
            if config_tmp.exists() {
                let file_size = match fs::metadata(&config_tmp) {
                    Ok(meta) => meta.len(),
                    Err(_) => 0,
                };
                total_size += file_size;
                files.push(CleanPreviewFile {
                    path: config_tmp,
                    size: file_size,
                });
            }

            // Scan shards directory (.plcbundle/shards/)
            let shards_dir = did_index_dir.join(constants::DID_INDEX_SHARDS);
            if shards_dir.exists()
                && let Ok(entries) = fs::read_dir(&shards_dir)
            {
                for entry in entries {
                    let entry = match entry {
                        Ok(e) => e,
                        Err(_) => continue,
                    };

                    let path = entry.path();
                    if !path.is_file() {
                        continue;
                    }

                    if path.extension().is_some_and(|ext| ext == "tmp") {
                        let file_size = match fs::metadata(&path) {
                            Ok(meta) => meta.len(),
                            Err(_) => 0,
                        };
                        total_size += file_size;
                        files.push(CleanPreviewFile {
                            path,
                            size: file_size,
                        });
                    }
                }
            }
        }

        Ok(CleanPreview { files, total_size })
    }

    /// Clean up all temporary files from the repository
    ///
    /// Removes all `.tmp` files from:
    /// - Repository root directory (e.g., `plc_bundles.json.tmp`)
    /// - DID index directory `.plcbundle/` (e.g., `config.json.tmp`)
    /// - DID index shards directory `.plcbundle/shards/` (e.g., `00.tmp`, `01.tmp`, etc.)
    ///
    /// # Returns
    /// Statistics about the cleanup operation
    pub fn clean(&self) -> Result<CleanResult> {
        use std::fs;

        let verbose = *self.verbose.lock().unwrap();

        if verbose {
            log::info!("Starting repository cleanup...");
        }

        let mut files_removed = 0;
        let mut bytes_freed = 0u64;
        let mut errors = Vec::new();

        // Clean repository root directory
        let root_dir = &self.directory;
        if verbose {
            log::info!("Scanning repository root directory: {}", root_dir.display());
        }

        if let Ok(entries) = fs::read_dir(root_dir) {
            for entry in entries {
                let entry = match entry {
                    Ok(e) => e,
                    Err(e) => {
                        errors.push(format!("Failed to read directory entry: {}", e));
                        continue;
                    }
                };

                let path = entry.path();
                if !path.is_file() {
                    continue;
                }

                if path.extension().is_some_and(|ext| ext == "tmp") {
                    let file_size = match fs::metadata(&path) {
                        Ok(meta) => {
                            let size = meta.len();
                            bytes_freed += size;
                            size
                        }
                        Err(_) => 0,
                    };

                    match fs::remove_file(&path) {
                        Ok(_) => {
                            files_removed += 1;
                            if verbose {
                                log::info!(
                                    "  ✓ Removed: {} ({})",
                                    path.file_name().and_then(|n| n.to_str()).unwrap_or("?"),
                                    crate::format::format_bytes(file_size)
                                );
                            }
                        }
                        Err(e) => {
                            let error_msg = format!("Failed to remove {}: {}", path.display(), e);
                            errors.push(error_msg.clone());
                            if verbose {
                                log::warn!("  ✗ {}", error_msg);
                            }
                        }
                    }
                }
            }
        }

        // Clean DID index directory (.plcbundle/)
        let did_index_dir = root_dir.join(constants::DID_INDEX_DIR);
        if did_index_dir.exists() {
            if verbose {
                log::info!("Scanning DID index directory: {}", did_index_dir.display());
            }

            // Clean config.json.tmp
            let config_tmp = did_index_dir.join(format!("{}.tmp", constants::DID_INDEX_CONFIG));
            if config_tmp.exists() {
                let file_size = match fs::metadata(&config_tmp) {
                    Ok(meta) => {
                        let size = meta.len();
                        bytes_freed += size;
                        size
                    }
                    Err(_) => 0,
                };

                match fs::remove_file(&config_tmp) {
                    Ok(_) => {
                        files_removed += 1;
                        if verbose {
                            log::info!(
                                "  ✓ Removed: {} ({})",
                                config_tmp
                                    .file_name()
                                    .and_then(|n| n.to_str())
                                    .unwrap_or("?"),
                                crate::format::format_bytes(file_size)
                            );
                        }
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to remove {}: {}", config_tmp.display(), e);
                        errors.push(error_msg.clone());
                        if verbose {
                            log::warn!("  ✗ {}", error_msg);
                        }
                    }
                }
            }

            // Clean shards directory (.plcbundle/shards/)
            let shards_dir = did_index_dir.join(constants::DID_INDEX_SHARDS);
            if shards_dir.exists() {
                if verbose {
                    log::info!("Scanning shards directory: {}", shards_dir.display());
                }
                if let Ok(entries) = fs::read_dir(&shards_dir) {
                    for entry in entries {
                        let entry = match entry {
                            Ok(e) => e,
                            Err(e) => {
                                errors
                                    .push(format!("Failed to read shards directory entry: {}", e));
                                continue;
                            }
                        };

                        let path = entry.path();
                        if !path.is_file() {
                            continue;
                        }

                        if path.extension().is_some_and(|ext| ext == "tmp") {
                            let file_size = match fs::metadata(&path) {
                                Ok(meta) => {
                                    let size = meta.len();
                                    bytes_freed += size;
                                    size
                                }
                                Err(_) => 0,
                            };

                            match fs::remove_file(&path) {
                                Ok(_) => {
                                    files_removed += 1;
                                    if verbose {
                                        log::info!(
                                            "  ✓ Removed: {} ({})",
                                            path.file_name()
                                                .and_then(|n| n.to_str())
                                                .unwrap_or("?"),
                                            crate::format::format_bytes(file_size)
                                        );
                                    }
                                }
                                Err(e) => {
                                    let error_msg =
                                        format!("Failed to remove {}: {}", path.display(), e);
                                    errors.push(error_msg.clone());
                                    if verbose {
                                        log::warn!("  ✗ {}", error_msg);
                                    }
                                }
                            }
                        }
                    }
                }
            } else if verbose {
                log::debug!("Shards directory does not exist: {}", shards_dir.display());
            }
        } else if verbose {
            log::debug!(
                "DID index directory does not exist: {}",
                did_index_dir.display()
            );
        }

        // Summary logging
        if verbose {
            if files_removed > 0 {
                log::info!(
                    "Cleanup complete: removed {} file(s), freed {}",
                    files_removed,
                    crate::format::format_bytes(bytes_freed)
                );
            } else {
                log::info!("Cleanup complete: no temporary files found");
            }

            if !errors.is_empty() {
                log::warn!("Encountered {} error(s) during cleanup", errors.len());
            }
        }

        Ok(CleanResult {
            files_removed,
            bytes_freed,
            errors: if errors.is_empty() {
                None
            } else {
                Some(errors)
            },
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

        let bundle_path = constants::bundle_path(&self.directory, bundle_num);
        if !bundle_path.exists() {
            anyhow::bail!(
                "Bundle {} file not found (exists in index but missing on disk)",
                bundle_num
            );
        }
        Ok(std::fs::File::open(bundle_path)?)
    }

    /// Stream bundle decompressed (JSONL) data
    /// Returns a reader that decompresses the bundle on-the-fly
    pub fn stream_bundle_decompressed(
        &self,
        bundle_num: u32,
    ) -> Result<Box<dyn std::io::Read + Send>> {
        let file = self.stream_bundle_raw(bundle_num)?;
        Ok(Box::new(zstd::Decoder::new(file)?))
    }

    /// Get current cursor (global position of last operation)
    /// Cursor = (last_bundle * BUNDLE_SIZE) + mempool_ops_count
    pub fn get_current_cursor(&self) -> u64 {
        let index = self.index.read().unwrap();
        let bundled_ops = total_operations_from_bundles(index.last_bundle);

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
                    normalized,
                    constants::DEFAULT_HANDLE_RESOLVER_URL,
                    normalized
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
                    normalized,
                    constants::DEFAULT_HANDLE_RESOLVER_URL,
                    normalized
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
        self.handle_resolver
            .as_ref()
            .map(|r| r.get_base_url().to_string())
    }

    /// Get a reference to the handle resolver
    /// Returns None if handle resolver is not configured
    pub fn get_handle_resolver(&self) -> Option<Arc<handle_resolver::HandleResolver>> {
        self.handle_resolver.clone()
    }

    /// Create a shallow clone suitable for `Arc` sharing
    pub fn clone_for_arc(&self) -> Self {
        Self {
            directory: self.directory.clone(),
            index: Arc::clone(&self.index),
            cache: Arc::clone(&self.cache),
            did_index: Arc::clone(&self.did_index),
            stats: Arc::clone(&self.stats),
            mempool: Arc::clone(&self.mempool),
            mempool_checked: Arc::clone(&self.mempool_checked),
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
            // Use Operation::from_json (sonic_rs) instead of serde deserialization
            let op = Operation::from_json(&line)?;
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
        if let Some(ref did) = filter.did
            && &op.did != did
        {
            return false;
        }

        if let Some(ref op_type) = filter.operation_type
            && &op.operation != op_type
        {
            return false;
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

    // === Repository Management ===

    /// Initialize a new repository with an empty index
    ///
    /// This is a static method that doesn't require an existing BundleManager.
    /// Creates all necessary directories and an empty index file.
    ///
    /// # Arguments
    /// * `directory` - Directory to initialize
    /// * `origin` - PLC directory URL or origin identifier
    /// * `force` - Whether to reinitialize if already exists
    ///
    /// # Returns
    /// True if initialized (created new), False if already existed and force=false
    pub fn init_repository<P: AsRef<std::path::Path>>(
        directory: P,
        origin: String,
        force: bool,
    ) -> Result<bool> {
        Index::init(directory, origin, force)
    }

    /// Rebuild index from existing bundle files
    ///
    /// This is a static method that doesn't require an existing BundleManager.
    /// It scans all .jsonl.zst files in the directory and reconstructs the index
    /// by extracting embedded metadata from each bundle's skippable frame.
    ///
    /// # Arguments
    /// * `directory` - Directory containing bundle files
    /// * `origin` - Optional origin URL (auto-detected from first bundle if None)
    /// * `progress_cb` - Optional progress callback (current, total)
    ///
    /// # Returns
    /// The reconstructed index (already saved to disk)
    pub fn rebuild_index<P: AsRef<std::path::Path>, F>(
        directory: P,
        origin: Option<String>,
        progress_cb: Option<F>,
    ) -> Result<Index>
    where
        F: Fn(usize, usize, u64, u64) + Send + Sync, // (current, total, bytes_processed, total_bytes)
    {
        let index = Index::rebuild_from_bundles(&directory, origin, progress_cb)?;
        index.save(&directory)?;
        Ok(index)
    }

    /// Clone repository from a remote plcbundle instance
    ///
    /// Downloads bundles from a remote instance and reconstructs the repository.
    /// This is a static async method that doesn't require an existing BundleManager.
    ///
    /// # Arguments
    /// * `remote_url` - URL of the remote plcbundle instance
    /// * `target_dir` - Directory to clone into
    /// * `remote_index` - Already fetched remote index
    /// * `bundles_to_download` - List of bundle numbers to download
    /// * `progress_callback` - Optional callback for progress updates (bundle_num, count, total_count, bytes)
    ///
    /// # Returns
    /// Tuple of (successful_downloads, failed_downloads)
    pub async fn clone_from_remote<P, F>(
        remote_url: String,
        target_dir: P,
        remote_index: &Index,
        bundles_to_download: Vec<u32>,
        progress_callback: Option<F>,
    ) -> Result<(usize, usize)>
    where
        P: AsRef<std::path::Path> + Send + Sync,
        F: Fn(u32, usize, usize, u64) + Send + Sync + 'static,
    {
        use crate::remote::RemoteClient;
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

        let target_dir = target_dir.as_ref();

        // Save index first
        remote_index.save(target_dir)?;

        // Progress tracking
        let downloaded = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicUsize::new(0));
        let bytes_downloaded = Arc::new(AtomicU64::new(0));
        let total_count = bundles_to_download.len();

        // Parallel download with semaphore (4 concurrent downloads)
        let semaphore = Arc::new(tokio::sync::Semaphore::new(4));
        let progress_cb = progress_callback.map(Arc::new);

        let mut tasks = Vec::new();

        for bundle_num in bundles_to_download {
            let client = RemoteClient::new(&remote_url)?;
            let target_dir = target_dir.to_path_buf();
            let downloaded = Arc::clone(&downloaded);
            let failed = Arc::clone(&failed);
            let bytes_downloaded = Arc::clone(&bytes_downloaded);
            let semaphore = Arc::clone(&semaphore);
            let progress_cb = progress_cb.clone();

            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();

                // Retry logic with exponential backoff
                let max_retries = 3;
                for attempt in 0..max_retries {
                    if attempt > 0 {
                        let delay = std::time::Duration::from_secs(1 << attempt);
                        tokio::time::sleep(delay).await;
                    }

                    match client.download_bundle_file(bundle_num).await {
                        Ok(data) => {
                            let data_len = data.len() as u64;

                            // Write bundle file
                            let bundle_path = constants::bundle_path(&target_dir, bundle_num);
                            if let Err(_e) = std::fs::write(&bundle_path, data) {
                                failed.fetch_add(1, Ordering::SeqCst);
                                return;
                            }

                            let count = downloaded.fetch_add(1, Ordering::SeqCst) + 1;
                            let bytes =
                                bytes_downloaded.fetch_add(data_len, Ordering::SeqCst) + data_len;

                            // Call progress callback
                            if let Some(ref cb) = progress_cb {
                                cb(bundle_num, count, total_count, bytes);
                            }
                            return;
                        }
                        Err(_) => {
                            continue; // Retry
                        }
                    }
                }

                // All retries failed
                failed.fetch_add(1, Ordering::SeqCst);
            });

            tasks.push(task);
        }

        // Wait for all downloads
        for task in tasks {
            let _ = task.await;
        }

        let downloaded_count = downloaded.load(Ordering::SeqCst);
        let failed_count = failed.load(Ordering::SeqCst);

        Ok((downloaded_count, failed_count))
    }

    /// Deletes a bundle file from the repository.
    ///
    /// This method removes a bundle file from the repository directory.
    ///
    /// # Arguments
    /// * `bundle_num` - The number of the bundle to delete.
    ///
    /// # Returns
    /// A `Result` indicating whether the operation was successful.
    pub fn delete_bundle_file(&self, bundle_num: u32) -> Result<()> {
        let bundle_path = constants::bundle_path(&self.directory, bundle_num);
        if bundle_path.exists() {
            std::fs::remove_file(bundle_path)?;
        }
        self.cache.remove(bundle_num);
        Ok(())
    }
}

// Supporting types moved here
/// Options controlling bundle loading behavior
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

/// Result from a bundle load operation
#[derive(Debug)]
pub struct LoadResult {
    pub bundle_number: u32,
    pub operations: Vec<Operation>,
    pub metadata: Option<BundleMetadata>,
}

/// Result for single-operation fetch with timing
#[derive(Debug)]
pub struct OperationResult {
    pub raw_json: String,
    pub size_bytes: usize,
    pub load_duration: std::time::Duration,
}

/// Specification for querying bundles
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

/// Bundle selection for queries, exports, and verification
#[derive(Debug, Clone)]
pub enum BundleRange {
    All,
    Single(u32),
    Range(u32, u32),
    List(Vec<u32>),
}

/// Specification for export operations
#[derive(Debug, Clone)]
pub struct ExportSpec {
    pub bundles: BundleRange,
    pub format: ExportFormat,
    pub filter: Option<OperationFilter>,
    pub count: Option<usize>,
    pub after_timestamp: Option<String>,
}

/// Output format for export
#[derive(Debug, Clone)]
pub enum ExportFormat {
    JsonLines,
}

/// Statistics collected during export
#[derive(Debug, Default)]
pub struct ExportStats {
    pub records_written: u64,
    pub bytes_written: u64,
}

/// Specification for bundle verification
#[derive(Debug, Clone)]
pub struct VerifySpec {
    pub check_hash: bool,
    pub check_content_hash: bool,
    pub check_operations: bool,
    pub fast: bool, // Fast mode: only check metadata frame, skip hash calculations
}

/// Result of verifying a single bundle
#[derive(Debug)]
pub struct VerifyResult {
    pub valid: bool,
    pub errors: Vec<String>,
}

/// Specification for chain verification
#[derive(Debug, Clone)]
pub struct ChainVerifySpec {
    pub start_bundle: u32,
    pub end_bundle: Option<u32>,
    pub check_parent_links: bool,
}

/// Result of chain verification across multiple bundles
#[derive(Debug)]
pub struct ChainVerifyResult {
    pub valid: bool,
    pub bundles_checked: u32,
    pub errors: Vec<(u32, String)>,
}

/// Aggregated bundle information with optional details
#[derive(Debug)]
pub struct BundleInfo {
    pub metadata: BundleMetadata,
    pub exists: bool,
    pub cached: bool,
    pub operations: Option<Vec<Operation>>,
    pub size_info: Option<SizeInfo>,
}

/// Size information (compressed and uncompressed) for a bundle
#[derive(Debug)]
pub struct SizeInfo {
    pub compressed: u64,
    pub uncompressed: u64,
}

/// Flags controlling `get_bundle_info` detail inclusion
#[derive(Debug, Clone)]
pub struct InfoFlags {
    pub include_operations: bool,
    pub include_size_info: bool,
}

/// Specification for rollback execution
#[derive(Debug, Clone)]
pub struct RollbackSpec {
    pub target_bundle: u32,
    pub dry_run: bool,
}

/// Plan produced by `rollback_plan`
#[derive(Debug)]
pub struct RollbackPlan {
    pub target_bundle: u32,
    pub affected_bundles: Vec<u32>,
    pub affected_operations: usize,
    pub affected_dids: usize,
    pub estimated_time_ms: u64,
}

/// Result returned by `rollback`
#[derive(Debug)]
pub struct RollbackResult {
    pub success: bool,
    pub bundles_removed: usize,
    pub plan: Option<RollbackPlan>,
}

/// Specification for cache warm-up
#[derive(Debug, Clone)]
pub struct WarmUpSpec {
    pub strategy: WarmUpStrategy,
}

/// Strategy selection for warm-up
#[derive(Debug, Clone)]
pub enum WarmUpStrategy {
    Recent(u32),
    Range(u32, u32),
    All,
}

/// Statistics from DID index rebuild
#[derive(Debug, Default, Clone)]
pub struct RebuildStats {
    pub bundles_processed: u32,
    pub operations_indexed: u64,
}
