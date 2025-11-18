//! plcbundle-rs: high-performance PLC bundle access, DID resolution, and tooling
//!
//! This crate provides `BundleManager` and supporting types to work with PLC repositories
//! consisting of compressed JSONL bundle files. It focuses on:
//! - Fast random access via multi-frame compression and metadata offsets
//! - A memory-efficient DID index for lookups across bundles and mempool
//! - Query, export, verification, and sync utilities
//!
//! # Quickstart
//!
//! ```no_run
//! use plcbundle::{BundleManager, ManagerOptions, QuerySpec, BundleRange, QueryMode};
//! use std::path::PathBuf;
//!
//! let mgr = BundleManager::new(PathBuf::from("/data/plc"), ManagerOptions::default())?;
//!
//! // Resolve a DID
//! let resolved = mgr.resolve_did("did:plc:abcdef...")?;
//! assert!(resolved.locations_found >= 0);
//!
//! // Query a bundle range
//! let spec = QuerySpec { bundles: BundleRange::Range(1, 10), filter: None, query: String::new(), mode: QueryMode::All };
//! for item in mgr.query(spec) { let _ = item?; }
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! # Features
//! - Sync new bundles from a PLC source with deduplication and verified hashes
//! - Export results in JSONL with optional compression
//! - Rollback, migrate, clean, and rebuild indices
//!
// src/lib.rs
pub mod bundle_format;
pub mod cache;
pub mod constants;
pub mod did_index;
pub mod ffi;
pub mod format;
pub mod handle_resolver;
pub mod index;
pub mod iterators;
pub mod manager;
pub mod mempool;
pub mod operations;
pub mod options;
pub mod plc_client;
pub mod processor;
pub mod remote;
pub mod resolver;
pub mod runtime;
#[cfg(feature = "server")]
pub mod server;
pub mod sync;
pub mod verification;

// Re-export main types
pub use constants::{
    BINARY_NAME, BUNDLE_SIZE, DEFAULT_HANDLE_RESOLVER_URL, DEFAULT_ORIGIN,
    DEFAULT_PLC_DIRECTORY_URL, DEFAULT_RATE_LIMIT, DID_INDEX_CONFIG, DID_INDEX_DIR,
    DID_INDEX_SHARDS, FRAME_SIZE, MEMPOOL_FILE_PREFIX, VERSION, ZSTD_COMPRESSION_LEVEL, created_by,
    user_agent, bundle_position_to_global, total_operations_from_bundles, mempool_position_to_global,
    global_to_bundle_position,
};
pub use did_index::{DIDLookupStats, DIDLookupTimings};
pub use format::{
    format_bytes, format_bytes_compact, format_bytes_per_sec, format_duration_compact,
    format_duration_verbose, format_number, format_std_duration, format_std_duration_auto,
    format_std_duration_ms, format_std_duration_verbose,
};
pub use handle_resolver::{HandleResolver, is_handle, normalize_handle, validate_handle_format};
pub use index::{BundleMetadata, Index};
pub use iterators::{ExportIterator, QueryIterator, RangeIterator};
pub use manager::{
    BundleInfo, BundleManager, BundleRange, ChainVerifyResult, ChainVerifySpec, CleanPreview,
    CleanPreviewFile, CleanResult, DIDIndexStats, ExportFormat, ExportSpec,
    InfoFlags, IntoManagerOptions, LoadOptions, LoadResult, ManagerOptions, ManagerStats,
    OperationResult, QuerySpec, RebuildStats, ResolveResult, RollbackFileStats, RollbackPlan,
    RollbackResult, RollbackSpec, SyncResult, VerifyResult, VerifySpec, WarmUpSpec, WarmUpStrategy,
};
pub use mempool::{Mempool, MempoolStats};
pub use operations::{Operation, OperationFilter, OperationRequest, OperationWithLocation};
pub use options::{Options, OptionsBuilder, QueryMode};
pub use processor::{OutputHandler, Processor, QueryEngine, Stats, parse_bundle_range, parse_operation_range};
// remote functions are methods on RemoteClient, not standalone functions
pub use resolver::{
    AuditLogEntry, DIDDocument, DIDState, build_did_state, format_audit_log, resolve_did_document,
    validate_did_format,
};
pub use plc_client::PLCClient;
pub use sync::{
    SyncLoggerImpl, SyncConfig, SyncEvent, SyncLogger, SyncManager, SyncStats,
};
pub use runtime::BundleRuntime;
