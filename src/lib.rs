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
pub(crate) mod bundle_format;
pub(crate) mod cache;
pub mod constants;
pub mod did_index;
pub(crate) mod ffi;
pub mod format;
pub mod handle_resolver;
pub mod index;
pub(crate) mod iterators;
mod manager;
pub mod mempool;
pub(crate) mod operations;
pub(crate) mod options;
pub mod plc_client;
pub mod processor;
pub mod remote;
pub mod resolver;
pub mod runtime;
#[cfg(feature = "server")]
pub mod server;
pub mod sync;
pub(crate) mod verification;

// Re-export main types
pub use iterators::{ExportIterator, QueryIterator, RangeIterator};
pub use manager::{
    BundleInfo, BundleManager, BundleRange, ChainVerifyResult, ChainVerifySpec, CleanPreview,
    CleanPreviewFile, CleanResult, DIDIndexStats, ExportFormat, ExportSpec, InfoFlags,
    IntoManagerOptions, LoadOptions, LoadResult, ManagerOptions, ManagerStats, OperationResult,
    QuerySpec, RebuildStats, ResolveResult, RollbackFileStats, RollbackPlan, RollbackResult,
    RollbackSpec, SyncResult, VerifyResult, VerifySpec, WarmUpSpec, WarmUpStrategy,
};
pub use operations::{Operation, OperationFilter, OperationRequest, OperationWithLocation};
pub use options::{Options, OptionsBuilder, QueryMode};
