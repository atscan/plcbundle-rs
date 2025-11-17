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
#[cfg(feature = "server")]
pub mod server;
pub mod sync;
pub mod verification;

// Re-export main types
pub use constants::{
    BINARY_NAME, BUNDLE_SIZE, DEFAULT_HANDLE_RESOLVER_URL, DEFAULT_ORIGIN,
    DEFAULT_PLC_DIRECTORY_URL, DEFAULT_RATE_LIMIT, DID_INDEX_CONFIG, DID_INDEX_DIR,
    DID_INDEX_SHARDS, FRAME_SIZE, MEMPOOL_FILE_PREFIX, VERSION, ZSTD_COMPRESSION_LEVEL, created_by,
    user_agent,
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
    CleanPreviewFile, CleanResult, CompressionType, DIDIndexStats, ExportFormat, ExportSpec,
    InfoFlags, LoadOptions, LoadResult, ManagerStats, OperationResult, QuerySpec, RebuildStats,
    ResolveResult, RollbackFileStats, RollbackPlan, RollbackResult, RollbackSpec, SyncResult,
    VerifyResult, VerifySpec, WarmUpSpec, WarmUpStrategy,
};
pub use mempool::{Mempool, MempoolStats};
pub use operations::{Operation, OperationFilter, OperationRequest, OperationWithLocation};
pub use options::{Options, OptionsBuilder, QueryMode};
pub use processor::{OutputHandler, Processor, QueryEngine, Stats, parse_bundle_range};
// remote functions are methods on RemoteClient, not standalone functions
pub use resolver::{
    AuditLogEntry, DIDDocument, DIDState, build_did_state, format_audit_log, resolve_did_document,
    validate_did_format,
};
pub use plc_client::PLCClient;
pub use sync::{
    CliLogger, ServerLogger, SyncConfig, SyncEvent, SyncLogger, SyncManager, SyncStats,
};
