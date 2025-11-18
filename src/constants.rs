// Constants for version and binary identification
use std::path::{Path, PathBuf};

/// Binary name used in user agents and metadata
pub const BINARY_NAME: &str = "plcbundle";

/// Package version from Cargo.toml (set at compile time)
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the user agent string for HTTP requests
pub fn user_agent() -> String {
    format!("{}/{}", BINARY_NAME, VERSION)
}

/// Returns the created_by string for bundle metadata
pub fn created_by() -> String {
    format!("{}/{}", BINARY_NAME, VERSION)
}

/// Returns the canonical bundle filename for a bundle number
pub fn bundle_filename(bundle_num: u32) -> String {
    format!("{:06}.jsonl.zst", bundle_num)
}

/// Resolves an on-disk bundle path relative to the provided directory
pub fn bundle_path(dir: impl AsRef<Path>, bundle_num: u32) -> PathBuf {
    dir.as_ref().join(bundle_filename(bundle_num))
}

// ============================================================================
// Bundle and Frame Constants
// ============================================================================

/// Number of operations per bundle
pub const BUNDLE_SIZE: usize = 10_000;

/// Number of operations per frame (for multi-frame compression)
pub const FRAME_SIZE: usize = 100;

// ============================================================================
// Rate Limiting Constants
// ============================================================================

/// Default rate limit for PLC API requests (requests per minute)
/// Set to 80% of quota (72 req/min) to provide safety margin and prevent rate limiting
pub const DEFAULT_RATE_LIMIT: usize = 72;

// ============================================================================
// Timeout Constants (in seconds)
// ============================================================================

/// Default HTTP request timeout
pub const HTTP_TIMEOUT_SECS: u64 = 60;

/// HTTP request timeout for index fetching
pub const HTTP_INDEX_TIMEOUT_SECS: u64 = 30;

/// HTTP request timeout for bundle fetching
pub const HTTP_BUNDLE_TIMEOUT_SECS: u64 = 60;

/// HTTP request timeout for handle resolver
pub const HTTP_RESOLVER_TIMEOUT_SECS: u64 = 10;

/// HTTP request timeout for ping operations
pub const HTTP_PING_TIMEOUT_SECS: u64 = 5;

/// Mempool file staleness threshold (1 hour in seconds)
pub const MEMPOOL_STALE_THRESHOLD_SECS: u64 = 3600;

/// Minimum time between bundle creation attempts (60 seconds)
pub const MIN_BUNDLE_CREATION_INTERVAL_SECS: i64 = 60;

// ============================================================================
// File and Directory Constants
// ============================================================================

/// Mempool file prefix (e.g., "plc_mempool_000001.jsonl")
pub const MEMPOOL_FILE_PREFIX: &str = "plc_mempool_";

/// DID index directory name (hidden directory in bundle repository)
pub const DID_INDEX_DIR: &str = ".plcbundle";

/// DID index shards subdirectory name
pub const DID_INDEX_SHARDS: &str = "shards";

/// DID index delta segments subdirectory name
pub const DID_INDEX_DELTAS: &str = "deltas";

/// DID index config file name
pub const DID_INDEX_CONFIG: &str = "config.json";

// ============================================================================
// Network Constants
// ============================================================================

/// Default PLC directory URL
pub const DEFAULT_PLC_DIRECTORY_URL: &str = "https://plc.directory";

/// Default handle resolver URL for AT Protocol handle resolution
pub const DEFAULT_HANDLE_RESOLVER_URL: &str = "https://quickdid.smokesignal.tools";

// ============================================================================
// Origin Constants
// ============================================================================

/// Default origin identifier for local repositories
pub const DEFAULT_ORIGIN: &str = "local";

// ============================================================================
// Compression Constants
// ============================================================================

/// Zstd compression level (1 = fast, 3 = balanced, 19 = maximum)
pub const ZSTD_COMPRESSION_LEVEL: i32 = 1;

// ============================================================================
// DID Index Constants
// ============================================================================

/// Default flush interval for DID index building (number of bundles before flushing to disk)
/// A value of 0 means flush only at the end (maximum memory usage)
pub const DID_INDEX_FLUSH_INTERVAL: u32 = 64;

// ============================================================================
// Position Calculation Helpers
// ============================================================================

/// Calculate global position from bundle number and position within bundle
/// Global position = ((bundle_number - 1) * BUNDLE_SIZE) + position
/// 
/// # Examples
/// - Bundle 1, position 0 → global 0
/// - Bundle 1, position 9999 → global 9999
/// - Bundle 2, position 0 → global 10000
/// - Bundle 2, position 500 → global 10500
pub fn bundle_position_to_global(bundle_number: u32, position: usize) -> u64 {
    ((bundle_number.saturating_sub(1)) as u64 * BUNDLE_SIZE as u64) + position as u64
}

/// Calculate total number of operations from all bundles
/// Total = last_bundle * BUNDLE_SIZE
pub fn total_operations_from_bundles(last_bundle: u32) -> u64 {
    last_bundle as u64 * BUNDLE_SIZE as u64
}

/// Calculate global position for a mempool operation
/// Global position = total_operations_from_bundles + mempool_position
pub fn mempool_position_to_global(last_bundle: u32, mempool_position: usize) -> u64 {
    total_operations_from_bundles(last_bundle) + mempool_position as u64
}

/// Convert global position to bundle number and position
/// Returns (bundle_number, position) where bundle_number is 1-indexed and position is 0-indexed
/// 
/// # Examples
/// - Global 0 → bundle 1, position 0
/// - Global 9999 → bundle 1, position 9999
/// - Global 10000 → bundle 2, position 0
/// - Global 10500 → bundle 2, position 500
pub fn global_to_bundle_position(global_pos: u64) -> (u32, usize) {
    let bundle_num = ((global_pos / BUNDLE_SIZE as u64) + 1) as u32;
    let position = (global_pos % BUNDLE_SIZE as u64) as usize;
    (bundle_num, position)
}
