//! Global constants and helpers for filenames/paths, networking defaults, compression, index settings, and position conversions
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_user_agent() {
        let ua = user_agent();
        assert!(ua.starts_with("plcbundle/"));
        assert!(ua.contains("plcbundle"));
    }

    #[test]
    fn test_created_by() {
        let cb = created_by();
        assert!(cb.starts_with("plcbundle/"));
        assert!(cb.contains("plcbundle"));
    }

    #[test]
    fn test_bundle_filename() {
        assert_eq!(bundle_filename(1), "000001.jsonl.zst");
        assert_eq!(bundle_filename(42), "000042.jsonl.zst");
        assert_eq!(bundle_filename(123456), "123456.jsonl.zst");
        assert_eq!(bundle_filename(0), "000000.jsonl.zst");
    }

    #[test]
    fn test_bundle_path() {
        let dir = Path::new("/tmp/test");
        let path = bundle_path(dir, 1);
        assert_eq!(path, Path::new("/tmp/test/000001.jsonl.zst"));

        let path2 = bundle_path(dir, 42);
        assert_eq!(path2, Path::new("/tmp/test/000042.jsonl.zst"));
    }

    #[test]
    fn test_bundle_position_to_global() {
        // Bundle 1, position 0 → global 0
        assert_eq!(bundle_position_to_global(1, 0), 0);
        
        // Bundle 1, position 9999 → global 9999
        assert_eq!(bundle_position_to_global(1, 9999), 9999);
        
        // Bundle 2, position 0 → global 10000
        assert_eq!(bundle_position_to_global(2, 0), 10000);
        
        // Bundle 2, position 500 → global 10500
        assert_eq!(bundle_position_to_global(2, 500), 10500);
        
        // Bundle 3, position 42 → global 20042
        assert_eq!(bundle_position_to_global(3, 42), 20042);
    }

    #[test]
    fn test_bundle_position_to_global_edge_cases() {
        // Bundle 0 (edge case, should handle gracefully)
        assert_eq!(bundle_position_to_global(0, 0), 0);
        
        // Large bundle numbers
        assert_eq!(bundle_position_to_global(100, 0), 990000);
        assert_eq!(bundle_position_to_global(100, 5000), 995000);
    }

    #[test]
    fn test_total_operations_from_bundles() {
        assert_eq!(total_operations_from_bundles(0), 0);
        assert_eq!(total_operations_from_bundles(1), 10000);
        assert_eq!(total_operations_from_bundles(2), 20000);
        assert_eq!(total_operations_from_bundles(100), 1000000);
    }

    #[test]
    fn test_mempool_position_to_global() {
        // With last_bundle = 0, mempool position 0 → global 0
        assert_eq!(mempool_position_to_global(0, 0), 0);
        
        // With last_bundle = 1, mempool position 0 → global 10000
        assert_eq!(mempool_position_to_global(1, 0), 10000);
        
        // With last_bundle = 1, mempool position 42 → global 10042
        assert_eq!(mempool_position_to_global(1, 42), 10042);
        
        // With last_bundle = 2, mempool position 100 → global 20100
        assert_eq!(mempool_position_to_global(2, 100), 20100);
    }

    #[test]
    fn test_global_to_bundle_position() {
        // Global 0 → bundle 1, position 0
        let (bundle, pos) = global_to_bundle_position(0);
        assert_eq!(bundle, 1);
        assert_eq!(pos, 0);
        
        // Global 9999 → bundle 1, position 9999
        let (bundle, pos) = global_to_bundle_position(9999);
        assert_eq!(bundle, 1);
        assert_eq!(pos, 9999);
        
        // Global 10000 → bundle 2, position 0
        let (bundle, pos) = global_to_bundle_position(10000);
        assert_eq!(bundle, 2);
        assert_eq!(pos, 0);
        
        // Global 10500 → bundle 2, position 500
        let (bundle, pos) = global_to_bundle_position(10500);
        assert_eq!(bundle, 2);
        assert_eq!(pos, 500);
        
        // Global 20042 → bundle 3, position 42
        let (bundle, pos) = global_to_bundle_position(20042);
        assert_eq!(bundle, 3);
        assert_eq!(pos, 42);
    }

    #[test]
    fn test_global_to_bundle_position_round_trip() {
        // Test round-trip conversion
        for bundle in 1..=10 {
            for position in [0, 100, 1000, 5000, 9999] {
                let global = bundle_position_to_global(bundle, position);
                let (bundle_back, pos_back) = global_to_bundle_position(global);
                assert_eq!(bundle, bundle_back, "Bundle mismatch for global {}", global);
                assert_eq!(position, pos_back, "Position mismatch for global {}", global);
            }
        }
    }

    #[test]
    fn test_constants_values() {
        assert_eq!(BUNDLE_SIZE, 10_000);
        assert_eq!(FRAME_SIZE, 100);
        assert_eq!(DEFAULT_RATE_LIMIT, 72);
        assert_eq!(HTTP_TIMEOUT_SECS, 60);
        assert_eq!(HTTP_INDEX_TIMEOUT_SECS, 30);
        assert_eq!(HTTP_BUNDLE_TIMEOUT_SECS, 60);
        assert_eq!(HTTP_RESOLVER_TIMEOUT_SECS, 10);
        assert_eq!(HTTP_PING_TIMEOUT_SECS, 5);
        assert_eq!(MEMPOOL_STALE_THRESHOLD_SECS, 3600);
        assert_eq!(MIN_BUNDLE_CREATION_INTERVAL_SECS, 60);
        assert_eq!(MEMPOOL_FILE_PREFIX, "plc_mempool_");
        assert_eq!(DID_INDEX_DIR, ".plcbundle");
        assert_eq!(DID_INDEX_SHARDS, "shards");
        assert_eq!(DID_INDEX_DELTAS, "deltas");
        assert_eq!(DID_INDEX_CONFIG, "config.json");
        assert_eq!(DEFAULT_PLC_DIRECTORY_URL, "https://plc.directory");
        assert_eq!(DEFAULT_HANDLE_RESOLVER_URL, "https://quickdid.smokesignal.tools");
        assert_eq!(DEFAULT_ORIGIN, "local");
        assert_eq!(ZSTD_COMPRESSION_LEVEL, 1);
        assert_eq!(DID_INDEX_FLUSH_INTERVAL, 64);
    }
}
