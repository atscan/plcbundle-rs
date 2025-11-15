// Constants for version and binary identification

/// Binary name used in user agents and metadata
pub const BINARY_NAME: &str = "plcbundle-rs";

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
