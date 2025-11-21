//! PLC synchronization: events and logger, boundary-CID deduplication, one-shot and continuous modes, and robust error/backoff handling

// Sync module - PLC directory synchronization
use crate::constants;
use crate::operations::Operation;
use crate::plc_client::PLCClient;
use anyhow::Result;
use serde::Deserialize;
use sonic_rs::{JsonValueTrait, Value};
use std::any::Any;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Deserialize)]
pub struct PLCOperation {
    did: String,
    operation: Value,
    cid: String,
    #[serde(default)]
    nullified: Option<Value>,
    #[serde(rename = "createdAt")]
    created_at: String,

    #[serde(skip)]
    pub raw_json: Option<String>,
}

impl From<PLCOperation> for Operation {
    fn from(plc: PLCOperation) -> Self {
        let is_nullified = plc.nullified.as_ref().is_some_and(|v| {
            v.as_bool().unwrap_or(false) || v.as_str().is_some_and(|s| !s.is_empty())
        });

        Self {
            did: plc.did,
            operation: plc.operation,
            cid: Some(plc.cid),
            nullified: is_nullified,
            created_at: plc.created_at,
            extra: sonic_rs::from_str("null").unwrap_or_else(|_| Value::new()),
            raw_json: plc.raw_json,
        }
    }
}

// ============================================================================
// Boundary CID Logic (CRITICAL for deduplication)
// ============================================================================

/// Get CIDs that share the same timestamp as the last operation
pub fn get_boundary_cids(operations: &[Operation]) -> HashSet<String> {
    if operations.is_empty() {
        return HashSet::new();
    }

    let last_time = &operations.last().unwrap().created_at;
    operations
        .iter()
        .rev()
        .take_while(|op| &op.created_at == last_time)
        .filter_map(|op| op.cid.clone())
        .collect()
}

/// Strip operations that match previous bundle's boundary CIDs
pub fn strip_boundary_duplicates(
    mut operations: Vec<Operation>,
    prev_boundary: &HashSet<String>,
) -> Vec<Operation> {
    if prev_boundary.is_empty() {
        return operations;
    }

    operations.retain(|op| {
        op.cid
            .as_ref()
            .is_none_or(|cid| !prev_boundary.contains(cid))
    });

    operations
}

// ============================================================================
// Sync Events
// ============================================================================

#[derive(Debug, Clone)]
pub enum SyncEvent {
    BundleCreated {
        bundle_num: u32,
        hash: String,
        age: String,
        fetch_duration_ms: u64,
        bundle_save_ms: u64,
        index_ms: u64,
        total_duration_ms: u64,
        fetch_requests: usize,
        did_index_compacted: bool,

        unique_dids: u32,
        size_bytes: u64,
        fetch_wait_ms: u64,
        fetch_http_ms: u64,
    },
    CaughtUp {
        next_bundle: u32,
        mempool_count: usize,
        new_ops: usize,
        fetch_duration_ms: u64,
    },
    InitialSyncComplete {
        total_bundles: u32,
        mempool_count: usize,
    },
    Error {
        error: String,
    },
}

// ============================================================================
// Sync Configuration
// ============================================================================

#[derive(Debug)]
pub struct SyncConfig {
    pub plc_url: String,
    pub continuous: bool,
    pub interval: Duration,
    pub max_bundles: usize,
    pub verbose: bool,
    pub shutdown_rx: Option<tokio::sync::watch::Receiver<bool>>,
    pub shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    pub fetch_log: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            plc_url: constants::DEFAULT_PLC_DIRECTORY_URL.to_string(),
            continuous: false,
            interval: Duration::from_secs(60),
            max_bundles: 0,
            verbose: false,
            shutdown_rx: None,
            shutdown_tx: None,
            fetch_log: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct SyncStats {
    pub bundles_synced: usize,
    pub operations_fetched: usize,
    pub total_duration: Duration,
}

// ============================================================================
// Sync Logger Trait
// ============================================================================

/// Trait for logging sync events
pub trait SyncLogger: Send + Sync {
    fn on_sync_start(&self, interval: Duration);

    #[allow(clippy::too_many_arguments)]
    fn on_bundle_created(
        &self,
        bundle_num: u32,
        hash: &str,
        age: &str,
        fetch_duration_ms: u64,
        bundle_save_ms: u64,
        index_ms: u64,
        total_duration_ms: u64,
        fetch_requests: usize,
        did_index_compacted: bool,
        unique_dids: u32,
        size_bytes: u64,
        fetch_wait_ms: u64,
        fetch_http_ms: u64,
    );

    // Allow the sync logger to accept multiple arguments for detailed bundle info
    // (Removed workaround method; use allow attribute on trait method instead)

    fn on_caught_up(
        &self,
        next_bundle: u32,
        mempool_count: usize,
        new_ops: usize,
        fetch_duration_ms: u64,
    );

    fn on_initial_sync_complete(
        &self,
        total_bundles: u32,
        mempool_count: usize,
        interval: Duration,
    );

    fn on_error(&self, error: &str);

    /// Get a reference to self as Any for downcasting
    fn as_any(&self) -> &dyn Any;
}

/// Unified sync logger (used for both CLI and server)
pub struct SyncLoggerImpl {
    verbose: Option<Arc<Mutex<bool>>>,
    interval: Option<Duration>,
}

impl SyncLoggerImpl {
    /// Create a new logger for server/continuous mode (with verbose and interval)
    pub fn new_server(verbose: bool, interval: Duration) -> Self {
        Self {
            verbose: Some(Arc::new(Mutex::new(verbose))),
            interval: Some(interval),
        }
    }

    /// Create a new logger for CLI/one-time mode
    pub fn new_cli() -> Self {
        Self {
            verbose: None,
            interval: None,
        }
    }

    /// Get a clone of the verbose state Arc for external access (server mode only)
    pub fn verbose_handle(&self) -> Option<Arc<Mutex<bool>>> {
        self.verbose.clone()
    }

    /// Toggle verbose mode (server mode only)
    pub fn toggle_verbose(&self) -> Option<bool> {
        self.verbose.as_ref().map(|verbose| {
            let mut v = verbose.lock().unwrap();
            *v = !*v;
            *v
        })
    }

    /// Set verbose mode (server mode only)
    pub fn set_verbose(&self, value: bool) {
        if let Some(verbose) = &self.verbose {
            let mut v = verbose.lock().unwrap();
            *v = value;
        }
    }
}

impl SyncLogger for SyncLoggerImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn on_sync_start(&self, interval: Duration) {
        eprintln!("[Sync] Starting initial sync...");
        if let Some(verbose) = &self.verbose
            && *verbose.lock().unwrap()
        {
            eprintln!("[Sync] Sync loop interval: {:?}", interval);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_bundle_created(
        &self,
        bundle_num: u32,
        hash: &str,
        age: &str,
        _fetch_duration_ms: u64,
        bundle_save_ms: u64,
        index_ms: u64,
        _total_duration_ms: u64,
        fetch_requests: usize,
        _did_index_compacted: bool,
        unique_dids: u32,
        size_bytes: u64,
        fetch_wait_ms: u64,
        fetch_http_ms: u64,
    ) {
        let fetch_secs = fetch_http_ms as f64 / 1000.0;
        let wait_secs = fetch_wait_ms as f64 / 1000.0;
        let size_kb = size_bytes as f64 / 1024.0;
        let size_str = if size_kb >= 1024.0 {
            format!("{:.1}MB", size_kb / 1024.0)
        } else {
            format!("{:.0}KB", size_kb)
        };
        let base = format!(
            "[INFO] → Bundle {:06} | {} | {} dids | {} | fetch: {:.2}s ({} reqs, {:.1}s wait) | save: {}ms",
            bundle_num,
            hash,
            unique_dids,
            size_str,
            fetch_secs,
            fetch_requests,
            wait_secs,
            bundle_save_ms
        );
        if index_ms > 0 {
            eprintln!("{} | index: {}ms | {}", base, index_ms, age);
        } else {
            eprintln!("{} | {}", base, age);
        }
    }

    fn on_caught_up(
        &self,
        next_bundle: u32,
        mempool_count: usize,
        new_ops: usize,
        fetch_duration_ms: u64,
    ) {
        if new_ops > 0 {
            eprintln!(
                "[Sync] ✓ Bundle {:06} (upcoming) | mempool: {} ({:+}) | fetch: {}ms",
                next_bundle, mempool_count, new_ops as i32, fetch_duration_ms
            );
        } else {
            eprintln!(
                "[Sync] ✓ Bundle {:06} (upcoming) | mempool: {} | fetch: {}ms",
                next_bundle, mempool_count, fetch_duration_ms
            );
        }
    }

    fn on_initial_sync_complete(
        &self,
        total_bundles: u32,
        mempool_count: usize,
        _interval: Duration,
    ) {
        eprintln!(
            "[Sync] ✓ Initial sync complete ({} bundles synced)",
            total_bundles
        );
        if mempool_count > 0 {
            eprintln!("[Sync] ✓ Mempool: {} operations", mempool_count);
        }
        // Only show monitoring message for continuous mode (when interval is stored)
        if let Some(display_interval) = self.interval {
            eprintln!(
                "[Sync] Now monitoring for new operations (interval: {:?})...",
                display_interval
            );
        }
    }

    fn on_error(&self, error: &str) {
        eprintln!("[Sync] Error during sync: {}", error);
    }
}

// ============================================================================
// Sync Manager
// ============================================================================

pub struct SyncManager {
    manager: std::sync::Arc<crate::manager::BundleManager>,
    client: PLCClient,
    config: SyncConfig,
    logger: Option<Box<dyn SyncLogger>>,
    #[allow(clippy::type_complexity)]
    event_callback: Option<Box<dyn Fn(&SyncEvent) + Send + Sync>>,
}

impl SyncManager {
    pub fn new(
        manager: std::sync::Arc<crate::manager::BundleManager>,
        client: PLCClient,
        config: SyncConfig,
    ) -> Self {
        Self {
            manager,
            client,
            config,
            logger: None,
            event_callback: None,
        }
    }

    /// Set a logger for sync events (replaces default formatting)
    pub fn with_logger<L>(mut self, logger: L) -> Self
    where
        L: SyncLogger + 'static,
    {
        self.logger = Some(Box::new(logger));
        self
    }

    /// Set a custom event callback (for advanced use cases)
    pub fn with_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(&SyncEvent) + Send + Sync + 'static,
    {
        self.event_callback = Some(Box::new(callback));
        self
    }

    fn handle_event(&self, event: &SyncEvent) {
        // First, call custom callback if provided
        if let Some(callback) = &self.event_callback {
            callback(event);
        }

        // Then, call logger if provided
        if let Some(logger) = &self.logger {
            match event {
                SyncEvent::BundleCreated {
                    bundle_num,
                    hash,
                    age,
                    fetch_duration_ms,
                    bundle_save_ms,
                    index_ms,
                    total_duration_ms,
                    fetch_requests,
                    did_index_compacted,
                    unique_dids,
                    size_bytes,
                    fetch_wait_ms,
                    fetch_http_ms,
                } => {
                    logger.on_bundle_created(
                        *bundle_num,
                        hash,
                        age,
                        *fetch_duration_ms,
                        *bundle_save_ms,
                        *index_ms,
                        *total_duration_ms,
                        *fetch_requests,
                        *did_index_compacted,
                        *unique_dids,
                        *size_bytes,
                        *fetch_wait_ms,
                        *fetch_http_ms,
                    );
                }
                SyncEvent::CaughtUp {
                    next_bundle,
                    mempool_count,
                    new_ops,
                    fetch_duration_ms,
                } => {
                    logger.on_caught_up(*next_bundle, *mempool_count, *new_ops, *fetch_duration_ms);
                }
                SyncEvent::InitialSyncComplete {
                    total_bundles,
                    mempool_count,
                } => {
                    logger.on_initial_sync_complete(
                        *total_bundles,
                        *mempool_count,
                        self.config.interval,
                    );
                }
                SyncEvent::Error { error } => {
                    logger.on_error(error);
                }
            }
        }
    }

    /// Show compaction message if index was compacted during bundle sync
    fn show_compaction_if_needed(
        &self,
        did_index_compacted: bool,
        delta_segments_before: u64,
        index_ms: u64,
    ) {
        if did_index_compacted {
            let stats_after = self.manager.get_did_index_stats();
            let delta_segments_after = stats_after
                .get("delta_segments")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let segments_compacted = delta_segments_before.saturating_sub(delta_segments_after);
            eprintln!(
                "[Sync] ✓ Index compacted | segments: {} → {} ({} removed) | index: {}ms",
                delta_segments_before, delta_segments_after, segments_compacted, index_ms
            );
        }
    }

    pub async fn run_once(&self, max_bundles: Option<usize>) -> Result<usize> {
        let mut synced = 0;

        loop {
            // Check for shutdown if configured
            if let Some(ref shutdown_rx) = self.config.shutdown_rx
                && *shutdown_rx.borrow()
            {
                break;
            }

            // Get stats before sync to track compaction
            let stats_before = self.manager.get_did_index_stats();
            let delta_segments_before = stats_before
                .get("delta_segments")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            match self
                .manager
                .sync_next_bundle(&self.client, None, true, self.config.fetch_log)
                .await
            {
                Ok(crate::manager::SyncResult::BundleCreated {
                    bundle_num,
                    mempool_count: _,
                    duration_ms,
                    fetch_duration_ms,
                    bundle_save_ms,
                    index_ms,
                    fetch_requests,
                    hash,
                    age,
                    did_index_compacted,
                    unique_dids,
                    size_bytes,
                    fetch_wait_ms,
                    fetch_http_ms,
                }) => {
                    synced += 1;

                    self.handle_event(&SyncEvent::BundleCreated {
                        bundle_num,
                        hash,
                        age,
                        fetch_duration_ms,
                        bundle_save_ms,
                        index_ms,
                        total_duration_ms: duration_ms,
                        fetch_requests,
                        did_index_compacted,
                        unique_dids,
                        size_bytes,
                        fetch_wait_ms,
                        fetch_http_ms,
                    });

                    // Show compaction message if index was compacted
                    self.show_compaction_if_needed(
                        did_index_compacted,
                        delta_segments_before,
                        index_ms,
                    );

                    // Check if we've reached the limit
                    if let Some(max) = max_bundles
                        && synced >= max
                    {
                        break;
                    }
                }
                Ok(crate::manager::SyncResult::CaughtUp {
                    next_bundle,
                    mempool_count,
                    new_ops,
                    fetch_duration_ms,
                }) => {
                    self.handle_event(&SyncEvent::CaughtUp {
                        next_bundle,
                        mempool_count,
                        new_ops,
                        fetch_duration_ms,
                    });
                    break;
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    self.handle_event(&SyncEvent::Error {
                        error: error_msg.clone(),
                    });

                    // Trigger shutdown on error if configured
                    // This ensures the application terminates on persistent errors
                    if let Some(ref shutdown_tx) = self.config.shutdown_tx {
                        let _ = shutdown_tx.send(true);
                    }

                    return Err(e);
                }
            }

            // Small delay between bundles
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        Ok(synced)
    }

    pub async fn run_continuous(&self) -> Result<()> {
        use tokio::time::sleep;

        let mut total_synced = 0u32;
        let mut is_initial_sync = true;
        let mut did_index_batch_done = false;
        let mut initial_sync_first_bundle: Option<u32> = None;

        // Notify logger that sync is starting
        if let Some(logger) = &self.logger {
            logger.on_sync_start(self.config.interval);
        }

        // Keyboard handler for verbose toggle - DISABLED
        // This feature is disabled because reading from stdin blocks shutdown.
        // Users can still use verbose mode by passing --verbose flag at startup.
        //
        // TODO: Implement a non-blocking alternative using signals or other IPC
        #[cfg(feature = "crossterm")]
        {
            // Keyboard input feature temporarily disabled to fix shutdown freeze
            // The stdin reading was causing the server to hang on Ctrl+C
        }

        loop {
            // Check for shutdown before starting sync
            if let Some(ref shutdown_rx) = self.config.shutdown_rx
                && *shutdown_rx.borrow()
            {
                if self.config.verbose {
                    eprintln!("[Sync] Shutdown requested, stopping...");
                }
                break;
            }

            // Update DID index on every bundle (now fast with delta segments)
            // Get stats before sync to track compaction
            let stats_before = self.manager.get_did_index_stats();
            let delta_segments_before = stats_before
                .get("delta_segments")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            let sync_result = self
                .manager
                .sync_next_bundle(
                    &self.client,
                    self.config.shutdown_rx.clone(),
                    !is_initial_sync,
                    self.config.fetch_log,
                )
                .await;

            match sync_result {
                Ok(crate::manager::SyncResult::BundleCreated {
                    bundle_num,
                    mempool_count: _,
                    duration_ms,
                    fetch_duration_ms,
                    bundle_save_ms,
                    index_ms,
                    fetch_requests,
                    hash,
                    age,
                    did_index_compacted,
                    unique_dids,
                    size_bytes,
                    fetch_wait_ms,
                    fetch_http_ms,
                }) => {
                    total_synced += 1;
                    if is_initial_sync && initial_sync_first_bundle.is_none() {
                        initial_sync_first_bundle = Some(bundle_num);
                    }

                    // Reset error counter on successful sync
                    use std::sync::atomic::{AtomicU32, Ordering};
                    static CONSECUTIVE_ERRORS: AtomicU32 = AtomicU32::new(0);
                    CONSECUTIVE_ERRORS.store(0, Ordering::Relaxed);

                    self.handle_event(&SyncEvent::BundleCreated {
                        bundle_num,
                        hash,
                        age,
                        fetch_duration_ms,
                        bundle_save_ms,
                        index_ms,
                        total_duration_ms: duration_ms,
                        fetch_requests,
                        did_index_compacted,
                        unique_dids,
                        size_bytes,
                        fetch_wait_ms,
                        fetch_http_ms,
                    });

                    // Show compaction message if index was compacted
                    self.show_compaction_if_needed(
                        did_index_compacted,
                        delta_segments_before,
                        index_ms,
                    );

                    // Check max bundles limit
                    if self.config.max_bundles > 0
                        && total_synced as usize >= self.config.max_bundles
                    {
                        if self.config.verbose {
                            eprintln!(
                                "[Sync] Reached max bundles limit ({})",
                                self.config.max_bundles
                            );
                        }
                        break;
                    }

                    // Check for shutdown before sleeping
                    if let Some(ref shutdown_rx) = self.config.shutdown_rx
                        && *shutdown_rx.borrow()
                    {
                        if self.config.verbose {
                            eprintln!("[Sync] Shutdown requested, stopping...");
                        }
                        break;
                    }

                    // During initial sync, sleep briefly (500ms) to avoid hammering the API
                    // After initial sync, use the full interval
                    // Use select to allow cancellation during sleep
                    let sleep_duration = if is_initial_sync {
                        Duration::from_millis(500)
                    } else {
                        self.config.interval
                    };

                    if let Some(ref shutdown_rx) = self.config.shutdown_rx {
                        let mut shutdown_rx = shutdown_rx.clone();
                        tokio::select! {
                            _ = sleep(sleep_duration) => {}
                            _ = shutdown_rx.changed() => {
                                if *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                        }
                    } else {
                        sleep(sleep_duration).await;
                    }
                }
                Ok(crate::manager::SyncResult::CaughtUp {
                    next_bundle,
                    mempool_count,
                    new_ops,
                    fetch_duration_ms,
                }) => {
                    // Check for shutdown
                    if let Some(ref shutdown_rx) = self.config.shutdown_rx
                        && *shutdown_rx.borrow()
                    {
                        if self.config.verbose {
                            eprintln!("[Sync] Shutdown requested, stopping...");
                        }
                        break;
                    }

                    // Caught up to the end of the chain
                    // When initial sync finishes, perform a single batch DID index update if the index is empty
                    // or if we created bundles during initial sync with per-bundle updates disabled.
                    if is_initial_sync && !did_index_batch_done {
                        let stats = self.manager.get_did_index_stats();
                        let total_dids = stats
                            .get("total_dids")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        let total_entries = stats
                            .get("total_entries")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);

                        let end_bundle = self.manager.get_last_bundle();
                        let start_bundle = initial_sync_first_bundle.unwrap_or(1);

                        // Only run batch update if there are bundles to process and either index is empty
                        // or we created some bundles during this initial sync.
                        let created_bundles = total_synced > 0;
                        let index_is_empty = total_dids == 0 && total_entries == 0;
                        if end_bundle >= start_bundle && (index_is_empty || created_bundles) {
                            if self.config.verbose {
                                eprintln!(
                                    "[Sync] Performing batch DID index update: {} → {} (index empty={}, created_bundles={})",
                                    start_bundle, end_bundle, index_is_empty, created_bundles
                                );
                            }
                            if let Err(e) = self
                                .manager
                                .batch_update_did_index_async(start_bundle, end_bundle, true)
                                .await
                            {
                                eprintln!(
                                    "[Sync] Batch DID index update failed after initial sync: {}",
                                    e
                                );
                            } else {
                                did_index_batch_done = true;
                            }
                        }

                        is_initial_sync = false;
                        self.handle_event(&SyncEvent::InitialSyncComplete {
                            total_bundles: total_synced,
                            mempool_count,
                        });
                    }

                    self.handle_event(&SyncEvent::CaughtUp {
                        next_bundle,
                        mempool_count,
                        new_ops,
                        fetch_duration_ms,
                    });

                    // Always sleep for the full interval when caught up (monitoring mode)
                    // Use select to allow cancellation during sleep
                    if let Some(ref shutdown_rx) = self.config.shutdown_rx {
                        let mut shutdown_rx = shutdown_rx.clone();
                        tokio::select! {
                            _ = sleep(self.config.interval) => {}
                            _ = shutdown_rx.changed() => {
                                if *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                        }
                    } else {
                        sleep(self.config.interval).await;
                    }
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    self.handle_event(&SyncEvent::Error {
                        error: error_msg.clone(),
                    });

                    // Determine if error is retryable
                    let is_retryable = is_retryable_error(&error_msg);

                    if is_retryable {
                        // Retry transient errors with exponential backoff
                        use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
                        static CONSECUTIVE_ERRORS: AtomicU32 = AtomicU32::new(0);
                        static LAST_ERROR_TIME_SECS: AtomicU64 = AtomicU64::new(0);

                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();

                        let last_error_secs = LAST_ERROR_TIME_SECS.load(Ordering::Relaxed);

                        // Reset error count if last error was more than 5 minutes ago
                        if last_error_secs > 0 && now - last_error_secs > 300 {
                            CONSECUTIVE_ERRORS.store(0, Ordering::Relaxed);
                        }

                        let error_count = CONSECUTIVE_ERRORS.fetch_add(1, Ordering::Relaxed) + 1;
                        LAST_ERROR_TIME_SECS.store(now, Ordering::Relaxed);

                        // Calculate backoff with exponential increase (cap at 5 minutes)
                        let backoff_secs = std::cmp::min(2u64.pow((error_count - 1).min(8)), 300);

                        if self.config.verbose || error_count == 1 {
                            eprintln!(
                                "[Sync] Retryable error (attempt {}): {}",
                                error_count, error_msg
                            );
                            eprintln!("[Sync] Retrying in {} seconds...", backoff_secs);
                        }

                        // Too many consecutive errors - give up
                        if error_count >= 10 {
                            eprintln!(
                                "[Sync] Too many consecutive errors ({}) - shutting down",
                                error_count
                            );

                            if let Some(ref shutdown_tx) = self.config.shutdown_tx {
                                let _ = shutdown_tx.send(true);
                            }
                            return Err(e);
                        }

                        // Wait with backoff, checking for shutdown
                        let backoff_duration = Duration::from_secs(backoff_secs);
                        if let Some(ref shutdown_rx) = self.config.shutdown_rx {
                            let mut shutdown_rx = shutdown_rx.clone();
                            tokio::select! {
                                _ = sleep(backoff_duration) => {}
                                _ = shutdown_rx.changed() => {
                                    if *shutdown_rx.borrow() {
                                        return Ok(());
                                    }
                                }
                            }
                        } else {
                            sleep(backoff_duration).await;
                        }
                    } else {
                        // Fatal error - shutdown immediately
                        eprintln!("[Sync] Fatal error - shutting down: {}", error_msg);

                        if let Some(ref shutdown_tx) = self.config.shutdown_tx {
                            let _ = shutdown_tx.send(true);
                        }
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Determine if an error is retryable
fn is_retryable_error(error_msg: &str) -> bool {
    let error_lower = error_msg.to_lowercase();

    // Retryable errors (transient issues)
    let retryable_patterns = [
        "connection",
        "timeout",
        "network",
        "temporary",
        "unavailable",
        "too many",
        "rate limit",
        "503",
        "502",
        "504",
        "broken pipe",
        "connection reset",
        "dns",
        "io error",
        "interrupted",
        "would block",
    ];

    // Fatal errors (permanent issues that won't be fixed by retrying)
    let fatal_patterns = [
        "no such file",
        "permission denied",
        "disk full",
        "quota exceeded",
        "corrupted",
        "invalid",
        "parse error",
        "404",
        "403",
        "401",
        "shutdown requested",
    ];

    // Check for fatal patterns first
    for pattern in &fatal_patterns {
        if error_lower.contains(pattern) {
            return false;
        }
    }

    // Check for retryable patterns
    for pattern in &retryable_patterns {
        if error_lower.contains(pattern) {
            return true;
        }
    }

    // Default: retry unknown errors (conservative approach)
    // This prevents the server from crashing on unexpected errors
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boundary_cids() {
        let ops = vec![
            Operation {
                did: "did:plc:1".into(),
                operation: sonic_rs::from_str("null").unwrap_or_else(|_| Value::new()),
                cid: Some("cid1".into()),
                nullified: false,
                created_at: "2024-01-01T00:00:00Z".into(),
                extra: sonic_rs::from_str("null").unwrap_or_else(|_| Value::new()),
                raw_json: None,
            },
            Operation {
                did: "did:plc:2".into(),
                operation: sonic_rs::from_str("null").unwrap_or_else(|_| Value::new()),
                cid: Some("cid2".into()),
                nullified: false,
                created_at: "2024-01-01T00:00:01Z".into(),
                extra: sonic_rs::from_str("null").unwrap_or_else(|_| Value::new()),
                raw_json: None,
            },
            Operation {
                did: "did:plc:3".into(),
                operation: sonic_rs::from_str("null").unwrap_or_else(|_| Value::new()),
                cid: Some("cid3".into()),
                nullified: false,
                created_at: "2024-01-01T00:00:01Z".into(), // Same time as cid2
                extra: sonic_rs::from_str("null").unwrap_or_else(|_| Value::new()),
                raw_json: None,
            },
        ];

        let boundary = get_boundary_cids(&ops);
        assert_eq!(boundary.len(), 2);
        assert!(boundary.contains("cid2"));
        assert!(boundary.contains("cid3"));
    }

    #[test]
    fn test_strip_duplicates() {
        let mut prev = HashSet::new();
        prev.insert("cid1".to_string());

        let ops = vec![
            Operation {
                did: "did:plc:1".into(),
                operation: sonic_rs::from_str("null").unwrap_or_else(|_| Value::new()),
                cid: Some("cid1".into()), // Duplicate
                nullified: false,
                created_at: "2024-01-01T00:00:00Z".into(),
                extra: sonic_rs::from_str("null").unwrap_or_else(|_| Value::new()),
                raw_json: None,
            },
            Operation {
                did: "did:plc:2".into(),
                operation: sonic_rs::from_str("null").unwrap_or_else(|_| Value::new()),
                cid: Some("cid2".into()), // New
                nullified: false,
                created_at: "2024-01-01T00:00:01Z".into(),
                extra: sonic_rs::from_str("null").unwrap_or_else(|_| Value::new()),
                raw_json: None,
            },
        ];

        let result = strip_boundary_duplicates(ops, &prev);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].cid.as_ref().unwrap(), "cid2");
    }

    // Additional comprehensive tests
    fn create_operation_helper(did: &str, cid: Option<&str>, created_at: &str) -> Operation {
        Operation {
            did: did.to_string(),
            operation: Value::new(),
            cid: cid.map(|s| s.to_string()),
            nullified: false,
            created_at: created_at.to_string(),
            extra: Value::new(),
            raw_json: None,
        }
    }

    #[test]
    fn test_get_boundary_cids_empty() {
        let ops = vec![];
        let cids = get_boundary_cids(&ops);
        assert!(cids.is_empty());
    }

    #[test]
    fn test_get_boundary_cids_single() {
        let ops = vec![create_operation_helper(
            "did:plc:test",
            Some("cid1"),
            "2024-01-01T00:00:00Z",
        )];
        let cids = get_boundary_cids(&ops);
        assert_eq!(cids.len(), 1);
        assert!(cids.contains("cid1"));
    }

    #[test]
    fn test_get_boundary_cids_multiple_same_time() {
        let ops = vec![
            create_operation_helper("did:plc:test1", Some("cid1"), "2024-01-01T00:00:00Z"),
            create_operation_helper("did:plc:test2", Some("cid2"), "2024-01-01T00:00:00Z"),
            create_operation_helper("did:plc:test3", Some("cid3"), "2024-01-01T00:00:00Z"),
        ];
        let cids = get_boundary_cids(&ops);
        assert_eq!(cids.len(), 3);
        assert!(cids.contains("cid1"));
        assert!(cids.contains("cid2"));
        assert!(cids.contains("cid3"));
    }

    #[test]
    fn test_get_boundary_cids_different_times() {
        let ops = vec![
            create_operation_helper("did:plc:test1", Some("cid1"), "2024-01-01T00:00:00Z"),
            create_operation_helper("did:plc:test2", Some("cid2"), "2024-01-01T00:00:01Z"),
            create_operation_helper("did:plc:test3", Some("cid3"), "2024-01-01T00:00:01Z"),
        ];
        let cids = get_boundary_cids(&ops);
        assert_eq!(cids.len(), 2);
        assert!(!cids.contains("cid1"));
        assert!(cids.contains("cid2"));
        assert!(cids.contains("cid3"));
    }

    #[test]
    fn test_get_boundary_cids_no_cid() {
        let ops = vec![
            create_operation_helper("did:plc:test1", None, "2024-01-01T00:00:00Z"),
            create_operation_helper("did:plc:test2", Some("cid2"), "2024-01-01T00:00:00Z"),
        ];
        let cids = get_boundary_cids(&ops);
        assert_eq!(cids.len(), 1);
        assert!(cids.contains("cid2"));
    }

    #[test]
    fn test_strip_boundary_duplicates_empty() {
        let ops = vec![];
        let prev_boundary = HashSet::new();
        let result = strip_boundary_duplicates(ops, &prev_boundary);
        assert!(result.is_empty());
    }

    #[test]
    fn test_strip_boundary_duplicates_no_prev_boundary() {
        let ops = vec![
            create_operation_helper("did:plc:test1", Some("cid1"), "2024-01-01T00:00:00Z"),
            create_operation_helper("did:plc:test2", Some("cid2"), "2024-01-01T00:00:00Z"),
        ];
        let prev_boundary = HashSet::new();
        let result = strip_boundary_duplicates(ops, &prev_boundary);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_strip_boundary_duplicates_with_matches() {
        let ops = vec![
            create_operation_helper("did:plc:test1", Some("cid1"), "2024-01-01T00:00:00Z"),
            create_operation_helper("did:plc:test2", Some("cid2"), "2024-01-01T00:00:00Z"),
            create_operation_helper("did:plc:test3", Some("cid3"), "2024-01-01T00:00:00Z"),
        ];
        let mut prev_boundary = HashSet::new();
        prev_boundary.insert("cid2".to_string());
        let result = strip_boundary_duplicates(ops, &prev_boundary);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].cid, Some("cid1".to_string()));
        assert_eq!(result[1].cid, Some("cid3".to_string()));
    }

    #[test]
    fn test_strip_boundary_duplicates_no_cid() {
        let ops = vec![
            create_operation_helper("did:plc:test1", None, "2024-01-01T00:00:00Z"),
            create_operation_helper("did:plc:test2", Some("cid2"), "2024-01-01T00:00:00Z"),
        ];
        let mut prev_boundary = HashSet::new();
        prev_boundary.insert("cid2".to_string());
        let result = strip_boundary_duplicates(ops, &prev_boundary);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].cid, None);
    }

    #[test]
    fn test_strip_boundary_duplicates_all_matched() {
        let ops = vec![
            create_operation_helper("did:plc:test1", Some("cid1"), "2024-01-01T00:00:00Z"),
            create_operation_helper("did:plc:test2", Some("cid2"), "2024-01-01T00:00:00Z"),
        ];
        let mut prev_boundary = HashSet::new();
        prev_boundary.insert("cid1".to_string());
        prev_boundary.insert("cid2".to_string());
        let result = strip_boundary_duplicates(ops, &prev_boundary);
        assert!(result.is_empty());
    }
}
