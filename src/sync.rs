// Sync module - PLC directory synchronization
use crate::constants;
use crate::operations::Operation;
use anyhow::Result;
use serde::Deserialize;
use std::any::Any;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ============================================================================
// PLC Client
// ============================================================================

pub struct PLCClient {
    client: reqwest::Client,
    base_url: String,
    rate_limiter: RateLimiter,
    last_retry_after: std::sync::Arc<tokio::sync::Mutex<Option<Duration>>>,
    request_timestamps: Arc<std::sync::Mutex<VecDeque<Instant>>>,
    rate_limit_period: Duration,
}

impl PLCClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let rate_limit_period = Duration::from_secs(constants::HTTP_TIMEOUT_SECS);
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(constants::HTTP_TIMEOUT_SECS))
                .build()?,
            base_url: base_url.into(),
            rate_limiter: RateLimiter::new(constants::DEFAULT_RATE_LIMIT, rate_limit_period),
            last_retry_after: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            request_timestamps: Arc::new(std::sync::Mutex::new(VecDeque::new())),
            rate_limit_period,
        })
    }

    /// Record a request timestamp and clean up old entries
    fn record_request(&self) {
        let now = Instant::now();
        let mut timestamps = self.request_timestamps.lock().unwrap();

        // Remove timestamps older than the rate limit period
        // If checked_sub fails (shouldn't happen in practice), use now as cutoff (counts all)
        let cutoff = now.checked_sub(self.rate_limit_period).unwrap_or(now);
        while let Some(&oldest) = timestamps.front() {
            if oldest < cutoff {
                timestamps.pop_front();
            } else {
                break;
            }
        }

        timestamps.push_back(now);
    }

    /// Count requests made in the rate limit period
    fn count_requests_in_period(&self) -> usize {
        let now = Instant::now();
        let timestamps = self.request_timestamps.lock().unwrap();

        // If checked_sub fails (shouldn't happen in practice), use now as cutoff (counts all)
        let cutoff = now.checked_sub(self.rate_limit_period).unwrap_or(now);
        timestamps.iter().filter(|&&ts| ts >= cutoff).count()
    }

    pub async fn fetch_operations(&self, after: &str, count: usize) -> Result<Vec<PLCOperation>> {
        self.fetch_operations_with_retry(after, count, 5).await
    }

    async fn fetch_operations_with_retry(
        &self,
        after: &str,
        count: usize,
        max_retries: usize,
    ) -> Result<Vec<PLCOperation>> {
        let mut backoff = Duration::from_secs(1);
        let mut last_err = None;

        for attempt in 1..=max_retries {
            // Wait for rate limiter token
            self.rate_limiter.wait().await;

            // Clear previous retry_after
            *self.last_retry_after.lock().await = None;

            // Record this request attempt
            self.record_request();

            match self.do_fetch_operations(after, count).await {
                Ok(operations) => return Ok(operations),
                Err(e) => {
                    last_err = Some(e);

                    // Check if it's a rate limit error (429)
                    let retry_after = self.last_retry_after.lock().await.take();
                    if let Some(retry_after) = retry_after {
                        let requests_in_period = self.count_requests_in_period();
                        let rate_limit = constants::DEFAULT_RATE_LIMIT;
                        eprintln!(
                            "[Sync] Rate limited by PLC directory ({} requests in last {:?}, limit: {}), waiting {:?} before retry {}/{}",
                            requests_in_period,
                            self.rate_limit_period,
                            rate_limit,
                            retry_after,
                            attempt,
                            max_retries
                        );
                        tokio::time::sleep(retry_after).await;
                        continue;
                    }

                    // Other errors - exponential backoff
                    if attempt < max_retries {
                        eprintln!(
                            "[Sync] Request failed (attempt {}/{}): {}, retrying in {:?}",
                            attempt,
                            max_retries,
                            last_err.as_ref().unwrap(),
                            backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff *= 2; // Exponential backoff
                    }
                }
            }
        }

        anyhow::bail!(
            "Failed after {} attempts: {}",
            max_retries,
            last_err.unwrap_or_else(|| anyhow::anyhow!("Unknown error"))
        )
    }

    async fn do_fetch_operations(&self, after: &str, count: usize) -> Result<Vec<PLCOperation>> {
        let url = format!("{}/export", self.base_url);
        let response = self
            .client
            .get(&url)
            .query(&[("after", after), ("count", &count.to_string())])
            .header("User-Agent", constants::user_agent())
            .send()
            .await?;

        // Handle rate limiting (429)
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = parse_retry_after(&response);
            *self.last_retry_after.lock().await = Some(retry_after);
            anyhow::bail!("Rate limited (429)");
        }

        if !response.status().is_success() {
            anyhow::bail!("PLC request failed: {}", response.status());
        }

        let body = response.text().await?;
        let mut operations = Vec::new();

        for line in body.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<PLCOperation>(line) {
                Ok(mut op) => {
                    // CRITICAL: Store raw JSON to preserve exact byte content
                    // This is required by the V1 specification (docs/specification.md § 4.2)
                    // to ensure content_hash is reproducible across implementations.
                    // Re-serializing would change key order/whitespace and break hash verification.
                    op.raw_json = Some(line.to_string());
                    operations.push(op);
                }
                Err(e) => eprintln!("Warning: failed to parse operation: {}", e),
            }
        }

        Ok(operations)
    }
}

/// Parse the Retry-After header from a response
/// Returns the duration to wait before retrying, capped at 60 seconds maximum
fn parse_retry_after(response: &reqwest::Response) -> Duration {
    const MAX_RETRY_SECONDS: u64 = 60;

    if let Some(retry_after_header) = response.headers().get("retry-after") {
        if let Ok(retry_after_str) = retry_after_header.to_str() {
            // Try parsing as seconds (integer) - most common format
            if let Ok(seconds) = retry_after_str.parse::<u64>() {
                // Cap at maximum wait time
                return Duration::from_secs(seconds.min(MAX_RETRY_SECONDS));
            }

            // Try parsing as HTTP date (RFC 7231)
            // httpdate::parse_http_date returns a SystemTime
            if let Ok(http_time) = httpdate::parse_http_date(retry_after_str) {
                if let Ok(duration) = http_time.duration_since(std::time::SystemTime::now()) {
                    // Cap at maximum wait time
                    return duration.min(Duration::from_secs(MAX_RETRY_SECONDS));
                }
            }
        }
    }

    // Default to 60 seconds if no header or parsing fails
    Duration::from_secs(MAX_RETRY_SECONDS)
}

// Simple token bucket rate limiter
// Prevents burst requests by starting with 0 permits and refilling at steady rate
struct RateLimiter {
    semaphore: std::sync::Arc<tokio::sync::Semaphore>,
}

impl RateLimiter {
    fn new(requests_per_period: usize, period: Duration) -> Self {
        // Use a proper token bucket rate limiter
        // Start with 0 permits to prevent initial burst
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let sem_clone = semaphore.clone();

        // Calculate refill rate: period / requests_per_period
        // For 72 req/min: 60 seconds / 72 = 0.833 seconds per request
        let refill_rate = period / requests_per_period as u32;

        // Spawn background task to refill permits at steady rate
        // CRITICAL: Add first permit immediately, then refill at steady rate
        let refill_rate_clone = refill_rate;
        tokio::spawn(async move {
            // Add first permit immediately so first request can proceed
            if sem_clone.available_permits() < requests_per_period {
                sem_clone.add_permits(1);
            }

            // Then refill at steady rate
            loop {
                tokio::time::sleep(refill_rate_clone).await;
                // Add one permit if under capacity
                // This creates a token bucket: max capacity is requests_per_period
                if sem_clone.available_permits() < requests_per_period {
                    sem_clone.add_permits(1);
                }
            }
        });

        Self { semaphore }
    }

    async fn wait(&self) {
        // Wait for a permit to become available
        // This will block until a permit is available (from refill task)
        let _ = self.semaphore.acquire().await;
    }
}

#[derive(Debug, Deserialize)]
pub struct PLCOperation {
    did: String,
    operation: serde_json::Value,
    cid: String,
    #[serde(default)]
    nullified: Option<serde_json::Value>,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(skip)]
    pub raw_json: Option<String>,
}

impl From<PLCOperation> for Operation {
    fn from(plc: PLCOperation) -> Self {
        let is_nullified = plc.nullified.as_ref().map_or(false, |v| {
            v.as_bool().unwrap_or(false) || v.as_str().map_or(false, |s| !s.is_empty())
        });

        Self {
            did: plc.did,
            operation: plc.operation,
            cid: Some(plc.cid),
            nullified: is_nullified,
            created_at: plc.created_at,
            extra: serde_json::Value::Null,
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
            .map_or(true, |cid| !prev_boundary.contains(cid))
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
    );

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

/// Server-style logger (detailed output with timing info)
pub struct ServerLogger {
    verbose: Arc<Mutex<bool>>,
    interval: Duration,
}

impl ServerLogger {
    pub fn new(verbose: bool, interval: Duration) -> Self {
        Self {
            verbose: Arc::new(Mutex::new(verbose)),
            interval,
        }
    }

    /// Get a clone of the verbose state Arc for external access
    pub fn verbose_handle(&self) -> Arc<Mutex<bool>> {
        self.verbose.clone()
    }

    /// Toggle verbose mode
    pub fn toggle_verbose(&self) -> bool {
        let mut verbose = self.verbose.lock().unwrap();
        *verbose = !*verbose;
        *verbose
    }

    /// Set verbose mode
    pub fn set_verbose(&self, value: bool) {
        let mut verbose = self.verbose.lock().unwrap();
        *verbose = value;
    }
}

impl SyncLogger for ServerLogger {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn on_sync_start(&self, interval: Duration) {
        eprintln!("[Sync] Starting initial sync...");
        if *self.verbose.lock().unwrap() {
            eprintln!("[Sync] Sync loop interval: {:?}", interval);
        }
    }

    fn on_bundle_created(
        &self,
        bundle_num: u32,
        hash: &str,
        age: &str,
        fetch_duration_ms: u64,
        bundle_save_ms: u64,
        index_ms: u64,
        _total_duration_ms: u64,
        fetch_requests: usize,
        _did_index_compacted: bool,
        unique_dids: u32,
        size_bytes: u64,
    ) {
        let fetch_secs = fetch_duration_ms as f64 / 1000.0;
        let size_kb = size_bytes as f64 / 1024.0;
        let size_str = if size_kb >= 1024.0 {
            format!("{:.1}MB", size_kb / 1024.0)
        } else {
            format!("{:.0}KB", size_kb)
        };

        eprintln!(
            "[INFO] → Bundle {:06} | {} | {} dids | {} | fetch: {:.2}s ({} reqs) | save: {}ms | index: {}ms | {}",
            bundle_num,
            hash,
            unique_dids,
            size_str,
            fetch_secs,
            fetch_requests,
            bundle_save_ms,
            index_ms,
            age
        );
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
                "[Sync] ✓ Bundle {:06} | mempool: {} ({:+}) | fetch: {}ms",
                next_bundle, mempool_count, new_ops as i32, fetch_duration_ms
            );
        } else {
            eprintln!(
                "[Sync] ✓ Bundle {:06} | mempool: {} | fetch: {}ms",
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
        eprintln!(
            "[Sync] Now monitoring for new operations (interval: {:?})...",
            self.interval
        );
    }

    fn on_error(&self, error: &str) {
        eprintln!("[Sync] Error during sync: {}", error);
    }
}

/// CLI-style logger (minimal output, only shows summaries)
pub struct CliLogger {
    quiet: bool,
}

impl CliLogger {
    pub fn new(quiet: bool) -> Self {
        Self { quiet }
    }
}

impl SyncLogger for CliLogger {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn on_sync_start(&self, _interval: Duration) {
        // CLI doesn't show sync start message
    }

    fn on_bundle_created(
        &self,
        _bundle_num: u32,
        _hash: &str,
        _age: &str,
        _fetch_duration_ms: u64,
        _bundle_save_ms: u64,
        _index_ms: u64,
        _total_duration_ms: u64,
        _fetch_requests: usize,
        _did_index_compacted: bool,
        _unique_dids: u32,
        _size_bytes: u64,
    ) {
        // CLI doesn't show individual bundle creation
    }

    fn on_caught_up(
        &self,
        _next_bundle: u32,
        _mempool_count: usize,
        _new_ops: usize,
        _fetch_duration_ms: u64,
    ) {
        // CLI doesn't show caught up events
    }

    fn on_initial_sync_complete(
        &self,
        _total_bundles: u32,
        _mempool_count: usize,
        _interval: Duration,
    ) {
        // CLI doesn't show initial sync complete
    }

    fn on_error(&self, error: &str) {
        if !self.quiet {
            eprintln!("Error: {}", error);
        }
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

    pub async fn run_once(&self, max_bundles: Option<usize>) -> Result<usize> {
        let mut synced = 0;

        loop {
            // Check for shutdown if configured
            if let Some(ref shutdown_rx) = self.config.shutdown_rx {
                if *shutdown_rx.borrow() {
                    break;
                }
            }

            match self.manager.sync_next_bundle(&self.client).await {
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
                    });

                    // Check if we've reached the limit
                    if let Some(max) = max_bundles {
                        if synced >= max {
                            break;
                        }
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
            if let Some(ref shutdown_rx) = self.config.shutdown_rx {
                if *shutdown_rx.borrow() {
                    if self.config.verbose {
                        eprintln!("[Sync] Shutdown requested, stopping...");
                    }
                    break;
                }
            }

            // Update DID index on every bundle (now fast with delta segments)
            let sync_result = self.manager.sync_next_bundle(&self.client).await;

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
                }) => {
                    total_synced += 1;

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
                    });

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
                    if let Some(ref shutdown_rx) = self.config.shutdown_rx {
                        if *shutdown_rx.borrow() {
                            if self.config.verbose {
                                eprintln!("[Sync] Shutdown requested, stopping...");
                            }
                            break;
                        }
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
                    if let Some(ref shutdown_rx) = self.config.shutdown_rx {
                        if *shutdown_rx.borrow() {
                            if self.config.verbose {
                                eprintln!("[Sync] Shutdown requested, stopping...");
                            }
                            break;
                        }
                    }

                    // Caught up to the end of the chain
                    // Mark initial sync as complete ONLY if we actually synced at least one bundle.
                    // This prevents premature "initial sync complete" when we just have a full
                    // mempool from a previous run but still have thousands of bundles to sync.
                    if is_initial_sync && total_synced > 0 {
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
                operation: serde_json::Value::Null,
                cid: Some("cid1".into()),
                nullified: false,
                created_at: "2024-01-01T00:00:00Z".into(),
                extra: serde_json::Value::Null,
                raw_json: None,
            },
            Operation {
                did: "did:plc:2".into(),
                operation: serde_json::Value::Null,
                cid: Some("cid2".into()),
                nullified: false,
                created_at: "2024-01-01T00:00:01Z".into(),
                extra: serde_json::Value::Null,
                raw_json: None,
            },
            Operation {
                did: "did:plc:3".into(),
                operation: serde_json::Value::Null,
                cid: Some("cid3".into()),
                nullified: false,
                created_at: "2024-01-01T00:00:01Z".into(), // Same time as cid2
                extra: serde_json::Value::Null,
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
                operation: serde_json::Value::Null,
                cid: Some("cid1".into()), // Duplicate
                nullified: false,
                created_at: "2024-01-01T00:00:00Z".into(),
                extra: serde_json::Value::Null,
                raw_json: None,
            },
            Operation {
                did: "did:plc:2".into(),
                operation: serde_json::Value::Null,
                cid: Some("cid2".into()), // New
                nullified: false,
                created_at: "2024-01-01T00:00:01Z".into(),
                extra: serde_json::Value::Null,
                raw_json: None,
            },
        ];

        let result = strip_boundary_duplicates(ops, &prev);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].cid.as_ref().unwrap(), "cid2");
    }
}
