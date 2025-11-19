//! Rate-limited HTTP client for PLC directory: fetch operations and DID documents with retries/backoff and token-bucket limiting
// PLC Client - HTTP client for interacting with PLC directory APIs
use crate::constants;
use crate::resolver::DIDDocument;
use anyhow::{Context, Result};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

// Import PLCOperation from sync module (needed for fetch_operations)
use crate::sync::PLCOperation;

/// HTTP client for PLC directory with rate limiting and retry logic
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
        let period = Duration::from_secs(constants::PLC_RATE_LIMIT_PERIOD);
        let requests_per_period = (constants::PLC_RATE_LIMIT_REQUEST as f64
            * constants::PLC_RATE_LIMIT_SAFETY_FACTOR)
            .floor() as usize;
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(constants::HTTP_TIMEOUT_SECS))
                .build()?,
            base_url: base_url.into(),
            rate_limiter: RateLimiter::new(requests_per_period, period),
            last_retry_after: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            request_timestamps: Arc::new(std::sync::Mutex::new(VecDeque::new())),
            rate_limit_period: period,
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

    /// Fetch operations from PLC directory export endpoint
    pub async fn fetch_operations(
        &self,
        after: &str,
        count: usize,
    ) -> Result<(Vec<PLCOperation>, Duration, Duration)> {
        self.fetch_operations_with_retry_cancelable(after, count, 5, None).await
    }

    pub async fn fetch_operations_cancelable(
        &self,
        after: &str,
        count: usize,
        shutdown_rx: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<(Vec<PLCOperation>, Duration, Duration)> {
        self.fetch_operations_with_retry_cancelable(after, count, 5, shutdown_rx).await
    }

    async fn fetch_operations_with_retry_cancelable(
        &self,
        after: &str,
        count: usize,
        max_retries: usize,
        shutdown_rx: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<(Vec<PLCOperation>, Duration, Duration)> {
        let mut backoff = Duration::from_secs(1);
        let mut last_err = None;
        let mut total_wait = Duration::from_secs(0);
        let mut total_http = Duration::from_secs(0);

        for attempt in 1..=max_retries {
            if let Some(ref rx) = shutdown_rx && *rx.borrow() {
                anyhow::bail!("Shutdown requested");
            }
            let export_url = format!(
                "{}/export?after={}&count={}",
                self.base_url, after, count
            );

            let permits = self.rate_limiter.available_permits();
            let requests_in_period = self.count_requests_in_period();
            log::debug!(
                "[PLCClient] Preparing /export request: {} | permits={} | window={:?} | requests_in_window={}",
                export_url,
                permits,
                self.rate_limit_period,
                requests_in_period
            );

            let wait_start = Instant::now();
            if let Some(mut rx) = shutdown_rx.clone() {
                tokio::select! {
                    _ = self.rate_limiter.wait() => {}
                    _ = rx.changed() => {
                        if *rx.borrow() { anyhow::bail!("Shutdown requested"); }
                    }
                }
            } else {
                self.rate_limiter.wait().await;
            }
            let wait_elapsed = wait_start.elapsed();
            if wait_elapsed.as_nanos() > 0 {
                log::debug!("[PLCClient] Rate limiter wait: {:?}", wait_elapsed);
            }
            total_wait += wait_elapsed;

            // Clear previous retry_after
            *self.last_retry_after.lock().await = None;

            // Record this request attempt
            self.record_request();

            match self.do_fetch_operations(after, count).await {
                Ok((operations, http_duration)) => {
                    total_http += http_duration;
                    return Ok((operations, total_wait, total_http));
                }
                Err(e) => {
                    last_err = Some(e);

                    // Check if it's a rate limit error (429)
                    let retry_after = self.last_retry_after.lock().await.take();
                    if let Some(retry_after) = retry_after {
                        let requests_in_period = self.count_requests_in_period();
                        let rate_limit = (constants::PLC_RATE_LIMIT_REQUEST as f64
                            * constants::PLC_RATE_LIMIT_SAFETY_FACTOR)
                            .floor() as usize;
                        eprintln!(
                            "[Sync] Rate limited by PLC directory ({} requests in last {:?}, limit: {}), waiting {:?} before retry {}/{}",
                            requests_in_period,
                            self.rate_limit_period,
                            rate_limit,
                            retry_after,
                            attempt,
                            max_retries
                        );
                        if let Some(mut rx) = shutdown_rx.clone() {
                            tokio::select! {
                                _ = tokio::time::sleep(retry_after) => {}
                                _ = rx.changed() => {
                                    if *rx.borrow() { anyhow::bail!("Shutdown requested"); }
                                }
                            }
                        } else {
                            tokio::time::sleep(retry_after).await;
                        }
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
                        if let Some(mut rx) = shutdown_rx.clone() {
                            tokio::select! {
                                _ = tokio::time::sleep(backoff) => {}
                                _ = rx.changed() => {
                                    if *rx.borrow() { anyhow::bail!("Shutdown requested"); }
                                }
                            }
                        } else {
                            tokio::time::sleep(backoff).await;
                        }
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

    async fn do_fetch_operations(
        &self,
        after: &str,
        count: usize,
    ) -> Result<(Vec<PLCOperation>, Duration)> {
        let url = format!("{}/export", self.base_url);
        let request_start = Instant::now();
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
        let request_duration = request_start.elapsed();
        let mut operations = Vec::new();

        for line in body.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match sonic_rs::from_str::<PLCOperation>(line) {
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

        Ok((operations, request_duration))
    }

    /// Fetch DID document raw JSON from PLC directory
    ///
    /// Fetches the raw JSON string for a DID document from the PLC directory.
    /// This preserves the exact byte content as received from the source.
    /// Uses the /{did} endpoint.
    pub async fn fetch_did_document_raw(&self, did: &str) -> Result<String> {
        use std::time::Instant;

        // Construct DID document URL
        // PLC directory exposes DID documents at /{did} (same as plcbundle instances)
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), did);

        log::debug!("Fetching DID document from: {}", url);
        let request_start = Instant::now();

        let response = self
            .client
            .get(&url)
            .header("User-Agent", constants::user_agent())
            .send()
            .await
            .context(format!("Failed to fetch DID document from {}", url))?;

        let request_duration = request_start.elapsed();
        log::debug!(
            "HTTP request completed in {:?}, status: {}",
            request_duration,
            response.status()
        );

        // Handle rate limiting (429)
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = parse_retry_after(&response);
            *self.last_retry_after.lock().await = Some(retry_after);
            log::warn!("Rate limited (429), retry after: {:?}", retry_after);
            anyhow::bail!("Rate limited (429)");
        }

        if !response.status().is_success() {
            log::error!(
                "Unexpected status code: {} for DID document at {}",
                response.status(),
                url
            );
            anyhow::bail!(
                "Unexpected status code: {} for DID document at {}",
                response.status(),
                url
            );
        }

        let data = response.text().await?;
        let data_size = data.len();
        log::debug!("Received response body: {} bytes", data_size);

        Ok(data)
    }

    /// Fetch DID document from PLC directory
    ///
    /// Fetches the W3C DID document for a given DID from the PLC directory.
    /// Uses the /did/{did} endpoint.
    pub async fn fetch_did_document(&self, did: &str) -> Result<DIDDocument> {
        let data = self.fetch_did_document_raw(did).await?;
        let document: DIDDocument =
            sonic_rs::from_str(&data).context("Failed to parse DID document JSON")?;
        Ok(document)
    }
}

/// Parse the Retry-After header from a response
/// Returns the duration to wait before retrying, capped at 60 seconds maximum
fn parse_retry_after(response: &reqwest::Response) -> Duration {
    const MAX_RETRY_SECONDS: u64 = 60;

    if let Some(retry_after_header) = response.headers().get("retry-after")
        && let Ok(retry_after_str) = retry_after_header.to_str()
    {
        // Try parsing as seconds (integer) - most common format
        if let Ok(seconds) = retry_after_str.parse::<u64>() {
            // Cap at maximum wait time
            return Duration::from_secs(seconds.min(MAX_RETRY_SECONDS));
        }

        // Try parsing as HTTP date (RFC 7231)
        // httpdate::parse_http_date returns a SystemTime
        if let Ok(http_time) = httpdate::parse_http_date(retry_after_str)
            && let Ok(duration) = http_time.duration_since(std::time::SystemTime::now())
        {
            // Cap at maximum wait time
            return duration.min(Duration::from_secs(MAX_RETRY_SECONDS));
        }
    }

    // Default to 60 seconds if no header or parsing fails
    Duration::from_secs(MAX_RETRY_SECONDS)
}

/// Simple token bucket rate limiter
/// Prevents burst requests by starting with 0 permits and refilling at steady rate
struct RateLimiter {
    semaphore: std::sync::Arc<tokio::sync::Semaphore>,
}

impl RateLimiter {
    fn new(requests_per_period: usize, period: Duration) -> Self {
        // Use a proper token bucket rate limiter
        // Start with 0 permits to prevent initial burst
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let sem_clone = semaphore.clone();

        let refill_rate = Duration::from_secs_f64(period.as_secs_f64() / requests_per_period as f64);

        // Spawn background task to refill permits at steady rate
        // CRITICAL: Add first permit immediately, then refill at steady rate
        let refill_rate_clone = refill_rate;
        let capacity = requests_per_period;
        tokio::spawn(async move {
            // Add first permit immediately so first request can proceed
            if sem_clone.available_permits() < capacity {
                sem_clone.add_permits(1);
            }

            // Then refill at steady rate
            loop {
                tokio::time::sleep(refill_rate_clone).await;
                // Add one permit if under capacity (burst allowed up to capacity)
                if sem_clone.available_permits() < capacity {
                    sem_clone.add_permits(1);
                }
            }
        });

        Self { semaphore }
    }

    async fn wait(&self) {
        match self.semaphore.acquire().await {
            Ok(permit) => permit.forget(),
            Err(_) => {
                log::warn!(
                    "[PLCClient] Rate limiter disabled (semaphore closed), proceeding without delay"
                );
            }
        }
    }

    fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_plc_client_new() {
        let client = PLCClient::new("https://plc.directory").unwrap();
        // Verify client was created successfully
        assert!(client.base_url.contains("plc.directory"));
    }

    #[tokio::test]
    async fn test_plc_client_new_with_trailing_slash() {
        let client = PLCClient::new("https://plc.directory/").unwrap();
        // URL should be stored as-is (no normalization in PLCClient)
        assert!(client.base_url.contains("plc.directory"));
    }
}
