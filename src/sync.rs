// Sync module - PLC directory synchronization
use crate::constants;
use crate::operations::Operation;
use anyhow::Result;
use serde::Deserialize;
use std::collections::HashSet;
use std::time::Duration;

// ============================================================================
// PLC Client
// ============================================================================

pub struct PLCClient {
    client: reqwest::Client,
    base_url: String,
    rate_limiter: RateLimiter,
    last_retry_after: std::sync::Arc<tokio::sync::Mutex<Option<Duration>>>,
}

impl PLCClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(constants::HTTP_TIMEOUT_SECS))
                .build()?,
            base_url: base_url.into(),
            rate_limiter: RateLimiter::new(constants::DEFAULT_RATE_LIMIT, Duration::from_secs(constants::HTTP_TIMEOUT_SECS)),
            last_retry_after: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    pub async fn fetch_operations(
        &self,
        after: &str,
        count: usize,
    ) -> Result<Vec<PLCOperation>> {
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

            match self.do_fetch_operations(after, count).await {
                Ok(operations) => return Ok(operations),
                Err(e) => {
                    last_err = Some(e);

                    // Check if it's a rate limit error (429)
                    let retry_after = self.last_retry_after.lock().await.take();
                    if let Some(retry_after) = retry_after {
                        eprintln!(
                            "[Sync] Rate limited by PLC directory, waiting {:?} before retry {}/{}",
                            retry_after, attempt, max_retries
                        );
                        tokio::time::sleep(retry_after).await;
                        continue;
                    }

                    // Other errors - exponential backoff
                    if attempt < max_retries {
                        eprintln!(
                            "[Sync] Request failed (attempt {}/{}): {}, retrying in {:?}",
                            attempt, max_retries, last_err.as_ref().unwrap(), backoff
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

    async fn do_fetch_operations(
        &self,
        after: &str,
        count: usize,
    ) -> Result<Vec<PLCOperation>> {
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
                    // Store raw JSON to preserve field order
                    op.raw_json = Some(line.to_string());
                    operations.push(op);
                },
                Err(e) => eprintln!("Warning: failed to parse operation: {}", e),
            }
        }

        Ok(operations)
    }
}

/// Parse the Retry-After header from a response
/// Returns the duration to wait before retrying, defaulting to 5 minutes if parsing fails
fn parse_retry_after(response: &reqwest::Response) -> Duration {
    if let Some(retry_after_header) = response.headers().get("retry-after") {
        if let Ok(retry_after_str) = retry_after_header.to_str() {
            // Try parsing as seconds (integer) - most common format
            if let Ok(seconds) = retry_after_str.parse::<u64>() {
                return Duration::from_secs(seconds);
            }

            // Try parsing as HTTP date (RFC 7231)
            // httpdate::parse_http_date returns a SystemTime
            if let Ok(http_time) = httpdate::parse_http_date(retry_after_str) {
                if let Ok(duration) = http_time.duration_since(std::time::SystemTime::now()) {
                    return duration;
                }
            }
        }
    }

    // Default to 5 minutes if no header or parsing fails
    Duration::from_secs(300)
}

// Simple token bucket rate limiter
struct RateLimiter {
    semaphore: std::sync::Arc<tokio::sync::Semaphore>,
}

impl RateLimiter {
    fn new(requests_per_period: usize, period: Duration) -> Self {
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(requests_per_period));
        let sem_clone = semaphore.clone();
        let refill_rate = period / requests_per_period as u32;

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(refill_rate).await;
                if sem_clone.available_permits() < requests_per_period {
                    sem_clone.add_permits(1);
                }
            }
        });

        Self { semaphore }
    }

    async fn wait(&self) {
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
// Sync Configuration
// ============================================================================

#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub plc_url: String,
    pub continuous: bool,
    pub interval: Duration,
    pub max_bundles: usize,
    pub verbose: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            plc_url: constants::DEFAULT_PLC_DIRECTORY_URL.to_string(),
            continuous: false,
            interval: Duration::from_secs(60),
            max_bundles: 0,
            verbose: false,
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
// Helper Functions
// ============================================================================

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
