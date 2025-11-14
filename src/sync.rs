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
}

impl PLCClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(constants::HTTP_TIMEOUT_SECS))
                .build()?,
            base_url: base_url.into(),
            rate_limiter: RateLimiter::new(constants::DEFAULT_RATE_LIMIT, Duration::from_secs(constants::HTTP_TIMEOUT_SECS)),
        })
    }

    pub async fn fetch_operations(
        &self,
        after: &str,
        count: usize,
    ) -> Result<Vec<PLCOperation>> {
        self.rate_limiter.wait().await;

        let url = format!("{}/export", self.base_url);
        let response = self
            .client
            .get(&url)
            .query(&[("after", after), ("count", &count.to_string())])
            .header("User-Agent", constants::user_agent())
            .send()
            .await?;

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
