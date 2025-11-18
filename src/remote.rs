//! Remote client for plcbundle instances: fetch index, bundles, operations, and DID documents; includes local index loader and URL normalization
// This module is for fetching from other plcbundle repositories, not from PLC directory
use crate::constants;
use crate::index::Index;
use crate::operations::Operation;
use crate::resolver::DIDDocument;
use crate::sync::PLCOperation;
use anyhow::{Context, Result};
use std::time::Duration;

/// HTTP client for remote plcbundle instances
pub struct RemoteClient {
    client: reqwest::Client,
    base_url: String,
}

impl RemoteClient {
    /// Create a new RemoteClient for a remote plcbundle instance
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(120))
                .build()?,
            base_url: normalize_base_url(base_url.into()),
        })
    }

    /// Fetch index from remote plcbundle instance
    pub async fn fetch_index(&self) -> Result<Index> {
        // Try both common index file names
        let mut url = format!("{}/index.json", self.base_url);

        let response = self
            .client
            .get(&url)
            .header("User-Agent", constants::user_agent())
            .send()
            .await
            .context("Failed to fetch index")?;

        if !response.status().is_success() {
            // Try alternative name
            url = format!("{}/plc_bundles.json", self.base_url);
            let response2 = self
                .client
                .get(&url)
                .header("User-Agent", constants::user_agent())
                .send()
                .await
                .context("Failed to fetch index")?;

            if !response2.status().is_success() {
                anyhow::bail!("Unexpected status code: {}", response2.status());
            }

            let data = response2.text().await?;
            let index: Index = sonic_rs::from_str(&data).context("Failed to parse index JSON")?;
            return Ok(index);
        }

        let data = response.text().await?;
        let index: Index = sonic_rs::from_str(&data).context("Failed to parse index JSON")?;

        Ok(index)
    }

    /// Fetch bundle operations from remote plcbundle instance
    pub async fn fetch_bundle_operations(&self, bundle_num: u32) -> Result<Vec<Operation>> {
        let bundle_url = format!("{}/jsonl/{}", self.base_url, bundle_num);

        let response = self
            .client
            .get(&bundle_url)
            .header("User-Agent", constants::user_agent())
            .send()
            .await
            .context(format!(
                "Failed to fetch bundle {} from {}",
                bundle_num, bundle_url
            ))?;

        if !response.status().is_success() {
            anyhow::bail!(
                "Unexpected status code: {} for bundle {}",
                response.status(),
                bundle_num
            );
        }

        let body = response.text().await?;
        let mut operations = Vec::new();

        for line in body.lines() {
            if line.trim().is_empty() {
                continue;
            }

            match sonic_rs::from_str::<PLCOperation>(line) {
                Ok(plc_op) => {
                    let op: Operation = plc_op.into();
                    operations.push(op);
                }
                Err(e) => {
                    eprintln!("Warning: failed to parse operation: {}", e);
                }
            }
        }

        Ok(operations)
    }

    /// Fetch a single operation from remote plcbundle instance
    pub async fn fetch_operation(&self, bundle_num: u32, position: usize) -> Result<String> {
        // For now, fetch the whole bundle and return the specific operation
        // This could be optimized if the remote API supports direct operation access
        let operations = self.fetch_bundle_operations(bundle_num).await?;

        if position >= operations.len() {
            anyhow::bail!(
                "Position {} out of bounds (bundle has {} operations)",
                position,
                operations.len()
            );
        }

        // Return raw JSON if available, otherwise serialize
        if let Some(ref raw) = operations[position].raw_json {
            Ok(raw.clone())
        } else {
            sonic_rs::to_string(&operations[position]).context("Failed to serialize operation")
        }
    }

    /// Fetch DID document from remote plcbundle instance
    ///
    /// Fetches the W3C DID document for a given DID from a remote plcbundle repository.
    /// Remote plcbundle instances expose DID documents at /{did} endpoint (same as PLC directory).
    pub async fn fetch_did_document(&self, did: &str) -> Result<DIDDocument> {
        let did_url = format!("{}/{}", self.base_url, did);

        let response = self
            .client
            .get(&did_url)
            .header("User-Agent", constants::user_agent())
            .send()
            .await
            .context(format!("Failed to fetch DID document from {}", did_url))?;

        if !response.status().is_success() {
            anyhow::bail!(
                "Unexpected status code: {} for DID document at {}",
                response.status(),
                did_url
            );
        }

        let data = response.text().await?;
        let document: DIDDocument =
            sonic_rs::from_str(&data).context("Failed to parse DID document JSON")?;

        Ok(document)
    }

    /// Download raw bundle file (zstd compressed) from remote plcbundle instance
    ///
    /// Downloads the raw compressed bundle file from the /data/{number} endpoint.
    /// Returns the raw bytes of the .jsonl.zst file.
    pub async fn download_bundle_file(&self, bundle_num: u32) -> Result<Vec<u8>> {
        let bundle_url = format!("{}/data/{}", self.base_url, bundle_num);

        let response = self
            .client
            .get(&bundle_url)
            .header("User-Agent", constants::user_agent())
            .send()
            .await
            .context(format!(
                "Failed to download bundle {} from {}",
                bundle_num, bundle_url
            ))?;

        if !response.status().is_success() {
            anyhow::bail!(
                "Unexpected status code: {} for bundle {} at {}",
                response.status(),
                bundle_num,
                bundle_url
            );
        }

        let bytes = response
            .bytes()
            .await
            .context(format!("Failed to read bundle {} bytes", bundle_num))?;

        Ok(bytes.to_vec())
    }
}

/// Normalize base URL by removing trailing slashes and index file names
fn normalize_base_url(mut url: String) -> String {
    url = url.trim_end_matches('/').to_string();
    // Remove index.json or plc_bundles.json if present
    url = url.trim_end_matches("/index.json").to_string();
    url = url.trim_end_matches("/plc_bundles.json").to_string();
    url
}

/// Load index from local file or directory path
pub fn load_local_index(target: &str) -> Result<Index> {
    use std::path::Path;
    let path = Path::new(target);
    if path.is_file() {
        // Direct file path
        let file = std::fs::File::open(path)?;
        Ok(sonic_rs::from_reader(file)?)
    } else {
        // Directory path - look for index file
        Index::load(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_base_url() {
        // Test removing trailing slash
        assert_eq!(
            normalize_base_url("https://example.com/".to_string()),
            "https://example.com"
        );

        // Test removing index.json
        assert_eq!(
            normalize_base_url("https://example.com/index.json".to_string()),
            "https://example.com"
        );

        // Test removing plc_bundles.json
        assert_eq!(
            normalize_base_url("https://example.com/plc_bundles.json".to_string()),
            "https://example.com"
        );

        // Test removing both trailing slash and index file
        assert_eq!(
            normalize_base_url("https://example.com/index.json/".to_string()),
            "https://example.com"
        );

        // Test already normalized URL
        assert_eq!(
            normalize_base_url("https://example.com".to_string()),
            "https://example.com"
        );

        // Test with path
        assert_eq!(
            normalize_base_url("https://example.com/api/".to_string()),
            "https://example.com/api"
        );
    }

    #[test]
    fn test_remote_client_new() {
        let client = RemoteClient::new("https://example.com").unwrap();
        // Can't easily test the internal state, but we can verify it was created
        assert!(client.base_url.contains("example.com"));
    }

    #[test]
    fn test_remote_client_new_normalizes_url() {
        let client = RemoteClient::new("https://example.com/").unwrap();
        assert!(!client.base_url.ends_with('/'));

        let client2 = RemoteClient::new("https://example.com/index.json").unwrap();
        assert!(!client2.base_url.ends_with("index.json"));
    }
}
