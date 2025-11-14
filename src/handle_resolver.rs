// Handle resolver - resolves AT Protocol handles to DIDs via XRPC

use anyhow::Result;
use regex::Regex;
use serde::Deserialize;
use std::time::Duration;

/// Default handle resolver URL
pub const DEFAULT_HANDLE_RESOLVER_URL: &str = "https://quickdid.smokesignal.tools";

/// Client for resolving AT Protocol handles to DIDs via XRPC
pub struct HandleResolver {
    base_url: String,
    client: reqwest::Client,
}

impl HandleResolver {
    /// Create a new handle resolver client
    pub fn new(base_url: impl Into<String>) -> Self {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(90))
            .http2_keep_alive_interval(Some(Duration::from_secs(30)))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .build()
            .expect("Failed to create HTTP client");

        Self { base_url, client }
    }

    /// Get the base URL of the resolver
    pub fn get_base_url(&self) -> &str {
        &self.base_url
    }

    /// Resolve a handle to a DID using com.atproto.identity.resolveHandle
    pub async fn resolve_handle(&self, handle: &str) -> Result<String> {
        // Validate handle format
        validate_handle_format(handle)?;

        // Build XRPC URL
        let endpoint = format!("{}/xrpc/com.atproto.identity.resolveHandle", self.base_url);
        let url = reqwest::Url::parse_with_params(&endpoint, &[("handle", handle)])?;

        // Execute request
        let response = self
            .client
            .get(url)
            .header("User-Agent", "plcbundle-rs/0.1.0")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Resolver returned status {}: {}", status, body);
        }

        // Parse response
        #[derive(Deserialize)]
        struct ResolveResponse {
            did: String,
        }

        let result: ResolveResponse = response.json().await?;

        if result.did.is_empty() {
            anyhow::bail!("Resolver returned empty DID");
        }

        // Validate returned DID
        if !result.did.starts_with("did:plc:") && !result.did.starts_with("did:web:") {
            anyhow::bail!("Invalid DID format returned: {}", result.did);
        }

        Ok(result.did)
    }
}

/// Validate AT Protocol handle format
pub fn validate_handle_format(handle: &str) -> Result<()> {
    if handle.is_empty() {
        anyhow::bail!("Handle cannot be empty");
    }

    // Handle can't be a DID
    if handle.starts_with("did:") {
        anyhow::bail!("Input is already a DID, not a handle");
    }

    // Basic length check
    if handle.len() > 253 {
        anyhow::bail!("Handle too long (max 253 chars)");
    }

    // Must have at least one dot (domain.tld)
    if !handle.contains('.') {
        anyhow::bail!("Handle must be a domain (e.g., user.bsky.social)");
    }

    // Valid handle pattern (simplified - matches AT Protocol spec)
    let valid_pattern = Regex::new(r"^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$")
        .expect("Invalid regex pattern");
    
    if !valid_pattern.is_match(handle) {
        anyhow::bail!("Invalid handle format");
    }

    Ok(())
}

/// Check if a string looks like a handle (not a DID)
pub fn is_handle(input: &str) -> bool {
    !input.starts_with("did:")
}

/// Normalize handle format (removes at:// prefix if present)
pub fn normalize_handle(handle: &str) -> String {
    handle
        .trim_start_matches("at://")
        .trim_start_matches("@")
        .to_string()
}

