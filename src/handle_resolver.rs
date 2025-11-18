//! AT Protocol handle resolver client using XRPC; includes validation and normalization helpers
// Handle resolver - resolves AT Protocol handles to DIDs via XRPC

use crate::constants;
use anyhow::Result;
use regex::Regex;
use serde::Deserialize;
use std::time::Duration;

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
            .header("User-Agent", constants::user_agent())
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

    /// Ping the resolver to keep the connection alive
    ///
    /// This performs a lightweight health check on the resolver service.
    /// It resolves a well-known handle (bsky.app) to keep HTTP/2 connections alive.
    pub async fn ping(&self) -> Result<()> {
        // Use a well-known handle that should always resolve
        let test_handle = "bsky.app";

        // Build XRPC URL
        let endpoint = format!("{}/xrpc/com.atproto.identity.resolveHandle", self.base_url);
        let url = reqwest::Url::parse_with_params(&endpoint, &[("handle", test_handle)])?;

        log::trace!("[HandleResolver] Sending ping to {}", url);

        // Execute request with shorter timeout for pings
        let response = self
            .client
            .get(url)
            .header("User-Agent", constants::user_agent())
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(|e| {
                log::trace!("[HandleResolver] Ping request failed: {}", e);
                e
            })?;

        let status = response.status();
        log::trace!("[HandleResolver] Ping response status: {}", status);

        if !status.is_success() {
            anyhow::bail!("Resolver ping failed with status {}", status);
        }

        // Consume the response to complete the request
        let bytes = response.bytes().await?;
        log::trace!("[HandleResolver] Ping response: {} bytes", bytes.len());

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_handle_format_valid() {
        // Valid handles
        assert!(validate_handle_format("user.bsky.social").is_ok());
        assert!(validate_handle_format("example.com").is_ok());
        assert!(validate_handle_format("test.example.org").is_ok());
        assert!(validate_handle_format("a.b").is_ok());
        assert!(validate_handle_format("sub.domain.example.com").is_ok());
    }

    #[test]
    fn test_validate_handle_format_empty() {
        let result = validate_handle_format("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_validate_handle_format_did() {
        let result = validate_handle_format("did:plc:test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already a DID"));
    }

    #[test]
    fn test_validate_handle_format_no_dot() {
        let result = validate_handle_format("nodomain");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be a domain"));
    }

    #[test]
    fn test_validate_handle_format_too_long() {
        let long_handle = "a".repeat(254) + ".com";
        let result = validate_handle_format(&long_handle);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too long"));
    }

    #[test]
    fn test_validate_handle_format_invalid_chars() {
        // Invalid characters
        assert!(validate_handle_format("user@bsky.social").is_err());
        assert!(validate_handle_format("user bsky.social").is_err());
        assert!(validate_handle_format("user_bsky.social").is_err());
    }

    #[test]
    fn test_is_handle() {
        assert!(is_handle("user.bsky.social"));
        assert!(is_handle("example.com"));
        assert!(!is_handle("did:plc:test"));
        assert!(!is_handle("did:web:example.com"));
        assert!(!is_handle("did:key:z6Mk"));
    }

    #[test]
    fn test_normalize_handle() {
        assert_eq!(normalize_handle("user.bsky.social"), "user.bsky.social");
        assert_eq!(
            normalize_handle("at://user.bsky.social"),
            "user.bsky.social"
        );
        assert_eq!(normalize_handle("@user.bsky.social"), "user.bsky.social");
        // Note: trim_start_matches removes all matches, so "at://@user" becomes "user" (both prefixes removed)
        assert_eq!(
            normalize_handle("at://@user.bsky.social"),
            "user.bsky.social"
        );
        assert_eq!(normalize_handle("example.com"), "example.com");
    }

    #[test]
    fn test_handle_resolver_new() {
        let resolver = HandleResolver::new("https://example.com");
        assert_eq!(resolver.get_base_url(), "https://example.com");
    }

    #[test]
    fn test_handle_resolver_new_trim_slash() {
        let resolver = HandleResolver::new("https://example.com/");
        assert_eq!(resolver.get_base_url(), "https://example.com");
    }
}
