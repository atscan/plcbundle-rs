// Utility functions for validation and common operations

use axum::http::{HeaderMap, HeaderValue, Uri};
use crate::constants;

/// Check if a path is a common browser file that should be ignored
pub fn is_common_browser_file(path: &str) -> bool {
    let common_files = [
        "favicon.ico",
        "robots.txt",
        "sitemap.xml",
        "apple-touch-icon.png",
        ".well-known",
    ];
    let common_extensions = [
        ".ico", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".css", ".js", ".woff", ".woff2", ".ttf",
        ".eot", ".xml", ".txt", ".html",
    ];

    for file in &common_files {
        if path == *file || path.starts_with(file) {
            return true;
        }
    }

    for ext in &common_extensions {
        if path.ends_with(ext) {
            return true;
        }
    }

    false
}

/// Validate if input is a valid DID or handle
pub fn is_valid_did_or_handle(input: &str) -> bool {
    if input.is_empty() {
        return false;
    }

    // If it's a DID
    if input.starts_with("did:") {
        // Only accept did:plc: method
        if !input.starts_with("did:plc:") {
            return false;
        }
        return true;
    }

    // Not a DID - validate as handle
    // Must have at least one dot
    if !input.contains('.') {
        return false;
    }

    // Must not have invalid characters
    for c in input.chars() {
        if !(c.is_ascii_lowercase() || c.is_ascii_uppercase() || c.is_ascii_digit() || c == '.' || c == '-') {
            return false;
        }
    }

    // Basic length check
    if input.len() > 253 {
        return false;
    }

    // Must not start or end with dot or hyphen
    if input.starts_with('.')
        || input.ends_with('.')
        || input.starts_with('-')
        || input.ends_with('-')
    {
        return false;
    }

    true
}

/// Parse operation pointer: "bundle:position" or global position
pub fn parse_operation_pointer(pointer: &str) -> anyhow::Result<(u32, usize)> {
    // Check if it's "bundle:position" format
    if let Some(colon_pos) = pointer.find(':') {
        let bundle_str = &pointer[..colon_pos];
        let pos_str = &pointer[colon_pos + 1..];

        let bundle_num: u32 = bundle_str
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid bundle number: {}", bundle_str))?;
        let position: usize = pos_str
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid position: {}", pos_str))?;

        if bundle_num < 1 {
            anyhow::bail!("Bundle number must be >= 1");
        }

        return Ok((bundle_num, position));
    }

    // Parse as global position
    let global_pos: u64 = pointer.parse().map_err(|_| {
        anyhow::anyhow!("Invalid position: must be number or 'bundle:position' format")
    })?;

    if global_pos < constants::BUNDLE_SIZE as u64 {
        // Small numbers are shorthand for bundle 1
        return Ok((1, global_pos as usize));
    }

    // Convert global position to bundle + position
    let (bundle_num, position) = crate::constants::global_to_bundle_position(global_pos);

    Ok((bundle_num, position))
}

/// Extract base URL from request headers and URI
pub fn extract_base_url(headers: &HeaderMap, uri: &Uri) -> String {
    if let Some(host_str) = headers.get("host").and_then(|h| h.to_str().ok()) {
            // Check if request is HTTPS (from X-Forwarded-Proto or X-Forwarded-Ssl)
            let is_https = headers
                .get("x-forwarded-proto")
                .and_then(|v| v.to_str().ok())
                .map(|s| s == "https")
                .unwrap_or(false)
                || headers
                    .get("x-forwarded-ssl")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s == "on")
                    .unwrap_or(false);

            let scheme = if is_https { "https" } else { "http" };
            return format!("{}://{}", scheme, host_str);
    }
    
    if let Some(authority) = uri.authority() {
        format!("http://{}", authority)
    } else {
        "http://127.0.0.1:8080".to_string()
    }
}

/// Create headers for bundle file download
pub fn bundle_download_headers(content_type: &'static str, filename: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(content_type));
    headers.insert(
        "Content-Disposition",
        HeaderValue::from_str(&format!("attachment; filename={}", filename)).unwrap(),
    );
    headers
}

/// Parse duration string (e.g., "60s", "5m", "1h") into Duration
pub fn parse_duration(s: &str) -> anyhow::Result<tokio::time::Duration> {
    use anyhow::Context;
    use tokio::time::Duration;
    
    // Simple parser: "60s", "5m", "1h"
    let s = s.trim();
    if let Some(stripped) = s.strip_suffix('s') {
        let secs: u64 = stripped.parse().context("Invalid duration format")?;
        Ok(Duration::from_secs(secs))
    } else if let Some(stripped) = s.strip_suffix('m') {
        let mins: u64 = stripped.parse().context("Invalid duration format")?;
        Ok(Duration::from_secs(mins * 60))
    } else if let Some(stripped) = s.strip_suffix('h') {
        let hours: u64 = stripped.parse().context("Invalid duration format")?;
        Ok(Duration::from_secs(hours * 3600))
    } else {
        // Try parsing as seconds
        let secs: u64 = s.parse().context("Invalid duration format")?;
        Ok(Duration::from_secs(secs))
    }
}


