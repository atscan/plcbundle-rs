// Shared utility functions for CLI commands

use anyhow::Result;
use std::path::{Path, PathBuf};

/// Parse bundle specification string into a vector of bundle numbers
pub fn parse_bundle_spec(spec: Option<String>, max_bundle: u32) -> Result<Vec<u32>> {
    match spec {
        None => Ok((1..=max_bundle).collect()),
        Some(s) => {
            if s.starts_with("latest:") {
                let count: u32 = s.strip_prefix("latest:").unwrap().parse()?;
                let start = max_bundle.saturating_sub(count - 1);
                Ok((start..=max_bundle).collect())
            } else {
                parse_bundle_range(&s, max_bundle)
            }
        }
    }
}

/// Parse bundle range string (e.g., "1-10,15,20-25")
pub fn parse_bundle_range(spec: &str, _max_bundle: u32) -> Result<Vec<u32>> {
    let mut result = Vec::new();
    
    for part in spec.split(',') {
        let part = part.trim();
        if part.contains('-') {
            let parts: Vec<&str> = part.split('-').collect();
            if parts.len() != 2 {
                anyhow::bail!("Invalid range format: {}", part);
            }
            let start: u32 = parts[0].parse()?;
            let end: u32 = parts[1].parse()?;
            if start > end {
                anyhow::bail!("Invalid range: {} > {}", start, end);
            }
            result.extend(start..=end);
        } else {
            let num: u32 = part.parse()?;
            result.push(num);
        }
    }
    
    Ok(result)
}

/// Parse a simple bundle range string (e.g., "1-100") into (start, end)
pub fn parse_bundle_range_simple(spec: &str) -> Result<(u32, u32)> {
    if spec.contains('-') {
        let parts: Vec<&str> = spec.split('-').collect();
        if parts.len() != 2 {
            anyhow::bail!("Invalid range format: {}", spec);
        }
        let start: u32 = parts[0].trim().parse()?;
        let end: u32 = parts[1].trim().parse()?;
        if start > end {
            anyhow::bail!("Invalid range: {} > {}", start, end);
        }
        Ok((start, end))
    } else {
        let num: u32 = spec.trim().parse()?;
        Ok((num, num))
    }
}

/// Format number with thousand separators
pub fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Format bytes in human-readable format
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        format!("{} {}", bytes, UNITS[0])
    } else {
        format!("{:.2} {}", size, UNITS[unit_idx])
    }
}

/// Display path resolving "." to absolute path
/// Per RULES.md: NEVER display "." in user-facing output
pub fn display_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// Get number of worker threads, auto-detecting if workers == 0
///
/// # Arguments
/// * `workers` - Number of workers requested (0 = auto-detect)
/// * `fallback` - Fallback value if auto-detection fails (default: 4)
///
/// # Returns
/// Number of worker threads to use
pub fn get_num_workers(workers: usize, fallback: usize) -> usize {
    if workers == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(fallback)
    } else {
        workers
    }
}

