// Shared utility functions for CLI commands

use anyhow::Result;
use plcbundle::BundleManager;
use std::path::{Path, PathBuf};

pub use plcbundle::format::{format_bytes, format_number};

/// Parse bundle specification string into a vector of bundle numbers
pub fn parse_bundle_spec(spec: Option<String>, max_bundle: u32) -> Result<Vec<u32>> {
    match spec {
        None => Ok((1..=max_bundle).collect()),
        Some(s) => {
            if s.starts_with("latest:") {
                let count: u32 = s.strip_prefix("latest:").unwrap().parse()?;
                let start = max_bundle.saturating_sub(count.saturating_sub(1));
                Ok((start..=max_bundle).collect())
            } else {
                plcbundle::parse_bundle_range(&s, max_bundle)
            }
        }
    }
}

/// Parse a simple bundle range string (e.g., "1-100") into (start, end)
pub fn parse_bundle_range_simple(spec: &str, max_bundle: u32) -> Result<(u32, u32)> {
    let (start, end) = if let Some((start, end)) = spec.split_once('-') {
        (start.trim().parse()?, end.trim().parse()?)
    } else {
        let num: u32 = spec.trim().parse()?;
        (num, num)
    };

    if start == 0 || end == 0 || start > end || end > max_bundle {
        anyhow::bail!("Invalid range: {}-{}", start, end);
    }

    Ok((start, end))
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
        match std::thread::available_parallelism() {
            Ok(n) => n.get(),
            Err(e) => {
                eprintln!(
                    "Warning: Failed to detect CPU count: {}, using fallback: {}",
                    e, fallback
                );
                fallback
            }
        }
    } else {
        workers
    }
}

/// Check if repository is empty (no bundles)
pub fn is_repository_empty(manager: &BundleManager) -> bool {
    manager.get_last_bundle() == 0
}

/// Create BundleManager with optional verbose flag
pub fn create_manager(dir: PathBuf, verbose: bool) -> Result<BundleManager> {
    Ok(BundleManager::new(dir)?.with_verbose(verbose))
}

/// Get all bundle metadata from the repository
/// This is more efficient than iterating through bundle numbers
pub fn get_all_bundle_metadata(manager: &BundleManager) -> Vec<plcbundle::index::BundleMetadata> {
    manager.get_index().bundles
}
