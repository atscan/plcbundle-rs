// Shared utility functions for CLI commands

use anyhow::Result;
use plcbundle::BundleManager;
use std::path::{Path, PathBuf};

/// ANSI color codes for terminal output
/// These are shared across all CLI commands for consistent coloring
pub mod colors {
    /// Standard green color (used for success, matches, etc.)
    pub const GREEN: &str = "\x1b[32m";
    
    /// Standard red color (used for errors, deletions, etc.)
    pub const RED: &str = "\x1b[31m";
    
    /// Reset color code
    pub const RESET: &str = "\x1b[0m";
    
    /// Dim/bright black color (used for context, unchanged lines, etc.)
    pub const DIM: &str = "\x1b[2m";
}

#[cfg(feature = "cli")]
mod colorize {
    use colored_json::prelude::*;

    /// Colorize pretty-printed JSON string with syntax highlighting
    ///
    /// Uses the colored_json crate which provides jq-compatible colorization.
    /// Automatically detects if output is a terminal and applies colors accordingly.
    pub fn colorize_json(json: &str) -> String {
        // Use to_colored_json_auto which automatically detects terminal and applies colors
        // This matches jq's behavior of only coloring when output is to a terminal
        json.to_colored_json_auto()
            .unwrap_or_else(|_| json.to_string())
    }
}

#[cfg(feature = "cli")]
pub use colorize::colorize_json;

/// Check if stdout is connected to an interactive terminal (TTY)
///
/// Returns true if stdout is a TTY, false otherwise.
/// This is useful for automatically enabling pretty printing and colors
/// when outputting to a terminal, while using raw output when piping.
#[cfg(feature = "cli")]
pub fn is_stdout_tty() -> bool {
    use is_terminal::IsTerminal;
    std::io::stdout().is_terminal()
}

pub use plcbundle::format::{format_bytes, format_bytes_per_sec, format_number};

/// Trait for extracting global flags from command objects
/// Commands that have verbose/quiet fields should implement this trait
pub trait HasGlobalFlags {
    fn verbose(&self) -> bool;
    fn quiet(&self) -> bool;
}

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
pub fn get_worker_threads(workers: usize, fallback: usize) -> usize {
    if workers == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(fallback)
    } else {
        workers
    }
}

/// Check if repository is empty (no bundles)
pub fn is_repository_empty(manager: &BundleManager) -> bool {
    manager.get_last_bundle() == 0
}

/// Create BundleManager with verbose/quiet flags
///
/// This is the standard way to create a BundleManager from CLI commands.
/// It respects the verbose and quiet flags for logging.
pub fn create_manager(dir: PathBuf, verbose: bool, _quiet: bool) -> Result<BundleManager> {
    use anyhow::Context;
    
    // Check if directory exists
    if !dir.exists() {
        anyhow::bail!(
            "Directory does not exist: {}\n\nHint: Make sure you're in a PLC bundle directory, or start a new repository with:\n  {} init        # Initialize empty repository\n  {} clone <url> # Clone from remote",
            display_path(&dir).display(),
            plcbundle::constants::BINARY_NAME,
            plcbundle::constants::BINARY_NAME
        );
    }
    
    // Check if it's a bundle directory (has plc_bundles.json)
    let index_path = dir.join("plc_bundles.json");
    if !index_path.exists() {
        anyhow::bail!(
            "Not a PLC bundle directory: {}\n\nThis directory does not contain 'plc_bundles.json'.\n\nHint: Make sure you're in a PLC bundle directory, or start a new repository with:\n  {} init        # Initialize empty repository\n  {} clone <url> # Clone from remote",
            display_path(&dir).display(),
            plcbundle::constants::BINARY_NAME,
            plcbundle::constants::BINARY_NAME
        );
    }
    
    let display_dir = display_path(&dir);
    let manager = BundleManager::new(dir)
        .with_context(|| format!("Failed to load bundle repository from: {}", display_dir.display()))?;

    if verbose {
        Ok(manager.with_verbose(true))
    } else {
        Ok(manager)
    }
}

/// Create BundleManager with global flags extracted from command
///
/// Convenience function for commands that implement `HasGlobalFlags`.
/// The global flags (verbose, quiet) are automatically extracted from the command.
pub fn create_manager_from_cmd<C: HasGlobalFlags>(dir: PathBuf, cmd: &C) -> Result<BundleManager> {
    create_manager(dir, cmd.verbose(), cmd.quiet())
}

/// Get all bundle metadata from the repository
/// This is more efficient than iterating through bundle numbers
pub fn get_all_bundle_metadata(manager: &BundleManager) -> Vec<plcbundle::index::BundleMetadata> {
    manager.get_index().bundles
}

/// Check available free disk space for a given path
///
/// Returns the available free space in bytes, or None if the check fails.
/// On Unix systems (macOS, Linux), uses statvfs to get filesystem statistics.
/// On other platforms, returns None (check is skipped).
#[cfg(unix)]
pub fn get_free_disk_space(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    
    let c_path = match CString::new(path.as_os_str().as_bytes()) {
        Ok(p) => p,
        Err(_) => return None,
    };
    
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
            // Calculate free space: available blocks * block size
            let free_bytes = stat.f_bavail as u64 * stat.f_frsize as u64;
            Some(free_bytes)
        } else {
            None
        }
    }
}

/// Check available free disk space for a given path
///
/// On non-Unix platforms, this function always returns None (check is skipped).
#[cfg(not(unix))]
pub fn get_free_disk_space(_path: &Path) -> Option<u64> {
    None
}
