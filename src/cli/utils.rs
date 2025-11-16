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
    
    /// Bold text
    pub const BOLD: &str = "\x1b[1m";
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
    let manager = BundleManager::new(dir)?;

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
