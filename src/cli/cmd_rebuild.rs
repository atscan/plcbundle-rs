// Rebuild plc_bundles.json from existing bundle files
use super::progress::ProgressBar;
use super::utils::{format_bytes, HasGlobalFlags};
use anyhow::Result;
use clap::{Args, ValueHint};
use plcbundle::BundleManager;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Args)]
#[command(
    about = "Rebuild bundle index from existing files",
    long_about = "Reconstruct the bundle index (plc_bundles.json) by scanning existing bundle
files and extracting metadata from their embedded metadata frames. This is essential
when the index file has been lost, corrupted, or needs to be regenerated.

The command scans all .jsonl.zst files in the directory, reads the metadata frame
from each bundle (stored in a zstd skippable frame), and reconstructs the complete
index with proper chain hashes and bundle relationships. The index is written
atomically to ensure data integrity.

This only works with bundles that have embedded metadata frames (the modern format).
Legacy bundles without metadata frames cannot be reconstructed this way. Use
--dry-run to preview what would be rebuilt without actually writing the index.

After rebuilding, verify the repository with 'verify' to ensure chain integrity
is correct.",
    help_template = crate::clap_help!(
        examples: "  # Rebuild index from current directory\n  \
                   {bin} rebuild\n\n  \
                   # Rebuild from specific directory\n  \
                   {bin} rebuild -C /path/to/bundles\n\n  \
                   # Show verbose output\n  \
                   {bin} rebuild -v\n\n  \
                   # Dry-run (just scan, don't write)\n  \
                   {bin} rebuild --dry-run"
    )
)]
pub struct RebuildCommand {
    /// Show what would be done without writing index
    #[arg(short = 'n', long)]
    pub dry_run: bool,

    /// Set origin URL (default: auto-detect from first bundle)
    #[arg(long, value_hint = ValueHint::Url)]
    pub origin: Option<String>,
}

impl HasGlobalFlags for RebuildCommand {
    fn verbose(&self) -> bool { false }
    fn quiet(&self) -> bool { false }
}

pub fn run(cmd: RebuildCommand, dir: PathBuf, _global_verbose: bool) -> Result<()> {
    eprintln!("Rebuilding bundle index from: {}\n", super::utils::display_path(&dir).display());

    let start = Instant::now();

    // Progress tracking
    eprintln!("Scanning for bundle files...");

    // We don't know the total upfront, so we'll create progress bar in the callback
    use std::sync::{Arc, Mutex};
    let progress_bar: Arc<Mutex<Option<ProgressBar>>> = Arc::new(Mutex::new(None));
    let progress_bar_clone = progress_bar.clone();

    // Rebuild index using BundleManager API
    let index = BundleManager::rebuild_index(
        &dir,
        cmd.origin,
        Some(move |current, total, bytes_processed, total_bytes| {
            let mut pb_guard = progress_bar_clone.lock().unwrap();
            if pb_guard.is_none() {
                *pb_guard = Some(ProgressBar::with_bytes(total, total_bytes));
            }
            if let Some(ref pb) = *pb_guard {
                pb.set_with_bytes(current, bytes_processed);
            }
        }),
    )?;

    if let Some(pb) = progress_bar.lock().unwrap().take() {
        pb.finish();
    }

    let elapsed = start.elapsed();

    // Display summary
    eprintln!();
    eprintln!("Rebuild Summary");
    eprintln!("═══════════════");
    eprintln!("  Bundles:           {}", index.bundles.len());
    eprintln!("  Range:             {:06} - {:06}",
        index.bundles.first().map(|b| b.bundle_number).unwrap_or(0),
        index.last_bundle
    );
    eprintln!("  Origin:            {}", index.origin);
    eprintln!("  Compressed size:   {}", format_bytes(index.total_size_bytes));
    eprintln!("  Uncompressed size: {}", format_bytes(index.total_uncompressed_size_bytes));
    eprintln!("  Scan time:         {:?}", elapsed);
    eprintln!();

    if cmd.dry_run {
        eprintln!("💡 Dry-run mode - index not written");
        return Ok(());
    }

    // Save index atomically
    eprintln!("Writing plc_bundles.json...");
    index.save(&dir)?;

    eprintln!("✓ Index rebuilt successfully");
    eprintln!("  Location: {}/plc_bundles.json", dir.display());

    Ok(())
}
