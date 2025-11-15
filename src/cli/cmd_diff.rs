// Diff command - compare repositories
use anyhow::{Context, Result, bail};
use clap::Args;
use plcbundle::index::Index;
use plcbundle::{BundleManager, constants, remote};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::runtime::Runtime;

// ANSI color codes for terminal output
const GREEN: &str = "\x1b[38;5;154m"; // Light yellow-green
const RESET: &str = "\x1b[0m";

#[derive(Args)]
#[command(
    about = "Compare repositories",
    long_about = "Compare local repository against remote or local target\n\n\
                  Compares bundle indexes to find differences such as:\n  \
                    • Missing bundles (in target but not local)\n  \
                    • Extra bundles (in local but not target)\n  \
                    • Hash mismatches (different content)\n  \
                    • Content mismatches (different data)\n\n\
                  For deeper analysis of specific bundles, use --bundle flag to see\n\
                  detailed differences in metadata and operations.\n\n\
                  The target can be:\n  \
                    • Remote HTTP URL (e.g., https://plc.example.com)\n  \
                    • Remote index URL (e.g., https://plc.example.com/index.json)\n  \
                    • Local file path (e.g., /path/to/plc_bundles.json)",
    after_help = "Examples:\n  \
        # High-level comparison\n  \
        plcbundle diff https://plc.example.com\n\n  \
        # Show all differences (verbose)\n  \
        plcbundle diff https://plc.example.com -v\n\n  \
        # Deep dive into specific bundle\n  \
        plcbundle diff https://plc.example.com --bundle 23\n\n  \
        # Compare bundle with operation samples\n  \
        plcbundle diff https://plc.example.com --bundle 23 --show-operations\n\n  \
        # Show first 50 operations\n  \
        plcbundle diff https://plc.example.com --bundle 23 --sample 50"
)]
pub struct DiffCommand {
    /// Target to compare against (URL or local path)
    pub target: String,

    /// Show all differences (verbose output)
    #[arg(short, long)]
    pub verbose: bool,

    /// Deep diff of specific bundle
    #[arg(short, long)]
    pub bundle: Option<u32>,

    /// Show operation differences (use with --bundle)
    #[arg(long)]
    pub show_operations: bool,

    /// Number of sample operations to show (use with --bundle)
    #[arg(long, default_value = "10")]
    pub sample: usize,
}

pub fn run(cmd: DiffCommand, dir: PathBuf) -> Result<()> {
    let manager = BundleManager::new(dir.clone())?;

    let rt = Runtime::new()?;

    // If specific bundle requested, do detailed diff
    if let Some(bundle_num) = cmd.bundle {
        rt.block_on(diff_specific_bundle(
            &manager,
            &cmd.target,
            bundle_num,
            cmd.show_operations,
            cmd.sample,
        ))
    } else {
        // Otherwise, do high-level index comparison
        rt.block_on(diff_indexes(
            &manager,
            &dir,
            &cmd.target,
            cmd.verbose,
            constants::BINARY_NAME,
        ))
    }
}

async fn diff_indexes(
    manager: &BundleManager,
    dir: &PathBuf,
    target: &str,
    verbose: bool,
    binary_name: &str,
) -> Result<()> {
    use super::utils::display_path;

    // Resolve "." to actual path (rule: always resolve dot to full path)
    let local_path = display_path(dir);

    // Resolve target "." as well (if it's a local path, not URL)
    let target_display =
        if target == "." && !target.starts_with("http://") && !target.starts_with("https://") {
            display_path(&PathBuf::from(target)).display().to_string()
        } else {
            target.to_string()
        };

    eprintln!("\n🔍 Comparing repositories");
    eprintln!("   Local:  {}", local_path.display());
    eprintln!("   Target: {}\n", target_display);

    // Load local index
    let local_index = manager.get_index();

    // Load target index
    eprintln!("📥 Loading target index...");
    let target_index = remote::fetch_index(target)
        .await
        .context("Failed to load target index")?;

    // Check origins - CRITICAL: must match for valid comparison
    let local_origin = &local_index.origin;
    let target_origin = &target_index.origin;
    let origins_match = local_origin == target_origin;

    eprintln!("\n🌐 Origin Check");
    eprintln!("═══════════════");
    eprintln!("   Local Origin:  {}", local_origin);
    eprintln!("   Target Origin: {}", target_origin);

    if !origins_match {
        eprintln!("\n⚠️  ⚠️  ⚠️  WARNING: DIFFERENT ORIGINS ⚠️  ⚠️  ⚠️");
        eprintln!("═══════════════════════════════════════════════════════════════");
        eprintln!("   The repositories are from DIFFERENT PLC directory sources!");
        eprintln!("   Local:  {}", local_origin);
        eprintln!("   Target: {}", target_origin);
        eprintln!("\n   ⚠️  Comparing bundles from different origins may show");
        eprintln!("      differences that are expected and not errors.");
        eprintln!("   ⚠️  Bundle numbers may overlap but contain different data.");
        eprintln!("   ⚠️  Hash mismatches are expected when origins differ.");
        eprintln!("═══════════════════════════════════════════════════════════════\n");
    } else {
        eprintln!("   ✅ Origins match - comparing same source\n");
    }

    // Perform comparison
    let comparison = compare_indexes(&local_index, &target_index, origins_match);

    // Display results
    display_comparison(&comparison, verbose, origins_match);

    // If there are hash mismatches, suggest deep dive
    if !comparison.hash_mismatches.is_empty() {
        eprintln!("\n💡 Tip: Investigate specific mismatches with:");
        eprintln!(
            "   {} diff {} --bundle {} --show-operations",
            binary_name, target, comparison.hash_mismatches[0].bundle_number
        );
    }

    // Only fail if there are critical hash mismatches AND origins match
    // Missing/extra bundles are not errors, just informational
    // Hash mismatches are expected when origins differ
    if origins_match
        && (!comparison.hash_mismatches.is_empty() || !comparison.content_mismatches.is_empty())
    {
        bail!("indexes have critical differences (hash mismatches)");
    }

    Ok(())
}

async fn diff_specific_bundle(
    manager: &BundleManager,
    target: &str,
    bundle_num: u32,
    show_ops: bool,
    sample_size: usize,
) -> Result<()> {
    // Store bundle_num for use in hints
    eprintln!("\n🔬 Deep Diff: Bundle {:06}", bundle_num);
    eprintln!("═══════════════════════════════════════════════════════════════\n");

    // Get local index and check origin
    let local_index = manager.get_index();
    let local_origin = &local_index.origin;

    // Load remote index to get metadata and origin
    eprintln!("📥 Loading remote index...");
    let remote_index = remote::fetch_index(target)
        .await
        .context("Failed to load remote index")?;
    let target_origin = &remote_index.origin;

    // Check origins - CRITICAL: must match for valid comparison
    let origins_match = local_origin == target_origin;

    eprintln!("\n🌐 Origin Check");
    eprintln!("═══════════════");
    eprintln!("   Local Origin:  {}", local_origin);
    eprintln!("   Target Origin: {}", target_origin);

    if !origins_match {
        eprintln!("\n⚠️  ⚠️  ⚠️  WARNING: DIFFERENT ORIGINS ⚠️  ⚠️  ⚠️");
        eprintln!("═══════════════════════════════════════════════════════════════");
        eprintln!("   The repositories are from DIFFERENT PLC directory sources!");
        eprintln!("   Local:  {}", local_origin);
        eprintln!("   Target: {}", target_origin);
        eprintln!(
            "\n   ⚠️  Bundle {:06} may have the same number but contain",
            bundle_num
        );
        eprintln!("      completely different data from different sources.");
        eprintln!("   ⚠️  All differences shown below are EXPECTED when origins differ.");
        eprintln!("   ⚠️  Do not treat hash/content mismatches as errors.");
        eprintln!("═══════════════════════════════════════════════════════════════\n");
    } else {
        eprintln!("   ✅ Origins match - comparing same source\n");
    }

    // Get local metadata
    let local_meta = manager
        .get_bundle_metadata(bundle_num)?
        .ok_or_else(|| anyhow::anyhow!("Bundle {} not found in local index", bundle_num))?;

    // Load local bundle
    eprintln!("📦 Loading local bundle {:06}...", bundle_num);
    let local_result = manager.load_bundle(bundle_num, plcbundle::LoadOptions::default())?;
    let local_ops = local_result.operations;

    let remote_meta = remote_index
        .get_bundle(bundle_num)
        .ok_or_else(|| anyhow::anyhow!("Bundle {} not found in remote index", bundle_num))?;

    // Load remote bundle
    eprintln!("📦 Loading remote bundle {:06}...\n", bundle_num);
    let remote_ops = if target.starts_with("http://") || target.starts_with("https://") {
        remote::fetch_bundle_operations(target, bundle_num)
            .await
            .context("Failed to load remote bundle")?
    } else {
        // Local path - try to load from directory
        use std::path::Path;
        let target_path = Path::new(target);
        let target_dir = if target_path.is_file() {
            target_path.parent().unwrap_or(Path::new("."))
        } else {
            target_path
        };

        // Try to load bundle from target directory
        let target_manager = BundleManager::new(target_dir.to_path_buf())
            .context("Failed to create manager for target directory")?;
        let target_result = target_manager
            .load_bundle(bundle_num, plcbundle::LoadOptions::default())
            .context("Failed to load bundle from target directory")?;
        target_result.operations
    };

    // Compare metadata
    display_bundle_metadata_comparison_full(
        &local_ops,
        &local_meta,
        remote_meta,
        bundle_num,
        origins_match,
    );

    // Compare operations
    if show_ops {
        eprintln!();
        display_operation_comparison(
            &local_ops,
            &remote_ops,
            sample_size,
            origins_match,
            bundle_num,
            target,
        );
    }

    // Compare hashes in detail
    eprintln!();
    display_hash_analysis_full(&local_meta, remote_meta, bundle_num, origins_match);

    Ok(())
}

// Comparison structures
#[derive(Debug, Default)]
struct IndexComparison {
    local_count: usize,
    target_count: usize,
    common_count: usize,
    missing_bundles: Vec<u32>,
    extra_bundles: Vec<u32>,
    hash_mismatches: Vec<HashMismatch>,
    content_mismatches: Vec<HashMismatch>,
    cursor_mismatches: Vec<HashMismatch>,
    local_range: Option<(u32, u32)>,
    target_range: Option<(u32, u32)>,
    local_total_size: u64,
    target_total_size: u64,
    local_updated: String,
    target_updated: String,
    origins_match: bool,
}

#[derive(Debug)]
struct HashMismatch {
    bundle_number: u32,
    local_hash: String,
    target_hash: String,
    local_content_hash: String,
    target_content_hash: String,
    local_cursor: String,
    target_cursor: String,
}

impl IndexComparison {
    fn has_differences(&self) -> bool {
        !self.missing_bundles.is_empty()
            || !self.extra_bundles.is_empty()
            || !self.hash_mismatches.is_empty()
            || !self.content_mismatches.is_empty()
            || !self.cursor_mismatches.is_empty()
    }
}

fn compare_indexes(local: &Index, target: &Index, origins_match: bool) -> IndexComparison {
    let mut comparison = IndexComparison::default();
    comparison.origins_match = origins_match;

    let local_map: HashMap<u32, &plcbundle::index::BundleMetadata> =
        local.bundles.iter().map(|b| (b.bundle_number, b)).collect();

    let target_map: HashMap<u32, &plcbundle::index::BundleMetadata> = target
        .bundles
        .iter()
        .map(|b| (b.bundle_number, b))
        .collect();

    comparison.local_count = local.bundles.len();
    comparison.target_count = target.bundles.len();
    comparison.local_total_size = local.total_size_bytes;
    comparison.target_total_size = target.total_size_bytes;
    comparison.local_updated = local.updated_at.clone();
    comparison.target_updated = target.updated_at.clone();

    // Get ranges
    if !local.bundles.is_empty() {
        comparison.local_range = Some((
            local.bundles[0].bundle_number,
            local.bundles[local.bundles.len() - 1].bundle_number,
        ));
    }

    if !target.bundles.is_empty() {
        comparison.target_range = Some((
            target.bundles[0].bundle_number,
            target.bundles[target.bundles.len() - 1].bundle_number,
        ));
    }

    // Find missing bundles
    for bundle_num in target_map.keys() {
        if !local_map.contains_key(bundle_num) {
            comparison.missing_bundles.push(*bundle_num);
        }
    }
    comparison.missing_bundles.sort();

    // Find extra bundles
    for bundle_num in local_map.keys() {
        if !target_map.contains_key(bundle_num) {
            comparison.extra_bundles.push(*bundle_num);
        }
    }
    comparison.extra_bundles.sort();

    // Compare hashes
    for (bundle_num, local_meta) in &local_map {
        if let Some(target_meta) = target_map.get(bundle_num) {
            comparison.common_count += 1;

            let chain_mismatch = local_meta.hash != target_meta.hash;
            let content_mismatch = local_meta.content_hash != target_meta.content_hash;

            let cursor_mismatch = local_meta.cursor != target_meta.cursor;

            if chain_mismatch || content_mismatch || cursor_mismatch {
                let mismatch = HashMismatch {
                    bundle_number: *bundle_num,
                    local_hash: local_meta.hash.clone(),
                    target_hash: target_meta.hash.clone(),
                    local_content_hash: local_meta.content_hash.clone(),
                    target_content_hash: target_meta.content_hash.clone(),
                    local_cursor: local_meta.cursor.clone(),
                    target_cursor: target_meta.cursor.clone(),
                };

                if chain_mismatch {
                    comparison.hash_mismatches.push(mismatch);
                } else if content_mismatch {
                    comparison.content_mismatches.push(mismatch);
                } else if cursor_mismatch {
                    // Cursor-only mismatch (hashes match but cursor differs)
                    comparison.cursor_mismatches.push(mismatch);
                }
            }
        }
    }

    // Sort mismatches
    comparison.hash_mismatches.sort_by_key(|m| m.bundle_number);
    comparison
        .content_mismatches
        .sort_by_key(|m| m.bundle_number);
    comparison
        .cursor_mismatches
        .sort_by_key(|m| m.bundle_number);

    comparison
}

fn display_comparison(c: &IndexComparison, verbose: bool, origins_match: bool) {
    eprintln!("\n📊 Comparison Results");
    eprintln!("═══════════════════════\n");

    if !origins_match {
        eprintln!("⚠️  COMPARING DIFFERENT ORIGINS - All differences are expected!\n");
    }

    eprintln!("Summary");
    eprintln!("───────");
    eprintln!("  Local bundles:      {}", c.local_count);
    eprintln!("  Target bundles:     {}", c.target_count);
    eprintln!("  Common bundles:     {}", c.common_count);

    // Missing bundles - informational, not critical
    if !c.missing_bundles.is_empty() {
        eprintln!(
            "  Missing bundles:    ℹ️  {} (in target, not in local)",
            c.missing_bundles.len()
        );
    } else {
        eprintln!(
            "  Missing bundles:    {}{} ✓{}",
            GREEN,
            c.missing_bundles.len(),
            RESET
        );
    }

    // Extra bundles - informational
    if !c.extra_bundles.is_empty() {
        eprintln!(
            "  Extra bundles:      ℹ️  {} (in local, not in target)",
            c.extra_bundles.len()
        );
    } else {
        eprintln!(
            "  Extra bundles:      {}{} ✓{}",
            GREEN,
            c.extra_bundles.len(),
            RESET
        );
    }

    // Hash mismatches - CRITICAL only if origins match
    if !c.hash_mismatches.is_empty() {
        if origins_match {
            eprintln!(
                "  Hash mismatches:    ⚠️  {} (CRITICAL - different content)",
                c.hash_mismatches.len()
            );
        } else {
            eprintln!(
                "  Hash mismatches:    ℹ️  {} (EXPECTED - different origins)",
                c.hash_mismatches.len()
            );
        }
    } else {
        eprintln!(
            "  Hash mismatches:    {}{} ✓{}",
            GREEN,
            c.hash_mismatches.len(),
            RESET
        );
    }

    // Content mismatches - less critical than chain hash
    if !c.content_mismatches.is_empty() {
        if origins_match {
            eprintln!(
                "  Content mismatches: ⚠️  {} (different content hash)",
                c.content_mismatches.len()
            );
        } else {
            eprintln!(
                "  Content mismatches: ℹ️  {} (EXPECTED - different origins)",
                c.content_mismatches.len()
            );
        }
    } else {
        eprintln!(
            "  Content mismatches: {}{} ✓{}",
            GREEN,
            c.content_mismatches.len(),
            RESET
        );
    }

    // Cursor mismatches - metadata issue
    if !c.cursor_mismatches.is_empty() {
        eprintln!(
            "  Cursor mismatches:  ⚠️  {} (cursor should match previous bundle end_time)",
            c.cursor_mismatches.len()
        );
    } else {
        eprintln!(
            "  Cursor mismatches:  {}{} ✓{}",
            GREEN,
            c.cursor_mismatches.len(),
            RESET
        );
    }

    if let Some((start, end)) = c.local_range {
        eprintln!("\n  Local range:        {:06} - {:06}", start, end);
        eprintln!(
            "  Local size:         {:.2} MB",
            c.local_total_size as f64 / (1024.0 * 1024.0)
        );
        eprintln!("  Local updated:      {}", c.local_updated);
    }

    if let Some((start, end)) = c.target_range {
        eprintln!("\n  Target range:       {:06} - {:06}", start, end);
        eprintln!(
            "  Target size:        {:.2} MB",
            c.target_total_size as f64 / (1024.0 * 1024.0)
        );
        eprintln!("  Target updated:     {}", c.target_updated);
    }

    // Show differences
    if !c.hash_mismatches.is_empty() {
        show_hash_mismatches(&c.hash_mismatches, verbose, origins_match);
    }

    if !c.cursor_mismatches.is_empty() {
        show_cursor_mismatches(&c.cursor_mismatches, verbose);
    }

    if !c.missing_bundles.is_empty() {
        show_missing_bundles(&c.missing_bundles, verbose);
    }

    if !c.extra_bundles.is_empty() {
        show_extra_bundles(&c.extra_bundles, verbose);
    }

    // Chain analysis - find where chain broke
    if !c.hash_mismatches.is_empty() && origins_match {
        analyze_chain_break(&c.hash_mismatches, &c.local_range, &c.target_range);
    }

    // Final status
    eprintln!();
    if !c.has_differences() {
        eprintln!("✅ Indexes are identical");
    } else {
        if !origins_match {
            // Different origins - all differences are expected
            eprintln!("ℹ️  Indexes differ (EXPECTED - comparing different origins)");
            eprintln!("   All differences shown above are normal when comparing");
            eprintln!("   repositories from different PLC directory sources.");
        } else {
            // Same origins - differences may be critical
            if !c.hash_mismatches.is_empty() {
                eprintln!("❌ Indexes differ (CRITICAL: hash mismatches detected)");
                eprintln!("\n⚠️  WARNING: Chain hash mismatches indicate different bundle content");
                eprintln!("   or chain integrity issues. This requires investigation.");
            } else {
                // Just missing/extra bundles - not critical
                eprintln!("ℹ️  Indexes differ (missing or extra bundles, but hashes match)");
                eprintln!(
                    "   This is normal when comparing repositories at different sync states."
                );
            }
        }
    }
}

fn show_hash_mismatches(mismatches: &[HashMismatch], verbose: bool, origins_match: bool) {
    if origins_match {
        eprintln!("\n⚠️  CHAIN HASH MISMATCHES (CRITICAL)");
        eprintln!("═══════════════════════════════════════════════════\n");
    } else {
        eprintln!("\nℹ️  CHAIN HASH MISMATCHES (EXPECTED - Different Origins)");
        eprintln!("═══════════════════════════════════════════════════════════════\n");
    }

    let display_count = if mismatches.len() > 10 && !verbose {
        10
    } else {
        mismatches.len()
    };

    for i in 0..display_count {
        let m = &mismatches[i];
        eprintln!("  Bundle {:06}:", m.bundle_number);
        eprintln!("    Chain Hash:");
        eprintln!("      Local:  {}", m.local_hash);
        eprintln!("      Target: {}", m.target_hash);

        if m.local_content_hash != m.target_content_hash {
            eprintln!("    Content Hash (also differs):");
            eprintln!("      Local:  {}", m.local_content_hash);
            eprintln!("      Target: {}", m.target_content_hash);
        }

        if m.local_cursor != m.target_cursor {
            eprintln!("    Cursor (also differs):");
            eprintln!("      Local:  {}", m.local_cursor);
            eprintln!("      Target: {}", m.target_cursor);
        }
        eprintln!();
    }

    if mismatches.len() > display_count {
        eprintln!(
            "  ... and {} more (use -v to show all)\n",
            mismatches.len() - display_count
        );
    }
}

fn analyze_chain_break(
    mismatches: &[HashMismatch],
    _local_range: &Option<(u32, u32)>,
    _target_range: &Option<(u32, u32)>,
) {
    if mismatches.is_empty() {
        return;
    }

    eprintln!("\n🔗 Chain Break Analysis");
    eprintln!("═══════════════════════════════════════════════════\n");

    // Find the first bundle where chain broke
    let first_break = mismatches[0].bundle_number;

    // Determine last good bundle (one before first break)
    let last_good = if first_break > 1 { first_break - 1 } else { 0 };

    eprintln!("  Chain Status:");
    if last_good > 0 {
        eprintln!("    ✅ Bundles 000001 - {:06}: Chain intact", last_good);
        eprintln!(
            "    ❌ Bundle {:06}: Chain broken (first mismatch)",
            first_break
        );
    } else {
        eprintln!("    ❌ Bundle 000001: Chain broken from start");
    }

    // Count consecutive breaks
    let mut consecutive_breaks = 1;
    for i in 1..mismatches.len() {
        if mismatches[i].bundle_number == first_break + consecutive_breaks {
            consecutive_breaks += 1;
        } else {
            break;
        }
    }

    if consecutive_breaks > 1 {
        let last_break = first_break + consecutive_breaks - 1;
        eprintln!(
            "    ❌ Bundles {:06} - {:06}: Chain broken ({} consecutive)",
            first_break, last_break, consecutive_breaks
        );
    }

    // Show total affected
    eprintln!("\n  Summary:");
    eprintln!("    Last good bundle:  {:06}", last_good);
    eprintln!("    First break:      {:06}", first_break);
    eprintln!("    Total mismatches: {}", mismatches.len());

    // Show parent hash comparison for first break
    if let Some(first_mismatch) = mismatches.first() {
        eprintln!("\n  First Break Details (Bundle {:06}):", first_break);
        eprintln!(
            "    Local chain hash:  {}",
            &first_mismatch.local_hash[..16]
        );
        eprintln!(
            "    Target chain hash: {}",
            &first_mismatch.target_hash[..16]
        );

        // Check if parent hashes match (indicates where divergence started)
        eprintln!("\n  💡 Interpretation:");
        if last_good > 0 {
            eprintln!("    • Bundles before {:06} are identical", last_good);
            eprintln!(
                "    • Bundle {:06} has different content or was created differently",
                first_break
            );
            eprintln!("    • All subsequent bundles will have different chain hashes");
            eprintln!("\n    To fix:");
            eprintln!("    1. Check bundle {:06} content differences", first_break);
            eprintln!(
                "    2. Verify operations in bundle {:06} match expected",
                first_break
            );
            eprintln!(
                "    3. Consider re-syncing from bundle {:06} onwards",
                first_break
            );
        } else {
            eprintln!("    • Chain broken from the very first bundle");
            eprintln!("    • This indicates completely different repositories");
        }
    }

    eprintln!();
}

fn show_cursor_mismatches(mismatches: &[HashMismatch], verbose: bool) {
    eprintln!("\n⚠️  CURSOR MISMATCHES");
    eprintln!("═══════════════════════════════════════════════════");
    eprintln!("  Cursor should match previous bundle's end_time per spec.\n");

    let display_count = if mismatches.len() > 10 && !verbose {
        10
    } else {
        mismatches.len()
    };

    for i in 0..display_count {
        let m = &mismatches[i];
        let local = if m.local_cursor.is_empty() {
            "(empty)"
        } else {
            &m.local_cursor
        };
        eprintln!(
            "  {:06}: Local: {} → Target: {}",
            m.bundle_number, local, m.target_cursor
        );
    }

    if mismatches.len() > display_count {
        eprintln!(
            "  ... and {} more (use -v to show all)",
            mismatches.len() - display_count
        );
    }
}

fn show_missing_bundles(bundles: &[u32], verbose: bool) {
    eprintln!("\nℹ️  Missing Bundles (in target but not in local)");
    eprintln!("─────────────────────────────────────────────────────────────");

    if verbose || bundles.len() <= 20 {
        let display_count = if bundles.len() > 20 && !verbose {
            20
        } else {
            bundles.len()
        };

        for i in 0..display_count {
            eprintln!("  {:06}", bundles[i]);
        }

        if bundles.len() > display_count {
            eprintln!(
                "  ... and {} more (use -v to show all)",
                bundles.len() - display_count
            );
        }
    } else {
        display_bundle_ranges(bundles, Some(bundles.len()));
    }
}

fn show_extra_bundles(bundles: &[u32], verbose: bool) {
    eprintln!("\nℹ️  Extra Bundles (in local but not in target)");
    eprintln!("─────────────────────────────────────────────────────────────");

    if verbose || bundles.len() <= 20 {
        let display_count = if bundles.len() > 20 && !verbose {
            20
        } else {
            bundles.len()
        };

        for i in 0..display_count {
            eprintln!("  {:06}", bundles[i]);
        }

        if bundles.len() > display_count {
            eprintln!(
                "  ... and {} more (use -v to show all)",
                bundles.len() - display_count
            );
        }
    } else {
        display_bundle_ranges(bundles, Some(bundles.len()));
    }
}

fn display_bundle_ranges(bundles: &[u32], total_count: Option<usize>) {
    if bundles.is_empty() {
        return;
    }

    let mut range_start = bundles[0];
    let mut range_end = bundles[0];
    let mut ranges = Vec::new();

    for i in 1..bundles.len() {
        if bundles[i] == range_end + 1 {
            range_end = bundles[i];
        } else {
            ranges.push((range_start, range_end));
            range_start = bundles[i];
            range_end = bundles[i];
        }
    }

    ranges.push((range_start, range_end));

    // Display all ranges except the last one
    for i in 0..ranges.len().saturating_sub(1) {
        let (start, end) = ranges[i];
        if start == end {
            eprintln!("  {:06}", start);
        } else {
            eprintln!("  {:06} - {:06}", start, end);
        }
    }

    // Display the last range with optional count
    if let Some((start, end)) = ranges.last() {
        if start == end {
            if let Some(count) = total_count {
                eprintln!(
                    "  {:06} ({} bundle{})",
                    start,
                    count,
                    if count == 1 { "" } else { "s" }
                );
            } else {
                eprintln!("  {:06}", start);
            }
        } else {
            if let Some(count) = total_count {
                eprintln!(
                    "  {:06} - {:06} ({} bundle{})",
                    start,
                    end,
                    count,
                    if count == 1 { "" } else { "s" }
                );
            } else {
                eprintln!("  {:06} - {:06}", start, end);
            }
        }
    }
}

fn display_bundle_metadata_comparison_full(
    local_ops: &[plcbundle::Operation],
    local_meta: &plcbundle::index::BundleMetadata,
    remote_meta: &plcbundle::index::BundleMetadata,
    _bundle_num: u32,
    origins_match: bool,
) {
    if !origins_match {
        eprintln!(
            "⚠️  NOTE: Comparing bundles from different origins - differences are expected\n"
        );
    }
    eprintln!("📋 Metadata Comparison");
    eprintln!("───────────────────────\n");

    eprintln!("  Bundle Number:      {:06}", remote_meta.bundle_number);

    // Compare operation counts
    let op_count_match = local_ops.len() == remote_meta.operation_count as usize;
    eprintln!(
        "  Operation Count:    {}  {}",
        if op_count_match {
            format!("{}", local_ops.len())
        } else {
            format!(
                "local={}, remote={}",
                local_ops.len(),
                remote_meta.operation_count
            )
        },
        if op_count_match { "✅" } else { "❌" }
    );
    eprintln!("    Local:  {}", local_ops.len());
    eprintln!("    Remote: {}", remote_meta.operation_count);

    // Compare DID counts
    let did_count_match = local_meta.did_count == remote_meta.did_count;
    eprintln!(
        "  DID Count:          {}  {}",
        if did_count_match {
            format!("{}", local_meta.did_count)
        } else {
            format!(
                "local={}, remote={}",
                local_meta.did_count, remote_meta.did_count
            )
        },
        if did_count_match { "✅" } else { "❌" }
    );

    // Compare sizes
    let size_match = local_meta.compressed_size == remote_meta.compressed_size;
    eprintln!(
        "  Compressed Size:    {}  {}",
        if size_match {
            format!("{}", local_meta.compressed_size)
        } else {
            format!(
                "local={}, remote={}",
                local_meta.compressed_size, remote_meta.compressed_size
            )
        },
        if size_match { "✅" } else { "❌" }
    );

    let uncomp_match = local_meta.uncompressed_size == remote_meta.uncompressed_size;
    eprintln!(
        "  Uncompressed Size:  {}  {}",
        if uncomp_match {
            format!("{}", local_meta.uncompressed_size)
        } else {
            format!(
                "local={}, remote={}",
                local_meta.uncompressed_size, remote_meta.uncompressed_size
            )
        },
        if uncomp_match { "✅" } else { "❌" }
    );

    // Compare times
    let start_match = local_meta.start_time == remote_meta.start_time;
    eprintln!(
        "  Start Time:         {}  {}",
        if start_match { "identical" } else { "differs" },
        if start_match { "✅" } else { "❌" }
    );
    eprintln!("    Local:  {}", local_meta.start_time);
    eprintln!("    Remote: {}", remote_meta.start_time);

    let end_match = local_meta.end_time == remote_meta.end_time;
    eprintln!(
        "  End Time:           {}  {}",
        if end_match { "identical" } else { "differs" },
        if end_match { "✅" } else { "❌" }
    );
    eprintln!("    Local:  {}", local_meta.end_time);
    eprintln!("    Remote: {}", remote_meta.end_time);

    // Compare cursor
    let cursor_match = local_meta.cursor == remote_meta.cursor;
    eprintln!(
        "  Cursor:             {}  {}",
        if cursor_match { "identical" } else { "differs" },
        if cursor_match { "✅" } else { "❌" }
    );
    eprintln!("    Local:  {}", local_meta.cursor);
    eprintln!("    Remote: {}", remote_meta.cursor);
    if !cursor_match {
        eprintln!("    ⚠️  Cursor should match previous bundle's end_time per spec");
    }
}

fn display_operation_comparison(
    local_ops: &[plcbundle::Operation],
    remote_ops: &[plcbundle::Operation],
    sample_size: usize,
    origins_match: bool,
    bundle_num: u32,
    target: &str,
) {
    eprintln!("🔍 Operation Comparison");
    eprintln!("════════════════════════\n");

    if !origins_match {
        eprintln!("  ⚠️  NOTE: Different origins - operation differences are expected\n");
    }

    if local_ops.len() != remote_ops.len() {
        if origins_match {
            eprintln!(
                "  ⚠️  Different operation counts: local={}, remote={}\n",
                local_ops.len(),
                remote_ops.len()
            );
        } else {
            eprintln!(
                "  ℹ️  Different operation counts (expected): local={}, remote={}\n",
                local_ops.len(),
                remote_ops.len()
            );
        }
    }

    // Build CID maps for comparison
    let mut local_cids: HashMap<String, usize> = HashMap::new();
    let mut remote_cids: HashMap<String, usize> = HashMap::new();

    for (i, op) in local_ops.iter().enumerate() {
        if let Some(ref cid) = op.cid {
            local_cids.insert(cid.clone(), i);
        }
    }

    for (i, op) in remote_ops.iter().enumerate() {
        if let Some(ref cid) = op.cid {
            remote_cids.insert(cid.clone(), i);
        }
    }

    // Find differences
    let mut missing_in_local: Vec<(String, usize)> = Vec::new();
    let mut missing_in_remote: Vec<(String, usize)> = Vec::new();
    let mut position_mismatches: Vec<(String, usize, usize)> = Vec::new();

    for (cid, remote_pos) in &remote_cids {
        match local_cids.get(cid) {
            Some(&local_pos) => {
                if local_pos != *remote_pos {
                    position_mismatches.push((cid.clone(), local_pos, *remote_pos));
                }
            }
            None => {
                missing_in_local.push((cid.clone(), *remote_pos));
            }
        }
    }

    for (cid, local_pos) in &local_cids {
        if !remote_cids.contains_key(cid) {
            missing_in_remote.push((cid.clone(), *local_pos));
        }
    }

    // Sort by position
    missing_in_local.sort_by_key(|(_, pos)| *pos);
    missing_in_remote.sort_by_key(|(_, pos)| *pos);
    position_mismatches.sort_by_key(|(_, local_pos, _)| *local_pos);

    // Display differences
    if !missing_in_local.is_empty() {
        eprintln!(
            "  Missing in Local ({} operations):",
            missing_in_local.len()
        );
        let display_count = sample_size.min(missing_in_local.len());
        for i in 0..display_count {
            let (cid, pos) = &missing_in_local[i];
            // Find the operation in remote_ops to get details
            if let Some(remote_op) = remote_ops.get(*pos) {
                eprintln!("    - [{:04}] {}", pos, cid);
                eprintln!("       DID:     {}", remote_op.did);
                eprintln!("       Time:     {}", remote_op.created_at);
                eprintln!("       Nullified: {}", remote_op.nullified);
                if let Some(op_type) = remote_op.operation.get("type").and_then(|v| v.as_str()) {
                    eprintln!("       Type:     {}", op_type);
                }
            } else {
                eprintln!("    - [{:04}] {}", pos, cid);
            }
        }
        if missing_in_local.len() > display_count {
            eprintln!(
                "    ... and {} more",
                missing_in_local.len() - display_count
            );
        }

        // Add hints for exploring missing operations
        if let Some((first_cid, first_pos)) = missing_in_local.first() {
            if target.starts_with("http") {
                let base_url = if target.ends_with('/') {
                    &target[..target.len() - 1]
                } else {
                    target
                };
                let global_pos =
                    (bundle_num as u64 - 1) * constants::BUNDLE_SIZE as u64 + *first_pos as u64;
                eprintln!("  💡 To explore missing operations:");
                eprintln!(
                    "     • Global position: {} (bundle {} position {})",
                    global_pos, bundle_num, first_pos
                );
                eprintln!(
                    "     • View in remote: curl '{}/op/{}' | grep '{}' | jq .",
                    base_url, global_pos, first_cid
                );
            }
        }
        eprintln!();
    }

    if !missing_in_remote.is_empty() {
        eprintln!(
            "  Missing in Remote ({} operations):",
            missing_in_remote.len()
        );
        let display_count = sample_size.min(missing_in_remote.len());
        for i in 0..display_count {
            let (cid, pos) = &missing_in_remote[i];
            // Find the operation in local_ops to get details
            if let Some(local_op) = local_ops.get(*pos) {
                eprintln!("    + [{:04}] {}", pos, cid);
                eprintln!("       DID:     {}", local_op.did);
                eprintln!("       Time:     {}", local_op.created_at);
                eprintln!("       Nullified: {}", local_op.nullified);
                if let Some(op_type) = local_op.operation.get("type").and_then(|v| v.as_str()) {
                    eprintln!("       Type:     {}", op_type);
                }
            } else {
                eprintln!("    + [{:04}] {}", pos, cid);
            }
        }
        if missing_in_remote.len() > display_count {
            eprintln!(
                "    ... and {} more",
                missing_in_remote.len() - display_count
            );
        }

        // Add hints for exploring missing operations
        if let Some((_first_cid, first_pos)) = missing_in_remote.first() {
            let global_pos =
                (bundle_num as u64 - 1) * constants::BUNDLE_SIZE as u64 + *first_pos as u64;
            eprintln!("  💡 To explore missing operations:");
            eprintln!(
                "     • Global position: {} (bundle {} position {})",
                global_pos, bundle_num, first_pos
            );
            eprintln!(
                "     • View in local bundle: {} op get {} {}",
                constants::BINARY_NAME,
                bundle_num,
                first_pos
            );
        }
        eprintln!();
    }

    if !position_mismatches.is_empty() {
        eprintln!(
            "  Position Mismatches ({} operations):",
            position_mismatches.len()
        );
        let display_count = sample_size.min(position_mismatches.len());
        for i in 0..display_count {
            let (cid, local_pos, remote_pos) = &position_mismatches[i];
            eprintln!("    ~ {}", cid);
            eprintln!("      Local:  position {:04}", local_pos);
            eprintln!("      Remote: position {:04}", remote_pos);
        }
        if position_mismatches.len() > display_count {
            eprintln!(
                "    ... and {} more",
                position_mismatches.len() - display_count
            );
        }
        eprintln!();
    }

    if missing_in_local.is_empty() && missing_in_remote.is_empty() && position_mismatches.is_empty()
    {
        eprintln!("  ✅ All operations match (same CIDs, same order)\n");
    }

    // Show sample operations for context
    if !local_ops.is_empty() {
        eprintln!(
            "Sample Operations (first {}):",
            sample_size.min(local_ops.len())
        );
        eprintln!("────────────────────────────────");
        for i in 0..sample_size.min(local_ops.len()) {
            let op = &local_ops[i];
            let remote_match = if let Some(ref cid) = op.cid {
                if let Some(&remote_pos) = remote_cids.get(cid) {
                    if remote_pos == i {
                        " ✅".to_string()
                    } else {
                        format!(" ⚠️  (remote pos: {:04})", remote_pos)
                    }
                } else {
                    " ❌ (missing in remote)".to_string()
                }
            } else {
                String::new()
            };

            eprintln!(
                "  [{:04}] {}{}",
                i,
                op.cid.as_ref().unwrap_or(&"<no-cid>".to_string()),
                remote_match
            );
            eprintln!("         DID: {}", op.did);
            eprintln!("         Time: {}", op.created_at);
        }
        eprintln!();
    }
}

fn display_hash_analysis_full(
    local_meta: &plcbundle::index::BundleMetadata,
    remote_meta: &plcbundle::index::BundleMetadata,
    _bundle_num: u32,
    origins_match: bool,
) {
    eprintln!("🔐 Hash Analysis");
    eprintln!("════════════════\n");

    if !origins_match {
        eprintln!("  ⚠️  NOTE: Different origins - hash mismatches are expected\n");
    }

    // Content hash (most important)
    let content_match = local_meta.content_hash == remote_meta.content_hash;
    eprintln!(
        "  Content Hash:       {}",
        if content_match {
            "✅"
        } else {
            if origins_match {
                "❌"
            } else {
                "ℹ️  (expected)"
            }
        }
    );
    eprintln!("    Local:  {}", local_meta.content_hash);
    eprintln!("    Remote: {}", remote_meta.content_hash);
    if !content_match {
        if origins_match {
            eprintln!("    ⚠️  Different bundle content!");
        } else {
            eprintln!("    ℹ️  Different bundle content (expected - different origins)");
        }
    }
    eprintln!();

    // Compressed hash
    let comp_match = local_meta.compressed_hash == remote_meta.compressed_hash;
    eprintln!(
        "  Compressed Hash:    {}",
        if comp_match {
            "✅"
        } else {
            if origins_match {
                "❌"
            } else {
                "ℹ️  (expected)"
            }
        }
    );
    eprintln!("    Local:  {}", local_meta.compressed_hash);
    eprintln!("    Remote: {}", remote_meta.compressed_hash);
    if !comp_match && content_match {
        eprintln!("    ℹ️  Different compression (same content)");
    } else if !comp_match && !origins_match {
        eprintln!("    ℹ️  Different compression (expected - different origins)");
    }
    eprintln!();

    // Chain hash
    let chain_match = local_meta.hash == remote_meta.hash;
    eprintln!(
        "  Chain Hash:         {}",
        if chain_match {
            "✅"
        } else {
            if origins_match {
                "❌"
            } else {
                "ℹ️  (expected)"
            }
        }
    );
    eprintln!("    Local:  {}", local_meta.hash);
    eprintln!("    Remote: {}", remote_meta.hash);
    if !chain_match {
        // Analyze why chain hash differs
        let parent_match = local_meta.parent == remote_meta.parent;
        eprintln!("\n  Chain Components:");
        eprintln!(
            "    Parent:  {}",
            if parent_match {
                "✅"
            } else {
                if origins_match {
                    "❌"
                } else {
                    "ℹ️  (expected)"
                }
            }
        );
        eprintln!("      Local:  {}", local_meta.parent);
        eprintln!("      Remote: {}", remote_meta.parent);

        if !origins_match {
            eprintln!("    ℹ️  Different origins → different chains (expected)");
        } else if !parent_match {
            eprintln!("    ⚠️  Different parent → chain diverged at earlier bundle");
        } else if !content_match {
            eprintln!("    ⚠️  Same parent but different content → different operations");
        }
    }
}

// Helper functions removed - using direct formatting with colors inline
