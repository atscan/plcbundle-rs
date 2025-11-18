use anyhow::Result;
use clap::{Args, ValueHint};
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use super::progress::ProgressBar;
use super::utils;
use plcbundle::constants::{
    bundle_position_to_global, global_to_bundle_position, total_operations_from_bundles,
};
use plcbundle::format::format_std_duration_auto;

#[derive(Args)]
#[command(
    about = "Export operations to stdout or file",
    long_about = "Export operations from bundles as raw JSONL (JSON Lines) format, suitable
for processing with standard Unix tools or importing into other systems.

This command streams operations directly from compressed bundles without
loading them fully into memory, making it efficient for large exports.
Operations are written one per line in JSON format, preserving all original
operation data including DID, CID, timestamps, and operation payloads.

Use --bundles to export specific bundles or ranges, or --all to export
everything. The --count flag limits the total number of operations exported,
useful for sampling or testing. Output goes to stdout by default, or specify
--output to write to a file.

This is the primary way to extract raw operation data from the repository
for analysis, backup, or migration to other systems.",
    help_template = crate::clap_help!(
        examples: "  # Export all bundles to stdout\n  \
                   {bin} export --all\n\n  \
                   # Export specific bundles\n  \
                   {bin} export 1-100\n  \
                   {bin} export 42\n\n  \
                   # Export to file\n  \
                   {bin} export --all -o operations.jsonl\n\n  \
                   # Export first 1000 operations\n  \
                   {bin} export --all --count 1000\n\n  \
                   # Export in reverse order\n  \
                   {bin} export --all --reverse\n\n  \
                   # Export specific operations by global position (0-indexed)\n  \
                   {bin} export --ops 0-999\n  \
                   {bin} export --ops 3255,553,0-9"
    )
)]
pub struct ExportCommand {
    /// Bundle range to export (e.g., "42", "1-100", or "1-10,20-30")
    #[arg(value_name = "BUNDLES", conflicts_with_all = ["all", "ops"])]
    pub bundles: Option<String>,

    /// Export all bundles
    #[arg(long, conflicts_with = "ops")]
    pub all: bool,

    /// Export specific operations by global position (0-indexed, e.g., "0-999", "3255,553,0-9")
    #[arg(long, conflicts_with_all = ["all", "bundles"])]
    pub ops: Option<String>,

    /// Output file (default: stdout)
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    pub output: Option<PathBuf>,

    /// Limit number of operations to export
    #[arg(short = 'n', long)]
    pub count: Option<usize>,

    /// Export in reverse order (bundles and operations within bundles)
    #[arg(long)]
    pub reverse: bool,
}

pub fn run(cmd: ExportCommand, dir: PathBuf, quiet: bool, verbose: bool) -> Result<()> {
    let output = cmd.output;
    let count = cmd.count;
    let reverse = cmd.reverse;
    // Create BundleManager (follows RULES.md - NO direct file access from CLI)
    let manager = super::utils::create_manager(dir.clone(), verbose, quiet, false)?;
    let index = manager.get_index();
    let max_bundle = index.last_bundle;

    // Determine what to export: bundles or specific operations
    // Use a more efficient representation: store ranges and individual ops separately
    // to avoid materializing millions of operations
    let ops_to_export: Option<OperationFilter> = if let Some(ops_str) = cmd.ops {
        // Calculate max operation number (0-indexed: (max_bundle * BUNDLE_SIZE) - 1)
        // This is an upper bound; actual last operation may be lower if last bundle isn't full
        let max_operation = total_operations_from_bundles(max_bundle).saturating_sub(1);
        Some(OperationFilter::parse(&ops_str, max_operation)?)
    } else {
        None
    };

    // Determine bundle numbers to process
    let mut bundle_numbers: Vec<u32> = if let Some(bundles_str) = cmd.bundles {
        utils::parse_bundle_spec(Some(bundles_str), max_bundle)?
    } else if cmd.all {
        (1..=max_bundle).collect()
    } else if let Some(ref filter) = ops_to_export {
        // When exporting specific operations, calculate which bundles are needed
        // from the operation ranges without materializing all operations
        filter.get_bundle_numbers(max_bundle)
    } else {
        anyhow::bail!("Must specify either --bundles, --all, or --ops");
    };

    // Reverse bundle order if requested
    if reverse {
        bundle_numbers.reverse();
    }

    if verbose && !quiet {
        log::debug!("Index: v{} ({})", index.version, index.origin);
        log::debug!("Processing {} bundles", bundle_numbers.len());
        if let Some(ref count) = count {
            log::debug!(
                "Export limit: {} operations",
                utils::format_number(*count as u64)
            );
        }
    }

    // Display what is being exported (always visible unless quiet)
    if !quiet {
        if let Some(ref filter) = ops_to_export {
            let op_count = filter.estimated_count();
            eprintln!("Exporting {} operations", utils::format_number(op_count));
        } else {
            let bundle_range_str = format_bundle_range(&bundle_numbers);
            eprintln!(
                "Exporting bundles: {} ({})",
                bundle_range_str,
                bundle_numbers.len()
            );
        }
    }

    // Open output with buffering
    let writer: Box<dyn Write> = if let Some(output_path) = output {
        Box::new(BufWriter::with_capacity(
            1024 * 1024,
            File::create(output_path)?,
        ))
    } else {
        Box::new(BufWriter::with_capacity(1024 * 1024, io::stdout()))
    };
    let mut writer = writer;

    if !quiet {
        log::info!("Exporting operations...");
    }

    // Calculate total uncompressed and compressed sizes for progress tracking
    let total_uncompressed_size = index.total_uncompressed_size_for_bundles(&bundle_numbers);
    let total_compressed_size: u64 = bundle_numbers
        .iter()
        .filter_map(|bundle_num| {
            index
                .get_bundle(*bundle_num)
                .map(|meta| meta.compressed_size)
        })
        .sum();

    // Create progress bar tracking bundles processed
    // Skip progress bar if quiet mode or if only one bundle needs to be loaded
    let pb = if quiet || bundle_numbers.len() == 1 {
        None
    } else {
        Some(ProgressBar::with_bytes(
            bundle_numbers.len(),
            total_uncompressed_size,
        ))
    };

    let start = Instant::now();
    let mut exported_count = 0;
    let mut bundles_processed = 0;
    let bytes_written = Arc::new(Mutex::new(0u64));
    let mut output_buffer = String::with_capacity(1024 * 1024); // 1MB buffer
    const BATCH_SIZE: usize = 10000;

    // Process bundles through BundleManager API (follows RULES.md)
    for bundle_num in bundle_numbers {
        // Check count limit
        if let Some(limit) = count
            && exported_count >= limit
        {
            break;
        }

        // Use BundleManager API to get decompressed stream
        let decoder = match manager.stream_bundle_decompressed(bundle_num) {
            Ok(decoder) => decoder,
            Err(_) => {
                // Bundle not found, skip it
                bundles_processed += 1;
                if let Some(ref pb) = pb {
                    let bytes = bytes_written.lock().unwrap();
                    pb.set_with_bytes(bundles_processed, *bytes);
                    drop(bytes);
                }
                continue;
            }
        };
        let reader = BufReader::with_capacity(1024 * 1024, decoder);

        // Collect lines from bundle with their positions
        let mut bundle_lines = Vec::new();
        for (pos, line_result) in reader.lines().enumerate() {
            let line = line_result?;
            if line.is_empty() {
                continue;
            }
            bundle_lines.push((pos, line));
        }

        // Reverse lines if requested (preserve position information for filtering)
        if reverse {
            bundle_lines.reverse();
        }

        // Write lines (filtering by operation position if --ops is specified)
        for (pos, line) in bundle_lines {
            // Check if this operation should be exported
            if let Some(ref filter) = ops_to_export {
                let global_pos = bundle_position_to_global(bundle_num, pos);
                if !filter.contains(global_pos) {
                    continue;
                }
            }

            // Check count limit
            if let Some(limit) = count
                && exported_count >= limit
            {
                break;
            }

            output_buffer.push_str(&line);
            output_buffer.push('\n');
            exported_count += 1;

            // Flush buffer when it gets large
            if output_buffer.len() >= 1024 * 1024 {
                let bytes = output_buffer.len();
                writer.write_all(output_buffer.as_bytes())?;
                let mut bytes_guard = bytes_written.lock().unwrap();
                *bytes_guard += bytes as u64;
                drop(bytes_guard);
                output_buffer.clear();
            }

            // Progress update (operations count in message, but bundles in progress bar)
            if let Some(ref pb) = pb
                && (exported_count % BATCH_SIZE == 0 || exported_count == 1)
            {
                let bytes = bytes_written.lock().unwrap();
                let total_bytes = *bytes + output_buffer.len() as u64;
                drop(bytes);
                pb.set_with_bytes(bundles_processed, total_bytes);
                pb.set_message(format!(
                    "{} ops",
                    utils::format_number(exported_count as u64)
                ));
            }
        }

        // Update progress after processing each bundle
        bundles_processed += 1;
        if let Some(ref pb) = pb {
            let bytes = bytes_written.lock().unwrap();
            let total_bytes = *bytes + output_buffer.len() as u64;
            drop(bytes);
            pb.set_with_bytes(bundles_processed, total_bytes);
            pb.set_message(format!(
                "{} ops",
                utils::format_number(exported_count as u64)
            ));
        }
    }

    // Flush remaining buffer
    if !output_buffer.is_empty() {
        let bytes = output_buffer.len();
        writer.write_all(output_buffer.as_bytes())?;
        let mut bytes_guard = bytes_written.lock().unwrap();
        *bytes_guard += bytes as u64;
        drop(bytes_guard);
    }
    writer.flush()?;

    // Final progress update
    if let Some(ref pb) = pb {
        let bytes = bytes_written.lock().unwrap();
        pb.set_with_bytes(bundles_processed, *bytes);
        drop(bytes);
        pb.finish();
    }

    if !quiet {
        let elapsed = start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        // Format duration with auto-scaling using utility function
        let duration_str = format_std_duration_auto(elapsed);

        let mut parts = Vec::new();
        parts.push(format!(
            "Exported: {} ops",
            utils::format_number(exported_count as u64)
        ));
        parts.push(format!("in {}", duration_str));

        if elapsed_secs > 0.0 {
            // Calculate throughputs
            let uncompressed_throughput_mb =
                (total_uncompressed_size as f64 / elapsed_secs) / (1024.0 * 1024.0);
            let compressed_throughput_mb =
                (total_compressed_size as f64 / elapsed_secs) / (1024.0 * 1024.0);

            parts.push(format!(
                "({:.1} MB/s uncompressed, {:.1} MB/s compressed)",
                uncompressed_throughput_mb, compressed_throughput_mb
            ));
        }

        eprintln!("{}", parts.join(" "));
    }

    Ok(())
}

/// Format bundle numbers as a compact range string (e.g., "1-10", "1, 5, 10", "1-5, 10-15")
/// For large lists, shows a simple min-max range to avoid verbose output
fn format_bundle_range(bundles: &[u32]) -> String {
    if bundles.is_empty() {
        return String::new();
    }
    if bundles.len() == 1 {
        return bundles[0].to_string();
    }

    // For large lists, just show min-max range
    if bundles.len() > 50 {
        let min = bundles.iter().min().copied().unwrap_or(0);
        let max = bundles.iter().max().copied().unwrap_or(0);
        return format!("{}-{}", min, max);
    }

    let mut ranges = Vec::new();
    let mut range_start = bundles[0];
    let mut range_end = bundles[0];

    for &bundle in bundles.iter().skip(1) {
        if bundle == range_end + 1 {
            range_end = bundle;
        } else {
            if range_start == range_end {
                ranges.push(range_start.to_string());
            } else {
                ranges.push(format!("{}-{}", range_start, range_end));
            }
            range_start = bundle;
            range_end = bundle;
        }
    }

    // Add the last range
    if range_start == range_end {
        ranges.push(range_start.to_string());
    } else {
        ranges.push(format!("{}-{}", range_start, range_end));
    }

    let result = ranges.join(", ");

    // If the formatted string is too long, fall back to min-max
    if result.len() > 200 {
        let min = bundles.iter().min().copied().unwrap_or(0);
        let max = bundles.iter().max().copied().unwrap_or(0);
        format!("{}-{}", min, max)
    } else {
        result
    }
}

/// Efficient filter for operation ranges that avoids materializing large lists
/// Stores ranges and individual operations separately for O(1) or O(log n) lookups
struct OperationFilter {
    /// Ranges of operations (inclusive start, inclusive end)
    ranges: Vec<(u64, u64)>,
    /// Individual operations (for non-range specs)
    individual: HashSet<u64>,
}

impl OperationFilter {
    /// Parse operation range specification without materializing all values
    /// This is much more efficient for large ranges like "0-10000000"
    fn parse(spec: &str, max_operation: u64) -> Result<Self> {
        use anyhow::Context;

        if max_operation == 0 {
            anyhow::bail!("No operations available");
        }

        let mut ranges = Vec::new();
        let mut individual = HashSet::new();

        for part in spec.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            if part.contains('-') {
                let range: Vec<&str> = part.split('-').collect();
                if range.len() != 2 {
                    anyhow::bail!("Invalid range format: {}", part);
                }
                let start_str = range[0].trim();
                let end_str = range[1].trim();

                let start: u64 = start_str
                    .parse()
                    .with_context(|| format!("Invalid operation number: {}", start_str))?;
                let end: u64 = end_str
                    .parse()
                    .with_context(|| format!("Invalid operation number: {}", end_str))?;

                if start > end {
                    anyhow::bail!("Invalid range: {} > {} (start must be <= end)", start, end);
                }
                if start > max_operation || end > max_operation {
                    anyhow::bail!(
                        "Invalid range: {}-{} (exceeds maximum operation {})",
                        start,
                        end,
                        max_operation
                    );
                }
                ranges.push((start, end));
            } else {
                let num: u64 = part
                    .parse()
                    .with_context(|| format!("Invalid operation number: {}", part))?;
                if num > max_operation {
                    anyhow::bail!(
                        "Operation number {} out of range (max: {})",
                        num,
                        max_operation
                    );
                }
                individual.insert(num);
            }
        }

        // Sort ranges for efficient lookup
        ranges.sort_unstable();

        Ok(OperationFilter { ranges, individual })
    }

    /// Check if a global operation position is included in the filter
    fn contains(&self, global_pos: u64) -> bool {
        // Check individual operations first (O(1))
        if self.individual.contains(&global_pos) {
            return true;
        }

        // Check ranges (O(log n) with binary search, but we use linear for simplicity)
        // For small number of ranges, linear is fine
        for &(start, end) in &self.ranges {
            if global_pos >= start && global_pos <= end {
                return true;
            }
        }

        false
    }

    /// Get bundle numbers that contain operations in this filter
    /// This calculates bundles from range bounds without materializing all operations
    fn get_bundle_numbers(&self, max_bundle: u32) -> Vec<u32> {
        let mut bundle_set = HashSet::new();

        // Process ranges
        for &(start, end) in &self.ranges {
            // Convert start and end to bundle numbers
            let (start_bundle, _) = global_to_bundle_position(start);
            let (end_bundle, _) = global_to_bundle_position(end);

            // Add all bundles in the range
            for bundle in start_bundle..=end_bundle.min(max_bundle) {
                bundle_set.insert(bundle);
            }
        }

        // Process individual operations
        for &op in &self.individual {
            let (bundle, _) = global_to_bundle_position(op);
            if bundle <= max_bundle {
                bundle_set.insert(bundle);
            }
        }

        let mut bundles: Vec<u32> = bundle_set.into_iter().collect();
        bundles.sort_unstable();
        bundles
    }

    /// Estimate the number of operations (for display purposes)
    fn estimated_count(&self) -> u64 {
        let range_count: u64 = self
            .ranges
            .iter()
            .map(|&(start, end)| end.saturating_sub(start).saturating_add(1))
            .sum();
        let individual_count = self.individual.len() as u64;
        range_count.saturating_add(individual_count)
    }
}
