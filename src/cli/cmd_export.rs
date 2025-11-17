use anyhow::Result;
use clap::{Args, ValueHint};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use super::progress::ProgressBar;
use super::utils;
use plcbundle::constants::BUNDLE_SIZE;
use plcbundle::format_std_duration_auto;
use plcbundle::parse_operation_range;

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


/// Convert global operation number (0-indexed) to (bundle, position) pair
/// Bundle is 1-indexed, position is 0-indexed within bundle
/// Global positions are 0-indexed: 0 = bundle 1 position 0, 1 = bundle 1 position 1, etc.
fn global_op_to_bundle_position(global_op: u64) -> (u32, usize) {
    // Global positions are 0-indexed: bundle = (global_pos / BUNDLE_SIZE) + 1
    let bundle_num = ((global_op / BUNDLE_SIZE as u64) + 1) as u32;
    let position = (global_op % BUNDLE_SIZE as u64) as usize;
    (bundle_num, position)
}

pub fn run(cmd: ExportCommand, dir: PathBuf, quiet: bool, verbose: bool) -> Result<()> {
    let output = cmd.output;
    let count = cmd.count;
    let reverse = cmd.reverse;
    // Create BundleManager (follows RULES.md - NO direct file access from CLI)
    let manager = super::utils::create_manager(dir.clone(), verbose, quiet)?;
    let index = manager.get_index();
    let max_bundle = index.last_bundle;

    // Determine what to export: bundles or specific operations
    let ops_to_export: Option<HashMap<(u32, usize), bool>> = if let Some(ops_str) = cmd.ops {
        // Calculate max operation number (0-indexed: (max_bundle * BUNDLE_SIZE) - 1)
        // This is an upper bound; actual last operation may be lower if last bundle isn't full
        let max_operation = (max_bundle as u64 * BUNDLE_SIZE as u64).saturating_sub(1);
        let global_ops = parse_operation_range(&ops_str, max_operation)?;
        
        // Convert to (bundle, position) pairs and create a lookup map
        let mut ops_map = HashMap::new();
        for global_op in global_ops {
            let (bundle, position) = global_op_to_bundle_position(global_op);
            ops_map.insert((bundle, position), true);
        }
        Some(ops_map)
    } else {
        None
    };

    // Determine bundle numbers to process
    let mut bundle_numbers: Vec<u32> = if let Some(bundles_str) = cmd.bundles {
        utils::parse_bundle_spec(Some(bundles_str), max_bundle)?
    } else if cmd.all {
        (1..=max_bundle).collect()
    } else if ops_to_export.is_some() {
        // When exporting specific operations, collect unique bundle numbers
        let mut bundles: Vec<u32> = ops_to_export
            .as_ref()
            .unwrap()
            .keys()
            .map(|(bundle, _)| *bundle)
            .collect();
        bundles.sort_unstable();
        bundles.dedup();
        bundles
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
        if let Some(ref ops_map) = ops_to_export {
            let op_count = ops_map.len();
            eprintln!("Exporting {} operations", utils::format_number(op_count as u64));
        } else {
            let bundle_range_str = format_bundle_range(&bundle_numbers);
            eprintln!("Exporting bundles: {} ({})", bundle_range_str, bundle_numbers.len());
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
            index.get_bundle(*bundle_num).map(|meta| meta.compressed_size)
        })
        .sum();
    
    // Create progress bar tracking bundles processed
    // Skip progress bar if quiet mode or if only one bundle needs to be loaded
    let pb = if quiet || bundle_numbers.len() == 1 {
        None
    } else {
        Some(ProgressBar::with_bytes(bundle_numbers.len(), total_uncompressed_size))
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
        if let Some(limit) = count {
            if exported_count >= limit {
                break;
            }
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
            if let Some(ref ops_map) = ops_to_export {
                if !ops_map.contains_key(&(bundle_num, pos)) {
                    continue;
                }
            }

            // Check count limit
            if let Some(limit) = count {
                if exported_count >= limit {
                    break;
                }
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
            if let Some(ref pb) = pb {
                if exported_count % BATCH_SIZE == 0 || exported_count == 1 {
                    let bytes = bytes_written.lock().unwrap();
                    let total_bytes = *bytes + output_buffer.len() as u64;
                    drop(bytes);
                    pb.set_with_bytes(bundles_processed, total_bytes);
                    pb.set_message(format!("{} ops", utils::format_number(exported_count as u64)));
                }
            }
        }
        
        // Update progress after processing each bundle
        bundles_processed += 1;
        if let Some(ref pb) = pb {
            let bytes = bytes_written.lock().unwrap();
            let total_bytes = *bytes + output_buffer.len() as u64;
            drop(bytes);
            pb.set_with_bytes(bundles_processed, total_bytes);
            pb.set_message(format!("{} ops", utils::format_number(exported_count as u64)));
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
        parts.push(format!("Exported: {} ops", utils::format_number(exported_count as u64)));
        parts.push(format!("in {}", duration_str));
        
        if elapsed_secs > 0.0 {
            // Calculate throughputs
            let uncompressed_throughput_mb = (total_uncompressed_size as f64 / elapsed_secs) / (1024.0 * 1024.0);
            let compressed_throughput_mb = (total_compressed_size as f64 / elapsed_secs) / (1024.0 * 1024.0);
            
            parts.push(format!("({:.1} MB/s uncompressed, {:.1} MB/s compressed)",
                uncompressed_throughput_mb,
                compressed_throughput_mb
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
