use anyhow::Result;
use clap::Args;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use super::progress::ProgressBar;
use super::utils;
use plcbundle::format_std_duration_auto;

/// Format bundle numbers as a compact range string (e.g., "1-10", "1, 5, 10", "1-5, 10-15")
fn format_bundle_range(bundles: &[u32]) -> String {
    if bundles.is_empty() {
        return String::new();
    }
    if bundles.len() == 1 {
        return bundles[0].to_string();
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

    ranges.join(", ")
}

#[derive(Args)]
#[command(about = "Export operations to different formats")]
pub struct ExportCommand {
    /// Bundle range to export (e.g., "42", "1-100", or "1-10,20-30")
    #[arg(value_name = "BUNDLES")]
    pub bundles: Option<String>,

    /// Export all bundles
    #[arg(long)]
    pub all: bool,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Limit number of operations to export
    #[arg(short = 'n', long)]
    pub count: Option<usize>,
}


pub fn run(cmd: ExportCommand, dir: PathBuf, quiet: bool, verbose: bool) -> Result<()> {
    let output = cmd.output;
    let count = cmd.count;
    // Create BundleManager (follows RULES.md - NO direct file access from CLI)
    let manager = super::utils::create_manager(dir.clone(), verbose, quiet)?;
    let index = manager.get_index();
    let max_bundle = index.last_bundle;

    // Determine bundle numbers to process
    let bundle_numbers: Vec<u32> = if let Some(bundles_str) = cmd.bundles {
        utils::parse_bundle_spec(Some(bundles_str), max_bundle)?
    } else if cmd.all {
        (1..=max_bundle).collect()
    } else {
        anyhow::bail!("Must specify either --bundles or --all");
    };

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

    // Display which bundles are being exported (always visible unless quiet)
    if !quiet {
        let bundle_range_str = format_bundle_range(&bundle_numbers);
        eprintln!("Exporting bundles: {} ({})", bundle_range_str, bundle_numbers.len());
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
    let pb = if quiet {
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

        // Pass through lines
        for line in reader.lines() {
            // Check count limit
            if let Some(limit) = count {
                if exported_count >= limit {
                    break;
                }
            }

            let line = line?;
            if line.is_empty() {
                continue;
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
