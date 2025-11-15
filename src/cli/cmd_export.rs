use anyhow::Result;
use indicatif::HumanDuration;
use plcbundle::BundleManager;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use super::progress::ProgressBar;
use super::utils;
// ExportFormat is defined in plcbundle-rs.rs
// Access it via the parent module
use super::ExportFormat;

pub fn cmd_export(
    dir: PathBuf,
    range: Option<String>,
    all: bool,
    bundles: Option<String>, // Legacy flag
    format: ExportFormat,
    output: Option<PathBuf>,
    count: Option<usize>,
    after: Option<String>,
    did: Option<String>,
    op_type: Option<String>,
    _compress: bool,
    quiet: bool,
    verbose: bool,
) -> Result<()> {
    // Create BundleManager (follows RULES.md - NO direct file access from CLI)
    let manager = BundleManager::new(dir.clone())?;
    let index = manager.get_index();
    let max_bundle = index.last_bundle;

    // Determine bundle numbers to process
    let bundle_numbers: Vec<u32> = if let Some(range_str) = range {
        // Parse range like "1-100"
        if range_str.contains('-') {
            let parts: Vec<&str> = range_str.split('-').collect();
            if parts.len() == 2 {
                let start: u32 = parts[0].parse()?;
                let end: u32 = parts[1].parse()?;
                if start > end || start == 0 || end > max_bundle {
                    anyhow::bail!("Invalid range: {}-{}", start, end);
                }
                (start..=end).collect()
            } else {
                anyhow::bail!("Invalid range format: {}", range_str);
            }
        } else {
            // Single bundle number
            let num: u32 = range_str.parse()?;
            if num == 0 || num > max_bundle {
                anyhow::bail!("Bundle number {} out of range", num);
            }
            vec![num]
        }
    } else if all {
        (1..=max_bundle).collect()
    } else if let Some(bundles_str) = bundles {
        // Legacy --bundles flag
        utils::parse_bundle_spec(Some(bundles_str), max_bundle)?
    } else {
        anyhow::bail!("Must specify either --range, --all, or --bundles");
    };

    // Filter bundles by timestamp metadata if --after is specified
    let bundle_numbers: Vec<u32> = if let Some(ref after_ts) = after {
        bundle_numbers
            .into_iter()
            .filter_map(|num| {
                if let Some(meta) = index.get_bundle(num) {
                    // Check if bundle's end_time is after the filter timestamp
                    // If bundle ends before the filter, skip it
                    if meta.end_time >= *after_ts {
                        Some(num)
                    } else {
                        None
                    }
                } else {
                    Some(num) // Include if metadata not found (will be checked during processing)
                }
            })
            .collect()
    } else {
        bundle_numbers
    };

    if verbose && !quiet {
        log::debug!("📦 Index: v{} ({})", index.version, index.origin);
        log::debug!("📊 Processing {} bundles", bundle_numbers.len());
        if let Some(ref count) = count {
            log::debug!(
                "🔢 Export limit: {} operations",
                utils::format_number(*count as u64)
            );
        }
        if let Some(ref after) = after {
            log::debug!("⏰ After timestamp: {}", after);
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
        log::info!("📤 Exporting operations...");
    }

    // Calculate total uncompressed size for progress tracking
    let total_uncompressed_size = index.total_uncompressed_size_for_bundles(&bundle_numbers);
    
    // Create progress bar tracking bundles processed
    let pb = if quiet {
        None
    } else {
        Some(ProgressBar::with_bytes(bundle_numbers.len(), total_uncompressed_size))
    };

    let start = Instant::now();
    let mut exported_count = 0;
    let mut bundles_processed = 0;
    let mut bytes_written = Arc::new(Mutex::new(0u64));
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

        // Fast path: no filters and Jsonl format - just pass through lines
        let needs_parsing = after.is_some()
            || did.is_some()
            || op_type.is_some()
            || matches!(
                format,
                ExportFormat::Json | ExportFormat::Csv | ExportFormat::Parquet
            );

        if !needs_parsing {
            // Fast path: no parsing needed, just copy lines
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
                        pb.set_message(format!("{} operations", utils::format_number(exported_count as u64)));
                    }
                }
            }
        } else {
            // Slow path: need to parse for filtering or formatting
            use sonic_rs::JsonValueTrait;

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

                // Parse JSON using sonic-rs for faster parsing
                let data: sonic_rs::Value = match sonic_rs::from_str(&line) {
                    Ok(data) => data,
                    Err(_) => continue, // Skip invalid JSON
                };

                // Apply filters using sonic-rs Value API
                if let Some(ref after_ts) = after {
                    if let Some(created_at) =
                        data.get("createdAt").or_else(|| data.get("created_at"))
                    {
                        if let Some(ts_str) = created_at.as_str() {
                            if ts_str < after_ts.as_str() {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }

                if let Some(ref did_filter) = did {
                    if let Some(did_val) = data.get("did") {
                        if let Some(did_str) = did_val.as_str() {
                            if did_str != did_filter {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }

                if let Some(ref op_type_filter) = op_type {
                    if let Some(op_val) = data.get("operation") {
                        let matches = if op_val.is_str() {
                            op_val.as_str().unwrap() == op_type_filter
                        } else if op_val.is_object() {
                            if let Some(typ_val) = op_val.get("type") {
                                typ_val.is_str() && typ_val.as_str().unwrap() == op_type_filter
                            } else {
                                false
                            }
                        } else {
                            false
                        };
                        if !matches {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }

                // Format operation
                let formatted = match format {
                    ExportFormat::Jsonl => {
                        // Already have the JSON string, use it directly
                        line
                    }
                    ExportFormat::Json => {
                        // Pretty print using sonic-rs
                        sonic_rs::to_string_pretty(&data)?
                    }
                    ExportFormat::Csv => {
                        let did = data.get("did").and_then(|v| v.as_str()).unwrap_or("");
                        let op = data
                            .get("operation")
                            .map(|v| sonic_rs::to_string(v).unwrap_or_default())
                            .unwrap_or_default();
                        let created_at = data
                            .get("createdAt")
                            .or_else(|| data.get("created_at"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let nullified = data
                            .get("nullified")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        format!("{},{},{},{}", did, op, created_at, nullified)
                    }
                    ExportFormat::Parquet => {
                        // Fall back to JSON for now
                        sonic_rs::to_string(&data)?
                    }
                };

                output_buffer.push_str(&formatted);
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
                        pb.set_message(format!("{} operations", utils::format_number(exported_count as u64)));
                    }
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
            pb.set_message(format!("{} operations", utils::format_number(exported_count as u64)));
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
        log::info!("✅ Complete in {}", HumanDuration(elapsed));
        log::info!(
            "   Exported: {} operations",
            utils::format_number(exported_count as u64)
        );
        if elapsed.as_secs_f64() > 0.0 {
            log::info!(
                "   Throughput: {:.0} ops/sec",
                exported_count as f64 / elapsed.as_secs_f64()
            );
        }
    }

    Ok(())
}
