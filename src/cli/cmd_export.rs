use anyhow::Result;
use indicatif::HumanDuration;
use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

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
    // Load index to get max bundle and metadata
    let index = plcbundle::Index::load(&dir)?;
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
        bundle_numbers.into_iter()
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
            log::debug!("🔢 Export limit: {} operations", utils::format_number(*count as u64));
        }
        if let Some(ref after) = after {
            log::debug!("⏰ After timestamp: {}", after);
        }
    }

    // Open output with buffering
    let writer: Box<dyn Write> = if let Some(output_path) = output {
        Box::new(BufWriter::with_capacity(1024 * 1024, File::create(output_path)?))
    } else {
        Box::new(BufWriter::with_capacity(1024 * 1024, io::stdout()))
    };
    let mut writer = writer;

    if !quiet {
        log::info!("📤 Exporting operations...");
    }

    let start = Instant::now();
    let mut exported_count = 0;
    let mut output_buffer = String::with_capacity(1024 * 1024); // 1MB buffer
    const BATCH_SIZE: usize = 10000;

    // Process bundles directly from files
    for bundle_num in bundle_numbers {
        // Check count limit
        if let Some(limit) = count {
            if exported_count >= limit {
                break;
            }
        }

        let bundle_path = dir.join(format!("{:06}.jsonl.zst", bundle_num));
        if !bundle_path.exists() {
            continue;
        }

        // Open and decode bundle file
        let file = File::open(&bundle_path)?;
        let decoder = zstd::Decoder::new(file)?;
        let reader = std::io::BufReader::with_capacity(1024 * 1024, decoder);

        // Fast path: no filters and Jsonl format - just pass through lines
        let needs_parsing = after.is_some() || did.is_some() || op_type.is_some() || 
                           matches!(format, ExportFormat::Json | ExportFormat::Csv | ExportFormat::Parquet);

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
                    writer.write_all(output_buffer.as_bytes())?;
                    output_buffer.clear();
                }

                // Progress update
                if !quiet && exported_count % BATCH_SIZE == 0 {
                    eprint!("\r   Exported: {} operations", utils::format_number(exported_count as u64));
                    io::stderr().flush()?;
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
                    if let Some(created_at) = data.get("createdAt").or_else(|| data.get("created_at")) {
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
                        let op = data.get("operation").map(|v| sonic_rs::to_string(v).unwrap_or_default()).unwrap_or_default();
                        let created_at = data.get("createdAt").or_else(|| data.get("created_at"))
                            .and_then(|v| v.as_str()).unwrap_or("");
                        let nullified = data.get("nullified").and_then(|v| v.as_bool()).unwrap_or(false);
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
                    writer.write_all(output_buffer.as_bytes())?;
                    output_buffer.clear();
                }

                // Progress update
                if !quiet && exported_count % BATCH_SIZE == 0 {
                    eprint!("\r   Exported: {} operations", utils::format_number(exported_count as u64));
                    io::stderr().flush()?;
                }
            }
        }
    }

    // Flush remaining buffer
    if !output_buffer.is_empty() {
        writer.write_all(output_buffer.as_bytes())?;
    }
    writer.flush()?;

    if !quiet {
        log::info!("\r   Exported: {} operations", utils::format_number(exported_count as u64));
        let elapsed = start.elapsed();
        log::info!("✅ Complete in {}", HumanDuration(elapsed));
        if elapsed.as_secs_f64() > 0.0 {
            log::info!("   Throughput: {:.0} ops/sec",
                exported_count as f64 / elapsed.as_secs_f64()
            );
        }
    }

    Ok(())
}
