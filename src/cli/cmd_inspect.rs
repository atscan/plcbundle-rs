// Inspect command - deep analysis of bundle contents
use anyhow::Result;
use chrono::DateTime;
use clap::{Args, ValueHint};
use plcbundle::format::{format_bytes, format_duration_verbose, format_number};
use plcbundle::{BundleManager, LoadOptions, Operation};
use serde::Serialize;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Args)]
#[command(
    about = "Deep analysis of bundle contents",
    long_about = "Perform comprehensive analysis of a bundle's contents, structure, and patterns.
This command provides detailed insights into what operations are stored, how DIDs
are distributed, what handles and domains are present, and how operations are
distributed over time.

The analysis includes embedded metadata extraction (from the zstd skippable frame),
operation type distribution, DID activity patterns (which DIDs appear most frequently),
handle and domain statistics, service endpoint analysis, temporal distribution
(peak hours, time spans), and detailed size analysis.

You can inspect bundles either by bundle number (from the repository index) or by
direct file path. Use --skip-patterns to speed up analysis by skipping handle and
service pattern extraction. Use --samples to see example operations from the bundle.

This command is invaluable for understanding bundle composition, identifying data
quality issues, and analyzing patterns in the PLC directory data.",
    help_template = crate::clap_help!(
        examples: "  # Inspect from repository\n  \
                   {bin} inspect 42\n\n  \
                   # Inspect specific file\n  \
                   {bin} inspect /path/to/000042.jsonl.zst\n  \
                   {bin} inspect 000042.jsonl.zst\n\n  \
                   # Skip certain analysis sections\n  \
                   {bin} inspect 42 --skip-patterns\n\n  \
                   # Show sample operations\n  \
                   {bin} inspect 42 --samples --sample-count 20\n\n  \
                   # JSON output (for scripting)\n  \
                   {bin} inspect 42 --json"
    )
)]
pub struct InspectCommand {
    /// Bundle number or file path to inspect
    #[arg(value_hint = ValueHint::AnyPath)]
    pub target: String,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,

    /// Skip embedded metadata section
    #[arg(long)]
    pub skip_metadata: bool,

    /// Skip pattern analysis (handles, services)
    #[arg(long)]
    pub skip_patterns: bool,

    /// Show sample operations
    #[arg(long)]
    pub samples: bool,

    /// Number of samples to show
    #[arg(long, default_value = "10")]
    pub sample_count: usize,
}

#[derive(Debug, Serialize)]
struct InspectResult {
    // File info
    file_path: String,
    file_size: u64,
    has_metadata_frame: bool,

    // Embedded metadata (from skippable frame)
    #[serde(skip_serializing_if = "Option::is_none")]
    embedded_metadata: Option<EmbeddedMetadataInfo>,

    // Index metadata (from plc_bundles.json)
    #[serde(skip_serializing_if = "Option::is_none")]
    index_metadata: Option<IndexMetadataInfo>,

    // Basic stats
    total_ops: usize,
    nullified_ops: usize,
    active_ops: usize,
    unique_dids: usize,

    // Operation types
    operation_types: HashMap<String, usize>,

    // DID patterns
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_dids: Vec<DIDActivity>,
    single_op_dids: usize,
    multi_op_dids: usize,

    // Handle patterns
    #[serde(skip_serializing_if = "Option::is_none")]
    total_handles: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_domains: Vec<DomainCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    invalid_handles: Option<usize>,

    // Service patterns
    #[serde(skip_serializing_if = "Option::is_none")]
    total_services: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unique_endpoints: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    top_pds_endpoints: Vec<EndpointCount>,

    // Temporal
    #[serde(skip_serializing_if = "Option::is_none")]
    time_distribution: Option<TimeDistribution>,
    avg_ops_per_minute: f64,

    // Size analysis
    avg_op_size: usize,
    min_op_size: usize,
    max_op_size: usize,
    total_op_size: u64,
}

#[derive(Debug, Serialize)]
struct EmbeddedMetadataInfo {
    format: String,
    origin: String,
    bundle_number: u32,
    created_by: String,
    created_at: String,
    operation_count: usize,
    did_count: usize,
    frame_count: usize,
    frame_size: usize,
    start_time: String,
    end_time: String,
    content_hash: String,
    parent_hash: Option<String>,
    frame_offsets: Vec<i64>,
    metadata_frame_size: Option<u64>,
}

#[derive(Debug, Serialize)]
struct IndexMetadataInfo {
    hash: String,
    parent: String,
    cursor: String,
    compressed_hash: String,
    compressed_size: u64,
    uncompressed_size: u64,
    compression_ratio: f64,
}

#[derive(Debug, Serialize)]
struct DIDActivity {
    did: String,
    count: usize,
}

#[derive(Debug, Serialize)]
struct DomainCount {
    domain: String,
    count: usize,
}

#[derive(Debug, Serialize)]
struct EndpointCount {
    endpoint: String,
    count: usize,
}

#[derive(Debug, Serialize)]
struct TimeDistribution {
    earliest_op: String,
    latest_op: String,
    time_span: String,
    peak_hour: String,
    peak_hour_ops: usize,
    total_hours: usize,
}

pub fn run(cmd: InspectCommand, dir: PathBuf) -> Result<()> {
    let manager = super::utils::create_manager(dir.clone(), false, false, false)?;

    // Resolve target to bundle number or file path
    let (bundle_num, file_path) = super::utils::resolve_bundle_target(&manager, &cmd.target, &dir)?;

    // Get file size - always use bundle metadata if available, otherwise read from filesystem
    let file_size = if let Some(num) = bundle_num {
        // Use bundle metadata from index (avoids direct file access per RULES.md)
        manager
            .get_bundle_metadata(num)?
            .map(|meta| meta.compressed_size)
            .unwrap_or(0)
    } else {
        // For arbitrary file paths, we still need filesystem access - this should be refactored
        // to use a manager method for loading from arbitrary paths in the future if supported.
        // For now, it will return an error as per `resolve_bundle_target`.
        anyhow::bail!("Loading from arbitrary paths not yet implemented. Please specify a bundle number.");
    };

    if !cmd.json {
        eprintln!("Inspecting: {}", file_path.display());
        eprintln!("File size: {}\n", format_bytes(file_size));
    }

    // Load bundle
    let load_result = if let Some(num) = bundle_num {
        manager.load_bundle(num, LoadOptions::default())?
    } else {
        // TODO: Add method to load from arbitrary path
        anyhow::bail!("Loading from arbitrary paths not yet implemented");
    };

    let operations = load_result.operations;

    // Analyze operations
    let analysis = analyze_operations(&operations, &cmd)?;

    // Extract metadata information using BundleManager API
    let (embedded_metadata, index_metadata, has_metadata) = if let Some(num) = bundle_num {
        // Get embedded metadata from skippable frame via BundleManager
        let embedded = manager.get_embedded_metadata(num)?;
        let index = manager.get_bundle_metadata(num)?;

        let embedded_info = embedded.as_ref().map(|meta| {
            let metadata_frame_size = meta
                .frame_offsets
                .last()
                .map(|&last_offset| file_size as i64 - last_offset)
                .filter(|&size| size > 0)
                .map(|s| s as u64);

            EmbeddedMetadataInfo {
                format: meta.format.clone(),
                origin: meta.origin.clone(),
                bundle_number: meta.bundle_number,
                created_by: meta.created_by.clone(),
                created_at: meta.created_at.clone(),
                operation_count: meta.operation_count,
                did_count: meta.did_count,
                frame_count: meta.frame_count,
                frame_size: meta.frame_size,
                start_time: meta.start_time.clone(),
                end_time: meta.end_time.clone(),
                content_hash: meta.content_hash.clone(),
                parent_hash: meta.parent_hash.clone(),
                frame_offsets: meta.frame_offsets.clone(),
                metadata_frame_size,
            }
        });

        let index_info = index.as_ref().map(|meta| {
            let compression_ratio =
                (1.0 - meta.compressed_size as f64 / meta.uncompressed_size as f64) * 100.0;
            IndexMetadataInfo {
                hash: meta.hash.clone(),
                parent: meta.parent.clone(),
                cursor: meta.cursor.clone(),
                compressed_hash: meta.compressed_hash.clone(),
                compressed_size: meta.compressed_size,
                uncompressed_size: meta.uncompressed_size,
                compression_ratio,
            }
        });

        (embedded_info, index_info, embedded.is_some())
    } else {
        (None, None, false)
    };

    let result = InspectResult {
        file_path: file_path.display().to_string(),
        file_size,
        has_metadata_frame: has_metadata,
        embedded_metadata,
        index_metadata,
        total_ops: analysis.total_ops,
        nullified_ops: analysis.nullified_ops,
        active_ops: analysis.active_ops,
        unique_dids: analysis.unique_dids,
        operation_types: analysis.operation_types,
        top_dids: analysis.top_dids,
        single_op_dids: analysis.single_op_dids,
        multi_op_dids: analysis.multi_op_dids,
        total_handles: analysis.total_handles,
        top_domains: analysis.top_domains,
        invalid_handles: analysis.invalid_handles,
        total_services: analysis.total_services,
        unique_endpoints: analysis.unique_endpoints,
        top_pds_endpoints: analysis.top_pds_endpoints,
        time_distribution: analysis.time_distribution,
        avg_ops_per_minute: analysis.avg_ops_per_minute,
        avg_op_size: analysis.avg_op_size,
        min_op_size: analysis.min_op_size,
        max_op_size: analysis.max_op_size,
        total_op_size: analysis.total_op_size,
    };

    if cmd.json {
        println!("{}", sonic_rs::to_string_pretty(&result)?);
    } else {
        display_human(&result, &operations, &cmd, bundle_num, &manager)?;
    }

    Ok(())
}

#[derive(Debug)]
struct Analysis {
    total_ops: usize,
    nullified_ops: usize,
    active_ops: usize,
    unique_dids: usize,
    operation_types: HashMap<String, usize>,
    top_dids: Vec<DIDActivity>,
    single_op_dids: usize,
    multi_op_dids: usize,
    total_handles: Option<usize>,
    top_domains: Vec<DomainCount>,
    invalid_handles: Option<usize>,
    total_services: Option<usize>,
    unique_endpoints: Option<usize>,
    top_pds_endpoints: Vec<EndpointCount>,
    time_distribution: Option<TimeDistribution>,
    avg_ops_per_minute: f64,
    avg_op_size: usize,
    min_op_size: usize,
    max_op_size: usize,
    total_op_size: u64,
}

fn analyze_operations(operations: &[Operation], cmd: &InspectCommand) -> Result<Analysis> {
    let total_ops = operations.len();
    let mut nullified_ops = 0;
    let mut did_activity: HashMap<String, usize> = HashMap::new();
    let mut operation_types: HashMap<String, usize> = HashMap::new();
    let mut domain_counts: HashMap<String, usize> = HashMap::new();
    let mut endpoint_counts: HashMap<String, usize> = HashMap::new();
    let mut total_handles = 0;
    let mut invalid_handles = 0;
    let mut total_services = 0;
    let mut total_op_size = 0u64;
    let mut min_op_size = usize::MAX;
    let mut max_op_size = 0;

    // Temporal analysis - group by minute
    let mut time_slots: HashMap<i64, usize> = HashMap::new();

    for op in operations {
        // Count nullified
        if op.nullified {
            nullified_ops += 1;
        }

        // DID activity
        *did_activity.entry(op.did.clone()).or_insert(0) += 1;

        // Operation size
        let op_size = op.raw_json.as_ref().map(|s| s.len()).unwrap_or(0);
        total_op_size += op_size as u64;
        min_op_size = min_op_size.min(op_size);
        max_op_size = max_op_size.max(op_size);

        // Parse operation for detailed analysis
        let op_val = &op.operation;
        // Operation type
        if let Some(op_type) = op_val.get("type").and_then(|v| v.as_str()) {
            *operation_types.entry(op_type.to_string()).or_insert(0) += 1;
        }

        // Pattern analysis (if not skipped)
        if !cmd.skip_patterns {
            // Handle analysis
            if let Some(aka) = op_val.get("alsoKnownAs").and_then(|v| v.as_array()) {
                for item in aka.iter() {
                    if let Some(aka_str) = item.as_str() {
                        if aka_str.starts_with("at://") {
                            total_handles += 1;

                            // Extract domain
                            let handle = aka_str.strip_prefix("at://").unwrap_or("");
                            let handle = handle.split('/').next().unwrap_or("");

                            // Count domain (TLD)
                            let parts: Vec<&str> = handle.split('.').collect();
                            if parts.len() >= 2 {
                                let domain = format!(
                                    "{}.{}",
                                    parts[parts.len() - 2],
                                    parts[parts.len() - 1]
                                );
                                *domain_counts.entry(domain).or_insert(0) += 1;
                            }

                            // Check for invalid patterns
                            if handle.contains('_') {
                                invalid_handles += 1;
                            }
                        }
                    }
                }
            }

            // Service analysis
            if let Some(services) = op_val.get("services").and_then(|v| v.as_object()) {
                total_services += services.len();

                // Extract PDS endpoints
                if let Some(pds_val) = op_val.get("services").and_then(|v| v.get("atproto_pds")) {
                    if let Some(_pds) = pds_val.as_object() {
                        if let Some(endpoint) = pds_val.get("endpoint").and_then(|v| v.as_str()) {
                            // Normalize endpoint
                            let endpoint = endpoint
                                .strip_prefix("https://")
                                .or_else(|| endpoint.strip_prefix("http://"))
                                .unwrap_or(endpoint);
                            let endpoint = endpoint.split('/').next().unwrap_or(endpoint);
                            *endpoint_counts.entry(endpoint.to_string()).or_insert(0) += 1;
                        }
                    }
                }
            }
        }

        // Time distribution (group by minute)
        if let Ok(dt) = DateTime::parse_from_rfc3339(&op.created_at) {
            let timestamp = dt.timestamp();
            let time_slot = timestamp / 60; // Group by minute
            *time_slots.entry(time_slot).or_insert(0) += 1;
        }
    }

    // Calculate derived stats
    let active_ops = total_ops - nullified_ops;
    let unique_dids = did_activity.len();

    // Count single vs multi-op DIDs
    let mut single_op_dids = 0;
    let mut multi_op_dids = 0;
    for &count in did_activity.values() {
        if count == 1 {
            single_op_dids += 1;
        } else {
            multi_op_dids += 1;
        }
    }

    // Top DIDs
    let top_dids = get_top_n(&did_activity, 10);

    // Top domains
    let top_domains = if !cmd.skip_patterns {
        get_top_domains(&domain_counts, 10)
    } else {
        Vec::new()
    };

    // Top endpoints
    let top_pds_endpoints = if !cmd.skip_patterns {
        get_top_endpoints(&endpoint_counts, 10)
    } else {
        Vec::new()
    };

    // Time distribution
    let time_distribution = calculate_time_distribution(&time_slots, operations);

    // Ops per minute
    let avg_ops_per_minute = if operations.len() > 1 {
        if let (Ok(first), Ok(last)) = (
            DateTime::parse_from_rfc3339(&operations[0].created_at),
            DateTime::parse_from_rfc3339(&operations[operations.len() - 1].created_at),
        ) {
            let duration = last.signed_duration_since(first);
            let minutes = duration.num_minutes() as f64;
            if minutes > 0.0 {
                operations.len() as f64 / minutes
            } else {
                0.0
            }
        } else {
            0.0
        }
    } else {
        0.0
    };

    // Average operation size
    let avg_op_size = if total_ops > 0 {
        (total_op_size / total_ops as u64) as usize
    } else {
        0
    };

    Ok(Analysis {
        total_ops,
        nullified_ops,
        active_ops,
        unique_dids,
        operation_types,
        top_dids,
        single_op_dids,
        multi_op_dids,
        total_handles: if cmd.skip_patterns {
            None
        } else {
            Some(total_handles)
        },
        top_domains,
        invalid_handles: if cmd.skip_patterns {
            None
        } else {
            Some(invalid_handles)
        },
        total_services: if cmd.skip_patterns {
            None
        } else {
            Some(total_services)
        },
        unique_endpoints: if cmd.skip_patterns {
            None
        } else {
            Some(endpoint_counts.len())
        },
        top_pds_endpoints,
        time_distribution,
        avg_ops_per_minute,
        avg_op_size,
        min_op_size: if min_op_size == usize::MAX {
            0
        } else {
            min_op_size
        },
        max_op_size,
        total_op_size,
    })
}

fn get_top_n(map: &HashMap<String, usize>, limit: usize) -> Vec<DIDActivity> {
    let mut results: Vec<_> = map
        .iter()
        .map(|(did, &count)| DIDActivity {
            did: did.clone(),
            count,
        })
        .collect();

    results.sort_by(|a, b| b.count.cmp(&a.count));
    results.truncate(limit);
    results
}

fn get_top_domains(map: &HashMap<String, usize>, limit: usize) -> Vec<DomainCount> {
    let mut results: Vec<_> = map
        .iter()
        .map(|(domain, &count)| DomainCount {
            domain: domain.clone(),
            count,
        })
        .collect();

    results.sort_by(|a, b| b.count.cmp(&a.count));
    results.truncate(limit);
    results
}

fn get_top_endpoints(map: &HashMap<String, usize>, limit: usize) -> Vec<EndpointCount> {
    let mut results: Vec<_> = map
        .iter()
        .map(|(endpoint, &count)| EndpointCount {
            endpoint: endpoint.clone(),
            count,
        })
        .collect();

    results.sort_by(|a, b| b.count.cmp(&a.count));
    results.truncate(limit);
    results
}

fn calculate_time_distribution(
    time_slots: &HashMap<i64, usize>,
    operations: &[Operation],
) -> Option<TimeDistribution> {
    if time_slots.is_empty() || operations.is_empty() {
        return None;
    }

    // Parse timestamps
    let earliest = DateTime::parse_from_rfc3339(&operations[0].created_at).ok()?;
    let latest = DateTime::parse_from_rfc3339(&operations[operations.len() - 1].created_at).ok()?;

    // Group by hour
    let mut hourly_slots: HashMap<i64, usize> = HashMap::new();
    for (&slot, &count) in time_slots {
        let hour = (slot / 60) * 60; // Truncate to hour
        *hourly_slots.entry(hour).or_insert(0) += count;
    }

    // Find peak hour
    let (peak_hour, peak_count) = hourly_slots
        .iter()
        .max_by_key(|&(_, count)| count)
        .map(|(&hour, &count)| (hour, count))
        .unwrap_or((0, 0));

    let duration = latest.signed_duration_since(earliest);

    Some(TimeDistribution {
        earliest_op: operations[0].created_at.clone(),
        latest_op: operations[operations.len() - 1].created_at.clone(),
        time_span: format_duration_verbose(duration),
        peak_hour: chrono::DateTime::from_timestamp(peak_hour * 60, 0)
            .unwrap()
            .format("%Y-%m-%d %H:%M")
            .to_string(),
        peak_hour_ops: peak_count,
        total_hours: hourly_slots.len(),
    })
}

fn display_human(
    result: &InspectResult,
    operations: &[Operation],
    cmd: &InspectCommand,
    _bundle_num: Option<u32>,
    _manager: &BundleManager,
) -> Result<()> {
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("                    Bundle Deep Inspection");
    println!("═══════════════════════════════════════════════════════════════\n");

    // File info
    println!("📁 File Information");
    println!("───────────────────");
    println!("  Path:                {}", result.file_path);
    println!("  Has metadata frame:  {}", result.has_metadata_frame);

    // Show size information from index if available
    if let Some(ref index_meta) = result.index_metadata {
        println!("\n  Size:");
        println!("    File size:         {}", format_bytes(result.file_size));
        println!(
            "    Uncompressed:      {}",
            format_bytes(index_meta.uncompressed_size)
        );
        println!(
            "    Compressed:        {}",
            format_bytes(index_meta.compressed_size)
        );
        println!(
            "    Compression:       {:.1}%",
            index_meta.compression_ratio
        );
        println!("    Compressed hash:   {}", index_meta.compressed_hash);
    } else {
        println!("  File size:           {}", format_bytes(result.file_size));
    }
    println!();

    // Embedded metadata (if available and not skipped)
    if !cmd.skip_metadata && result.has_metadata_frame {
        if let Some(ref meta) = result.embedded_metadata {
            println!("📋 Embedded Metadata (Skippable Frame)");
            println!("───────────────────────────────────────");
            println!("  Format:              {}", meta.format);
            println!("  Origin:              {}", meta.origin);
            println!("  Bundle Number:       {}", meta.bundle_number);

            if !meta.created_by.is_empty() {
                println!("  Created by:          {}", meta.created_by);
            }
            println!("  Created at:          {}", meta.created_at);

            println!("\n  Content:");
            println!(
                "    Operations:        {}",
                format_number(meta.operation_count)
            );
            println!("    Unique DIDs:       {}", format_number(meta.did_count));
            println!(
                "    Frames:            {} × {} ops",
                meta.frame_count,
                format_number(meta.frame_size)
            );
            println!(
                "    Timespan:          {} → {}",
                meta.start_time, meta.end_time
            );

            let duration = if let (Ok(start), Ok(end)) = (
                DateTime::parse_from_rfc3339(&meta.start_time),
                DateTime::parse_from_rfc3339(&meta.end_time),
            ) {
                end.signed_duration_since(start)
            } else {
                chrono::Duration::seconds(0)
            };
            println!(
                "    Duration:          {}",
                format_duration_verbose(duration)
            );

            println!("\n  Integrity:");
            println!("    Content hash:      {}", meta.content_hash);
            if let Some(ref parent) = meta.parent_hash {
                if !parent.is_empty() {
                    println!("    Parent hash:       {}", parent);
                }
            }

            // Index metadata for chain info
            if let Some(ref index_meta) = result.index_metadata {
                println!("\n  Chain:");
                println!("    Chain hash:        {}", index_meta.hash);
                if !index_meta.parent.is_empty() {
                    println!("    Parent:            {}", index_meta.parent);
                }
                if !index_meta.cursor.is_empty() {
                    println!("    Cursor:            {}", index_meta.cursor);
                }
            }

            if !meta.frame_offsets.is_empty() {
                println!("\n  Frame Index:");
                println!("    {} frame offsets (embedded)", meta.frame_offsets.len());

                if let Some(metadata_size) = meta.metadata_frame_size {
                    println!("    Metadata size:     {}", format_bytes(metadata_size));
                }

                // Show compact list of first few offsets
                if meta.frame_offsets.len() <= 10 {
                    println!("    Offsets:           {:?}", meta.frame_offsets);
                } else {
                    println!(
                        "    First offsets:     {:?} ... ({} more)",
                        &meta.frame_offsets[..5],
                        meta.frame_offsets.len() - 5
                    );
                }
            }

            println!();
        }
    }

    // Operations breakdown
    println!("📊 Operations Analysis");
    println!("──────────────────────");
    println!("  Total operations:    {}", format_number(result.total_ops));
    println!(
        "  Active:              {} ({:.1}%)",
        format_number(result.active_ops),
        (result.active_ops as f64 / result.total_ops as f64 * 100.0)
    );
    if result.nullified_ops > 0 {
        println!(
            "  Nullified:           {} ({:.1}%)",
            format_number(result.nullified_ops),
            (result.nullified_ops as f64 / result.total_ops as f64 * 100.0)
        );
    }

    if !result.operation_types.is_empty() {
        println!("\n  Operation Types:");
        let mut types: Vec<_> = result.operation_types.iter().collect();
        types.sort_by(|a, b| b.1.cmp(a.1));
        for (op_type, count) in types {
            let pct = *count as f64 / result.total_ops as f64 * 100.0;
            println!(
                "    {:<25} {} ({:.1}%)",
                op_type,
                format_number(*count),
                pct
            );
        }
    }
    println!();

    // DID patterns
    println!("👤 DID Activity Patterns");
    println!("────────────────────────");
    println!(
        "  Unique DIDs:         {}",
        format_number(result.unique_dids)
    );
    println!(
        "  Single-op DIDs:      {} ({:.1}%)",
        format_number(result.single_op_dids),
        (result.single_op_dids as f64 / result.unique_dids as f64 * 100.0)
    );
    println!(
        "  Multi-op DIDs:       {} ({:.1}%)",
        format_number(result.multi_op_dids),
        (result.multi_op_dids as f64 / result.unique_dids as f64 * 100.0)
    );

    if !result.top_dids.is_empty() {
        println!("\n  Most Active DIDs:");
        for (i, da) in result.top_dids.iter().enumerate().take(5) {
            println!("    {}. {} ({} ops)", i + 1, da.did, da.count);
        }
    }
    println!();

    // Handle patterns
    if let Some(total_handles) = result.total_handles {
        println!("🏷️  Handle Statistics");
        println!("────────────────────");
        println!("  Total handles:       {}", format_number(total_handles));
        if let Some(invalid) = result.invalid_handles {
            if invalid > 0 {
                println!(
                    "  Invalid patterns:    {} ({:.1}%)",
                    format_number(invalid),
                    (invalid as f64 / total_handles as f64 * 100.0)
                );
            }
        }

        if !result.top_domains.is_empty() {
            println!("\n  Top Domains:");
            for dc in &result.top_domains {
                let pct = dc.count as f64 / total_handles as f64 * 100.0;
                println!(
                    "    {:<25} {} ({:.1}%)",
                    dc.domain,
                    format_number(dc.count),
                    pct
                );
            }
        }
        println!();
    }

    // Service patterns
    if let Some(total_services) = result.total_services {
        println!("🌐 Service Endpoints");
        println!("────────────────────");
        println!("  Total services:      {}", format_number(total_services));
        if let Some(unique) = result.unique_endpoints {
            println!("  Unique endpoints:    {}", format_number(unique));
        }

        if !result.top_pds_endpoints.is_empty() {
            println!("\n  Top PDS Endpoints:");
            for ec in &result.top_pds_endpoints {
                println!("    {:<40} {} ops", ec.endpoint, format_number(ec.count));
            }
        }
        println!();
    }

    // Temporal analysis
    println!("⏱️  Time Distribution");
    println!("───────────────────────");
    if let Some(ref td) = result.time_distribution {
        println!("  Earliest operation:  {}", td.earliest_op);
        println!("  Latest operation:    {}", td.latest_op);
        println!("  Time span:           {}", td.time_span);
        println!(
            "  Peak hour:           {} ({} ops)",
            td.peak_hour, td.peak_hour_ops
        );
        println!("  Total active hours:  {}", td.total_hours);
        println!("  Avg ops/minute:      {:.1}", result.avg_ops_per_minute);
    }
    println!();

    // Size analysis
    println!("📏 Size Analysis");
    println!("────────────────");
    println!(
        "  Total data:          {}",
        format_bytes(result.total_op_size)
    );
    println!(
        "  Average per op:      {}",
        format_bytes(result.avg_op_size as u64)
    );
    println!(
        "  Min operation:       {}",
        format_bytes(result.min_op_size as u64)
    );
    println!(
        "  Max operation:       {}\n",
        format_bytes(result.max_op_size as u64)
    );

    // Sample operations
    if cmd.samples && !operations.is_empty() {
        println!(
            "📝 Sample Operations (first {})",
            cmd.sample_count.min(operations.len())
        );
        println!("────────────────────────────────");
        for (i, op) in operations.iter().enumerate().take(cmd.sample_count) {
            println!(
                "  [{:04}] {}",
                i,
                op.cid.as_ref().unwrap_or(&"<no-cid>".to_string())
            );
            println!("         DID: {}", op.did);
            println!("         Time: {}", op.created_at);
            if op.nullified {
                println!("         Nullified: true");
            }
        }
        println!();
    }

    Ok(())
}
