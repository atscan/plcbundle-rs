// Inspect command - deep analysis of bundle contents
use anyhow::{Result, Context};
use clap::Args;
use plcbundle::{BundleManager, LoadOptions, Operation};
use std::path::PathBuf;
use std::collections::HashMap;
use serde::Serialize;
use serde_json::Value;
use chrono::DateTime;

#[derive(Args)]
#[command(
    about = "Deep analysis of bundle contents",
    long_about = "Performs comprehensive analysis of a bundle including:\n  \
                  • Embedded metadata (from skippable frame)\n  \
                  • Operation type breakdown\n  \
                  • DID activity patterns\n  \
                  • Handle and domain statistics\n  \
                  • Service endpoint analysis\n  \
                  • Temporal distribution\n  \
                  • Size analysis\n\n\
                  Can inspect either by bundle number (from repository) or direct file path."
)]
pub struct InspectCommand {
    /// Bundle number or file path to inspect
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
    let manager = BundleManager::new(dir.clone())?;

    // Resolve target to bundle number or file path
    let (bundle_num, file_path) = resolve_target(&cmd.target, &dir)?;

    // Get file metadata
    let metadata = std::fs::metadata(&file_path)
        .context(format!("Failed to read file: {}", file_path.display()))?;
    let file_size = metadata.len();

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

    // Check for metadata frame
    let has_metadata = if let Some(num) = bundle_num {
        manager.get_bundle_metadata(num)?.is_some()
    } else {
        false
    };

    let result = InspectResult {
        file_path: file_path.display().to_string(),
        file_size,
        has_metadata_frame: has_metadata,
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
        println!("{}", serde_json::to_string_pretty(&result)?);
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
        if let Value::Object(ref map) = op.operation {
            // Operation type
            if let Some(Value::String(op_type)) = map.get("type") {
                *operation_types.entry(op_type.clone()).or_insert(0) += 1;
            }

            // Pattern analysis (if not skipped)
            if !cmd.skip_patterns {
                // Handle analysis
                if let Some(Value::Array(aka)) = map.get("alsoKnownAs") {
                    for item in aka {
                        if let Value::String(aka_str) = item {
                            if aka_str.starts_with("at://") {
                                total_handles += 1;

                                // Extract domain
                                let handle = aka_str.strip_prefix("at://").unwrap_or("");
                                let handle = handle.split('/').next().unwrap_or("");

                                // Count domain (TLD)
                                let parts: Vec<&str> = handle.split('.').collect();
                                if parts.len() >= 2 {
                                    let domain = format!("{}.{}",
                                        parts[parts.len() - 2],
                                        parts[parts.len() - 1]);
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
                if let Some(Value::Object(services)) = map.get("services") {
                    total_services += services.len();

                    // Extract PDS endpoints
                    if let Some(Value::Object(pds)) = services.get("atproto_pds") {
                        if let Some(Value::String(endpoint)) = pds.get("endpoint") {
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
        total_handles: if cmd.skip_patterns { None } else { Some(total_handles) },
        top_domains,
        invalid_handles: if cmd.skip_patterns { None } else { Some(invalid_handles) },
        total_services: if cmd.skip_patterns { None } else { Some(total_services) },
        unique_endpoints: if cmd.skip_patterns { None } else { Some(endpoint_counts.len()) },
        top_pds_endpoints,
        time_distribution,
        avg_ops_per_minute,
        avg_op_size,
        min_op_size: if min_op_size == usize::MAX { 0 } else { min_op_size },
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
        time_span: format_duration(duration.num_seconds()),
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
    bundle_num: Option<u32>,
    manager: &BundleManager,
) -> Result<()> {
    println!();
    println!("═══════════════════════════════════════════════════════════════");
    println!("                    Bundle Deep Inspection");
    println!("═══════════════════════════════════════════════════════════════\n");

    // File info
    println!("📁 File Information");
    println!("───────────────────");
    println!("  Path:                {}", result.file_path);
    println!("  Size:                {}", format_bytes(result.file_size));
    println!("  Has metadata frame:  {}\n", result.has_metadata_frame);

    // Embedded metadata (if available and not skipped)
    if !cmd.skip_metadata && result.has_metadata_frame {
        if let Some(num) = bundle_num {
            if let Some(meta) = manager.get_bundle_metadata(num)? {
                println!("📋 Embedded Metadata");
                println!("────────────────────");
                println!("  Bundle Number:       {:06}", meta.bundle_number);
                println!("  Operations:          {}", format_number(meta.operation_count as usize));
                println!("  Unique DIDs:         {}", format_number(meta.did_count as usize));
                println!("  Timespan:            {} → {}", meta.start_time, meta.end_time);
                println!("  Content hash:        {}", &meta.content_hash);
                println!();
            }
        }
    }

    // Operations breakdown
    println!("📊 Operations Analysis");
    println!("──────────────────────");
    println!("  Total operations:    {}", format_number(result.total_ops));
    println!("  Active:              {} ({:.1}%)",
        format_number(result.active_ops),
        (result.active_ops as f64 / result.total_ops as f64 * 100.0));
    if result.nullified_ops > 0 {
        println!("  Nullified:           {} ({:.1}%)",
            format_number(result.nullified_ops),
            (result.nullified_ops as f64 / result.total_ops as f64 * 100.0));
    }

    if !result.operation_types.is_empty() {
        println!("\n  Operation Types:");
        let mut types: Vec<_> = result.operation_types.iter().collect();
        types.sort_by(|a, b| b.1.cmp(a.1));
        for (op_type, count) in types {
            let pct = *count as f64 / result.total_ops as f64 * 100.0;
            println!("    {:<25} {} ({:.1}%)", op_type, format_number(*count), pct);
        }
    }
    println!();

    // DID patterns
    println!("👤 DID Activity Patterns");
    println!("────────────────────────");
    println!("  Unique DIDs:         {}", format_number(result.unique_dids));
    println!("  Single-op DIDs:      {} ({:.1}%)",
        format_number(result.single_op_dids),
        (result.single_op_dids as f64 / result.unique_dids as f64 * 100.0));
    println!("  Multi-op DIDs:       {} ({:.1}%)",
        format_number(result.multi_op_dids),
        (result.multi_op_dids as f64 / result.unique_dids as f64 * 100.0));

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
                println!("  Invalid patterns:    {} ({:.1}%)",
                    format_number(invalid),
                    (invalid as f64 / total_handles as f64 * 100.0));
            }
        }

        if !result.top_domains.is_empty() {
            println!("\n  Top Domains:");
            for dc in &result.top_domains {
                let pct = dc.count as f64 / total_handles as f64 * 100.0;
                println!("    {:<25} {} ({:.1}%)", dc.domain, format_number(dc.count), pct);
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
        println!("  Peak hour:           {} ({} ops)", td.peak_hour, td.peak_hour_ops);
        println!("  Total active hours:  {}", td.total_hours);
        println!("  Avg ops/minute:      {:.1}", result.avg_ops_per_minute);
    }
    println!();

    // Size analysis
    println!("📏 Size Analysis");
    println!("────────────────");
    println!("  Total data:          {}", format_bytes(result.total_op_size));
    println!("  Average per op:      {}", format_bytes(result.avg_op_size as u64));
    println!("  Min operation:       {}", format_bytes(result.min_op_size as u64));
    println!("  Max operation:       {}\n", format_bytes(result.max_op_size as u64));

    // Sample operations
    if cmd.samples && !operations.is_empty() {
        println!("📝 Sample Operations (first {})", cmd.sample_count.min(operations.len()));
        println!("────────────────────────────────");
        for (i, op) in operations.iter().enumerate().take(cmd.sample_count) {
            println!("  [{:04}] {}", i, op.cid.as_ref().unwrap_or(&"<no-cid>".to_string()));
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

fn resolve_target(target: &str, dir: &PathBuf) -> Result<(Option<u32>, PathBuf)> {
    // Try to parse as bundle number
    if let Ok(num) = target.parse::<u32>() {
        let path = dir.join(format!("{:06}.jsonl.zst", num));
        if path.exists() {
            return Ok((Some(num), path));
        } else {
            anyhow::bail!("Bundle {:06} not found in repository", num);
        }
    }

    // Otherwise treat as file path
    let path = PathBuf::from(target);
    if path.exists() {
        Ok((None, path))
    } else {
        anyhow::bail!("File not found: {}", target)
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let chars: Vec<_> = s.chars().collect();
    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }
    result
}

fn format_duration(seconds: i64) -> String {
    let days = seconds / 86400;
    let hours = (seconds % 86400) / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    if days > 0 {
        format!("{}d {}h {}m {}s", days, hours, minutes, secs)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, secs)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}
