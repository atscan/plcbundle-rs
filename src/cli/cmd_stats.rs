use anyhow::Result;
use clap::{Args, ValueEnum};
use plcbundle::BundleManager;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

use super::utils;

#[derive(Args)]
#[command(
    about = "Display statistics about bundles",
    long_about = "Generate comprehensive statistics about bundles, operations, DIDs, and
timeline patterns. Useful for understanding repository composition, analyzing
growth trends, and identifying patterns in the data.

Supports multiple statistic types:
  • summary    - Overall repository metrics (default)
  • operations - Operation type distribution
  • dids       - DID activity patterns
  • timeline   - Temporal distribution and growth rates

Use --stat-type to select which analysis to perform. Statistics can be
computed for specific bundle ranges using --bundles, or for the entire
repository if omitted. Use --json for machine-readable output suitable for
further processing or visualization.

This command provides insights into repository health, data distribution,
and usage patterns that help with capacity planning and optimization."
)]
pub struct StatsCommand {
    /// Bundle range
    #[arg(short, long)]
    pub bundles: Option<String>,

    /// Statistics type
    #[arg(short = 't', long, default_value = "summary")]
    pub stat_type: StatType,

    /// Number of threads to use (0 = auto-detect)
    #[arg(short = 'j', long, default_value = "0")]
    pub threads: usize,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum StatType {
    /// Summary statistics
    Summary,
    /// Operation type distribution
    Operations,
    /// DID statistics
    Dids,
    /// Timeline statistics
    Timeline,
}

pub fn run(cmd: StatsCommand, dir: PathBuf) -> Result<()> {
    let manager = utils::create_manager(dir.clone(), false, false)?;
    let index = manager.get_index();

    if utils::is_repository_empty(&manager) {
        println!("Repository is empty (no bundles)");
        return Ok(());
    }

    let bundle_nums = utils::parse_bundle_spec(cmd.bundles, index.last_bundle)?;

    match cmd.stat_type {
        StatType::Summary => {
            let stats = collect_summary_stats(&manager, &index, &bundle_nums)?;
            print_stats(&stats, cmd.json, StatType::Summary)?;
        }
        StatType::Operations => {
            let stats = collect_operation_stats(&manager, &bundle_nums)?;
            print_stats(&stats, cmd.json, StatType::Operations)?;
        }
        StatType::Dids => {
            let stats = collect_did_stats(&manager, &index, &bundle_nums)?;
            print_stats(&stats, cmd.json, StatType::Dids)?;
        }
        StatType::Timeline => {
            let stats = collect_timeline_stats(&manager, &index, &bundle_nums)?;
            print_stats(&stats, cmd.json, StatType::Timeline)?;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, serde::Serialize)]
struct SummaryStats {
    bundle_count: usize,
    total_operations: u64,
    total_dids: u64,
    total_compressed_size: u64,
    total_uncompressed_size: u64,
    compression_ratio: f64,
    avg_operations_per_bundle: f64,
    avg_dids_per_bundle: f64,
    bundle_range: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct OperationStats {
    total_operations: usize,
    nullified_operations: usize,
    operation_types: HashMap<String, usize>,
    operation_type_percentages: HashMap<String, f64>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct DIDStats {
    total_unique_dids: usize,
    total_did_operations: u64,
    avg_operations_per_did: f64,
    dids_with_single_operation: usize,
    dids_with_multiple_operations: usize,
    max_operations_for_did: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
struct TimelineStats {
    earliest_time: Option<String>,
    latest_time: Option<String>,
    time_span_days: Option<f64>,
    operations_per_day: Option<f64>,
    bundles_per_day: Option<f64>,
    time_distribution: HashMap<String, usize>,
}

fn collect_summary_stats(
    _manager: &BundleManager,
    index: &plcbundle::index::Index,
    bundle_nums: &[u32],
) -> Result<serde_json::Value> {
    let bundle_metadatas: Vec<_> = index
        .bundles
        .iter()
        .filter(|b| bundle_nums.contains(&b.bundle_number))
        .collect();

    let bundle_count = bundle_metadatas.len();
    let total_operations: u64 = bundle_metadatas.iter().map(|b| b.operation_count as u64).sum();
    let total_dids: u64 = bundle_metadatas.iter().map(|b| b.did_count as u64).sum();
    let total_compressed_size: u64 = bundle_metadatas.iter().map(|b| b.compressed_size).sum();
    let total_uncompressed_size: u64 = bundle_metadatas
        .iter()
        .map(|b| b.uncompressed_size)
        .sum();

    let compression_ratio = if total_uncompressed_size > 0 {
        (1.0 - total_compressed_size as f64 / total_uncompressed_size as f64) * 100.0
    } else {
        0.0
    };

    let avg_operations_per_bundle = if bundle_count > 0 {
        total_operations as f64 / bundle_count as f64
    } else {
        0.0
    };

    let avg_dids_per_bundle = if bundle_count > 0 {
        total_dids as f64 / bundle_count as f64
    } else {
        0.0
    };

    let bundle_range = if bundle_nums.len() == 1 {
        format!("{}", bundle_nums[0])
    } else if let (Some(&min), Some(&max)) = (bundle_nums.first(), bundle_nums.last()) {
        if min == max {
            format!("{}", min)
        } else {
            format!("{}-{}", min, max)
        }
    } else {
        "all".to_string()
    };

    let stats = SummaryStats {
        bundle_count,
        total_operations,
        total_dids,
        total_compressed_size,
        total_uncompressed_size,
        compression_ratio,
        avg_operations_per_bundle,
        avg_dids_per_bundle,
        bundle_range,
    };

    Ok(serde_json::to_value(stats)?)
}

fn collect_operation_stats(
    manager: &BundleManager,
    bundle_nums: &[u32],
) -> Result<serde_json::Value> {
    let mut operation_types: HashMap<String, usize> = HashMap::new();
    let mut total_operations = 0;
    let mut nullified_operations = 0;

    if let (Some(&start), Some(&end)) = (bundle_nums.first(), bundle_nums.last()) {
        let iter = manager.get_operations_range(start, end, None);
        for op_result in iter {
            let op = op_result?;
            total_operations += 1;

            if op.nullified {
                nullified_operations += 1;
            }

            if let Value::Object(ref map) = op.operation {
                if let Some(Value::String(op_type)) = map.get("type") {
                    *operation_types.entry(op_type.clone()).or_insert(0) += 1;
                } else {
                    *operation_types.entry("unknown".to_string()).or_insert(0) += 1;
                }
            } else {
                *operation_types.entry("unknown".to_string()).or_insert(0) += 1;
            }
        }
    }

    let mut operation_type_percentages: HashMap<String, f64> = HashMap::new();
    if total_operations > 0 {
        for (op_type, count) in &operation_types {
            let percentage = (*count as f64 / total_operations as f64) * 100.0;
            operation_type_percentages.insert(op_type.clone(), percentage);
        }
    }

    let stats = OperationStats {
        total_operations,
        nullified_operations,
        operation_types,
        operation_type_percentages,
    };

    Ok(serde_json::to_value(stats)?)
}

fn collect_did_stats(
    _manager: &BundleManager,
    index: &plcbundle::index::Index,
    bundle_nums: &[u32],
) -> Result<serde_json::Value> {
    let bundle_metadatas: Vec<_> = index
        .bundles
        .iter()
        .filter(|b| bundle_nums.contains(&b.bundle_number))
        .collect();

    let total_did_operations: u64 = bundle_metadatas.iter().map(|b| b.operation_count as u64).sum();
    let total_unique_dids: usize = bundle_metadatas.iter().map(|b| b.did_count as usize).sum();

    // For more detailed stats, we'd need to iterate operations, but that's expensive
    // So we'll use approximations from metadata
    let avg_operations_per_did = if total_unique_dids > 0 {
        total_did_operations as f64 / total_unique_dids as f64
    } else {
        0.0
    };

    // These would require full iteration, so we'll approximate or skip
    let dids_with_single_operation = 0; // Would need full iteration
    let dids_with_multiple_operations = 0; // Would need full iteration
    let max_operations_for_did = 0; // Would need full iteration

    let stats = DIDStats {
        total_unique_dids,
        total_did_operations,
        avg_operations_per_did,
        dids_with_single_operation,
        dids_with_multiple_operations,
        max_operations_for_did,
    };

    Ok(serde_json::to_value(stats)?)
}

fn collect_timeline_stats(
    _manager: &BundleManager,
    index: &plcbundle::index::Index,
    bundle_nums: &[u32],
) -> Result<serde_json::Value> {
    let bundle_metadatas: Vec<_> = index
        .bundles
        .iter()
        .filter(|b| bundle_nums.contains(&b.bundle_number))
        .collect();

    if bundle_metadatas.is_empty() {
        return Ok(serde_json::json!({
            "earliest_time": null,
            "latest_time": null,
            "time_span_days": null,
            "operations_per_day": null,
            "bundles_per_day": null,
        }));
    }

    let earliest_time = bundle_metadatas
        .iter()
        .map(|b| &b.start_time)
        .min()
        .cloned();
    let latest_time = bundle_metadatas
        .iter()
        .map(|b| &b.end_time)
        .max()
        .cloned();

    let time_span_days = if let (Some(ref earliest), Some(ref latest)) = (earliest_time.as_ref(), latest_time.as_ref()) {
        if let (Ok(e), Ok(l)) = (
            chrono::DateTime::parse_from_rfc3339(earliest),
            chrono::DateTime::parse_from_rfc3339(latest),
        ) {
            let duration = l.signed_duration_since(e);
            Some(duration.num_seconds() as f64 / 86400.0)
        } else {
            None
        }
    } else {
        None
    };

    let total_operations: u64 = bundle_metadatas.iter().map(|b| b.operation_count as u64).sum();
    let operations_per_day = time_span_days.and_then(|days| {
        if days > 0.0 {
            Some(total_operations as f64 / days)
        } else {
            None
        }
    });

    let bundles_per_day = time_span_days.and_then(|days| {
        if days > 0.0 {
            Some(bundle_metadatas.len() as f64 / days)
        } else {
            None
        }
    });

    // Group by date (YYYY-MM-DD)
    let mut time_distribution: HashMap<String, usize> = HashMap::new();
    for meta in &bundle_metadatas {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&meta.start_time) {
            let date_str = dt.format("%Y-%m-%d").to_string();
            *time_distribution.entry(date_str).or_insert(0) += meta.operation_count as usize;
        }
    }

    let stats = TimelineStats {
        earliest_time,
        latest_time,
        time_span_days,
        operations_per_day,
        bundles_per_day,
        time_distribution,
    };

    Ok(serde_json::to_value(stats)?)
}

fn print_stats(stats: &serde_json::Value, json: bool, stat_type: StatType) -> Result<()> {
    if json {
        println!("{}", sonic_rs::to_string_pretty(stats)?);
        Ok(())
    } else {
        print_human_stats(stats, stat_type)
    }
}

fn print_human_stats(stats: &serde_json::Value, stat_type: StatType) -> Result<()> {
    match stat_type {
        StatType::Summary => {
            println!("📊 Summary Statistics");
            println!("═══════════════════════════════════════════════════════════════");
            println!();
            println!("  Bundle Range:        {}", stats["bundle_range"]);
            println!("  Total Bundles:       {}", utils::format_number(stats["bundle_count"].as_u64().unwrap_or(0)));
            println!("  Total Operations:    {}", utils::format_number(stats["total_operations"].as_u64().unwrap_or(0)));
            println!("  Total DIDs:          {}", utils::format_number(stats["total_dids"].as_u64().unwrap_or(0)));
            println!();
            println!("  Storage:");
            println!("    Compressed:        {}", utils::format_bytes(stats["total_compressed_size"].as_u64().unwrap_or(0)));
            println!("    Uncompressed:      {}", utils::format_bytes(stats["total_uncompressed_size"].as_u64().unwrap_or(0)));
            println!("    Compression:      {:.1}%", stats["compression_ratio"].as_f64().unwrap_or(0.0));
            println!();
            println!("  Averages:");
            println!("    Ops per Bundle:    {:.1}", stats["avg_operations_per_bundle"].as_f64().unwrap_or(0.0));
            println!("    DIDs per Bundle:   {:.1}", stats["avg_dids_per_bundle"].as_f64().unwrap_or(0.0));
        }
        StatType::Operations => {
            println!("🔧 Operation Statistics");
            println!("═══════════════════════════════════════════════════════════════");
            println!();
            println!("  Total Operations:    {}", utils::format_number(stats["total_operations"].as_u64().unwrap_or(0)));
            println!("  Nullified:           {}", utils::format_number(stats["nullified_operations"].as_u64().unwrap_or(0)));
            println!();
            
            if let Some(types) = stats.get("operation_types").and_then(|v| v.as_object()) {
                println!("  Operation Types:");
                let mut type_vec: Vec<_> = types.iter().collect();
                type_vec.sort_by(|a, b| {
                    let count_a = a.1.as_u64().unwrap_or(0);
                    let count_b = b.1.as_u64().unwrap_or(0);
                    count_b.cmp(&count_a)
                });

                for (op_type, count_val) in type_vec {
                    let count = count_val.as_u64().unwrap_or(0);
                    let percentage = stats["operation_type_percentages"]
                        .as_object()
                        .and_then(|p| p.get(op_type))
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    println!("    {:<20} {:>10} ({:>5.1}%)", 
                        op_type, 
                        utils::format_number(count),
                        percentage
                    );
                }
            }
        }
        StatType::Dids => {
            println!("🆔 DID Statistics");
            println!("═══════════════════════════════════════════════════════════════");
            println!();
            println!("  Unique DIDs:         {}", utils::format_number(stats["total_unique_dids"].as_u64().unwrap_or(0)));
            println!("  Total Operations:    {}", utils::format_number(stats["total_did_operations"].as_u64().unwrap_or(0)));
            println!("  Avg Ops per DID:     {:.2}", stats["avg_operations_per_did"].as_f64().unwrap_or(0.0));
        }
        StatType::Timeline => {
            println!("⏰ Timeline Statistics");
            println!("═══════════════════════════════════════════════════════════════");
            println!();
            if let Some(earliest) = stats["earliest_time"].as_str() {
                println!("  Earliest:            {}", earliest);
            }
            if let Some(latest) = stats["latest_time"].as_str() {
                println!("  Latest:              {}", latest);
            }
            if let Some(days) = stats["time_span_days"].as_f64() {
                println!("  Time Span:           {:.1} days", days);
            }
            if let Some(ops_per_day) = stats["operations_per_day"].as_f64() {
                println!("  Operations/Day:      {:.1}", ops_per_day);
            }
            if let Some(bundles_per_day) = stats["bundles_per_day"].as_f64() {
                println!("  Bundles/Day:         {:.2}", bundles_per_day);
            }
        }
    }
    println!();
    Ok(())
}

