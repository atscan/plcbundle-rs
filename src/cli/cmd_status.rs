use anyhow::Result;
use clap::Parser;
use plcbundle::*;
use std::path::{Path, PathBuf};

use super::utils;

#[derive(Parser)]
#[command(
    about = "Show comprehensive repository status",
    long_about = "Display a comprehensive overview of the repository's current state, including
bundle statistics, DID index status, mempool state, and actionable health
recommendations.

This command provides a quick way to understand your repository's health and
configuration. It shows total bundles, operations, unique DIDs, storage usage,
compression ratios, and the status of optional components like the DID index
and mempool.

Use --detailed to see additional information like recent bundles and more
comprehensive statistics. Use --json for machine-readable output suitable for
monitoring systems or automated health checks.

The command automatically identifies potential issues (like missing DID index)
and suggests next steps to improve repository functionality. This makes it an
excellent starting point for understanding repository state and planning
maintenance tasks.",
    alias = "info",
    help_template = crate::clap_help!(
        examples: "  # Show repository status\n  \
                   {bin} status\n\n  \
                   # Show detailed status with recent bundles\n  \
                   {bin} status --detailed\n\n  \
                   # JSON output for scripting\n  \
                   {bin} status --json\n\n  \
                   # Using legacy 'info' alias\n  \
                   {bin} info"
    )
)]
pub struct StatusCommand {
    /// Show detailed information
    #[arg(short, long)]
    pub detailed: bool,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub fn run(cmd: StatusCommand, dir: PathBuf) -> Result<()> {
    let manager = utils::create_manager(dir.clone(), false, false, false)?;
    let index = manager.get_index();

    if cmd.json {
        print_json_status(&manager, &index, &dir)?;
    } else {
        print_human_status(&manager, &index, &dir, cmd.detailed)?;
    }

    Ok(())
}

fn print_human_status(
    manager: &BundleManager,
    index: &Index,
    dir: &Path,
    detailed: bool
) -> Result<()> {
    let dir_display = utils::display_path(dir);

    // Header
    println!("📦 PLC Bundle Repository Status");
    println!("═══════════════════════════════════════════════════════════════");
    println!();

    // Repository Information
    println!("📁 Repository");
    println!("───────────────────────────────────────────────────────────────");
    println!("  Directory:       {}", dir_display.display());
    println!("  Version:         {}", index.version);
    println!("  Origin:          {}", index.origin);
    println!("  Last Updated:    {}", index.updated_at);
    println!();

    // Bundle Statistics
    println!("📊 Bundle Statistics");
    println!("───────────────────────────────────────────────────────────────");

    let is_empty = utils::is_repository_empty(manager);

    if is_empty {
        println!("  Status:          Empty (no bundles)");
        println!();
    } else {
        let total_bundles = index.last_bundle;
        let total_operations = plcbundle::total_operations_from_bundles(total_bundles);

        println!("  Total Bundles:   {}", utils::format_number(total_bundles as u64));
        println!("  Operations:      ~{}", utils::format_number(total_operations));
        println!("  Unique DIDs:     {}", utils::format_number(calculate_total_dids(index) as u64));
        println!();

        // Storage
        println!("  Compressed:      {}", utils::format_bytes(index.total_size_bytes));
        println!("  Uncompressed:    {}", utils::format_bytes(index.total_uncompressed_size_bytes));

        let compression_ratio = if index.total_uncompressed_size_bytes > 0 {
            (1.0 - index.total_size_bytes as f64 / index.total_uncompressed_size_bytes as f64) * 100.0
        } else {
            0.0
        };
        println!("  Compression:     {:.1}%", compression_ratio);
        println!();

        // Recent bundles
        if detailed {
            let recent_count = 5;
            let start_idx = index.bundles.len().saturating_sub(recent_count);
            let recent_bundles = &index.bundles[start_idx..];

            println!("  Recent Bundles:");
            for meta in recent_bundles {
                println!("    #{:<6} {} → {} ({} ops, {} DIDs)",
                    meta.bundle_number,
                    &meta.start_time[..19], // Just date and time without timezone
                    &meta.end_time[..19],
                    utils::format_number(meta.operation_count as u64),
                    utils::format_number(meta.did_count as u64)
                );
            }
            println!();
        }
    }

    // DID Index Status
    println!("🔍 DID Index");
    println!("───────────────────────────────────────────────────────────────");

    let did_index_exists = check_did_index_exists(dir);

    if did_index_exists {
        println!("  Status:          ✓ Built");

        let stats = manager.get_did_index_stats_struct();
        if stats.total_dids > 0 {
            println!("  Indexed DIDs:    {}", utils::format_number(stats.total_dids as u64));
        }
    } else {
        println!("  Status:          ✗ Not built");
        println!("  Hint:            Run 'plcbundle index build' to create DID index");
    }
    println!();

    // Mempool Status
    println!("⏳ Mempool");
    println!("───────────────────────────────────────────────────────────────");

    if let Ok(mempool_stats) = manager.get_mempool_stats() {
        if mempool_stats.count > 0 {
            println!("  Operations:      {}", utils::format_number(mempool_stats.count as u64));
            println!("  Target Bundle:   #{}", mempool_stats.target_bundle);
            println!("  Can Bundle:      {}", if mempool_stats.can_create_bundle { "Yes" } else { "No" });

            if let Some(first) = mempool_stats.first_time {
                println!("  First Op:        {}", first);
            }
            if let Some(last) = mempool_stats.last_time {
                println!("  Last Op:         {}", last);
            }
        } else {
            println!("  Status:          Empty");
        }
    } else {
        println!("  Status:          Not initialized");
    }
    println!();

    // Health & Recommendations
    if !is_empty {
        println!("💡 Status & Recommendations");
        println!("───────────────────────────────────────────────────────────────");

        let mut suggestions = Vec::new();
        let mut hints = Vec::new();

        if !did_index_exists {
            suggestions.push("Build DID index for fast lookups: plcbundle index build");
        }

        if let Ok(mempool_stats) = manager.get_mempool_stats()
            && mempool_stats.count >= constants::BUNDLE_SIZE {
                suggestions.push("Mempool ready to create new bundle: plcbundle sync");
        }

        // Add general hints
        hints.push("Verify integrity: plcbundle verify --chain");
        if did_index_exists {
            hints.push("Verify DID index: plcbundle index verify");
        }
        hints.push("List bundles: plcbundle ls");

        if suggestions.is_empty() {
            println!("  ✓ Repository is healthy");
            println!();
            if detailed {
                println!("  Useful commands:");
                for hint in hints {
                    println!("    • {}", hint);
                }
            }
        } else {
            println!("  Suggestions:");
            for suggestion in suggestions {
                println!("    • {}", suggestion);
            }
            println!();
            if detailed {
                println!("  Useful commands:");
                for hint in hints {
                    println!("    • {}", hint);
                }
            }
        }
        println!();
    }

    Ok(())
}

fn print_json_status(
    manager: &BundleManager,
    index: &Index,
    dir: &Path,
) -> Result<()> {
    use serde_json::json;

    let dir_display = utils::display_path(dir);
    let is_empty = utils::is_repository_empty(manager);

    let mut status = json!({
        "repository": {
            "directory": dir_display.display().to_string(),
            "version": index.version,
            "origin": index.origin,
            "last_updated": index.updated_at,
        },
        "bundles": {
            "count": index.last_bundle,
            "total_size_bytes": index.total_size_bytes,
            "total_uncompressed_size_bytes": index.total_uncompressed_size_bytes,
            "compression_ratio": if index.total_uncompressed_size_bytes > 0 {
                (1.0 - index.total_size_bytes as f64 / index.total_uncompressed_size_bytes as f64) * 100.0
            } else {
                0.0
            },
        }
    });

    // DID Index
    let did_index_exists = check_did_index_exists(dir);
    let mut did_index_info = json!({
        "exists": did_index_exists,
    });

    if did_index_exists {
        let stats = manager.get_did_index_stats_struct();
        if stats.total_dids > 0 {
            did_index_info["indexed_dids"] = json!(stats.total_dids);
        }
    }
    status["did_index"] = did_index_info;

    // Mempool
    let mut mempool_info = json!({});
    if let Ok(mempool_stats) = manager.get_mempool_stats() {
        mempool_info = json!({
            "count": mempool_stats.count,
            "target_bundle": mempool_stats.target_bundle,
            "can_create_bundle": mempool_stats.can_create_bundle,
            "validated": mempool_stats.validated,
            "first_time": mempool_stats.first_time.map(|t| t.to_rfc3339()),
            "last_time": mempool_stats.last_time.map(|t| t.to_rfc3339()),
        });
    }
    status["mempool"] = mempool_info;

    // Health
    let mut health = Vec::new();
    if !is_empty && !did_index_exists {
        health.push("did_index_not_built");
    }
    if let Ok(mempool_stats) = manager.get_mempool_stats()
        && mempool_stats.count >= constants::BUNDLE_SIZE {
            health.push("mempool_ready_to_bundle");
    }
    status["health"] = json!(health);

    println!("{}", sonic_rs::to_string_pretty(&status)?);

    Ok(())
}

fn check_did_index_exists(dir: &Path) -> bool {
    let did_index_dir = dir.join(constants::DID_INDEX_DIR).join(constants::DID_INDEX_SHARDS);
    did_index_dir.exists() && did_index_dir.is_dir()
}

fn calculate_total_dids(index: &Index) -> u32 {
    index.bundles.iter().map(|b| b.did_count).sum()
}
