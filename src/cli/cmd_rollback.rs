use super::utils;
use anyhow::{Result, bail};
use clap::Args;
use plcbundle::BundleManager;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Args)]
#[command(
    about = "Rollback repository to earlier state",
    long_about = "Remove bundles from the end of the chain to restore the repository to an
earlier state. This is useful for undoing recent syncs, removing corrupted
bundles, or reverting to a known-good state.

You can rollback to a specific bundle number using --to (keeps that bundle
and all earlier ones), or remove the last N bundles using --last. The command
shows a detailed plan before execution, including which bundles will be deleted,
how much data will be removed, and what additional impacts there are (like
mempool clearing or DID index invalidation).

By default, bundle files are permanently deleted. Use --keep-files to update
the index only while leaving bundle files on disk. Use --rebuild-did-index
to automatically rebuild the DID index after rollback.

This operation cannot be undone, so use with caution. The command requires
explicit confirmation by typing 'rollback' unless --force is used.",
    help_template = crate::clap_help!(
        examples: "  # Rollback TO bundle 100 (keeps 1-100, removes 101+)\n  \
                   {bin} rollback --to 100\n\n  \
                   # Remove last 5 bundles\n  \
                   {bin} rollback --last 5\n\n  \
                   # Rollback without confirmation\n  \
                   {bin} rollback --to 50 -f\n\n  \
                   # Rollback and rebuild DID index\n  \
                   {bin} rollback --to 100 --rebuild-did-index\n\n  \
                   # Rollback but keep bundle files (index-only)\n  \
                   {bin} rollback --to 100 --keep-files"
    )
)]
pub struct RollbackCommand {
    /// Rollback TO this bundle (keeps it)
    #[arg(long)]
    pub to: Option<u32>,

    /// Rollback last N bundles
    #[arg(long)]
    pub last: Option<u32>,

    /// Skip confirmation prompt
    #[arg(short, long)]
    pub force: bool,

    /// Rebuild DID index after rollback
    #[arg(long)]
    pub rebuild_did_index: bool,

    /// Update index only (don't delete bundle files)
    #[arg(long)]
    pub keep_files: bool,
}

#[derive(Debug)]
struct RollbackPlan {
    target_bundle: u32,
    bundles_to_keep: usize,
    bundles_to_delete: Vec<u32>,
    deleted_ops: usize,
    deleted_size: u64,
    has_mempool: bool,
    has_did_index: bool,
    affected_period: Option<(String, String)>,
}

pub fn run(cmd: RollbackCommand, dir: PathBuf, global_verbose: bool) -> Result<()> {
    // Step 1: Validate options and calculate plan
    let mut manager = super::utils::create_manager(dir.clone(), global_verbose, false, false)?;
    let plan = calculate_rollback_plan(&manager, &cmd)?;

    // Step 2: Display plan and get confirmation
    display_rollback_plan(&dir, &plan)?;

    if !cmd.force {
        if !confirm_rollback(cmd.keep_files)? {
            println!("Cancelled");
            return Ok(());
        }
        println!();
    }

    // Step 3: Execute rollback
    perform_rollback(&mut manager, &dir, &plan, &cmd, global_verbose)?;

    // Step 4: Display success summary
    display_rollback_success(&plan, &cmd)?;

    Ok(())
}

fn calculate_rollback_plan(manager: &BundleManager, cmd: &RollbackCommand) -> Result<RollbackPlan> {
    // Validate options
    if cmd.to.is_none() && cmd.last.is_none() {
        bail!("either --to or --last must be specified");
    }

    if cmd.to.is_some() && cmd.last.is_some() {
        bail!("cannot use both --to and --last together");
    }

    if super::utils::is_repository_empty(manager) {
        bail!("no bundles to rollback");
    }

    let last_bundle = manager.get_last_bundle();

    // Calculate target bundle
    let target_bundle = if let Some(to) = cmd.to {
        to
    } else if let Some(last) = cmd.last {
        if last >= last_bundle {
            bail!(
                "cannot rollback {} bundles, only {} exist",
                last,
                last_bundle
            );
        }
        let calculated = last_bundle - last;

        // Prevent accidental deletion of all bundles via --last
        if calculated == 0 {
            bail!("invalid rollback: would delete all bundles (use --to 0 explicitly if intended)");
        }

        calculated
    } else {
        unreachable!()
    };

    // Validate target
    if target_bundle >= last_bundle {
        bail!("already at bundle {} (nothing to rollback)", last_bundle);
    }

    // Build list of bundles to delete
    let bundles_to_delete: Vec<u32> = (target_bundle + 1..=last_bundle).collect();

    if bundles_to_delete.is_empty() {
        bail!("already at bundle {} (nothing to rollback)", target_bundle);
    }

    // Calculate statistics
    let mut deleted_ops = 0;
    let mut deleted_size = 0u64;
    let mut start_time = None;
    let mut end_time = None;

    for bundle_num in &bundles_to_delete {
        if let Some(meta) = manager.get_bundle_metadata(*bundle_num)? {
            deleted_ops += meta.operation_count as usize;
            deleted_size += meta.compressed_size;

            if start_time.is_none() {
                start_time = Some(meta.start_time.clone());
            }
            end_time = Some(meta.end_time.clone());
        }
    }

    let affected_period = if let (Some(start), Some(end)) = (start_time, end_time) {
        Some((start, end))
    } else {
        None
    };

    // Check mempool and DID index
    let mempool_stats = manager.get_mempool_stats()?;
    let has_mempool = mempool_stats.count > 0;

    let did_stats = manager.get_did_index_stats();
    let has_did_index = did_stats
        .get("total_dids")
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        > 0;

    Ok(RollbackPlan {
        target_bundle,
        bundles_to_keep: target_bundle as usize,
        bundles_to_delete,
        deleted_ops,
        deleted_size,
        has_mempool,
        has_did_index,
        affected_period,
    })
}

fn display_rollback_plan(dir: &Path, plan: &RollbackPlan) -> Result<()> {
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                      ROLLBACK PLAN                             ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    println!("📁 Repository");
    println!("   Directory:       {}", utils::display_path(dir).display());

    let current_bundles = plan.bundles_to_keep + plan.bundles_to_delete.len();
    if current_bundles > 0 {
        let last = plan.bundles_to_delete.last().unwrap();
        println!(
            "   Current state:   {} bundles ({} → {})",
            current_bundles, 1, last
        );
    }
    println!("   Target:          bundle {}\n", plan.target_bundle);

    println!("🗑️  Will Delete");
    println!("   Bundles:         {}", plan.bundles_to_delete.len());
    println!(
        "   Operations:      {}",
        super::utils::format_number(plan.deleted_ops as u64)
    );
    println!(
        "   Data size:       {}",
        super::utils::format_bytes(plan.deleted_size)
    );

    if let Some((start, end)) = &plan.affected_period {
        let start_dt = chrono::DateTime::parse_from_rfc3339(start)
            .unwrap_or_else(|_| chrono::Utc::now().into());
        let end_dt =
            chrono::DateTime::parse_from_rfc3339(end).unwrap_or_else(|_| chrono::Utc::now().into());
        println!(
            "   Time period:     {} to {}",
            start_dt.format("%Y-%m-%d %H:%M"),
            end_dt.format("%Y-%m-%d %H:%M")
        );
    }
    println!();

    // Show sample of deleted bundles
    if !plan.bundles_to_delete.is_empty() {
        println!("   Bundles to delete:");
        let display_count = std::cmp::min(10, plan.bundles_to_delete.len());
        for &bundle_num in &plan.bundles_to_delete[..display_count] {
            println!("   • {}", bundle_num);
        }
        if plan.bundles_to_delete.len() > display_count {
            println!(
                "   ... and {} more",
                plan.bundles_to_delete.len() - display_count
            );
        }
        println!();
    }

    // Show impacts
    println!("⚠️  Additional Impacts");
    if plan.has_mempool {
        println!("   • Mempool will be cleared");
    }
    if plan.has_did_index {
        println!("   • DID index will need rebuilding");
    }
    if plan.bundles_to_keep == 0 {
        println!("   • Repository will be EMPTY after rollback");
    }
    println!();

    Ok(())
}

fn confirm_rollback(keep_files: bool) -> Result<bool> {
    if keep_files {
        print!("Type 'rollback-index' to confirm (index-only mode): ");
    } else {
        println!("⚠️  This will permanently DELETE data!");
        print!("Type 'rollback' to confirm: ");
    }

    io::stdout().flush()?;

    let mut response = String::new();
    io::stdin().read_line(&mut response)?;

    let expected = if keep_files {
        "rollback-index"
    } else {
        "rollback"
    };

    Ok(response.trim() == expected)
}

fn perform_rollback(
    manager: &mut BundleManager,
    _dir: &PathBuf,
    plan: &RollbackPlan,
    cmd: &RollbackCommand,
    _verbose: bool,
) -> Result<()> {
    let total_steps = 4;
    let mut current_step = 0;

    // Step 1: Delete bundle files (or skip if keep_files)
    current_step += 1;
    if !cmd.keep_files {
        println!(
            "[{}/{}] Deleting bundle files...",
            current_step, total_steps
        );
        delete_bundle_files(manager, &plan.bundles_to_delete)?;
        println!("      ✓ Deleted {} file(s)\n", plan.bundles_to_delete.len());
    } else {
        println!(
            "[{}/{}] Skipping file deletion (--keep-files)...",
            current_step, total_steps
        );
        println!("      ℹ Bundle files remain on disk\n");
    }

    // Step 2: Clear mempool
    current_step += 1;
    println!("[{}/{}] Clearing mempool...", current_step, total_steps);
    if plan.has_mempool {
        manager.clear_mempool()?;
        println!("      ✓ Mempool cleared\n");
    } else {
        println!("      (no mempool data)\n");
    }

    // Step 3: Update index
    current_step += 1;
    println!(
        "[{}/{}] Updating bundle index...",
        current_step, total_steps
    );
    manager.rollback_to_bundle(plan.target_bundle)?;
    println!("      ✓ Index updated ({} bundles)\n", plan.bundles_to_keep);

    // Step 4: Handle DID index
    current_step += 1;
    println!("[{}/{}] DID index...", current_step, total_steps);
    handle_did_index(manager, plan, cmd)?;

    Ok(())
}

fn delete_bundle_files(manager: &BundleManager, bundles: &[u32]) -> Result<()> {
    let stats = manager.delete_bundle_files(bundles)?;

    if stats.failed > 0 {
        eprintln!("      ⚠️  Failed to delete {} file(s)", stats.failed);
        bail!("Failed to delete {} bundle files", stats.failed);
    }

    Ok(())
}

fn handle_did_index(
    manager: &mut BundleManager,
    plan: &RollbackPlan,
    cmd: &RollbackCommand,
) -> Result<()> {
    if !plan.has_did_index {
        println!("      (no DID index)");
        return Ok(());
    }

    if cmd.rebuild_did_index {
        println!("      Rebuilding DID index...");

        if plan.bundles_to_keep == 0 {
            println!("      ℹ No bundles to index");
            return Ok(());
        }

        // Use default flush interval for rollback
        let _stats = manager.build_did_index(plcbundle::constants::DID_INDEX_FLUSH_INTERVAL, None::<fn(u32, u32, u64, u64)>, None, None)?;
        println!(
            "      ✓ DID index rebuilt ({} bundles)",
            plan.bundles_to_keep
        );
    } else {
        let did_stats = manager.get_did_index_stats();
        println!("      ⚠️  DID index is out of date");
        if did_stats
            .get("total_entries")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            > 0
        {
            println!(
                "         Run: {} index rebuild",
                plcbundle::constants::BINARY_NAME
            );
        }
    }

    Ok(())
}

fn display_rollback_success(plan: &RollbackPlan, cmd: &RollbackCommand) -> Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                  ROLLBACK COMPLETE                             ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    if plan.bundles_to_keep > 0 {
        println!("📦 New State");
        println!(
            "   Bundles:         {} ({} → {})",
            plan.bundles_to_keep, 1, plan.target_bundle
        );
    } else {
        println!("📦 New State");
        println!("   Repository:      EMPTY (all bundles removed)");
        if cmd.keep_files {
            println!("   Note:            Bundle files remain on disk");
        }
    }

    println!();

    // Show what was removed
    println!("🗑️  Removed");
    println!("   Bundles:         {}", plan.bundles_to_delete.len());
    println!(
        "   Operations:      {}",
        super::utils::format_number(plan.deleted_ops as u64)
    );
    println!(
        "   Data freed:      {}",
        super::utils::format_bytes(plan.deleted_size)
    );

    if cmd.keep_files {
        println!("   Files:           kept on disk");
    }

    println!();

    // Next steps
    if !cmd.rebuild_did_index && plan.has_did_index {
        println!("💡 Next Steps");
        println!("   DID index is out of date. Rebuild with:");
        println!("   {} index rebuild\n", plcbundle::constants::BINARY_NAME);
    }

    Ok(())
}
