use anyhow::Result;
use clap::{Args, Subcommand};
use plcbundle::{LoadOptions, constants};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Args)]
#[command(
    about = "Operation queries and inspection",
    long_about = "Direct access to individual operations within bundles using either:\n  • Bundle number + position (e.g., 42 1337)\n  • Global position (e.g., 420000)\n\nGlobal position format: (bundleNumber × 10,000) + position\nExample: 88410345 = bundle 8841, position 345",
    alias = "operation",
    alias = "record",
    after_help = "Examples:\n  \
            # Get operation as JSON\n  \
            plcbundle op get 42 1337\n  \
            plcbundle op get 420000\n\n  \
            # Show operation (formatted)\n  \
            plcbundle op show 42 1337\n  \
            plcbundle op show 88410345\n\n  \
            # Find by CID\n  \
            plcbundle op find bafyreig3..."
)]
pub struct OpCommand {
    #[command(subcommand)]
    pub command: OpCommands,
}

#[derive(Subcommand)]
pub enum OpCommands {
    /// Get operation as JSON (machine-readable)
    ///
    /// Supports two input formats:
    ///   1. Bundle number + position: get 42 1337
    ///   2. Global position: get 420000
    ///
    /// Global position = (bundleNumber × 10,000) + position
    #[command(after_help = "Examples:\n  \
            # By bundle + position\n  \
            plcbundle op get 42 1337\n\n  \
            # By global position\n  \
            plcbundle op get 88410345\n\n  \
            # Pipe to jq\n  \
            plcbundle op get 42 1337 | jq .did")]
    Get {
        /// Bundle number (or global position if only one arg)
        bundle: u32,

        /// Operation position within bundle (optional if using global position)
        position: Option<usize>,
    },

    /// Show operation with formatted output
    ///
    /// Displays operation in human-readable format with:
    ///   • Bundle location and global position
    ///   • DID and CID
    ///   • Timestamp
    ///   • Nullification status
    ///   • Parsed operation details
    ///   • Performance metrics (when not quiet)
    #[command(after_help = "Examples:\n  \
            # By bundle + position\n  \
            plcbundle op show 42 1337\n\n  \
            # By global position\n  \
            plcbundle op show 88410345\n\n  \
            # Quiet mode (minimal output)\n  \
            plcbundle op show 42 1337 -q")]
    Show {
        /// Bundle number (or global position if only one arg)
        bundle: u32,

        /// Operation position within bundle (optional if using global position)
        position: Option<usize>,
    },

    /// Find operation by CID across all bundles
    ///
    /// Searches the entire repository for an operation with the given CID
    /// and returns its location (bundle + position).
    ///
    /// Note: This performs a full scan and can be slow on large repositories.
    #[command(after_help = "Examples:\n  \
            # Find by CID\n  \
            plcbundle op find bafyreig3tg4k...\n\n  \
            # Use with op get\n  \
            plcbundle op find bafyreig3... | awk '{print $3, $5}' | xargs plcbundle op get")]
    Find {
        /// CID to search for
        cid: String,
    },
}

pub fn run(cmd: OpCommand, dir: PathBuf, quiet: bool) -> Result<()> {
    match cmd.command {
        OpCommands::Get { bundle, position } => {
            cmd_op_get(dir, bundle, position, quiet)?;
        }
        OpCommands::Show { bundle, position } => {
            cmd_op_show(dir, bundle, position, quiet)?;
        }
        OpCommands::Find { cid } => {
            cmd_op_find(dir, cid, quiet)?;
        }
    }
    Ok(())
}

/// Parse operation position - supports both global position and bundle + position
/// Mimics Go version: small numbers (< 10000) are treated as bundle 1, position N
pub fn parse_op_position(bundle: u32, position: Option<usize>) -> (u32, usize) {
    match position {
        Some(pos) => (bundle, pos),
        None => {
            // Single argument: interpret as global or shorthand
            if bundle < constants::BUNDLE_SIZE as u32 {
                // Small numbers: shorthand for "bundle 1, position N"
                // op get 1 → bundle 1, position 1
                // op get 100 → bundle 1, position 100
                (1, bundle as usize)
            } else {
                // Large numbers: global position
                // op get 10000 → bundle 2, position 0
                // op get 88410345 → bundle 8842, position 345
                let global_pos = bundle as u64;
                let bundle_num = 1 + (global_pos / constants::BUNDLE_SIZE as u64) as u32;
                let op_pos = (global_pos % constants::BUNDLE_SIZE as u64) as usize;
                (bundle_num, op_pos)
            }
        }
    }
}

pub fn cmd_op_get(dir: PathBuf, bundle: u32, position: Option<usize>, quiet: bool) -> Result<()> {
    let (bundle_num, op_index) = parse_op_position(bundle, position);

    let manager = super::utils::create_manager(dir, false, quiet)?;

    if quiet {
        // Just output JSON - no stats
        let json = manager.get_operation_raw(bundle_num, op_index)?;
        println!("{}", json);
    } else {
        // Output with stats
        let result = manager.get_operation_with_stats(bundle_num, op_index)?;
        let global_pos =
            ((bundle_num - 1) as u64 * constants::BUNDLE_SIZE as u64) + op_index as u64;

        log::info!(
            "[Load] Bundle {:06}:{:04} (pos={}) in {:?} | {} bytes",
            bundle_num,
            op_index,
            global_pos,
            result.load_duration,
            result.size_bytes
        );

        println!("{}", result.raw_json);
    }

    Ok(())
}

pub fn cmd_op_show(dir: PathBuf, bundle: u32, position: Option<usize>, quiet: bool) -> Result<()> {
    let (bundle_num, op_index) = parse_op_position(bundle, position);

    let manager = super::utils::create_manager(dir, false, quiet)?;

    // Use the new get_operation API instead of loading entire bundle
    let load_start = Instant::now();
    let op = manager.get_operation(bundle_num, op_index)?;
    let load_duration = load_start.elapsed();

    let parse_start = Instant::now();
    // Operation is already parsed by get_operation
    let parse_duration = parse_start.elapsed();

    let global_pos = ((bundle_num - 1) as u64 * constants::BUNDLE_SIZE as u64) + op_index as u64;

    // Extract operation details
    let op_type = op
        .operation
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let handle = op
        .operation
        .get("handle")
        .and_then(|v| v.as_str())
        .or_else(|| {
            op.operation
                .get("alsoKnownAs")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_str())
                .map(|s| s.trim_start_matches("at://"))
        });

    let pds = op
        .operation
        .get("services")
        .and_then(|v| v.as_object())
        .and_then(|services| services.get("atproto_pds"))
        .and_then(|v| v.as_object())
        .and_then(|pds| pds.get("endpoint"))
        .and_then(|v| v.as_str());

    // Display formatted output
    println!("═══════════════════════════════════════════════════════════════");
    println!("                    Operation {}", global_pos);
    println!("═══════════════════════════════════════════════════════════════\n");

    println!("Location");
    println!("────────");
    println!("  Bundle:          {:06}", bundle_num);
    println!("  Position:        {}", op_index);
    println!("  Global position: {}\n", global_pos);

    println!("Identity");
    println!("────────");
    println!("  DID:             {}", op.did);
    if let Some(cid) = &op.cid {
        println!("  CID:             {}\n", cid);
    } else {
        println!();
    }

    println!("Timestamp");
    println!("─────────");
    println!("  Created:         {}", op.created_at);
    println!();

    println!("Status");
    println!("──────");
    let status = if op.nullified {
        "✗ Nullified"
    } else {
        "✓ Active"
    };
    println!("  {}\n", status);

    // Performance metrics (when not quiet)
    if !quiet {
        let total_time = load_duration + parse_duration;
        let json_size = serde_json::to_string(&op).map(|s| s.len()).unwrap_or(0);

        println!("Performance");
        println!("───────────");
        println!("  Load time:       {:?}", load_duration);
        println!("  Parse time:      {:?}", parse_duration);
        println!("  Total time:      {:?}", total_time);

        if json_size > 0 {
            println!("  Data size:       {} bytes", json_size);
            let mb_per_sec = (json_size as f64) / load_duration.as_secs_f64() / (1024.0 * 1024.0);
            println!("  Load speed:      {:.2} MB/s", mb_per_sec);
        }

        println!();
    }

    // Operation details
    if !op.nullified {
        println!("Details");
        println!("───────");
        println!("  Type:            {}", op_type);

        if let Some(h) = handle {
            println!("  Handle:          {}", h);
        }

        if let Some(p) = pds {
            println!("  PDS:             {}", p);
        }

        println!();
    }

    // Show full JSON when not quiet
    if !quiet {
        println!("Raw JSON");
        println!("────────");
        let json = serde_json::to_string_pretty(&op)?;
        println!("{}\n", json);
    }

    Ok(())
}

pub fn cmd_op_find(dir: PathBuf, cid: String, quiet: bool) -> Result<()> {
    let manager = super::utils::create_manager(dir, false, quiet)?;
    let last_bundle = manager.get_last_bundle();

    if !quiet {
        log::info!("Searching {} bundles for CID: {}\n", last_bundle, cid);
    }

    for bundle_num in 1..=last_bundle {
        let result = match manager.load_bundle(bundle_num, LoadOptions::default()) {
            Ok(r) => r,
            Err(_) => continue,
        };

        for (i, op) in result.operations.iter().enumerate() {
            let cid_matches = op.cid.as_ref().map_or(false, |c| c == &cid);

            if cid_matches {
                let global_pos =
                    ((bundle_num - 1) as u64 * constants::BUNDLE_SIZE as u64) + i as u64;

                println!("Found: bundle {:06}, position {}", bundle_num, i);
                println!("Global position: {}\n", global_pos);

                println!("  DID:        {}", op.did);
                println!("  Created:    {}", op.created_at);

                let status = if op.nullified {
                    "✗ Nullified"
                } else {
                    "✓ Active"
                };
                println!("  Status:     {}", status);

                return Ok(());
            }
        }

        // Progress indicator every 100 bundles
        if !quiet && bundle_num % 100 == 0 {
            eprint!("Searched through bundle {:06}...\r", bundle_num);
            use std::io::Write;
            std::io::stderr().flush()?;
        }
    }

    if !quiet {
        log::error!("\nCID not found: {}", cid);
        log::info!("(Searched {} bundles)", last_bundle);
    }

    anyhow::bail!("CID not found");
}
