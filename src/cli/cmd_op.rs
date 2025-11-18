use anyhow::Result;
use clap::{Args, Subcommand};
use plcbundle::{LoadOptions, constants};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Args)]
#[command(
    about = "Operation queries and inspection",
    long_about = "Access individual operations within bundles using flexible addressing schemes.
You can reference operations by bundle number and position (e.g., '42 1337' for
bundle 42, position 1337), or by global position (e.g., '410000' which represents
bundle 42, position 0).

Global positions are 0-indexed and calculated as ((bundleNumber - 1) × 10,000) + position,
making it easy to reference operations across the entire repository with a single number.
For example, 0 = bundle 1 position 0, 1 = bundle 1 position 1, 10000 = bundle 2 position 0.

The 'get' subcommand retrieves operations as JSON, with automatic pretty-printing
and syntax highlighting when outputting to a terminal. The 'show' subcommand
displays operations in a human-readable format with detailed metadata. The 'find'
subcommand searches across all bundles for an operation with a specific CID.

All subcommands support JMESPath queries to extract specific fields from operations,
making it easy to script and process operation data.",
    alias = "operation",
    alias = "record",
    help_template = "  # Get operation as JSON\n  \
                   {bin} op get 42 1337\n  \
                   {bin} op get 420000\n\n  \
                   # Show operation (formatted)\n  \
                   {bin} op show 42 1337\n  \
                   {bin} op show 88410345\n\n  \
                   # Find by CID\n  \
                   {bin} op find bafyreig3..."
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
    ///   2. Global position (0-indexed): get 0 (first operation), get 410000 (bundle 42 position 0)
    ///
    /// Global position (0-indexed) = ((bundleNumber - 1) × 10,000) + position
    ///
    /// By default, pretty-prints with colors when outputting to a terminal.
    /// Use --raw to force raw JSON output (useful for piping).
    /// Use -q/--query to extract a value using JMESPath.
    #[command(help_template = crate::clap_help!(
        examples: "  # By bundle + position (auto pretty-print in terminal)\n  \
                   {bin} op get 42 1337\n\n  \
                   # By global position (0-indexed)\n  \
                   {bin} op get 0\n  \
                   {bin} op get 410000\n\n  \
                   {bin} op get 88410345\n\n  \
                   # Query with JMESPath\n  \
                   {bin} op get 42 1337 -q 'operation.type'\n  \
                   {bin} op get 42 1337 -q 'did'\n  \
                   {bin} op get 88410345 -q 'did'\n\n  \
                   # Force raw JSON output\n  \
                   {bin} op get 42 1337 --raw\n\n  \
                   # Pipe to jq (auto-detects non-TTY, uses raw)\n  \
                   {bin} op get 42 1337 | jq .did"
    ))]
    Get {
        /// Bundle number (or global position if only one arg provided)
        bundle: u32,

        /// Operation position within bundle (optional if using global position)
        #[arg(value_name = "POSITION")]
        position: Option<usize>,

        /// Query operation JSON using JMESPath expression
        ///
        /// Extracts a value from the operation using JMESPath.
        /// Examples: 'did', 'operation.type', 'operation.handle', 'cid'
        #[arg(short = 'q', long = "query")]
        query: Option<String>,

        /// Force raw JSON output (no pretty printing, no colors)
        ///
        /// By default, output is pretty-printed with colors when writing to a terminal.
        /// Use this flag to force raw JSON output, useful for piping to other tools.
        #[arg(long = "raw")]
        raw: bool,
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
    #[command(help_template = crate::clap_help!(
        examples: "  # By bundle + position\n  \
                   {bin} op show 42 1337\n\n  \
                   # By global position (0-indexed)\n  \
                   {bin} op show 0\n  \
                   {bin} op show 88410345\n\n  \
                   # Quiet mode (minimal output)\n  \
                   {bin} op show 42 1337 -q"
    ))]
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
    #[command(help_template = crate::clap_help!(
        examples: "  # Find by CID\n  \
                   {bin} op find bafyreig3tg4k...\n\n  \
                   # Use with op get\n  \
                   {bin} op find bafyreig3... | awk '{print $3, $5}' | xargs {bin} op get"
    ))]
    Find {
        /// CID to search for
        cid: String,
    },
}

pub fn run(cmd: OpCommand, dir: PathBuf, quiet: bool) -> Result<()> {
    match cmd.command {
        OpCommands::Get { bundle, position, query, raw } => {
            cmd_op_get(dir, bundle, position, query, raw, quiet)?;
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
/// Small numbers (< 10000) are treated as bundle 1, position N (0-indexed)
/// Global positions are 0-indexed: 0 = bundle 1 position 0, 10000 = bundle 2 position 0
pub fn parse_op_position(bundle: u32, position: Option<usize>) -> (u32, usize) {
    match position {
        Some(pos) => (bundle, pos),
        None => {
            // Single argument: interpret as global or shorthand
            if bundle < constants::BUNDLE_SIZE as u32 {
                // Small numbers: shorthand for "bundle 1, position N" (0-indexed)
                // op get 0 → bundle 1, position 0
                // op get 1 → bundle 1, position 1
                // op get 9999 → bundle 1, position 9999
                (1, bundle as usize)
            } else {
                // Large numbers: global position (0-indexed)
                // op get 10000 → bundle 2, position 0
                // op get 88410345 → bundle 8842, position 345
                let global_pos = bundle as u64;
                let (bundle_num, op_pos) = plcbundle::global_to_bundle_position(global_pos);
                (bundle_num, op_pos)
            }
        }
    }
}

pub fn cmd_op_get(dir: PathBuf, bundle: u32, position: Option<usize>, query: Option<String>, raw: bool, quiet: bool) -> Result<()> {
    let (bundle_num, op_index) = parse_op_position(bundle, position);

    let manager = super::utils::create_manager(dir, false, quiet, false)?;

    // Get the operation JSON
    let json = if quiet {
        manager.get_operation_raw(bundle_num, op_index)?
    } else {
        let result = manager.get_operation_with_stats(bundle_num, op_index)?;
        let global_pos =
            plcbundle::bundle_position_to_global(bundle_num, op_index);

        log::info!(
            "[Load] Bundle {}:{:04} (pos={}) in {:?} | {} bytes",
            bundle_num,
            op_index,
            global_pos,
            result.load_duration,
            result.size_bytes
        );

        result.raw_json
    };

    // If query is provided, apply JMESPath query
    let output_json = if let Some(query_expr) = query {
        // Compile JMESPath expression
        let expr = jmespath::compile(&query_expr)
            .map_err(|e| anyhow::anyhow!("Failed to compile JMESPath query '{}': {}", query_expr, e))?;

        // Execute query
        let data = jmespath::Variable::from_json(&json)
            .map_err(|e| anyhow::anyhow!("Failed to parse operation JSON: {}", e))?;

        let result = expr.search(&data)
            .map_err(|e| anyhow::anyhow!("JMESPath query failed: {}", e))?;

        if result.is_null() {
            anyhow::bail!("Query '{}' returned null (field not found)", query_expr);
        }

        // Convert result to JSON string
        // Note: jmespath uses serde_json internally, so we use serde_json here (not bundle/operation data)
        if result.is_string() {
            result.as_string().unwrap().to_string()
        } else {
            serde_json::to_string(&*result)
                .map_err(|e| anyhow::anyhow!("Failed to serialize query result: {}", e))?
        }
    } else {
        json
    };

    // Determine if we should pretty print:
    // - Pretty print if stdout is a TTY (interactive terminal) and --raw is not set
    // - Use raw output if --raw is set or if output is piped (not a TTY)
    #[cfg(feature = "cli")]
    let should_pretty = !raw && super::utils::is_stdout_tty();
    #[cfg(not(feature = "cli"))]
    let should_pretty = false; // No TTY detection without CLI feature

    if should_pretty {
        // Try to parse and pretty print the result
        match sonic_rs::from_str::<sonic_rs::Value>(&output_json) {
            Ok(parsed) => {
                let pretty_json = sonic_rs::to_string_pretty(&parsed)?;
                #[cfg(feature = "cli")]
                {
                    println!("{}", super::utils::colorize_json(&pretty_json));
                }
                #[cfg(not(feature = "cli"))]
                {
                    println!("{}", pretty_json);
                }
            }
            Err(_) => {
                // If it's not valid JSON (e.g., a string result), just output as-is
                println!("{}", output_json);
            }
        }
    } else {
        println!("{}", output_json);
    }

    Ok(())
}

pub fn cmd_op_show(dir: PathBuf, bundle: u32, position: Option<usize>, quiet: bool) -> Result<()> {
    let (bundle_num, op_index) = parse_op_position(bundle, position);

    let manager = super::utils::create_manager(dir, false, quiet, false)?;

    // Use the new get_operation API instead of loading entire bundle
    let load_start = Instant::now();
    let op = manager.get_operation(bundle_num, op_index)?;
    let load_duration = load_start.elapsed();

    let parse_start = Instant::now();
    // Operation is already parsed by get_operation
    let parse_duration = parse_start.elapsed();

    let global_pos = plcbundle::bundle_position_to_global(bundle_num, op_index);

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
        .and_then(|services_val| services_val.get("atproto_pds"))
        .and_then(|pds_val| pds_val.get("endpoint"))
        .and_then(|v| v.as_str());

    // Display formatted output
    println!("═══════════════════════════════════════════════════════════════");
    println!("                    Operation {}", global_pos);
    println!("═══════════════════════════════════════════════════════════════\n");

    println!("Location");
    println!("────────");
    println!("  Bundle:          {}", bundle_num);
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
    use super::utils::colors;
    let status = if op.nullified {
        format!("{}✗ Nullified{}", colors::RED, colors::RESET)
    } else {
        format!("{}✓ Active{}", colors::GREEN, colors::RESET)
    };
    println!("  {}\n", status);

    // Performance metrics (when not quiet)
    if !quiet {
        let total_time = load_duration + parse_duration;
        let json_size = sonic_rs::to_string(&op).map(|s| s.len()).unwrap_or(0);

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
        let json = sonic_rs::to_string_pretty(&op)?;
        println!("{}\n", json);
    }

    Ok(())
}

pub fn cmd_op_find(dir: PathBuf, cid: String, quiet: bool) -> Result<()> {
    let manager = super::utils::create_manager(dir, false, quiet, false)?;
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
                    plcbundle::bundle_position_to_global(bundle_num, i);

                println!("Found: bundle {}, position {}", bundle_num, i);
                println!("Global position: {}\n", global_pos);

                println!("  DID:        {}", op.did);
                println!("  Created:    {}", op.created_at);

                use super::utils::colors;
                let status = if op.nullified {
                    format!("{}✗ Nullified{}", colors::RED, colors::RESET)
                } else {
                    format!("{}✓ Active{}", colors::GREEN, colors::RESET)
                };
                println!("  Status:     {}", status);

                return Ok(());
            }
        }

        // Progress indicator every 100 bundles
        if !quiet && bundle_num % 100 == 0 {
            eprint!("Searched through bundle {}...\r", bundle_num);
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
