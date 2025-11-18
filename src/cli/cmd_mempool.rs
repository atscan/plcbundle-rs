// src/bin/commands/mempool.rs
use super::utils;
use super::utils::HasGlobalFlags;
use anyhow::Result;
use clap::{Args, Subcommand, ValueHint};
use plcbundle::format::format_number;
use plcbundle::{BundleManager, constants};
use std::io::{self, Write};
use std::path::Path;
use std::path::PathBuf;

#[derive(Args)]
#[command(
    about = "Manage mempool operations",
    long_about = "Manage the mempool, which stores operations waiting to be bundled into the next
bundle file. The mempool maintains strict chronological order and automatically
validates operation consistency to ensure data integrity.

Operations accumulate in the mempool until there are enough (10,000) to create a
new bundle. The 'status' subcommand shows how many operations are currently in the
mempool and whether a new bundle can be created. The 'dump' subcommand exports all
mempool operations as JSONL for backup or analysis.

The 'clear' subcommand removes all operations from the mempool, useful for resetting
the state or removing invalid operations. This is typically done automatically when
a bundle is created, but can be done manually if needed.

Mempool operations are stored in a temporary file that gets converted to a bundle
file when the threshold is reached. The mempool ensures operations are processed
in the correct order and maintains consistency with the bundle chain.",
    alias = "mp",
    help_template = crate::clap_help!(
        examples: "  # Show mempool status\n  \
                   {bin} mempool\n  \
                   {bin} mempool status\n\n  \
                   # Clear all operations\n  \
                   {bin} mempool clear\n\n  \
                   # Export operations as JSONL\n  \
                   {bin} mempool dump\n  \
                   {bin} mempool dump > operations.jsonl\n\n  \
                   # Using alias\n  \
                   {bin} mp status"
    )
)]
pub struct MempoolCommand {
    #[command(subcommand)]
    pub command: Option<MempoolSubcommand>,
}

#[derive(Subcommand)]
pub enum MempoolSubcommand {
    /// Show mempool status
    #[command(alias = "s", alias = "info")]
    Status {
        /// Show sample operations
        #[arg(short, long)]
        verbose: bool,
    },

    /// Clear all operations from mempool
    #[command(alias = "c", alias = "reset")]
    Clear {
        /// Skip confirmation prompt
        #[arg(short, long)]
        force: bool,
    },

    /// Export mempool operations as JSONL
    #[command(alias = "export", alias = "d")]
    Dump {
        /// Output file (default: stdout)
        #[arg(short, long, value_hint = ValueHint::FilePath)]
        output: Option<PathBuf>,
    },
}

impl HasGlobalFlags for MempoolCommand {
    fn verbose(&self) -> bool {
        false
    }
    fn quiet(&self) -> bool {
        false
    }
}

pub fn run(cmd: MempoolCommand, dir: PathBuf, global_verbose: bool) -> Result<()> {
    let manager = utils::create_manager(dir.clone(), global_verbose, false, false)?;

    match cmd.command {
        Some(MempoolSubcommand::Status { verbose }) => {
            show_status(&manager, &dir, verbose || global_verbose)
        }
        Some(MempoolSubcommand::Clear { force }) => clear(&manager, &dir, force),
        Some(MempoolSubcommand::Dump { output }) => dump(&manager, output),
        None => {
            // Default to status
            show_status(&manager, &dir, global_verbose)
        }
    }
}

fn show_status(manager: &BundleManager, dir: &Path, verbose: bool) -> Result<()> {
    manager.load_mempool()?;
    let stats = manager.get_mempool_stats()?;

    // Show mempool file location
    let mempool_filename = format!(
        "{}{:06}.jsonl",
        constants::MEMPOOL_FILE_PREFIX,
        stats.target_bundle
    );
    let mempool_path: PathBuf = utils::display_path(dir).join(mempool_filename);

    println!("Mempool Status");
    println!("══════════════\n");
    println!("  Directory:      {}", utils::display_path(dir).display());
    println!("  Mempool File:   {}", mempool_path.display());
    println!("  Target bundle:  {}", stats.target_bundle);
    println!(
        "  Operations:     {} / {}",
        stats.count,
        constants::BUNDLE_SIZE
    );
    println!(
        "  Min timestamp:  {}\n",
        stats.min_timestamp.format("%Y-%m-%d %H:%M:%S")
    );

    // Validation status
    let validation_icon = if stats.validated { "✓" } else { "⚠️" };
    println!(
        "  Validated:      {} {}\n",
        validation_icon, stats.validated
    );

    if stats.count > 0 {
        // Size information
        if let Some(size_bytes) = stats.size_bytes {
            println!("  Size:           {:.2} KB", size_bytes as f64 / 1024.0);
        }

        // Time range
        if let Some(first_time) = stats.first_time {
            println!(
                "  First op:       {}",
                first_time.format("%Y-%m-%d %H:%M:%S")
            );
        }
        if let Some(last_time) = stats.last_time {
            println!(
                "  Last op:        {}",
                last_time.format("%Y-%m-%d %H:%M:%S")
            );
        }

        println!();

        // Progress bar
        let progress = (stats.count as f64 / constants::BUNDLE_SIZE as f64) * 100.0;
        println!(
            "  Progress:       {:.1}% ({}/{})",
            progress,
            stats.count,
            constants::BUNDLE_SIZE
        );

        let bar_width = 40;
        let filled =
            ((bar_width as f64) * (stats.count as f64) / constants::BUNDLE_SIZE as f64) as usize;
        let filled = filled.min(bar_width);
        let bar = "█".repeat(filled) + &"░".repeat(bar_width - filled);
        println!("  [{}]\n", bar);

        // Bundle creation status
        if stats.can_create_bundle {
            println!("  ✓ Ready to create bundle");
        } else {
            let remaining = constants::BUNDLE_SIZE - stats.count;
            println!("  Need {} more operations", format_number(remaining));
        }
    } else {
        println!("  (empty)");
    }

    println!();

    // Verbose: Show sample operations
    if verbose && stats.count > 0 {
        println!("Sample Operations (first 10)");
        println!("────────────────────────────\n");

        let ops = manager.get_mempool_operations()?;

        let show_count = 10.min(ops.len());

        for (i, op) in ops.iter().take(show_count).enumerate() {
            println!("  {}. DID: {}", i + 1, op.did);
            if let Some(ref cid) = op.cid {
                println!("     CID: {}", cid);
            }
            println!("     Created: {}", op.created_at);
            println!();
        }

        if ops.len() > show_count {
            println!("  ... and {} more\n", ops.len() - show_count);
        }
    }

    Ok(())
}

fn clear(manager: &BundleManager, dir: &Path, force: bool) -> Result<()> {
    manager.load_mempool()?;
    let stats = manager.get_mempool_stats()?;
    let count = stats.count;

    if count == 0 {
        println!("Mempool is already empty");
        return Ok(());
    }

    println!("Working in: {}\n", utils::display_path(dir).display());

    if !force {
        print!(
            "⚠️  This will clear {} operations from the mempool.\nAre you sure? [y/N]: ",
            count
        );
        io::stdout().flush()?;

        let mut response = String::new();
        io::stdin().read_line(&mut response)?;

        if response.trim().to_lowercase() != "y" {
            println!("Cancelled");
            return Ok(());
        }
    }

    manager.clear_mempool()?;

    println!("\n✓ Mempool cleared ({} operations removed)", count);
    Ok(())
}

fn dump(manager: &BundleManager, output: Option<PathBuf>) -> Result<()> {
    manager.load_mempool()?;
    let ops = manager.get_mempool_operations()?;

    if ops.is_empty() {
        eprintln!("Mempool is empty");
        return Ok(());
    }

    // Determine output destination
    let mut writer: Box<dyn Write> = if let Some(path) = output {
        eprintln!("Exporting to: {}", path.display());
        Box::new(std::fs::File::create(path)?)
    } else {
        Box::new(io::stdout())
    };

    // Write JSONL
    for op in &ops {
        let json = sonic_rs::to_string(op)?;
        writeln!(writer, "{}", json)?;
    }

    eprintln!("Exported {} operations from mempool", ops.len());
    Ok(())
}
