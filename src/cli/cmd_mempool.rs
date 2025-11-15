// src/bin/commands/mempool.rs
use super::utils;
use anyhow::Result;
use clap::{Args, Subcommand};
use plcbundle::{BundleManager, constants};
use std::io::{self, Write};
use std::path::PathBuf;

#[derive(Args)]
pub struct MempoolCommand {
    #[command(subcommand)]
    pub command: Option<MempoolSubcommand>,

    /// Directory containing PLC bundles
    #[arg(short, long, default_value = ".")]
    pub dir: PathBuf,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,
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
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}

pub fn run(cmd: MempoolCommand) -> Result<()> {
    let manager = BundleManager::new(cmd.dir.clone())?.with_verbose(cmd.verbose);

    match cmd.command {
        Some(MempoolSubcommand::Status { verbose }) => {
            show_status(&manager, &cmd.dir, verbose || cmd.verbose)
        }
        Some(MempoolSubcommand::Clear { force }) => clear(&manager, &cmd.dir, force),
        Some(MempoolSubcommand::Dump { output }) => dump(&manager, output),
        None => {
            // Default to status
            show_status(&manager, &cmd.dir, cmd.verbose)
        }
    }
}

fn show_status(manager: &BundleManager, dir: &PathBuf, verbose: bool) -> Result<()> {
    let stats = manager.get_mempool_stats()?;

    println!("Mempool Status");
    println!("══════════════\n");
    println!("  Directory:      {}", utils::display_path(dir).display());
    println!("  Target bundle:  {:06}", stats.target_bundle);
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

    // Show mempool file location
    let mempool_filename = format!(
        "{}{:06}.jsonl",
        constants::MEMPOOL_FILE_PREFIX,
        stats.target_bundle
    );
    let mempool_path = utils::display_path(dir).join(mempool_filename);
    println!("File: {}", mempool_path.display());

    Ok(())
}

fn clear(manager: &BundleManager, dir: &PathBuf, force: bool) -> Result<()> {
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
        let json = serde_json::to_string(op)?;
        writeln!(writer, "{}", json)?;
    }

    eprintln!("Exported {} operations from mempool", ops.len());
    Ok(())
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();

    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }

    result
}
