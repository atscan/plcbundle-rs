use super::utils;
use anyhow::Result;
use clap::Args;
use plcbundle::BundleManager;
use std::path::PathBuf;

#[derive(Args)]
#[command(
    about = "Remove all temporary files from the repository",
    alias = "cleanup",
    long_about = "Cleans up all temporary files (.tmp) from the repository.\n\n\
                  This includes:\n\
                  - Temporary index files (plc_bundles.json.tmp)\n\
                  - Temporary DID index files (.plcbundle/config.json.tmp)\n\
                  - Temporary shard files (.plcbundle/shards/*.tmp)\n\n\
                  Temporary files are created during atomic write operations and\n\
                  should normally be cleaned up automatically. This command is useful\n\
                  for cleaning up leftover temporary files from interrupted operations.",
    after_help = "Examples:\n  \
            # Clean all temporary files\n  \
            plcbundle clean\n\n  \
            # Clean with verbose output\n  \
            plcbundle clean --verbose\n\n  \
            # Using alias\n  \
            plcbundle cleanup"
)]
pub struct CleanCommand {
    /// Show verbose output
    #[arg(short, long)]
    pub verbose: bool,
}

pub fn run(cmd: CleanCommand, dir: PathBuf, global_verbose: bool) -> Result<()> {
    let verbose = cmd.verbose || global_verbose;
    let manager = utils::create_manager(dir.clone(), verbose, false)?;

    if verbose {
        println!("Cleaning temporary files from repository...");
        println!("Directory: {}\n", utils::display_path(&dir).display());
    }

    let result = manager.clean()?;

    // Display results
    if result.files_removed == 0 {
        println!("✓ No temporary files found - repository is clean");
    } else {
        println!("✓ Cleaned {} temporary file(s)", result.files_removed);
        if result.bytes_freed > 0 {
            println!("  Freed: {}", utils::format_bytes(result.bytes_freed));
        }
    }

    // Show errors if any
    if let Some(errors) = &result.errors {
        if !errors.is_empty() {
            eprintln!("\n⚠ Warning: Some errors occurred during cleanup:");
            for error in errors {
                eprintln!("  • {}", error);
            }
        }
    }

    Ok(())
}

