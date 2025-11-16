use super::utils;
use anyhow::Result;
use clap::Args;
use plcbundle::CleanPreview;
use std::io::{self, Write};
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
            # Clean all temporary files (with confirmation)\n  \
            {bin} clean\n\n  \
            # Clean without confirmation prompt\n  \
            {bin} clean --force\n\n  \
            # Clean with verbose output\n  \
            {bin} clean --verbose\n\n  \
            # Using alias\n  \
            {bin} cleanup"
)]
pub struct CleanCommand {
    /// Show verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Skip confirmation prompt
    #[arg(short, long)]
    pub force: bool,
}

pub fn run(cmd: CleanCommand, dir: PathBuf, global_verbose: bool) -> Result<()> {
    let verbose = cmd.verbose || global_verbose;
    let manager = utils::create_manager(dir.clone(), verbose, false)?;

    // Step 1: Preview what will be cleaned
    let preview = manager.clean_preview()?;

    // Step 2: Display preview
    display_clean_preview(&dir, &preview, verbose)?;

    // Step 3: Ask for confirmation (unless --force)
    if preview.files.is_empty() {
        println!("✓ No temporary files found - repository is clean");
        return Ok(());
    }

    if !cmd.force {
        if !confirm_clean()? {
            println!("Cancelled");
            return Ok(());
        }
        println!();
    }

    // Step 4: Perform cleanup
    if verbose {
        println!("Cleaning temporary files from repository...");
        println!("Directory: {}\n", utils::display_path(&dir).display());
    }

    let result = manager.clean()?;

    // Step 5: Display results
    println!("✓ Cleaned {} temporary file(s)", result.files_removed);
    if result.bytes_freed > 0 {
        println!("  Freed: {}", utils::format_bytes(result.bytes_freed));
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

fn display_clean_preview(dir: &PathBuf, preview: &CleanPreview, _verbose: bool) -> Result<()> {
    println!("Files to be deleted:");
    println!();

    if preview.files.is_empty() {
        return Ok(());
    }

    // Group files by directory for better display
    let mut root_files = Vec::new();
    let mut config_files = Vec::new();
    let mut shard_files = Vec::new();

    for file in &preview.files {
        let path_str = file.path.to_string_lossy();
        if path_str.contains(".plcbundle/shards/") {
            shard_files.push(file);
        } else if path_str.contains(".plcbundle/") {
            config_files.push(file);
        } else {
            root_files.push(file);
        }
    }

    // Display root files
    if !root_files.is_empty() {
        println!("  Repository root:");
        for file in &root_files {
            let rel_path = file.path.strip_prefix(dir)
                .unwrap_or(&file.path)
                .to_string_lossy();
            println!("    • {} ({})", rel_path, utils::format_bytes(file.size));
        }
        println!();
    }

    // Display config files
    if !config_files.is_empty() {
        println!("  DID index directory:");
        for file in &config_files {
            let rel_path = file.path.strip_prefix(dir)
                .unwrap_or(&file.path)
                .to_string_lossy();
            println!("    • {} ({})", rel_path, utils::format_bytes(file.size));
        }
        println!();
    }

    // Display shard files
    if !shard_files.is_empty() {
        println!("  Shards directory:");
        for file in &shard_files {
            let rel_path = file.path.strip_prefix(dir)
                .unwrap_or(&file.path)
                .to_string_lossy();
            println!("    • {} ({})", rel_path, utils::format_bytes(file.size));
        }
        println!();
    }

    println!("  Total: {} file(s), {}", preview.files.len(), utils::format_bytes(preview.total_size));
    println!();

    Ok(())
}

fn confirm_clean() -> Result<bool> {
    print!("Delete these files? [y/N]: ");
    io::stdout().flush()?;

    let mut response = String::new();
    io::stdin().read_line(&mut response)?;

    let response = response.trim().to_lowercase();
    Ok(response == "y" || response == "yes")
}

