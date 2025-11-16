use anyhow::{Context, Result};
use clap::Args;
use plcbundle::{constants, remote::RemoteClient, BundleManager};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Args)]
#[command(
    about = "Clone a remote PLC bundle repository",
    long_about = "Download all bundles from a remote plcbundle instance in parallel.\n\n\
                  Similar to 'git clone' - creates a complete copy of the remote repository.\n\n\
                  This command downloads bundles in parallel for maximum speed and\n\
                  reconstructs the plc_bundles.json index file during the process.\n\n\
                  The command should be run outside the repository. It takes:\n  \
                    • Source URL: The remote plcbundle instance URL\n  \
                    • Target directory: Where to create the new repository",
    after_help = "Examples:\n  \
            # Clone from remote instance\n  \
            plcbundle clone https://plc.example.com /path/to/local\n\n  \
            # Clone to current directory\n  \
            plcbundle clone https://plc.example.com .\n\n  \
            # Clone with custom parallelism\n  \
            plcbundle clone https://plc.example.com /path/to/local --parallel 8"
)]
pub struct CloneCommand {
    /// Remote plcbundle instance URL
    pub source_url: String,

    /// Target directory for cloned repository
    pub target_dir: PathBuf,

    /// Number of parallel downloads (default: 4)
    #[arg(long, default_value = "4")]
    pub parallel: usize,

    /// Resume partial clone (skip existing bundles)
    #[arg(long)]
    pub resume: bool,
}

pub fn run(cmd: CloneCommand) -> Result<()> {
    // Create tokio runtime for async operations
    tokio::runtime::Runtime::new()?.block_on(async {
        run_async(cmd).await
    })
}

async fn run_async(cmd: CloneCommand) -> Result<()> {
    use super::utils::display_path;

    // Validate parallel count
    if cmd.parallel == 0 || cmd.parallel > 32 {
        anyhow::bail!("Parallel downloads must be between 1 and 32");
    }

    // Resolve target directory to absolute path
    let target_dir = if cmd.target_dir.is_absolute() {
        cmd.target_dir.clone()
    } else {
        std::env::current_dir()?.join(&cmd.target_dir)
    };

    println!("Cloning from: {}", cmd.source_url);
    println!("Target:       {}", display_path(&target_dir).display());
    println!("Parallelism:  {} downloads", cmd.parallel);
    println!();

    // Create remote client
    let client = RemoteClient::new(&cmd.source_url)?;

    // Fetch remote index
    println!("Fetching index...");
    let remote_index = client
        .fetch_index()
        .await
        .context("Failed to fetch remote index")?;

    let total_bundles = remote_index.bundles.len();
    let last_bundle = remote_index.last_bundle;

    // Get root hash (first bundle) and head hash (last bundle)
    let root_hash = remote_index
        .bundles
        .first()
        .map(|b| b.hash.as_str())
        .unwrap_or("(none)");
    let head_hash = remote_index
        .bundles
        .last()
        .map(|b| b.hash.as_str())
        .unwrap_or("(none)");

    // Format total compressed size
    let total_size = remote_index.total_size_bytes;
    let size_display = plcbundle::format::format_bytes(total_size);

    println!("✓ Remote index fetched");
    println!("  Version:       {}", remote_index.version);
    println!("  Origin:        {}", remote_index.origin);
    println!("  Last bundle:   {}", last_bundle);
    println!("  Total bundles: {}", total_bundles);
    println!("  Total size:    {}", size_display);
    println!("  Root hash:     {}", root_hash);
    println!("  Head hash:     {}", head_hash);
    println!();

    // Create target directory if it doesn't exist
    if !target_dir.exists() {
        std::fs::create_dir_all(&target_dir)
            .context("Failed to create target directory")?;
    }

    // Check if target directory is empty or if resuming
    let index_path = target_dir.join("plc_bundles.json");
    if index_path.exists() && !cmd.resume {
        anyhow::bail!(
            "Target directory already contains plc_bundles.json\nUse --resume to continue partial clone"
        );
    }

    // Determine which bundles to download
    let bundles_to_download: Vec<u32> = if cmd.resume {
        // Check which bundles already exist
        let existing_count = remote_index
            .bundles
            .iter()
            .filter(|meta| {
                let bundle_path = constants::bundle_path(&target_dir, meta.bundle_number);
                bundle_path.exists()
            })
            .count();

        if existing_count > 0 {
            println!("Resuming: {} bundles already downloaded", existing_count);
        }

        remote_index
            .bundles
            .iter()
            .filter_map(|meta| {
                let bundle_path = constants::bundle_path(&target_dir, meta.bundle_number);
                if !bundle_path.exists() {
                    Some(meta.bundle_number)
                } else {
                    None
                }
            })
            .collect()
    } else {
        remote_index
            .bundles
            .iter()
            .map(|meta| meta.bundle_number)
            .collect()
    };

    let bundles_count = bundles_to_download.len();
    if bundles_count == 0 {
        println!("✓ All bundles already downloaded");
        return Ok(());
    }

    // Calculate total bytes to download
    let total_bytes: u64 = remote_index
        .bundles
        .iter()
        .filter(|meta| bundles_to_download.contains(&meta.bundle_number))
        .map(|meta| meta.compressed_size)
        .sum();

    println!("Downloading {} bundle(s)...", bundles_count);
    println!();

    // Create progress bar with byte tracking
    let progress = Arc::new(super::progress::ProgressBar::with_bytes(bundles_count, total_bytes));

    // Clone using BundleManager API with progress callback
    let progress_clone = Arc::clone(&progress);
    let (downloaded_count, failed_count) = BundleManager::clone_from_remote(
        cmd.source_url.clone(),
        &target_dir,
        &remote_index,
        bundles_to_download,
        Some(move |_bundle_num, count, _total, bytes| {
            progress_clone.set_with_bytes(count, bytes);
        }),
    )
    .await?;

    progress.finish();
    println!();

    if failed_count > 0 {
        eprintln!("✗ Clone incomplete: {} succeeded, {} failed", downloaded_count, failed_count);
        eprintln!("  Use --resume to retry failed downloads");
        anyhow::bail!("Clone failed");
    }

    println!("✓ Clone complete!");
    println!("  Location: {}", display_path(&target_dir).display());
    println!("  Bundles:  {}", downloaded_count);
    println!();
    println!("Next steps:");
    println!("  cd {}", display_path(&target_dir).display());
    println!("  {} verify --all  # Verify bundle integrity", constants::BINARY_NAME);
    println!("  {} sync          # Fetch new bundles", constants::BINARY_NAME);

    Ok(())
}
