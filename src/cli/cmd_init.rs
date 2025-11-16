use anyhow::Result;
use clap::Args;
use plcbundle::{constants, BundleManager};
use std::path::PathBuf;

#[derive(Args)]
#[command(
    about = "Initialize a new PLC bundle repository",
    long_about = "Create a new repository for storing PLC bundle data. This command sets up
the necessary directory structure and creates an empty index file (plc_bundles.json)
that will track all bundles in the repository.

During initialization, you'll be prompted to select a PLC directory URL (the source
of bundle data). You can also specify it directly with --plc to skip the prompt.
The origin URL is stored in the index and used to verify that bundles come from
the expected source.

After initialization, use 'sync' to fetch bundles from the PLC directory, or
'clone' to copy bundles from an existing repository. The repository is ready
to use immediately after initialization.",
    help_template = crate::clap_help!(
        examples: "  # Initialize in current directory\n  \
                   {bin} init\n\n  \
                   # Initialize in specific directory\n  \
                   {bin} init /path/to/bundles\n\n  \
                   # Set PLC directory URL\n  \
                   {bin} init --plc https://plc.directory\n\n  \
                   # Force reinitialize existing repository\n  \
                   {bin} init --force"
    )
)]
pub struct InitCommand {
    /// Directory to initialize (default: current directory)
    #[arg(default_value = ".")]
    pub dir: PathBuf,

    /// PLC Directory URL (if not provided, will prompt interactively)
    #[arg(long)]
    pub plc: Option<String>,

    /// Origin identifier for this repository (deprecated: use --plc instead)
    #[arg(long, hide = true)]
    pub origin: Option<String>,

    /// Force initialization even if directory already exists
    #[arg(short, long)]
    pub force: bool,
}

pub fn run(cmd: InitCommand) -> Result<()> {
    // Get absolute path for display
    let dir = if cmd.dir.is_absolute() {
        cmd.dir.clone()
    } else {
        std::env::current_dir()?.join(&cmd.dir)
    };

    // Determine PLC Directory URL
    let plc_url = if let Some(plc) = cmd.plc {
        // Use provided --plc flag
        plc
    } else if let Some(origin) = cmd.origin {
        // Backward compatibility: use --origin if provided
        origin
    } else {
        // Interactive prompt
        prompt_plc_directory_url()?
    };

    // Initialize repository using BundleManager API
    let initialized = BundleManager::init_repository(&dir, plc_url.clone(), cmd.force)?;

    if !initialized {
        eprintln!("Repository already initialized at: {}", dir.display());
        eprintln!("Use --force to reinitialize");
        return Ok(());
    }

    // Check if user needs to cd to the directory
    let current_dir = std::env::current_dir()?;
    let need_cd = current_dir != dir;

    println!("✓ Initialized PLC bundle repository");
    println!("  Location: {}", dir.display());
    println!("  Origin:   {}", plc_url);
    println!("  Index:    plc_bundles.json");

    if need_cd {
        println!("\n⚠ Warning: You initialized in a different directory");
        println!("  Please run the following command first:");
        println!("    cd {}", dir.display());
    }

    println!("\nNext steps:");
    println!(
        "  {} sync           # Fetch bundles from PLC directory",
        plcbundle::constants::BINARY_NAME
    );
    println!(
        "  {} info           # Show repository info",
        plcbundle::constants::BINARY_NAME
    );
    println!(
        "  {} mempool status # Check mempool status",
        plcbundle::constants::BINARY_NAME
    );

    Ok(())
}

fn prompt_plc_directory_url() -> Result<String> {
    use dialoguer::{Select, theme::ColorfulTheme};

    println!("\n┌  Welcome to {}!", constants::BINARY_NAME);
    println!("│");
    println!("◆  Which PLC Directory would you like to use?");
    println!("│");

    let options = vec![
        format!("plc.directory ({})", constants::DEFAULT_PLC_DIRECTORY_URL),
        "local (for local development/testing)".to_string(),
        "Custom (enter your own URL)".to_string(),
    ];

    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("")
        .default(0)
        .items(&options)
        .interact()
        .map_err(|e| anyhow::anyhow!("Failed to read user input: {}", e))?;

    let url = match selection {
        0 => constants::DEFAULT_PLC_DIRECTORY_URL.to_string(),
        1 => constants::DEFAULT_ORIGIN.to_string(),
        2 => {
            use dialoguer::Input;
            Input::with_theme(&ColorfulTheme::default())
                .with_prompt("Enter PLC Directory URL")
                .validate_with(|input: &String| -> Result<(), &str> {
                    if input.trim().is_empty() {
                        Err("URL cannot be empty")
                    } else if !input.starts_with("http://") && !input.starts_with("https://") {
                        Err("URL must start with http:// or https://")
                    } else {
                        Ok(())
                    }
                })
                .interact_text()
                .map_err(|e| anyhow::anyhow!("Failed to read user input: {}", e))?
        }
        _ => unreachable!(),
    };

    println!("└");
    println!("\n{}", "─".repeat(60)); // Add clear separator line

    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_init_creates_index() {
        let temp = TempDir::new().unwrap();
        let cmd = InitCommand {
            dir: temp.path().to_path_buf(),
            plc: Some("test".to_string()),
            origin: None,
            force: false,
        };

        run(cmd).unwrap();

        let index_path = temp.path().join("plc_bundles.json");
        assert!(index_path.exists());

        let content = std::fs::read_to_string(index_path).unwrap();
        assert!(content.contains("\"origin\": \"test\""));
        assert!(content.contains("\"last_bundle\": 0"));
    }

    #[test]
    fn test_init_prevents_overwrite() {
        let temp = TempDir::new().unwrap();

        // First init
        let cmd = InitCommand {
            dir: temp.path().to_path_buf(),
            plc: Some("first".to_string()),
            origin: None,
            force: false,
        };
        run(cmd).unwrap();

        // Second init without force
        let cmd = InitCommand {
            dir: temp.path().to_path_buf(),
            plc: Some("second".to_string()),
            origin: None,
            force: false,
        };
        run(cmd).unwrap(); // Should succeed but not overwrite

        let index_path = temp.path().join("plc_bundles.json");
        let content = std::fs::read_to_string(index_path).unwrap();
        assert!(content.contains("\"origin\": \"first\"")); // Still first
    }

    #[test]
    fn test_init_force_overwrites() {
        let temp = TempDir::new().unwrap();

        // First init
        let cmd = InitCommand {
            dir: temp.path().to_path_buf(),
            plc: Some("first".to_string()),
            origin: None,
            force: false,
        };
        run(cmd).unwrap();

        // Second init with force
        let cmd = InitCommand {
            dir: temp.path().to_path_buf(),
            plc: Some("second".to_string()),
            origin: None,
            force: true,
        };
        run(cmd).unwrap();

        let index_path = temp.path().join("plc_bundles.json");
        let content = std::fs::read_to_string(index_path).unwrap();
        assert!(content.contains("\"origin\": \"second\"")); // Overwritten
    }
}
