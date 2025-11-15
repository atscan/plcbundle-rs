use anyhow::Result;
use clap::Args;
use plcbundle::constants;
use std::path::PathBuf;

#[derive(Args)]
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

    let index_path = dir.join("plc_bundles.json");

    // Check if already initialized
    if index_path.exists() && !cmd.force {
        eprintln!("Repository already initialized at: {}", dir.display());
        eprintln!("Use --force to reinitialize");
        return Ok(());
    }

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

    // Create directory if it doesn't exist
    if !dir.exists() {
        std::fs::create_dir_all(&dir)?;
        println!("Created directory: {}", dir.display());
    }

    // Create .plcbundle directory for DID index
    let plcbundle_dir = dir.join(constants::DID_INDEX_DIR);
    if !plcbundle_dir.exists() {
        std::fs::create_dir_all(&plcbundle_dir)?;
    }

    // Create empty index
    let index = serde_json::json!({
        "version": "1.0",
        "origin": plc_url,
        "last_bundle": 0,
        "updated_at": chrono::Utc::now().to_rfc3339(),
        "total_size_bytes": 0,
        "total_uncompressed_size_bytes": 0,
        "bundles": []
    });

    // Write index file
    let json = serde_json::to_string_pretty(&index)?;
    std::fs::write(&index_path, json)?;

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
