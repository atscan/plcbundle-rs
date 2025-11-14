use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct InitCommand {
    /// Directory to initialize (default: current directory)
    #[arg(default_value = ".")]
    pub dir: PathBuf,

    /// Origin identifier for this repository
    #[arg(long, default_value = "local")]
    pub origin: String,

    /// Force initialization even if directory already exists
    #[arg(short, long)]
    pub force: bool,
}

pub fn run(cmd: InitCommand) -> Result<()> {
    let dir = cmd.dir.canonicalize().unwrap_or(cmd.dir.clone());
    let index_path = dir.join("plc_bundles.json");

    // Check if already initialized
    if index_path.exists() && !cmd.force {
        eprintln!("Repository already initialized at: {}", dir.display());
        eprintln!("Use --force to reinitialize");
        return Ok(());
    }

    // Create directory if it doesn't exist
    if !dir.exists() {
        std::fs::create_dir_all(&dir)?;
        println!("Created directory: {}", dir.display());
    }

    // Create empty index
    let index = serde_json::json!({
        "version": "1.0",
        "origin": cmd.origin,
        "last_bundle": 0,
        "updated_at": chrono::Utc::now().to_rfc3339(),
        "total_size_bytes": 0,
        "total_uncompressed_size_bytes": 0,
        "bundles": []
    });

    // Write index file
    let json = serde_json::to_string_pretty(&index)?;
    std::fs::write(&index_path, json)?;

    println!("\n✓ Initialized PLC bundle repository");
    println!("  Location: {}", dir.display());
    println!("  Origin:   {}", cmd.origin);
    println!("  Index:    plc_bundles.json");
    println!("\nNext steps:");
    println!("  plcbundle-rs sync           # Fetch bundles from PLC directory");
    println!("  plcbundle-rs info           # Show repository info");
    println!("  plcbundle-rs mempool status # Check mempool status");

    Ok(())
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
            origin: "test".to_string(),
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
            origin: "first".to_string(),
            force: false,
        };
        run(cmd).unwrap();

        // Second init without force
        let cmd = InitCommand {
            dir: temp.path().to_path_buf(),
            origin: "second".to_string(),
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
            origin: "first".to_string(),
            force: false,
        };
        run(cmd).unwrap();

        // Second init with force
        let cmd = InitCommand {
            dir: temp.path().to_path_buf(),
            origin: "second".to_string(),
            force: true,
        };
        run(cmd).unwrap();

        let index_path = temp.path().join("plc_bundles.json");
        let content = std::fs::read_to_string(index_path).unwrap();
        assert!(content.contains("\"origin\": \"second\"")); // Overwritten
    }
}
