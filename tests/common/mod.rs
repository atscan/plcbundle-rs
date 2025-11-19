use anyhow::Result;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

use plcbundle::BundleManager;
use plcbundle::Operation;
use plcbundle::index::{BundleMetadata, Index};

pub fn setup_manager(dir: &PathBuf) -> Result<BundleManager> {
    Index::init(dir, "http://localhost:1234".to_string(), true)?;
    let manager = BundleManager::new(dir.clone(), ())?;
    Ok(manager)
}

pub fn setup_temp_dir() -> Result<TempDir> {
    tempfile::tempdir().map_err(anyhow::Error::from)
}

#[cfg(feature = "server")]
#[allow(dead_code)]
pub async fn start_test_server(
    manager: Arc<BundleManager>,
    port: u16,
) -> Result<tokio::task::JoinHandle<()>> {
    let config = plcbundle::server::ServerConfig {
        sync_mode: false,
        sync_interval_seconds: 0,
        enable_websocket: false,
        enable_resolver: true,
        version: "test".to_string(),
    };
    let server = plcbundle::server::Server::new(manager, config);
    let app = server.router();
    let addr = format!("127.0.0.1:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Ok(server_handle)
}

#[cfg(not(feature = "server"))]
#[allow(dead_code)]
pub async fn start_test_server(
    _manager: Arc<BundleManager>,
    _port: u16,
) -> Result<tokio::task::JoinHandle<()>> {
    anyhow::bail!("server feature not enabled for tests");
}

pub fn add_dummy_bundle(dir: &PathBuf) -> Result<()> {
    let operations: Vec<Operation> = (0..10)
        .map(|i| {
            // Generate a valid DID identifier (24 chars, lowercase a-z)
            let mut id = "a".repeat(24);
            let last_char = ((b'a' + (i as u8 % 26)) as char).to_string();
            id.replace_range(23..24, &last_char);
            let did = format!("did:plc:{}", id);
            Operation::from_json(&format!(
                "{{ \"did\": \"{}\", \"operation\": {{ \"type\": \"create\" }}, \"cid\": \"bafy...\", \"nullified\": false, \"created_at\": \"2024-01-01T00:00:0{}Z\" }}",
                did,
                i
            ))
            .unwrap()
        })
        .collect();

    let jsonl = operations
        .iter()
        .map(|op| serde_json::to_string(op).unwrap())
        .collect::<Vec<String>>()
        .join("\n");

    let compressed_bundle = zstd::encode_all(jsonl.as_bytes(), 0)?;
    let bundle_path = dir.join("000001.jsonl.zst");
    let mut file = std::fs::File::create(&bundle_path)?;
    file.write_all(&compressed_bundle)?;

    let metadata = BundleMetadata {
        bundle_number: 1,
        operation_count: operations.len() as u32,
        did_count: operations.len() as u32,
        compressed_size: compressed_bundle.len() as u64,
        uncompressed_size: jsonl.len() as u64,
        start_time: "2024-01-01T00:00:00Z".to_string(),
        end_time: "2024-01-01T00:00:09Z".to_string(),
        hash: "dummy_hash".to_string(),
        parent: String::new(),
        content_hash: "dummy_content_hash".to_string(),
        compressed_hash: "dummy_compressed_hash".to_string(),
        cursor: "dummy_cursor".to_string(),
        created_at: "2024-01-01T00:00:00Z".to_string(),
    };

    let mut index = Index::load(dir)?;
    index.bundles.push(metadata);
    index.last_bundle = 1;
    index.save(dir)?;

    Ok(())
}
