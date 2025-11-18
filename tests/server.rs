mod common;

use anyhow::Result;
use std::sync::Arc;

#[tokio::test]
async fn test_server_endpoints() -> Result<()> {
    let dir = common::setup_temp_dir()?;
    let manager = common::setup_manager(&dir.path().to_path_buf())?;
    let manager = Arc::new(manager);
    let port = 3030;
    let server_handle = common::start_test_server(Arc::clone(&manager), port).await?;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{}", port);

    // Test root endpoint
    let res = client.get(format!("{}/", base_url)).send().await?;
    assert!(res.status().is_success());
    let body = res.text().await?;
    assert!(body.contains("plcbundle server"));

    // Test status endpoint
    let res = client.get(format!("{}/status", base_url)).send().await?;
    assert!(res.status().is_success());
    let json: serde_json::Value = res.json().await?;
    assert_eq!(json["server"]["version"], "test");

    // Test debug endpoints
    let debug_endpoints = vec!["memory", "didindex", "resolver"];
    for endpoint in debug_endpoints {
        let url = format!("{}/debug/{}", base_url, endpoint);
        let res = client.get(&url).send().await?;
        assert!(res.status().is_success());
        if endpoint == "memory" {
            let body = res.text().await?;
            assert!(body.contains("Memory Stats"));
        } else {
            let _: serde_json::Value = res.json().await?;
        }
    }

    server_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_server_did_endpoints() -> Result<()> {
    let dir = common::setup_temp_dir()?;
    let dir_path = dir.path().to_path_buf();

    // Initialize repo and add a dummy bundle with DID operations
    common::setup_manager(&dir_path)?;
    common::add_dummy_bundle(&dir_path)?;

    // Create manager that loads the index and build DID index for lookups
    let manager = plcbundle::BundleManager::new(dir_path, ())?;
    let manager = Arc::new(manager);
    // Build DID index so the resolver can find operations in bundles
    manager.batch_update_did_index_async(1, manager.get_last_bundle()).await?;
    let port = 3032;
    let server_handle = common::start_test_server(Arc::clone(&manager), port).await?;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{}", port);

    // Test DID document endpoint (first DID from dummy bundle)
    let first_did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    let res = client.get(format!("{}/{}", base_url, first_did)).send().await?;
    let status = res.status();
    let header_x_request_type = res
        .headers()
        .get("x-request-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let header_x_did = res
        .headers()
        .get("x-did")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let body_text = res.text().await?;
    if !status.is_success() {
        panic!("DID document request failed: {}\n{}", status, body_text);
    }
    assert_eq!(header_x_request_type.unwrap(), "did");
    assert_eq!(header_x_did.unwrap(), first_did);
    let json: serde_json::Value = serde_json::from_str(&body_text)?;
    assert_eq!(json["id"], first_did);

    // Test DID data endpoint
    let res = client.get(format!("{}/did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/data", base_url)).send().await?;
    let status = res.status();
    let body_text = res.text().await?;
    if !status.is_success() {
        panic!("DID data request failed: {}\n{}", status, body_text);
    }
    let json: serde_json::Value = serde_json::from_str(&body_text)?;
    assert_eq!(json["did"], "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");

    // Test DID audit log endpoint
    let res = client.get(format!("{}/did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/log/audit", base_url)).send().await?;
    let status = res.status();
    let body_text = res.text().await?;
    if !status.is_success() {
        panic!("DID audit log request failed: {}\n{}", status, body_text);
    }
    let json: serde_json::Value = serde_json::from_str(&body_text)?;
    assert!(json.is_array());
    assert!(json.as_array().unwrap().len() >= 1);
    assert_eq!(json.as_array().unwrap()[0]["did"], "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");

    server_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_server_data_endpoints() -> Result<()> {
    let dir = common::setup_temp_dir()?;
    let dir_path = dir.path().to_path_buf();

    // 1. Initialize repo
    common::setup_manager(&dir_path)?;

    // 2. Add dummy bundle to the index file
    common::add_dummy_bundle(&dir_path)?;

    // 3. Create a new manager that will load the modified index
    let manager = plcbundle::BundleManager::new(dir_path, ())?;
    let manager = Arc::new(manager);
    // Ensure DID index is available for data/op lookups
    manager.batch_update_did_index_async(1, manager.get_last_bundle()).await?;
    let port = 3031;
    let server_handle = common::start_test_server(Arc::clone(&manager), port).await?;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{}", port);

    // Test index.json endpoint
    let res = client.get(format!("{}/index.json", base_url)).send().await?;
    println!("Testing endpoint: /index.json, status: {}", res.status());
    assert!(res.status().is_success());
    let json: serde_json::Value = res.json().await?;
    assert_eq!(json["bundles"].as_array().unwrap().len(), 1);

    // Test bundle endpoint
    let res = client.get(format!("{}/bundle/1", base_url)).send().await?;
    println!("Testing endpoint: /bundle/1, status: {}", res.status());
    assert!(res.status().is_success());
    let json: serde_json::Value = res.json().await?;
    assert_eq!(json["bundle_number"], 1);

    // Test data endpoint
    let res = client.get(format!("{}/data/1", base_url)).send().await?;
    println!("Testing endpoint: /data/1, status: {}", res.status());
    assert!(res.status().is_success());
    let body = res.bytes().await?;
    assert!(!body.is_empty());

    // Test jsonl endpoint
    let res = client.get(format!("{}/jsonl/1", base_url)).send().await?;
    println!("Testing endpoint: /jsonl/1, status: {}", res.status());
    assert!(res.status().is_success());
    let body = res.text().await?;
    assert!(body.contains("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"));

    // Test op endpoint
    let res = client.get(format!("{}/op/1:0", base_url)).send().await?;
    println!("Testing endpoint: /op/1:0, status: {}", res.status());
    assert!(res.status().is_success());
    let json: serde_json::Value = res.json().await?;
    assert_eq!(json["did"], "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");

    // Test mempool endpoint (not available when sync_mode=false)
    let res = client.get(format!("{}/mempool", base_url)).send().await?;
    println!("Testing endpoint: /mempool, status: {}", res.status());
    assert_eq!(res.status().as_u16(), 404);

    server_handle.abort();
    Ok(())
}