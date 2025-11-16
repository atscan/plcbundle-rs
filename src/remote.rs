// Remote access module for fetching data from remote PLC instances
use crate::constants;
use crate::index::Index;
use crate::operations::Operation;
use crate::resolver::DIDDocument;
use crate::sync::PLCOperation;
use anyhow::{Context, Result};
use std::time::Duration;

/// Fetch index from remote URL or local file path
pub async fn fetch_index(target: &str) -> Result<Index> {
    if target.starts_with("http://") || target.starts_with("https://") {
        fetch_index_from_url(target).await
    } else {
        // Local file or directory path
        use std::path::Path;
        let path = Path::new(target);
        if path.is_file() {
            // Direct file path
            let file = std::fs::File::open(path)?;
            Ok(sonic_rs::from_reader(file)?)
        } else {
            // Directory path - look for index file
            Index::load(path)
        }
    }
}

/// Fetch index from remote URL
async fn fetch_index_from_url(url: &str) -> Result<Index> {
    let mut url = url.to_string();

    // Normalize URL - add /index.json or /plc_bundles.json if needed
    if !url.ends_with(".json") {
        url = url.trim_end_matches('/').to_string();
        // Try both common index file names
        if !url.ends_with("/index.json") && !url.ends_with("/plc_bundles.json") {
            url.push_str("/index.json");
        }
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let response = client
        .get(&url)
        .header("User-Agent", constants::user_agent())
        .send()
        .await
        .context("Failed to fetch index")?;

    if !response.status().is_success() {
        anyhow::bail!("Unexpected status code: {}", response.status());
    }

    let data = response.text().await?;
    let index: Index = sonic_rs::from_str(&data).context("Failed to parse index JSON")?;

    Ok(index)
}

/// Fetch bundle operations from remote URL
pub async fn fetch_bundle_operations(base_url: &str, bundle_num: u32) -> Result<Vec<Operation>> {
    // Normalize base URL
    let mut url = base_url.trim_end_matches('/').to_string();

    // Remove index.json or plc_bundles.json if present
    url = url.trim_end_matches("/index.json").to_string();
    url = url.trim_end_matches("/plc_bundles.json").to_string();

    // Construct bundle URL - try common patterns
    let bundle_url = format!("{}/jsonl/{}", url, bundle_num);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;

    let response = client
        .get(&bundle_url)
        .header("User-Agent", constants::user_agent())
        .send()
        .await
        .context(format!(
            "Failed to fetch bundle {} from {}",
            bundle_num, bundle_url
        ))?;

    if !response.status().is_success() {
        anyhow::bail!(
            "Unexpected status code: {} for bundle {}",
            response.status(),
            bundle_num
        );
    }

    let body = response.text().await?;
    let mut operations = Vec::new();

    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }

        match sonic_rs::from_str::<PLCOperation>(line) {
            Ok(plc_op) => {
                let op: Operation = plc_op.into();
                operations.push(op);
            }
            Err(e) => {
                eprintln!("Warning: failed to parse operation: {}", e);
            }
        }
    }

    Ok(operations)
}

/// Fetch a single operation from remote URL
pub async fn fetch_operation(base_url: &str, bundle_num: u32, position: usize) -> Result<String> {
    // For now, fetch the whole bundle and return the specific operation
    // This could be optimized if the remote API supports direct operation access
    let operations = fetch_bundle_operations(base_url, bundle_num).await?;

    if position >= operations.len() {
        anyhow::bail!(
            "Position {} out of bounds (bundle has {} operations)",
            position,
            operations.len()
        );
    }

    // Return raw JSON if available, otherwise serialize
    if let Some(ref raw) = operations[position].raw_json {
        Ok(raw.clone())
    } else {
        sonic_rs::to_string(&operations[position]).context("Failed to serialize operation")
    }
}

/// Fetch DID document from remote PLC directory
pub async fn fetch_did_document(base_url: &str, did: &str) -> Result<DIDDocument> {
    // Normalize base URL
    let url = base_url.trim_end_matches('/').to_string();

    // Construct DID document URL
    // PLC directory typically exposes DID documents at /did/{did}
    let did_url = format!("{}/did/{}", url, did);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let response = client
        .get(&did_url)
        .header("User-Agent", constants::user_agent())
        .send()
        .await
        .context(format!("Failed to fetch DID document from {}", did_url))?;

    if !response.status().is_success() {
        anyhow::bail!(
            "Unexpected status code: {} for DID document at {}",
            response.status(),
            did_url
        );
    }

    let data = response.text().await?;
    let document: DIDDocument = sonic_rs::from_str(&data)
        .context("Failed to parse DID document JSON")?;

    Ok(document)
}
