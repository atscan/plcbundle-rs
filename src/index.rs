// Replace your current src/index.rs with this:

use anyhow::Result;
use serde::{Deserialize, Serialize};  // Add Serialize here
use std::fs::File;
use std::path::Path;

#[derive(Debug, Deserialize, Serialize)]  // Add Serialize here
pub struct Index {
    pub version: String,
    pub origin: String,
    pub last_bundle: u32,
    pub updated_at: String,
    pub total_size_bytes: u64,
    pub total_uncompressed_size_bytes: u64,
    pub bundles: Vec<BundleMetadata>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]  // Add Serialize here
pub struct BundleMetadata {
    pub bundle_number: u32,
    pub start_time: String,
    pub end_time: String,
    pub operation_count: u32,
    pub did_count: u32,
    pub hash: String,
    pub content_hash: String,
    #[serde(default)]
    pub parent: String,  // Empty string for first bundle
    pub compressed_hash: String,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    #[serde(default)]
    pub cursor: String,  // Empty string for first bundle
    pub created_at: String,
}

impl Index {
    pub fn load<P: AsRef<Path>>(directory: P) -> Result<Self> {
        let index_path = directory.as_ref().join("plc_bundles.json");
        let file = File::open(&index_path)?;
        Ok(sonic_rs::from_reader(file)?)
    }

    pub fn get_bundle(&self, bundle_number: u32) -> Option<&BundleMetadata> {
        self.bundles.iter().find(|b| b.bundle_number == bundle_number)
    }
}