// src/did_index.rs
use crate::manager::DIDIndexStats;
use std::collections::HashMap;
use std::path::Path;
use anyhow::Result;

pub struct DIDIndex {
    index: HashMap<String, Vec<u32>>,
}

impl DIDIndex {
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    pub fn load_or_create(directory: &Path) -> Result<Self> {
        let index_path = directory.join("did_index.bin");
        if index_path.exists() {
            Self::load(&index_path)
        } else {
            Ok(Self::new())
        }
    }

    pub fn add_operation(&mut self, bundle: u32, did: &str) {
        self.index.entry(did.to_string())
            .or_insert_with(Vec::new)
            .push(bundle);
    }

    pub fn get_bundles_for_did(&self, did: &str) -> Result<Vec<u32>> {
        Ok(self.index.get(did).cloned().unwrap_or_default())
    }

    pub fn get_stats(&self) -> DIDIndexStats {
        let total_entries: usize = self.index.values().map(|v| v.len()).sum();
        DIDIndexStats {
            total_dids: self.index.len(),
            total_entries,
            avg_operations_per_did: if self.index.is_empty() {
                0.0
            } else {
                total_entries as f64 / self.index.len() as f64
            },
        }
    }

    pub fn save(&self, _directory: &Path) -> Result<()> {
        // TODO: Implement serialization
        Ok(())
    }

    fn load(_path: &Path) -> Result<Self> {
        // TODO: Implement deserialization
        Ok(Self::new())
    }
}
