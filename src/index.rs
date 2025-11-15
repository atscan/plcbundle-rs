// Replace your current src/index.rs with this:

use anyhow::Result;
use serde::{Deserialize, Serialize}; // Add Serialize here
use std::fs::File;
use std::path::Path;

#[derive(Debug, Deserialize, Serialize, Clone)] // Add Clone here
pub struct Index {
    pub version: String,
    pub origin: String,
    pub last_bundle: u32,
    pub updated_at: String,
    pub total_size_bytes: u64,
    pub total_uncompressed_size_bytes: u64,
    pub bundles: Vec<BundleMetadata>,
}

#[derive(Debug, Deserialize, Serialize, Clone)] // Add Serialize here
pub struct BundleMetadata {
    pub bundle_number: u32,
    pub start_time: String,
    pub end_time: String,
    pub operation_count: u32,
    pub did_count: u32,
    pub hash: String,
    pub content_hash: String,
    #[serde(default)]
    pub parent: String, // Empty string for first bundle
    pub compressed_hash: String,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    #[serde(default)]
    pub cursor: String, // Empty string for first bundle
    pub created_at: String,
}

impl Index {
    pub fn load<P: AsRef<Path>>(directory: P) -> Result<Self> {
        let index_path = directory.as_ref().join("plc_bundles.json");
        let file = File::open(&index_path)?;
        Ok(sonic_rs::from_reader(file)?)
    }

    /// Save index to disk atomically
    pub fn save<P: AsRef<Path>>(&self, directory: P) -> Result<()> {
        use anyhow::Context;

        let index_path = directory.as_ref().join("plc_bundles.json");
        let temp_path = index_path.with_extension("json.tmp");

        let json = serde_json::to_string_pretty(self)
            .context("Failed to serialize index")?;

        std::fs::write(&temp_path, json)
            .with_context(|| format!("Failed to write temp index: {}", temp_path.display()))?;

        std::fs::rename(&temp_path, &index_path)
            .with_context(|| format!("Failed to rename index: {}", index_path.display()))?;

        Ok(())
    }

    /// Initialize a new repository with an empty index
    ///
    /// Creates all necessary directories and an empty index file.
    /// This is idempotent - if the repository already exists, it will return an error
    /// unless `force` is true, in which case it will reinitialize.
    ///
    /// # Arguments
    /// * `directory` - Directory to initialize
    /// * `origin` - PLC directory URL or origin identifier
    /// * `force` - Whether to reinitialize if already exists
    ///
    /// # Returns
    /// True if initialized (created new), False if already existed and force=false
    pub fn init<P: AsRef<Path>>(directory: P, origin: String, force: bool) -> Result<bool> {
        use anyhow::Context;

        let dir = directory.as_ref();
        let index_path = dir.join("plc_bundles.json");

        // Check if already initialized
        if index_path.exists() && !force {
            return Ok(false); // Already initialized
        }

        // Create directory if it doesn't exist
        if !dir.exists() {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("Failed to create directory: {}", dir.display()))?;
        }

        // Create .plcbundle directory for DID index
        let plcbundle_dir = dir.join(crate::constants::DID_INDEX_DIR);
        if !plcbundle_dir.exists() {
            std::fs::create_dir_all(&plcbundle_dir)
                .with_context(|| format!("Failed to create DID index directory: {}", plcbundle_dir.display()))?;
        }

        // Create and save empty index
        let index = Index {
            version: "1.0".to_string(),
            origin,
            last_bundle: 0,
            updated_at: chrono::Utc::now().to_rfc3339(),
            total_size_bytes: 0,
            total_uncompressed_size_bytes: 0,
            bundles: Vec::new(),
        };

        index.save(dir)?;

        Ok(true) // Successfully initialized
    }

    /// Rebuild index from existing bundle files by scanning their metadata
    ///
    /// This scans all .jsonl.zst files in the directory and reconstructs the index
    /// by extracting embedded metadata from each bundle's skippable frame.
    ///
    /// # Arguments
    /// * `directory` - Directory containing bundle files
    /// * `origin` - Optional origin URL (auto-detected from first bundle if None)
    /// * `progress_cb` - Optional progress callback (current, total)
    ///
    /// # Returns
    /// Reconstructed Index ready to be saved
    pub fn rebuild_from_bundles<P: AsRef<Path>, F>(
        directory: P,
        origin: Option<String>,
        progress_cb: Option<F>,
    ) -> Result<Self>
    where
        F: Fn(usize, usize),
    {
        use anyhow::Context;

        let dir = directory.as_ref();

        // Find all bundle files
        let mut bundle_files: Vec<(u32, std::path::PathBuf)> = Vec::new();

        for entry in std::fs::read_dir(dir)
            .with_context(|| format!("Failed to read directory: {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            let filename = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");

            // Match pattern: NNNNNN.jsonl.zst (16 chars: 6 digits + 10 chars for .jsonl.zst)
            if filename.ends_with(".jsonl.zst") && filename.len() == 16 {
                if let Ok(bundle_num) = filename[0..6].parse::<u32>() {
                    bundle_files.push((bundle_num, path));
                }
            }
        }

        if bundle_files.is_empty() {
            anyhow::bail!("No bundle files found in directory");
        }

        // Sort by bundle number
        bundle_files.sort_by_key(|(num, _)| *num);

        // Extract metadata from each bundle
        let mut bundles_metadata: Vec<BundleMetadata> = Vec::new();
        let mut total_compressed_size = 0u64;
        let mut total_uncompressed_size = 0u64;
        let mut detected_origin: Option<String> = origin;

        for (i, (bundle_num, bundle_path)) in bundle_files.iter().enumerate() {
            if let Some(ref cb) = progress_cb {
                cb(i + 1, bundle_files.len());
            }

            // Extract embedded metadata from bundle file
            let embedded = crate::bundle_format::extract_metadata_from_file(bundle_path)
                .with_context(|| format!("Failed to extract metadata from bundle {:06}", bundle_num))?;

            // Auto-detect origin from first bundle if not provided
            if detected_origin.is_none() {
                detected_origin = Some(embedded.origin.clone());
            }

            // Verify origin matches
            if let Some(ref expected_origin) = detected_origin {
                if embedded.origin != *expected_origin {
                    anyhow::bail!(
                        "Bundle {:06}: origin mismatch (expected '{}', got '{}')",
                        bundle_num,
                        expected_origin,
                        embedded.origin
                    );
                }
            }

            // Get file size
            let metadata = std::fs::metadata(bundle_path)?;
            let compressed_size = metadata.len();

            // Calculate compressed hash
            let compressed_data = std::fs::read(bundle_path)?;
            let compressed_hash = {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(&compressed_data);
                format!("{:x}", hasher.finalize())
            };

            // Build bundle metadata for index
            let bundle_meta = BundleMetadata {
                bundle_number: *bundle_num,
                start_time: embedded.start_time.clone(),
                end_time: embedded.end_time.clone(),
                operation_count: embedded.operation_count as u32,
                did_count: embedded.did_count as u32,
                hash: String::new(), // Will be calculated after collecting all
                content_hash: embedded.content_hash.clone(),
                parent: String::new(), // Will be set during chain hash calculation
                compressed_hash,
                compressed_size,
                uncompressed_size: 0, // Will need to decompress to get exact size
                cursor: String::new(), // Will be set from previous bundle's end_time
                created_at: embedded.created_at.clone(),
            };

            total_compressed_size += compressed_size;
            bundles_metadata.push(bundle_meta);
        }

        // Calculate uncompressed sizes and chain hashes
        for i in 0..bundles_metadata.len() {
            if let Some(ref cb) = progress_cb {
                cb(i + 1, bundles_metadata.len());
            }

            // Get uncompressed size by loading the bundle
            let bundle_num = bundles_metadata[i].bundle_number;
            let bundle_path = crate::constants::bundle_path(dir, bundle_num);

            if let Ok(ops) = crate::bundle_format::load_bundle_as_json_strings(&bundle_path) {
                let uncompressed_jsonl: String = ops.iter()
                    .map(|op| format!("{}\n", op))
                    .collect();
                bundles_metadata[i].uncompressed_size = uncompressed_jsonl.len() as u64;
                total_uncompressed_size += bundles_metadata[i].uncompressed_size;
            }

            // Set parent hash and calculate chain hash
            if i == 0 {
                // Genesis bundle
                use sha2::{Digest, Sha256};
                bundles_metadata[i].parent = String::new();
                bundles_metadata[i].cursor = String::new();
                let content_hash = bundles_metadata[i].content_hash.clone();
                let chain_input = format!("plcbundle:genesis:{}", content_hash);
                let mut hasher = Sha256::new();
                hasher.update(chain_input.as_bytes());
                bundles_metadata[i].hash = format!("{:x}", hasher.finalize());
            } else {
                use sha2::{Digest, Sha256};
                let prev_hash = bundles_metadata[i - 1].hash.clone();
                let prev_end_time = bundles_metadata[i - 1].end_time.clone();
                let content_hash = bundles_metadata[i].content_hash.clone();

                bundles_metadata[i].parent = prev_hash.clone();
                bundles_metadata[i].cursor = prev_end_time;

                let chain_input = format!("{}:{}", prev_hash, content_hash);
                let mut hasher = Sha256::new();
                hasher.update(chain_input.as_bytes());
                bundles_metadata[i].hash = format!("{:x}", hasher.finalize());
            }
        }

        let last_bundle = bundles_metadata.last().unwrap().bundle_number;
        let origin_str = detected_origin.unwrap_or_else(|| crate::constants::DEFAULT_ORIGIN.to_string());

        Ok(Index {
            version: "1.0".to_string(),
            origin: origin_str,
            last_bundle,
            updated_at: chrono::Utc::now().to_rfc3339(),
            total_size_bytes: total_compressed_size,
            total_uncompressed_size_bytes: total_uncompressed_size,
            bundles: bundles_metadata,
        })
    }

    pub fn get_bundle(&self, bundle_number: u32) -> Option<&BundleMetadata> {
        self.bundles
            .iter()
            .find(|b| b.bundle_number == bundle_number)
    }

    /// Calculate total uncompressed size for a set of bundle numbers.
    /// Optimizes by using the pre-calculated total when all bundles are selected.
    ///
    /// # Arguments
    /// * `bundle_numbers` - Vector of bundle numbers to calculate size for
    ///
    /// # Returns
    /// Total uncompressed size in bytes
    pub fn total_uncompressed_size_for_bundles(&self, bundle_numbers: &[u32]) -> u64 {
        // Check if we're querying all bundles (1 to last_bundle)
        let is_all_bundles = !bundle_numbers.is_empty()
            && bundle_numbers.len() == self.last_bundle as usize
            && bundle_numbers.first() == Some(&1)
            && bundle_numbers.last() == Some(&self.last_bundle);

        if is_all_bundles {
            // Use pre-calculated total from index
            self.total_uncompressed_size_bytes
        } else {
            // Sum only the selected bundles
            bundle_numbers
                .iter()
                .filter_map(|bundle_num| {
                    self.get_bundle(*bundle_num).map(|meta| meta.uncompressed_size)
                })
                .sum()
        }
    }
}
