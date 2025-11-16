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
        let display_path = index_path.canonicalize().unwrap_or_else(|_| index_path.clone());
        log::debug!("[BundleManager] Loading index from: {}", display_path.display());
        let start = std::time::Instant::now();
        let file = File::open(&index_path)?;
        let index: Index = sonic_rs::from_reader(file)?;
        let elapsed = start.elapsed();
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
        log::debug!(
            "[BundleManager] Index loaded: v{} ({}), {} bundles, last bundle: {} ({:.3}ms)",
            index.version,
            index.origin,
            index.bundles.len(),
            index.last_bundle,
            elapsed_ms
        );

        // Validate bundle sequence integrity
        index.validate_bundle_sequence()?;

        Ok(index)
    }

    /// Validate that bundles form a consecutive sequence starting from 1
    /// This ensures no gaps in the bundle chain and that the first bundle is always 1
    fn validate_bundle_sequence(&self) -> Result<()> {
        if self.bundles.is_empty() {
            return Ok(()); // Empty index is valid
        }

        // First bundle must be 1
        let first_bundle = self.bundles.first().unwrap().bundle_number;
        if first_bundle != 1 {
            anyhow::bail!(
                "Invalid bundle sequence: first bundle is {} but must be 1",
                first_bundle
            );
        }

        // Check for gaps in sequence
        for i in 0..self.bundles.len() {
            let expected = (i + 1) as u32;
            let actual = self.bundles[i].bundle_number;
            if actual != expected {
                anyhow::bail!(
                    "Gap detected in bundle sequence: expected bundle {}, found bundle {}",
                    expected,
                    actual
                );
            }
        }

        // Verify last_bundle matches the last bundle in the array
        let last_in_array = self.bundles.last().unwrap().bundle_number;
        if self.last_bundle != last_in_array {
            anyhow::bail!(
                "Inconsistent last_bundle: index says {}, but last bundle in array is {}",
                self.last_bundle,
                last_in_array
            );
        }

        Ok(())
    }

    /// Save index to disk atomically
    pub fn save<P: AsRef<Path>>(&self, directory: P) -> Result<()> {
        use anyhow::Context;

        // Validate bundle sequence before saving
        self.validate_bundle_sequence()
            .context("Cannot save invalid index")?;

        let index_path = directory.as_ref().join("plc_bundles.json");
        let temp_path = index_path.with_extension("json.tmp");

        let json = sonic_rs::to_string_pretty(self)
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
        F: Fn(usize, usize, u64, u64) + Send + Sync, // (current, total, bytes_processed, total_bytes)
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

        // Validate that first bundle is 1
        let first_bundle_num = bundle_files[0].0;
        if first_bundle_num != 1 {
            anyhow::bail!(
                "Invalid bundle sequence: first bundle file is {:06}.jsonl.zst but must be 000001.jsonl.zst",
                first_bundle_num
            );
        }

        // Validate no gaps in bundle sequence
        for i in 0..bundle_files.len() {
            let expected = (i + 1) as u32;
            let actual = bundle_files[i].0;
            if actual != expected {
                anyhow::bail!(
                    "Gap detected in bundle files: expected {:06}.jsonl.zst, found {:06}.jsonl.zst",
                    expected,
                    actual
                );
            }
        }

        // Pre-calculate total bytes for progress tracking (parallel)
        use rayon::prelude::*;
        let total_bytes: u64 = bundle_files
            .par_iter()
            .filter_map(|(_, bundle_path)| std::fs::metadata(bundle_path).ok())
            .map(|metadata| metadata.len())
            .sum();

        // Extract metadata from each bundle in parallel
        use std::sync::{Arc, Mutex};
        use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
        
        let detected_origin: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(origin));
        let progress_cb_arc: Arc<Mutex<Option<F>>> = Arc::new(Mutex::new(progress_cb));
        let count_atomic = Arc::new(AtomicUsize::new(0));
        let bytes_atomic = Arc::new(AtomicU64::new(0));
        
        // Update progress bar less frequently to reduce contention
        let update_interval = (rayon::current_num_threads().max(1) * 4).max(10);

        // Process bundles in parallel
        let bundle_count = bundle_files.len();
        let mut bundles_metadata: Vec<BundleMetadata> = bundle_files
            .par_iter()
            .map(|(bundle_num, bundle_path)| -> Result<BundleMetadata> {
                // Get file size
                let metadata = std::fs::metadata(bundle_path)?;
                let compressed_size = metadata.len();
                let bytes_processed = bytes_atomic.fetch_add(compressed_size, Ordering::Relaxed) + compressed_size;
                let current_count = count_atomic.fetch_add(1, Ordering::Relaxed) + 1;

                // Update progress periodically
                if current_count % update_interval == 0 || current_count == 1 || current_count == bundle_count {
                    if let Ok(cb_guard) = progress_cb_arc.lock() {
                        if let Some(ref cb) = *cb_guard {
                            cb(current_count, bundle_count, bytes_processed, total_bytes);
                        }
                    }
                }

                // Extract embedded metadata from bundle file
                let embedded = crate::bundle_format::extract_metadata_from_file(bundle_path)
                    .with_context(|| format!("Failed to extract metadata from bundle {:06}", bundle_num))?;

                // Auto-detect origin from first bundle if not provided
                {
                    let mut origin_guard = detected_origin.lock().unwrap();
                    if origin_guard.is_none() {
                        *origin_guard = Some(embedded.origin.clone());
                    }
                }

                // Verify origin matches
                {
                    let origin_guard = detected_origin.lock().unwrap();
                    if let Some(ref expected_origin) = *origin_guard {
                        if embedded.origin != *expected_origin {
                            anyhow::bail!(
                                "Bundle {:06}: origin mismatch (expected '{}', got '{}')",
                                bundle_num,
                                expected_origin,
                                embedded.origin
                            );
                        }
                    }
                }

                // Calculate compressed hash
                let compressed_data = std::fs::read(bundle_path)?;
                let compressed_hash = {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(&compressed_data);
                    format!("{:x}", hasher.finalize())
                };

                // Get uncompressed_size from embedded metadata if available
                // Fall back to 0 for legacy bundles without this field
                let uncompressed_size = embedded.uncompressed_size.unwrap_or(0);

                // Build bundle metadata for index
                Ok(BundleMetadata {
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
                    uncompressed_size,
                    cursor: String::new(), // Will be set from previous bundle's end_time
                    created_at: embedded.created_at.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Sort by bundle number to maintain order (parallel processing may reorder)
        bundles_metadata.sort_by_key(|b| b.bundle_number);

        let total_compressed_size: u64 = bundles_metadata.iter().map(|b| b.compressed_size).sum();
        let detected_origin = detected_origin.lock().unwrap().clone();

        // Calculate total uncompressed size
        // Note: For legacy bundles without uncompressed_size in metadata, it will be 0
        let total_uncompressed_size: u64 = bundles_metadata.iter().map(|b| b.uncompressed_size).sum();

        // Calculate chain hashes sequentially (depends on previous bundles)
        for i in 0..bundles_metadata.len() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_empty_index() {
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 0,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 0,
            total_uncompressed_size_bytes: 0,
            bundles: Vec::new(),
        };

        assert!(index.validate_bundle_sequence().is_ok());
    }

    #[test]
    fn test_validate_single_bundle_correct() {
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 1,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 100,
            total_uncompressed_size_bytes: 200,
            bundles: vec![BundleMetadata {
                bundle_number: 1,
                start_time: "2024-01-01T00:00:00Z".to_string(),
                end_time: "2024-01-01T01:00:00Z".to_string(),
                operation_count: 10,
                did_count: 5,
                hash: "hash1".to_string(),
                content_hash: "content1".to_string(),
                parent: String::new(),
                compressed_hash: "comp1".to_string(),
                compressed_size: 100,
                uncompressed_size: 200,
                cursor: String::new(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
            }],
        };

        assert!(index.validate_bundle_sequence().is_ok());
    }

    #[test]
    fn test_validate_first_bundle_not_one() {
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 2,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 100,
            total_uncompressed_size_bytes: 200,
            bundles: vec![BundleMetadata {
                bundle_number: 2,
                start_time: "2024-01-01T00:00:00Z".to_string(),
                end_time: "2024-01-01T01:00:00Z".to_string(),
                operation_count: 10,
                did_count: 5,
                hash: "hash1".to_string(),
                content_hash: "content1".to_string(),
                parent: String::new(),
                compressed_hash: "comp1".to_string(),
                compressed_size: 100,
                uncompressed_size: 200,
                cursor: String::new(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
            }],
        };

        let result = index.validate_bundle_sequence();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("first bundle is 2 but must be 1"));
    }

    #[test]
    fn test_validate_gap_in_sequence() {
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 3,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 200,
            total_uncompressed_size_bytes: 400,
            bundles: vec![
                BundleMetadata {
                    bundle_number: 1,
                    start_time: "2024-01-01T00:00:00Z".to_string(),
                    end_time: "2024-01-01T01:00:00Z".to_string(),
                    operation_count: 10,
                    did_count: 5,
                    hash: "hash1".to_string(),
                    content_hash: "content1".to_string(),
                    parent: String::new(),
                    compressed_hash: "comp1".to_string(),
                    compressed_size: 100,
                    uncompressed_size: 200,
                    cursor: String::new(),
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                },
                BundleMetadata {
                    bundle_number: 3, // Gap: missing bundle 2
                    start_time: "2024-01-01T01:00:00Z".to_string(),
                    end_time: "2024-01-01T02:00:00Z".to_string(),
                    operation_count: 10,
                    did_count: 5,
                    hash: "hash3".to_string(),
                    content_hash: "content3".to_string(),
                    parent: "hash1".to_string(),
                    compressed_hash: "comp3".to_string(),
                    compressed_size: 100,
                    uncompressed_size: 200,
                    cursor: "2024-01-01T01:00:00Z".to_string(),
                    created_at: "2024-01-01T01:00:00Z".to_string(),
                },
            ],
        };

        let result = index.validate_bundle_sequence();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expected bundle 2, found bundle 3"));
    }

    #[test]
    fn test_validate_consecutive_sequence() {
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 3,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 300,
            total_uncompressed_size_bytes: 600,
            bundles: vec![
                BundleMetadata {
                    bundle_number: 1,
                    start_time: "2024-01-01T00:00:00Z".to_string(),
                    end_time: "2024-01-01T01:00:00Z".to_string(),
                    operation_count: 10,
                    did_count: 5,
                    hash: "hash1".to_string(),
                    content_hash: "content1".to_string(),
                    parent: String::new(),
                    compressed_hash: "comp1".to_string(),
                    compressed_size: 100,
                    uncompressed_size: 200,
                    cursor: String::new(),
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                },
                BundleMetadata {
                    bundle_number: 2,
                    start_time: "2024-01-01T01:00:00Z".to_string(),
                    end_time: "2024-01-01T02:00:00Z".to_string(),
                    operation_count: 10,
                    did_count: 5,
                    hash: "hash2".to_string(),
                    content_hash: "content2".to_string(),
                    parent: "hash1".to_string(),
                    compressed_hash: "comp2".to_string(),
                    compressed_size: 100,
                    uncompressed_size: 200,
                    cursor: "2024-01-01T01:00:00Z".to_string(),
                    created_at: "2024-01-01T01:00:00Z".to_string(),
                },
                BundleMetadata {
                    bundle_number: 3,
                    start_time: "2024-01-01T02:00:00Z".to_string(),
                    end_time: "2024-01-01T03:00:00Z".to_string(),
                    operation_count: 10,
                    did_count: 5,
                    hash: "hash3".to_string(),
                    content_hash: "content3".to_string(),
                    parent: "hash2".to_string(),
                    compressed_hash: "comp3".to_string(),
                    compressed_size: 100,
                    uncompressed_size: 200,
                    cursor: "2024-01-01T02:00:00Z".to_string(),
                    created_at: "2024-01-01T02:00:00Z".to_string(),
                },
            ],
        };

        assert!(index.validate_bundle_sequence().is_ok());
    }

    #[test]
    fn test_validate_last_bundle_mismatch() {
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 5, // Incorrect: actual last bundle is 2
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 200,
            total_uncompressed_size_bytes: 400,
            bundles: vec![
                BundleMetadata {
                    bundle_number: 1,
                    start_time: "2024-01-01T00:00:00Z".to_string(),
                    end_time: "2024-01-01T01:00:00Z".to_string(),
                    operation_count: 10,
                    did_count: 5,
                    hash: "hash1".to_string(),
                    content_hash: "content1".to_string(),
                    parent: String::new(),
                    compressed_hash: "comp1".to_string(),
                    compressed_size: 100,
                    uncompressed_size: 200,
                    cursor: String::new(),
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                },
                BundleMetadata {
                    bundle_number: 2,
                    start_time: "2024-01-01T01:00:00Z".to_string(),
                    end_time: "2024-01-01T02:00:00Z".to_string(),
                    operation_count: 10,
                    did_count: 5,
                    hash: "hash2".to_string(),
                    content_hash: "content2".to_string(),
                    parent: "hash1".to_string(),
                    compressed_hash: "comp2".to_string(),
                    compressed_size: 100,
                    uncompressed_size: 200,
                    cursor: "2024-01-01T01:00:00Z".to_string(),
                    created_at: "2024-01-01T01:00:00Z".to_string(),
                },
            ],
        };

        let result = index.validate_bundle_sequence();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("last_bundle: index says 5, but last bundle in array is 2"));
    }
}
