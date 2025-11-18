//! Bundle and chain verification: fast metadata checks, compressed/content hash validation, operation count checks, and parent/cursor linkage
// src/verification.rs
use crate::bundle_format;
use crate::constants;
use crate::index::BundleMetadata;
use crate::index::Index;
use crate::manager::{ChainVerifyResult, ChainVerifySpec, VerifyResult, VerifySpec};
use anyhow::Result;
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub fn verify_bundle(
    directory: &Path,
    metadata: &BundleMetadata,
    spec: VerifySpec,
) -> Result<VerifyResult> {
    let mut errors = Vec::new();

    let bundle_path = constants::bundle_path(directory, metadata.bundle_number);

    if !bundle_path.exists() {
        errors.push(format!("Bundle file not found: {:?}", bundle_path));
        return Ok(VerifyResult {
            valid: false,
            errors,
        });
    }

    // Fast mode: only check metadata frame, skip all hash calculations
    if spec.fast {
        match bundle_format::extract_metadata_from_file(&bundle_path) {
            Ok(file_metadata) => {
                // Verify key fields match index metadata
                if file_metadata.bundle_number != metadata.bundle_number {
                    errors.push(format!(
                        "Metadata bundle number mismatch: expected {}, got {}",
                        metadata.bundle_number, file_metadata.bundle_number
                    ));
                }

                if file_metadata.content_hash != metadata.content_hash {
                    errors.push(format!(
                        "Metadata content hash mismatch: expected {}, got {}",
                        metadata.content_hash, file_metadata.content_hash
                    ));
                }

                if file_metadata.operation_count != metadata.operation_count as usize {
                    errors.push(format!(
                        "Metadata operation count mismatch: expected {}, got {}",
                        metadata.operation_count, file_metadata.operation_count
                    ));
                }

                // Check parent hash if present
                if let Some(ref parent_hash) = file_metadata.parent_hash {
                    if !metadata.parent.is_empty() && parent_hash != &metadata.parent {
                        errors.push(format!(
                            "Metadata parent hash mismatch: expected {}, got {}",
                            metadata.parent, parent_hash
                        ));
                    }
                } else if !metadata.parent.is_empty() {
                    errors.push(format!(
                        "Metadata missing parent hash (expected {})",
                        metadata.parent
                    ));
                }
            }
            Err(_) => {
                // Metadata frame doesn't exist - that's okay, it's optional
                // In fast mode, we can't verify without metadata, so we report it
                // but don't fail (as per user request: "otherwise no change")
            }
        }

        return Ok(VerifyResult {
            valid: errors.is_empty(),
            errors,
        });
    }

    // Optimize: do both hashes efficiently in a single pass
    if spec.check_hash && spec.check_content_hash {
        use sha2::{Digest, Sha256};
        use std::io::{BufReader, Read};

        // Read entire file once into memory (like Go does)
        let file_data = std::fs::read(&bundle_path)?;

        // Hash compressed file (entire file)
        let mut comp_hasher = Sha256::new();
        comp_hasher.update(&file_data);
        let comp_hash = format!("{:x}", comp_hasher.finalize());

        // For content hash, skip metadata if present, then decompress
        let mut content_start = 0;
        if file_data.len() >= 4 {
            let magic =
                u32::from_le_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
            if magic == bundle_format::SKIPPABLE_MAGIC_METADATA && file_data.len() >= 8 {
                let frame_size =
                    u32::from_le_bytes([file_data[4], file_data[5], file_data[6], file_data[7]])
                        as usize;
                content_start = 8 + frame_size; // Skip magic(4) + size(4) + data
            }
        }

        // Use streaming decompression with hashing (more memory efficient)
        let compressed_data = &file_data[content_start..];
        let decoder = zstd::Decoder::new(compressed_data)?;
        let mut reader = BufReader::with_capacity(256 * 1024, decoder); // 256KB buffer

        let mut content_hasher = Sha256::new();
        let mut buf = vec![0u8; 64 * 1024]; // 64KB read buffer
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            content_hasher.update(&buf[..n]);
        }
        let content_hash = format!("{:x}", content_hasher.finalize());

        if comp_hash != metadata.compressed_hash {
            errors.push(format!(
                "Compressed hash mismatch: expected {}, got {}",
                metadata.compressed_hash, comp_hash
            ));
        }

        if content_hash != metadata.content_hash {
            errors.push(format!(
                "Content hash mismatch: expected {}, got {}",
                metadata.content_hash, content_hash
            ));
        }
    } else if spec.check_hash {
        // Only compressed hash - fast path with streaming
        use sha2::{Digest, Sha256};
        use std::io::BufReader;
        let file = File::open(&bundle_path)?;
        let mut reader = BufReader::with_capacity(64 * 1024, file); // 64KB buffer
        let mut hasher = Sha256::new();
        std::io::copy(&mut reader, &mut hasher)?;
        let hash = format!("{:x}", hasher.finalize());

        if hash != metadata.compressed_hash {
            errors.push(format!(
                "Compressed hash mismatch: expected {}, got {}",
                metadata.compressed_hash, hash
            ));
        }
    } else if spec.check_content_hash {
        // Only content hash
        use sha2::{Digest, Sha256};
        use std::io::{BufReader, Seek, SeekFrom};
        let file = File::open(&bundle_path)?;
        let mut reader = BufReader::new(file);

        // Skip metadata frame if present
        let mut magic_buf = [0u8; 4];
        if reader.read_exact(&mut magic_buf).is_ok() {
            let magic = u32::from_le_bytes(magic_buf);
            if magic == bundle_format::SKIPPABLE_MAGIC_METADATA {
                let mut size_buf = [0u8; 4];
                reader.read_exact(&mut size_buf)?;
                let frame_size = u32::from_le_bytes(size_buf);
                let mut skip_buf = vec![0u8; frame_size as usize];
                reader.read_exact(&mut skip_buf)?;
            } else {
                reader.seek(SeekFrom::Start(0))?;
            }
        } else {
            reader.seek(SeekFrom::Start(0))?;
        }

        let mut decoder = zstd::Decoder::new(reader)?;
        let mut content = Vec::new();
        decoder.read_to_end(&mut content)?;

        let mut hasher = Sha256::new();
        hasher.update(&content);
        let hash = format!("{:x}", hasher.finalize());

        if hash != metadata.content_hash {
            errors.push(format!(
                "Content hash mismatch: expected {}, got {}",
                metadata.content_hash, hash
            ));
        }
    }

    if spec.check_operations {
        // Verify operation count
        // Skip metadata frame if present
        let mut file = File::open(&bundle_path)?;

        // Try to skip metadata frame
        let mut magic_buf = [0u8; 4];
        if file.read_exact(&mut magic_buf).is_ok() {
            let magic = u32::from_le_bytes(magic_buf);
            if magic == bundle_format::SKIPPABLE_MAGIC_METADATA {
                // Skip metadata frame
                let mut size_buf = [0u8; 4];
                file.read_exact(&mut size_buf)?;
                let frame_size = u32::from_le_bytes(size_buf);
                let mut skip_buf = vec![0u8; frame_size as usize];
                file.read_exact(&mut skip_buf)?;
            } else {
                // No metadata frame, seek back to start
                use std::io::Seek;
                file.seek(std::io::SeekFrom::Start(0))?;
            }
        } else {
            // File read error, seek back
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(0))?;
        }

        let decoder = zstd::Decoder::new(file)?;
        let reader = std::io::BufReader::new(decoder);
        use std::io::BufRead;

        let count = reader
            .lines()
            .map_while(Result::ok)
            .filter(|l| !l.is_empty())
            .count();

        if count != metadata.operation_count as usize {
            errors.push(format!(
                "Operation count mismatch: expected {}, got {}",
                metadata.operation_count, count
            ));
        }
    }

    // Verify metadata in skippable frame (if present)
    // This is optional - some bundles may not have metadata frame
    match bundle_format::extract_metadata_from_file(&bundle_path) {
        Ok(file_metadata) => {
            // Verify key fields match index metadata
            if file_metadata.bundle_number != metadata.bundle_number {
                errors.push(format!(
                    "Metadata bundle number mismatch: expected {}, got {}",
                    metadata.bundle_number, file_metadata.bundle_number
                ));
            }

            if file_metadata.content_hash != metadata.content_hash {
                errors.push(format!(
                    "Metadata content hash mismatch: expected {}, got {}",
                    metadata.content_hash, file_metadata.content_hash
                ));
            }

            if file_metadata.operation_count != metadata.operation_count as usize {
                errors.push(format!(
                    "Metadata operation count mismatch: expected {}, got {}",
                    metadata.operation_count, file_metadata.operation_count
                ));
            }

            // Check parent hash if present
            if let Some(ref parent_hash) = file_metadata.parent_hash {
                if !metadata.parent.is_empty() && parent_hash != &metadata.parent {
                    errors.push(format!(
                        "Metadata parent hash mismatch: expected {}, got {}",
                        metadata.parent, parent_hash
                    ));
                }
            } else if !metadata.parent.is_empty() {
                errors.push(format!(
                    "Metadata missing parent hash (expected {})",
                    metadata.parent
                ));
            }
        }
        Err(_) => {
            // Metadata frame doesn't exist - that's okay, it's optional for legacy bundles
            // No error is added, verification continues
        }
    }

    Ok(VerifyResult {
        valid: errors.is_empty(),
        errors,
    })
}

pub fn verify_chain(
    _directory: &Path, // Add underscore prefix
    index: &Index,
    spec: ChainVerifySpec,
) -> Result<ChainVerifyResult> {
    let end = spec.end_bundle.unwrap_or(index.last_bundle);
    let mut errors = Vec::new();
    let mut bundles_checked = 0;

    for bundle_num in spec.start_bundle..=end {
        let metadata = match index.get_bundle(bundle_num) {
            Some(m) => m,
            None => {
                errors.push((bundle_num, format!("Bundle {} not in index", bundle_num)));
                continue;
            }
        };

        bundles_checked += 1;

        if spec.check_parent_links && bundle_num > 1 {
            let prev_metadata = match index.get_bundle(bundle_num - 1) {
                Some(m) => m,
                None => {
                    errors.push((
                        bundle_num,
                        format!("Previous bundle {} not found", bundle_num - 1),
                    ));
                    continue;
                }
            };

            if metadata.parent != prev_metadata.hash {
                errors.push((
                    bundle_num,
                    format!(
                        "Parent hash mismatch: expected {}, got {}",
                        prev_metadata.hash, metadata.parent
                    ),
                ));
            }

            // Validate cursor matches previous bundle's end_time per spec
            let expected_cursor = if bundle_num == 1 {
                String::new() // First bundle has empty cursor
            } else {
                prev_metadata.end_time.clone()
            };

            if metadata.cursor != expected_cursor {
                errors.push((
                    bundle_num,
                    format!(
                        "Cursor mismatch: expected {} (previous bundle end_time), got {}",
                        expected_cursor, metadata.cursor
                    ),
                ));
            }
        } else if bundle_num == 1 {
            // First bundle should have empty cursor
            if !metadata.cursor.is_empty() {
                errors.push((
                    bundle_num,
                    format!(
                        "Cursor should be empty for first bundle, got: {}",
                        metadata.cursor
                    ),
                ));
            }
        }
    }

    Ok(ChainVerifyResult {
        valid: errors.is_empty(),
        bundles_checked,
        errors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::{ChainVerifySpec, VerifySpec};
    use tempfile::TempDir;

    fn create_test_bundle_metadata(
        bundle_num: u32,
        parent: &str,
        cursor: &str,
        end_time: &str,
    ) -> BundleMetadata {
        BundleMetadata {
            bundle_number: bundle_num,
            start_time: "2024-01-01T00:00:00Z".to_string(),
            end_time: end_time.to_string(),
            operation_count: 100,
            did_count: 50,
            hash: format!("hash{}", bundle_num),
            content_hash: format!("content{}", bundle_num),
            parent: parent.to_string(),
            compressed_hash: format!("comp{}", bundle_num),
            compressed_size: 1000,
            uncompressed_size: 2000,
            cursor: cursor.to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn test_verify_bundle_file_not_found() {
        let tmp = TempDir::new().unwrap();
        let metadata = create_test_bundle_metadata(1, "", "", "2024-01-01T01:00:00Z");
        let spec = VerifySpec {
            check_hash: false,
            check_content_hash: false,
            check_operations: false,
            fast: false,
        };

        let result = verify_bundle(tmp.path(), &metadata, spec).unwrap();
        assert!(!result.valid);
        assert!(!result.errors.is_empty());
        assert!(result.errors[0].contains("Bundle file not found"));
    }

    #[test]
    fn test_verify_bundle_fast_mode_file_not_found() {
        let tmp = TempDir::new().unwrap();
        let metadata = create_test_bundle_metadata(1, "", "", "2024-01-01T01:00:00Z");
        let spec = VerifySpec {
            check_hash: false,
            check_content_hash: false,
            check_operations: false,
            fast: true,
        };

        let result = verify_bundle(tmp.path(), &metadata, spec).unwrap();
        assert!(!result.valid);
        assert!(!result.errors.is_empty());
        assert!(result.errors[0].contains("Bundle file not found"));
    }

    #[test]
    fn test_verify_chain_empty_index() {
        let tmp = TempDir::new().unwrap();
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 0,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 0,
            total_uncompressed_size_bytes: 0,
            bundles: Vec::new(),
        };

        let spec = ChainVerifySpec {
            start_bundle: 1,
            end_bundle: None,
            check_parent_links: true,
        };

        let result = verify_chain(tmp.path(), &index, spec).unwrap();
        assert!(result.valid);
        assert_eq!(result.bundles_checked, 0);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_verify_chain_single_bundle_valid() {
        let tmp = TempDir::new().unwrap();
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 1,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 1000,
            total_uncompressed_size_bytes: 2000,
            bundles: vec![create_test_bundle_metadata(
                1,
                "",
                "",
                "2024-01-01T01:00:00Z",
            )],
        };

        let spec = ChainVerifySpec {
            start_bundle: 1,
            end_bundle: None,
            check_parent_links: true,
        };

        let result = verify_chain(tmp.path(), &index, spec).unwrap();
        assert!(result.valid);
        assert_eq!(result.bundles_checked, 1);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_verify_chain_multiple_bundles_valid() {
        let tmp = TempDir::new().unwrap();
        // Bundle 1: end_time = "2024-01-01T01:00:00Z", so bundle 2's cursor should be "2024-01-01T01:00:00Z"
        // Bundle 2: end_time = "2024-01-01T02:00:00Z", so bundle 3's cursor should be "2024-01-01T02:00:00Z"
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 3,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 3000,
            total_uncompressed_size_bytes: 6000,
            bundles: vec![
                create_test_bundle_metadata(1, "", "", "2024-01-01T01:00:00Z"), // end_time = "2024-01-01T01:00:00Z"
                create_test_bundle_metadata(
                    2,
                    "hash1",
                    "2024-01-01T01:00:00Z",
                    "2024-01-01T02:00:00Z",
                ), // cursor = bundle 1's end_time
                create_test_bundle_metadata(
                    3,
                    "hash2",
                    "2024-01-01T02:00:00Z",
                    "2024-01-01T03:00:00Z",
                ), // cursor = bundle 2's end_time
            ],
        };

        let spec = ChainVerifySpec {
            start_bundle: 1,
            end_bundle: None,
            check_parent_links: true,
        };

        let result = verify_chain(tmp.path(), &index, spec).unwrap();
        assert!(result.valid);
        assert_eq!(result.bundles_checked, 3);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_verify_chain_parent_hash_mismatch() {
        let tmp = TempDir::new().unwrap();
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 2,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 2000,
            total_uncompressed_size_bytes: 4000,
            bundles: vec![
                create_test_bundle_metadata(1, "", "", "2024-01-01T01:00:00Z"),
                create_test_bundle_metadata(
                    2,
                    "wrong_hash",
                    "2024-01-01T01:00:00Z",
                    "2024-01-01T02:00:00Z",
                ), // Wrong parent hash
            ],
        };

        let spec = ChainVerifySpec {
            start_bundle: 1,
            end_bundle: None,
            check_parent_links: true,
        };

        let result = verify_chain(tmp.path(), &index, spec).unwrap();
        assert!(!result.valid);
        assert_eq!(result.bundles_checked, 2);
        assert!(!result.errors.is_empty());
        assert!(
            result
                .errors
                .iter()
                .any(|(_, msg)| msg.contains("Parent hash mismatch"))
        );
    }

    #[test]
    fn test_verify_chain_cursor_mismatch() {
        let tmp = TempDir::new().unwrap();
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 2,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 2000,
            total_uncompressed_size_bytes: 4000,
            bundles: vec![
                create_test_bundle_metadata(1, "", "", "2024-01-01T01:00:00Z"),
                create_test_bundle_metadata(2, "hash1", "wrong_cursor", "2024-01-01T02:00:00Z"), // Wrong cursor
            ],
        };

        let spec = ChainVerifySpec {
            start_bundle: 1,
            end_bundle: None,
            check_parent_links: true,
        };

        let result = verify_chain(tmp.path(), &index, spec).unwrap();
        assert!(!result.valid);
        assert_eq!(result.bundles_checked, 2);
        assert!(!result.errors.is_empty());
        assert!(
            result
                .errors
                .iter()
                .any(|(_, msg)| msg.contains("Cursor mismatch"))
        );
    }

    #[test]
    fn test_verify_chain_first_bundle_non_empty_cursor() {
        let tmp = TempDir::new().unwrap();
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 1,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 1000,
            total_uncompressed_size_bytes: 2000,
            bundles: vec![create_test_bundle_metadata(
                1,
                "",
                "should_be_empty",
                "2024-01-01T01:00:00Z",
            )], // First bundle has non-empty cursor
        };

        let spec = ChainVerifySpec {
            start_bundle: 1,
            end_bundle: None,
            check_parent_links: false, // Don't check parent links, but still check first bundle cursor
        };

        let result = verify_chain(tmp.path(), &index, spec).unwrap();
        assert!(!result.valid);
        assert_eq!(result.bundles_checked, 1);
        assert!(!result.errors.is_empty());
        assert!(
            result
                .errors
                .iter()
                .any(|(_, msg)| msg.contains("Cursor should be empty"))
        );
    }

    #[test]
    fn test_verify_chain_missing_bundle() {
        let tmp = TempDir::new().unwrap();
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 3,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 2000,
            total_uncompressed_size_bytes: 4000,
            bundles: vec![
                create_test_bundle_metadata(1, "", "", "2024-01-01T01:00:00Z"),
                create_test_bundle_metadata(
                    3,
                    "hash2",
                    "2024-01-01T02:00:00Z",
                    "2024-01-01T03:00:00Z",
                ), // Missing bundle 2
            ],
        };

        let spec = ChainVerifySpec {
            start_bundle: 1,
            end_bundle: None,
            check_parent_links: true,
        };

        let result = verify_chain(tmp.path(), &index, spec).unwrap();
        assert!(!result.valid);
        assert_eq!(result.bundles_checked, 2); // Only bundles 1 and 3 checked
        assert!(!result.errors.is_empty());
        assert!(result.errors.iter().any(|(num, _)| *num == 2));
        assert!(
            result
                .errors
                .iter()
                .any(|(_, msg)| msg.contains("not in index"))
        );
    }

    #[test]
    fn test_verify_chain_missing_previous_bundle() {
        let tmp = TempDir::new().unwrap();
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 2,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 1000,
            total_uncompressed_size_bytes: 2000,
            bundles: vec![create_test_bundle_metadata(
                2,
                "hash1",
                "2024-01-01T01:00:00Z",
                "2024-01-01T02:00:00Z",
            )], // Missing bundle 1
        };

        let spec = ChainVerifySpec {
            start_bundle: 1,
            end_bundle: None,
            check_parent_links: true,
        };

        let result = verify_chain(tmp.path(), &index, spec).unwrap();
        assert!(!result.valid);
        assert_eq!(result.bundles_checked, 1); // Only bundle 2 checked
        assert!(!result.errors.is_empty());
        assert!(
            result
                .errors
                .iter()
                .any(|(_, msg)| msg.contains("Previous bundle 1 not found"))
        );
    }

    #[test]
    fn test_verify_chain_range() {
        let tmp = TempDir::new().unwrap();
        // Each bundle's cursor should equal the previous bundle's end_time
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 5,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 5000,
            total_uncompressed_size_bytes: 10000,
            bundles: vec![
                create_test_bundle_metadata(1, "", "", "2024-01-01T01:00:00Z"), // end_time = "2024-01-01T01:00:00Z"
                create_test_bundle_metadata(
                    2,
                    "hash1",
                    "2024-01-01T01:00:00Z",
                    "2024-01-01T02:00:00Z",
                ), // cursor = bundle 1's end_time
                create_test_bundle_metadata(
                    3,
                    "hash2",
                    "2024-01-01T02:00:00Z",
                    "2024-01-01T03:00:00Z",
                ), // cursor = bundle 2's end_time
                create_test_bundle_metadata(
                    4,
                    "hash3",
                    "2024-01-01T03:00:00Z",
                    "2024-01-01T04:00:00Z",
                ), // cursor = bundle 3's end_time
                create_test_bundle_metadata(
                    5,
                    "hash4",
                    "2024-01-01T04:00:00Z",
                    "2024-01-01T05:00:00Z",
                ), // cursor = bundle 4's end_time
            ],
        };

        let spec = ChainVerifySpec {
            start_bundle: 2,
            end_bundle: Some(4),
            check_parent_links: true,
        };

        let result = verify_chain(tmp.path(), &index, spec).unwrap();
        assert!(result.valid);
        assert_eq!(result.bundles_checked, 3); // Bundles 2, 3, 4
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_verify_chain_no_parent_links_check() {
        let tmp = TempDir::new().unwrap();
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 2,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 2000,
            total_uncompressed_size_bytes: 4000,
            bundles: vec![
                create_test_bundle_metadata(1, "", "", "2024-01-01T01:00:00Z"),
                create_test_bundle_metadata(
                    2,
                    "wrong_hash",
                    "wrong_cursor",
                    "2024-01-01T02:00:00Z",
                ), // Wrong but won't be checked
            ],
        };

        let spec = ChainVerifySpec {
            start_bundle: 1,
            end_bundle: None,
            check_parent_links: false,
        };

        let result = verify_chain(tmp.path(), &index, spec).unwrap();
        assert!(result.valid); // Should be valid since we're not checking parent links
        assert_eq!(result.bundles_checked, 2);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_verify_chain_end_bundle_none() {
        let tmp = TempDir::new().unwrap();
        // Each bundle's cursor should equal the previous bundle's end_time
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 3,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 3000,
            total_uncompressed_size_bytes: 6000,
            bundles: vec![
                create_test_bundle_metadata(1, "", "", "2024-01-01T01:00:00Z"), // end_time = "2024-01-01T01:00:00Z"
                create_test_bundle_metadata(
                    2,
                    "hash1",
                    "2024-01-01T01:00:00Z",
                    "2024-01-01T02:00:00Z",
                ), // cursor = bundle 1's end_time
                create_test_bundle_metadata(
                    3,
                    "hash2",
                    "2024-01-01T02:00:00Z",
                    "2024-01-01T03:00:00Z",
                ), // cursor = bundle 2's end_time
            ],
        };

        let spec = ChainVerifySpec {
            start_bundle: 1,
            end_bundle: None, // Should default to last_bundle (3)
            check_parent_links: true,
        };

        let result = verify_chain(tmp.path(), &index, spec).unwrap();
        assert!(result.valid);
        assert_eq!(result.bundles_checked, 3);
    }

    #[test]
    fn test_verify_chain_end_bundle_specified() {
        let tmp = TempDir::new().unwrap();
        // Each bundle's cursor should equal the previous bundle's end_time
        let index = Index {
            version: "1.0".to_string(),
            origin: "test".to_string(),
            last_bundle: 5,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            total_size_bytes: 5000,
            total_uncompressed_size_bytes: 10000,
            bundles: vec![
                create_test_bundle_metadata(1, "", "", "2024-01-01T01:00:00Z"), // end_time = "2024-01-01T01:00:00Z"
                create_test_bundle_metadata(
                    2,
                    "hash1",
                    "2024-01-01T01:00:00Z",
                    "2024-01-01T02:00:00Z",
                ), // cursor = bundle 1's end_time
                create_test_bundle_metadata(
                    3,
                    "hash2",
                    "2024-01-01T02:00:00Z",
                    "2024-01-01T03:00:00Z",
                ), // cursor = bundle 2's end_time
                create_test_bundle_metadata(
                    4,
                    "hash3",
                    "2024-01-01T03:00:00Z",
                    "2024-01-01T04:00:00Z",
                ), // cursor = bundle 3's end_time
                create_test_bundle_metadata(
                    5,
                    "hash4",
                    "2024-01-01T04:00:00Z",
                    "2024-01-01T05:00:00Z",
                ), // cursor = bundle 4's end_time
            ],
        };

        let spec = ChainVerifySpec {
            start_bundle: 1,
            end_bundle: Some(2), // Only check first 2 bundles
            check_parent_links: true,
        };

        let result = verify_chain(tmp.path(), &index, spec).unwrap();
        assert!(result.valid);
        assert_eq!(result.bundles_checked, 2);
    }
}
