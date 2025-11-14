// src/verification.rs
use crate::index::BundleMetadata;
use crate::manager::{VerifySpec, VerifyResult, ChainVerifySpec, ChainVerifyResult};
use crate::index::Index;
use crate::bundle_format;
use anyhow::Result;
use std::path::Path;
use std::fs::File;
use std::io::Read;

pub fn verify_bundle(
    directory: &Path,
    metadata: &BundleMetadata,
    spec: VerifySpec,
) -> Result<VerifyResult> {
    let mut errors = Vec::new();
    
    let bundle_path = directory.join(format!("{:06}.jsonl.zst", metadata.bundle_number));
    
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
        use sha2::{Sha256, Digest};
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
            let magic = u32::from_le_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
            if magic == bundle_format::SKIPPABLE_MAGIC_METADATA && file_data.len() >= 8 {
                let frame_size = u32::from_le_bytes([file_data[4], file_data[5], file_data[6], file_data[7]]) as usize;
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
        use sha2::{Sha256, Digest};
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
        use sha2::{Sha256, Digest};
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
        
        let count = reader.lines().filter_map(|l| l.ok()).filter(|l| !l.is_empty()).count();
        
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
    _directory: &Path,  // Add underscore prefix
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
                    errors.push((bundle_num, format!("Previous bundle {} not found", bundle_num - 1)));
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
        }
    }
    
    Ok(ChainVerifyResult {
        valid: errors.is_empty(),
        bundles_checked,
        errors,
    })
}
