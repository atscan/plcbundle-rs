// src/verification.rs
use crate::index::BundleMetadata;
use crate::manager::{VerifySpec, VerifyResult, ChainVerifySpec, ChainVerifyResult};
use crate::index::Index;
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
    
    if spec.check_hash {
        // Verify compressed hash
        let mut file = File::open(&bundle_path)?;
        let mut hasher = blake3::Hasher::new();
        std::io::copy(&mut file, &mut hasher)?;
        let hash = hasher.finalize().to_hex();
        
        if hash.to_string() != metadata.compressed_hash {
            errors.push(format!(
                "Compressed hash mismatch: expected {}, got {}",
                metadata.compressed_hash, hash
            ));
        }
    }
    
    if spec.check_content_hash {
        // Verify content hash (decompressed)
        let file = File::open(&bundle_path)?;
        let mut decoder = zstd::Decoder::new(file)?;
        let mut content = Vec::new();
        decoder.read_to_end(&mut content)?;
        
        let mut hasher = blake3::Hasher::new();
        hasher.update(&content);
        let hash = hasher.finalize().to_hex();
        
        if hash.to_string() != metadata.content_hash {
            errors.push(format!(
                "Content hash mismatch: expected {}, got {}",
                metadata.content_hash, hash
            ));
        }
    }
    
    if spec.check_operations {
        // Verify operation count
        let file = File::open(&bundle_path)?;
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
