// src/bundle_format.rs
//! Bundle file format implementation with zstd skippable frames and multi-frame compression
//! 
//! Format:
//! - Skippable frame with metadata (magic 0x184D2A50)
//! - Multiple zstd frames (100 operations each)
//! - Frame offsets in metadata allow efficient random access

use crate::constants;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write, Seek, SeekFrom};

/// Skippable frame magic number for metadata
pub const SKIPPABLE_MAGIC_METADATA: u32 = 0x184D2A50;

/// Bundle metadata stored in skippable frame
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleMetadata {
    /// Format version
    pub format: String,
    
    /// Bundle number
    pub bundle_number: u32,
    
    /// Source origin
    pub origin: String,
    
    /// Content hash (SHA256 of uncompressed JSONL)
    pub content_hash: String,
    
    /// Parent bundle hash (for chaining)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_hash: Option<String>,
    
    /// Number of operations
    pub operation_count: usize,
    
    /// Number of unique DIDs
    pub did_count: usize,
    
    /// First operation timestamp
    pub start_time: String,
    
    /// Last operation timestamp
    pub end_time: String,
    
    /// Creation timestamp
    pub created_at: String,
    
    /// Creator version (e.g., "plcbundle-rs/0.9.0")
    pub created_by: String,
    
    /// Number of frames
    pub frame_count: usize,
    
    /// Operations per frame
    pub frame_size: usize,
    
    /// Frame byte offsets (RELATIVE to first data frame)
    pub frame_offsets: Vec<i64>,
}

/// Write a zstd skippable frame
pub fn write_skippable_frame<W: Write>(writer: &mut W, magic: u32, data: &[u8]) -> Result<usize> {
    let frame_size = data.len() as u32;
    
    // Write magic number (little-endian)
    writer.write_all(&magic.to_le_bytes())?;
    
    // Write frame size (little-endian)
    writer.write_all(&frame_size.to_le_bytes())?;
    
    // Write data
    writer.write_all(data)?;
    
    Ok(8 + data.len()) // magic(4) + size(4) + data
}

/// Read a zstd skippable frame
pub fn read_skippable_frame<R: Read>(reader: &mut R) -> Result<(u32, Vec<u8>)> {
    // Read magic number
    let mut magic_buf = [0u8; 4];
    reader.read_exact(&mut magic_buf)?;
    let magic = u32::from_le_bytes(magic_buf);
    
    // Verify it's a skippable frame (0x184D2A50 - 0x184D2A5F)
    if magic < 0x184D2A50 || magic > 0x184D2A5F {
        anyhow::bail!("Not a skippable frame: magic=0x{:08X}", magic);
    }
    
    // Read frame size
    let mut size_buf = [0u8; 4];
    reader.read_exact(&mut size_buf)?;
    let frame_size = u32::from_le_bytes(size_buf);
    
    // Read data
    let mut data = vec![0u8; frame_size as usize];
    reader.read_exact(&mut data)?;
    
    Ok((magic, data))
}

/// Write metadata as skippable frame
pub fn write_metadata_frame<W: Write>(writer: &mut W, metadata: &BundleMetadata) -> Result<usize> {
    let json_data = serde_json::to_vec(metadata)?;
    write_skippable_frame(writer, SKIPPABLE_MAGIC_METADATA, &json_data)
}

/// Read metadata from skippable frame
pub fn read_metadata_frame<R: Read>(reader: &mut R) -> Result<BundleMetadata> {
    let (magic, data) = read_skippable_frame(reader)?;
    
    if magic != SKIPPABLE_MAGIC_METADATA {
        anyhow::bail!("Unexpected magic: 0x{:08X} (expected 0x{:08X})", 
            magic, SKIPPABLE_MAGIC_METADATA);
    }
    
    let metadata: BundleMetadata = serde_json::from_slice(&data)?;
    Ok(metadata)
}

/// Extract metadata from bundle file without decompressing data
pub fn extract_metadata_from_file(path: &std::path::Path) -> Result<BundleMetadata> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    read_metadata_frame(&mut reader)
}

/// Load single operation using frame index for efficient access
pub fn load_operation_at_position<R: Read + Seek>(
    reader: &mut R,
    position: usize,
    frame_offsets: &[i64],
    metadata_frame_size: i64,
) -> Result<String> {
    let frame_index = position / constants::FRAME_SIZE;
    let line_in_frame = position % constants::FRAME_SIZE;
    
    if frame_index >= frame_offsets.len() - 1 {
        anyhow::bail!("Position {} out of bounds (frame {}, total frames {})",
            position, frame_index, frame_offsets.len() - 1);
    }
    
    // Convert relative offsets to absolute
    let start_offset = metadata_frame_size + frame_offsets[frame_index];
    let end_offset = metadata_frame_size + frame_offsets[frame_index + 1];
    let frame_length = end_offset - start_offset;
    
    if frame_length <= 0 || frame_length > 10 * 1024 * 1024 {
        anyhow::bail!("Invalid frame length: {} (offsets: {}-{})",
            frame_length, start_offset, end_offset);
    }
    
    // Seek to frame start
    reader.seek(SeekFrom::Start(start_offset as u64))?;
    
    // Read compressed frame
    let mut compressed_frame = vec![0u8; frame_length as usize];
    reader.read_exact(&mut compressed_frame)?;
    
    // Decompress frame
    let decompressed = zstd::bulk::decompress(&compressed_frame, 10 * 1024 * 1024)?;
    
    // Find the line we want
    use std::io::BufRead;
    let cursor = std::io::Cursor::new(decompressed);
    let lines = cursor.lines();
    
    for (idx, line_result) in lines.enumerate() {
        if idx == line_in_frame {
            return Ok(line_result?);
        }
    }
    
    anyhow::bail!("Position {} not found in frame {}", position, frame_index)
}

/// Result of compressing operations into frames
#[derive(Debug)]
pub struct FrameCompressionResult {
    /// Compressed frames (one per FRAME_SIZE operations)
    pub compressed_frames: Vec<Vec<u8>>,
    /// Frame offsets (relative to first data frame)
    pub frame_offsets: Vec<i64>,
    /// Total uncompressed size
    pub uncompressed_size: u64,
    /// Total compressed size
    pub compressed_size: u64,
}

/// Compress operations into multiple frames
/// 
/// Each frame contains FRAME_SIZE operations (except possibly the last frame).
/// Returns the compressed frames and their relative offsets.
pub fn compress_operations_to_frames(operations: &[crate::operations::Operation]) -> anyhow::Result<FrameCompressionResult> {
    let mut frame_offsets = Vec::new();
    let mut compressed_frames: Vec<Vec<u8>> = Vec::new();
    let mut total_uncompressed = 0u64;

    // Process operations in frames of FRAME_SIZE
    let num_frames = (operations.len() + constants::FRAME_SIZE - 1) / constants::FRAME_SIZE;
    
    for frame_idx in 0..num_frames {
        let frame_start = frame_idx * constants::FRAME_SIZE;
        let frame_end = (frame_start + constants::FRAME_SIZE).min(operations.len());
        let frame_ops = &operations[frame_start..frame_end];

        // Serialize frame to JSONL
        let mut frame_data = Vec::new();
        for op in frame_ops {
            let json = if let Some(raw) = &op.raw_json {
                raw.clone()
            } else {
                sonic_rs::to_string(op)?
            };
            frame_data.extend_from_slice(json.as_bytes());
            frame_data.push(b'\n');
        }
        total_uncompressed += frame_data.len() as u64;

        // Compress frame
        let compressed_frame = zstd::encode_all(frame_data.as_slice(), constants::ZSTD_COMPRESSION_LEVEL)?;
        
        // Record offset (relative to first data frame)
        let offset = if frame_offsets.is_empty() {
            0i64
        } else {
            let prev_frame_size = compressed_frames.last().unwrap().len() as i64;
            frame_offsets.last().unwrap() + prev_frame_size
        };
        frame_offsets.push(offset);
        
        compressed_frames.push(compressed_frame);
    }
    
    // Add final offset (end of last frame)
    if let Some(last_frame) = compressed_frames.last() {
        let final_offset = frame_offsets.last().unwrap() + last_frame.len() as i64;
        frame_offsets.push(final_offset);
    }

    let compressed_size: u64 = compressed_frames.iter().map(|f| f.len() as u64).sum();

    Ok(FrameCompressionResult {
        compressed_frames,
        frame_offsets,
        uncompressed_size: total_uncompressed,
        compressed_size,
    })
}

/// Serialize operations to JSONL (uncompressed)
pub fn serialize_operations_to_jsonl(operations: &[crate::operations::Operation]) -> anyhow::Result<Vec<u8>> {
    let mut data = Vec::new();
    for op in operations {
        let json = if let Some(raw) = &op.raw_json {
            raw.clone()
        } else {
            sonic_rs::to_string(op)?
        };
        data.extend_from_slice(json.as_bytes());
        data.push(b'\n');
    }
    Ok(data)
}

/// Calculate content hash (SHA256 of uncompressed JSONL)
pub fn calculate_content_hash(operations: &[crate::operations::Operation]) -> anyhow::Result<String> {
    use sha2::{Sha256, Digest};
    
    let jsonl_data = serialize_operations_to_jsonl(operations)?;
    let mut hasher = Sha256::new();
    hasher.update(&jsonl_data);
    Ok(format!("{:x}", hasher.finalize()))
}

/// Calculate compressed hash (SHA256 of compressed data)
pub fn calculate_compressed_hash(compressed_data: &[u8]) -> String {
    use sha2::{Sha256, Digest};
    
    let mut hasher = Sha256::new();
    hasher.update(compressed_data);
    format!("{:x}", hasher.finalize())
}

/// Create bundle metadata structure
pub fn create_bundle_metadata(
    bundle_number: u32,
    origin: &str,
    content_hash: &str,
    parent_hash: Option<&str>,
    operation_count: usize,
    did_count: usize,
    start_time: &str,
    end_time: &str,
    frame_count: usize,
    frame_size: usize,
    frame_offsets: &[i64],
) -> BundleMetadata {
    BundleMetadata {
        format: "plcbundle/1.0".to_string(),
        bundle_number,
        origin: origin.to_string(),
        content_hash: content_hash.to_string(),
        parent_hash: parent_hash.map(|s| s.to_string()),
        operation_count,
        did_count,
        start_time: start_time.to_string(),
        end_time: end_time.to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        created_by: crate::constants::created_by(),
        frame_count,
        frame_size,
        frame_offsets: frame_offsets.to_vec(),
    }
}

/// Write bundle to file with multi-frame format
/// 
/// Writes metadata frame followed by compressed data frames.
pub fn write_bundle_with_frames<W: Write>(
    writer: &mut W,
    metadata: &BundleMetadata,
    compressed_frames: &[Vec<u8>],
) -> anyhow::Result<()> {
    // Write metadata as skippable frame first
    write_metadata_frame(writer, metadata)?;
    
    // Write all compressed frames
    for frame in compressed_frames {
        writer.write_all(frame)?;
    }
    
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_skippable_frame_roundtrip() {
        let data = b"test data";
        let mut buffer = Vec::new();
        
        write_skippable_frame(&mut buffer, SKIPPABLE_MAGIC_METADATA, data).unwrap();
        
        let mut cursor = std::io::Cursor::new(&buffer);
        let (magic, read_data) = read_skippable_frame(&mut cursor).unwrap();
        
        assert_eq!(magic, SKIPPABLE_MAGIC_METADATA);
        assert_eq!(read_data, data);
    }
    
    #[test]
    fn test_metadata_frame_roundtrip() {
        let metadata = BundleMetadata {
            format: "plcbundle-v1".to_string(),
            bundle_number: 42,
            origin: constants::DEFAULT_PLC_DIRECTORY_URL.to_string(),
            content_hash: "abc123".to_string(),
            parent_hash: None,
            operation_count: 10000,
            did_count: 5000,
            start_time: "2024-01-01T00:00:00Z".to_string(),
            end_time: "2024-01-01T23:59:59Z".to_string(),
            created_at: "2024-01-02T00:00:00Z".to_string(),
            created_by: crate::constants::created_by(),
            frame_count: 100,
            frame_size: 100,
            frame_offsets: vec![0, 1000, 2000],
        };
        
        let mut buffer = Vec::new();
        write_metadata_frame(&mut buffer, &metadata).unwrap();
        
        let mut cursor = std::io::Cursor::new(&buffer);
        let read_metadata = read_metadata_frame(&mut cursor).unwrap();
        
        assert_eq!(read_metadata.bundle_number, 42);
        assert_eq!(read_metadata.frame_count, 100);
    }
}

