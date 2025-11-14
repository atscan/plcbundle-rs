// src/bundle_format.rs
//! Bundle file format implementation with zstd skippable frames and multi-frame compression
//! 
//! Format:
//! - Skippable frame with metadata (magic 0x184D2A50)
//! - Multiple zstd frames (100 operations each)
//! - Frame offsets in metadata allow efficient random access

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write, Seek, SeekFrom};

/// Skippable frame magic number for metadata
pub const SKIPPABLE_MAGIC_METADATA: u32 = 0x184D2A50;

/// Operations per frame
pub const FRAME_SIZE: usize = 100;

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
    
    /// Creator version (e.g., "plcbundle-rs/0.1.0")
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
    let frame_index = position / FRAME_SIZE;
    let line_in_frame = position % FRAME_SIZE;
    
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
            origin: "https://plc.directory".to_string(),
            content_hash: "abc123".to_string(),
            parent_hash: None,
            operation_count: 10000,
            did_count: 5000,
            start_time: "2024-01-01T00:00:00Z".to_string(),
            end_time: "2024-01-01T23:59:59Z".to_string(),
            created_at: "2024-01-02T00:00:00Z".to_string(),
            created_by: "plcbundle-rs/0.1.0".to_string(),
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

