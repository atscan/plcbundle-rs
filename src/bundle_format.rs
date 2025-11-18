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
use std::io::{Read, Seek, SeekFrom, Write};

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

    /// Total uncompressed size in bytes
    #[serde(default)]
    pub uncompressed_size: Option<u64>,

    /// Total compressed size in bytes (excluding metadata frame)
    #[serde(default)]
    pub compressed_size: Option<u64>,

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

    /// Creator version (e.g., "plcbundle/0.9.0")
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
    if !(0x184D2A50..=0x184D2A5F).contains(&magic) {
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
    let json_data = sonic_rs::to_vec(metadata)?;
    write_skippable_frame(writer, SKIPPABLE_MAGIC_METADATA, &json_data)
}

/// Read metadata from skippable frame
pub fn read_metadata_frame<R: Read>(reader: &mut R) -> Result<BundleMetadata> {
    let (magic, data) = read_skippable_frame(reader)?;

    if magic != SKIPPABLE_MAGIC_METADATA {
        anyhow::bail!(
            "Unexpected magic: 0x{:08X} (expected 0x{:08X})",
            magic,
            SKIPPABLE_MAGIC_METADATA
        );
    }

    let metadata: BundleMetadata = sonic_rs::from_slice(&data)?;
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
        anyhow::bail!(
            "Position {} out of bounds (frame {}, total frames {})",
            position,
            frame_index,
            frame_offsets.len() - 1
        );
    }

    // Convert relative offsets to absolute
    let start_offset = metadata_frame_size + frame_offsets[frame_index];
    let end_offset = metadata_frame_size + frame_offsets[frame_index + 1];
    let frame_length = end_offset - start_offset;

    if frame_length <= 0 || frame_length > 10 * 1024 * 1024 {
        anyhow::bail!(
            "Invalid frame length: {} (offsets: {}-{})",
            frame_length,
            start_offset,
            end_offset
        );
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
    /// Time spent serializing (in milliseconds)
    pub serialize_time_ms: f64,
    /// Time spent compressing (in milliseconds)
    pub compress_time_ms: f64,
}

/// Compress operations into multiple frames using parallel compression
///
/// Each frame contains FRAME_SIZE operations (except possibly the last frame).
/// Returns the compressed frames and their relative offsets.
/// Uses rayon to compress multiple frames in parallel for better performance.
pub fn compress_operations_to_frames_parallel(
    operations: &[crate::operations::Operation],
) -> anyhow::Result<FrameCompressionResult> {
    use rayon::prelude::*;
    use std::time::Instant;

    let num_frames = operations.len().div_ceil(constants::FRAME_SIZE);

    // Process all frames in parallel
    let frame_results: Vec<_> = (0..num_frames)
        .into_par_iter()
        .map(|frame_idx| {
            let frame_start = frame_idx * constants::FRAME_SIZE;
            let frame_end = (frame_start + constants::FRAME_SIZE).min(operations.len());
            let frame_ops = &operations[frame_start..frame_end];

            // Serialize frame to JSONL
            let serialize_start = Instant::now();
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
            let serialize_time = serialize_start.elapsed();
            let uncompressed_size = frame_data.len() as u64;

            // Compress frame with content size and checksum
            let compress_start = Instant::now();
            let mut compressed_frame = Vec::new();
            {
                let mut encoder =
                    zstd::Encoder::new(&mut compressed_frame, constants::ZSTD_COMPRESSION_LEVEL)?;
                encoder.set_pledged_src_size(Some(frame_data.len() as u64))?;
                encoder.include_contentsize(true)?;
                encoder.include_checksum(true)?; // Enable XXH64 checksum
                std::io::copy(&mut frame_data.as_slice(), &mut encoder)?;
                encoder.finish()?;
            }
            let compress_time = compress_start.elapsed();

            Ok::<_, anyhow::Error>((
                compressed_frame,
                uncompressed_size,
                serialize_time,
                compress_time,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Calculate offsets sequentially (must be done in order)
    let mut frame_offsets = Vec::with_capacity(num_frames + 1);
    let mut compressed_frames: Vec<Vec<u8>> = Vec::with_capacity(num_frames);
    let mut total_uncompressed = 0u64;
    let mut total_serialize_time = std::time::Duration::ZERO;
    let mut total_compress_time = std::time::Duration::ZERO;

    for (compressed_frame, uncompressed_size, serialize_time, compress_time) in frame_results {
        let offset = if frame_offsets.is_empty() {
            0i64
        } else {
            let prev_frame_size = compressed_frames.last().unwrap().len() as i64;
            frame_offsets.last().unwrap() + prev_frame_size
        };
        frame_offsets.push(offset);
        compressed_frames.push(compressed_frame);
        total_uncompressed += uncompressed_size;
        total_serialize_time += serialize_time;
        total_compress_time += compress_time;
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
        serialize_time_ms: total_serialize_time.as_secs_f64() * 1000.0,
        compress_time_ms: total_compress_time.as_secs_f64() * 1000.0,
    })
}

/// Compress operations into multiple frames (sequential version)
///
/// Each frame contains FRAME_SIZE operations (except possibly the last frame).
/// Returns the compressed frames and their relative offsets.
pub fn compress_operations_to_frames(
    operations: &[crate::operations::Operation],
) -> anyhow::Result<FrameCompressionResult> {
    use std::time::Instant;

    let mut frame_offsets = Vec::new();
    let mut compressed_frames: Vec<Vec<u8>> = Vec::new();
    let mut total_uncompressed = 0u64;

    let mut total_serialize_time = std::time::Duration::ZERO;
    let mut total_compress_time = std::time::Duration::ZERO;

    // Process operations in frames of FRAME_SIZE
    let num_frames = operations.len().div_ceil(constants::FRAME_SIZE);

    for frame_idx in 0..num_frames {
        let frame_start = frame_idx * constants::FRAME_SIZE;
        let frame_end = (frame_start + constants::FRAME_SIZE).min(operations.len());
        let frame_ops = &operations[frame_start..frame_end];

        // Serialize frame to JSONL
        let serialize_start = Instant::now();
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
        total_serialize_time += serialize_start.elapsed();
        total_uncompressed += frame_data.len() as u64;

        // Compress frame with content size and checksum
        let compress_start = Instant::now();
        let mut compressed_frame = Vec::new();
        {
            let mut encoder =
                zstd::Encoder::new(&mut compressed_frame, constants::ZSTD_COMPRESSION_LEVEL)?;
            encoder.set_pledged_src_size(Some(frame_data.len() as u64))?;
            encoder.include_contentsize(true)?;
            encoder.include_checksum(true)?; // Enable XXH64 checksum
            std::io::copy(&mut frame_data.as_slice(), &mut encoder)?;
            encoder.finish()?;
        }
        total_compress_time += compress_start.elapsed();

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
        serialize_time_ms: total_serialize_time.as_secs_f64() * 1000.0,
        compress_time_ms: total_compress_time.as_secs_f64() * 1000.0,
    })
}

/// Serialize operations to JSONL (uncompressed)
///
/// CRITICAL: This function implements the V1 specification requirement (docs/specification.md § 4.2)
/// for deterministic content hash calculation. It MUST use the raw JSON bytes when available
/// to preserve exact byte content, including field order and whitespace.
pub fn serialize_operations_to_jsonl(
    operations: &[crate::operations::Operation],
) -> anyhow::Result<Vec<u8>> {
    let mut data = Vec::new();
    for op in operations {
        // CRITICAL: Use raw_json if available to preserve exact byte content
        // This is required for deterministic content_hash calculation.
        // Re-serialization would change field order/whitespace and break hash verification.
        let json = if let Some(raw) = &op.raw_json {
            raw.clone()
        } else {
            // Fallback: Re-serialize if raw_json is not available
            // WARNING: This may produce different content_hash than the original!
            sonic_rs::to_string(op)?
        };
        data.extend_from_slice(json.as_bytes());
        data.push(b'\n');
    }
    Ok(data)
}

/// Calculate content hash (SHA256 of uncompressed JSONL)
pub fn calculate_content_hash(
    operations: &[crate::operations::Operation],
) -> anyhow::Result<String> {
    use sha2::{Digest, Sha256};

    let jsonl_data = serialize_operations_to_jsonl(operations)?;
    let mut hasher = Sha256::new();
    hasher.update(&jsonl_data);
    Ok(format!("{:x}", hasher.finalize()))
}

/// Calculate compressed hash (SHA256 of compressed data)
pub fn calculate_compressed_hash(compressed_data: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(compressed_data);
    format!("{:x}", hasher.finalize())
}

/// Load all operations from a bundle file as raw JSON strings
///
/// This function reads a bundle file and returns the operations as raw JSON strings
/// without parsing them into Operation structs. Useful for calculating uncompressed
/// sizes and other metadata operations.
///
/// # Arguments
/// * `path` - Path to the bundle file
///
/// # Returns
/// Vector of raw JSON strings, one per operation
pub fn load_bundle_as_json_strings(path: &std::path::Path) -> Result<Vec<String>> {
    use std::io::Read;

    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);

    // Skip metadata frame if present
    let mut magic_buf = [0u8; 4];
    reader.read_exact(&mut magic_buf)?;
    let magic = u32::from_le_bytes(magic_buf);

    if (0x184D2A50..=0x184D2A5F).contains(&magic) {
        // Skip metadata frame
        let mut size_buf = [0u8; 4];
        reader.read_exact(&mut size_buf)?;
        let frame_size = u32::from_le_bytes(size_buf);

        let mut skip_buf = vec![0u8; frame_size as usize];
        reader.read_exact(&mut skip_buf)?;
    } else {
        // Rewind if not a skippable frame
        drop(reader);
        reader = std::io::BufReader::new(std::fs::File::open(path)?);
    }

    // Decompress remaining data
    let decoder = zstd::Decoder::new(reader)?;
    let mut decompressed = String::new();
    std::io::BufReader::new(decoder).read_to_string(&mut decompressed)?;

    // Split into lines
    Ok(decompressed.lines().map(|s| s.to_string()).collect())
}

/// Create bundle metadata structure
#[allow(clippy::too_many_arguments)]
pub fn create_bundle_metadata(
    bundle_number: u32,
    origin: &str,
    content_hash: &str,
    parent_hash: Option<&str>,
    uncompressed_size: Option<u64>,
    compressed_size: Option<u64>,            
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
        uncompressed_size,
        compressed_size,              
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
    fn test_zstd_content_size_and_checksum() {
        // Test that our compression includes content size and checksum in frame header
        let test_data = b"Test data for zstd compression with content size and checksum";

        // NEW method: with content size and checksum
        let mut compressed_new = Vec::new();
        {
            let mut encoder = zstd::Encoder::new(&mut compressed_new, 3).unwrap();
            encoder
                .set_pledged_src_size(Some(test_data.len() as u64))
                .unwrap();
            encoder.include_contentsize(true).unwrap();
            encoder.include_checksum(true).unwrap();
            std::io::copy(&mut &test_data[..], &mut encoder).unwrap();
            encoder.finish().unwrap();
        }

        // OLD method: without content size or checksum (for comparison)
        let compressed_old = zstd::encode_all(&test_data[..], 3).unwrap();

        // Write both for comparison
        let test_file_new = "/tmp/test_with_metadata.zst";
        let test_file_old = "/tmp/test_without_metadata.zst";
        std::fs::write(test_file_new, &compressed_new).unwrap();
        std::fs::write(test_file_old, &compressed_old).unwrap();

        // Both should decompress correctly
        let decompressed_new = zstd::decode_all(&compressed_new[..]).unwrap();
        let decompressed_old = zstd::decode_all(&compressed_old[..]).unwrap();
        assert_eq!(&decompressed_new, test_data);
        assert_eq!(&decompressed_old, test_data);

        println!("✓ Created test files:");
        println!("  With metadata (size+checksum): {}", test_file_new);
        println!("  Without metadata:              {}", test_file_old);
        println!();
        println!("Compare with: zstd -l /tmp/test_with*.zst /tmp/test_without*.zst");
        println!("Expected: New file should show 'Uncompressed' size and 'XXH64' check");
    }

    #[test]
    fn test_parallel_compression_matches_sequential() {
        use crate::operations::Operation;
        use sonic_rs::{Value, from_str};

        // Create test operations
        let mut operations = Vec::new();
        for i in 0..250 {
            // Multiple frames (250 ops = 3 frames with FRAME_SIZE=100)
            let operation_json = format!(
                r#"{{"type":"create","data":"test data {}"}}"#,
                i
            );
            let operation_value: Value = from_str(&operation_json).unwrap();
            let extra_value: Value = from_str("{}").unwrap_or_else(|_| Value::new());
            operations.push(Operation {
                did: format!("did:plc:test{}", i),
                operation: operation_value,
                cid: Some(format!("cid{}", i)),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                nullified: false,
                extra: extra_value,
                raw_json: None,
            });
        }

        // Compress using both methods
        let result_sequential = compress_operations_to_frames(&operations).unwrap();
        let result_parallel = compress_operations_to_frames_parallel(&operations).unwrap();

        // Verify results match
        assert_eq!(
            result_sequential.compressed_frames.len(),
            result_parallel.compressed_frames.len()
        );
        assert_eq!(
            result_sequential.frame_offsets,
            result_parallel.frame_offsets
        );
        assert_eq!(
            result_sequential.uncompressed_size,
            result_parallel.uncompressed_size
        );
        assert_eq!(
            result_sequential.compressed_size,
            result_parallel.compressed_size
        );

        // Verify compressed frames are identical
        for (seq_frame, par_frame) in result_sequential
            .compressed_frames
            .iter()
            .zip(result_parallel.compressed_frames.iter())
        {
            assert_eq!(seq_frame, par_frame, "Compressed frames must be identical");
        }

        println!("✓ Parallel compression produces identical output to sequential");
        println!("  Frames: {}", result_parallel.compressed_frames.len());
        println!(
            "  Compressed size: {} bytes",
            result_parallel.compressed_size
        );
    }

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
            uncompressed_size: None,
            compressed_size: None,
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

    #[test]
    fn test_skippable_frame_empty_data() {
        let data = b"";
        let mut buffer = Vec::new();

        write_skippable_frame(&mut buffer, SKIPPABLE_MAGIC_METADATA, data).unwrap();

        let mut cursor = std::io::Cursor::new(&buffer);
        let (magic, read_data) = read_skippable_frame(&mut cursor).unwrap();

        assert_eq!(magic, SKIPPABLE_MAGIC_METADATA);
        assert_eq!(read_data, data);
        assert_eq!(read_data.len(), 0);
    }

    #[test]
    fn test_skippable_frame_large_data() {
        let data = vec![0x42u8; 10000];
        let mut buffer = Vec::new();

        write_skippable_frame(&mut buffer, SKIPPABLE_MAGIC_METADATA, &data).unwrap();

        let mut cursor = std::io::Cursor::new(&buffer);
        let (magic, read_data) = read_skippable_frame(&mut cursor).unwrap();

        assert_eq!(magic, SKIPPABLE_MAGIC_METADATA);
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_skippable_frame_invalid_magic() {
        let data = b"test";
        let mut buffer = Vec::new();

        // Write with invalid magic (not in skippable range)
        buffer.write_all(&0x12345678u32.to_le_bytes()).unwrap();
        buffer.write_all(&(data.len() as u32).to_le_bytes()).unwrap();
        buffer.write_all(data).unwrap();

        let mut cursor = std::io::Cursor::new(&buffer);
        let result = read_skippable_frame(&mut cursor);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Not a skippable frame"));
    }

    #[test]
    fn test_skippable_frame_valid_magic_range() {
        // Test all valid magic values in range 0x184D2A50 - 0x184D2A5F
        for magic_val in 0x184D2A50..=0x184D2A5F {
            let data = b"test";
            let mut buffer = Vec::new();

            write_skippable_frame(&mut buffer, magic_val, data).unwrap();

            let mut cursor = std::io::Cursor::new(&buffer);
            let (read_magic, read_data) = read_skippable_frame(&mut cursor).unwrap();

            assert_eq!(read_magic, magic_val);
            assert_eq!(read_data, data);
        }
    }

    #[test]
    fn test_metadata_frame_with_parent_hash() {
        let metadata = BundleMetadata {
            format: "plcbundle-v1".to_string(),
            bundle_number: 2,
            origin: "test".to_string(),
            content_hash: "hash2".to_string(),
            parent_hash: Some("hash1".to_string()),
            uncompressed_size: Some(1000000),
            compressed_size: Some(500000),
            operation_count: 10000,
            did_count: 5000,
            start_time: "2024-01-01T00:00:00Z".to_string(),
            end_time: "2024-01-01T23:59:59Z".to_string(),
            created_at: "2024-01-02T00:00:00Z".to_string(),
            created_by: "test/1.0".to_string(),
            frame_count: 100,
            frame_size: 100,
            frame_offsets: vec![0, 1000, 2000, 3000],
        };

        let mut buffer = Vec::new();
        write_metadata_frame(&mut buffer, &metadata).unwrap();

        let mut cursor = std::io::Cursor::new(&buffer);
        let read_metadata = read_metadata_frame(&mut cursor).unwrap();

        assert_eq!(read_metadata.bundle_number, 2);
        assert_eq!(read_metadata.parent_hash, Some("hash1".to_string()));
        assert_eq!(read_metadata.uncompressed_size, Some(1000000));
        assert_eq!(read_metadata.compressed_size, Some(500000));
        assert_eq!(read_metadata.frame_offsets.len(), 4);
    }

    #[test]
    fn test_metadata_frame_wrong_magic() {
        let metadata = BundleMetadata {
            format: "plcbundle-v1".to_string(),
            bundle_number: 1,
            origin: "test".to_string(),
            content_hash: "hash1".to_string(),
            parent_hash: None,
            uncompressed_size: None,
            compressed_size: None,
            operation_count: 100,
            did_count: 50,
            start_time: "2024-01-01T00:00:00Z".to_string(),
            end_time: "2024-01-01T23:59:59Z".to_string(),
            created_at: "2024-01-02T00:00:00Z".to_string(),
            created_by: "test/1.0".to_string(),
            frame_count: 1,
            frame_size: 100,
            frame_offsets: vec![0, 1000],
        };

        let mut buffer = Vec::new();
        // Write with wrong magic
        write_skippable_frame(&mut buffer, 0x184D2A51, &sonic_rs::to_vec(&metadata).unwrap()).unwrap();

        let mut cursor = std::io::Cursor::new(&buffer);
        let result = read_metadata_frame(&mut cursor);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unexpected magic"));
    }

    #[test]
    fn test_skippable_frame_size_calculation() {
        let data = b"hello world";
        let mut buffer = Vec::new();

        let written = write_skippable_frame(&mut buffer, SKIPPABLE_MAGIC_METADATA, data).unwrap();

        // Should be: magic(4) + size(4) + data(11) = 19
        assert_eq!(written, 8 + data.len());
        assert_eq!(buffer.len(), written);
    }
}
