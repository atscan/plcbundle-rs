//! Persistent pre-bundle operation store with strict chronological validation, CID deduplication, incremental saving, and fast DID lookups
// src/mempool.rs
use crate::constants;
use crate::format::format_std_duration_ms;
use crate::operations::Operation;
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use log::{debug, info};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Mempool stores operations waiting to be bundled
/// Operations must be strictly chronological
pub struct Mempool {
    operations: Vec<Operation>,
    // Index by DID for fast lookups (maintained in sync with operations)
    did_index: HashMap<String, Vec<usize>>, // DID -> indices in operations vec
    target_bundle: u32,
    min_timestamp: DateTime<Utc>,
    file: PathBuf,
    validated: bool,
    dirty: bool,
    verbose: bool,

    // Incremental save tracking
    last_saved_len: usize,
    last_save_time: std::time::Instant,
    save_threshold: usize,
    save_interval: std::time::Duration,
}

impl Mempool {
    /// Creates a new mempool for a specific bundle number
    pub fn new(
        bundle_dir: &Path,
        target_bundle: u32,
        min_timestamp: DateTime<Utc>,
        verbose: bool,
    ) -> Result<Self> {
        let filename = format!(
            "{}{:06}.jsonl",
            constants::MEMPOOL_FILE_PREFIX,
            target_bundle
        );
        let file = bundle_dir.join(filename);

        let mut mempool = Self {
            operations: Vec::new(),
            did_index: HashMap::new(),
            target_bundle,
            min_timestamp,
            file,
            validated: false,
            dirty: false,
            verbose,
            last_saved_len: 0,
            last_save_time: std::time::Instant::now(),
            save_threshold: 100,
            save_interval: std::time::Duration::from_secs(15),
        };

        // Try to load existing mempool
        if mempool.file.exists() {
            mempool.load()?;
        }

        Ok(mempool)
    }

    fn add_internal(&mut self, ops: Vec<Operation>, collect_cids: bool) -> Result<(usize, Vec<String>)> {
        if ops.is_empty() {
            return Ok((0, Vec::new()));
        }

        let mut existing_cids: HashSet<String> = self
            .operations
            .iter()
            .filter_map(|op| op.cid.clone())
            .collect();

        let mut new_ops = Vec::new();
        let total_in = ops.len();
        let mut skipped_no_cid = 0usize;
        let mut skipped_dupe = 0usize;
        let mut added_cids = Vec::new();

        let mut last_time = if !self.operations.is_empty() {
            self.parse_timestamp(&self.operations.last().unwrap().created_at)?
        } else {
            self.min_timestamp
        };

        let start_add = std::time::Instant::now();
        let mut first_added_time: Option<DateTime<Utc>> = None;
        let mut last_added_time: Option<DateTime<Utc>> = None;
        for op in ops {
            let cid = match &op.cid {
                Some(c) => c,
                None => {
                    skipped_no_cid += 1;
                    continue;
                }
            };

            if existing_cids.contains(cid) {
                skipped_dupe += 1;
                continue;
            }

            let op_time = self.parse_timestamp(&op.created_at)?;
            if op_time < last_time {
                bail!(
                    "chronological violation: operation {} at {} is before {}",
                    cid,
                    op.created_at,
                    last_time.to_rfc3339()
                );
            }
            if op_time < self.min_timestamp {
                bail!(
                    "operation {} at {} is before minimum timestamp {} (belongs in earlier bundle)",
                    cid,
                    op.created_at,
                    self.min_timestamp.to_rfc3339()
                );
            }

            new_ops.push(op.clone());
            if collect_cids {
                added_cids.push(cid.clone());
            }
            existing_cids.insert(cid.clone());
            last_time = op_time;
            if first_added_time.is_none() {
                first_added_time = Some(op_time);
            }
            last_added_time = Some(op_time);
        }

        let added = new_ops.len();

        let start_idx = self.operations.len();
        self.operations.extend(new_ops);

        for (offset, op) in self.operations[start_idx..].iter().enumerate() {
            let idx = start_idx + offset;
            self.did_index.entry(op.did.clone()).or_default().push(idx);
        }

        self.validated = true;
        self.dirty = true;

        if self.verbose {
            let dur = start_add.elapsed();
            info!(
                "mempool add: +{} unique from {} ({} no-cid, {} dupes) in {} • total {}",
                added,
                total_in,
                skipped_no_cid,
                skipped_dupe,
                format_std_duration_ms(dur),
                self.operations.len()
            );
            if let (Some(f), Some(l)) = (first_added_time, last_added_time) {
                debug!(
                    "mempool add range: {} → {}",
                    f.format("%Y-%m-%d %H:%M:%S"),
                    l.format("%Y-%m-%d %H:%M:%S")
                );
            }
            if added == 0 && (skipped_no_cid > 0 || skipped_dupe > 0) {
                debug!("mempool add made no progress");
            }
        }

        Ok((added, added_cids))
    }

    /// Add operations to the mempool with strict validation
    pub fn add(&mut self, ops: Vec<Operation>) -> Result<usize> {
        let (added, _) = self.add_internal(ops, false)?;
        Ok(added)
    }

    pub fn add_and_collect_cids(&mut self, ops: Vec<Operation>) -> Result<(usize, Vec<String>)> {
        self.add_internal(ops, true)
    }

    /// Validate performs a full chronological validation of all operations
    pub fn validate(&self) -> Result<()> {
        if self.operations.is_empty() {
            return Ok(());
        }

        // Check all operations are after minimum timestamp
        for (i, op) in self.operations.iter().enumerate() {
            let op_time = self.parse_timestamp(&op.created_at)?;
            if op_time < self.min_timestamp {
                bail!(
                    "operation {} (CID: {:?}) at {} is before minimum timestamp {}",
                    i,
                    op.cid,
                    op.created_at,
                    self.min_timestamp.to_rfc3339()
                );
            }
        }

        // Check chronological order
        for i in 1..self.operations.len() {
            let prev = &self.operations[i - 1];
            let curr = &self.operations[i];

            let prev_time = self.parse_timestamp(&prev.created_at)?;
            let curr_time = self.parse_timestamp(&curr.created_at)?;

            if curr_time < prev_time {
                bail!(
                    "chronological violation at index {}: {:?} ({}) is before {:?} ({})",
                    i,
                    curr.cid,
                    curr.created_at,
                    prev.cid,
                    prev.created_at
                );
            }
        }

        // Check for duplicate CIDs
        let mut cid_map: HashMap<String, usize> = HashMap::new();
        for (i, op) in self.operations.iter().enumerate() {
            if let Some(cid) = &op.cid {
                if let Some(prev_idx) = cid_map.get(cid) {
                    bail!("duplicate CID {} at indices {} and {}", cid, prev_idx, i);
                }
                cid_map.insert(cid.clone(), i);
            }
        }

        Ok(())
    }

    /// Count returns the number of operations in mempool
    pub fn count(&self) -> usize {
        self.operations.len()
    }

    /// Take removes and returns up to n operations from the front
    pub fn take(&mut self, n: usize) -> Result<Vec<Operation>> {
        // Validate before taking
        self.validate_locked()?;

        let take_count = n.min(self.operations.len());

        let result: Vec<Operation> = self.operations.drain(..take_count).collect();

        // Rebuild DID index after removing operations (indices have shifted)
        self.rebuild_did_index();

        // ALWAYS reset tracking after Take
        // Take() means we're consuming these ops for a bundle
        // Any remaining ops are "new" and unsaved
        self.last_saved_len = 0;
        self.last_save_time = std::time::Instant::now();

        // Mark dirty only if ops remain
        self.dirty = !self.operations.is_empty();
        self.validated = false;

        Ok(result)
    }

    /// validateLocked performs validation (internal helper)
    fn validate_locked(&mut self) -> Result<()> {
        if self.validated {
            return Ok(());
        }

        if self.operations.is_empty() {
            return Ok(());
        }

        // Check chronological order
        let mut last_time = self.min_timestamp;
        for (i, op) in self.operations.iter().enumerate() {
            let op_time = self.parse_timestamp(&op.created_at)?;
            if op_time < last_time {
                bail!(
                    "chronological violation at index {}: {} is before {}",
                    i,
                    op.created_at,
                    last_time.to_rfc3339()
                );
            }
            last_time = op_time;
        }

        self.validated = true;
        Ok(())
    }

    /// Peek returns up to n operations without removing them
    pub fn peek(&self, n: usize) -> Vec<Operation> {
        let count = n.min(self.operations.len());
        self.operations[..count].to_vec()
    }

    /// Clear removes all operations
    pub fn clear(&mut self) {
        let prev = self.operations.len();
        self.operations.clear();
        self.operations.shrink_to_fit();
        self.did_index.clear();
        self.did_index.shrink_to_fit();
        self.validated = false;
        self.dirty = true;
        if self.verbose {
            info!("mempool clear: removed {} ops", prev);
        }
    }

    /// ShouldSave checks if threshold/interval is met
    pub fn should_save(&self) -> bool {
        if !self.dirty {
            return false;
        }

        let new_ops = self.operations.len().saturating_sub(self.last_saved_len);
        let time_since_last_save = self.last_save_time.elapsed();

        new_ops >= self.save_threshold || time_since_last_save >= self.save_interval
    }

    /// SaveIfNeeded saves only if threshold is met
    pub fn save_if_needed(&mut self) -> Result<()> {
        if !self.should_save() {
            return Ok(());
        }
        self.save()
    }

    /// Save - always append-only since mempool only grows
    pub fn save(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }

        if self.operations.is_empty() {
            // Remove file if empty
            if self.file.exists() {
                fs::remove_file(&self.file)?;
            }
            self.last_saved_len = 0;
            self.last_save_time = std::time::Instant::now();
            self.dirty = false;
            return Ok(());
        }

        // Validate before saving
        self.validate_locked()?;

        // Bounds check to prevent panic
        if self.last_saved_len > self.operations.len() {
            if self.verbose {
                eprintln!(
                    "Warning: lastSavedLen ({}) > operations ({}), resetting to 0",
                    self.last_saved_len,
                    self.operations.len()
                );
            }
            self.last_saved_len = 0;
        }

        // Get only new operations since last save
        let new_ops = &self.operations[self.last_saved_len..];

        if new_ops.is_empty() {
            // Nothing new to save
            self.dirty = false;
            return Ok(());
        }

        // Open for append (or create if first save)
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file)?;

        let mut writer = BufWriter::new(file);

        // Write only new operations
        // CRITICAL: Use raw_json if available to preserve exact byte content
        // This is required for deterministic content_hash calculation.
        // Re-serialization would change field order/whitespace and break hash verification.
        let mut bytes_written = 0usize;
        let mut appended = 0usize;
        for op in new_ops {
            let json = if let Some(ref raw) = op.raw_json {
                raw.clone()
            } else {
                // Fallback: Re-serialize if raw_json is not available
                // WARNING: This may produce different content_hash than the original!
                sonic_rs::to_string(op)?
            };
            writeln!(writer, "{}", json)?;
            bytes_written += json.len() + 1;
            appended += 1;
        }

        writer.flush()?;

        // Get the underlying file to sync
        let file = writer.into_inner()?;
        file.sync_all()?;

        self.last_saved_len = self.operations.len();
        self.last_save_time = std::time::Instant::now();
        self.dirty = false;

        if self.verbose {
            info!(
                "mempool save: appended {} ops ({} bytes) to {}",
                appended,
                bytes_written,
                self.get_filename()
            );
        }

        Ok(())
    }

    /// Load reads mempool from disk and validates it
    pub fn load(&mut self) -> Result<()> {
        let start = std::time::Instant::now();
        let file = File::open(&self.file)?;
        let reader = BufReader::new(file);

        self.operations.clear();
        self.did_index.clear();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            // CRITICAL: Preserve raw JSON for content hash calculation
            // This is required by the V1 specification (docs/specification.md § 4.2)
            // to ensure content_hash remains reproducible.
            // Without this, re-serialization would change the hash.
            // Use Operation::from_json (sonic_rs) instead of serde deserialization
            let op = Operation::from_json(&line)?;
            let idx = self.operations.len();
            self.operations.push(op);

            // Update DID index
            let did = self.operations[idx].did.clone();
            self.did_index.entry(did).or_default().push(idx);
        }

        // Validate loaded data
        self.validate_locked()?;

        // Mark as saved (just loaded from disk)
        self.last_saved_len = self.operations.len();
        self.last_save_time = std::time::Instant::now();
        self.dirty = false;

        if self.verbose {
            info!(
                "mempool load: {} ops for bundle {:06} in {}",
                self.operations.len(),
                self.target_bundle,
                format_std_duration_ms(start.elapsed())
            );
        }

        Ok(())
    }

    /// GetFirstTime returns the created_at of the first operation
    pub fn get_first_time(&self) -> Option<String> {
        self.operations.first().map(|op| op.created_at.clone())
    }

    /// GetLastTime returns the created_at of the last operation
    pub fn get_last_time(&self) -> Option<String> {
        self.operations.last().map(|op| op.created_at.clone())
    }

    /// GetTargetBundle returns the bundle number this mempool is for
    pub fn get_target_bundle(&self) -> u32 {
        self.target_bundle
    }

    /// GetMinTimestamp returns the minimum timestamp for operations
    pub fn get_min_timestamp(&self) -> DateTime<Utc> {
        self.min_timestamp
    }

    /// Stats returns mempool statistics
    pub fn stats(&self) -> MempoolStats {
        let count = self.operations.len();

        let mut stats = MempoolStats {
            count,
            can_create_bundle: count >= constants::BUNDLE_SIZE,
            target_bundle: self.target_bundle,
            min_timestamp: self.min_timestamp,
            validated: self.validated,
            first_time: None,
            last_time: None,
            size_bytes: None,
            did_count: None,
        };

        if count > 0 {
            stats.first_time = self
                .operations
                .first()
                .and_then(|op| self.parse_timestamp(&op.created_at).ok());
            stats.last_time = self
                .operations
                .last()
                .and_then(|op| self.parse_timestamp(&op.created_at).ok());

            let mut total_size = 0;
            for op in &self.operations {
                if let Ok(json) = serde_json::to_string(op) {
                    total_size += json.len();
                }
            }

            stats.size_bytes = Some(total_size);
            stats.did_count = Some(self.did_index.len());
        }

        stats
    }

    /// Delete removes the mempool file
    pub fn delete(&self) -> Result<()> {
        if self.file.exists() {
            fs::remove_file(&self.file)?;
        }
        Ok(())
    }

    /// GetFilename returns the mempool filename
    pub fn get_filename(&self) -> String {
        self.file
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string()
    }

    /// FindDIDOperations searches for operations matching a DID
    /// Uses DID index for O(1) lookup instead of linear scan
    pub fn find_did_operations(&self, did: &str) -> Vec<Operation> {
        if let Some(indices) = self.did_index.get(did) {
            // Fast path: use index
            indices
                .iter()
                .map(|&idx| self.operations[idx].clone())
                .collect()
        } else {
            // DID not in index (shouldn't happen if index is maintained correctly)
            Vec::new()
        }
    }

    /// Rebuild DID index from operations (used after take/clear)
    fn rebuild_did_index(&mut self) {
        self.did_index.clear();
        for (idx, op) in self.operations.iter().enumerate() {
            self.did_index.entry(op.did.clone()).or_default().push(idx);
        }
    }

    /// FindLatestDIDOperation finds the most recent non-nullified operation for a DID
    /// Returns the operation and its position/index in the mempool
    /// Uses DID index for O(1) lookup, then finds latest by index (operations are chronologically sorted)
    pub fn find_latest_did_operation(&self, did: &str) -> Option<(Operation, usize)> {
        if let Some(indices) = self.did_index.get(did) {
            // Operations are in chronological order, so highest index = latest
            // Find the highest index that's not nullified
            indices.iter().rev().find_map(|&idx| {
                let op = &self.operations[idx];
                if !op.nullified {
                    Some((op.clone(), idx))
                } else {
                    None
                }
            })
        } else {
            None
        }
    }

    /// Get all operations (for dump command)
    pub fn get_operations(&self) -> &[Operation] {
        &self.operations
    }

    /// Parse timestamp string to DateTime
    fn parse_timestamp(&self, s: &str) -> Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(s)?.with_timezone(&Utc))
    }
}

#[derive(Debug, Clone)]
pub struct MempoolStats {
    pub count: usize,
    pub can_create_bundle: bool,
    pub target_bundle: u32,
    pub min_timestamp: DateTime<Utc>,
    pub validated: bool,
    pub first_time: Option<DateTime<Utc>>,
    pub last_time: Option<DateTime<Utc>>,
    pub size_bytes: Option<usize>,
    pub did_count: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::Operation;
    use sonic_rs::Value;
    use tempfile::TempDir;

    fn create_test_operation(did: &str, cid: &str, created_at: &str) -> Operation {
        Operation {
            did: did.to_string(),
            operation: Value::new(),
            cid: Some(cid.to_string()),
            nullified: false,
            created_at: created_at.to_string(),
            extra: Value::new(),
            raw_json: None,
        }
    }

    #[test]
    fn test_mempool_count() {
        let tmp = TempDir::new().unwrap();
        let min_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut mempool = Mempool::new(tmp.path(), 1, min_time, false).unwrap();

        assert_eq!(mempool.count(), 0);

        let ops = vec![
            create_test_operation("did:plc:test1", "cid1", "2024-01-01T00:00:01Z"),
            create_test_operation("did:plc:test2", "cid2", "2024-01-01T00:00:02Z"),
        ];
        mempool.add(ops).unwrap();
        assert_eq!(mempool.count(), 2);
    }

    #[test]
    fn test_mempool_get_target_bundle() {
        let tmp = TempDir::new().unwrap();
        let min_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mempool = Mempool::new(tmp.path(), 42, min_time, false).unwrap();
        assert_eq!(mempool.get_target_bundle(), 42);
    }

    #[test]
    fn test_mempool_get_min_timestamp() {
        let tmp = TempDir::new().unwrap();
        let min_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mempool = Mempool::new(tmp.path(), 1, min_time, false).unwrap();
        assert_eq!(mempool.get_min_timestamp(), min_time);
    }

    #[test]
    fn test_mempool_get_first_time() {
        let tmp = TempDir::new().unwrap();
        let min_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut mempool = Mempool::new(tmp.path(), 1, min_time, false).unwrap();

        assert_eq!(mempool.get_first_time(), None);

        let ops = vec![create_test_operation(
            "did:plc:test1",
            "cid1",
            "2024-01-01T00:00:01Z",
        )];
        mempool.add(ops).unwrap();
        assert_eq!(
            mempool.get_first_time(),
            Some("2024-01-01T00:00:01Z".to_string())
        );
    }

    #[test]
    fn test_mempool_get_last_time() {
        let tmp = TempDir::new().unwrap();
        let min_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut mempool = Mempool::new(tmp.path(), 1, min_time, false).unwrap();

        assert_eq!(mempool.get_last_time(), None);

        let ops = vec![
            create_test_operation("did:plc:test1", "cid1", "2024-01-01T00:00:01Z"),
            create_test_operation("did:plc:test2", "cid2", "2024-01-01T00:00:02Z"),
        ];
        mempool.add(ops).unwrap();
        assert_eq!(
            mempool.get_last_time(),
            Some("2024-01-01T00:00:02Z".to_string())
        );
    }

    #[test]
    fn test_mempool_peek() {
        let tmp = TempDir::new().unwrap();
        let min_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut mempool = Mempool::new(tmp.path(), 1, min_time, false).unwrap();

        let ops = vec![
            create_test_operation("did:plc:test1", "cid1", "2024-01-01T00:00:01Z"),
            create_test_operation("did:plc:test2", "cid2", "2024-01-01T00:00:02Z"),
            create_test_operation("did:plc:test3", "cid3", "2024-01-01T00:00:03Z"),
        ];
        mempool.add(ops).unwrap();

        let peeked = mempool.peek(2);
        assert_eq!(peeked.len(), 2);
        assert_eq!(peeked[0].cid, Some("cid1".to_string()));
        assert_eq!(peeked[1].cid, Some("cid2".to_string()));

        // Peek should not remove operations
        assert_eq!(mempool.count(), 3);
    }

    #[test]
    fn test_mempool_peek_more_than_available() {
        let tmp = TempDir::new().unwrap();
        let min_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut mempool = Mempool::new(tmp.path(), 1, min_time, false).unwrap();

        let ops = vec![create_test_operation(
            "did:plc:test1",
            "cid1",
            "2024-01-01T00:00:01Z",
        )];
        mempool.add(ops).unwrap();

        let peeked = mempool.peek(10);
        assert_eq!(peeked.len(), 1);
    }

    #[test]
    fn test_mempool_clear() {
        let tmp = TempDir::new().unwrap();
        let min_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut mempool = Mempool::new(tmp.path(), 1, min_time, false).unwrap();

        let ops = vec![
            create_test_operation("did:plc:test1", "cid1", "2024-01-01T00:00:01Z"),
            create_test_operation("did:plc:test2", "cid2", "2024-01-01T00:00:02Z"),
        ];
        mempool.add(ops).unwrap();
        assert_eq!(mempool.count(), 2);

        mempool.clear();
        assert_eq!(mempool.count(), 0);
    }

    #[test]
    fn test_mempool_find_did_operations() {
        let tmp = TempDir::new().unwrap();
        let min_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut mempool = Mempool::new(tmp.path(), 1, min_time, false).unwrap();

        let ops = vec![
            create_test_operation("did:plc:test1", "cid1", "2024-01-01T00:00:01Z"),
            create_test_operation("did:plc:test1", "cid2", "2024-01-01T00:00:02Z"),
            create_test_operation("did:plc:test2", "cid3", "2024-01-01T00:00:03Z"),
        ];
        mempool.add(ops).unwrap();

        let found = mempool.find_did_operations("did:plc:test1");
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].cid, Some("cid1".to_string()));
        assert_eq!(found[1].cid, Some("cid2".to_string()));

        let found = mempool.find_did_operations("did:plc:test2");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].cid, Some("cid3".to_string()));

        let found = mempool.find_did_operations("did:plc:nonexistent");
        assert_eq!(found.len(), 0);
    }

    #[test]
    fn test_mempool_stats_empty() {
        let tmp = TempDir::new().unwrap();
        let min_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mempool = Mempool::new(tmp.path(), 1, min_time, false).unwrap();

        let stats = mempool.stats();
        assert_eq!(stats.count, 0);
        assert!(!stats.can_create_bundle);
        assert_eq!(stats.target_bundle, 1);
    }

    #[test]
    fn test_mempool_stats_with_operations() {
        let tmp = TempDir::new().unwrap();
        let min_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut mempool = Mempool::new(tmp.path(), 1, min_time, false).unwrap();

        let ops = vec![
            create_test_operation("did:plc:test1", "cid1", "2024-01-01T00:00:01Z"),
            create_test_operation("did:plc:test2", "cid2", "2024-01-01T00:00:02Z"),
        ];
        mempool.add(ops).unwrap();

        let stats = mempool.stats();
        assert_eq!(stats.count, 2);
        assert!(!stats.can_create_bundle); // Need BUNDLE_SIZE (10000) ops
        assert_eq!(stats.target_bundle, 1);
        assert!(stats.first_time.is_some());
        assert!(stats.last_time.is_some());
        assert_eq!(stats.did_count, Some(2));
    }

    #[test]
    fn test_mempool_get_filename() {
        let tmp = TempDir::new().unwrap();
        let min_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mempool = Mempool::new(tmp.path(), 42, min_time, false).unwrap();

        let filename = mempool.get_filename();
        assert!(filename.contains("plc_mempool_"));
        assert!(filename.contains("000042"));
        assert!(filename.ends_with(".jsonl"));
    }
}
