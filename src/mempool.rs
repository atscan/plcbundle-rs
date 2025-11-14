// src/mempool.rs
use crate::constants;
use crate::operations::Operation;
use anyhow::{Result, bail};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use chrono::{DateTime, Utc};

/// Mempool stores operations waiting to be bundled
/// Operations must be strictly chronological
pub struct Mempool {
    operations: Vec<Operation>,
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
        let filename = format!("{}{:06}.jsonl", constants::MEMPOOL_FILE_PREFIX, target_bundle);
        let file = bundle_dir.join(filename);

        let mut mempool = Self {
            operations: Vec::new(),
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

    /// Add operations to the mempool with strict validation
    pub fn add(&mut self, ops: Vec<Operation>) -> Result<usize> {
        if ops.is_empty() {
            return Ok(0);
        }

        // Build existing CID set
        let mut existing_cids: HashSet<String> = self.operations
            .iter()
            .filter_map(|op| op.cid.clone())
            .collect();

        let mut new_ops = Vec::new();

        // Start from last operation time if we have any
        let mut last_time = if !self.operations.is_empty() {
            self.parse_timestamp(&self.operations.last().unwrap().created_at)?
        } else {
            self.min_timestamp
        };

        for op in ops {
            // Skip if no CID
            let cid = match &op.cid {
                Some(c) => c,
                None => continue,
            };

            // Skip duplicates
            if existing_cids.contains(cid) {
                continue;
            }

            let op_time = self.parse_timestamp(&op.created_at)?;

            // CRITICAL: Validate chronological order
            if op_time < last_time {
                bail!(
                    "chronological violation: operation {} at {} is before {}",
                    cid,
                    op.created_at,
                    last_time.to_rfc3339()
                );
            }

            // Validate operation is after minimum timestamp
            if op_time < self.min_timestamp {
                bail!(
                    "operation {} at {} is before minimum timestamp {} (belongs in earlier bundle)",
                    cid,
                    op.created_at,
                    self.min_timestamp.to_rfc3339()
                );
            }

            new_ops.push(op.clone());
            existing_cids.insert(cid.clone());
            last_time = op_time;
        }

        let added = new_ops.len();

        // Add new operations
        self.operations.extend(new_ops);
        self.validated = true;
        self.dirty = true;

        Ok(added)
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
        self.operations.clear();
        self.validated = false;
        self.dirty = true;
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
        for op in new_ops {
            let json = if let Some(ref raw) = op.raw_json {
                raw.clone()
            } else {
                // Fallback: Re-serialize if raw_json is not available
                // WARNING: This may produce different content_hash than the original!
                serde_json::to_string(op)?
            };
            writeln!(writer, "{}", json)?;
        }

        writer.flush()?;

        // Get the underlying file to sync
        let file = writer.into_inner()?;
        file.sync_all()?;

        self.last_saved_len = self.operations.len();
        self.last_save_time = std::time::Instant::now();
        self.dirty = false;

        Ok(())
    }

    /// Load reads mempool from disk and validates it
    pub fn load(&mut self) -> Result<()> {
        let file = File::open(&self.file)?;
        let reader = BufReader::new(file);

        self.operations.clear();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            // CRITICAL: Preserve raw JSON for content hash calculation
            // This is required by the V1 specification (docs/specification.md § 4.2)
            // to ensure content_hash remains reproducible.
            // Without this, re-serialization would change the hash.
            let mut op: Operation = serde_json::from_str(&line)?;
            op.raw_json = Some(line);
            self.operations.push(op);
        }

        // Validate loaded data
        self.validate_locked()?;

        // Mark as saved (just loaded from disk)
        self.last_saved_len = self.operations.len();
        self.last_save_time = std::time::Instant::now();
        self.dirty = false;

        if self.verbose && !self.operations.is_empty() {
            eprintln!(
                "Loaded {} operations from mempool for bundle {:06}",
                self.operations.len(),
                self.target_bundle
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
            stats.first_time = self.operations.first().and_then(|op| {
                self.parse_timestamp(&op.created_at).ok()
            });
            stats.last_time = self.operations.last().and_then(|op| {
                self.parse_timestamp(&op.created_at).ok()
            });

            // Calculate size and unique DIDs
            let mut total_size = 0;
            let mut did_set: HashSet<String> = HashSet::new();

            for op in &self.operations {
                // Approximate JSON size
                if let Ok(json) = serde_json::to_string(op) {
                    total_size += json.len();
                }
                did_set.insert(op.did.clone());
            }

            stats.size_bytes = Some(total_size);
            stats.did_count = Some(did_set.len());
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
    pub fn find_did_operations(&self, did: &str) -> Vec<Operation> {
        self.operations
            .iter()
            .filter(|op| op.did == did)
            .cloned()
            .collect()
    }

    /// FindLatestDIDOperation finds the most recent non-nullified operation for a DID
    pub fn find_latest_did_operation(&self, did: &str) -> Option<Operation> {
        // Search backwards from most recent
        self.operations
            .iter()
            .rev()
            .find(|op| op.did == did && !op.nullified)
            .cloned()
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
