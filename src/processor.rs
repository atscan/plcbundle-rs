use crate::options::Options;
use crate::query::QueryEngine;
use crate::index::Index;
use anyhow::Result;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Default, Clone, Debug)]
pub struct Stats {
    pub operations: usize,
    pub matches: usize,
    pub total_bytes: u64,
    pub matched_bytes: u64,
}

/// Main processor for PLC bundles
pub struct Processor {
    options: Options,
    query_engine: QueryEngine,
}

pub trait OutputHandler: Send + Sync {
    fn write_batch(&self, batch: &str) -> Result<()>;
}

pub struct OutputBuffer {
    buffer: String,
    capacity: usize,
    count: usize,
    matched_bytes: u64,
}

impl OutputBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: String::with_capacity(capacity * 100),
            capacity,
            count: 0,
            matched_bytes: 0,
        }
    }

    pub fn push(&mut self, line: &str) -> bool {
        self.matched_bytes += line.len() as u64 + 1;
        self.buffer.push_str(line);
        self.buffer.push('\n');
        self.count += 1;
        self.count >= self.capacity
    }

    pub fn flush(&mut self) -> String {
        self.count = 0;
        std::mem::replace(&mut self.buffer, String::with_capacity(self.capacity * 100))
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn get_matched_bytes(&self) -> u64 {
        self.matched_bytes
    }
}

impl Processor {
    /// Create a new processor with the given options
    pub fn new(options: Options) -> Result<Self> {
        let query_engine = QueryEngine::new(&options.query, options.query_mode)?;
        
        Ok(Self {
            options,
            query_engine,
        })
    }

    /// Load the bundle index
    pub fn load_index(&self) -> Result<Index> {
        Index::load(&self.options.directory)
    }

    /// Get the directory path
    pub fn directory(&self) -> &PathBuf {
        &self.options.directory
    }

    /// Process specified bundles
    pub fn process<F>(
        &self,
        bundle_numbers: &[u32],
        output_handler: Arc<dyn OutputHandler>,
        progress_callback: Option<F>,
    ) -> Result<Stats>
    where
        F: Fn(usize, &Stats) + Send + Sync,
    {
        if self.options.num_threads > 1 {
            rayon::ThreadPoolBuilder::new()
                .num_threads(self.options.num_threads)
                .build_global()?;
        }

        let total_stats = Arc::new(Mutex::new(Stats::default()));
        let callback = Arc::new(progress_callback);

        let process_fn = |bundle_num: &u32| -> Result<()> {
            let bundle_path = self.options.directory.join(format!("{:06}.jsonl.zst", bundle_num));
            
            if !bundle_path.exists() {
                return Ok(());
            }

            let stats = self.process_bundle(&bundle_path, &output_handler)?;

            let mut total = total_stats.lock().unwrap();
            total.operations += stats.operations;
            total.matches += stats.matches;
            total.total_bytes += stats.total_bytes;
            total.matched_bytes += stats.matched_bytes;

            if let Some(ref cb) = *callback {
                cb(1, &total);
            }

            Ok(())
        };

        if self.options.num_threads == 1 {
            bundle_numbers.iter().try_for_each(process_fn)?;
        } else {
            bundle_numbers.par_iter().try_for_each(process_fn)?;
        }

        let final_stats = total_stats.lock().unwrap().clone();
        Ok(final_stats)
    }

    fn process_bundle(
        &self,
        bundle_path: &PathBuf,
        output_handler: &Arc<dyn OutputHandler>,
    ) -> Result<Stats> {
        let file = File::open(bundle_path)?;
        let decoder = zstd::Decoder::new(file)?;
        let reader = BufReader::with_capacity(1024 * 1024, decoder);

        let mut stats = Stats::default();
        let mut output_buffer = OutputBuffer::new(self.options.batch_size);

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }

            stats.operations += 1;
            stats.total_bytes += line.len() as u64 + 1;

            if let Some(result) = self.query_engine.query(&line)? {
                stats.matches += 1;
                if output_buffer.push(&result) {
                    output_handler.write_batch(&output_buffer.flush())?;
                }
            }
        }

        if !output_buffer.is_empty() {
            output_handler.write_batch(&output_buffer.flush())?;
        }

        stats.matched_bytes = output_buffer.get_matched_bytes();
        Ok(stats)
    }
}

/// Parse bundle range specification (e.g., "1-10,15,20-25")
pub fn parse_bundle_range(spec: &str, max_bundle: u32) -> Result<Vec<u32>> {
    let mut bundles = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.contains('-') {
            let range: Vec<&str> = part.split('-').collect();
            if range.len() != 2 {
                anyhow::bail!("Invalid range format: {}", part);
            }
            let start: u32 = range[0].parse()?;
            let end: u32 = range[1].parse()?;
            if start > end || start == 0 || end > max_bundle {
                anyhow::bail!("Invalid range: {}-{}", start, end);
            }
            bundles.extend(start..=end);
        } else {
            let num: u32 = part.parse()?;
            if num == 0 || num > max_bundle {
                anyhow::bail!("Bundle number {} out of range", num);
            }
            bundles.push(num);
        }
    }
    bundles.sort_unstable();
    bundles.dedup();
    Ok(bundles)
}
