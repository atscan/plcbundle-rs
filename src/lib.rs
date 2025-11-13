// src/lib.rs
use anyhow::Result;
use rayon::prelude::*;
use serde::Deserialize;
use sonic_rs::JsonValueTrait;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Debug, Deserialize)]
pub struct Index {
    pub version: String,
    pub origin: String,
    pub last_bundle: u32,
    pub bundles: Vec<BundleMetadata>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BundleMetadata {
    pub bundle_number: u32,
    pub operation_count: u32,
}

pub enum QueryEngine {
    JmesPath(jmespath::Expression<'static>),
    Simple(Vec<String>),
}

#[derive(Default, Clone)]
pub struct Stats {
    pub operations: usize,
    pub matches: usize,
    pub total_bytes: u64,
    pub matched_bytes: u64,
}

pub struct BundleProcessor {
    pub bundle_dir: PathBuf,
    pub query_engine: QueryEngine,
    pub num_threads: usize,
    pub batch_size: usize,
}

// Changed: OutputHandler now receives batches instead of individual lines
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

impl BundleProcessor {
    pub fn new(
        bundle_dir: PathBuf,
        query: &str,
        simple_mode: bool,
        num_threads: usize,
        batch_size: usize,
    ) -> Result<Self> {
        let query_engine = if simple_mode {
            let path: Vec<String> = query.split('.').map(|s| s.to_string()).collect();
            QueryEngine::Simple(path)
        } else {
            let expr = jmespath::compile(query)
                .map_err(|e| anyhow::anyhow!("Failed to compile JMESPath: {}", e))?;
            let expr = Box::leak(Box::new(expr));
            QueryEngine::JmesPath(expr.clone())
        };

        Ok(Self {
            bundle_dir,
            query_engine,
            num_threads,
            batch_size,
        })
    }

    pub fn load_index(&self) -> Result<Index> {
        let index_path = self.bundle_dir.join("plc_bundles.json");
        let file = File::open(&index_path)?;
        Ok(sonic_rs::from_reader(file)?)
    }

    pub fn process_bundles<F>(
        &self,
        bundle_numbers: &[u32],
        output_handler: Arc<dyn OutputHandler>,
        progress_callback: Option<F>,
    ) -> Result<Stats>
    where
        F: Fn(usize, &Stats) + Send + Sync,
    {
        if self.num_threads > 1 {
            rayon::ThreadPoolBuilder::new()
                .num_threads(self.num_threads)
                .build_global()?;
        }

        let total_stats = Arc::new(Mutex::new(Stats::default()));
        let callback = Arc::new(progress_callback);

        let process_fn = |bundle_num: &u32| -> Result<()> {
            let bundle_path = self.bundle_dir.join(format!("{:06}.jsonl.zst", bundle_num));
            
            if !bundle_path.exists() {
                return Ok(());
            }

            let stats = match &self.query_engine {
                QueryEngine::Simple(path) => {
                    self.process_bundle_simple(&bundle_path, path, &output_handler)?
                }
                QueryEngine::JmesPath(expr) => {
                    self.process_bundle_jmespath(&bundle_path, expr, &output_handler)?
                }
            };

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

        if self.num_threads == 1 {
            bundle_numbers.iter().try_for_each(process_fn)?;
        } else {
            bundle_numbers.par_iter().try_for_each(process_fn)?;
        }

        let final_stats = total_stats.lock().unwrap().clone();
        Ok(final_stats)
    }

    fn process_bundle_simple(
        &self,
        bundle_path: &PathBuf,
        path: &[String],
        output_handler: &Arc<dyn OutputHandler>,
    ) -> Result<Stats> {
        let file = File::open(bundle_path)?;
        let decoder = zstd::Decoder::new(file)?;
        let reader = BufReader::with_capacity(1024 * 1024, decoder);

        let mut stats = Stats::default();
        let mut output_buffer = OutputBuffer::new(self.batch_size);

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }

            stats.operations += 1;
            stats.total_bytes += line.len() as u64 + 1;

            let data: sonic_rs::Value = sonic_rs::from_str(&line)?;
            
            if let Some(result) = query_simple(&data, path) {
                if !result.is_null() {
                    stats.matches += 1;
                    let output = if result.is_str() {
                        result.as_str().unwrap().to_string()
                    } else {
                        sonic_rs::to_string(&result)?
                    };
                    
                    if output_buffer.push(&output) {
                        output_handler.write_batch(&output_buffer.flush())?;
                    }
                }
            }
        }

        if !output_buffer.is_empty() {
            output_handler.write_batch(&output_buffer.flush())?;
        }

        stats.matched_bytes = output_buffer.get_matched_bytes();
        Ok(stats)
    }

    fn process_bundle_jmespath(
        &self,
        bundle_path: &PathBuf,
        expr: &jmespath::Expression<'_>,
        output_handler: &Arc<dyn OutputHandler>,
    ) -> Result<Stats> {
        let file = File::open(bundle_path)?;
        let decoder = zstd::Decoder::new(file)?;
        let reader = BufReader::with_capacity(1024 * 1024, decoder);

        let mut stats = Stats::default();
        let mut output_buffer = OutputBuffer::new(self.batch_size);

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }

            stats.operations += 1;
            stats.total_bytes += line.len() as u64 + 1;

            let data = jmespath::Variable::from_json(&line)
                .map_err(|e| anyhow::anyhow!("Failed to parse JSON: {}", e))?;
            
            let result = expr.search(&data)
                .map_err(|e| anyhow::anyhow!("JMESPath search failed: {}", e))?;
            
            if !result.is_null() {
                stats.matches += 1;
                let output = if result.is_string() {
                    result.as_string().unwrap().to_string()
                } else {
                    sonic_rs::to_string(&result)?
                };
                
                if output_buffer.push(&output) {
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

pub fn query_simple(value: &sonic_rs::Value, path: &[String]) -> Option<sonic_rs::Value> {
    let mut current = value;
    for key in path {
        current = current.get(key)?;
    }
    Some(current.clone())
}

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

pub mod ffi;