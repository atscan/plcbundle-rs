//! Query engine and processor supporting simple path and JMESPath queries with parallel bundle processing and batched output
use crate::constants;
use crate::index::Index;
use crate::options::{Options, QueryMode};
use anyhow::Result;
use rayon::prelude::*;
use sonic_rs::JsonValueTrait;
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

/// Query engine that supports both simple and JMESPath queries
pub enum QueryEngine {
    JmesPath(jmespath::Expression<'static>),
    Simple(Vec<String>),
}

impl QueryEngine {
    pub fn new(query: &str, mode: QueryMode) -> Result<Self> {
        match mode {
            QueryMode::Simple => {
                let path: Vec<String> = query.split('.').map(|s| s.to_string()).collect();
                Ok(QueryEngine::Simple(path))
            }
            QueryMode::JmesPath => {
                let expr = jmespath::compile(query)
                    .map_err(|e| anyhow::anyhow!("Failed to compile JMESPath: {}", e))?;
                let expr = Box::leak(Box::new(expr));
                Ok(QueryEngine::JmesPath(expr.clone()))
            }
        }
    }

    pub fn query(&self, json_line: &str) -> Result<Option<String>> {
        match self {
            QueryEngine::Simple(path) => self.query_simple(json_line, path),
            QueryEngine::JmesPath(expr) => self.query_jmespath(json_line, expr),
        }
    }

    fn query_simple(&self, json_line: &str, path: &[String]) -> Result<Option<String>> {
        let data: sonic_rs::Value = sonic_rs::from_str(json_line)?;

        let mut current = &data;
        for key in path {
            match current.get(key) {
                Some(v) => current = v,
                None => return Ok(None),
            }
        }

        if current.is_null() {
            return Ok(None);
        }

        let output = if current.is_str() {
            current.as_str().unwrap().to_string()
        } else {
            sonic_rs::to_string(current)?
        };

        Ok(Some(output))
    }

    fn query_jmespath(
        &self,
        json_line: &str,
        expr: &jmespath::Expression<'_>,
    ) -> Result<Option<String>> {
        let data = jmespath::Variable::from_json(json_line)
            .map_err(|e| anyhow::anyhow!("Failed to parse JSON: {}", e))?;

        let result = expr
            .search(&data)
            .map_err(|e| anyhow::anyhow!("JMESPath search failed: {}", e))?;

        if result.is_null() {
            return Ok(None);
        }

        let output = if result.is_string() {
            result.as_string().unwrap().to_string()
        } else {
            // Convert jmespath result to JSON string
            // Note: jmespath uses serde_json internally, so we use serde_json here (not bundle/operation data)
            serde_json::to_string(&*result)
                .map_err(|e| anyhow::anyhow!("Failed to serialize result: {}", e))?
        };

        Ok(Some(output))
    }
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
            let bundle_path = constants::bundle_path(&self.options.directory, *bundle_num);

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

/// Resolve bundle keyword to concrete bundle number
fn resolve_bundle_keyword(keyword: &str, max_bundle: u32) -> Result<u32> {
    let keyword = keyword.trim();

    if keyword == "root" {
        return Ok(1);
    }

    if keyword == "head" {
        return Ok(max_bundle);
    }

    // Handle "head~N" syntax
    if let Some(rest) = keyword.strip_prefix("head~") {
        let offset: u32 = rest
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid offset in 'head~{}': expected a number", rest))?;
        if offset >= max_bundle {
            anyhow::bail!(
                "Offset {} in 'head~{}' exceeds maximum bundle {}",
                offset,
                rest,
                max_bundle
            );
        }
        return Ok(max_bundle - offset);
    }

    // Handle "~N" shorthand syntax
    if let Some(rest) = keyword.strip_prefix('~') {
        let offset: u32 = rest
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid offset in '~{}': expected a number", rest))?;
        if offset >= max_bundle {
            anyhow::bail!(
                "Offset {} in '~{}' exceeds maximum bundle {}",
                offset,
                rest,
                max_bundle
            );
        }
        return Ok(max_bundle - offset);
    }

    // Not a keyword, try parsing as number
    let num: u32 = keyword.parse().map_err(|_| {
        anyhow::anyhow!(
            "Invalid bundle specifier: '{}' (expected number, 'root', 'head', 'head~N', or '~N')",
            keyword
        )
    })?;
    Ok(num)
}

/// Parse bundle range specification (e.g., "1-10,15,20-25", "head", "head~5", "root")
pub fn parse_bundle_range(spec: &str, max_bundle: u32) -> Result<Vec<u32>> {
    if max_bundle == 0 {
        anyhow::bail!("No bundles available");
    }

    let mut bundles = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if part.contains('-') {
            let range: Vec<&str> = part.split('-').collect();
            if range.len() != 2 {
                anyhow::bail!("Invalid range format: {}", part);
            }
            let start_str = range[0].trim();
            let end_str = range[1].trim();

            let start = resolve_bundle_keyword(start_str, max_bundle)?;
            let end = resolve_bundle_keyword(end_str, max_bundle)?;

            if start == 0 || end == 0 {
                anyhow::bail!("Bundle number cannot be 0");
            }
            if start > end {
                anyhow::bail!("Invalid range: {} > {} (start must be <= end)", start, end);
            }
            if start > max_bundle || end > max_bundle {
                anyhow::bail!(
                    "Invalid range: {}-{} (exceeds maximum bundle {})",
                    start,
                    end,
                    max_bundle
                );
            }
            bundles.extend(start..=end);
        } else {
            let num = resolve_bundle_keyword(part, max_bundle)?;
            if num == 0 {
                anyhow::bail!("Bundle number cannot be 0");
            }
            if num > max_bundle {
                anyhow::bail!("Bundle number {} out of range (max: {})", num, max_bundle);
            }
            bundles.push(num);
        }
    }
    bundles.sort_unstable();
    bundles.dedup();
    Ok(bundles)
}

/// Parse operation range specification (e.g., "0-999", "3255,553,0-9")
/// Operations are 0-indexed global positions (0 = first operation, bundle 1 position 0)
pub fn parse_operation_range(spec: &str, max_operation: u64) -> Result<Vec<u64>> {
    use anyhow::Context;

    if max_operation == 0 {
        anyhow::bail!("No operations available");
    }

    let mut operations = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if part.contains('-') {
            let range: Vec<&str> = part.split('-').collect();
            if range.len() != 2 {
                anyhow::bail!("Invalid range format: {}", part);
            }
            let start_str = range[0].trim();
            let end_str = range[1].trim();

            let start: u64 = start_str
                .parse()
                .with_context(|| format!("Invalid operation number: {}", start_str))?;
            let end: u64 = end_str
                .parse()
                .with_context(|| format!("Invalid operation number: {}", end_str))?;

            if start > end {
                anyhow::bail!("Invalid range: {} > {} (start must be <= end)", start, end);
            }
            if start > max_operation || end > max_operation {
                anyhow::bail!(
                    "Invalid range: {}-{} (exceeds maximum operation {})",
                    start,
                    end,
                    max_operation
                );
            }
            operations.extend(start..=end);
        } else {
            let num: u64 = part
                .parse()
                .with_context(|| format!("Invalid operation number: {}", part))?;
            if num > max_operation {
                anyhow::bail!(
                    "Operation number {} out of range (max: {})",
                    num,
                    max_operation
                );
            }
            operations.push(num);
        }
    }
    operations.sort_unstable();
    operations.dedup();
    Ok(operations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_engine_simple() {
        let engine = QueryEngine::new("did", QueryMode::Simple).unwrap();
        let json = r#"{"did": "did:plc:test", "operation": {"type": "create"}}"#;
        let result = engine.query(json).unwrap();
        assert_eq!(result, Some("did:plc:test".to_string()));
    }

    #[test]
    fn test_query_engine_simple_nested() {
        let engine = QueryEngine::new("operation.type", QueryMode::Simple).unwrap();
        let json = r#"{"did": "did:plc:test", "operation": {"type": "create"}}"#;
        let result = engine.query(json).unwrap();
        assert_eq!(result, Some("create".to_string()));
    }

    #[test]
    fn test_query_engine_simple_missing() {
        let engine = QueryEngine::new("missing.field", QueryMode::Simple).unwrap();
        let json = r#"{"did": "did:plc:test"}"#;
        let result = engine.query(json).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_query_engine_simple_null() {
        let engine = QueryEngine::new("null_field", QueryMode::Simple).unwrap();
        let json = r#"{"null_field": null}"#;
        let result = engine.query(json).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_query_engine_jmespath() {
        let engine = QueryEngine::new("did", QueryMode::JmesPath).unwrap();
        let json = r#"{"did": "did:plc:test", "operation": {"type": "create"}}"#;
        let result = engine.query(json).unwrap();
        assert_eq!(result, Some("did:plc:test".to_string()));
    }

    #[test]
    fn test_query_engine_jmespath_nested() {
        let engine = QueryEngine::new("operation.type", QueryMode::JmesPath).unwrap();
        let json = r#"{"did": "did:plc:test", "operation": {"type": "create"}}"#;
        let result = engine.query(json).unwrap();
        assert_eq!(result, Some("create".to_string()));
    }

    #[test]
    fn test_output_buffer_new() {
        let buffer = OutputBuffer::new(100);
        assert!(buffer.is_empty());
        assert_eq!(buffer.get_matched_bytes(), 0);
    }

    #[test]
    fn test_output_buffer_push() {
        let mut buffer = OutputBuffer::new(2);
        assert!(!buffer.push("line1")); // count = 1, capacity = 2, so false
        // When we push line2, count becomes 2, which equals capacity, so returns true
        assert!(buffer.push("line2")); // Should return true when capacity reached
        // After flush, count resets to 0
        buffer.flush();
        assert!(!buffer.push("line3")); // count = 1 again
    }

    #[test]
    fn test_output_buffer_flush() {
        let mut buffer = OutputBuffer::new(10);
        buffer.push("line1");
        buffer.push("line2");

        let flushed = buffer.flush();
        assert_eq!(flushed, "line1\nline2\n");
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_output_buffer_matched_bytes() {
        let mut buffer = OutputBuffer::new(10);
        buffer.push("line1");
        buffer.push("line2");

        // Each line adds len + 1 (for newline)
        assert_eq!(buffer.get_matched_bytes(), 5 + 1 + 5 + 1);
    }

    #[test]
    fn test_parse_bundle_range_single() {
        let result = parse_bundle_range("1", 10).unwrap();
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_parse_bundle_range_multiple() {
        let result = parse_bundle_range("1,3,5", 10).unwrap();
        assert_eq!(result, vec![1, 3, 5]);
    }

    #[test]
    fn test_parse_bundle_range_range() {
        let result = parse_bundle_range("1-3", 10).unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_parse_bundle_range_mixed() {
        let result = parse_bundle_range("1,3-5,7", 10).unwrap();
        assert_eq!(result, vec![1, 3, 4, 5, 7]);
    }

    #[test]
    fn test_parse_bundle_range_dedup() {
        let result = parse_bundle_range("1,1,2,2", 10).unwrap();
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn test_parse_bundle_range_empty_max() {
        let result = parse_bundle_range("1", 0);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No bundles available")
        );
    }

    #[test]
    fn test_parse_bundle_range_out_of_range() {
        let result = parse_bundle_range("11", 10);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of range"));
    }

    #[test]
    fn test_parse_bundle_range_invalid_range() {
        let result = parse_bundle_range("5-3", 10);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("start must be <= end")
        );
    }

    #[test]
    fn test_parse_bundle_range_invalid_format() {
        let result = parse_bundle_range("1-2-3", 10);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid range format")
        );
    }

    #[test]
    fn test_parse_bundle_range_keyword_root() {
        let result = parse_bundle_range("root", 10).unwrap();
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_parse_bundle_range_keyword_head() {
        let result = parse_bundle_range("head", 10).unwrap();
        assert_eq!(result, vec![10]);
    }

    #[test]
    fn test_parse_bundle_range_keyword_head_offset() {
        let result = parse_bundle_range("head~3", 10).unwrap();
        assert_eq!(result, vec![7]); // 10 - 3 = 7
    }

    #[test]
    fn test_parse_bundle_range_keyword_shorthand_offset() {
        let result = parse_bundle_range("~2", 10).unwrap();
        assert_eq!(result, vec![8]); // 10 - 2 = 8
    }

    #[test]
    fn test_parse_bundle_range_keyword_mixed() {
        let result = parse_bundle_range("root,head,head~1", 10).unwrap();
        assert_eq!(result, vec![1, 9, 10]);
    }

    #[test]
    fn test_parse_bundle_range_keyword_head_offset_invalid() {
        let result = parse_bundle_range("head~10", 10);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("exceeds maximum bundle")
        );
    }

    #[test]
    fn test_parse_bundle_range_keyword_shorthand_offset_invalid() {
        let result = parse_bundle_range("~10", 10);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("exceeds maximum bundle")
        );
    }

    #[test]
    fn test_parse_bundle_range_keyword_invalid_offset() {
        let result = parse_bundle_range("head~abc", 10);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid offset"));
    }

    #[test]
    fn test_parse_operation_range_single() {
        let result = parse_operation_range("0", 1000).unwrap();
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_parse_operation_range_multiple() {
        let result = parse_operation_range("0,5,10", 1000).unwrap();
        assert_eq!(result, vec![0, 5, 10]);
    }

    #[test]
    fn test_parse_operation_range_range() {
        let result = parse_operation_range("0-2", 1000).unwrap();
        assert_eq!(result, vec![0, 1, 2]);
    }

    #[test]
    fn test_parse_operation_range_mixed() {
        let result = parse_operation_range("0,5-7,10", 1000).unwrap();
        assert_eq!(result, vec![0, 5, 6, 7, 10]);
    }

    #[test]
    fn test_parse_operation_range_dedup() {
        let result = parse_operation_range("0,0,1,1", 1000).unwrap();
        assert_eq!(result, vec![0, 1]);
    }

    #[test]
    fn test_parse_operation_range_empty_max() {
        let result = parse_operation_range("0", 0);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No operations available")
        );
    }

    #[test]
    fn test_parse_operation_range_out_of_range() {
        let result = parse_operation_range("1001", 1000);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of range"));
    }

    #[test]
    fn test_parse_operation_range_invalid_range() {
        let result = parse_operation_range("10-5", 1000);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("start must be <= end")
        );
    }

    #[test]
    fn test_parse_operation_range_invalid_format() {
        let result = parse_operation_range("1-2-3", 1000);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid range format")
        );
    }

    #[test]
    fn test_parse_operation_range_invalid_number() {
        let result = parse_operation_range("abc", 1000);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid operation number")
        );
    }

    #[test]
    fn test_stats_default() {
        let stats = Stats::default();
        assert_eq!(stats.operations, 0);
        assert_eq!(stats.matches, 0);
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.matched_bytes, 0);
    }
}
