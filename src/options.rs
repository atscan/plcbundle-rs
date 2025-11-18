//! Processor options and builder, including `QueryMode` for simple path vs JMESPath queries
use std::path::PathBuf;

/// Configuration options for the PLC bundle processor
#[derive(Debug, Clone)]
pub struct Options {
    /// Directory containing bundle files
    pub directory: PathBuf,
    /// Query expression (JMESPath or simple path)
    pub query: String,
    /// Query mode (simple or jmespath)
    pub query_mode: QueryMode,
    /// Number of worker threads (0 = auto)
    pub num_threads: usize,
    /// Output batch size (number of lines before flush)
    pub batch_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryMode {
    Simple,
    JmesPath,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("."),
            query: String::from("did"),
            query_mode: QueryMode::Simple,
            num_threads: 0,
            batch_size: 2000,
        }
    }
}

/// Builder for Options
pub struct OptionsBuilder {
    options: Options,
}

impl OptionsBuilder {
    pub fn new() -> Self {
        Self {
            options: Options::default(),
        }
    }

    pub fn directory<P: Into<PathBuf>>(mut self, dir: P) -> Self {
        self.options.directory = dir.into();
        self
    }

    pub fn query<S: Into<String>>(mut self, query: S) -> Self {
        self.options.query = query.into();
        self
    }

    pub fn query_mode(mut self, mode: QueryMode) -> Self {
        self.options.query_mode = mode;
        self
    }

    pub fn num_threads(mut self, n: usize) -> Self {
        self.options.num_threads = n;
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.options.batch_size = size;
        self
    }

    pub fn build(self) -> Options {
        self.options
    }
}

impl Default for OptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_options_default() {
        let opts = Options::default();
        assert_eq!(opts.directory, PathBuf::from("."));
        assert_eq!(opts.query, "did");
        assert_eq!(opts.query_mode, QueryMode::Simple);
        assert_eq!(opts.num_threads, 0);
        assert_eq!(opts.batch_size, 2000);
    }

    #[test]
    fn test_query_mode_equality() {
        assert_eq!(QueryMode::Simple, QueryMode::Simple);
        assert_eq!(QueryMode::JmesPath, QueryMode::JmesPath);
        assert_ne!(QueryMode::Simple, QueryMode::JmesPath);
    }

    #[test]
    fn test_options_builder_new() {
        let builder = OptionsBuilder::new();
        let opts = builder.build();
        assert_eq!(opts.query, "did");
        assert_eq!(opts.query_mode, QueryMode::Simple);
    }

    #[test]
    fn test_options_builder_default() {
        let builder = OptionsBuilder::default();
        let opts = builder.build();
        assert_eq!(opts.query, "did");
    }

    #[test]
    fn test_options_builder_directory() {
        let opts = OptionsBuilder::new()
            .directory("/tmp/test")
            .build();
        assert_eq!(opts.directory, PathBuf::from("/tmp/test"));
    }

    #[test]
    fn test_options_builder_query() {
        let opts = OptionsBuilder::new()
            .query("operation.type")
            .build();
        assert_eq!(opts.query, "operation.type");
    }

    #[test]
    fn test_options_builder_query_mode() {
        let opts = OptionsBuilder::new()
            .query_mode(QueryMode::JmesPath)
            .build();
        assert_eq!(opts.query_mode, QueryMode::JmesPath);
    }

    #[test]
    fn test_options_builder_num_threads() {
        let opts = OptionsBuilder::new()
            .num_threads(4)
            .build();
        assert_eq!(opts.num_threads, 4);
    }

    #[test]
    fn test_options_builder_batch_size() {
        let opts = OptionsBuilder::new()
            .batch_size(5000)
            .build();
        assert_eq!(opts.batch_size, 5000);
    }

    #[test]
    fn test_options_builder_chain() {
        let opts = OptionsBuilder::new()
            .directory("/tmp/test")
            .query("did")
            .query_mode(QueryMode::JmesPath)
            .num_threads(8)
            .batch_size(1000)
            .build();
        
        assert_eq!(opts.directory, PathBuf::from("/tmp/test"));
        assert_eq!(opts.query, "did");
        assert_eq!(opts.query_mode, QueryMode::JmesPath);
        assert_eq!(opts.num_threads, 8);
        assert_eq!(opts.batch_size, 1000);
    }
}
