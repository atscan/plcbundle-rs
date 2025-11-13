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
