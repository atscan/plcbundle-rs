//! plcbundle-rs - High-performance PLC bundle query library
//! 
//! This library provides efficient querying and processing of PLC bundles.

pub mod options;
pub mod processor;
pub mod query;
pub mod index;
pub mod ffi;

// Re-export main types
pub use options::{Options, OptionsBuilder, QueryMode};
pub use processor::{Processor, Stats, OutputHandler};
pub use query::QueryEngine;
pub use index::{Index, BundleMetadata};

// Re-export utility functions
pub use processor::parse_bundle_range;

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_bundle_range() {
        let result = parse_bundle_range("1-5,10,15-17", 20).unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 10, 15, 16, 17]);
    }
}
