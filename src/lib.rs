// src/lib.rs
pub mod options;
pub mod processor;
pub mod query;
pub mod index;
pub mod ffi;
pub mod manager;
pub mod operations;
pub mod did_index;
pub mod cache;
pub mod verification;
pub mod iterators;
pub mod bundle_format;
pub mod resolver;

// Re-export main types
pub use options::{Options, OptionsBuilder, QueryMode};
pub use processor::{Processor, Stats, OutputHandler, parse_bundle_range};
pub use query::QueryEngine;
pub use index::{Index, BundleMetadata};
pub use manager::{BundleManager, LoadOptions, LoadResult, OperationResult, QuerySpec, ExportSpec,
                  BundleRange, ExportFormat, CompressionType,
                  VerifySpec, VerifyResult, ChainVerifySpec, ChainVerifyResult,
                  BundleInfo, InfoFlags, RollbackSpec, RollbackPlan, RollbackResult,
                  WarmUpSpec, WarmUpStrategy, RebuildStats, DIDIndexStats, ManagerStats};
pub use operations::{Operation, OperationFilter, OperationRequest};
pub use iterators::{QueryIterator, ExportIterator, RangeIterator};
pub use resolver::{DIDDocument, DIDState, resolve_did_document, build_did_state, validate_did_format};
