// src/operations.rs
use serde::{Deserialize, Serialize};
use sonic_rs::{self, Value};

/// PLC Operation
///
/// Represents a single operation from the PLC directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub did: String,
    #[serde(alias = "operation")]
    pub operation: Value,
    #[serde(default)]
    pub cid: Option<String>,
    #[serde(default)]
    pub nullified: bool,
    #[serde(rename = "createdAt", alias = "created_at")]
    pub created_at: String,
    #[serde(flatten)]
    pub extra: Value,

    /// CRITICAL: Raw JSON bytes as received from source
    ///
    /// This field stores the exact JSON representation as received from the PLC directory
    /// or loaded from disk. It is required by the V1 specification (docs/specification.md § 4.2)
    /// to ensure content_hash is reproducible across implementations.
    ///
    /// **IMPORTANT**: All code that creates or loads operations MUST populate this field
    /// with the original JSON string. Re-serializing the struct will produce different
    /// byte output (different field order, whitespace) which breaks hash verification.
    ///
    /// This field is:
    /// - Set when fetching from PLC directory (src/sync.rs)
    /// - Set when loading from bundle files (src/manager.rs)
    /// - Used when serializing to bundles (src/bundle_format.rs)
    /// - Skipped during JSON serialization (#[serde(skip)])
    #[serde(skip)]
    pub raw_json: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct OperationFilter {
    pub did: Option<String>,
    pub operation_type: Option<String>,
    pub time_range: Option<(String, String)>,
    pub include_nullified: bool,
}

#[derive(Debug, Clone)]
pub struct OperationRequest {
    pub bundle: u32,
    pub index: Option<usize>,
    pub filter: Option<OperationFilter>,
}

/// Operation with location information (bundle number and position)
#[derive(Debug, Clone)]
pub struct OperationWithLocation {
    pub operation: Operation,
    pub bundle: u32,
    pub position: usize,
    pub nullified: bool,
}
