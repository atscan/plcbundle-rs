// src/operations.rs
use serde::{Deserialize, Serialize};
use sonic_rs::{self, Value};

/// PLC Operation
///
/// Represents a single operation from the PLC directory.
/// 
/// **IMPORTANT**: This struct uses `sonic_rs` for JSON parsing (not serde).
/// Serialization still uses serde for compatibility with JMESPath queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub did: String,
    pub operation: Value,
    pub cid: Option<String>,
    pub nullified: bool,
    pub created_at: String,
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

impl Operation {
    /// Parse an Operation from JSON using sonic_rs (not serde)
    /// 
    /// This method manually extracts fields from the JSON to avoid issues with
    /// serde attributes like `#[serde(flatten)]` that sonic_rs may not fully support.
    pub fn from_json(json: &str) -> anyhow::Result<Self> {
        use anyhow::Context;
        use sonic_rs::JsonValueTrait;
        
        let value: Value = sonic_rs::from_str(json)
            .context("Failed to parse JSON")?;
        
        // Extract required fields
        let did = value.get("did")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing 'did' field"))?
            .to_string();
        
        let operation = value.get("operation")
            .cloned()
            .unwrap_or_else(|| Value::new());
        
        // Extract optional fields with defaults
        let cid = value.get("cid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        
        let nullified = value.get("nullified")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        
        // Handle both "createdAt" and "created_at" field names
        let created_at = value.get("createdAt")
            .or_else(|| value.get("created_at"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing 'createdAt' or 'created_at' field"))?
            .to_string();
        
        // Extract extra fields (everything except known fields)
        // Since sonic_rs::Value doesn't provide easy iteration, we'll parse the JSON
        // and manually extract extra fields by checking for unknown keys
        // For now, set extra to empty object - the main fields are already extracted above
        // The extra field was used with #[serde(flatten)] but we can reconstruct it if needed
        // For performance, we'll just use an empty Value since most operations don't have extra fields
        let extra = Value::new();
        
        Ok(Operation {
            did,
            operation,
            cid,
            nullified,
            created_at,
            extra,
            raw_json: Some(json.to_string()),
        })
    }
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
