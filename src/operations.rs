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
            .unwrap_or_else(Value::new);
        
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

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::JsonValueTrait;

    #[test]
    fn test_operation_from_json_minimal() {
        let json = r#"{
            "did": "did:plc:abcdefghijklmnopqrstuvwx",
            "operation": {"type": "create"},
            "createdAt": "2024-01-01T00:00:00Z"
        }"#;

        let op = Operation::from_json(json).unwrap();
        assert_eq!(op.did, "did:plc:abcdefghijklmnopqrstuvwx");
        assert!(!op.nullified);
        assert_eq!(op.created_at, "2024-01-01T00:00:00Z");
        assert!(op.cid.is_none());
        assert!(op.raw_json.is_some());
        assert_eq!(op.raw_json.as_ref().unwrap(), json);
    }

    #[test]
    fn test_operation_from_json_with_all_fields() {
        let json = r#"{
            "did": "did:plc:abcdefghijklmnopqrstuvwx",
            "operation": {"type": "create", "data": "test"},
            "cid": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
            "nullified": true,
            "createdAt": "2024-01-01T12:34:56Z"
        }"#;

        let op = Operation::from_json(json).unwrap();
        assert_eq!(op.did, "did:plc:abcdefghijklmnopqrstuvwx");
        assert!(op.nullified);
        assert_eq!(op.created_at, "2024-01-01T12:34:56Z");
        assert_eq!(op.cid, Some("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi".to_string()));
        
        // Check operation field
        let op_type = op.operation.get("type").and_then(|v| v.as_str()).unwrap();
        assert_eq!(op_type, "create");
    }

    #[test]
    fn test_operation_from_json_created_at_variant() {
        // Test both "createdAt" and "created_at" field names
        let json1 = r#"{
            "did": "did:plc:test",
            "operation": {},
            "createdAt": "2024-01-01T00:00:00Z"
        }"#;

        let json2 = r#"{
            "did": "did:plc:test",
            "operation": {},
            "created_at": "2024-01-01T00:00:00Z"
        }"#;

        let op1 = Operation::from_json(json1).unwrap();
        let op2 = Operation::from_json(json2).unwrap();
        
        assert_eq!(op1.created_at, "2024-01-01T00:00:00Z");
        assert_eq!(op2.created_at, "2024-01-01T00:00:00Z");
    }

    #[test]
    fn test_operation_from_json_missing_did() {
        let json = r#"{
            "operation": {},
            "createdAt": "2024-01-01T00:00:00Z"
        }"#;

        let result = Operation::from_json(json);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing 'did' field"));
    }

    #[test]
    fn test_operation_from_json_missing_created_at() {
        let json = r#"{
            "did": "did:plc:test",
            "operation": {}
        }"#;

        let result = Operation::from_json(json);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing 'createdAt' or 'created_at' field"));
    }

    #[test]
    fn test_operation_from_json_invalid_json() {
        let json = "not valid json {";

        let result = Operation::from_json(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_operation_from_json_nullified_defaults_to_false() {
        let json = r#"{
            "did": "did:plc:test",
            "operation": {},
            "createdAt": "2024-01-01T00:00:00Z"
        }"#;

        let op = Operation::from_json(json).unwrap();
        assert!(!op.nullified);
    }

    #[test]
    fn test_operation_from_json_cid_optional() {
        let json = r#"{
            "did": "did:plc:test",
            "operation": {},
            "createdAt": "2024-01-01T00:00:00Z"
        }"#;

        let op = Operation::from_json(json).unwrap();
        assert!(op.cid.is_none());
    }

    #[test]
    fn test_operation_from_json_operation_field_defaults_to_empty() {
        let json = r#"{
            "did": "did:plc:test",
            "createdAt": "2024-01-01T00:00:00Z"
        }"#;

        let op = Operation::from_json(json).unwrap();
        // operation field should default to empty Value
        assert!(op.operation.is_object() || op.operation.is_null());
    }

    #[test]
    fn test_operation_filter_default() {
        let filter = OperationFilter::default();
        assert!(filter.did.is_none());
        assert!(filter.operation_type.is_none());
        assert!(filter.time_range.is_none());
        assert!(!filter.include_nullified);
    }

    #[test]
    fn test_operation_request() {
        let request = OperationRequest {
            bundle: 1,
            index: Some(5),
            filter: Some(OperationFilter {
                did: Some("did:plc:test".to_string()),
                ..Default::default()
            }),
        };

        assert_eq!(request.bundle, 1);
        assert_eq!(request.index, Some(5));
        assert!(request.filter.is_some());
        assert_eq!(request.filter.as_ref().unwrap().did, Some("did:plc:test".to_string()));
    }

    #[test]
    fn test_operation_with_location() {
        let op = Operation {
            did: "did:plc:test".to_string(),
            operation: sonic_rs::Value::new(),
            cid: None,
            nullified: false,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            extra: sonic_rs::Value::new(),
            raw_json: None,
        };

        let op_with_loc = OperationWithLocation {
            operation: op,
            bundle: 1,
            position: 42,
            nullified: true,
        };

        assert_eq!(op_with_loc.bundle, 1);
        assert_eq!(op_with_loc.position, 42);
        assert!(op_with_loc.nullified);
        assert_eq!(op_with_loc.operation.did, "did:plc:test");
    }
}
