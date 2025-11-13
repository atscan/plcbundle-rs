// src/operations.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub did: String,
    pub operation: String,
    #[serde(default)]
    pub cid: Option<String>,
    #[serde(default)]
    pub nullified: bool,
    pub created_at: String,
    #[serde(flatten)]
    pub extra: serde_json::Value,
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
