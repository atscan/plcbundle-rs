// DID Resolution - Convert PLC operations to W3C DID Documents
use crate::operations::Operation;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};
use std::collections::HashMap;

// ============================================================================
// DID State (PLC-specific format)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DIDState {
    pub did: String,
    #[serde(rename = "rotationKeys")]
    pub rotation_keys: Vec<String>,
    #[serde(rename = "verificationMethods")]
    pub verification_methods: HashMap<String, String>,
    #[serde(rename = "alsoKnownAs")]
    pub also_known_as: Vec<String>,
    pub services: HashMap<String, ServiceDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceDefinition {
    #[serde(rename = "type")]
    pub service_type: String,
    pub endpoint: String,
}

// ============================================================================
// W3C DID Document
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DIDDocument {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    pub id: String,
    #[serde(rename = "alsoKnownAs", skip_serializing_if = "Vec::is_empty")]
    pub also_known_as: Vec<String>,
    #[serde(rename = "verificationMethod")]
    pub verification_method: Vec<VerificationMethod>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub service: Vec<Service>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationMethod {
    pub id: String,
    #[serde(rename = "type")]
    pub key_type: String,
    pub controller: String,
    #[serde(rename = "publicKeyMultibase")]
    pub public_key_multibase: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Service {
    pub id: String,
    #[serde(rename = "type")]
    pub service_type: String,
    #[serde(rename = "serviceEndpoint")]
    pub service_endpoint: String,
}

// ============================================================================
// Resolution Functions
// ============================================================================

/// Resolve DID to W3C DID Document from operations
pub fn resolve_did_document(did: &str, operations: &[Operation]) -> Result<DIDDocument> {
    if operations.is_empty() {
        anyhow::bail!("no operations found for DID");
    }

    // Build current state from operations
    let state = build_did_state(did, operations)?;

    // Convert to DID document format
    Ok(state_to_did_document(&state))
}

/// Build DID state by applying operations in order
pub fn build_did_state(did: &str, operations: &[Operation]) -> Result<DIDState> {
    let mut state: Option<DIDState> = None;

    for op in operations {
        // Skip nullified operations
        if op.nullified {
            continue;
        }

        // Check operation type
        if let Some(op_type) = op.operation.get("type").and_then(|v| v.as_str()) {
            // Handle tombstone (deactivated DID)
            if op_type == "plc_tombstone" {
                anyhow::bail!("DID has been deactivated");
            }
        }

        // Initialize state on first operation
        if state.is_none() {
            state = Some(DIDState {
                did: did.to_string(),
                rotation_keys: Vec::new(),
                verification_methods: HashMap::new(),
                also_known_as: Vec::new(),
                services: HashMap::new(),
            });
        }

        // Apply operation to state
        apply_operation_to_state(state.as_mut().unwrap(), &op.operation);
    }

    state.ok_or_else(|| anyhow::anyhow!("no valid operations found"))
}

/// Apply a single operation to the state
fn apply_operation_to_state(state: &mut DIDState, op_data: &Value) {
    // Update rotation keys
    if let Some(rot_keys) = op_data.get("rotationKeys").and_then(|v| v.as_array()) {
        state.rotation_keys = rot_keys
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
    }

    // Update verification methods
    if let Some(vm) = op_data
        .get("verificationMethods")
        .and_then(|v| v.as_object())
    {
        state.verification_methods = vm
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.to_string(), s.to_string())))
            .collect();
    }

    // Handle legacy signingKey format
    if let Some(signing_key) = op_data.get("signingKey").and_then(|v| v.as_str()) {
        state
            .verification_methods
            .insert("atproto".to_string(), signing_key.to_string());
    }

    // Update alsoKnownAs
    if let Some(aka) = op_data.get("alsoKnownAs").and_then(|v| v.as_array()) {
        state.also_known_as = aka
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
    }

    // Handle legacy handle format
    if let Some(handle) = op_data.get("handle").and_then(|v| v.as_str()) {
        if state.also_known_as.is_empty() {
            state.also_known_as = vec![format!("at://{}", handle)];
        }
    }

    // Update services
    if let Some(services) = op_data.get("services").and_then(|v| v.as_object()) {
        state.services = services
            .iter()
            .filter_map(|(k, v)| {
                let service_type = v.get("type")?.as_str()?.to_string();
                let endpoint = v.get("endpoint")?.as_str()?.to_string();
                Some((
                    k.to_string(),
                    ServiceDefinition {
                        service_type,
                        endpoint: normalize_service_endpoint(&endpoint),
                    },
                ))
            })
            .collect();
    }

    // Handle legacy service format
    if let Some(service) = op_data.get("service").and_then(|v| v.as_str()) {
        state.services.insert(
            "atproto_pds".to_string(),
            ServiceDefinition {
                service_type: "AtprotoPersonalDataServer".to_string(),
                endpoint: normalize_service_endpoint(service),
            },
        );
    }
}

/// Convert PLC state to W3C DID Document
fn state_to_did_document(state: &DIDState) -> DIDDocument {
    // Base contexts - always include multikey
    let mut contexts = vec![
        "https://www.w3.org/ns/did/v1".to_string(),
        "https://w3id.org/security/multikey/v1".to_string(),
    ];

    let mut has_secp256k1 = false;
    let mut has_p256 = false;

    // Check verification method key types
    for did_key in state.verification_methods.values() {
        match detect_key_type(did_key) {
            KeyType::Secp256k1 => has_secp256k1 = true,
            KeyType::P256 => has_p256 = true,
            _ => {}
        }
    }

    // Add suite-specific contexts
    if has_secp256k1 {
        contexts.push("https://w3id.org/security/suites/secp256k1-2019/v1".to_string());
    }
    if has_p256 {
        contexts.push("https://w3id.org/security/suites/ecdsa-2019/v1".to_string());
    }

    // Convert services
    let services = state
        .services
        .iter()
        .map(|(id, svc)| Service {
            id: format!("#{}", id),
            service_type: svc.service_type.clone(),
            service_endpoint: svc.endpoint.clone(),
        })
        .collect();

    // Convert verification methods
    let verification_methods = state
        .verification_methods
        .iter()
        .map(|(id, did_key)| VerificationMethod {
            id: format!("{}#{}", state.did, id),
            key_type: "Multikey".to_string(),
            controller: state.did.clone(),
            public_key_multibase: extract_multibase_from_did_key(did_key),
        })
        .collect();

    DIDDocument {
        context: contexts,
        id: state.did.clone(),
        also_known_as: state.also_known_as.clone(),
        verification_method: verification_methods,
        service: services,
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

#[derive(Debug, PartialEq)]
enum KeyType {
    Secp256k1,
    P256,
    Ed25519,
    Unknown,
}

fn detect_key_type(did_key: &str) -> KeyType {
    let multibase = extract_multibase_from_did_key(did_key);

    if multibase.len() < 3 {
        return KeyType::Unknown;
    }

    // The 'z' is base58btc multibase prefix, check next characters
    match &multibase[1..3] {
        "Q3" => KeyType::Secp256k1, // zQ3s...
        "Dn" => KeyType::P256,      // zDn...
        "6M" => KeyType::Ed25519,   // z6Mk...
        _ => KeyType::Unknown,
    }
}

fn extract_multibase_from_did_key(did_key: &str) -> String {
    did_key
        .strip_prefix("did:key:")
        .unwrap_or(did_key)
        .to_string()
}

fn normalize_service_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("https://{}", endpoint)
    }
}

// ============================================================================
// Validation
// ============================================================================

pub fn validate_did_format(did: &str) -> Result<()> {
    if !did.starts_with("did:plc:") {
        anyhow::bail!("invalid DID method: must start with 'did:plc:'");
    }

    if did.len() != 32 {
        anyhow::bail!("invalid DID length: expected 32 chars, got {}", did.len());
    }

    // Validate identifier part (24 chars, base32 alphabet)
    let identifier = &did[8..];
    if identifier.len() != 24 {
        anyhow::bail!(
            "invalid identifier length: expected 24 chars, got {}",
            identifier.len()
        );
    }

    // Check base32 alphabet (a-z, 2-7)
    for c in identifier.chars() {
        if !matches!(c, 'a'..='z' | '2'..='7') {
            anyhow::bail!(
                "invalid character in identifier: {} (must be base32: a-z, 2-7)",
                c
            );
        }
    }

    Ok(())
}

// ============================================================================
// Audit Log Formatting
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub did: String,
    #[serde(skip)]
    pub operation: Value,
    pub cid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullified: Option<bool>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

/// Format operations as an audit log
pub fn format_audit_log(operations: &[Operation]) -> Vec<AuditLogEntry> {
    operations
        .iter()
        .map(|op| AuditLogEntry {
            did: op.did.clone(),
            operation: op.operation.clone(),
            cid: op.cid.clone(),
            nullified: if op.nullified { Some(true) } else { None },
            created_at: op.created_at.clone(),
        })
        .collect()
}
