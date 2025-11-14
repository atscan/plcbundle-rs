# plcbundle-rs High-Level API Design

## Overview

The `plcbundle-rs` library provides a unified, high-level API through the `BundleManager` type. This API is designed to be:

- **Consistent**: Same patterns across all operations
- **Efficient**: Minimal allocations, streaming where possible
- **FFI-friendly**: Clean C bindings that map naturally to Go
- **CLI-first**: The Rust CLI uses only public high-level APIs

## Quick Reference: All Methods

```rust
// === Initialization ===
BundleManager::new(directory: PathBuf) -> Result<Self>
BundleManager::with_cache_size(directory: PathBuf, cache_size: usize) -> Result<Self>

// === Bundle Loading ===
load_bundle(num: u32, options: LoadOptions) -> Result<LoadResult>
get_bundle_info(num: u32, flags: InfoFlags) -> Result<BundleInfo>

// === Single Operation Access ===
get_operation_raw(bundle_num: u32, position: usize) -> Result<String>
get_operation(bundle_num: u32, position: usize) -> Result<Operation>
get_operation_with_stats(bundle_num: u32, position: usize) -> Result<OperationResult>

// === Batch Operations ===
get_operations_batch(requests: Vec<OperationRequest>) -> Result<Vec<Operation>>
get_operations_range(start: u32, end: u32, filter: Option<OperationFilter>) -> RangeIterator

// === Query & Export ===
query(spec: QuerySpec) -> QueryIterator
export(spec: ExportSpec) -> ExportIterator
export_to_writer<W: Write>(spec: ExportSpec, writer: W) -> Result<ExportStats>

// === DID Operations ===
get_did_operations(did: &str) -> Result<Vec<Operation>>
resolve_did(did: &str) -> Result<DIDDocument>
batch_resolve_dids(dids: Vec<String>) -> Result<HashMap<String, Vec<Operation>>>

// === Verification ===
verify_bundle(num: u32, spec: VerifySpec) -> Result<VerifyResult>
verify_chain(spec: ChainVerifySpec) -> Result<ChainVerifyResult>

// === Rollback ===
rollback_plan(spec: RollbackSpec) -> Result<RollbackPlan>
rollback(spec: RollbackSpec) -> Result<RollbackResult>

// === Performance & Caching ===
prefetch_bundles(nums: Vec<u32>) -> Result<()>
warm_up(spec: WarmUpSpec) -> Result<()>
clear_caches()

// === DID Index ===
rebuild_did_index(progress_cb: Option<ProgressCallback>) -> Result<RebuildStats>
get_did_index_stats() -> DIDIndexStats

// === Observability ===
get_stats() -> ManagerStats
get_last_bundle() -> u32
directory() -> &PathBuf
```

---

## Design Principles

1. **Single Entry Point**: All operations go through `BundleManager`
2. **Options Pattern**: Complex operations use dedicated option structs
3. **Result Types**: Operations return structured result types, not raw tuples
4. **Streaming by Default**: Use iterators for large datasets
5. **No Direct File Access**: CLI never opens bundle files directly

## API Structure

### Core Manager

```rust
pub struct BundleManager {
    // Private fields
}

impl BundleManager {
    pub fn new(directory: PathBuf) -> Result<Self>
    pub fn with_cache_size(directory: PathBuf, cache_size: usize) -> Result<Self>
}
```

---

## 1. Bundle Loading

### Individual Bundle Loading

```rust
pub fn load_bundle(&self, bundle_num: u32, options: LoadOptions) -> Result<LoadResult>
```

**Purpose**: Load a single bundle with control over parsing, verification, and caching.

**Options:**
```rust
pub struct LoadOptions {
    pub verify_hash: bool,      // Verify bundle integrity
    pub decompress: bool,        // Decompress bundle data
    pub cache: bool,             // Cache in memory
    pub parse_operations: bool,  // Parse JSON into Operation structs
}
```

**Result:**
```rust
pub struct LoadResult {
    pub bundle_number: u32,
    pub operations: Vec<Operation>,
    pub metadata: BundleMetadata,
    pub hash: Option<String>,
}
```

**Use Cases:**
- CLI: `plcbundle-rs info --bundle 42`
- CLI: `plcbundle-rs verify --bundle 42`

---

## 2. Operation Access

### Single Operation

```rust
pub fn get_operation(&self, bundle_num: u32, position: usize, options: OperationOptions) -> Result<Operation>
```

**Purpose**: Efficiently retrieve a single operation without loading entire bundle.

**Options:**
```rust
pub struct OperationOptions {
    pub raw_json: bool,          // Return raw JSON string (faster)
    pub parse: bool,             // Parse into Operation struct
}
```

**Use Cases:**
- CLI: `plcbundle-rs op get 42 1337`
- CLI: `plcbundle-rs op show 420000`

### Batch Operations

```rust
pub fn get_operations_batch(&self, requests: Vec<OperationRequest>) -> Result<Vec<Operation>>
```

**Purpose**: Retrieve multiple operations in one call (optimizes file I/O).

```rust
pub struct OperationRequest {
    pub bundle_num: u32,
    pub position: usize,
}
```

### Range Operations

```rust
pub fn get_operations_range(&self, start: u64, end: u64, filter: Option<OperationFilter>) -> Result<RangeIterator>
```

**Purpose**: Stream operations across bundle boundaries by global position.

---

## 3. DID Operations

### DID Lookup

```rust
pub fn get_did_operations(&self, did: &str) -> Result<Vec<Operation>>
```

**Purpose**: Get all operations for a specific DID (requires DID index).

### Batch DID Lookup

```rust
pub fn batch_resolve_dids(&self, dids: Vec<String>) -> Result<HashMap<String, Vec<Operation>>>
```

**Purpose**: Efficiently resolve multiple DIDs in one call.

**Use Cases:**
- Bulk DID resolution
- Identity verification workflows

---

## 4. Query & Export (Streaming)

### Query Operations

```rust
pub fn query(&self, spec: QuerySpec) -> Result<QueryIterator>
```

**Purpose**: Execute JMESPath or simple path queries across bundles.

```rust
pub struct QuerySpec {
    pub expression: String,          // JMESPath or simple path
    pub bundles: BundleRange,        // Which bundles to search
    pub mode: QueryMode,             // Auto, Simple, or JMESPath
    pub filter: Option<OperationFilter>,
    pub parallel: Option<usize>,     // Number of threads (0=auto)
    pub batch_size: usize,           // Output batch size
}

pub enum QueryMode {
    Auto,
    Simple,      // Fast path: direct field access
    JMESPath,    // Full JMESPath evaluation
}
```

**Iterator:**
```rust
pub struct QueryIterator {
    // Implements Iterator<Item = Result<String>>
}
```

**Use Cases:**
- CLI: `plcbundle-rs query "did" --bundles 1-100`
- CLI: `plcbundle-rs query "operation.type" --mode simple`

### Export Operations

```rust
pub fn export(&self, spec: ExportSpec) -> Result<ExportIterator>
```

**Purpose**: Export operations in various formats with filtering.

```rust
pub struct ExportSpec {
    pub bundles: BundleRange,
    pub format: ExportFormat,
    pub filter: Option<OperationFilter>,
    pub compression: Option<CompressionType>,
    pub count: Option<usize>,            // Limit number of operations
    pub after_timestamp: Option<String>, // Filter by timestamp
}

pub enum ExportFormat {
    Jsonl,
    Json,
    Csv,
}

pub enum BundleRange {
    Single(u32),
    Range(u32, u32),
    List(Vec<u32>),
    All,
}
```

**Use Cases:**
- CLI: `plcbundle-rs export --range 1-100 --format jsonl`
- CLI: `plcbundle-rs export --all --after 2024-01-01T00:00:00Z`

### Export with Callback

```rust
pub fn export_to_writer<W: Write>(&self, spec: ExportSpec, writer: W) -> Result<ExportStats>
```

**Purpose**: Export directly to a writer (file, stdout, network).

```rust
pub struct ExportStats {
    pub records_written: u64,
    pub bytes_written: u64,
    pub bundles_processed: u32,
    pub duration: Duration,
}
```

**Use Cases:**
- FFI: Stream to Go `io.Writer`
- Direct file writing without buffering

---

## 5. Verification

### Bundle Verification

```rust
pub fn verify_bundle(&self, bundle_num: u32, spec: VerifySpec) -> Result<VerifyResult>
```

**Purpose**: Verify bundle integrity with various checks.

```rust
pub struct VerifySpec {
    pub check_hash: bool,
    pub check_compression: bool,
    pub check_json: bool,
    pub parallel: bool,
}

pub struct VerifyResult {
    pub valid: bool,
    pub hash_match: Option<bool>,
    pub compression_ok: Option<bool>,
    pub json_valid: Option<bool>,
    pub errors: Vec<String>,
}
```

### Chain Verification

```rust
pub fn verify_chain(&self, spec: ChainVerifySpec) -> Result<ChainVerifyResult>
```

**Purpose**: Verify chain continuity and prev pointers.

```rust
pub struct ChainVerifySpec {
    pub bundles: BundleRange,
    pub check_prev_links: bool,
    pub check_timestamps: bool,
}

pub struct ChainVerifyResult {
    pub valid: bool,
    pub bundles_checked: u32,
    pub chain_breaks: Vec<ChainBreak>,
}

pub struct ChainBreak {
    pub bundle_num: u32,
    pub did: String,
    pub reason: String,
}
```

**Use Cases:**
- CLI: `plcbundle-rs verify --chain --bundles 1-100`

---

## 6. Bundle Information

```rust
pub fn get_bundle_info(&self, bundle_num: u32, flags: InfoFlags) -> Result<BundleInfo>
```

**Purpose**: Get metadata and statistics for a bundle.

```rust
pub struct InfoFlags {
    pub include_dids: bool,
    pub include_types: bool,
    pub include_timeline: bool,
}

pub struct BundleInfo {
    pub bundle_number: u32,
    pub operation_count: u32,
    pub did_count: Option<u32>,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub start_time: String,
    pub end_time: String,
    pub hash: String,
    pub operation_types: Option<HashMap<String, u32>>,
}
```

**Use Cases:**
- CLI: `plcbundle-rs info --bundle 42 --detailed`

---

## 7. Rollback Operations

### Plan Rollback

```rust
pub fn rollback_plan(&self, spec: RollbackSpec) -> Result<RollbackPlan>
```

**Purpose**: Preview what would be rolled back (dry-run).

```rust
pub struct RollbackSpec {
    pub target_bundle: u32,
    pub keep_index: bool,
}

pub struct RollbackPlan {
    pub bundles_to_remove: Vec<u32>,
    pub total_size: u64,
    pub operations_affected: u64,
}
```

### Execute Rollback

```rust
pub fn rollback(&self, spec: RollbackSpec) -> Result<RollbackResult>
```

**Purpose**: Execute the rollback operation.

```rust
pub struct RollbackResult {
    pub bundles_removed: Vec<u32>,
    pub bytes_freed: u64,
    pub success: bool,
}
```

---

## 8. Performance & Caching

### Cache Management

```rust
pub fn prefetch_bundles(&self, bundle_nums: Vec<u32>) -> Result<()>
```

**Purpose**: Preload bundles into cache for faster access.

```rust
pub fn warm_up(&self, spec: WarmUpSpec) -> Result<()>
```

**Purpose**: Warm up caches with intelligent prefetching.

```rust
pub struct WarmUpSpec {
    pub bundles: BundleRange,
    pub strategy: WarmUpStrategy,
}

pub enum WarmUpStrategy {
    Sequential,    // Load in order
    Parallel,      // Load concurrently
    Adaptive,      // Based on access patterns
}
```

```rust
pub fn clear_caches(&self)
```

**Purpose**: Clear all in-memory caches.

---

## 9. DID Index Management

### Rebuild Index

```rust
pub fn rebuild_did_index(&self, progress_cb: Option<ProgressCallback>) -> Result<RebuildStats>
```

**Purpose**: Rebuild the DID → operations index.

```rust
pub type ProgressCallback = Box<dyn Fn(u32, u32) + Send>; // (current, total)

pub struct RebuildStats {
    pub bundles_processed: u32,
    pub operations_indexed: u64,
    pub unique_dids: usize,
    pub duration: Duration,
}
```

### Index Statistics

```rust
pub fn get_did_index_stats(&self) -> Result<DIDIndexStats>
```

```rust
pub struct DIDIndexStats {
    pub total_dids: usize,
    pub total_operations: usize,
    pub index_size_bytes: usize,
}
```

---

## 10. Observability

### Manager Statistics

```rust
pub fn get_stats(&self) -> ManagerStats
```

**Purpose**: Get runtime statistics for monitoring.

```rust
pub struct ManagerStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub bundles_loaded: u64,
    pub operations_read: u64,
    pub bytes_read: u64,
}
```

---

## Common Types

### Operation Filter

```rust
pub struct OperationFilter {
    pub did: Option<String>,
    pub operation_type: Option<String>,
    pub time_range: Option<(String, String)>,
    pub include_nullified: bool,
}
```

Used across query, export, and range operations.

### Operation

```rust
pub struct Operation {
    pub did: String,
    pub operation: serde_json::Value,
    pub cid: Option<String>,
    pub nullified: bool,
    pub created_at: String,
    // Optional: raw_json field for zero-copy access
}
```

---

## CLI Usage Examples

All CLI commands use only the high-level API:

```bash
# Uses: get_operation()
plcbundle-rs op get 42 1337

# Uses: query()
plcbundle-rs query "did" --bundles 1-100

# Uses: export_to_writer()
plcbundle-rs export --range 1-100 -o output.jsonl

# Uses: get_bundle_info()
plcbundle-rs info --bundle 42

# Uses: verify_bundle()
plcbundle-rs verify --bundle 42 --checksums

# Uses: get_did_operations()
plcbundle-rs did-lookup did:plc:xyz123
```

---

## FFI Mapping

The high-level API maps cleanly to C/Go:

### C Bindings

```c
// Direct 1:1 mapping
CBundleManager* bundle_manager_new(const char* path);
int bundle_manager_load_bundle(CBundleManager* mgr, uint32_t num, CLoadOptions* opts, CLoadResult* result);
int bundle_manager_get_operation(CBundleManager* mgr, uint32_t bundle, size_t pos, COperation* out);
int bundle_manager_export(CBundleManager* mgr, CExportSpec* spec, ExportCallback cb, void* user_data);
```

### Go Bindings

```go
type BundleManager struct { /* ... */ }

func (m *BundleManager) LoadBundle(num uint32, opts LoadOptions) (*LoadResult, error)
func (m *BundleManager) GetOperation(bundle uint32, position int, opts OperationOptions) (*Operation, error)
func (m *BundleManager) Query(spec QuerySpec) (*QueryIterator, error)
func (m *BundleManager) Export(spec ExportSpec, opts ExportOptions) (*ExportStats, error)
```

---

## Implementation Status

### ✅ Implemented
- `load_bundle()`
- `get_bundle_info()`
- `query()`
- `export()` / `export_to_writer()`
- `verify_bundle()`
- `verify_chain()`
- `get_stats()`
- `rebuild_did_index()`

### 🚧 Needs Refactoring
- `get_operation()` - Currently in CLI, should be in `BundleManager`
- `get_operations_batch()` - Not yet implemented
- `get_operations_range()` - Partially implemented

### 📋 To Implement
- `get_did_operations()` - Uses DID index
- `batch_resolve_dids()` - Batch DID lookup
- `prefetch_bundles()` - Cache warming
- `warm_up()` - Intelligent prefetching

---

## Migration Plan

### Phase 1: Move Operation Access to Manager ✅ PRIORITY

```rust
// Add to BundleManager
impl BundleManager {
    /// Get a single operation efficiently (reads only the needed line)
    pub fn get_operation(&self, bundle_num: u32, position: usize) -> Result<OperationData>
    
    /// Get operation with raw JSON (zero-copy)
    pub fn get_operation_raw(&self, bundle_num: u32, position: usize) -> Result<String>
}

pub struct OperationData {
    pub raw_json: String,           // Original JSON from file
    pub parsed: Option<Operation>,  // Lazy parsing
}
```

### Phase 2: Update CLI to Use API

```rust
// Before (direct file access):
let file = std::fs::File::open(bundle_path)?;
let decoder = zstd::Decoder::new(file)?;
// ... manual line reading

// After (high-level API):
let op_data = manager.get_operation_raw(bundle_num, position)?;
println!("{}", op_data);
```

### Phase 3: Add to FFI/Go

Once in `BundleManager`, automatically available to FFI:

```c
int bundle_manager_get_operation_raw(
    CBundleManager* mgr,
    uint32_t bundle_num,
    size_t position,
    char** out_json,
    size_t* out_len
);
```

```go
func (m *BundleManager) GetOperationRaw(bundle uint32, position int) (string, error)
```

---

## Design Benefits

1. **Single Source of Truth**: All functionality in `BundleManager`
2. **Testable**: High-level API is easy to unit test
3. **Consistent**: Same patterns across Rust, C, and Go
4. **Maintainable**: Changes in one place affect all consumers
5. **Efficient**: API designed for performance (streaming, lazy parsing)
6. **Safe**: No direct file access by consumers

---

## Next Steps

1. ✅ Add `get_operation()` / `get_operation_raw()` to `BundleManager`
2. ✅ Update CLI `op get` to use new API
3. ✅ Add FFI bindings for operation access
4. ⏭️ Implement `get_operations_batch()`
5. ⏭️ Implement DID lookup operations
6. ⏭️ Add cache warming APIs

