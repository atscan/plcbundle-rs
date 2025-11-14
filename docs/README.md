# plcbundle-rs Documentation

## Overview

This directory contains the design and architecture documentation for the `plcbundle-rs` library.

## Documents

### 📘 [API.md](./API.md) - High-Level API Design

**Purpose**: Complete specification of the `BundleManager` API.

**Contents**:
- All public methods with signatures, purposes, and examples
- Option structs and result types
- Common patterns and design principles
- FFI mapping guidelines
- Implementation status

**Read this to**: Understand the complete API surface and design philosophy.

---

### 🔧 [REFACTORING.md](./REFACTORING.md) - Migration Roadmap

**Purpose**: Actionable plan to move CLI from direct file access to high-level API.

**Contents**:
- Current problems and anti-patterns
- Step-by-step refactoring guide
- Code examples (before/after)
- Testing strategy
- Timeline and checklist

**Read this to**: Understand what needs to be done to complete the API unification.

---

## Key Principles

### 1. Single Source of Truth

**All functionality lives in `BundleManager`**:
```
┌─────────────────────────────────────────┐
│         BundleManager (Rust)            │  ← Single Implementation
│  • Bundle loading                       │
│  • Operation access                     │
│  • Query/Export                         │
│  • Verification                         │
└─────────────────┬───────────────────────┘
                  │
      ┌───────────┴───────────┐
      │                       │
      ▼                       ▼
┌─────────────┐         ┌──────────┐
│  Rust CLI   │         │ C/Go FFI │
│  (consumer) │         │(consumer)│
└─────────────┘         └──────────┘
```

### 2. No Direct File Access

**Bad** ❌:
```rust
// CLI directly opens files
let file = std::fs::File::open("bundle.zst")?;
```

**Good** ✅:
```rust
// CLI uses high-level API
let op = manager.get_operation_raw(bundle, pos)?;
```

### 3. Options Pattern

Complex operations use dedicated option structs:
```rust
pub struct ExportSpec {
    pub bundles: BundleRange,
    pub format: ExportFormat,
    pub filter: Option<OperationFilter>,
    pub count: Option<usize>,
}

manager.export(spec)?;
```

### 4. Streaming by Default

Large datasets use iterators:
```rust
let iter = manager.query(spec)?;
for result in iter {
    println!("{}", result?);
}
```

### 5. FFI-Friendly Design

Every Rust API has a clean C mapping:
```rust
// Rust
manager.get_operation_raw(bundle, pos) -> Result<String>

// C
bundle_manager_get_operation_raw(mgr, bundle, pos, &out_json, &out_len) -> int

// Go
manager.GetOperationRaw(bundle, pos) (string, error)
```

---

## Current Status

### ✅ Implemented in `BundleManager`
- Bundle loading (`load_bundle`)
- Query operations (`query`)
- Export operations (`export`, `export_to_writer`)
- Verification (`verify_bundle`, `verify_chain`)
- Statistics (`get_stats`)
- DID indexing (`rebuild_did_index`)

### 🚧 Needs Refactoring
- **Operation access** - Currently in CLI only
  - `get_operation()` - Should be in `BundleManager`
  - `get_operation_raw()` - Should be in `BundleManager`
  - See [REFACTORING.md](./REFACTORING.md) for details

### 📋 To Implement
- Batch operations (`get_operations_batch`)
- Range operations (`get_operations_range`)
- DID lookup (`get_did_operations`, `batch_resolve_dids`)
- Cache warming (`prefetch_bundles`, `warm_up`)

---

## API Philosophy

### Performance First

The API is designed for **zero-cost abstractions**:
- Direct line reading for `get_operation_raw()` - **68µs**
- Streaming for large exports - **constant memory**
- Parallel query execution - **scales with cores**

### Flexibility

The API supports multiple use cases:
```rust
// Fast: Get raw JSON (preserves field order)
let json = manager.get_operation_raw(1, 0)?;

// Structured: Get parsed operation
let op = manager.get_operation(1, 0)?;

// Batch: Get multiple operations efficiently
let ops = manager.get_operations_batch(requests)?;

// Stream: Export millions of operations
let iter = manager.export(spec)?;
for line in iter { /* ... */ }
```

### Safety

Type-safe interfaces prevent common errors:
```rust
pub enum BundleRange {
    Single(u32),
    Range(u32, u32),
    List(Vec<u32>),
    All,
}

// Can't accidentally pass invalid range
manager.export(ExportSpec {
    bundles: BundleRange::Range(1, 100),
    // ...
})?;
```

---

## CLI Relationship

The CLI is a **thin wrapper** around `BundleManager`:

```rust
// CLI command handler (thin)
fn cmd_export(...) -> Result<()> {
    let manager = BundleManager::new(dir)?;
    
    let spec = ExportSpec {
        bundles: parse_range(range)?,
        format: format,
        // ...
    };
    
    let iter = manager.export(spec)?;
    
    for line in iter {
        println!("{}", line?);
    }
    
    Ok(())
}
```

**Benefits**:
- CLI is simple and testable
- Logic in `BundleManager` is reusable
- FFI can expose same functionality
- Easy to add new CLIs (Python, Node.js, etc.)

---

## FFI Architecture

```
┌────────────────────────────────────────────────┐
│                  Rust Core                     │
│            (src/manager.rs)                    │
│                                                │
│  pub struct BundleManager { ... }              │
│  impl BundleManager {                          │
│    pub fn get_operation_raw(...) -> Result<..> │
│    pub fn export(...) -> Result<...>           │
│    // ... all high-level APIs                  │
│  }                                             │
└──────────────────┬─────────────────────────────┘
                   │
                   │ Direct Rust API
                   │
      ┌────────────┼────────────┐
      │                         │
      ▼                         ▼
┌─────────────┐         ┌──────────────────┐
│  Rust CLI   │         │   FFI Layer      │
│             │         │  (src/ffi.rs)    │
│ Uses Rust   │         │                  │
│ API directly│         │  Exposes C API   │
└─────────────┘         └────────┬─────────┘
                                 │
                                 │ C ABI
                                 │
                        ┌────────▼──────────┐
                        │   Go Bindings     │
                        │(bindings/go/)     │
                        │                   │
                        │  Wraps C API in   │
                        │  idiomatic Go     │
                        └───────────────────┘
```

---

## Testing Strategy

### Unit Tests (Rust)
Test `BundleManager` methods directly:
```rust
#[test]
fn test_get_operation_raw() {
    let mgr = BundleManager::new("test_bundles").unwrap();
    let json = mgr.get_operation_raw(1, 0).unwrap();
    assert!(json.contains("did:plc:"));
}
```

### Integration Tests (FFI)
Test C API:
```rust
#[test]
fn test_ffi_operation_access() {
    let mgr = bundle_manager_new(c_str!("test_bundles"));
    let ret = bundle_manager_get_operation_raw(mgr, 1, 0, &mut json, &mut len);
    assert_eq!(ret, 0);
}
```

### Integration Tests (Go)
Test Go bindings:
```go
func TestGetOperation(t *testing.T) {
    mgr, _ := NewBundleManager("test_bundles")
    json, err := mgr.GetOperationRaw(1, 0)
    require.NoError(t, err)
}
```

---

## Next Steps

See [REFACTORING.md](./REFACTORING.md) for the detailed plan, but the immediate priorities are:

1. **Move `get_operation()` to `BundleManager`** ← Most urgent
2. Update CLI to use new API
3. Add FFI bindings
4. Add Go bindings
5. Write tests

This ensures:
- ✅ No direct file access in CLI
- ✅ Operation access available in Go
- ✅ Single source of truth
- ✅ Consistent API across languages

---

## Questions?

For specific implementation details:
- **API design**: See [API.md](./API.md)
- **Refactoring**: See [REFACTORING.md](./REFACTORING.md)
- **Code**: See `src/manager.rs` for current implementation

