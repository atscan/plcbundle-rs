# plcbundle-rs Coding Rules

> **READ THIS FIRST**: All code contributors and AI assistants must follow these rules.

## 🚨 Critical Architecture Rule

### NO DIRECT FILE ACCESS FROM CLI

**The #1 Rule: CLI commands NEVER open bundle files directly**

```rust
// ❌ WRONG - Direct file access
let file = File::open(bundle_path)?;
let data = std::fs::read(path)?;
std::fs::remove_file(path)?;

// ✅ CORRECT - Via BundleManager API
manager.load_bundle(num, options)?;
manager.get_operation_raw(bundle, pos)?;
manager.delete_bundle_files(&[num])?;
```

## Architecture

All operations flow through `BundleManager`:

```
┌─────────────┐      ┌──────────────────┐      ┌────────────────┐
│ CLI Command │─────→│  BundleManager   │─────→│  File System   │
│             │      │   (Public API)   │      │                │
└─────────────┘      └──────────────────┘      └────────────────┘
     Uses                 Provides                Opens files
     Only                Public API               Directly
```

### Why This Rule Exists

1. **Single Source of Truth**: All file operations go through one place
2. **Consistency**: Same behavior across CLI, Go bindings, and library users
3. **Caching**: BundleManager handles caching transparently
4. **Testing**: Easy to mock and test through clean API
5. **Safety**: Centralized error handling and validation

## Core Design Principles

1. **Single Entry Point**: All operations go through `BundleManager`
2. **Options Pattern**: Complex operations use dedicated option structs  
3. **Result Types**: Operations return structured result types, not raw tuples
4. **Streaming by Default**: Use iterators for large datasets
5. **Modular Architecture**: `manager.rs` orchestrates, functionality lives in dedicated modules

## Module Organization

Functionality should be split into logical modules under `/src`:

```
src/
├── manager.rs          # Orchestrates components, provides public API
├── bundle_loading.rs   # Bundle loading operations
├── bundle_format.rs    # Bundle format (frames, compression)
├── operations.rs       # Operation types and filters
├── query.rs           # Query engine
├── export.rs          # Export operations
├── verification.rs    # Bundle verification
├── did_index.rs       # DID indexing
├── resolver.rs        # DID resolution
├── mempool.rs         # Mempool operations
├── sync.rs            # Sync from PLC directory
└── cache.rs           # Caching layer
```

**manager.rs should:**
- Define `BundleManager` struct
- Provide clean public API methods
- Delegate to specialized modules
- **NOT** contain complex implementation logic

**Specialized modules should:**
- Contain the actual implementation
- Be used by `manager.rs`
- Can have internal functions not exposed in public API

## When Adding New Features

### Step-by-Step Process

1. **Design the API signature** in `manager.rs`
2. **Document it** in `docs/API.md`
3. **Implement in appropriate module** (or create new module)
4. **Export types** in `src/lib.rs` if public
5. **Use from CLI** through the public API

### Example: Adding a New Feature

```rust
// 1. Add to manager.rs (public API)
impl BundleManager {
    pub fn new_feature(&self, param: Param) -> Result<Output> {
        // Delegate to specialized module
        specialized_module::do_the_work(self, param)
    }
}

// 2. Implement in specialized_module.rs
pub(crate) fn do_the_work(manager: &BundleManager, param: Param) -> Result<Output> {
    // Complex logic here
}

// 3. Use from CLI
pub fn cmd_new_feature(dir: PathBuf, param: Param) -> Result<()> {
    let manager = BundleManager::new(dir)?;
    let result = manager.new_feature(param)?;  // ✅ Via API
    // Display result
    Ok(())
}
```

## Common Mistakes to Avoid

### ❌ Don't Do This

```rust
// CLI command opening files directly
let bundle_path = dir.join(format!("{:06}.jsonl.zst", num));
let file = File::open(bundle_path)?;
let decoder = zstd::Decoder::new(file)?;
```

### ✅ Do This Instead

```rust
// CLI command using BundleManager API
let manager = BundleManager::new(dir)?;
let result = manager.load_bundle(num, LoadOptions::default())?;
```

### ❌ Don't Do This

```rust
// Complex logic in manager.rs
impl BundleManager {
    pub fn complex_operation(&self) -> Result<Output> {
        // 200 lines of implementation
        // parsing, processing, formatting...
    }
}
```

### ✅ Do This Instead

```rust
// Manager delegates to specialized module
impl BundleManager {
    pub fn complex_operation(&self) -> Result<Output> {
        specialized_module::perform_complex_operation(self)
    }
}

// Implementation in specialized_module.rs
pub(crate) fn perform_complex_operation(manager: &BundleManager) -> Result<Output> {
    // 200 lines of implementation here
}
```

## Testing Guidelines

- Test through the public API, not internal implementation
- CLI tests should use `BundleManager` instances
- Mock file system through `BundleManager` in tests
- Integration tests in `tests/` directory

## Documentation

When adding/changing APIs:

1. Update `docs/API.md` with method signature and examples
2. Add doc comments to public functions
3. Update CHANGELOG.md if user-facing

## Questions?

**Need file access?**
1. Check if `BundleManager` has the method ✅
2. If not, add it to `BundleManager` first ✅
3. Implement in appropriate module ✅
4. Update `docs/API.md` ✅
5. Use from CLI ✅

**Remember**: The CLI is just a thin wrapper around `BundleManager`!

---

See also:
- `docs/API.md` - Complete API reference
- `docs/BUNDLE_FORMAT.md` - Bundle file format specification
- `.cursorrules` - Cursor-specific rules

