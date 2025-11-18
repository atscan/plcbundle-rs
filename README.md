# plcbundle-rs

A high-performance [plcbundle](https://tangled.org/atscan.net/plcbundle/) management tool and library written in Rust.

## Overview

`plcbundle-rs` provides a universal and efficient interface for managing PLC bundle repositories. It offers:

- **Rust Library**: Core functionality with a clean, high-level API
- **CLI Tool**: Comprehensive command-line interface for bundle operations
- **HTTP Server**: Built-in server with WebSocket support
- **FFI Bindings**: C and Go bindings for cross-language integration

## Features

- ✅ **Bundle Management**: Load, inspect, and manipulate PLC bundles
- ✅ **DID Resolution**: Resolve DIDs and query operations
- ✅ **Synchronization**: Sync with remote PLC servers
- ✅ **Verification**: Verify bundle integrity and operation chains
- ✅ **Query & Export**: Flexible querying and export capabilities
- ✅ **Index Management**: Efficient DID indexing and lookup
- ✅ **Performance**: Memory-mapped files, parallel processing, and optimized data structures

## Installation

### From Source

```bash
# Clone the repository
git clone https://tangled.org/atscan.net/plcbundle-rs
cd plcbundle-rs

# Build with default features (CLI)
cargo build --release

# Install the CLI tool
cargo install --path .
```

### As a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
plcbundle = "0.9"
```

## Quick Start

### CLI Usage

```bash
# Initialize a new repository
plcbundle init --origin https://plc.directory

# Sync with remote server
plcbundle sync

# Query operations for a DID
plcbundle did <did:plc:...>

# Export bundles
plcbundle export --format ndjson

# Start HTTP server
plcbundle server --port 8080

# View repository status
plcbundle status
```

### Library Usage

```rust
use plcbundle::{BundleManager, ManagerOptions};

// Initialize manager
let manager = BundleManager::new(
    "/path/to/bundles",
    ManagerOptions::default()
)?;

// Resolve a DID
let doc = manager.resolve_did("did:plc:example")?;

// Query operations
let ops = manager.get_did_operations("did:plc:example")?;

// Load a bundle
let result = manager.load_bundle(1, Default::default())?;
```

## Project Structure

```
plcbundle-rs/
├── src/
│   ├── lib.rs              # Library entry point
│   ├── manager.rs          # Core BundleManager API
│   ├── cli/                # CLI commands
│   ├── server/             # HTTP server implementation
│   └── ...                 # Core modules
├── bindings/
│   └── go/                 # Go FFI bindings
├── docs/                   # Documentation
│   ├── API.md             # API reference
│   ├── BUNDLE_FORMAT.md   # Bundle format specification
│   └── ...
└── tests/                  # Integration tests
```

## Documentation

- [API Documentation](docs/API.md) - Complete API reference
- [Bundle Format](docs/BUNDLE_FORMAT.md) - Bundle file format specification
- [Logging](docs/LOGGING.md) - Logging configuration
- [Specification](docs/specification.md) - PLC specification

## Development

### Building

```bash
# Build with all features
cargo build --all-features

# Run tests
cargo test

# Generate documentation
cargo doc --open
```

### Features

- `cli` (default): Enables CLI functionality
- `server`: Enables HTTP server and WebSocket support

### Running Tests

```bash
# Run all tests
cargo test

# Run specific test suite
cargo test --test manager
cargo test --test server --features server
```

## 🚨 Important for Contributors and AI Assistants

**Please read [`RULES.md`](RULES.md) before contributing or generating code.**

Key principle: **CLI commands and server code NEVER open bundle files directly** - all operations go through the `BundleManager` API.

## License

Dual-licensed under MIT OR Apache-2.0

## Author

Tree <tree@tree.fail>

## Repository

https://tangled.org/atscan.net/plcbundle-rs